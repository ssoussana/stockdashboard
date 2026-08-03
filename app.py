"""
Market Pulse dashboard server.

Runs locally, fetches Finnhub quotes server-side (no CORS issue that way),
and serves the dashboard page. Your API key stays on your machine only —
it's read from an environment variable and never sent to the browser.

Setup:
    pip install flask requests
    export FINNHUB_API_KEY="your-key-here"
    export TWELVE_DATA_API_KEY="your-key-here"   (optional, powers the SMA/RSI line and
        after-hours prices — free at twelvedata.com, no card required. Without it,
        those sections just show "n/a".)
    export FRED_API_KEY="your-key-here"           (optional, powers real crude oil
        $/barrel and 10Y Treasury yield — free at fredaccount.stlouisfed.org, no
        card required, no paid tiers at all. Without it, that section is empty.)
    python app.py

Then open http://localhost:5000
"""

import collections
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
import requests
from flask import Flask, jsonify, request, send_from_directory

API_KEY = os.environ.get("FINNHUB_API_KEY")
if not API_KEY:
    raise SystemExit(
        "FINNHUB_API_KEY is not set.\n"
        "Set it first, e.g.:\n"
        "  export FINNHUB_API_KEY=your-key-here   (Mac/Linux)\n"
        "  setx FINNHUB_API_KEY your-key-here      (Windows)"
    )

# Optional — only needed for the SMA feature. If unset, /api/sma just
# reports every symbol as unavailable instead of crashing the whole app.
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

# Optional — currently only used by the /api/debug/test-fmp-* routes,
# which exist to probe whether specific FMP free-tier endpoints (market
# hours/holidays, index quotes) are actually accessible on Steve's key
# before building anything real on top of them — FMP's free tier has
# already surprised us twice with 402s on endpoints that looked free
# from the docs (sp500-constituent, batch-quote).
FMP_API_KEY = os.environ.get("FMP_API_KEY")

# Optional — only needed for real crude oil/treasury-yield data.
FRED_API_KEY = os.environ.get("FRED_API_KEY")

# Optional — only needed for a real Nasdaq Composite index value. Falls
# back to the ONEQ ETF proxy automatically if not set or if it fails.
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")

DEFAULT_SYMBOLS = ["PLTR", "NVDA", "CLS", "NBIS", "HOOD", "SPY", "QQQ",
                    "GLW", "CRDO", "COHR", "SOXL", "DRAM", "IREN"]

# The watchlist is shared — everyone viewing the dashboard sees the same
# list, and adding/removing a ticker affects everyone's view. Persisted to
# a JSON file. DATA_DIR defaults to the app's own directory (fine locally,
# but that directory gets rebuilt from scratch on every real Render deploy
# — only ordinary restarts within the same deploy keep it). Set DATA_DIR to
# a mounted persistent disk's path (e.g. /data) so this survives deploys too.
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.json")
_watchlist_lock = threading.Lock()


def load_shared_watchlist():
    try:
        with open(WATCHLIST_FILE) as f:
            data = json.load(f)
        if isinstance(data, list) and data:
            # Uppercase + dedupe while preserving order.
            return list(dict.fromkeys(str(s).strip().upper() for s in data if str(s).strip()))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[watchlist] failed to load {WATCHLIST_FILE}: {e!r}", flush=True)
    return list(DEFAULT_SYMBOLS)


def save_shared_watchlist():
    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(_shared_watchlist, f)
    except Exception as e:
        print(f"[watchlist] failed to save {WATCHLIST_FILE}: {e!r}", flush=True)


_shared_watchlist = load_shared_watchlist()
print(f"[watchlist] using {WATCHLIST_FILE} (DATA_DIR={'set' if 'DATA_DIR' in os.environ else 'default, NOT persistent across deploys'})", flush=True)

# One shared connection pool for every outbound call, instead of `requests`
# implicitly building a fresh connection/SSL context on every single get().
# On a memory-constrained instance (this app's free-tier host gives it only
# 512MB), that per-call overhead was likely a real contributor to the OOM
# kills we've been seeing — those wipe every in-memory cache when they
# happen, which explains a lot of the "why did this reset" confusion.
_process_started_at = time.time()
_http = requests.Session()

CACHE_SECONDS = 60  # per-symbol, shared across all visitors requesting that symbol


def load_json_cache(filename):
    """Generic loader for the disk-backed caches below. Returns {} on any
    failure (missing file, bad JSON, no DATA_DIR mounted yet) so this is
    always safe to call at startup regardless of whether a persistent disk
    is actually attached."""
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[cache] failed to load {path}: {e!r}", flush=True)
        return {}


def save_json_cache(filename, data):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"[cache] failed to save {path}: {e!r}", flush=True)


_quote_cache = load_json_cache("quote_cache.json")   # symbol -> {"data": {...}, "ts": float}

# SMA is daily data, doesn't need to update every minute — cache it longer.
# Loaded from disk at startup so a fresh deploy shows the last known values
# immediately (even if a bit stale) instead of blank "not fetched yet" while
# a new background pass completes — only actually persists across deploys
# if DATA_DIR points at a mounted disk, same as the watchlist file above.
SMA_CACHE_SECONDS = 60 * 60 * 4  # 4 hours
SMA_CACHE_FILE = "sma_cache.json"
_sma_cache = load_json_cache(SMA_CACHE_FILE)      # symbol -> {"data": {...}, "ts": float}

# The full set of symbols anyone's watchlist currently includes. Grows as
# people add tickers; the SMA background loop iterates over this each pass.
# A symbol only leaves the background rotation via the TTL check below —
# nothing explicitly removes it when a user removes it from the shared
# watchlist. Seeded from BOTH the hardcoded defaults and the persisted
# shared watchlist (not just the defaults) — otherwise every restart would
# treat the full existing watchlist as "brand new," firing an immediate-fetch
# thread per symbol all at once and flooding the shared rate-limit budget
# before the main scheduled pass ever gets a fair turn at it.
_known_symbols = set(DEFAULT_SYMBOLS) | set(_shared_watchlist)
# Seeded with the current time so none of these are wrongly treated as
# "stale" by the TTL check before any real browser request ever arrives.
_known_last_seen = {sym: time.time() for sym in _known_symbols}
_known_lock = threading.Lock()
_wake_event = threading.Event()  # lets a newly-added symbol skip the long wait
KNOWN_SYMBOL_TTL = 2 * 60 * 60  # drop from background refresh after 2h of no requests


def parse_symbols_param():
    raw = request.args.get("symbols", "")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return symbols or list(DEFAULT_SYMBOLS)


def register_known_symbols(symbols):
    """Adds newly-seen symbols to the shared watch set, kicks off an
    immediate one-off SMA/RSI and earnings fetch for each brand-new one
    (so neither has to wait its turn in the next scheduled pass), and
    wakes the SMA background loop so the new symbol is included in future
    full passes. Also refreshes each symbol's last-seen time, which
    active_known_symbols() uses to drop stale/orphaned symbols out of the
    background rotation."""
    now = time.time()
    with _known_lock:
        new = set(symbols) - _known_symbols
        if new:
            _known_symbols.update(new)
        for sym in symbols:
            _known_last_seen[sym] = now
    if new:
        _wake_event.set()
        for sym in new:
            threading.Thread(target=fetch_earnings_immediate, args=(sym,), daemon=True).start()
        if TWELVE_DATA_API_KEY:
            for sym in new:
                threading.Thread(target=fetch_sma_immediate, args=(sym,), daemon=True).start()


def active_known_symbols():
    """Symbols actually requested within the TTL window — used by the SMA
    background pass so orphaned/abandoned symbols stop consuming budget."""
    now = time.time()
    with _known_lock:
        return sorted(
            s for s in _known_symbols
            if now - _known_last_seen.get(s, 0) < KNOWN_SYMBOL_TTL
        )

app = Flask(__name__, static_folder=".")


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


def fetch_quote_one(sym):
    try:
        r = _http.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": sym, "token": API_KEY},
            timeout=6,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("c") is None:
            raise ValueError("no data")
        return {"ok": True, **data}
    except requests.exceptions.HTTPError:
        # Report only the status code — never the underlying exception,
        # which embeds the full request URL including the API key.
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception:
        return {"ok": False, "error": "fetch failed"}


_quote_executor = ThreadPoolExecutor(max_workers=4)  # reduced from 8 — fewer
                                                       # simultaneous open
                                                       # connections, still
                                                       # fast for typical
                                                       # watchlist sizes


def is_market_hours_with_buffer(buffer_minutes=15):
    """Same as is_regular_market_hours but widened by a buffer on each
    side — used to gate live stock quote fetching specifically, so it
    starts a little before open and keeps going a little after close."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return False
    start = (datetime.combine(now_et.date(), dt_time(9, 30)) - timedelta(minutes=buffer_minutes)).time()
    end = (datetime.combine(now_et.date(), dt_time(16, 0)) + timedelta(minutes=buffer_minutes)).time()
    return start <= now_et.time() < end


def get_quote_cached(sym):
    entry = _quote_cache.get(sym)
    if entry and (time.time() - entry["ts"]) < CACHE_SECONDS:
        return entry["data"]
    return None


@app.route("/api/watchlist")
def get_watchlist():
    with _watchlist_lock:
        symbols = list(_shared_watchlist)
    register_known_symbols(symbols)
    return jsonify({"symbols": symbols})


@app.route("/api/watchlist/add", methods=["POST"])
def add_to_watchlist():
    body = request.get_json(silent=True) or {}
    sym = str(body.get("symbol", "")).strip().upper()
    if not sym:
        return jsonify({"error": "symbol required"}), 400
    with _watchlist_lock:
        if sym not in _shared_watchlist:
            _shared_watchlist.append(sym)
            save_shared_watchlist()
        symbols = list(_shared_watchlist)
    register_known_symbols(symbols)
    return jsonify({"symbols": symbols})


@app.route("/api/watchlist/remove", methods=["POST"])
def remove_from_watchlist():
    body = request.get_json(silent=True) or {}
    sym = str(body.get("symbol", "")).strip().upper()
    with _watchlist_lock:
        if sym in _shared_watchlist:
            _shared_watchlist.remove(sym)
            save_shared_watchlist()
        symbols = list(_shared_watchlist)
    return jsonify({"symbols": symbols})


@app.route("/api/quotes")
def quotes():
    symbols = parse_symbols_param()
    register_known_symbols(symbols)

    out = {}
    to_fetch = []
    for sym in symbols:
        cached = get_quote_cached(sym)
        if cached is not None:
            out[sym] = cached
        else:
            to_fetch.append(sym)

    if to_fetch:
        if is_market_hours_with_buffer():
            # Fetch cache misses concurrently rather than one at a time — with
            # a growing watchlist, sequential fetching was slow enough to trip
            # the host's request timeout and crash the worker mid-request,
            # wiping other in-memory data (including SMA/RSI) along with it.
            results = list(_quote_executor.map(fetch_quote_one, to_fetch))
            now = time.time()
            for sym, data in zip(to_fetch, results):
                _quote_cache[sym] = {"data": data, "ts": now}
                out[sym] = data
            save_json_cache("quote_cache.json", _quote_cache)
        else:
            # Outside market hours (+/- 15min buffer) — don't spend API
            # calls on prices that aren't moving. Serve the last known
            # price if we have one, even if stale, rather than a fresh pull.
            for sym in to_fetch:
                stale = _quote_cache.get(sym)
                if stale:
                    out[sym] = stale["data"]
                else:
                    out[sym] = {"ok": False, "error": "market closed — no cached price yet"}

    return jsonify(out)


@app.route("/api/search")
def search():
    """Proxies Finnhub's symbol search server-side, so the API key never
    reaches the browser. Used by the watchlist's add-a-ticker box."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    try:
        r = _http.get(
            "https://finnhub.io/api/v1/search",
            params={"q": q, "token": API_KEY},
            timeout=6,
        )
        r.raise_for_status()
        results = r.json().get("result", [])
        # Keep it to plain US-listed tickers (skip symbols with a "."
        # suffix, which are mostly foreign-exchange listings Finnhub's
        # free tier doesn't quote well anyway) and cap the list short.
        out = [
            {"symbol": item["symbol"], "description": item.get("description", "")}
            for item in results
            if item.get("symbol") and "." not in item["symbol"]
        ][:8]
        return jsonify(out)
    except Exception:
        return jsonify([])


TWELVE_DATA_BATCH_SIZE = 4   # free-tier limit is 8 credits/minute, but this
                              # is set to 4 (half) deliberately: Render can
                              # briefly run two worker processes at once
                              # during a deploy, each with its own
                              # independent budget tracker — confirmed via
                              # the account's own usage graph showing a
                              # 23-credit spike in one minute against the
                              # real 8/min limit. Halving this means even a
                              # two-worker overlap (4+4=8) stays within the
                              # real account-wide limit instead of doubling
                              # past it.

# One shared limiter for every Twelve Data call, whichever feature makes it
# (SMA/RSI's scheduled pass, an immediate one-off add, or after-hours
# prices). Blocks the caller until enough credits are free in the trailing
# 60-second window, so two features can never independently burst past the
# combined free-tier limit — replaces the old fixed-pause approach, which
# only worked as long as nothing else was also calling Twelve Data.
_credit_lock = threading.Lock()
_credit_timestamps = collections.deque()


def acquire_twelvedata_credits(n):
    waited = 0
    while True:
        with _credit_lock:
            now = time.time()
            while _credit_timestamps and now - _credit_timestamps[0] > 60:
                _credit_timestamps.popleft()
            if len(_credit_timestamps) + n <= TWELVE_DATA_BATCH_SIZE:
                for _ in range(n):
                    _credit_timestamps.append(now)
                return
            in_use = len(_credit_timestamps)
        if waited and waited % 30 == 0:
            print(f"[twelvedata] still waiting for {n} credit(s) after {waited}s "
                  f"({in_use}/{TWELVE_DATA_BATCH_SIZE} in use)", flush=True)
        time.sleep(2)
        waited += 2


def twelvedata_get(url, params, timeout=(5, 15)):
    """GET with one automatic retry on a 429. Our credit tracker only
    knows about calls made by *this* process — during a Render deploy
    transition, the outgoing and incoming workers can briefly run at the
    same time, each thinking it has the full budget, and together exceed
    the real account-wide limit. That's a real, if narrow, gap in a
    purely in-memory rate limiter; retrying once after a short wait
    covers it without needing a shared external store."""
    r = _http.get(url, params=params, timeout=timeout)
    if r.status_code == 429:
        print(f"[twelvedata] 429 rate limited, waiting 20s and retrying once: {url}", flush=True)
        time.sleep(20)
        r = _http.get(url, params=params, timeout=timeout)
    return r


def compute_rsi14(closes, period=14):
    """Standard 14-day RSI using Wilder's smoothing method."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def fetch_sma_batch(symbols):
    """One HTTP call per batch (size set by TWELVE_DATA_BATCH_SIZE) — Twelve Data accepts
    comma-separated symbols in a single request, so this needs far fewer
    round trips than fetching one symbol at a time."""
    acquire_twelvedata_credits(len(symbols))
    print(f"[sma] requesting batch: {symbols}", flush=True)
    t0 = time.time()
    r = twelvedata_get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": ",".join(symbols),
            "interval": "1day",
            "outputsize": 100,  # a bit more than SMA needs, for RSI to converge well
            "apikey": TWELVE_DATA_API_KEY,
        },
    )
    print(f"[sma] batch {symbols} responded in {time.time()-t0:.1f}s, status {r.status_code}", flush=True)
    payload = r.json()
    # A single-symbol request returns one object with a top-level "values"
    # key; a multi-symbol request returns an object keyed by symbol. Normalize
    # to the latter shape so the parsing below is the same either way.
    if len(symbols) == 1 and "values" in payload:
        payload = {symbols[0]: payload}

    out = {}
    for sym in symbols:
        entry = payload.get(sym)
        try:
            if not entry:
                raise ValueError("symbol missing from batch response")
            if entry.get("status") == "error":
                raise ValueError(entry.get("message", "twelve data error"))
            values = entry.get("values")
            if not values:
                raise ValueError("no data returned")
            closes = [float(v["close"]) for v in reversed(values)]  # oldest → newest
            if len(closes) < 20:
                raise ValueError(f"only {len(closes)} days of history returned")
            sma20 = sum(closes[-20:]) / 20
            sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
            rsi14 = compute_rsi14(closes)
            out[sym] = {"ok": True, "sma20": round(sma20, 2),
                        "sma50": round(sma50, 2) if sma50 else None,
                        "rsi14": rsi14}
        except Exception as e:
            # No secret is embedded in this error (unlike the Finnhub
            # handling above), so it's safe to surface directly.
            out[sym] = {"ok": False, "error": str(e)}
    return out


_immediate_lock = threading.Lock()  # serializes immediate fetches so rapid
                                     # successive adds don't burst past the
                                     # free-tier rate limit


def fetch_sma_immediate(sym):
    """Fetches SMA/RSI for one newly-added symbol right away, independent
    of the scheduled background pass — runs in its own short-lived thread
    so the request that added the symbol isn't held up waiting on it."""
    with _immediate_lock:
        print(f"[sma] immediate fetch for new symbol {sym}", flush=True)
        try:
            batch_out = fetch_sma_batch([sym])
            _sma_cache[sym] = {"data": batch_out[sym], "ts": time.time()}
        except Exception as e:
            print(f"[sma] immediate fetch for {sym} failed: {e!r}", flush=True)
            _sma_cache[sym] = {"data": {"ok": False, "error": f"immediate fetch failed: {e}"}, "ts": time.time()}


def fetch_sma_all(symbols):
    """Fetches every symbol in batches, writing each batch's results into
    the shared per-symbol cache as soon as that batch finishes — not just
    at the end — so results (or a real error) show up within seconds
    instead of only after the full pass. Pacing between batches happens
    automatically inside fetch_sma_batch via the shared credit limiter."""
    total_batches = (len(symbols) + TWELVE_DATA_BATCH_SIZE - 1) // TWELVE_DATA_BATCH_SIZE
    _sma_status["total_batches"] = total_batches
    for i in range(0, len(symbols), TWELVE_DATA_BATCH_SIZE):
        batch_num = i // TWELVE_DATA_BATCH_SIZE + 1
        batch = symbols[i:i + TWELVE_DATA_BATCH_SIZE]
        _sma_status["current_batch"] = batch_num
        _sma_status["current_batch_started"] = time.time()
        _sma_status["current_step"] = "acquiring credits"
        print(f"[sma] batch {batch_num}/{total_batches}: acquiring credits for {batch}", flush=True)
        try:
            batch_out = fetch_sma_batch(batch)
        except Exception as e:
            print(f"[sma] batch {batch} raised: {e!r}", flush=True)
            batch_out = {sym: {"ok": False, "error": f"batch request failed: {e}"} for sym in batch}
        now = time.time()
        for sym, data in batch_out.items():
            _sma_cache[sym] = {"data": data, "ts": now}
        save_json_cache(SMA_CACHE_FILE, _sma_cache)
        _sma_status["current_step"] = "batch complete"
    _sma_status["current_batch"] = None
    print("[sma] pass complete", flush=True)


@app.route("/api/sma")
def sma():
    symbols = parse_symbols_param()
    register_known_symbols(symbols)

    if not TWELVE_DATA_API_KEY:
        return jsonify({sym: {"ok": False, "error": "TWELVE_DATA_API_KEY not set"} for sym in symbols})

    out = {}
    for sym in symbols:
        entry = _sma_cache.get(sym)
        if entry:
            out[sym] = entry["data"]
        else:
            elapsed = int(time.time() - _sma_status["attempt_started"])
            progress = ""
            if _sma_status.get("current_batch"):
                progress = (f", batch {_sma_status['current_batch']}/{_sma_status.get('total_batches','?')}"
                            f" ({_sma_status.get('current_step','?')})")
            out[sym] = {"ok": False, "error": f"not fetched yet ({elapsed}s since current pass started{progress})"}
    return jsonify(out)


_sma_status = {"attempt_started": time.time()}


def _sma_background_loop():
    """Runs forever in its own thread, refreshing the SMA cache on a slow
    cadence. Kept out of the request/response cycle entirely — the pacing
    this needs (to respect Twelve Data's free-tier rate limit) would
    otherwise make /api/sma block long enough to get killed by the host's
    request timeout, which is exactly what happened before this.

    Waits on _wake_event rather than a plain sleep, so adding a brand-new
    ticker to the watchlist triggers a fresh pass soon instead of waiting
    up to the full cache window."""
    print("[sma] background thread started", flush=True)
    while True:
        if TWELVE_DATA_API_KEY:
            symbols = active_known_symbols()
            _sma_status["attempt_started"] = time.time()
            try:
                fetch_sma_all(symbols)
            except Exception as e:
                print(f"[sma] fetch_sma_all raised: {e!r}", flush=True)
        else:
            print("[sma] TWELVE_DATA_API_KEY not set, skipping", flush=True)
        _wake_event.clear()
        print(f"[sma] sleeping up to {SMA_CACHE_SECONDS}s (or until a new symbol wakes this up)", flush=True)
        _wake_event.wait(timeout=SMA_CACHE_SECONDS)


threading.Thread(target=_sma_background_loop, daemon=True).start()


# ---------- S&P 500 constituents (used to filter market movers) ----------
# FMP's /stable/sp500-constituent endpoint turned out to require a paid
# plan (402 Payment Required on Steve's free-tier key). Membership only
# changes a handful of times a year, so instead of paying for a live API,
# this re-scrapes Wikipedia's public "List of S&P 500 companies" page
# every 45 days in the background — no redeploy needed for the list
# itself to stay current, only this mechanism needed deploying once.
#
# SP500_SYMBOLS_SEED is the fallback: a static snapshot (sourced from the
# same Wikipedia page) current as of 2026-08-03, used until the first
# live scrape succeeds, and used again afterward if a scrape ever fails
# or returns something implausible (Wikipedia's table markup changing
# would break the parser below — this bounds the damage from that).
SP500_SYMBOLS_SEED = frozenset({
    "A", "AAPL", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI", "ADM",
    "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIZ", "AJG", "AKAM",
    "ALB", "ALGN", "ALL", "ALLE", "AMAT", "AMCR", "AMD", "AME", "AMGN", "AMP",
    "AMT", "AMZN", "ANET", "AON", "AOS", "APA", "APD", "APH", "APO", "APP",
    "APTV", "ARE", "ARES", "ATO", "AVB", "AVGO", "AVY", "AWK", "AXON", "AXP",
    "AZO", "BA", "BAC", "BALL", "BAX", "BBY", "BDX", "BEN", "BF.B", "BG",
    "BIIB", "BKNG", "BKR", "BLDR", "BLK", "BMY", "BNY", "BR", "BRK.B", "BRO",
    "BSX", "BX", "BXP", "C", "CAG", "CAH", "CARR", "CASY", "CAT", "CB",
    "CBOE", "CBRE", "CCI", "CCL", "CDNS", "CDW", "CEG", "CF", "CFG", "CHD",
    "CHRW", "CHTR", "CI", "CIEN", "CINF", "CL", "CLX", "CMCSA", "CME", "CMG",
    "CMI", "CMS", "CNC", "CNP", "COF", "COHR", "COIN", "COO", "COP", "COR",
    "COST", "CPAY", "CPB", "CPRT", "CPT", "CRH", "CRL", "CRM", "CRWD", "CSCO",
    "CSGP", "CSX", "CTAS", "CTSH", "CTVA", "CVNA", "CVS", "CVX", "D", "DAL",
    "DASH", "DD", "DDOG", "DE", "DECK", "DELL", "DG", "DGX", "DHI", "DHR",
    "DIS", "DLR", "DLTR", "DOC", "DOV", "DOW", "DPZ", "DRI", "DTE", "DUK",
    "DVA", "DVN", "DXCM", "EA", "EBAY", "ECL", "ED", "EFX", "EG", "EIX",
    "EL", "ELV", "EME", "EMR", "EOG", "EPAM", "EQIX", "EQR", "EQT", "ERIE",
    "ES", "ESS", "ETN", "ETR", "EVRG", "EW", "EXC", "EXE", "EXPD", "EXPE",
    "EXR", "F", "FANG", "FAST", "FCX", "FDS", "FDX", "FE", "FFIV", "FICO",
    "FIS", "FISV", "FITB", "FIX", "FOX", "FOXA", "FRT", "FSLR", "FTNT", "FTV",
    "GD", "GDDY", "GE", "GEHC", "GEN", "GEV", "GILD", "GIS", "GL", "GLW",
    "GM", "GNRC", "GOOG", "GOOGL", "GPC", "GPN", "GRMN", "GS", "GWW", "HAL",
    "HAS", "HBAN", "HCA", "HD", "HIG", "HII", "HLT", "HON", "HOOD", "HPE",
    "HPQ", "HRL", "HSIC", "HST", "HSY", "HUBB", "HUM", "HWM", "IBKR", "IBM",
    "ICE", "IDXX", "IEX", "IFF", "INCY", "INTC", "INTU", "INVH", "IP", "IQV",
    "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "J", "JBHT", "JBL", "JCI",
    "JKHY", "JNJ", "JPM", "KDP", "KEY", "KEYS", "KHC", "KIM", "KKR", "KLAC",
    "KMB", "KMI", "KO", "KR", "KVUE", "L", "LDOS", "LEN", "LH", "LHX",
    "LII", "LIN", "LITE", "LLY", "LMT", "LNT", "LOW", "LRCX", "LULU", "LUV",
    "LVS", "LYB", "LYV", "MA", "MAA", "MAR", "MAS", "MCD", "MCHP", "MCK",
    "MCO", "MDLZ", "MDT", "MET", "META", "MGM", "MKC", "MLM", "MMM", "MNST",
    "MO", "MOS", "MPC", "MPWR", "MRK", "MRNA", "MRSH", "MS", "MSCI", "MSFT",
    "MSI", "MTB", "MTD", "MU", "NCLH", "NDAQ", "NDSN", "NEE", "NEM", "NFLX",
    "NI", "NKE", "NOC", "NOW", "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVDA",
    "NVR", "NWS", "NWSA", "NXPI", "O", "ODFL", "OKE", "OMC", "ON", "ORCL",
    "ORLY", "OTIS", "OXY", "PANW", "PAYX", "PCAR", "PCG", "PEG", "PEP", "PFE",
    "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PLD", "PLTR", "PM", "PNC",
    "PNR", "PNW", "PODD", "POOL", "PPG", "PPL", "PRU", "PSA", "PSKY", "PSX",
    "PTC", "PWR", "PYPL", "Q", "QCOM", "RCL", "REG", "REGN", "RF", "RJF",
    "RL", "RMD", "ROK", "ROL", "ROP", "ROST", "RSG", "RTX", "RVTY", "SATS",
    "SBAC", "SBUX", "SCHW", "SHW", "SJM", "SLB", "SMCI", "SNA", "SNDK", "SNPS",
    "SO", "SOLV", "SPG", "SPGI", "SRE", "STE", "STLD", "STT", "STX", "STZ",
    "SW", "SWK", "SWKS", "SYF", "SYK", "SYY", "T", "TAP", "TDG", "TDY",
    "TECH", "TEL", "TER", "TFC", "TGT", "TJX", "TKO", "TMO", "TMUS", "TPL",
    "TPR", "TRGP", "TRMB", "TROW", "TRV", "TSCO", "TSLA", "TSN", "TT", "TTD",
    "TTWO", "TXN", "TXT", "TYL", "UAL", "UBER", "UDR", "UHS", "ULTA", "UNH",
    "UNP", "UPS", "URI", "USB", "V", "VEEV", "VICI", "VLO", "VLTO", "VMC",
    "VRSK", "VRSN", "VRT", "VRTX", "VST", "VTR", "VTRS", "VZ", "WAB", "WAT",
    "WBD", "WDAY", "WDC", "WEC", "WELL", "WFC", "WM", "WMB", "WMT", "WRB",
    "WSM", "WST", "WTW", "WY", "WYNN", "XEL", "XOM", "XYL", "XYZ", "YUM",
    "ZBH", "ZBRA", "ZTS",
})

SP500_REFRESH_SECONDS = 45 * 24 * 60 * 60  # ~45 days
SP500_WIKI_URL = "https://en.wikipedia.org/api/rest_v1/page/html/List_of_S%26P_500_companies"
# A parsed list is only trusted if it falls in this range — the real
# count is ~503 (500 companies, a few with dual share classes). Anything
# wildly outside this suggests the parser broke against a markup change,
# not a real S&P 500 size change.
SP500_PLAUSIBLE_COUNT_RANGE = (450, 550)

_sp500_cache = load_json_cache("sp500_constituents_cache.json") or {"data": None, "ts": 0}
_sp500_status = {"last_attempt": None, "last_error": None, "last_success_source": None}


def _strip_html(fragment):
    """Strips tags and unescapes entities from an HTML fragment — used
    instead of pulling in a full HTML-parsing dependency for this one
    scheduled task."""
    import html as _html_mod
    text = re.sub(r"<[^>]+>", "", fragment)
    return _html_mod.unescape(text).strip()


def fetch_sp500_symbols_from_wikipedia():
    """Scrapes Wikipedia's "List of S&P 500 companies" page for the
    current constituent table. There's no clean structured API for this
    data for free, so this parses the rendered HTML table directly —
    more fragile than a real API, which is why every result is sanity-
    checked against SP500_PLAUSIBLE_COUNT_RANGE before being trusted."""
    _sp500_status["last_attempt"] = time.time()
    try:
        r = _http.get(
            SP500_WIKI_URL,
            headers={"User-Agent": "StockDashboard-Personal/1.0 (personal project; low-frequency, every 45 days)"},
            timeout=(5, 20),
        )
        r.raise_for_status()
        html_text = r.text

        # The constituents table is the first table on the page marked
        # both "wikitable" and "sortable" (the separate historical
        # "changes" table further down isn't marked sortable).
        table_match = None
        for m in re.finditer(r"<table\b([^>]*)>(.*?)</table>", html_text, re.S | re.I):
            attrs, body = m.group(1), m.group(2)
            if "wikitable" in attrs and "sortable" in attrs:
                table_match = body
                break
        if table_match is None:
            raise ValueError("couldn't find a wikitable+sortable table on the page")

        rows = re.findall(r"<tr\b.*?>(.*?)</tr>", table_match, re.S | re.I)
        symbols = []
        rejected = []
        for row in rows:
            cells = re.findall(r"<td\b.*?>(.*?)</td>", row, re.S | re.I)
            if not cells:
                continue  # header row uses <th>, not <td> — skip it
            symbol = _strip_html(cells[0])
            # Tickers are short, uppercase, and at most one "." (e.g.
            # BRK.B). Anything else suggests we grabbed the wrong cell —
            # e.g. a stray footnote marker or a markup change — so it's
            # dropped rather than silently polluting the filter list.
            if symbol and re.fullmatch(r"[A-Z]{1,6}(\.[A-Z]{1,2})?", symbol):
                symbols.append(symbol)
            elif symbol:
                rejected.append(symbol)

        symbols = sorted(set(symbols))
        lo, hi = SP500_PLAUSIBLE_COUNT_RANGE
        if not (lo <= len(symbols) <= hi):
            raise ValueError(
                f"parsed {len(symbols)} valid-looking symbols ({len(rejected)} rejected), "
                f"outside plausible range {lo}-{hi} — Wikipedia's table markup may have changed. "
                f"Sample rejected: {rejected[:5]}"
            )

        _sp500_cache["data"] = symbols
        _sp500_cache["ts"] = time.time()
        save_json_cache("sp500_constituents_cache.json", _sp500_cache)
        _sp500_status["last_error"] = None
        _sp500_status["last_success_source"] = "wikipedia"
        print(f"[sp500] updated from Wikipedia: {len(symbols)} symbols", flush=True)
    except Exception as e:
        _sp500_status["last_error"] = str(e)
        print(f"[sp500] Wikipedia scrape failed, keeping last-known-good list: {e!r}", flush=True)
        raise


def get_sp500_symbols():
    """The set actually used for filtering — live-scraped data if
    available, falling back to the embedded seed otherwise."""
    live = _sp500_cache.get("data")
    return frozenset(live) if live else SP500_SYMBOLS_SEED


@app.route("/api/debug/sp500-constituents")
def sp500_constituents_debug():
    live = _sp500_cache.get("data")
    return jsonify({
        "active_count": len(get_sp500_symbols()),
        "source": "wikipedia (live)" if live else "seed fallback (static, 2026-08-03)",
        "cache_seconds_old": round(time.time() - _sp500_cache["ts"], 1) if live else None,
        "last_attempt_seconds_ago": round(time.time() - _sp500_status["last_attempt"], 1) if _sp500_status["last_attempt"] else None,
        "last_error": _sp500_status["last_error"],
        "refresh_interval_days": SP500_REFRESH_SECONDS / 86400,
    })


def _sp500_background_loop():
    print("[sp500] background thread started", flush=True)
    time.sleep(35)  # staggered — see movers loop comment
    while True:
        cache_age = time.time() - _sp500_cache["ts"] if _sp500_cache.get("data") else None
        if cache_age is None or cache_age >= SP500_REFRESH_SECONDS:
            try:
                fetch_sp500_symbols_from_wikipedia()
            except Exception as e:
                print(f"[sp500] fetch_sp500_symbols_from_wikipedia raised: {e!r}", flush=True)
            sleep_s = SP500_REFRESH_SECONDS
        else:
            # A fresh-enough cache already exists (e.g. survived a
            # redeploy) — just wait out the rest of its 45-day window.
            sleep_s = SP500_REFRESH_SECONDS - cache_age
        time.sleep(sleep_s)


threading.Thread(target=_sp500_background_loop, daemon=True).start()


# ---------- Market movers (top gainers/losers, computed from S&P 500
# constituents directly — not tied to the watchlist) ----------
# Previously tried FMP's whole-market "biggest movers" feed filtered to
# S&P 500 (near-empty — blue-chips rarely make "biggest % movers" lists
# dominated by micro-caps), then FMP's batch-quote endpoint (requires a
# paid $59/mo Premium plan). This uses Finnhub instead — the same
# provider (and the same fetch_quote_one/_quote_executor/_quote_cache)
# already powering the watchlist — so it costs nothing extra, and any
# symbol that's in both the watchlist and S&P 500 (e.g. NVDA, PLTR,
# HOOD) is a cache hit here instead of a duplicate API call.
#
# Finnhub's free tier is 60 calls/minute. This paces the 503-symbol
# sweep in batches of MOVERS_SP500_PACE_PER_MIN per minute rather than
# firing them all at once, deliberately leaving headroom: even if the
# watchlist grows to ~25 symbols (current + 10ish more), combined usage
# stays comfortably under 60/min. At 25/min the full sweep takes ~20
# minutes, well inside the 30-minute refresh cadence.
MOVERS_CACHE_SECONDS = 30 * 60
MOVERS_SP500_PACE_PER_MIN = 25
_movers_cache = load_json_cache("movers_cache.json") or {"data": None, "ts": 0}
_movers_status = {"last_attempt": None, "last_error": None}


def fetch_sp500_quotes_paced():
    symbols = sorted(get_sp500_symbols())
    quotes = []
    for i in range(0, len(symbols), MOVERS_SP500_PACE_PER_MIN):
        batch_start = time.time()
        batch = symbols[i:i + MOVERS_SP500_PACE_PER_MIN]

        # Cache hits (e.g. symbols also on the watchlist, already fetched
        # this minute) don't count against pacing — only symbols we
        # actually need to hit Finnhub for do.
        to_fetch = []
        for sym in batch:
            cached = get_quote_cached(sym)
            if cached is not None:
                if cached.get("ok"):
                    quotes.append({"symbol": sym, **cached})
            else:
                to_fetch.append(sym)

        if to_fetch:
            results = list(_quote_executor.map(fetch_quote_one, to_fetch))
            now = time.time()
            for sym, data in zip(to_fetch, results):
                _quote_cache[sym] = {"data": data, "ts": now}
                if data.get("ok"):
                    quotes.append({"symbol": sym, **data})
            save_json_cache("quote_cache.json", _quote_cache)

        is_last_batch = (i + MOVERS_SP500_PACE_PER_MIN) >= len(symbols)
        if not is_last_batch:
            elapsed = time.time() - batch_start
            remaining = 60 - elapsed
            if remaining > 0:
                time.sleep(remaining)
    return quotes


def fetch_movers_all():
    _movers_status["last_attempt"] = time.time()
    print("[movers] requesting S&P 500 quotes via Finnhub (paced sweep)", flush=True)
    try:
        quotes = fetch_sp500_quotes_paced()
    except Exception as e:
        _movers_status["last_error"] = str(e)
        print(f"[movers] fetch failed: {e!r}", flush=True)
        raise

    valid = []
    for q in quotes:
        price = q.get("c")
        pct = q.get("dp")
        if price is None or pct is None:
            continue
        valid.append({
            "symbol": q["symbol"],
            # Finnhub's quote endpoint doesn't return a company name —
            # the ticker itself is what's shown.
            "name": q["symbol"],
            "price": price,
            "change": q.get("d"),
            "changesPercentage": pct,
        })

    gainers = sorted(valid, key=lambda x: x["changesPercentage"], reverse=True)[:5]
    losers = sorted(valid, key=lambda x: x["changesPercentage"])[:5]

    _movers_cache["data"] = {"gainers": gainers, "losers": losers}
    _movers_cache["ts"] = time.time()
    save_json_cache("movers_cache.json", _movers_cache)
    _movers_status["last_error"] = None
    print(f"[movers] updated: {len(gainers)} gainers, {len(losers)} losers "
          f"(from {len(valid)} of {len(quotes)} S&P 500 quotes)", flush=True)


@app.route("/api/movers")
def movers():
    if _movers_cache["data"] is None:
        return jsonify({"error": "not fetched yet", "gainers": [], "losers": []})
    return jsonify(_movers_cache["data"])


@app.route("/api/debug/movers")
def movers_debug():
    return jsonify({
        "cache_seconds_old": round(time.time() - _movers_cache["ts"], 1) if _movers_cache["data"] else None,
        "cached_data": _movers_cache["data"],
        "last_attempt_seconds_ago": round(time.time() - _movers_status["last_attempt"], 1) if _movers_status["last_attempt"] else None,
        "last_error": _movers_status["last_error"],
        "process_id": os.getpid(),
        "process_uptime_seconds": round(time.time() - _process_started_at, 1),
        "source": "S&P 500 quotes via Finnhub (paced sweep, shares cache with watchlist)",
        "sp500_constituent_count": len(get_sp500_symbols()),
        "pace_per_minute": MOVERS_SP500_PACE_PER_MIN,
    })


def _movers_background_loop():
    print("[movers] background thread started", flush=True)
    time.sleep(5)  # staggered so background threads don't all burst-fetch
                   # simultaneously at boot, competing for this host's 0.5 CPU

    # Pull immediately on boot if currently within market hours, so a
    # redeploy during the trading day doesn't leave movers sitting on
    # stale data until the next scheduled window. Unlike the index/gold
    # loops' immediate-fetch (a single cheap call), this one is a full
    # ~500-call paced sweep — not worth running on a redeploy at 2am
    # when prices aren't moving anyway, so it's gated by market hours
    # here rather than being truly unconditional.
    if is_market_hours_with_buffer():
        try:
            fetch_movers_all()
        except Exception as e:
            print(f"[movers] initial fetch raised: {e!r}", flush=True)

    while True:
        # Same market-hours +/-15min window already used to gate regular
        # watchlist quote fetching — no point sweeping 503 symbols for
        # prices that aren't moving outside trading hours.
        if is_market_hours_with_buffer():
            try:
                fetch_movers_all()
            except Exception as e:
                print(f"[movers] fetch_movers_all raised: {e!r}", flush=True)
            sleep_s = MOVERS_CACHE_SECONDS
        else:
            print("[movers] outside movers update window, skipping", flush=True)
            now_et = datetime.now(ZoneInfo("America/New_York"))
            next_window_open = next_market_open_et(now_et) - timedelta(minutes=15)
            minutes_to_open = (next_window_open - now_et).total_seconds() / 60
            sleep_s = min(max(minutes_to_open, 1) * 60, 30 * 60)
        time.sleep(sleep_s)


threading.Thread(target=_movers_background_loop, daemon=True).start()


# ---------- Macro (crude oil, 10-year Treasury yield) ----------
MACRO_CACHE_SECONDS = 60 * 60  # FRED itself only updates once per business day
MACRO_CACHE_FILE = "macro_cache.json"
_macro_cache = load_json_cache(MACRO_CACHE_FILE) or {"data": None, "ts": 0}
MACRO_SERIES = {"crude_oil": "DCOILWTICO", "treasury_10y": "DGS10"}
MACRO_LABELS = {"crude_oil": "Crude Oil, WTI $/barrel (FRED)", "treasury_10y": "10Y Treasury Yield (FRED)"}


def fetch_fred_series(series_id):
    """Latest two valid observations for a FRED series, used to compute the
    latest value plus day-over-day change. FRED sometimes reports the most
    recent date(s) as "." (not yet published) — pulling a handful of recent
    observations and skipping missing ones handles that."""
    r = _http.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 10,
        },
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    if "error_message" in payload:
        raise ValueError(payload["error_message"])

    valid = [o for o in payload.get("observations", []) if o.get("value") not in (None, ".")]
    if not valid:
        raise ValueError("no published observations")

    latest = float(valid[0]["value"])
    result = {"value": latest, "date": valid[0]["date"], "change": None, "percent_change": None, "prior_percent_change": None}
    if len(valid) > 1:
        prev = float(valid[1]["value"])
        result["change"] = latest - prev
        result["percent_change"] = (latest - prev) / prev * 100 if prev else None
    if len(valid) > 2:
        # The period-over-period change one step further back — lets a
        # caller compare "this period's rate" to "the prior period's rate"
        # to see if a trend is accelerating or decelerating.
        prior = float(valid[2]["value"])
        result["prior_percent_change"] = (prev - prior) / prior * 100 if prior else None
    return result


def fetch_macro_all():
    """Real crude oil ($/barrel) and real 10-year Treasury yield, straight
    from FRED (the St. Louis Fed) — the actual official government data,
    not an ETF proxy. Free with no paid tiers, but only updates once per
    business day (not intraday), which is why this refreshes hourly rather
    than every minute — checking more often wouldn't find anything new."""
    print("[macro] requesting crude oil and 10Y treasury from FRED", flush=True)
    out = {}
    for key, series_id in MACRO_SERIES.items():
        try:
            r = fetch_fred_series(series_id)
            out[key] = {
                "ok": True,
                "value": round(r["value"], 2),
                "change": round(r["change"], 2) if r["change"] is not None else None,
                "percent_change": round(r["percent_change"], 2) if r["percent_change"] is not None else None,
                "as_of": r["date"],
            }
        except Exception as e:
            print(f"[macro] {key} ({series_id}) failed: {e!r}", flush=True)
            out[key] = {"ok": False, "error": str(e)}
    _macro_cache["data"] = out
    _macro_cache["ts"] = time.time()
    save_json_cache(MACRO_CACHE_FILE, _macro_cache)
    print(f"[macro] updated: {out}", flush=True)


@app.route("/api/macro")
def macro():
    if not FRED_API_KEY:
        return jsonify({k: {"ok": False, "error": "FRED_API_KEY not set"} for k in MACRO_SERIES})
    if _macro_cache["data"] is None:
        return jsonify({k: {"ok": False, "error": "not fetched yet"} for k in MACRO_SERIES})
    return jsonify(_macro_cache["data"])


@app.route("/api/debug/macro")
def macro_debug():
    return jsonify({
        "data_source": "FRED (St. Louis Fed) — real values, updates once/day",
        "fred_key_set": bool(FRED_API_KEY),
        "series": MACRO_SERIES,
        "labels": MACRO_LABELS,
        "cache_seconds_old": round(time.time() - _macro_cache["ts"], 1) if _macro_cache["data"] else None,
        "cached_data": _macro_cache["data"],
    })


def _macro_background_loop():
    print("[macro] background thread started", flush=True)
    time.sleep(10)  # staggered — see movers loop comment
    while True:
        if FRED_API_KEY:
            try:
                fetch_macro_all()
            except Exception as e:
                print(f"[macro] fetch_macro_all raised: {e!r}", flush=True)
        else:
            print("[macro] FRED_API_KEY not set, skipping", flush=True)
        time.sleep(MACRO_CACHE_SECONDS)


threading.Thread(target=_macro_background_loop, daemon=True).start()


# ---------- Earnings calendar ----------
EARNINGS_CACHE_SECONDS = 24 * 60 * 60  # earnings dates rarely change intraday
EARNINGS_CACHE_FILE = "earnings_cache.json"
_earnings_cache = load_json_cache(EARNINGS_CACHE_FILE)  # symbol -> {"data": {...}, "ts": float}
_earnings_executor = ThreadPoolExecutor(max_workers=4)


def fetch_earnings_one(sym):
    """Earnings info for one symbol, via Finnhub's earnings calendar
    (already using this key elsewhere — no new signup needed). Prefers a
    result reported within the last 14 days (real numbers, beat/miss vs
    estimates) over a future scheduled date, since a just-reported quarter
    is more useful to see than "next earnings in 3 months"."""
    today = datetime.now().date()
    try:
        r = _http.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={
                "symbol": sym,
                "from": (today - timedelta(days=14)).isoformat(),
                "to": (today + timedelta(days=180)).isoformat(),
                "token": API_KEY,
            },
            timeout=6,
        )
        r.raise_for_status()
        events = r.json().get("earningsCalendar", [])
        if not events:
            return {"ok": True, "kind": None}

        # A "recent" report: date in the past 14 days AND actually has
        # reported numbers (epsActual present) — a date alone doesn't
        # guarantee the report has landed yet.
        recent = [
            e for e in events
            if e.get("date") and e.get("epsActual") is not None
            and (today - timedelta(days=14)).isoformat() <= e["date"] <= today.isoformat()
        ]
        if recent:
            e = sorted(recent, key=lambda e: e["date"])[-1]  # most recent
            return {
                "ok": True, "kind": "recent", "date": e.get("date"),
                "eps_actual": e.get("epsActual"), "eps_estimate": e.get("epsEstimate"),
                "revenue_actual": e.get("revenueActual"), "revenue_estimate": e.get("revenueEstimate"),
            }

        upcoming = [e for e in events if e.get("date") and e["date"] >= today.isoformat()]
        if upcoming:
            e = sorted(upcoming, key=lambda e: e["date"])[0]  # soonest
            return {
                "ok": True, "kind": "upcoming", "date": e.get("date"),
                "hour": e.get("hour"), "eps_estimate": e.get("epsEstimate"),
            }

        return {"ok": True, "kind": None}
    except requests.exceptions.HTTPError:
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_earnings_immediate(sym):
    """One-off fetch for a brand-new symbol, so it doesn't wait for the
    next scheduled daily pass."""
    print(f"[earnings] immediate fetch for new symbol {sym}", flush=True)
    data = fetch_earnings_one(sym)
    _earnings_cache[sym] = {"data": data, "ts": time.time()}
    save_json_cache(EARNINGS_CACHE_FILE, _earnings_cache)


def fetch_earnings_all(symbols):
    print(f"[earnings] requesting {len(symbols)} symbols", flush=True)
    results = list(_earnings_executor.map(fetch_earnings_one, symbols))
    now = time.time()
    for sym, data in zip(symbols, results):
        _earnings_cache[sym] = {"data": data, "ts": now}
    save_json_cache(EARNINGS_CACHE_FILE, _earnings_cache)
    print("[earnings] pass complete", flush=True)


@app.route("/api/earnings")
def earnings():
    symbols = parse_symbols_param()
    register_known_symbols(symbols)
    out = {}
    for sym in symbols:
        entry = _earnings_cache.get(sym)
        out[sym] = entry["data"] if entry else {"ok": False, "error": "not fetched yet"}
    return jsonify(out)


@app.route("/api/debug/earnings")
def earnings_debug():
    ages = {sym: round(time.time() - e["ts"], 1) for sym, e in _earnings_cache.items()}
    return jsonify({
        "finnhub_key_set": bool(API_KEY),
        "known_symbols_active": active_known_symbols(),
        "cache_seconds_old": ages,
    })


def _earnings_background_loop():
    print("[earnings] background thread started", flush=True)
    time.sleep(15)  # staggered — see movers loop comment. This one's the
                     # heaviest burst (4 parallel workers), so it goes last.
    while True:
        try:
            fetch_earnings_all(active_known_symbols())
        except Exception as e:
            print(f"[earnings] fetch_earnings_all raised: {e!r}", flush=True)
        time.sleep(EARNINGS_CACHE_SECONDS)


threading.Thread(target=_earnings_background_loop, daemon=True).start()


# ---------- Gold / Silver / Bitcoin ----------
GOLD_API_BASE = "https://api.gold-api.com"
GOLD_CACHE_SECONDS = 60 * 60  # hourly, per request
GOLD_SYMBOLS = {"gold": "XAU", "silver": "XAG", "bitcoin": "BTC"}
GOLD_LABELS = {"gold": "Gold $/oz", "silver": "Silver $/oz", "bitcoin": "Bitcoin"}
_gold_cache = load_json_cache("gold_cache.json")
# gold-api.com's free price endpoint gives no daily change data, so we track
# our own rolling history and compute a 24h change from it — persisted to
# disk so this doesn't have to rebuild from scratch on every redeploy.
_gold_history = load_json_cache("gold_history.json")  # symbol -> [{"price":.., "ts":..}, ...]
GOLD_HISTORY_MAX_AGE = 48 * 60 * 60  # prune anything older than this


GOLD_API_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36")
}


_gold_status = {"last_attempt": None, "last_error": None}


def compute_24h_change(key, current_price, now):
    """Finds the history entry closest to 24h ago and returns percent
    change from it to current_price. Returns None if we don't have
    anything old enough yet (e.g. shortly after first deploy)."""
    history = _gold_history.get(key, [])
    target = now - 24 * 60 * 60
    candidates = [h for h in history if h["ts"] <= target]
    if not candidates:
        return None
    closest = max(candidates, key=lambda h: h["ts"])  # nearest to 24h ago, not older than needed
    old_price = closest["price"]
    if old_price == 0:
        return None
    return round((current_price - old_price) / old_price * 100, 2)


def fetch_gold_all():
    _gold_status["last_attempt"] = time.time()
    print("[gold] requesting gold/silver/bitcoin", flush=True)
    out = {}
    now = time.time()
    try:
        for key, symbol in GOLD_SYMBOLS.items():
            try:
                r = _http.get(f"{GOLD_API_BASE}/price/{symbol}", headers=GOLD_API_HEADERS, timeout=(5, 15))
                r.raise_for_status()
                data = r.json()
                price = data.get("price")
                if price is None:
                    raise ValueError("no price in response")
                price = round(float(price), 2)

                # Record this reading, then prune anything past the max age.
                hist = _gold_history.setdefault(key, [])
                hist.append({"price": price, "ts": now})
                _gold_history[key] = [h for h in hist if now - h["ts"] <= GOLD_HISTORY_MAX_AGE]

                out[key] = {
                    "ok": True,
                    "value": price,
                    "updated_at": data.get("updatedAt"),
                    "percent_change": compute_24h_change(key, price, now),
                }
            except Exception as e:
                print(f"[gold] {key} ({symbol}) failed: {e!r}", flush=True)
                out[key] = {"ok": False, "error": str(e)}
        _gold_cache["data"] = out
        _gold_cache["ts"] = now
        save_json_cache("gold_cache.json", _gold_cache)
        save_json_cache("gold_history.json", _gold_history)
        _gold_status["last_error"] = None
        print(f"[gold] updated: {out}", flush=True)
    except Exception as e:
        # Catches anything OUTSIDE the per-symbol loop — e.g. save_json_cache
        # or something unexpected — so a whole-batch failure is visible too,
        # not just per-symbol ones.
        _gold_status["last_error"] = f"whole-batch failure: {e!r}"
        print(f"[gold] fetch_gold_all whole-batch failure: {e!r}", flush=True)
        raise


@app.route("/api/gold")
def gold():
    if _gold_cache.get("data") is None:
        return jsonify({k: {"ok": False, "error": "not fetched yet"} for k in GOLD_SYMBOLS})
    return jsonify(_gold_cache["data"])


@app.route("/api/debug/gold")
def gold_debug():
    now = time.time()
    history_summary = {}
    for key, entries in _gold_history.items():
        if entries:
            oldest_age_hours = round((now - min(e["ts"] for e in entries)) / 3600, 2)
            history_summary[key] = {
                "entries": len(entries),
                "oldest_entry_age_hours": oldest_age_hours,
                "has_24h_reference": oldest_age_hours >= 24,
            }
        else:
            history_summary[key] = {"entries": 0}
    return jsonify({
        "symbols": GOLD_SYMBOLS,
        "labels": GOLD_LABELS,
        "cache_seconds_old": round(time.time() - _gold_cache["ts"], 1) if _gold_cache.get("data") else None,
        "cached_data": _gold_cache.get("data"),
        "last_attempt_seconds_ago": round(time.time() - _gold_status["last_attempt"], 1) if _gold_status["last_attempt"] else None,
        "last_error": _gold_status["last_error"],
        "history": history_summary,
    })


def _gold_background_loop():
    print("[gold] background thread started", flush=True)
    time.sleep(20)  # staggered — see movers loop comment
    while True:
        try:
            fetch_gold_all()
        except Exception as e:
            print(f"[gold] fetch_gold_all raised: {e!r}", flush=True)
        time.sleep(GOLD_CACHE_SECONDS)


threading.Thread(target=_gold_background_loop, daemon=True).start()


# ---------- Market sentiment (Fear & Greed) ----------
# Previously fetched directly from the browser on every refresh, with no
# way to confirm whether a frozen-looking score was genuinely stale or a
# client-side/CDN caching artifact. Proxying it server-side, the same way
# gold/indices already work, gives us a real cache timestamp and a debug
# endpoint to actually verify freshness.
FEAR_GREED_CACHE_SECONDS = 15 * 60  # matches feargreedchart.com's own stated server-side cache window
_fear_greed_cache = load_json_cache("fear_greed_cache.json")
_fear_greed_status = {"last_attempt": None, "last_error": None}


def fetch_fear_greed():
    _fear_greed_status["last_attempt"] = time.time()
    try:
        r = _http.get("https://feargreedchart.com/api/?action=all", timeout=(5, 15))
        r.raise_for_status()
        data = r.json()
        _fear_greed_cache["data"] = data
        _fear_greed_cache["ts"] = time.time()
        save_json_cache("fear_greed_cache.json", _fear_greed_cache)
        _fear_greed_status["last_error"] = None
        score = (data.get("score") or {}).get("score")
        print(f"[fear_greed] updated: score={score}", flush=True)
    except Exception as e:
        _fear_greed_status["last_error"] = str(e)
        print(f"[fear_greed] fetch failed: {e!r}", flush=True)
        raise


@app.route("/api/fear-greed")
def fear_greed():
    if _fear_greed_cache.get("data") is None:
        return jsonify({"ok": False, "error": "not fetched yet"})
    return jsonify(_fear_greed_cache["data"])


@app.route("/api/debug/fear-greed")
def fear_greed_debug():
    cached = _fear_greed_cache.get("data")
    return jsonify({
        "cache_seconds_old": round(time.time() - _fear_greed_cache["ts"], 1) if cached else None,
        "cached_score": (cached.get("score") or {}).get("score") if cached else None,
        "last_attempt_seconds_ago": round(time.time() - _fear_greed_status["last_attempt"], 1) if _fear_greed_status["last_attempt"] else None,
        "last_error": _fear_greed_status["last_error"],
    })


def _fear_greed_background_loop():
    print("[fear_greed] background thread started", flush=True)
    time.sleep(24)  # staggered — see movers loop comment
    while True:
        try:
            fetch_fear_greed()
        except Exception as e:
            print(f"[fear_greed] fetch_fear_greed raised: {e!r}", flush=True)
        time.sleep(FEAR_GREED_CACHE_SECONDS)


threading.Thread(target=_fear_greed_background_loop, daemon=True).start()


# ---------- Fed calendar: next CPI, next PPI, next FOMC meeting ----------
FED_CACHE_SECONDS = 24 * 60 * 60  # these schedules don't change intraday
_fed_cache = load_json_cache("fed_cache.json")

# FRED release IDs (fixed, don't change): CPI = 10, PPI = 46.
FRED_RELEASE_IDS = {"cpi": 10, "ppi": 46, "jobs": 50}  # Employment Situation = 50
FRED_LAST_VALUE_SERIES = {"cpi_last": "CPILFESL", "ppi_last": "PPICOR", "jobs_last": "PAYEMS"}

# FOMC meeting dates aren't a "data release" FRED tracks, so this is a
# maintained schedule instead — sourced from the Federal Reserve's own
# published calendar (federalreserve.gov/monetarypolicy/fomccalendars.htm).
# The Fed publishes a new year's dates about 6-12 months ahead; this list
# needs a manual update roughly once a year when that happens. 2027 dates
# are the Fed's own "tentative" preview and could shift slightly.
FOMC_MEETINGS = [
    {"start": "2026-09-15", "end": "2026-09-16"},
    {"start": "2026-10-27", "end": "2026-10-28"},
    {"start": "2026-12-08", "end": "2026-12-09"},
    {"start": "2027-01-26", "end": "2027-01-27"},
    {"start": "2027-03-16", "end": "2027-03-17"},
    {"start": "2027-04-27", "end": "2027-04-28"},
    {"start": "2027-06-08", "end": "2027-06-09"},
    {"start": "2027-07-27", "end": "2027-07-28"},
    {"start": "2027-09-14", "end": "2027-09-15"},
    {"start": "2027-10-26", "end": "2027-10-27"},
    {"start": "2027-12-07", "end": "2027-12-08"},
]


def fetch_next_fred_release(release_id):
    today = datetime.now().date().isoformat()
    r = _http.get(
        "https://api.stlouisfed.org/fred/release/dates",
        params={
            "release_id": release_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "realtime_start": today,
            "include_release_dates_with_no_data": "true",  # needed to see
                                                             # future scheduled
                                                             # dates, not just
                                                             # ones with data
                                                             # already attached
            "sort_order": "asc",
            "limit": 1,
        },
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    if "error_message" in payload:
        raise ValueError(payload["error_message"])
    dates = payload.get("release_dates", [])
    if not dates:
        raise ValueError("no upcoming release date found")
    return dates[0]["date"]


def next_fomc_meeting():
    today = datetime.now().date().isoformat()
    upcoming = [m for m in FOMC_MEETINGS if m["end"] >= today]
    return upcoming[0] if upcoming else None


_fed_status = {"last_attempt": None, "last_error": None}


def fetch_fed_calendar():
    _fed_status["last_attempt"] = time.time()
    print("[fed] requesting next CPI/PPI release dates", flush=True)
    try:
        out = {}
        for key, release_id in FRED_RELEASE_IDS.items():
            try:
                out[key] = {"ok": True, "next_date": fetch_next_fred_release(release_id)}
            except Exception as e:
                print(f"[fed] {key} failed: {e!r}", flush=True)
                out[key] = {"ok": False, "error": str(e)}

        meeting = next_fomc_meeting()
        if meeting:
            out["fomc"] = {"ok": True, "start": meeting["start"], "end": meeting["end"]}
        else:
            out["fomc"] = {"ok": False, "error": "no meeting found in the maintained schedule — needs updating"}

        for key, series_id in FRED_LAST_VALUE_SERIES.items():
            try:
                r = fetch_fred_series(series_id)
                if key == "jobs_last":
                    # PAYEMS is already in thousands of persons, so the raw
                    # month-over-month change IS the standard "+150K jobs"
                    # headline figure — no unit conversion needed.
                    value = round(r["change"], 1) if r["change"] is not None else None
                    unit = "K jobs"
                    out[key] = {"ok": True, "value": value, "unit": unit, "date": r["date"]}
                else:
                    value = round(r["percent_change"], 2) if r["percent_change"] is not None else None
                    prior = round(r["prior_percent_change"], 2) if r["prior_percent_change"] is not None else None
                    unit = "%"
                    out[key] = {"ok": True, "value": value, "unit": unit, "date": r["date"], "prior_value": prior}
            except Exception as e:
                print(f"[fed] {key} ({series_id}) failed: {e!r}", flush=True)
                out[key] = {"ok": False, "error": str(e)}

        _fed_cache["data"] = out
        _fed_cache["ts"] = time.time()
        save_json_cache("fed_cache.json", _fed_cache)
        _fed_status["last_error"] = None
        print(f"[fed] updated: {out}", flush=True)
    except Exception as e:
        # Catches anything OUTSIDE the per-key loop, so a whole-batch
        # failure is visible too, not just per-key ones.
        _fed_status["last_error"] = f"whole-batch failure: {e!r}"
        print(f"[fed] fetch_fed_calendar whole-batch failure: {e!r}", flush=True)
        raise


@app.route("/api/fed-calendar")
def fed_calendar():
    if not FRED_API_KEY:
        return jsonify({
            "cpi": {"ok": False, "error": "FRED_API_KEY not set"},
            "ppi": {"ok": False, "error": "FRED_API_KEY not set"},
            "fomc": {"ok": True, **(next_fomc_meeting() or {"start": None, "end": None})},
        })
    if _fed_cache.get("data") is None:
        return jsonify({k: {"ok": False, "error": "not fetched yet"} for k in ("cpi", "ppi", "jobs", "fomc", "cpi_last", "ppi_last", "jobs_last")})
    return jsonify(_fed_cache["data"])


@app.route("/api/debug/fed-calendar")
def fed_calendar_debug():
    return jsonify({
        "fred_key_set": bool(FRED_API_KEY),
        "release_ids": FRED_RELEASE_IDS,
        "last_value_series": FRED_LAST_VALUE_SERIES,
        "cache_seconds_old": round(time.time() - _fed_cache["ts"], 1) if _fed_cache.get("data") else None,
        "cached_data": _fed_cache.get("data"),
        "last_attempt_seconds_ago": round(time.time() - _fed_status["last_attempt"], 1) if _fed_status["last_attempt"] else None,
        "last_error": _fed_status["last_error"],
        "next_fomc_from_schedule": next_fomc_meeting(),
        "fomc_schedule_last_entry": FOMC_MEETINGS[-1] if FOMC_MEETINGS else None,
    })


def _fed_background_loop():
    print("[fed] background thread started", flush=True)
    time.sleep(25)  # staggered — see movers loop comment
    while True:
        if FRED_API_KEY:
            try:
                fetch_fed_calendar()
            except Exception as e:
                print(f"[fed] fetch_fed_calendar raised: {e!r}", flush=True)
        else:
            print("[fed] FRED_API_KEY not set, skipping", flush=True)
        time.sleep(FED_CACHE_SECONDS)


threading.Thread(target=_fed_background_loop, daemon=True).start()


# ---------- Standalone diagnostics — not used by the dashboard itself ----------
@app.route("/api/debug/twelvedata-symbol-search")
def twelvedata_symbol_search_debug():
    """Query Twelve Data's own symbol reference directly (e.g. ?q=Dow Jones)
    to find the exact correct symbol string for something, rather than
    guessing from documentation or search results."""
    query = request.args.get("q", "")
    if not TWELVE_DATA_API_KEY:
        return jsonify({"error": "TWELVE_DATA_API_KEY not set"})
    if not query:
        return jsonify({"error": "pass a ?q=... search term, e.g. ?q=Dow+Jones"})
    r = _http.get(
        "https://api.twelvedata.com/symbol_search",
        params={"symbol": query, "apikey": TWELVE_DATA_API_KEY},
        timeout=(5, 15),
    )
    return jsonify(r.json())


@app.route("/api/debug/twelvedata-quote-test")
def twelvedata_quote_test_debug():
    """Test any candidate symbol format directly against the real /quote
    endpoint (e.g. ?symbol=.IXIC) — symbol_search doesn't always surface
    every format the quote endpoint itself might actually accept."""
    symbol = request.args.get("symbol", "")
    if not TWELVE_DATA_API_KEY:
        return jsonify({"error": "TWELVE_DATA_API_KEY not set"})
    if not symbol:
        return jsonify({"error": "pass a ?symbol=... to test, e.g. ?symbol=.IXIC"})
    r = _http.get(
        "https://api.twelvedata.com/quote",
        params={"symbol": symbol, "apikey": TWELVE_DATA_API_KEY},
        timeout=(5, 15),
    )
    return jsonify(r.json())


@app.route("/api/debug/finnhub-quote-test")
def finnhub_quote_test_debug():
    """Test any candidate symbol format directly against Finnhub's real
    /quote endpoint (e.g. ?symbol=.IXIC) — Finnhub is the primary quote
    provider already used everywhere else, so worth checking directly
    rather than assuming from one earlier ^VIX test."""
    symbol = request.args.get("symbol", "")
    if not symbol:
        return jsonify({"error": "pass a ?symbol=... to test, e.g. ?symbol=.IXIC"})
    r = _http.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": symbol, "token": API_KEY},
        timeout=(5, 15),
    )
    return jsonify(r.json())


# ---------- Real indices (Nasdaq, S&P 500), with ETF fallback ----------
def is_regular_market_hours():
    """US regular session only: 9:30am-4:00pm ET, Mon-Fri. Approximation —
    doesn't account for market holidays or early-close days."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:  # Sat=5, Sun=6
        return False
    t = now_et.time()
    return dt_time(9, 30) <= t < dt_time(16, 0)


def next_market_open_et(now_et=None):
    """Next 9:30am ET on a weekday — if today's open already passed (or is
    currently in progress), rolls to the next weekday. Approximation —
    doesn't account for market holidays."""
    now_et = now_et or datetime.now(ZoneInfo("America/New_York"))
    candidate = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if candidate <= now_et:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def is_crude_oil_hours():
    """Crude oil update window: 15 minutes before NYSE open through 15
    minutes after NYSE close (9:15am-4:15pm ET, Mon-Fri). WTI futures
    actually trade nearly around the clock, but Steve only uses the
    dashboard during/near stock market hours, so there's no value in
    tracking crude oil's overnight session — and staying inside this
    narrower window keeps us comfortably within the 100/month quota (see
    REAL_INDEX_CADENCE_SECONDS below). Approximation — doesn't account for
    market holidays."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:  # Sat=5, Sun=6
        return False
    t = now_et.time()
    return dt_time(9, 15) <= t < dt_time(16, 15)


# Real index values via RapidAPI. Dow and Nasdaq share one listing
# ("live-stock-market"); S&P 500 has its own separate listing
# ("yahoo-finance-real-time1") to itself — confirmed via live tests
# (2026-08-03) that all three return real INDEX data, not ETF proxies.
# See REAL_INDEX_CADENCE_SECONDS below (defined after REAL_INDEX_SOURCES)
# for per-index refresh rates sized to each quota.


def fetch_yahoo127_key_statistics(yahoo_symbol):
    """Crude Oil — via the "yahoo-finance127" listing's key-statistics
    endpoint, on its own dedicated 100/month quota (separate from
    everything else). Values come wrapped as {"raw": x, "fmt": "..."}
    rather than flat numbers, and there's no direct change field — it's
    computed from current price vs previous close ourselves."""
    r = _http.get(
        f"https://yahoo-finance127.p.rapidapi.com/key-statistics/{yahoo_symbol}",
        headers={
            "Content-Type": "application/json",
            "x-rapidapi-host": "yahoo-finance127.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    try:
        price = float(payload["regularMarketPrice"]["raw"])
        prev_close = float(payload["regularMarketPreviousClose"]["raw"])
        change = price - prev_close
        percent_change = (change / prev_close * 100) if prev_close else None
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(f"unexpected response shape: {e!r} — raw: {str(payload)[:200]}")
    return {
        "ok": True,
        "value": round(price, 2),
        "change": round(change, 2),
        "percent_change": round(percent_change, 2) if percent_change is not None else None,
    }


def fetch_yahoo_realtime1_quote(yahoo_symbol):
    """Nasdaq/S&P — via the "yahoo-finance-real-time1" listing's
    stock/get-options endpoint, which includes a full quote object for the
    underlying symbol even though it's nominally an options-chain endpoint."""
    r = _http.get(
        "https://yahoo-finance-real-time1.p.rapidapi.com/stock/get-options",
        params={"symbol": yahoo_symbol, "lang": "en-US", "region": "US"},
        headers={
            "Content-Type": "application/json",
            "x-rapidapi-host": "yahoo-finance-real-time1.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    try:
        quote = payload["optionChain"]["result"][0]["quote"]
        price = float(quote["regularMarketPrice"])
        change = float(quote["regularMarketChange"])
        percent_change = float(quote["regularMarketChangePercent"])
    except (KeyError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"unexpected response shape: {e!r} — raw: {str(payload)[:200]}")
    return {"ok": True, "value": round(price, 2), "change": round(change, 2), "percent_change": round(percent_change, 2)}


def fetch_via_live_stock_market(yahoo_symbol):
    """Dow and Nasdaq share this — the "live-stock-market" listing's chart
    endpoint. This one returns historical OHLC data rather than a simple
    quote, so change is computed from the last two closing prices ourselves."""
    r = _http.get(
        "https://live-stock-market.p.rapidapi.com/v1/index/chart",
        params={"symbol": yahoo_symbol, "interval": "1d", "range": "5d"},
        headers={
            "Content-Type": "application/json",
            "x-rapidapi-host": "live-stock-market.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    try:
        # Response is wrapped in an extra {"status":200,"data":{...}} envelope
        chart_payload = payload.get("data", payload)
        result = chart_payload["chart"]["result"][0]
        price = float(result["meta"]["regularMarketPrice"])
        closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
        if len(closes) < 2:
            raise ValueError("not enough close data points to compute change")
        prev = float(closes[-2])
        change = price - prev
        percent_change = (change / prev * 100) if prev else None
    except (KeyError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"unexpected response shape: {e!r} — raw: {str(payload)[:200]}")
    return {
        "ok": True,
        "value": round(price, 2),
        "change": round(change, 2),
        "percent_change": round(percent_change, 2) if percent_change is not None else None,
    }


def etf_fallback(etf_symbol):
    """Fallback factory — a tracking ETF via Finnhub, same source already
    used elsewhere in the app. Returns a no-arg callable."""
    def fallback():
        r = _http.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": etf_symbol, "token": API_KEY},
            timeout=(5, 15),
        )
        r.raise_for_status()
        q = r.json()
        if q.get("c") is None or q.get("c") == 0:
            raise ValueError(f"{etf_symbol} quote unavailable")
        return {"ok": True, "value": q["c"], "change": q.get("d"), "percent_change": q.get("dp"), "via": f"{etf_symbol} (proxy)"}
    return fallback


def macro_fallback(macro_key):
    """Fallback factory for crude oil/treasury — reads from the existing
    FRED-backed /api/macro cache instead of an ETF proxy, since that's a
    more accurate stand-in (ETFs don't track a bond's yield directly, and
    FRED already gives the genuine underlying value, just less fresh)."""
    def fallback():
        entry = (_macro_cache.get("data") or {}).get(macro_key)
        if not entry or not entry.get("ok") or entry.get("value") is None:
            raise ValueError(f"no FRED-cached {macro_key} value available")
        return {
            "ok": True,
            "value": entry["value"],
            "change": entry.get("change"),
            "percent_change": entry.get("percent_change"),
            "via": "FRED (daily)",
        }
    return fallback


REAL_INDEX_SOURCES = {
    "dow": (lambda: fetch_via_live_stock_market("^DJI"), etf_fallback("DIA")),
    "nasdaq": (lambda: fetch_via_live_stock_market("^IXIC"), etf_fallback("ONEQ")),
    "sp500": (lambda: fetch_yahoo_realtime1_quote("^GSPC"), etf_fallback("SPY")),
    "crude_oil": (lambda: fetch_yahoo127_key_statistics("CL=F"), macro_fallback("crude_oil")),
    "treasury_10y": (lambda: fetch_via_live_stock_market("^TNX"), macro_fallback("treasury_10y")),
}
# Per-index cadence, sized to each quota:
# - Dow+Nasdaq+Treasury share the "live-stock-market" 500/mo pool
#   (43min+43min+91min ~= 470/mo combined)
# - S&P has "yahoo-finance-real-time1" to itself (20min ~= 410/mo)
# - Crude Oil has its own dedicated "yahoo-finance127" quota, just
#   100/mo — window is 9:15am-4:15pm ET (~7hrs/day, ~152hrs/mo);
#   95min ~= 96/mo, just under quota
REAL_INDEX_CADENCE_SECONDS = {
    "dow": 43 * 60,
    "nasdaq": 43 * 60,
    "sp500": 20 * 60,
    "crude_oil": 95 * 60,
    "treasury_10y": 91 * 60,
}
_index_caches_loaded = load_json_cache("indices_cache.json")
_index_caches = {key: _index_caches_loaded.get(key, {"data": None, "ts": 0}) for key in REAL_INDEX_SOURCES}
_index_status = {key: {"last_attempt": None, "last_error": None} for key in REAL_INDEX_SOURCES}
_index_consecutive_failures = {key: 0 for key in REAL_INDEX_SOURCES}

# After a transient real-index failure (e.g. a RapidAPI read timeout), we
# retry sooner than the full per-index cadence rather than leaving the
# ETF-scale fallback value on screen for up to ~90 minutes. Capped at 2
# quick retries so a genuine extended outage doesn't blow through the
# monthly quota — after that we fall back to the normal slower cadence
# until a call succeeds again.
INDEX_RETRY_DELAY_SECONDS = 5 * 60
INDEX_MAX_QUICK_RETRIES = 2


def fetch_index_all(key):
    real_fetch_fn, fallback_fn = REAL_INDEX_SOURCES[key]
    status = _index_status[key]
    status["last_attempt"] = time.time()
    result = None
    if RAPIDAPI_KEY:
        try:
            result = real_fetch_fn()
            result["via"] = "real index"
            status["last_error"] = None
        except Exception as e:
            print(f"[{key}] real index attempt failed, falling back: {e!r}", flush=True)
            status["last_error"] = str(e)
    if result is None:
        try:
            result = fallback_fn()
        except Exception as e:
            print(f"[{key}] fallback also failed: {e!r}", flush=True)
            result = {"ok": False, "error": str(e)}
            if status["last_error"] is None:
                status["last_error"] = str(e)
    _index_caches[key]["data"] = result
    _index_caches[key]["ts"] = time.time()
    save_json_cache("indices_cache.json", _index_caches)
    print(f"[{key}] updated: {result}", flush=True)


@app.route("/api/index/<key>")
def real_index_route(key):
    if key not in REAL_INDEX_SOURCES:
        return jsonify({"ok": False, "error": f"unknown index key '{key}'"}), 404
    if _index_caches[key]["data"] is None:
        return jsonify({"ok": False, "error": "not fetched yet"})
    # Merge in the actual server-side fetch time — separate from page
    # refresh time, which is what the frontend previously (incorrectly)
    # showed in the tooltip on hover.
    payload = dict(_index_caches[key]["data"])
    payload["fetched_at"] = _index_caches[key]["ts"]
    return jsonify(payload)


@app.route("/api/debug/index/<key>")
def real_index_debug(key):
    if key not in REAL_INDEX_SOURCES:
        return jsonify({"ok": False, "error": f"unknown index key '{key}'"}), 404
    cache = _index_caches[key]
    status = _index_status[key]
    return jsonify({
        "rapidapi_key_set": bool(RAPIDAPI_KEY),
        "cache_seconds_old": round(time.time() - cache["ts"], 1) if cache["data"] else None,
        "cached_data": cache["data"],
        "last_attempt_seconds_ago": round(time.time() - status["last_attempt"], 1) if status["last_attempt"] else None,
        "last_error": status["last_error"],
        "consecutive_failures": _index_consecutive_failures[key],
    })


def _make_index_background_loop(key, stagger_seconds):
    def loop():
        print(f"[{key}] background thread started", flush=True)
        time.sleep(stagger_seconds)

        # Pull immediately on boot regardless of market hours, so a
        # redeploy doesn't leave the dashboard sitting blank until the
        # next scheduled check (which, outside the open-window logic
        # below, could otherwise be up to an hour away).
        try:
            fetch_index_all(key)
        except Exception as e:
            print(f"[{key}] initial fetch raised: {e!r}", flush=True)

        while True:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            open_dt = next_market_open_et(now_et)
            minutes_to_open = (open_dt - now_et).total_seconds() / 60
            near_open = -5 <= minutes_to_open <= 5  # 5 min before through 5 min after

            if is_regular_market_hours() or near_open:
                try:
                    fetch_index_all(key)
                except Exception as e:
                    print(f"[{key}] fetch_index_all raised: {e!r}", flush=True)

                failed = _index_status[key]["last_error"] is not None
                if failed:
                    _index_consecutive_failures[key] += 1
                else:
                    _index_consecutive_failures[key] = 0

                if failed and _index_consecutive_failures[key] <= INDEX_MAX_QUICK_RETRIES:
                    # Transient failure — retry again soon instead of
                    # sitting on a wrong-scale fallback value for the
                    # rest of this index's normal cadence window.
                    sleep_s = INDEX_RETRY_DELAY_SECONDS
                else:
                    # Tighter loop right around open to catch it precisely;
                    # normal per-index cadence once solidly into the session.
                    sleep_s = 2 * 60 if near_open and not is_regular_market_hours() else REAL_INDEX_CADENCE_SECONDS[key]
            else:
                print(f"[{key}] outside regular market hours, skipping", flush=True)
                # Sleep until 5 min before the near-open window starts,
                # capped so we don't oversleep past it, but not so short
                # that we're needlessly checking all night/weekend either.
                sleep_s = min(max(minutes_to_open - 5, 1) * 60, 30 * 60)
            time.sleep(sleep_s)
    return loop


def _make_commodity_background_loop(key, stagger_seconds):
    """Same quick-retry logic as _make_index_background_loop, but gated on
    is_crude_oil_hours() (market hours +/- 15min) instead of the raw
    9:30am-4pm ET window, and sleeps efficiently until the next window
    opens rather than polling repeatedly overnight."""
    def loop():
        print(f"[{key}] background thread started", flush=True)
        time.sleep(stagger_seconds)

        try:
            fetch_index_all(key)
        except Exception as e:
            print(f"[{key}] initial fetch raised: {e!r}", flush=True)

        while True:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            if is_crude_oil_hours():
                try:
                    fetch_index_all(key)
                except Exception as e:
                    print(f"[{key}] fetch_index_all raised: {e!r}", flush=True)

                failed = _index_status[key]["last_error"] is not None
                if failed:
                    _index_consecutive_failures[key] += 1
                else:
                    _index_consecutive_failures[key] = 0

                if failed and _index_consecutive_failures[key] <= INDEX_MAX_QUICK_RETRIES:
                    sleep_s = INDEX_RETRY_DELAY_SECONDS
                else:
                    sleep_s = REAL_INDEX_CADENCE_SECONDS[key]
            else:
                print(f"[{key}] outside crude oil update window, skipping", flush=True)
                # Sleep until 15 min before the next market open (i.e. the
                # start of the next window), capped so we don't oversleep
                # past it or needlessly poll all night/weekend.
                next_window_open = next_market_open_et(now_et) - timedelta(minutes=15)
                minutes_to_open = (next_window_open - now_et).total_seconds() / 60
                sleep_s = min(max(minutes_to_open, 1) * 60, 30 * 60)
            time.sleep(sleep_s)
    return loop


threading.Thread(target=_make_index_background_loop("nasdaq", 8), daemon=True).start()
threading.Thread(target=_make_index_background_loop("sp500", 14), daemon=True).start()
threading.Thread(target=_make_index_background_loop("dow", 20), daemon=True).start()
threading.Thread(target=_make_commodity_background_loop("crude_oil", 26), daemon=True).start()
threading.Thread(target=_make_index_background_loop("treasury_10y", 32), daemon=True).start()


# ---------- Temporary: FMP free-tier exploration routes ----------
# One-off test routes to check whether specific FMP endpoints are
# actually accessible on Steve's free-tier key, before building
# anything real on top of them. FMP's free tier has already surprised
# us twice with 402 Payment Required on endpoints their own docs list
# without any obvious "paid only" marker (sp500-constituent,
# batch-quote) — these just report the raw result so we know for sure
# either way. Safe to delete once we've decided whether to use either.
@app.route("/api/debug/test-fmp-market-hours")
def test_fmp_market_hours():
    if not FMP_API_KEY:
        return jsonify({"error": "FMP_API_KEY not set"})
    try:
        r = _http.get(
            "https://financialmodelingprep.com/stable/exchange-market-hours",
            params={"exchange": "NASDAQ", "apikey": FMP_API_KEY},
            timeout=(5, 15),
        )
        return jsonify({
            "status_code": r.status_code,
            "is_html_block_page": r.text.lstrip().startswith("<"),
            "raw_response": r.text[:1000],
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/debug/test-fmp-index-quote")
def test_fmp_index_quote():
    if not FMP_API_KEY:
        return jsonify({"error": "FMP_API_KEY not set"})
    try:
        r = _http.get(
            "https://financialmodelingprep.com/stable/quote",
            params={"symbol": "^GSPC", "apikey": FMP_API_KEY},
            timeout=(5, 15),
        )
        return jsonify({
            "status_code": r.status_code,
            "is_html_block_page": r.text.lstrip().startswith("<"),
            "raw_response": r.text[:1000],
        })
    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Default watchlist: {', '.join(DEFAULT_SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
