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
    export FMP_API_KEY="your-key-here"           (optional, powers the market-movers
        list — free at financialmodelingprep.com, no card required. Without it, that
        section is just empty.)
    export FRED_API_KEY="your-key-here"           (optional, powers real crude oil
        $/barrel and 10Y Treasury yield — free at fredaccount.stlouisfed.org, no
        card required, no paid tiers at all. Without it, that section is empty.)
    python app.py

Then open http://localhost:5000
"""

import collections
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta
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

# Optional — only needed for the market-movers (gainers/losers) list.
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
_quote_cache = {}   # symbol -> {"data": {...}, "ts": float}


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
        # Fetch cache misses concurrently rather than one at a time — with
        # a growing watchlist, sequential fetching was slow enough to trip
        # the host's request timeout and crash the worker mid-request,
        # wiping other in-memory data (including SMA/RSI) along with it.
        results = list(_quote_executor.map(fetch_quote_one, to_fetch))
        now = time.time()
        for sym, data in zip(to_fetch, results):
            _quote_cache[sym] = {"data": data, "ts": now}
            out[sym] = data

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


# ---------- Market movers (top gainers/losers, whole-market — not tied to
# the watchlist) ----------
MOVERS_CACHE_SECONDS = 15 * 60  # FMP free tier: 250 requests/day — keep this gentle
_movers_cache = {"data": None, "ts": 0}


def _fetch_movers_list_raw(endpoint):
    r = _http.get(
        f"https://financialmodelingprep.com/stable/{endpoint}",
        params={"apikey": FMP_API_KEY},
        timeout=(5, 15),
    )
    if r.text.lstrip().startswith("<"):
        # Same failure mode we hit with Stooq earlier — some providers block
        # cloud-hosting IPs by silently returning an HTML block/challenge
        # page instead of a proper error status.
        raise ValueError(f"got HTML instead of JSON (likely blocked) — first 150 chars: {r.text[:150]!r}")
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"unexpected response shape: {str(data)[:150]}")
    return [
        {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "price": item.get("price"),
            "change": item.get("change"),
            "changesPercentage": item.get("changesPercentage"),
        }
        for item in data[:5]
        if item.get("symbol")
    ]


_movers_fetch_executor = ThreadPoolExecutor(max_workers=4)  # more than 1 —
    # result(timeout=...) abandons a stuck call from the caller's side but
    # doesn't kill the underlying thread, so a genuinely-hung request would
    # otherwise permanently occupy the only worker and block every future
    # attempt from ever getting a fresh connection


def fetch_movers_list(endpoint):
    """Wraps the raw fetch with a hard-enforced 25s timeout from the
    OUTSIDE, via a thread that gets abandoned if it doesn't return in
    time — confirmed via /api/debug/movers that a real request can sit
    unfinished for minutes despite requests' own (5,15)s timeout, which
    points to something (likely DNS resolution) that timeout doesn't
    reliably cover on this host."""
    future = _movers_fetch_executor.submit(_fetch_movers_list_raw, endpoint)
    try:
        return future.result(timeout=25)
    except FutureTimeoutError:
        raise TimeoutError(f"{endpoint} hard-timed-out after 25s (request never returned — likely a DNS/network hang)")


_movers_status = {"last_attempt": None, "last_error": None}


def fetch_movers_all():
    _movers_status["last_attempt"] = time.time()
    print("[movers] requesting biggest-gainers and biggest-losers", flush=True)
    try:
        gainers = fetch_movers_list("biggest-gainers")
        losers = fetch_movers_list("biggest-losers")
    except Exception as e:
        _movers_status["last_error"] = str(e)
        print(f"[movers] fetch failed: {e!r}", flush=True)
        raise
    _movers_cache["data"] = {"gainers": gainers, "losers": losers}
    _movers_cache["ts"] = time.time()
    _movers_status["last_error"] = None
    print(f"[movers] updated: {len(gainers)} gainers, {len(losers)} losers", flush=True)


@app.route("/api/movers")
def movers():
    if not FMP_API_KEY:
        return jsonify({"error": "FMP_API_KEY not set", "gainers": [], "losers": []})
    if _movers_cache["data"] is None:
        return jsonify({"error": "not fetched yet", "gainers": [], "losers": []})
    return jsonify(_movers_cache["data"])


@app.route("/api/debug/movers")
def movers_debug():
    return jsonify({
        "fmp_key_set": bool(FMP_API_KEY),
        "cache_seconds_old": round(time.time() - _movers_cache["ts"], 1) if _movers_cache["data"] else None,
        "cached_data": _movers_cache["data"],
        "last_attempt_seconds_ago": round(time.time() - _movers_status["last_attempt"], 1) if _movers_status["last_attempt"] else None,
        "last_error": _movers_status["last_error"],
        "process_id": os.getpid(),
        "process_uptime_seconds": round(time.time() - _process_started_at, 1),
    })


def _movers_background_loop():
    print("[movers] background thread started", flush=True)
    time.sleep(5)  # staggered so 6 background threads don't all burst-fetch
                   # simultaneously at boot, competing for this host's 0.5 CPU
    while True:
        if FMP_API_KEY:
            try:
                fetch_movers_all()
            except Exception as e:
                print(f"[movers] fetch_movers_all raised: {e!r}", flush=True)
        time.sleep(MOVERS_CACHE_SECONDS)


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


# ---------- Nasdaq Composite: real index attempt, with ONEQ fallback ----------
NASDAQ_CACHE_SECONDS = 5 * 60  # conservative — this vendor's rate limits are unknown
_nasdaq_cache = {"data": None, "ts": 0}
_nasdaq_status = {"last_attempt": None, "last_error": None}


def fetch_nasdaq_real_index():
    """Real Nasdaq Composite via a RapidAPI Yahoo Finance wrapper — the only
    endpoint format confirmed from their docs is "STOCK History" (POST),
    so this pulls a short history and treats the most recent two points as
    the current value and prior close. Response shape isn't documented
    anywhere accessible, so this checks a few plausible variants."""
    r = _http.post(
        "https://yahoo-finance160.p.rapidapi.com/history",
        headers={
            "Content-Type": "application/json",
            "x-rapidapi-host": "yahoo-finance160.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        json={"stock": "^IXIC", "period": "5d"},
        timeout=(5, 15),
    )
    r.raise_for_status()
    payload = r.json()
    rows = payload if isinstance(payload, list) else (
        payload.get("data") or payload.get("history") or payload.get("result")
    )
    if not isinstance(rows, list) or len(rows) < 2:
        raise ValueError(f"unexpected or insufficient response shape: {str(payload)[:200]}")

    def get_close(row):
        for key in ("close", "Close", "adjClose", "Adj Close"):
            if row.get(key) is not None:
                return float(row[key])
        raise ValueError(f"no close field in row: {str(row)[:100]}")

    latest = get_close(rows[-1])
    prev = get_close(rows[-2])
    return {
        "ok": True,
        "value": round(latest, 2),
        "change": round(latest - prev, 2),
        "percent_change": round((latest - prev) / prev * 100, 2) if prev else None,
    }


def fetch_nasdaq_via_oneq():
    """Fallback — the ONEQ ETF via Finnhub, same source already used
    elsewhere in the app."""
    r = _http.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": "ONEQ", "token": API_KEY},
        timeout=(5, 15),
    )
    r.raise_for_status()
    q = r.json()
    if q.get("c") is None or q.get("c") == 0:
        raise ValueError("ONEQ quote unavailable")
    return {"ok": True, "value": q["c"], "change": q.get("d"), "percent_change": q.get("dp"), "via": "ONEQ (proxy)"}


def fetch_nasdaq_all():
    _nasdaq_status["last_attempt"] = time.time()
    result = None
    if RAPIDAPI_KEY:
        try:
            result = fetch_nasdaq_real_index()
            result["via"] = "real index"
            _nasdaq_status["last_error"] = None
        except Exception as e:
            print(f"[nasdaq] real index attempt failed, falling back to ONEQ: {e!r}", flush=True)
            _nasdaq_status["last_error"] = str(e)
    if result is None:
        try:
            result = fetch_nasdaq_via_oneq()
        except Exception as e:
            print(f"[nasdaq] ONEQ fallback also failed: {e!r}", flush=True)
            result = {"ok": False, "error": str(e)}
            if _nasdaq_status["last_error"] is None:
                _nasdaq_status["last_error"] = str(e)
    _nasdaq_cache["data"] = result
    _nasdaq_cache["ts"] = time.time()
    print(f"[nasdaq] updated: {result}", flush=True)


@app.route("/api/nasdaq")
def nasdaq():
    if _nasdaq_cache["data"] is None:
        return jsonify({"ok": False, "error": "not fetched yet"})
    return jsonify(_nasdaq_cache["data"])


@app.route("/api/debug/nasdaq")
def nasdaq_debug():
    return jsonify({
        "rapidapi_key_set": bool(RAPIDAPI_KEY),
        "cache_seconds_old": round(time.time() - _nasdaq_cache["ts"], 1) if _nasdaq_cache["data"] else None,
        "cached_data": _nasdaq_cache["data"],
        "last_attempt_seconds_ago": round(time.time() - _nasdaq_status["last_attempt"], 1) if _nasdaq_status["last_attempt"] else None,
        "last_error": _nasdaq_status["last_error"],
    })


def _nasdaq_background_loop():
    print("[nasdaq] background thread started", flush=True)
    time.sleep(8)  # staggered
    while True:
        try:
            fetch_nasdaq_all()
        except Exception as e:
            print(f"[nasdaq] fetch_nasdaq_all raised: {e!r}", flush=True)
        time.sleep(NASDAQ_CACHE_SECONDS)


threading.Thread(target=_nasdaq_background_loop, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Default watchlist: {', '.join(DEFAULT_SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
