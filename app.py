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
# Default urllib3 pool caps at 10 connections per host. With quotes (up
# to 4 workers), PE (2), and earnings (2) all sharing this one session
# and all hitting finnhub.io around the same refresh cycle, that's a
# real bottleneck — raised well above worst-case concurrent demand
# across every feature combined.
_http_adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
_http.mount("https://", _http_adapter)
_http.mount("http://", _http_adapter)

CACHE_SECONDS = 60  # per-symbol, shared across all visitors requesting that symbol
FAILED_QUOTE_CACHE_SECONDS = 10  # a FAILED quote is cached much more briefly
    # than a successful one — otherwise a single bad fetch gets echoed back
    # for the full 60s cache window, and since the frontend also refreshes
    # on a ~60s cycle, a symbol that fails once could get "stuck" showing
    # that same failure every cycle before ever getting a fair retry.


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
            threading.Thread(target=fetch_pe_immediate, args=(sym,), daemon=True).start()
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


# ---------- PWA (installable "Market Pulse" app on Android) ----------
@app.route("/manifest.json")
def pwa_manifest():
    return send_from_directory(".", "manifest.json", mimetype="application/manifest+json")


@app.route("/service-worker.js")
def pwa_service_worker():
    # Root-scoped (not under /static/) so its default scope covers the
    # whole site — a service worker can only control paths at or below
    # wherever it's served from.
    return send_from_directory(".", "service-worker.js", mimetype="application/javascript")


@app.route("/icon-192.png")
def pwa_icon_192():
    return send_from_directory(".", "icon-192.png", mimetype="image/png")


@app.route("/icon-512.png")
def pwa_icon_512():
    return send_from_directory(".", "icon-512.png", mimetype="image/png")


@app.route("/icon-192-maskable.png")
def pwa_icon_192_maskable():
    return send_from_directory(".", "icon-192-maskable.png", mimetype="image/png")


@app.route("/icon-512-maskable.png")
def pwa_icon_512_maskable():
    return send_from_directory(".", "icon-512-maskable.png", mimetype="image/png")


# One shared limiter for every Finnhub call, across all call sites
# (watchlist quotes, symbol search, earnings, P/E, ETF fallbacks for
# indices). These used to fire completely independently, each with its
# own worker pool and no coordination — individually none looked
# excessive, but combined (especially with a 21-symbol watchlist now
# hit by three separate executors around the same refresh cycle) they
# could burst past Finnhub's real free-tier limit (60/min), which
# likely explains widespread "HTTP 503"/"fetch failed" errors across
# many symbols at once rather than isolated blips. Same sliding-window
# approach as acquire_twelvedata_credits, just for a different provider.
_finnhub_credit_lock = threading.Lock()
_finnhub_credit_timestamps = collections.deque()
FINNHUB_RATE_LIMIT = 55  # free tier is 60/min; small safety margin


def acquire_finnhub_credit():
    waited = 0
    while True:
        with _finnhub_credit_lock:
            now = time.time()
            while _finnhub_credit_timestamps and now - _finnhub_credit_timestamps[0] > 60:
                _finnhub_credit_timestamps.popleft()
            if len(_finnhub_credit_timestamps) < FINNHUB_RATE_LIMIT:
                _finnhub_credit_timestamps.append(now)
                return
            in_use = len(_finnhub_credit_timestamps)
        if waited and waited % 10 == 0:
            print(f"[finnhub] still waiting for a credit after {waited}s ({in_use}/{FINNHUB_RATE_LIMIT} in use)", flush=True)
        time.sleep(1)
        waited += 1


def fetch_quote_one(sym, _retry=True):
    acquire_finnhub_credit()
    try:
        r = _http.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": sym, "token": API_KEY},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("c") is None:
            raise ValueError("no data")
        return {"ok": True, **data}
    except requests.exceptions.HTTPError:
        # Status code + a snippet of Finnhub's own response body — their
        # error responses are typically just {"error": "..."} and don't
        # echo back the request/API key, so this is safe to surface.
        # Persistent identical failures across many minutes (not
        # rotating between symbols) survived both a retry-with-backoff
        # AND a shared cross-feature rate limiter, which rules out
        # simple burst/pacing issues — this body text is what actually
        # tells us whether it's a real account-level cap.
        status = r.status_code
        body_snippet = r.text[:200]
        if _retry and status in (429, 500, 502, 503, 504):
            time.sleep(2)
            return fetch_quote_one(sym, _retry=False)
        return {"ok": False, "error": f"HTTP {status}: {body_snippet}"}
    except Exception:
        if _retry:
            time.sleep(2)
            return fetch_quote_one(sym, _retry=False)
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
    if not entry:
        return None
    age = time.time() - entry["ts"]
    ttl = CACHE_SECONDS if entry["data"].get("ok") else FAILED_QUOTE_CACHE_SECONDS
    if age < ttl:
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
            #
            # Dispatch is staggered rather than firing all workers at the
            # same instant — a single isolated Finnhub request succeeds
            # reliably (confirmed via /api/debug/finnhub-quote-test) while
            # the same symbols fail specifically as part of this batch,
            # which survived both a retry-with-backoff and a 55/min global
            # rate limiter. That points to Finnhub enforcing a stricter
            # per-second/concurrent-connection limit separate from its
            # per-minute one — 4 truly simultaneous requests can trip that
            # even when the per-minute average looks completely reasonable.
            futures = []
            for i, sym in enumerate(to_fetch):
                if i > 0:
                    time.sleep(0.3)
                futures.append(_quote_executor.submit(fetch_quote_one, sym))
            results = [f.result() for f in futures]
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
        acquire_finnhub_credit()
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


_twelvedata_executor = ThreadPoolExecutor(max_workers=2)


def twelvedata_get(url, params, timeout=(5, 15)):
    """GET with one automatic retry on a 429. Our credit tracker only
    knows about calls made by *this* process — during a Render deploy
    transition, the outgoing and incoming workers can briefly run at the
    same time, each thinking it has the full budget, and together exceed
    the real account-wide limit. That's a real, if narrow, gap in a
    purely in-memory rate limiter; retrying once after a short wait
    covers it without needing a shared external store.

    Wrapped in a hard-enforced 45s timeout from the OUTSIDE — this host
    has shown before (FMP/movers) that requests' own connect/read
    timeouts aren't reliably enforced, almost certainly a DNS resolution
    hang that those timeout parameters don't cover. Without this, a
    single hung request freezes the entire sequential SMA batch loop
    forever — confirmed via a real incident where a batch sat stuck on
    "acquiring credits" for 9+ hours, actually hung downstream in this
    exact call, silently blocking every symbol queued after it."""
    def _do_request():
        r = _http.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            print(f"[twelvedata] 429 rate limited, waiting 20s and retrying once: {url}", flush=True)
            time.sleep(20)
            r = _http.get(url, params=params, timeout=timeout)
        return r

    future = _twelvedata_executor.submit(_do_request)
    try:
        return future.result(timeout=45)  # generous — covers the possible 20s 429-retry pause
    except FutureTimeoutError:
        raise TimeoutError(f"Twelve Data request hard-timed-out after 45s (likely a DNS/network hang): {url}")


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
    time.sleep(10)  # staggered so background threads don't all burst-fetch simultaneously at boot, competing for this host's 0.5 CPU
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
_earnings_executor = ThreadPoolExecutor(max_workers=2)


def fetch_earnings_one(sym, _retry=True):
    """Earnings info for one symbol, via Finnhub's earnings calendar
    (already using this key elsewhere — no new signup needed). Prefers a
    result reported within the last 14 days (real numbers, beat/miss vs
    estimates) over a future scheduled date, since a just-reported quarter
    is more useful to see than "next earnings in 3 months".

    Retries once after a brief pause on any failure — repeated observation
    showed 100% of symbols failing simultaneously with the same connection
    timeout, which points to general host contention on this 0.5 CPU
    instance rather than a one-off fluke, so a single retry gives a
    transient slow patch a second chance rather than poisoning the cache
    for a full day."""
    today = datetime.now().date()
    try:
        acquire_finnhub_credit()
        r = _http.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={
                "symbol": sym,
                "from": (today - timedelta(days=14)).isoformat(),
                "to": (today + timedelta(days=180)).isoformat(),
                "token": API_KEY,
            },
            timeout=15,
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
        if _retry:
            time.sleep(3)
            return fetch_earnings_one(sym, _retry=False)
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
    ok_count = 0
    for sym, data in zip(symbols, results):
        _earnings_cache[sym] = {"data": data, "ts": now}
        if data.get("ok"):
            ok_count += 1
    save_json_cache(EARNINGS_CACHE_FILE, _earnings_cache)
    print(f"[earnings] pass complete ({ok_count}/{len(symbols)} ok)", flush=True)
    return ok_count, len(symbols)


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


# After a pass where every symbol failed (all connection timeouts,
# suggesting transient host contention rather than real "no data"),
# retry sooner than the full 24h cadence rather than leaving the cache
# poisoned with errors for the rest of the day. Capped so a genuine
# extended Finnhub outage doesn't retry indefinitely.
EARNINGS_RETRY_DELAY_SECONDS = 10 * 60
EARNINGS_MAX_QUICK_RETRIES = 2


def _earnings_background_loop():
    print("[earnings] background thread started", flush=True)
    time.sleep(15)  # staggered so background threads don't all burst-fetch
                     # simultaneously at boot, competing for this host's 0.5 CPU
    consecutive_failures = 0
    while True:
        try:
            ok_count, total = fetch_earnings_all(active_known_symbols())
            if total > 0 and ok_count == 0:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
        except Exception as e:
            print(f"[earnings] fetch_earnings_all raised: {e!r}", flush=True)
            consecutive_failures += 1

        if 0 < consecutive_failures <= EARNINGS_MAX_QUICK_RETRIES:
            print(f"[earnings] pass came back empty, retrying sooner "
                  f"({consecutive_failures}/{EARNINGS_MAX_QUICK_RETRIES})", flush=True)
            sleep_s = EARNINGS_RETRY_DELAY_SECONDS
        else:
            sleep_s = EARNINGS_CACHE_SECONDS
        time.sleep(sleep_s)


threading.Thread(target=_earnings_background_loop, daemon=True).start()


# ---------- P/E ratio ----------
# Company fundamentals — a different, heavier Finnhub endpoint than the
# quote endpoint, so it's cached like earnings (once daily, P/E doesn't
# move fast enough to need live updates) rather than every minute.
PE_CACHE_SECONDS = 24 * 60 * 60
PE_CACHE_FILE = "pe_cache.json"
_pe_cache = load_json_cache(PE_CACHE_FILE)  # symbol -> {"data": {...}, "ts": float}
_pe_executor = ThreadPoolExecutor(max_workers=2)  # same conservative
    # concurrency as earnings — this host has shown it doesn't tolerate
    # bursts well.

# Same retry-sooner-on-failure pattern as earnings.
PE_RETRY_DELAY_SECONDS = 10 * 60
PE_MAX_QUICK_RETRIES = 2

# Rolling history so we can compute week-over-week P/E change (used to
# color-code valuation trend rather than absolute level) — same pattern
# already used for gold's 24h change. Since PE is only fetched once a
# day, one entry accumulates per day; kept a few days past a week for
# buffer so there's always a data point at-or-before the 7-day mark.
PE_HISTORY_FILE = "pe_history.json"
PE_HISTORY_MAX_AGE = 10 * 24 * 60 * 60
_pe_history = load_json_cache(PE_HISTORY_FILE)  # symbol -> [{"pe": x, "ts": ...}, ...]


def compute_pe_week_change(sym, current_pe, now):
    """% change from the P/E reading closest to (but not after) 7 days
    ago, to today's. Returns None if we don't have anything old enough
    yet (e.g. the first week after this feature was added)."""
    history = _pe_history.get(sym, [])
    target = now - 7 * 24 * 60 * 60
    candidates = [h for h in history if h["ts"] <= target]
    if not candidates:
        return None
    closest = max(candidates, key=lambda h: h["ts"])
    old_pe = closest.get("pe")
    if not old_pe:
        return None
    return round((current_pe - old_pe) / old_pe * 100, 1)


def record_pe_history(sym, pe_value, now):
    hist = _pe_history.setdefault(sym, [])
    hist.append({"pe": pe_value, "ts": now})
    _pe_history[sym] = [h for h in hist if now - h["ts"] <= PE_HISTORY_MAX_AGE]


def fetch_pe_one(sym, _retry=True):
    """Trailing-twelve-month P/E via Finnhub's company-fundamentals
    endpoint. A single retry after a brief pause, same reasoning as
    fetch_earnings_one — this host has shown bursts can transiently
    fail together."""
    try:
        acquire_finnhub_credit()
        r = _http.get(
            "https://finnhub.io/api/v1/stock/metric",
            params={"symbol": sym, "metric": "all", "token": API_KEY},
            timeout=15,
        )
        r.raise_for_status()
        metric = r.json().get("metric", {})
        pe = metric.get("peTTM")
        if pe is None:
            pe = metric.get("peBasicExclExtraTTM")
        if pe is None:
            return {"ok": True, "pe": None}  # valid response, just no P/E (e.g. unprofitable company)
        return {"ok": True, "pe": round(float(pe), 1)}
    except requests.exceptions.HTTPError:
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        if _retry:
            time.sleep(3)
            return fetch_pe_one(sym, _retry=False)
        return {"ok": False, "error": str(e)}


def fetch_pe_immediate(sym):
    """One-off fetch for a brand-new watchlist symbol, so it doesn't
    wait for the next scheduled daily pass."""
    print(f"[pe] immediate fetch for new symbol {sym}", flush=True)
    now = time.time()
    data = fetch_pe_one(sym)
    if data.get("ok") and data.get("pe") is not None:
        data["week_change_pct"] = compute_pe_week_change(sym, data["pe"], now)
        record_pe_history(sym, data["pe"], now)
        save_json_cache(PE_HISTORY_FILE, _pe_history)
    _pe_cache[sym] = {"data": data, "ts": now}
    save_json_cache(PE_CACHE_FILE, _pe_cache)


def fetch_pe_all(symbols):
    print(f"[pe] requesting {len(symbols)} symbols", flush=True)
    results = list(_pe_executor.map(fetch_pe_one, symbols))
    now = time.time()
    ok_count = 0
    for sym, data in zip(symbols, results):
        if data.get("ok") and data.get("pe") is not None:
            data["week_change_pct"] = compute_pe_week_change(sym, data["pe"], now)
            record_pe_history(sym, data["pe"], now)
        _pe_cache[sym] = {"data": data, "ts": now}
        if data.get("ok"):
            ok_count += 1
    save_json_cache(PE_CACHE_FILE, _pe_cache)
    save_json_cache(PE_HISTORY_FILE, _pe_history)
    print(f"[pe] pass complete ({ok_count}/{len(symbols)} ok)", flush=True)
    return ok_count, len(symbols)


@app.route("/api/pe")
def pe():
    symbols = parse_symbols_param()
    register_known_symbols(symbols)
    out = {}
    for sym in symbols:
        entry = _pe_cache.get(sym)
        out[sym] = entry["data"] if entry else {"ok": False, "error": "not fetched yet"}
    return jsonify(out)


@app.route("/api/debug/pe")
def pe_debug():
    ages = {sym: round(time.time() - e["ts"], 1) for sym, e in _pe_cache.items()}
    history_summary = {sym: len(entries) for sym, entries in _pe_history.items()}
    return jsonify({
        "finnhub_key_set": bool(API_KEY),
        "known_symbols_active": active_known_symbols(),
        "cache_seconds_old": ages,
        "cached_data": {sym: e["data"] for sym, e in _pe_cache.items()},
        "history_entry_counts": history_summary,
    })


def _pe_background_loop():
    print("[pe] background thread started", flush=True)
    time.sleep(20)  # staggered so background threads don't all burst-fetch
                     # simultaneously at boot, competing for this host's 0.5 CPU
    consecutive_failures = 0
    while True:
        try:
            ok_count, total = fetch_pe_all(active_known_symbols())
            if total > 0 and ok_count == 0:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
        except Exception as e:
            print(f"[pe] fetch_pe_all raised: {e!r}", flush=True)
            consecutive_failures += 1

        if 0 < consecutive_failures <= PE_MAX_QUICK_RETRIES:
            print(f"[pe] pass came back empty, retrying sooner "
                  f"({consecutive_failures}/{PE_MAX_QUICK_RETRIES})", flush=True)
            sleep_s = PE_RETRY_DELAY_SECONDS
        else:
            sleep_s = PE_CACHE_SECONDS
        time.sleep(sleep_s)


threading.Thread(target=_pe_background_loop, daemon=True).start()


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
    time.sleep(20)  # staggered so background threads don't all burst-fetch simultaneously at boot, competing for this host's 0.5 CPU
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
    # Merge in the actual server-side fetch time, same reasoning as the
    # index tooltip fix — the frontend previously had no way to show
    # when this was genuinely last refreshed.
    payload = dict(_fear_greed_cache["data"])
    payload["fetched_at"] = _fear_greed_cache["ts"]
    return jsonify(payload)


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
    time.sleep(24)  # staggered so background threads don't all burst-fetch simultaneously at boot, competing for this host's 0.5 CPU
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
# Current Fed funds target range — used as the baseline to classify
# Kalshi's KXFED outcomes as hold/hike/cut, and to show "current: X-Y%"
# alongside Market Odds. Updates same-day whenever the Fed actually
# changes rates (unlike the FOMC meeting schedule, no manual upkeep needed).
FRED_RATE_SERIES = {"fed_funds_upper": "DFEDTARU", "fed_funds_lower": "DFEDTARL"}

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

        for key, series_id in FRED_RATE_SERIES.items():
            try:
                r = fetch_fred_series(series_id)
                out[key] = {"ok": True, "value": r["value"], "date": r["date"]}
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
        return jsonify({k: {"ok": False, "error": "not fetched yet"} for k in ("cpi", "ppi", "jobs", "fomc", "cpi_last", "ppi_last", "jobs_last", "fed_funds_upper", "fed_funds_lower")})
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
    time.sleep(25)  # staggered so background threads don't all burst-fetch simultaneously at boot, competing for this host's 0.5 CPU
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


# ---------- Fed rate decision probabilities (Kalshi prediction market) ----------
# CME's real FedWatch API is a paid enterprise product (monthly
# subscription, no public pricing) — not worth it for a personal
# dashboard. This uses Kalshi instead: a CFTC-regulated, real-money
# prediction market with a "KXFED" series specifically for Fed rate
# decisions. Confirmed via live testing (2026-08-05) that Kalshi's
# market-data read endpoints work with NO authentication required —
# only placing trades needs an API key/signed requests, not reading
# prices. A 2026 study (NBER working paper) found Kalshi's Fed-rate
# predictions were at least as accurate as CME FedWatch and professional
# forecasters, so this isn't just a free substitute — it's a
# well-regarded one in its own right.
#
# This first pass just gets the raw data flowing into a debug endpoint.
# Kalshi's exact grouping (event_ticker per FOMC meeting, one market per
# possible outcome like "hold" / "hike 25bp" / "cut 25bp") needs to be
# confirmed against real output before building the actual display —
# same approach already used successfully for FMP's index/market-hours
# endpoints earlier in this project.
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_FED_SERIES = "KXFED"
FED_PROB_CACHE_SECONDS = 30 * 60  # prediction market prices don't need to be second-by-second fresh for this use
FED_PROB_CACHE_FILE = "fed_prob_cache.json"
_fed_prob_cache = load_json_cache(FED_PROB_CACHE_FILE) or {"data": None, "ts": 0}
_fed_prob_status = {"last_attempt": None, "last_error": None}


def fetch_fed_probabilities():
    _fed_prob_status["last_attempt"] = time.time()
    try:
        r = _http.get(
            f"{KALSHI_BASE_URL}/markets",
            params={"series_ticker": KALSHI_FED_SERIES, "status": "open", "limit": 100},
            timeout=(5, 15),
        )
        r.raise_for_status()
        data = r.json()
        markets = data.get("markets", [])

        # Group by event_ticker — expected to be one group per FOMC
        # meeting, containing several yes/no contracts for different
        # rate outcomes at that meeting.
        events = {}
        for m in markets:
            event_ticker = m.get("event_ticker")
            if not event_ticker:
                continue
            events.setdefault(event_ticker, []).append({
                "ticker": m.get("ticker"),
                "title": m.get("title"),
                "yes_sub_title": m.get("yes_sub_title"),
                "yes_bid_dollars": m.get("yes_bid_dollars"),
                "yes_ask_dollars": m.get("yes_ask_dollars"),
                "volume_24h": m.get("volume_24h_fp"),
                "close_time": m.get("close_time"),
            })

        _fed_prob_cache["data"] = {"events": events, "raw_market_count": len(markets)}
        _fed_prob_cache["ts"] = time.time()
        save_json_cache(FED_PROB_CACHE_FILE, _fed_prob_cache)
        _fed_prob_status["last_error"] = None
        print(f"[fed_prob] updated: {len(events)} event(s), {len(markets)} total market(s)", flush=True)
    except Exception as e:
        _fed_prob_status["last_error"] = str(e)
        print(f"[fed_prob] fetch failed: {e!r}", flush=True)
        raise


def compute_fed_meeting_outcomes(events_dict):
    """Kalshi's KXFED series doesn't expose discrete hike/hold/cut
    contracts directly — it's a ladder of "will the upper bound be above
    X%" cumulative threshold markets (e.g. Above 3.75%, Above 3.50%,
    Above 3.25%, ...). This converts that ladder into discrete bucket
    probabilities per meeting (e.g. "ends at 3.50-3.75%: 50%") by taking
    the difference between adjacent thresholds' implied probabilities —
    the standard way to derive a distribution from a cumulative
    strike ladder. Uses the bid/ask midpoint rather than just the bid,
    since the bid alone is biased low by the spread.

    Each bucket includes numeric lower/upper bounds (not just the display
    string) so callers can classify buckets as hold/hike/cut against the
    current Fed funds rate (fetched from FRED, see FRED_RATE_SERIES)
    without re-parsing formatted range text."""
    meetings = []
    for event_ticker, markets in events_dict.items():
        parsed = []
        for m in markets:
            ticker = m.get("ticker", "") or ""
            match = re.search(r"-T([\d.]+)$", ticker)
            if not match:
                continue
            try:
                threshold = float(match.group(1))
            except ValueError:
                continue

            bid = m.get("yes_bid_dollars")
            ask = m.get("yes_ask_dollars")
            try:
                bid_f = float(bid) if bid is not None else None
            except (TypeError, ValueError):
                bid_f = None
            try:
                ask_f = float(ask) if ask is not None else None
            except (TypeError, ValueError):
                ask_f = None

            if bid_f is not None and ask_f is not None:
                prob_above = (bid_f + ask_f) / 2 * 100
            elif bid_f is not None:
                prob_above = bid_f * 100
            else:
                continue

            parsed.append({"threshold": threshold, "prob_above": prob_above, "close_time": m.get("close_time")})

        if not parsed:
            continue

        parsed.sort(key=lambda x: x["threshold"])  # ascending
        close_time = parsed[0]["close_time"]
        meeting_date = close_time[:10] if close_time else None

        buckets = []
        n = len(parsed)
        for i, entry in enumerate(parsed):
            if i == n - 1:
                # Highest threshold's own probability IS the "ends above this" tail bucket.
                bucket_prob = entry["prob_above"]
                range_label = f"above {entry['threshold']:.2f}%"
                lower, upper = entry["threshold"], None
            else:
                nxt = parsed[i + 1]  # next-higher threshold
                # P(ends in (this, next]) = P(above this) - P(above next).
                # max(0, ...) guards against tiny negative noise from the
                # bid/ask spread — probabilities should be non-decreasing
                # as the threshold falls, but real market quotes aren't
                # perfectly monotonic tick to tick.
                bucket_prob = max(0.0, entry["prob_above"] - nxt["prob_above"])
                range_label = f"{entry['threshold']:.2f}\u2013{nxt['threshold']:.2f}%"
                lower, upper = entry["threshold"], nxt["threshold"]
            if bucket_prob >= 0.5:  # drop negligible/noise-level buckets
                buckets.append({"range": range_label, "probability": round(bucket_prob, 1), "lower": lower, "upper": upper})

        buckets.sort(key=lambda b: b["probability"], reverse=True)
        meetings.append({
            "event_ticker": event_ticker,
            "date": meeting_date,
            "outcomes": buckets[:5],
        })

    meetings.sort(key=lambda m: m["date"] or "9999-99-99")
    return meetings


@app.route("/api/fed-probabilities")
def fed_probabilities():
    if _fed_prob_cache["data"] is None:
        return jsonify({"ok": False, "error": "not fetched yet"})
    try:
        meetings = compute_fed_meeting_outcomes(_fed_prob_cache["data"].get("events", {}))
    except Exception as e:
        return jsonify({"ok": False, "error": f"computation failed: {e}"})

    # Current Fed funds range, from FRED (fetched alongside CPI/PPI/jobs
    # in fetch_fed_calendar) — used to classify each outcome bucket as
    # hold/hike/cut, and to show "current: X-Y%" next to Market Odds.
    fed_data = _fed_cache.get("data") or {}
    upper_info = fed_data.get("fed_funds_upper") or {}
    lower_info = fed_data.get("fed_funds_lower") or {}
    current_upper = upper_info.get("value") if upper_info.get("ok") else None
    current_lower = lower_info.get("value") if lower_info.get("ok") else None

    if current_upper is not None and current_lower is not None:
        for meeting in meetings:
            for outcome in meeting["outcomes"]:
                lo, hi = outcome["lower"], outcome["upper"]
                if lo == current_lower and hi == current_upper:
                    outcome["direction"] = "hold"
                elif lo >= current_upper:
                    outcome["direction"] = "hike"
                elif hi is not None and hi <= current_lower:
                    outcome["direction"] = "cut"
                else:
                    outcome["direction"] = "other"  # spans the current range unevenly — shouldn't normally happen with 25bp-aligned buckets

    return jsonify({
        "ok": True,
        "meetings": meetings,
        "fetched_at": _fed_prob_cache["ts"],
        "current_rate": {"lower": current_lower, "upper": current_upper} if current_upper is not None else None,
    })


@app.route("/api/debug/fed-probabilities")
def fed_prob_debug():
    computed = None
    computed_error = None
    if _fed_prob_cache["data"]:
        try:
            computed = compute_fed_meeting_outcomes(_fed_prob_cache["data"].get("events", {}))
        except Exception as e:
            computed_error = str(e)
    return jsonify({
        "cache_seconds_old": round(time.time() - _fed_prob_cache["ts"], 1) if _fed_prob_cache["data"] else None,
        # Computed outcomes shown FIRST and directly from the same raw
        # data below — no need to hand-transcribe the ladder to check
        # whether they actually match what's being served to the dashboard.
        "computed_from_this_raw_data": computed,
        "computed_error": computed_error,
        "cached_data": _fed_prob_cache["data"],
        "last_attempt_seconds_ago": round(time.time() - _fed_prob_status["last_attempt"], 1) if _fed_prob_status["last_attempt"] else None,
        "last_error": _fed_prob_status["last_error"],
    })


def _fed_prob_background_loop():
    print("[fed_prob] background thread started", flush=True)
    time.sleep(30)  # staggered so background threads don't all burst-fetch simultaneously at boot, competing for this host's 0.5 CPU
    while True:
        try:
            fetch_fed_probabilities()
        except Exception as e:
            print(f"[fed_prob] fetch_fed_probabilities raised: {e!r}", flush=True)
        time.sleep(FED_PROB_CACHE_SECONDS)


threading.Thread(target=_fed_prob_background_loop, daemon=True).start()


# ---------- Standalone diagnostics — not used by the dashboard itself ----------
@app.route("/api/debug/test-yahoo-chart")
def test_yahoo_chart_debug():
    """Tests Yahoo Finance's unofficial (undocumented, unauthenticated)
    chart endpoint directly — the same one the yfinance Python library
    wraps internally — without pulling in yfinance's heavy pandas/numpy
    dependencies just to check one number. Main question: does Render's
    cloud-hosting IP get blocked the way Stooq blocked us earlier, or
    does this endpoint let it through?"""
    results = {}
    for sym in ["^VIX", "^VXN"]:
        try:
            r = _http.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"interval": "1d", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
                timeout=(5, 15),
            )
            results[sym] = {
                "status_code": r.status_code,
                "is_html_block_page": r.text.lstrip().startswith("<"),
                "body": r.text[:800],
            }
        except Exception as e:
            results[sym] = {"error": str(e)}
    return jsonify(results)


@app.route("/api/debug/test-alt-vix-vxn")
def test_alt_vix_vxn_debug():
    """Checks whether Twelve Data, Finnhub, or Stooq can serve VIX/VXN as
    a free alternative to FMP, which paywalls ^VXN (and possibly ^VIX —
    same free-tier restriction that already hit ^TNX). Tries a few
    plausible symbol formats per provider since conventions differ."""
    results = {}

    if TWELVE_DATA_API_KEY:
        for sym in ["VIX", "VXN"]:
            try:
                r = _http.get(
                    "https://api.twelvedata.com/quote",
                    params={"symbol": sym, "apikey": TWELVE_DATA_API_KEY},
                    timeout=(5, 15),
                )
                results[f"twelvedata_{sym}"] = {"status_code": r.status_code, "body": r.text[:500]}
            except Exception as e:
                results[f"twelvedata_{sym}"] = {"error": str(e)}
    else:
        results["twelvedata"] = {"error": "TWELVE_DATA_API_KEY not set"}

    for sym in ["^VIX", "^VXN", "VIX", "VXN"]:
        try:
            acquire_finnhub_credit()
            r = _http.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": sym, "token": API_KEY},
                timeout=(5, 15),
            )
            results[f"finnhub_{sym}"] = {"status_code": r.status_code, "body": r.text[:500]}
        except Exception as e:
            results[f"finnhub_{sym}"] = {"error": str(e)}

    for sym in ["^vix", "^vxn"]:
        try:
            r = _http.get(
                "https://stooq.com/q/l/",
                params={"s": sym, "f": "sd2t2ohlcv", "h": "", "e": "csv"},
                timeout=(5, 15),
            )
            results[f"stooq_{sym}"] = {"status_code": r.status_code, "body": r.text[:500]}
        except Exception as e:
            results[f"stooq_{sym}"] = {"error": str(e)}

    return jsonify(results)


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
    acquire_finnhub_credit()
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


# Real index values. Dow, Nasdaq, and S&P 500 now come from FMP's
# standard quote endpoint (confirmed via live testing 2026-08-03 that
# ^GSPC/^DJI/^IXIC all work on the free tier — including genuine NASDAQ
# COMPOSITE data via ^IXIC, not a QQQ/Nasdaq-100 proxy). Crude oil and
# 10Y Treasury stay on RapidAPI, since FMP's free tier 402'd on both
# (^TNX, and every WTI crude symbol tried). See REAL_INDEX_CADENCE_SECONDS
# below for per-index refresh rates sized to each quota.


def fetch_fmp_index_quote(fmp_symbol):
    """Dow/Nasdaq/S&P 500 — FMP's standard quote endpoint, which also
    supports index symbols on the free tier. Simpler than the old
    RapidAPI setup: one provider, one shared 250/day quota, and real
    index data straight from FMP rather than juggling multiple RapidAPI
    listings with separate monthly caps."""
    if not FMP_API_KEY:
        raise ValueError("FMP_API_KEY not set")
    r = _http.get(
        "https://financialmodelingprep.com/stable/quote",
        params={"symbol": fmp_symbol, "apikey": FMP_API_KEY},
        timeout=(5, 15),
    )
    if r.text.lstrip().startswith("<"):
        raise ValueError(f"got HTML instead of JSON (likely blocked) — first 150 chars: {r.text[:150]!r}")
    if r.status_code == 402:
        raise ValueError("402 Payment Required — this symbol isn't available on the free FMP plan")
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise ValueError(f"unexpected response shape: {str(data)[:150]}")
    item = data[0]
    price = item.get("price")
    if price is None:
        raise ValueError(f"no price in response: {str(item)[:150]}")
    change = item.get("change")
    pct = item.get("changePercentage")
    return {
        "ok": True,
        "value": round(float(price), 2),
        "change": round(float(change), 2) if change is not None else None,
        "percent_change": round(float(pct), 2) if pct is not None else None,
    }


def fetch_yahoo127_key_statistics(yahoo_symbol):
    """Crude Oil — via the "yahoo-finance127" listing's key-statistics
    endpoint, on its own dedicated 100/month quota (separate from
    everything else). Values come wrapped as {"raw": x, "fmt": "..."}
    rather than flat numbers, and there's no direct change field — it's
    computed from current price vs previous close ourselves."""
    if not RAPIDAPI_KEY:
        raise ValueError("RAPIDAPI_KEY not set")
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


def fetch_via_live_stock_market(yahoo_symbol):
    """10Y Treasury — the "live-stock-market" listing's chart endpoint.
    Used to be shared with Dow/Nasdaq too, but now it's Treasury's alone
    (see cadence comment below), so it gets a much roomier refresh rate
    than before. Returns historical OHLC data rather than a simple
    quote, so change is computed from the last two closing prices ourselves."""
    if not RAPIDAPI_KEY:
        raise ValueError("RAPIDAPI_KEY not set")
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


def no_fallback(reason):
    """Fallback factory for indices with no reasonable ETF/proxy stand-in
    (e.g. VXN — there's no widely-traded ETF that tracks Nasdaq-100
    volatility the way SPY/DIA/ONEQ track their indices). Always raises,
    so fetch_index_all cleanly reports "unavailable" instead of crashing
    or silently showing something misleading."""
    def fallback():
        raise ValueError(reason)
    return fallback


def etf_fallback(etf_symbol):
    """Fallback factory — a tracking ETF via Finnhub, same source already
    used elsewhere in the app. Returns a no-arg callable."""
    def fallback():
        acquire_finnhub_credit()
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


def fetch_yahoo_chart_quote(symbol):
    """Yahoo Finance's unofficial, unauthenticated chart endpoint —
    confirmed working from Render's IP (2026-08-06) with real data for
    both ^VIX and ^VXN, unlike Stooq which blocks cloud-hosting IPs
    outright. Same underlying endpoint the yfinance Python library
    wraps, called directly here to avoid pulling in yfinance's heavy
    pandas/numpy dependencies for what's just a single current price."""
    r = _http.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        params={"interval": "1d", "range": "1d"},
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
        timeout=(5, 15),
    )
    if r.text.lstrip().startswith("<"):
        raise ValueError(f"got HTML instead of JSON (likely blocked) — first 150 chars: {r.text[:150]!r}")
    r.raise_for_status()
    data = r.json()
    try:
        meta = data["chart"]["result"][0]["meta"]
        price = meta["regularMarketPrice"]
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
    except (KeyError, IndexError, TypeError) as e:
        raise ValueError(f"unexpected response shape: {e!r} — raw: {str(data)[:200]}")
    if price is None:
        raise ValueError(f"no regularMarketPrice in response: {str(meta)[:200]}")
    change = (price - prev_close) if prev_close else None
    percent_change = (change / prev_close * 100) if (change is not None and prev_close) else None
    return {
        "ok": True,
        "value": round(float(price), 2),
        "change": round(float(change), 2) if change is not None else None,
        "percent_change": round(float(percent_change), 2) if percent_change is not None else None,
    }


REAL_INDEX_SOURCES = {
    "dow": (lambda: fetch_fmp_index_quote("^DJI"), etf_fallback("DIA")),
    "nasdaq": (lambda: fetch_fmp_index_quote("^IXIC"), etf_fallback("ONEQ")),
    "sp500": (lambda: fetch_fmp_index_quote("^GSPC"), etf_fallback("SPY")),
    "crude_oil": (lambda: fetch_yahoo127_key_statistics("CL=F"), macro_fallback("crude_oil")),
    "treasury_10y": (lambda: fetch_via_live_stock_market("^TNX"), macro_fallback("treasury_10y")),
    "vix": (lambda: fetch_fmp_index_quote("^VIX"), lambda: fetch_yahoo_chart_quote("^VIX")),
    "vxn": (lambda: fetch_yahoo_chart_quote("^VXN"), no_fallback("Yahoo chart is already the primary source for VXN — FMP 402s on this symbol")),
}
# Per-index cadence, sized to each quota:
# - Dow, Nasdaq, S&P 500, and VIX share FMP's single 250/day free quota
#   (nothing else in the app uses FMP). 15min each, market hours only
#   (~7hrs/day): 7*60/15=28 calls/day per index * 4 = 112/day
#   combined — comfortable margin under 250/day.
# - VXN doesn't touch FMP at all — it's primary source is Yahoo's
#   unofficial chart endpoint (FMP 402s on this symbol), no quota to
#   track since that endpoint is unauthenticated/undocumented.
# - Treasury now has the "live-stock-market" RapidAPI quota to itself
#   (previously shared 3 ways with Dow/Nasdaq) — 25min ~= 364/mo, safely
#   under its 500/mo cap with real margin to spare.
# - Crude Oil has its own dedicated "yahoo-finance127" quota, just
#   100/mo — window is 9:15am-4:15pm ET (~7hrs/day, ~152hrs/mo);
#   95min ~= 96/mo, just under quota
REAL_INDEX_CADENCE_SECONDS = {
    "dow": 15 * 60,
    "nasdaq": 15 * 60,
    "sp500": 15 * 60,
    "crude_oil": 95 * 60,
    "treasury_10y": 25 * 60,
    "vix": 15 * 60,
    "vxn": 15 * 60,
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
        "fmp_key_set": bool(FMP_API_KEY),
        "provider": "FMP" if key in ("dow", "nasdaq", "sp500") else "RapidAPI",
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
threading.Thread(target=_make_index_background_loop("vix", 38), daemon=True).start()
threading.Thread(target=_make_index_background_loop("vxn", 44), daemon=True).start()


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
    results = {}
    for symbol in ["^GSPC", "^DJI", "^IXIC", "^TNX", "CLUSD", "CL=F", "USOUSD"]:
        try:
            r = _http.get(
                "https://financialmodelingprep.com/stable/quote",
                params={"symbol": symbol, "apikey": FMP_API_KEY},
                timeout=(5, 15),
            )
            results[symbol] = {
                "status_code": r.status_code,
                "is_html_block_page": r.text.lstrip().startswith("<"),
                "raw_response": r.text[:500],
            }
        except Exception as e:
            results[symbol] = {"error": str(e)}
    return jsonify(results)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Default watchlist: {', '.join(DEFAULT_SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
