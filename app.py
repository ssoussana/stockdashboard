"""
Market Pulse dashboard server.

Runs locally, fetches Finnhub quotes server-side (no CORS issue that way),
and serves the dashboard page. Your API key stays on your machine only —
it's read from an environment variable and never sent to the browser.

Setup:
    pip install flask requests
    export FINNHUB_API_KEY="your-key-here"
    export TWELVE_DATA_API_KEY="your-key-here"   (optional, powers the SMA line —
        free at twelvedata.com, no card required. Without it, SMA just shows "n/a".)
    python app.py

Then open http://localhost:5000
"""

import collections
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dt_time
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

DEFAULT_SYMBOLS = ["PLTR", "NVDA", "CLS", "NBIS", "HOOD", "SPY", "QQQ",
                    "GLW", "CRDO", "COHR", "SOXL", "DRAM", "IREN"]

CACHE_SECONDS = 60  # per-symbol, shared across all visitors requesting that symbol
_quote_cache = {}   # symbol -> {"data": {...}, "ts": float}

# SMA is daily data, doesn't need to update every minute — cache it longer.
SMA_CACHE_SECONDS = 60 * 60 * 4  # 4 hours
_sma_cache = {}      # symbol -> {"data": {...}, "ts": float}

# The full set of symbols anyone's watchlist currently includes. Grows as
# people add tickers; the SMA background loop iterates over this each pass.
_known_symbols = set(DEFAULT_SYMBOLS)
_known_lock = threading.Lock()
_wake_event = threading.Event()  # lets a newly-added symbol skip the long wait


def parse_symbols_param():
    raw = request.args.get("symbols", "")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return symbols or list(DEFAULT_SYMBOLS)


def register_known_symbols(symbols):
    """Adds newly-seen symbols to the shared watch set, kicks off an
    immediate one-off SMA/RSI fetch for each brand-new one (so it doesn't
    have to wait its turn in the next scheduled pass), and wakes the
    background loop so the new symbol is included in future full passes."""
    with _known_lock:
        new = set(symbols) - _known_symbols
        if new:
            _known_symbols.update(new)
    if new:
        _wake_event.set()
        if TWELVE_DATA_API_KEY:
            for sym in new:
                threading.Thread(target=fetch_sma_immediate, args=(sym,), daemon=True).start()

app = Flask(__name__, static_folder=".")


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


def fetch_quote_one(sym):
    try:
        r = requests.get(
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


_quote_executor = ThreadPoolExecutor(max_workers=8)


def get_quote_cached(sym):
    entry = _quote_cache.get(sym)
    if entry and (time.time() - entry["ts"]) < CACHE_SECONDS:
        return entry["data"]
    return None


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
        r = requests.get(
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


TWELVE_DATA_BATCH_SIZE = 8   # free-tier limit: 8 credits/minute

# One shared limiter for every Twelve Data call, whichever feature makes it
# (SMA/RSI's scheduled pass, an immediate one-off add, or after-hours
# prices). Blocks the caller until enough credits are free in the trailing
# 60-second window, so two features can never independently burst past the
# combined free-tier limit — replaces the old fixed-pause approach, which
# only worked as long as nothing else was also calling Twelve Data.
_credit_lock = threading.Lock()
_credit_timestamps = collections.deque()


def acquire_twelvedata_credits(n):
    while True:
        with _credit_lock:
            now = time.time()
            while _credit_timestamps and now - _credit_timestamps[0] > 60:
                _credit_timestamps.popleft()
            if len(_credit_timestamps) + n <= TWELVE_DATA_BATCH_SIZE:
                for _ in range(n):
                    _credit_timestamps.append(now)
                return
        time.sleep(2)


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
    """One HTTP call per batch of up to 8 symbols — Twelve Data accepts
    comma-separated symbols in a single request, so this needs far fewer
    round trips than fetching one symbol at a time."""
    acquire_twelvedata_credits(len(symbols))
    print(f"[sma] requesting batch: {symbols}", flush=True)
    t0 = time.time()
    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": ",".join(symbols),
            "interval": "1day",
            "outputsize": 100,  # a bit more than SMA needs, for RSI to converge well
            "apikey": TWELVE_DATA_API_KEY,
        },
        timeout=(5, 15),  # (connect timeout, read timeout) — explicit, not a single shared value
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
    for i in range(0, len(symbols), TWELVE_DATA_BATCH_SIZE):
        batch = symbols[i:i + TWELVE_DATA_BATCH_SIZE]
        try:
            batch_out = fetch_sma_batch(batch)
        except Exception as e:
            print(f"[sma] batch {batch} raised: {e!r}", flush=True)
            batch_out = {sym: {"ok": False, "error": f"batch request failed: {e}"} for sym in batch}
        now = time.time()
        for sym, data in batch_out.items():
            _sma_cache[sym] = {"data": data, "ts": now}
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
            out[sym] = {"ok": False, "error": f"not fetched yet ({elapsed}s since current pass started)"}
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
            with _known_lock:
                symbols = sorted(_known_symbols)
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


# ---------- After-hours price ----------
# US market extended-hours windows, Eastern time. This is an approximation
# — it doesn't account for market holidays or early-close days.
PRE_MARKET_START = dt_time(4, 0)
PRE_MARKET_END = dt_time(9, 30)
POST_MARKET_START = dt_time(16, 0)
POST_MARKET_END = dt_time(20, 0)

AFTERHOURS_CACHE_SECONDS = 15 * 60  # per-user request: 15-minute refresh
_afterhours_cache = {}  # symbol -> {"data": {...}, "ts": float}


def market_session_now():
    """Returns 'pre', 'post', or 'closed' (which also covers regular market
    hours — no extended price to show then)."""
    try:
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception as e:
        print(f"[afterhours] ZoneInfo lookup failed: {e!r} — treating as closed", flush=True)
        return "closed"
    if now_et.weekday() >= 5:  # Saturday/Sunday
        return "closed"
    t = now_et.time()
    if PRE_MARKET_START <= t < PRE_MARKET_END:
        return "pre"
    if POST_MARKET_START <= t < POST_MARKET_END:
        return "post"
    return "closed"


def fetch_afterhours_batch(symbols):
    """One HTTP call per batch of up to 8 symbols, using Twelve Data's
    prepost=true quote option to get the latest extended-hours print
    alongside the regular session's numbers."""
    acquire_twelvedata_credits(len(symbols))
    print(f"[afterhours] requesting batch: {symbols}", flush=True)
    r = requests.get(
        "https://api.twelvedata.com/quote",
        params={
            "symbol": ",".join(symbols),
            "prepost": "true",
            "apikey": TWELVE_DATA_API_KEY,
        },
        timeout=(5, 15),
    )
    print(f"[afterhours] batch {symbols} responded, status {r.status_code}", flush=True)
    payload = r.json()
    if len(symbols) == 1 and ("close" in payload or "symbol" in payload):
        payload = {symbols[0]: payload}

    out = {}
    for sym in symbols:
        entry = payload.get(sym)
        try:
            if not entry:
                raise ValueError("symbol missing from batch response")
            if entry.get("status") == "error":
                raise ValueError(entry.get("message", "twelve data error"))
            is_ext = str(entry.get("is_extended_hours", "")).lower() == "true"
            if not is_ext:
                out[sym] = {"ok": True, "active": False}
                continue
            # Field name isn't fully confirmed from documentation alone —
            # fall back to "close" (which Twelve Data's own example shows
            # holding the extended print when is_extended_hours is true)
            # if a dedicated "extended_price" field isn't present.
            price = entry.get("extended_price", entry.get("close"))
            if price is None:
                raise ValueError("no extended price in response")
            out[sym] = {"ok": True, "active": True, "price": round(float(price), 2)}
        except Exception as e:
            out[sym] = {"ok": False, "error": str(e)}
    return out


def _afterhours_background_loop():
    print("[afterhours] background thread started", flush=True)
    while True:
        session = market_session_now()
        if session in ("pre", "post") and TWELVE_DATA_API_KEY:
            with _known_lock:
                symbols = sorted(_known_symbols)
            print(f"[afterhours] market session is '{session}', refreshing {len(symbols)} symbols", flush=True)
            for i in range(0, len(symbols), TWELVE_DATA_BATCH_SIZE):
                batch = symbols[i:i + TWELVE_DATA_BATCH_SIZE]
                try:
                    batch_out = fetch_afterhours_batch(batch)
                except Exception as e:
                    print(f"[afterhours] batch {batch} raised: {e!r}", flush=True)
                    batch_out = {sym: {"ok": False, "error": f"batch request failed: {e}"} for sym in batch}
                now = time.time()
                for sym, data in batch_out.items():
                    _afterhours_cache[sym] = {"data": data, "ts": now}
            print("[afterhours] pass complete", flush=True)
        else:
            print(f"[afterhours] market session is '{session}', skipping", flush=True)
        time.sleep(AFTERHOURS_CACHE_SECONDS)


threading.Thread(target=_afterhours_background_loop, daemon=True).start()


@app.route("/api/afterhours")
def afterhours():
    symbols = parse_symbols_param()
    register_known_symbols(symbols)

    if not TWELVE_DATA_API_KEY:
        return jsonify({sym: {"ok": False, "error": "TWELVE_DATA_API_KEY not set"} for sym in symbols})

    out = {}
    for sym in symbols:
        entry = _afterhours_cache.get(sym)
        out[sym] = entry["data"] if entry else {"ok": True, "active": False}
    return jsonify(out)


@app.route("/api/debug/afterhours")
def afterhours_debug():
    """Diagnostic snapshot — what the server thinks is happening right
    now, without having to dig through logs."""
    now = time.time()
    cache_ages = {
        sym: round(now - entry["ts"], 1)
        for sym, entry in _afterhours_cache.items()
    }
    try:
        now_et = datetime.now(ZoneInfo("America/New_York")).isoformat()
        tz_error = None
    except Exception as e:
        now_et = None
        tz_error = repr(e)
    return jsonify({
        "computed_session": market_session_now(),
        "server_time_et": now_et,
        "timezone_error": tz_error,
        "twelve_data_key_set": bool(TWELVE_DATA_API_KEY),
        "known_symbols": sorted(_known_symbols),
        "afterhours_cache_seconds_old": cache_ages,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Default watchlist: {', '.join(DEFAULT_SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
