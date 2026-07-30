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

import os
import threading
import time
import requests
from flask import Flask, jsonify, send_from_directory

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

SYMBOLS = ["PLTR", "NVDA", "CLS", "NBIS", "HOOD", "SPY", "QQQ",
           "GLW", "CRDO", "COHR", "SOXL", "DRAM", "IREN"]

CACHE_SECONDS = 60  # shared across all visitors, protects your Finnhub rate limit
_cache = {"data": None, "ts": 0}

# SMA is daily data, doesn't need to update every minute — cache it longer
# to keep load on Stooq (a free, no-key data source) light.
SMA_CACHE_SECONDS = 60 * 60 * 4  # 4 hours
_sma_cache = {"data": None, "ts": 0}

app = Flask(__name__, static_folder=".")


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


@app.route("/api/quotes")
def quotes():
    now = time.time()
    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_SECONDS:
        return jsonify(_cache["data"])

    out = {}
    for sym in SYMBOLS:
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
            out[sym] = {"ok": True, **data}
        except requests.exceptions.HTTPError:
            # Report only the status code — never the underlying exception,
            # which embeds the full request URL including the API key.
            out[sym] = {"ok": False, "error": f"HTTP {r.status_code}"}
        except Exception:
            out[sym] = {"ok": False, "error": "fetch failed"}

    _cache["data"] = out
    _cache["ts"] = now
    return jsonify(out)


TWELVE_DATA_BATCH_SIZE = 8   # free-tier limit: 8 credits/minute
TWELVE_DATA_BATCH_PAUSE = 61  # seconds — wait out the per-minute window


def fetch_sma_batch(symbols):
    """One HTTP call per batch of up to 8 symbols — Twelve Data accepts
    comma-separated symbols in a single request, so this needs far fewer
    round trips than fetching one symbol at a time."""
    print(f"[sma] requesting batch: {symbols}", flush=True)
    t0 = time.time()
    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": ",".join(symbols),
            "interval": "1day",
            "outputsize": 50,
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
            out[sym] = {"ok": True, "sma20": round(sma20, 2),
                        "sma50": round(sma50, 2) if sma50 else None}
        except Exception as e:
            # No secret is embedded in this error (unlike the Finnhub
            # handling above), so it's safe to surface directly.
            out[sym] = {"ok": False, "error": str(e)}
    return out


def fetch_sma_all(symbols):
    """Fetches every symbol in batches, writing results into the shared
    cache after EACH batch — not just at the end — so results (or a real
    error) show up within seconds instead of only after the full pass,
    and so a mid-run restart doesn't erase progress that already landed."""
    out = dict(_sma_cache["data"] or {})
    for i in range(0, len(symbols), TWELVE_DATA_BATCH_SIZE):
        batch = symbols[i:i + TWELVE_DATA_BATCH_SIZE]
        try:
            out.update(fetch_sma_batch(batch))
        except Exception as e:
            print(f"[sma] batch {batch} raised: {e!r}", flush=True)
            for sym in batch:
                out[sym] = {"ok": False, "error": f"batch request failed: {e}"}
        _sma_cache["data"] = dict(out)
        _sma_cache["ts"] = time.time()
        if i + TWELVE_DATA_BATCH_SIZE < len(symbols):
            print(f"[sma] pausing {TWELVE_DATA_BATCH_PAUSE}s before next batch", flush=True)
            time.sleep(TWELVE_DATA_BATCH_PAUSE)
    print("[sma] pass complete", flush=True)
    return out


@app.route("/api/sma")
def sma():
    if not TWELVE_DATA_API_KEY:
        out = {sym: {"ok": False, "error": "TWELVE_DATA_API_KEY not set"} for sym in SYMBOLS}
        return jsonify(out)

    if _sma_cache["data"] is None:
        elapsed = int(time.time() - _sma_status["attempt_started"])
        out = {sym: {"ok": False, "error": f"still loading ({elapsed}s so far)"} for sym in SYMBOLS}
        return jsonify(out)

    return jsonify(_sma_cache["data"])


_sma_status = {"attempt_started": time.time()}


def _sma_background_loop():
    """Runs forever in its own thread, refreshing the SMA cache on a slow
    cadence. Kept out of the request/response cycle entirely — the pacing
    this needs (to respect Twelve Data's free-tier rate limit) would
    otherwise make /api/sma block long enough to get killed by the host's
    request timeout, which is exactly what happened before this."""
    print("[sma] background thread started", flush=True)
    while True:
        if TWELVE_DATA_API_KEY:
            _sma_status["attempt_started"] = time.time()
            try:
                fetch_sma_all(SYMBOLS)  # writes into _sma_cache incrementally as it runs
            except Exception as e:
                print(f"[sma] fetch_sma_all raised: {e!r}", flush=True)
        else:
            print("[sma] TWELVE_DATA_API_KEY not set, skipping", flush=True)
        nap = SMA_CACHE_SECONDS if _sma_cache["data"] is not None else 5 * 60
        print(f"[sma] sleeping {nap}s until next refresh", flush=True)
        time.sleep(nap)


threading.Thread(target=_sma_background_loop, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Watching: {', '.join(SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
