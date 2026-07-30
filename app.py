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


def fetch_sma_one(sym):
    """Daily closes from Twelve Data's free tier, used to compute 20/50-day SMA.
    (Finnhub's free tier doesn't expose the candle data this needs, and Stooq
    blocks requests from most cloud hosts, so this uses a third provider.)"""
    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": sym,
            "interval": "1day",
            "outputsize": 50,
            "apikey": TWELVE_DATA_API_KEY,
        },
        timeout=8,
    )
    data = r.json()
    if data.get("status") == "error":
        raise ValueError(data.get("message", "twelve data error"))
    values = data.get("values")
    if not values:
        raise ValueError("no data returned")

    closes = [float(v["close"]) for v in reversed(values)]  # oldest → newest
    if len(closes) < 20:
        raise ValueError(f"only {len(closes)} days of history returned")

    sma20 = sum(closes[-20:]) / 20
    sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
    return {"sma20": round(sma20, 2), "sma50": round(sma50, 2) if sma50 else None}


TWELVE_DATA_BATCH_SIZE = 8   # free-tier limit: 8 requests/minute
TWELVE_DATA_BATCH_PAUSE = 61  # seconds — wait out the per-minute window


def fetch_sma_all(symbols):
    out = {}
    for i in range(0, len(symbols), TWELVE_DATA_BATCH_SIZE):
        batch = symbols[i:i + TWELVE_DATA_BATCH_SIZE]
        for sym in batch:
            try:
                out[sym] = {"ok": True, **fetch_sma_one(sym)}
            except Exception as e:
                # No secret is embedded in this error (unlike the Finnhub
                # handling above), so it's safe to surface directly.
                out[sym] = {"ok": False, "error": str(e)}
        if i + TWELVE_DATA_BATCH_SIZE < len(symbols):
            time.sleep(TWELVE_DATA_BATCH_PAUSE)
    return out


@app.route("/api/sma")
def sma():
    now = time.time()
    if _sma_cache["data"] is not None and (now - _sma_cache["ts"]) < SMA_CACHE_SECONDS:
        return jsonify(_sma_cache["data"])

    if not TWELVE_DATA_API_KEY:
        out = {sym: {"ok": False, "error": "TWELVE_DATA_API_KEY not set"} for sym in SYMBOLS}
        return jsonify(out)

    out = fetch_sma_all(SYMBOLS)
    _sma_cache["data"] = out
    _sma_cache["ts"] = now
    return jsonify(out)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Watching: {', '.join(SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
