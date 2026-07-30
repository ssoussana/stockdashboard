"""
Market Pulse dashboard server.

Runs locally, fetches Finnhub quotes server-side (no CORS issue that way),
and serves the dashboard page. Your API key stays on your machine only —
it's read from an environment variable and never sent to the browser.

Setup:
    pip install flask requests
    export FINNHUB_API_KEY="your-key-here"
    python app.py

Then open http://localhost:5000
"""

import csv
import io
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


STOOQ_HEADERS = {
    # Stooq's anti-bot filtering often rejects the default python-requests
    # User-Agent (and can be stricter about cloud-hosting IP ranges like
    # Render's). A normal browser UA gets past this in most cases.
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36")
}


def fetch_sma(sym):
    """20/50-day SMA from Stooq's free daily CSV — no API key needed.
    Finnhub's free tier doesn't expose the candle data required for this."""
    r = requests.get(
        "https://stooq.com/q/d/l/",
        params={"s": f"{sym.lower()}.us", "i": "d"},
        headers=STOOQ_HEADERS,
        timeout=6,
    )
    r.raise_for_status()
    text = r.text.strip()
    if not text or text.startswith("N/D") or "Exceeded" in text:
        raise ValueError(f"stooq returned: {text[:80]!r}")
    if text.lstrip().startswith("<"):
        raise ValueError("stooq returned HTML, not CSV (likely blocked)")

    rows = list(csv.DictReader(io.StringIO(text)))
    closes = [float(row["Close"]) for row in rows if row.get("Close")]
    if len(closes) < 20:
        raise ValueError(f"only {len(closes)} rows of history returned")

    sma20 = sum(closes[-20:]) / 20
    sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
    return {"sma20": round(sma20, 2), "sma50": round(sma50, 2) if sma50 else None}


@app.route("/api/sma")
def sma():
    now = time.time()
    if _sma_cache["data"] is not None and (now - _sma_cache["ts"]) < SMA_CACHE_SECONDS:
        return jsonify(_sma_cache["data"])

    out = {}
    for sym in SYMBOLS:
        try:
            out[sym] = {"ok": True, **fetch_sma(sym)}
        except Exception as e:
            # Stooq has no secret/key involved, so it's safe to surface
            # the real reason here (unlike the Finnhub error handling above).
            out[sym] = {"ok": False, "error": str(e)}

    _sma_cache["data"] = out
    _sma_cache["ts"] = now
    return jsonify(out)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Watching: {', '.join(SYMBOLS)}")
    print(f"Dashboard running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
