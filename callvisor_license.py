"""
CallVisor licensing blueprint.

Handles the purchase page, Stripe Checkout + webhook, license key
generation/signing, and device activation for CallVisor. Designed to be
registered onto the existing Market Pulse Flask app (shares its DATA_DIR
persistent disk, its own SQLite file rather than the JSON-cache pattern
used elsewhere in app.py, since license records involve real money and
need real transactions rather than whole-file overwrites).

Environment variables required:
    STRIPE_SECRET_KEY          sk_test_... (or sk_live_... in production)
    STRIPE_WEBHOOK_SECRET      whsec_... (from the Stripe webhook endpoint,
                                added after this is deployed — see notes)
    STRIPE_PRICE_ID            price_... (the one-time $9.99 CallVisor price)
    RESEND_API_KEY             re_...
    CALLVISOR_FROM_EMAIL       e.g. "CallVisor <license@call-visor.com>"
                                (the sending domain must be verified in Resend)
    CALLVISOR_SIGNING_PRIVATE_KEY
                                PEM-encoded ECDSA P-256 private key (see
                                generate_signing_key.py — generate this ONCE,
                                never regenerate after real keys have been
                                issued, or every existing key stops validating)

The public half of that signing key is NOT a secret and will eventually be
baked into the PC app for fully offline license validation. It is printed
by generate_signing_key.py alongside the private key.
"""

import base64
import json
import os
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests
import stripe
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, utils
from flask import Blueprint, jsonify, redirect, request

# ---------- Config ----------

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
CALLVISOR_FROM_EMAIL = os.environ.get("CALLVISOR_FROM_EMAIL", "CallVisor <license@call-visor.com>")
CALLVISOR_SIGNING_PRIVATE_KEY = os.environ.get("CALLVISOR_SIGNING_PRIVATE_KEY")

# Hosts that should be treated as "this is the CallVisor site" for the
# host-based routing added in app.py.
CALLVISOR_HOSTS = {"call-visor.com", "www.call-visor.com"}

MAX_ACTIVATIONS_DEFAULT = 2

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

_signing_key = None
if CALLVISOR_SIGNING_PRIVATE_KEY:
    _signing_key = serialization.load_pem_private_key(
        CALLVISOR_SIGNING_PRIVATE_KEY.encode(), password=None
    )
else:
    print("[callvisor] WARNING: CALLVISOR_SIGNING_PRIVATE_KEY not set — "
          "license generation will fail until this is configured.", flush=True)

# ---------- Storage (SQLite on the shared persistent disk) ----------

DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(DATA_DIR, "callvisor_licenses.db")
_db_lock = threading.Lock()


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _db_lock, _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS licenses (
                license_key TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                stripe_session_id TEXT UNIQUE NOT NULL,
                issued_at TEXT NOT NULL,
                max_activations INTEGER NOT NULL DEFAULT 2,
                revoked INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                UNIQUE(license_key, fingerprint)
            )
        """)


init_db()

# ---------- License key format ----------
#
# A key is:  CV1.<payload_b64url>.<signature_b64url>
# payload is compact JSON: {"email": "...", "sid": "<stripe session id>",
#                            "iat": "<issued_at iso>", "max": 2}
# signed with ECDSA P-256 over SHA-256 of the payload bytes.
#
# Validation only needs the PUBLIC key, so it can eventually be done
# entirely offline by the PC app — this server-side /activate endpoint
# additionally tracks activation counts, which is inherently a shared,
# server-side concept (a single device can't know how many *other*
# devices have already used the same key).


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def generate_license_key(email: str, stripe_session_id: str, max_activations: int = MAX_ACTIVATIONS_DEFAULT) -> str:
    if _signing_key is None:
        raise RuntimeError("CALLVISOR_SIGNING_PRIVATE_KEY is not configured")

    payload = {
        "email": email,
        "sid": stripe_session_id,
        "iat": datetime.now(timezone.utc).isoformat(),
        "max": max_activations,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    signature = _signing_key.sign(payload_bytes, ec.ECDSA(hashes.SHA256()))

    return f"CV1.{_b64url_encode(payload_bytes)}.{_b64url_encode(signature)}"


def verify_license_key(license_key: str):
    """Returns (valid: bool, payload: dict|None, reason: str|None)."""
    if _signing_key is None:
        return False, None, "server signing key not configured"

    try:
        prefix, payload_b64, sig_b64 = license_key.split(".")
        if prefix != "CV1":
            return False, None, "unrecognized key format"
        payload_bytes = _b64url_decode(payload_b64)
        signature = _b64url_decode(sig_b64)
    except Exception:
        return False, None, "malformed key"

    public_key = _signing_key.public_key()
    try:
        public_key.verify(signature, payload_bytes, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature:
        return False, None, "signature invalid"

    try:
        payload = json.loads(payload_bytes)
    except Exception:
        return False, None, "malformed payload"

    return True, payload, None


# ---------- Email (Resend) ----------

def send_license_email(to_email: str, license_key: str):
    if not RESEND_API_KEY:
        print("[callvisor] RESEND_API_KEY not set, skipping email send", flush=True)
        return False
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": CALLVISOR_FROM_EMAIL,
                "to": [to_email],
                "subject": "Your CallVisor license key",
                "html": f"""
                    <p>Thanks for purchasing CallVisor!</p>
                    <p>Your license key:</p>
                    <p style="font-family: monospace; font-size: 14px; background: #f4f4f4;
                       padding: 12px; border-radius: 6px; word-break: break-all;">{license_key}</p>
                    <p>Paste this into CallVisor's Settings &rarr; License tab to activate.
                    This key can be used on up to 2 devices.</p>
                    <p>Questions? Just reply to this email.</p>
                """,
            },
            timeout=15,
        )
        if r.status_code >= 300:
            print(f"[callvisor] Resend send failed: {r.status_code} {r.text}", flush=True)
            return False
        return True
    except Exception as e:
        print(f"[callvisor] Resend send raised: {e!r}", flush=True)
        return False


# ---------- Blueprint / routes ----------

callvisor_bp = Blueprint("callvisor", __name__, url_prefix="/callvisor")


@callvisor_bp.route("/")
def landing():
    price_display = "$9.99"
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>CallVisor — See and answer phone calls in VR</title>
        <style>
            body {{ background: #0A0A0C; color: #fff; font-family: -apple-system, Segoe UI, sans-serif;
                    display: flex; flex-direction: column; align-items: center; padding: 64px 24px; }}
            h1 {{ font-size: 28px; margin-bottom: 4px; }}
            p.tagline {{ color: #9CA3AF; margin-top: 0; }}
            .price {{ font-size: 20px; font-weight: bold; margin: 24px 0 8px; }}
            button {{ background: #4F46E5; color: #fff; border: none; padding: 16px 32px;
                      border-radius: 999px; font-size: 16px; font-weight: bold; cursor: pointer; }}
            button:hover {{ background: #7C74F0; }}
            #error {{ color: #E53E3E; margin-top: 12px; font-size: 14px; }}
        </style>
    </head>
    <body>
        <h1>CallVisor</h1>
        <p class="tagline">See and answer your phone calls without leaving VR.</p>
        <div class="price">{price_display} &mdash; one-time purchase, up to 2 devices</div>
        <button id="buy">Buy CallVisor</button>
        <div id="error"></div>
        <script>
            document.getElementById('buy').addEventListener('click', async () => {{
                const btn = document.getElementById('buy');
                btn.disabled = true;
                btn.textContent = 'Redirecting...';
                try {{
                    const res = await fetch('/callvisor/create-checkout-session', {{ method: 'POST' }});
                    const data = await res.json();
                    if (data.url) {{
                        window.location.href = data.url;
                    }} else {{
                        throw new Error(data.error || 'Unknown error');
                    }}
                }} catch (e) {{
                    document.getElementById('error').textContent = 'Something went wrong: ' + e.message;
                    btn.disabled = false;
                    btn.textContent = 'Buy CallVisor';
                }}
            }});
        </script>
    </body>
    </html>
    """


@callvisor_bp.route("/create-checkout-session", methods=["POST"])
def create_checkout_session():
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        return jsonify({"error": "Stripe is not configured on the server"}), 500
    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url="https://call-visor.com/callvisor/success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://call-visor.com/callvisor/",
        )
        return jsonify({"url": session.url})
    except Exception as e:
        print(f"[callvisor] checkout session creation failed: {e!r}", flush=True)
        return jsonify({"error": str(e)}), 500


@callvisor_bp.route("/success")
def success():
    return """
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><title>Thanks!</title>
    <style>
        body { background: #0A0A0C; color: #fff; font-family: -apple-system, Segoe UI, sans-serif;
               display: flex; flex-direction: column; align-items: center; padding: 96px 24px; text-align: center; }
    </style>
    </head>
    <body>
        <h1>Thanks for purchasing CallVisor!</h1>
        <p>Your license key is on its way to your email — it can take a minute or two to arrive.</p>
    </body>
    </html>
    """


@callvisor_bp.route("/stripe-webhook", methods=["POST"])
def stripe_webhook():
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        print("[callvisor] STRIPE_WEBHOOK_SECRET not set, rejecting webhook", flush=True)
        return jsonify({"error": "webhook not configured"}), 500

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except (ValueError, stripe.error.SignatureVerificationError) as e:
        print(f"[callvisor] webhook signature verification failed: {e!r}", flush=True)
        return jsonify({"error": "invalid signature"}), 400

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        # Newer stripe-python returns a typed object here, not a plain
        # dict — .get() isn't supported on it directly, so convert once
        # up front and work with a real dict for the rest of this block.
        if hasattr(session, "to_dict"):
            session = session.to_dict()
        session_id = session["id"]
        email = (session.get("customer_details") or {}).get("email") or session.get("customer_email")

        if not email:
            print(f"[callvisor] checkout.session.completed with no email, session={session_id}", flush=True)
            return jsonify({"ok": True})

        with _db_lock, _get_conn() as conn:
            # Idempotency: Stripe can and does retry webhook delivery. If
            # we've already issued a key for this exact session, don't
            # issue (or email) a second one.
            existing = conn.execute(
                "SELECT license_key FROM licenses WHERE stripe_session_id = ?", (session_id,)
            ).fetchone()
            if existing:
                print(f"[callvisor] duplicate webhook for session={session_id}, skipping", flush=True)
                return jsonify({"ok": True})

            license_key = generate_license_key(email, session_id)
            conn.execute(
                "INSERT INTO licenses (license_key, email, stripe_session_id, issued_at, max_activations) "
                "VALUES (?, ?, ?, ?, ?)",
                (license_key, email, session_id, datetime.now(timezone.utc).isoformat(), MAX_ACTIVATIONS_DEFAULT),
            )

        sent = send_license_email(email, license_key)
        print(f"[callvisor] issued license for {email} (session={session_id}), email_sent={sent}", flush=True)

    return jsonify({"ok": True})


@callvisor_bp.route("/activate", methods=["POST"])
def activate():
    """Called once by the CallVisor PC app the first time a key is entered.
    Body: {"license_key": "...", "fingerprint": "..."}
    """
    data = request.get_json(silent=True) or {}
    license_key = data.get("license_key", "")
    fingerprint = data.get("fingerprint", "")

    if not license_key or not fingerprint:
        return jsonify({"valid": False, "reason": "missing license_key or fingerprint"}), 400

    valid, payload, reason = verify_license_key(license_key)
    if not valid:
        return jsonify({"valid": False, "reason": reason}), 200

    with _db_lock, _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM licenses WHERE license_key = ?", (license_key,)
        ).fetchone()
        if row is None:
            # Signature is valid but we have no record of it — shouldn't
            # happen under normal operation, but treat as invalid rather
            # than trusting the token alone.
            return jsonify({"valid": False, "reason": "unknown license key"}), 200
        if row["revoked"]:
            return jsonify({"valid": False, "reason": "license revoked"}), 200

        already_activated = conn.execute(
            "SELECT 1 FROM activations WHERE license_key = ? AND fingerprint = ?",
            (license_key, fingerprint),
        ).fetchone()
        if already_activated:
            # Same device re-activating (reinstall/upgrade) — always allowed,
            # doesn't consume a new slot.
            return jsonify({"valid": True, "reason": "already activated on this device"})

        activation_count = conn.execute(
            "SELECT COUNT(*) as c FROM activations WHERE license_key = ?", (license_key,)
        ).fetchone()["c"]

        if activation_count >= row["max_activations"]:
            return jsonify({"valid": False, "reason": "activation limit reached for this key"}), 200

        conn.execute(
            "INSERT INTO activations (license_key, fingerprint, activated_at) VALUES (?, ?, ?)",
            (license_key, fingerprint, datetime.now(timezone.utc).isoformat()),
        )

    return jsonify({"valid": True})
