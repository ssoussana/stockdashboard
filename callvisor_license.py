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
    CALLVISOR_ADMIN_TOKEN       a long random string only Steve knows — guards
                                the POST /callvisor/admin/generate-beta-key
                                endpoint used to mint beta-tester keys by hand

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
from datetime import datetime, timedelta, timezone

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

# Shared secret for the admin-only beta-key-generation endpoint. Not
# exposed anywhere on the public site — only known to Steve, passed as
# an X-Admin-Token header. Generate a long random value and set it in
# Render; this is NOT the same as any Stripe or signing key.
CALLVISOR_ADMIN_TOKEN = os.environ.get("CALLVISOR_ADMIN_TOKEN")

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
                activation_code TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                stripe_session_id TEXT UNIQUE NOT NULL,
                issued_at TEXT NOT NULL,
                max_activations INTEGER NOT NULL DEFAULT 2,
                revoked INTEGER NOT NULL DEFAULT 0,
                key_type TEXT NOT NULL DEFAULT 'paid',
                expires_at TEXT
            )
        """)
        # Migrations, each independently best-effort since SQLite has no
        # "IF NOT EXISTS" for ALTER — safe to run on every startup.
        for stmt in (
            "ALTER TABLE licenses ADD COLUMN key_type TEXT NOT NULL DEFAULT 'paid'",
            "ALTER TABLE licenses ADD COLUMN expires_at TEXT",
            # Short opaque activation codes replaced the old self-contained
            # signed keys as the primary key column (see the block comment
            # below) — renames any pre-existing DB from the old name.
            "ALTER TABLE licenses RENAME COLUMN license_key TO activation_code",
        ):
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass  # column already exists / already renamed
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activation_code TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                UNIQUE(activation_code, fingerprint)
            )
        """)
        try:
            conn.execute("ALTER TABLE activations RENAME COLUMN license_key TO activation_code")
        except sqlite3.OperationalError:
            pass


init_db()

# ---------- Activation codes and device tokens ----------
#
# Two different things, deliberately kept separate:
#
# 1. ACTIVATION CODE — a short, opaque, random string like
#    "K7QM-3XNP-B9WT-R2FH-5CDJ". This is what the customer sees in their
#    email and types into CallVisor. It carries no information of its
#    own — it's purely a lookup key into the `licenses` table (email,
#    expiration, activation limit all live in the DB row).
#
# 2. DEVICE TOKEN — the long "CV1.<payload>.<signature>" signed token
#    from before. No longer shown to anyone — it's minted by /activate
#    the moment an activation code is redeemed, and the PC app caches it
#    silently. This is what Test-CallVisorLicenseKey verifies offline on
#    every launch, exactly as before; nothing about that offline-check
#    logic changed, only when/how the token gets created.
#
# Net effect: short, professional-looking keys for customers, with zero
# loss of offline verification once a device has activated — the only
# new requirement is that a brand-new device needs one internet
# connection at activation time to redeem its code, which was already
# true (that's the same call that enforces the per-key device limit).

from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature, encode_dss_signature

_P256_COORD_BYTES = 32

# Crockford's Base32 alphabet — excludes I, L, O, U specifically to avoid
# visual confusion (0/O, 1/I/L) when a customer is reading a code off an
# email and typing it by hand.
_CODE_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _der_sig_to_raw(der_sig: bytes) -> bytes:
    r, s = decode_dss_signature(der_sig)
    return r.to_bytes(_P256_COORD_BYTES, "big") + s.to_bytes(_P256_COORD_BYTES, "big")


def _raw_sig_to_der(raw_sig: bytes) -> bytes:
    if len(raw_sig) != _P256_COORD_BYTES * 2:
        raise ValueError("raw signature is the wrong length for P-256")
    r = int.from_bytes(raw_sig[:_P256_COORD_BYTES], "big")
    s = int.from_bytes(raw_sig[_P256_COORD_BYTES:], "big")
    return encode_dss_signature(r, s)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def generate_activation_code(group_count: int = 5, group_size: int = 4) -> str:
    """E.g. "K7QM-3XNP-B9WT-R2FH-5CDJ" — 20 random characters (100 bits
    of entropy) formatted as 5 groups of 4, 24 characters including
    dashes. Collisions are astronomically unlikely, but the DB's
    PRIMARY KEY constraint on this column is the actual backstop."""
    groups = ["".join(secrets.choice(_CODE_ALPHABET) for _ in range(group_size)) for _ in range(group_count)]
    return "-".join(groups)


def sign_device_token(
    email: str,
    ref_id: str,
    expires_at: str | None,
    max_activations: int,
    key_type: str,
) -> str:
    """Mints a device token for an ALREADY-DETERMINED expiration (pass
    through the licenses row's expires_at as-is — this does not compute
    a new expiration relative to "now", since a beta code issued with a
    30-day window should still expire 30 days from ISSUANCE, not 30
    days from whenever the customer happens to activate it)."""
    if _signing_key is None:
        raise RuntimeError("CALLVISOR_SIGNING_PRIVATE_KEY is not configured")

    payload = {
        "email": email,
        "sid": ref_id,
        "iat": datetime.now(timezone.utc).isoformat(),
        "exp": expires_at,
        "max": max_activations,
        "type": key_type,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    der_signature = _signing_key.sign(payload_bytes, ec.ECDSA(hashes.SHA256()))
    raw_signature = _der_sig_to_raw(der_signature)

    return f"CV1.{_b64url_encode(payload_bytes)}.{_b64url_encode(raw_signature)}"


def verify_license_key(license_key: str):
    """Returns (valid: bool, payload: dict|None, reason: str|None).
    Verifies a DEVICE TOKEN (the long CV1... string), not an activation
    code — offline, using only the public key."""
    if _signing_key is None:
        return False, None, "server signing key not configured"

    try:
        prefix, payload_b64, sig_b64 = license_key.split(".")
        if prefix != "CV1":
            return False, None, "unrecognized key format"
        payload_bytes = _b64url_decode(payload_b64)
        raw_signature = _b64url_decode(sig_b64)
        der_signature = _raw_sig_to_der(raw_signature)
    except Exception:
        return False, None, "malformed key"

    public_key = _signing_key.public_key()
    try:
        public_key.verify(der_signature, payload_bytes, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature:
        return False, None, "signature invalid"

    try:
        payload = json.loads(payload_bytes)
    except Exception:
        return False, None, "malformed payload"

    return True, payload, None


# ---------- Email (Resend) ----------

def send_license_email(to_email: str, activation_code: str):
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
                    <p style="font-family: monospace; font-size: 20px; letter-spacing: 1px; background: #f4f4f4;
                       padding: 12px; border-radius: 6px; text-align: center;">{activation_code}</p>
                    <p>Enter this into CallVisor's About tab to activate.
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


def send_beta_key_email(to_email: str, activation_code: str, expires_at: str):
    if not RESEND_API_KEY:
        print("[callvisor] RESEND_API_KEY not set, skipping beta email send", flush=True)
        return False
    try:
        expires_display = datetime.fromisoformat(expires_at).strftime("%B %-d, %Y") if expires_at else "N/A"
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": CALLVISOR_FROM_EMAIL,
                "to": [to_email],
                "subject": "Your CallVisor beta license key",
                "html": f"""
                    <p>Thanks for helping test CallVisor!</p>
                    <p>Your beta license key (valid through <b>{expires_display}</b>):</p>
                    <p style="font-family: monospace; font-size: 20px; letter-spacing: 1px; background: #f4f4f4;
                       padding: 12px; border-radius: 6px; text-align: center;">{activation_code}</p>
                    <p>Enter this into CallVisor's About tab to activate.
                    This key can be used on up to 2 devices. It'll stop working after the date
                    above — reach out if you'd like it extended.</p>
                    <p>Found a bug or have feedback? Just reply to this email.</p>
                """,
            },
            timeout=15,
        )
        if r.status_code >= 300:
            print(f"[callvisor] Resend beta send failed: {r.status_code} {r.text}", flush=True)
            return False
        return True
    except Exception as e:
        print(f"[callvisor] Resend beta send raised: {e!r}", flush=True)
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
            # we've already issued a code for this exact session, don't
            # issue (or email) a second one.
            existing = conn.execute(
                "SELECT activation_code FROM licenses WHERE stripe_session_id = ?", (session_id,)
            ).fetchone()
            if existing:
                print(f"[callvisor] duplicate webhook for session={session_id}, skipping", flush=True)
                return jsonify({"ok": True})

            activation_code = generate_activation_code()
            conn.execute(
                "INSERT INTO licenses (activation_code, email, stripe_session_id, issued_at, max_activations, key_type, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (activation_code, email, session_id, datetime.now(timezone.utc).isoformat(),
                 MAX_ACTIVATIONS_DEFAULT, "paid", None),
            )

        sent = send_license_email(email, activation_code)
        print(f"[callvisor] issued activation code for {email} (session={session_id}), email_sent={sent}", flush=True)

    return jsonify({"ok": True})


@callvisor_bp.route("/activate", methods=["POST"])
def activate():
    """Called once by the CallVisor PC app the first time an activation
    code is entered. Looks the code up directly (it's an opaque DB key,
    not a signed token, so no signature check happens here) and, if
    valid, mints and returns a signed device token for the PC app to
    cache and verify offline from then on.

    Body: {"activation_code": "...", "fingerprint": "..."}
    """
    data = request.get_json(silent=True) or {}
    activation_code = (data.get("activation_code", "") or "").strip().upper()
    fingerprint = data.get("fingerprint", "")

    if not activation_code or not fingerprint:
        return jsonify({"valid": False, "reason": "missing activation_code or fingerprint"}), 400

    with _db_lock, _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM licenses WHERE activation_code = ?", (activation_code,)
        ).fetchone()
        if row is None:
            return jsonify({"valid": False, "reason": "unrecognized activation code"}), 200
        if row["revoked"]:
            return jsonify({"valid": False, "reason": "this code has been revoked"}), 200

        exp = row["expires_at"]
        if exp and datetime.fromisoformat(exp) < datetime.now(timezone.utc):
            return jsonify({"valid": False, "reason": "this code has expired"}), 200

        already_activated = conn.execute(
            "SELECT 1 FROM activations WHERE activation_code = ? AND fingerprint = ?",
            (activation_code, fingerprint),
        ).fetchone()

        if not already_activated:
            activation_count = conn.execute(
                "SELECT COUNT(*) as c FROM activations WHERE activation_code = ?", (activation_code,)
            ).fetchone()["c"]
            if activation_count >= row["max_activations"]:
                return jsonify({"valid": False, "reason": "activation limit reached for this code"}), 200

            conn.execute(
                "INSERT INTO activations (activation_code, fingerprint, activated_at) VALUES (?, ?, ?)",
                (activation_code, fingerprint, datetime.now(timezone.utc).isoformat()),
            )

        # Always mint a fresh device token on success, whether this is a
        # brand-new activation or a repeat one (reinstall/upgrade) — a
        # repeat call means the PC app's local cache was wiped and needs
        # a new token handed back either way.
        device_token = sign_device_token(
            email=row["email"],
            ref_id=activation_code,
            expires_at=exp,
            max_activations=row["max_activations"],
            key_type=row["key_type"],
        )

    return jsonify({"valid": True, "device_token": device_token, "expires_at": exp})


@callvisor_bp.route("/admin/generate-beta-key", methods=["POST"])
def admin_generate_beta_key():
    """Steve-only endpoint for minting beta-test activation codes with an
    expiration, without needing a Stripe purchase at all.

    Body: {"email": "tester@example.com", "days": 30, "send_email": true}
    Header: X-Admin-Token: <CALLVISOR_ADMIN_TOKEN>
    """
    if not CALLVISOR_ADMIN_TOKEN:
        return jsonify({"error": "admin endpoint not configured"}), 500
    if request.headers.get("X-Admin-Token") != CALLVISOR_ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    days = data.get("days", 30)
    send_email = data.get("send_email", True)

    if not email:
        return jsonify({"error": "email is required"}), 400
    try:
        days = int(days)
        if not (1 <= days <= 90):
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "days must be an integer between 1 and 90"}), 400

    # No real Stripe session for a beta code — use a synthetic, still-unique
    # identifier so it fits the same UNIQUE stripe_session_id column
    # (also makes it obvious at a glance in the DB which rows are beta).
    synthetic_session_id = f"beta-{secrets.token_hex(8)}"
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    activation_code = generate_activation_code()

    with _db_lock, _get_conn() as conn:
        conn.execute(
            "INSERT INTO licenses (activation_code, email, stripe_session_id, issued_at, max_activations, key_type, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (activation_code, email, synthetic_session_id, datetime.now(timezone.utc).isoformat(),
             MAX_ACTIVATIONS_DEFAULT, "beta", expires_at),
        )

    sent = False
    if send_email:
        sent = send_beta_key_email(email, activation_code, expires_at)

    return jsonify({"activation_code": activation_code, "expires_at": expires_at, "email_sent": sent})
