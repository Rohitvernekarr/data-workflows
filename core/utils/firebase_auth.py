"""Firebase ID token acquisition and caching for authenticated requests."""

import json
import os
import time
import urllib.error
import urllib.request
from prefect import get_run_logger

from .config_utils import Config


try:
    import firebase_admin
    FIREBASE_ADMIN_AVAILABLE = True
except ImportError:
    FIREBASE_ADMIN_AVAILABLE = False

_firebase_id_token: str = ""
_firebase_token_expiry: float = 0.0
_firebase_token_config: Config | None = None


def _exchange_custom_token(custom_token: str, api_key: str) -> tuple[str, int]:
    url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken"
        f"?key={api_key}"
    )
    payload = json.dumps({"token": custom_token, "returnSecureToken": True}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"identitytoolkit signInWithCustomToken failed (HTTP {exc.code}): {err_body}"
        ) from exc
    id_token   = body.get("idToken", "")
    expires_in = int(body.get("expiresIn", 3600))
    if not id_token:
        raise RuntimeError(f"signInWithCustomToken: no idToken in response — {body}")
    return id_token, expires_in


def _sign_in_with_email(email: str, password: str, api_key: str) -> tuple[str, int]:
    url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={api_key}"
    )
    payload = json.dumps({
        "email": email, "password": password, "returnSecureToken": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"identitytoolkit signInWithPassword failed (HTTP {exc.code}): {err_body}"
        ) from exc
    id_token   = body.get("idToken", "")
    expires_in = int(body.get("expiresIn", 3600))
    if not id_token:
        raise RuntimeError(f"signInWithPassword: no idToken in response — {body}")
    return id_token, expires_in


def get_firebase_token(cfg: Config) -> str:
    global _firebase_id_token, _firebase_token_expiry, _firebase_token_config

    static_token = cfg.get("firebase_id_token", fallback="")
    if static_token:
        print(f"🔑 Firebase idToken: {static_token}")
        return static_token

    now = time.time()
    if cfg is _firebase_token_config and _firebase_id_token and now < _firebase_token_expiry - 300:
        return _firebase_id_token

    api_key  = cfg.get("firebase_api_key", fallback="")
    email    = cfg.get("firebase_script_email", fallback="")
    password = cfg.get("firebase_script_password", fallback="")

    if email and password:
        if not api_key:
            raise RuntimeError("firebase_api_key is required for email/password sign-in.")
        get_run_logger().info("🔑 Firebase: signing in with email '%s' …", email)
        id_token, expires_in = _sign_in_with_email(email, password, api_key)
        _firebase_id_token    = id_token
        _firebase_token_expiry = now + expires_in
        _firebase_token_config = cfg
        get_run_logger().info("🔑 Firebase ID token obtained via email/password (expires in %ds)", expires_in)
        print(f"🔑 Firebase idToken: {id_token}")
        return _firebase_id_token

    sa_path = cfg.get("firebase_service_account_json", fallback="")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError(
            "No Firebase credentials configured.\n"
            "Set firebase_script_email, firebase_script_password, and firebase_api_key in [default] of config.ini."
        )
    if not api_key:
        raise RuntimeError("firebase_api_key not set in [default] of config.ini.")
    if not FIREBASE_ADMIN_AVAILABLE:
        raise RuntimeError("firebase-admin is not installed.  Run:  pip install firebase-admin")

    uid = cfg.get("firebase_service_account_uid", fallback="bomisco-script")
    get_run_logger().info("🔑 Firebase: minting custom token via service account (uid='%s') …", uid)
    try:
        import firebase_admin
        from firebase_admin import auth as fb_auth, credentials as fb_creds
        if not firebase_admin._apps:
            firebase_admin.initialize_app(fb_creds.Certificate(sa_path))
        custom_token_bytes = fb_auth.create_custom_token(uid)
    except Exception as exc:
        raise RuntimeError(f"firebase-admin create_custom_token failed: {exc}") from exc

    id_token, expires_in = _exchange_custom_token(
        custom_token_bytes.decode("utf-8"), api_key
    )
    _firebase_id_token    = id_token
    _firebase_token_expiry = now + expires_in
    _firebase_token_config = cfg
    get_run_logger().info("🔑 Firebase ID token obtained via custom token (expires in %ds)", expires_in)
    print(f"🔑 Firebase idToken: {id_token}")
    return _firebase_id_token


def auth_headers(cfg: Config) -> dict:
    if cfg.get_bool("firebase_auth_disabled"):
        get_run_logger().debug("Firebase auth disabled — sending unauthenticated request.")
        return {}
    token = get_firebase_token(cfg)
    return {"Authorization": f"Bearer {token}"}
