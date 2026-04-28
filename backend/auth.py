import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
from dataclasses import dataclass

import pyotp


COOKIE_NAME = "grid_session"
HASH_SCHEME = "pbkdf2_sha256"
HASH_ITERATIONS = 260000


@dataclass
class AuthSettings:
    required: bool
    username: str
    password_hash: str
    totp_secret: str
    session_secret: str
    cookie_secure: bool

    @property
    def configured(self) -> bool:
        return all([self.username, self.password_hash, self.totp_secret, self.session_secret])


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_auth_settings() -> AuthSettings:
    return AuthSettings(
        required=_env_bool("AUTH_REQUIRED", False),
        username=os.getenv("ADMIN_USERNAME", "admin").strip(),
        password_hash=os.getenv("ADMIN_PASSWORD_HASH", "").strip(),
        totp_secret=os.getenv("TOTP_SECRET", "").replace(" ", "").strip(),
        session_secret=os.getenv("SESSION_SECRET", "").strip(),
        cookie_secure=_env_bool("AUTH_COOKIE_SECURE", False),
    )


def hash_password(password: str) -> str:
    salt = secrets.token_urlsafe(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        HASH_ITERATIONS,
    )
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"{HASH_SCHEME}${HASH_ITERATIONS}${salt}${encoded}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        scheme, iterations_text, salt, expected = stored_hash.split("$", 3)
        if scheme != HASH_SCHEME:
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            int(iterations_text),
        )
        actual = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


def verify_totp(code: str, secret: str) -> bool:
    normalized = "".join(ch for ch in str(code) if ch.isdigit())
    if len(normalized) != 6:
        return False
    return pyotp.TOTP(secret).verify(normalized, valid_window=1)


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def create_session(username: str, settings: AuthSettings, ttl_seconds: int = 43200) -> str:
    payload = {
        "sub": username,
        "exp": int(time.time()) + ttl_seconds,
        "nonce": secrets.token_urlsafe(12),
    }
    payload_text = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(
        settings.session_secret.encode("utf-8"),
        payload_text.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{payload_text}.{_b64encode(signature)}"


def verify_session(token: str, settings: AuthSettings) -> str | None:
    try:
        payload_text, signature_text = token.split(".", 1)
        expected = hmac.new(
            settings.session_secret.encode("utf-8"),
            payload_text.encode("ascii"),
            hashlib.sha256,
        ).digest()
        actual = _b64decode(signature_text)
        if not hmac.compare_digest(actual, expected):
            return None

        payload = json.loads(_b64decode(payload_text).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        username = str(payload.get("sub", ""))
        return username or None
    except Exception:
        return None


def build_totp_uri(settings: AuthSettings) -> str:
    return pyotp.TOTP(settings.totp_secret).provisioning_uri(
        name=settings.username,
        issuer_name="Grid Trading",
    )


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "hash":
        print(hash_password(sys.argv[2]))
        return
    if len(sys.argv) >= 2 and sys.argv[1] == "totp":
        print(pyotp.random_base32())
        return
    print("Usage:")
    print("  python backend/auth.py hash \"your-password\"")
    print("  python backend/auth.py totp")


if __name__ == "__main__":
    main()
