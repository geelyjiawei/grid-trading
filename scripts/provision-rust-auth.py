#!/usr/bin/env python3

"""Create Rust web credentials without printing or committing secrets."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


HASH_ITERATIONS = 260_000
ENV_PATTERN = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path.home() / ".grid-trading" / "rust-admin.txt",
    )
    parser.add_argument("--username", default="admin")
    parser.add_argument(
        "--cookie-secure",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def read_environment(path: Path) -> tuple[list[str], dict[str, str]]:
    if not path.is_file():
        raise RuntimeError(f"environment file does not exist: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        match = ENV_PATTERN.match(line)
        if match:
            values[match.group(1)] = match.group(2).strip()
    return lines, values


def password_hash(password: str) -> str:
    salt = secrets.token_urlsafe(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        HASH_ITERATIONS,
    )
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"pbkdf2_sha256${HASH_ITERATIONS}${salt}${encoded}"


def replace_environment(lines: list[str], updates: dict[str, str]) -> str:
    output: list[str] = []
    written: set[str] = set()
    for line in lines:
        match = ENV_PATTERN.match(line)
        key = match.group(1) if match else None
        if key not in updates:
            output.append(line)
            continue
        if key not in written:
            output.append(f"{key}={updates[key]}")
            written.add(key)
    for key, value in updates.items():
        if key not in written:
            output.append(f"{key}={value}")
    return "\n".join(output) + "\n"


def compose_literal(value: str) -> str:
    """Keep dollar signs literal when Compose reads the env_file."""
    if "'" in value or "\n" in value or "\r" in value:
        raise RuntimeError("generated environment value cannot be represented safely")
    return f"'{value}'"


def write_private(path: Path, content: str, *, replace: bool) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if replace else os.O_EXCL)
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    os.chmod(path, 0o600)


def main() -> int:
    args = parse_args()
    try:
        lines, existing = read_environment(args.env_file)
        has_hash = bool(existing.get("ADMIN_PASSWORD_HASH"))
        has_totp = bool(existing.get("TOTP_SECRET"))
        configured = (
            existing.get("AUTH_REQUIRED", "").lower() == "true"
            and bool(existing.get("ADMIN_USERNAME"))
            and has_hash
            and has_totp
        )
        if configured and not args.force:
            stored_hash = existing["ADMIN_PASSWORD_HASH"]
            if not (stored_hash.startswith("'") and stored_hash.endswith("'")):
                normalized = replace_environment(
                    lines,
                    {"ADMIN_PASSWORD_HASH": compose_literal(stored_hash)},
                )
                temporary = args.env_file.with_name(
                    f".{args.env_file.name}.tmp-{os.getpid()}"
                )
                write_private(temporary, normalized, replace=False)
                os.replace(temporary, args.env_file)
                os.chmod(args.env_file, 0o600)
                print(
                    "Existing Rust password hash was quoted for literal Docker Compose loading."
                )
            print(f"Rust web authentication is already configured in {args.env_file}.")
            return 0
        if has_hash != has_totp and not args.force:
            raise RuntimeError(
                "ADMIN_PASSWORD_HASH and TOTP_SECRET are only partially configured; "
                "use --force to rotate both together"
            )
        if args.credentials_file.exists() and not args.force:
            raise RuntimeError(
                f"credentials file already exists: {args.credentials_file}; "
                "remove it or use --force"
            )

        username = args.username.strip()
        if not username or any(character.isspace() for character in username):
            raise RuntimeError("username must be non-empty and contain no whitespace")
        password = secrets.token_urlsafe(24)
        totp_secret = base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")
        updates = {
            "AUTH_REQUIRED": "true",
            "ADMIN_USERNAME": username,
            "ADMIN_PASSWORD_HASH": compose_literal(password_hash(password)),
            "TOTP_SECRET": totp_secret,
            "SESSION_SECRET": existing.get("SESSION_SECRET") or secrets.token_urlsafe(48),
            "AUTH_COOKIE_SECURE": args.cookie_secure,
            "GRID_RUST_ADMIN_TOKEN": existing.get("GRID_RUST_ADMIN_TOKEN")
            or secrets.token_urlsafe(48),
        }

        env_content = replace_environment(lines, updates)
        temporary = args.env_file.with_name(f".{args.env_file.name}.tmp-{os.getpid()}")
        write_private(temporary, env_content, replace=False)
        os.replace(temporary, args.env_file)
        os.chmod(args.env_file, 0o600)

        account = quote(username, safe="")
        issuer = quote("Grid Trading", safe="")
        uri = (
            f"otpauth://totp/{issuer}:{account}?secret={totp_secret}"
            f"&issuer={issuer}&algorithm=SHA1&digits=6&period=30"
        )
        credentials = "\n".join(
            (
                "Grid Trading Rust production login",
                f"Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
                f"Username: {username}",
                f"Temporary password: {password}",
                f"TOTP secret: {totp_secret}",
                f"Authenticator URI: {uri}",
                "",
                "Store this file securely and delete it after adding the TOTP secret.",
                "",
            )
        )
        write_private(args.credentials_file, credentials, replace=args.force)
        print(f"Rust web authentication was written to {args.env_file}.")
        print(f"One-time login details were written to {args.credentials_file} (mode 0600).")
        return 0
    except (OSError, RuntimeError) as error:
        print(f"authentication provisioning failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
