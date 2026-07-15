#!/usr/bin/env python3

"""Prove generated credentials can authenticate without exposing them to argv."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.cookiejar
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials-file", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    return parser.parse_args()


def credential_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise RuntimeError(f"credentials file does not exist: {path}")
    if os.name != "nt" and path.stat().st_mode & 0o077:
        raise RuntimeError("credentials file must not be readable by group or others")
    labels = {
        "Username": "username",
        "Temporary password": "password",
        "TOTP secret": "totp_secret",
    }
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        label, separator, value = line.partition(": ")
        if separator and label in labels:
            values[labels[label]] = value.strip()
    missing = sorted(set(labels.values()) - values.keys())
    if missing or any(not values[key] for key in values):
        raise RuntimeError(f"credentials file is missing fields: {', '.join(missing)}")
    return values


def totp(secret: str, unix_seconds: int | None = None) -> str:
    normalized = "".join(secret.split()).upper()
    padding = "=" * (-len(normalized) % 8)
    key = base64.b32decode(normalized + padding, casefold=True)
    counter = int(time.time() if unix_seconds is None else unix_seconds) // 30
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return f"{value % 1_000_000:06d}"


def json_request(
    opener: urllib.request.OpenerDirector,
    url: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with opener.open(request, timeout=5) as response:
            body = json.loads(response.read().decode("utf-8"))
            return response.status, body
    except urllib.error.HTTPError as error:
        try:
            body = json.loads(error.read().decode("utf-8"))
        except Exception:
            body = {}
        return error.code, body


def main() -> int:
    args = parse_args()
    try:
        credentials = credential_values(args.credentials_file)
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPCookieProcessor(cookie_jar),
        )
        base_url = args.base_url.rstrip("/")
        login_status, _ = json_request(
            opener,
            f"{base_url}/api/auth/login",
            method="POST",
            payload={
                "username": credentials["username"],
                "password": credentials["password"],
                "code": totp(credentials["totp_secret"]),
            },
        )
        if login_status != 200:
            raise RuntimeError(f"login returned HTTP {login_status}")
        status_code, status = json_request(opener, f"{base_url}/api/grid/status")
        if status_code != 200:
            raise RuntimeError(f"authenticated strategy status returned HTTP {status_code}")
        if status.get("trading_enabled") is not True:
            raise RuntimeError("authenticated strategy status is not trading-enabled")
        if int(status.get("running_count", -1)) != 0:
            raise RuntimeError("initial cutover login proof found an active Rust strategy")
        logout_status, _ = json_request(
            opener,
            f"{base_url}/api/auth/logout",
            method="POST",
            payload={},
        )
        if logout_status != 200:
            raise RuntimeError(f"logout returned HTTP {logout_status}")
        print("Rust production login and authenticated zero-strategy status verified.")
        return 0
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"Rust production login verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
