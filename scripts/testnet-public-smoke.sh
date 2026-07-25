#!/bin/sh
set -eu

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

probe_get() {
    name="$1"
    url="$2"
    expected="$3"
    body="$work_dir/$name.json"
    metrics="$(curl --fail --silent --show-error \
        --connect-timeout 5 --max-time 10 \
        --output "$body" \
        --write-out '%{http_code} %{time_total}' \
        "$url")"
    grep -q "$expected" "$body"
    printf '%s REST OK http=%s seconds=%s\n' "$name" ${metrics}
}

probe_post() {
    name="$1"
    url="$2"
    payload="$3"
    expected="$4"
    body="$work_dir/$name.json"
    metrics="$(curl --fail --silent --show-error \
        --connect-timeout 5 --max-time 10 \
        --header 'Content-Type: application/json' \
        --data "$payload" \
        --output "$body" \
        --write-out '%{http_code} %{time_total}' \
        "$url")"
    grep -q "$expected" "$body"
    printf '%s REST OK http=%s seconds=%s\n' "$name" ${metrics}
}

probe_get \
    binance \
    'https://testnet.binancefuture.com/fapi/v1/time' \
    'serverTime'
probe_get \
    bybit \
    'https://api-testnet.bybit.com/v5/market/time' \
    '"retCode":0'
probe_get \
    aster \
    'https://fapi.asterdex-testnet.com/fapi/v1/time' \
    'serverTime'
probe_post \
    trade_xyz \
    'https://api.hyperliquid-testnet.xyz/info' \
    '{"type":"allMids"}' \
    '{'
