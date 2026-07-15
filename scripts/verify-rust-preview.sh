#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PREVIEW_COMPOSE:-docker-compose.rust-vue.yml}
base_url=${GRID_RUST_PREVIEW_URL:-http://127.0.0.1:8001}
preview_project=${GRID_RUST_PREVIEW_PROJECT:-grid-trading-rust-preview}
expected_binding=${GRID_RUST_PREVIEW_EXPECTED_BINDING:-127.0.0.1:8001}
expected_commit=${GRID_RUST_PREVIEW_EXPECTED_COMMIT:-}
service_name=grid-trading-rust-preview

fail() {
    printf 'preview verification failed: %s\n' "$1" >&2
    exit 1
}

require_text() {
    file=$1
    expected=$2
    grep -F "$expected" "$file" >/dev/null || fail "missing expected response: $expected"
}

command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
test -f "$compose_file" || fail "$compose_file does not exist"

if test -n "$expected_commit"; then
    command -v git >/dev/null 2>&1 || fail "git is unavailable"
    expected_commit=$(git rev-parse --verify "${expected_commit}^{commit}" 2>/dev/null) \
        || fail "the expected Git commit is unavailable in this checkout"
    actual_commit=$(git rev-parse --verify HEAD) \
        || fail "the current Git commit cannot be resolved"
    test "$actual_commit" = "$expected_commit" \
        || fail "current Git commit $actual_commit does not match expected commit $expected_commit"
fi

container_id=${GRID_RUST_PREVIEW_CONTAINER_ID:-}
if test -z "$container_id"; then
    container_id=$(docker compose --project-name "$preview_project" -f "$compose_file" ps -q "$service_name")
fi
test -n "$container_id" || fail "candidate container is not running"
test "$(docker inspect -f '{{.State.Running}}' "$container_id")" = "true" \
    || fail "candidate container is not running"

published_port=
binding_attempt=1
while test "$binding_attempt" -le 30; do
    container_state=$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)
    test "$container_state" = "running" \
        || fail "candidate container left the running state before binding (state: ${container_state:-missing})"
    published_port=$(docker port "$container_id" 8000/tcp 2>/dev/null || true)
    if test -n "$published_port"; then
        break
    fi
    sleep 1
    binding_attempt=$((binding_attempt + 1))
done
test "$published_port" = "$expected_binding" \
    || fail "candidate binding mismatch (expected: $expected_binding, observed: ${published_port:-none})"

docker exec "$container_id" sh -eu -c '
    test "$(id -u)" = "10001"
    test -x /usr/local/bin/grid-trading-server
    test -f /app/web/index.html
    test -d /app/data/rust-control/idempotency
    test -d /app/data/rust-control/strategies
    test -w /app/data
    test -w /app/data/rust-control/idempotency
    test -w /app/data/rust-control/strategies
    probe=/app/data/.preview-write-probe-$$
    umask 077
    : >"$probe"
    rm -f "$probe"
    ! command -v python >/dev/null 2>&1
    ! command -v node >/dev/null 2>&1
' || fail "candidate image composition is unsafe"

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

health_file=$temporary_directory/health.json
ready=false
attempt=1
while test "$attempt" -le 60; do
    if curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/healthz" >"$health_file"; then
        ready=true
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
test "$ready" = "true" || fail "candidate health endpoint did not become ready"
require_text "$health_file" '"ok":true'
require_text "$health_file" '"runtime":"rust"'
require_text "$health_file" '"trading_enabled":false'
require_text "$health_file" '"runtime_ready":true'
require_text "$health_file" '"active_strategies":0'

auth_file=$temporary_directory/auth.json
curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/api/auth/status" >"$auth_file" \
    || fail "authentication status is unavailable"

index_file=$temporary_directory/index.html
curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/" >"$index_file" \
    || fail "Vue entrypoint is unavailable"
require_text "$index_file" '<div id="app"></div>'

if grep -F '"required":true' "$auth_file" >/dev/null; then
    require_text "$auth_file" '"configured":true'
    protected_file=$temporary_directory/protected.json
    protected_status=$(curl --noproxy '*' --silent --show-error --max-time 3 \
        --output "$protected_file" --write-out '%{http_code}' "$base_url/api/grid/status")
    test "$protected_status" = "401" || fail "protected API did not reject an anonymous request"
    require_text "$protected_file" '"code":"authentication_required"'
    auth_mode=required
else
    require_text "$auth_file" '"required":false'
    require_text "$auth_file" '"authenticated":true'
    config_file=$temporary_directory/config.json
    status_file=$temporary_directory/status.json
    history_file=$temporary_directory/history.json
    trades_file=$temporary_directory/trades.json
    missing_file=$temporary_directory/missing.json
    curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/api/config" >"$config_file" \
        || fail "exchange configuration status is unavailable"
    curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/api/grid/status" >"$status_file" \
        || fail "strategy status is unavailable"
    curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/api/grid/history?limit=100" >"$history_file" \
        || fail "strategy history is unavailable"
    curl --noproxy '*' --fail --silent --show-error --max-time 3 \
        "$base_url/api/trades/MUUSDT?exchange=binance&limit=100" >"$trades_file" \
        || fail "strategy trade audit is unavailable"
    require_text "$status_file" '"trading_enabled":false'
    require_text "$history_file" '"source":"durable_strategy_state"'
    require_text "$trades_file" '"source":"durable_exchange_execution_audit"'
    missing_status=$(curl --noproxy '*' --silent --show-error --max-time 3 \
        --output "$missing_file" --write-out '%{http_code}' "$base_url/api/not-a-real-route")
    test "$missing_status" = "404" || fail "unknown API route did not return 404"
    require_text "$missing_file" '"code":"api_route_not_found"'
    auth_mode=disabled
fi

printf 'Rust preview verified: container=%s commit=%s url=%s auth=%s trading_enabled=false\n' \
    "$container_id" "${expected_commit:-unchecked}" "$base_url" "$auth_mode"
