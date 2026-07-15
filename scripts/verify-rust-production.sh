#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PRODUCTION_COMPOSE:-docker-compose.rust-production.yml}
project=${GRID_RUST_PRODUCTION_PROJECT:-grid-trading-rust-production}
service_name=grid-trading-rust
container_name=${GRID_RUST_PRODUCTION_CONTAINER:-grid-trading-rust}
port=${GRID_RUST_PRODUCTION_PORT:-8000}
base_url=${GRID_RUST_PRODUCTION_URL:-http://127.0.0.1:$port}
expected_binding=${GRID_RUST_PRODUCTION_EXPECTED_BINDING:-127.0.0.1:$port}
expected_commit=${GRID_RUST_PRODUCTION_EXPECTED_COMMIT:-}
expected_active=${GRID_RUST_EXPECT_ACTIVE_STRATEGIES:-0}
expected_legacy_state=${GRID_RUST_EXPECT_LEGACY_STATE:-stopped}
legacy_container=${GRID_LEGACY_CONTAINER:-grid-trading}
image=${GRID_RUST_IMAGE:-grid-trading-vue-rust:production}

fail() {
    printf 'Rust production verification failed: %s\n' "$1" >&2
    exit 1
}

require_text() {
    file=$1
    expected=$2
    grep -F "$expected" "$file" >/dev/null || fail "missing expected response: $expected"
}

command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
test -f "$compose_file" || fail "$compose_file does not exist"

if test -n "$expected_commit"; then
    command -v git >/dev/null 2>&1 || fail "git is unavailable"
    expected_commit=$(git rev-parse --verify "${expected_commit}^{commit}" 2>/dev/null) \
        || fail "the expected Git commit is unavailable"
    actual_commit=$(git rev-parse --verify HEAD) \
        || fail "the current Git commit cannot be resolved"
    test "$actual_commit" = "$expected_commit" \
        || fail "checkout $actual_commit does not match expected commit $expected_commit"
fi

container_id=$(docker compose --project-name "$project" -f "$compose_file" ps -q "$service_name")
test -n "$container_id" || fail "Rust production container is not running"
test "$(docker inspect -f '{{.State.Running}}' "$container_id")" = "true" \
    || fail "Rust production container is not running"
test "$(docker inspect -f '{{.Name}}' "$container_id")" = "/$container_name" \
    || fail "unexpected Rust production container name"

expected_image_id=$(docker image inspect -f '{{.Id}}' "$image" 2>/dev/null) \
    || fail "expected image does not exist: $image"
actual_image_id=$(docker inspect -f '{{.Image}}' "$container_id")
test "$actual_image_id" = "$expected_image_id" \
    || fail "running container does not use the expected image"

published_port=
attempt=1
while test "$attempt" -le 30; do
    published_port=$(docker port "$container_id" 8000/tcp 2>/dev/null || true)
    if test -n "$published_port"; then
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
test "$published_port" = "$expected_binding" \
    || fail "binding mismatch (expected $expected_binding, observed ${published_port:-none})"

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM
health_file=$temporary_directory/health.json
ready=false
attempt=1
while test "$attempt" -le 60; do
    if curl --noproxy '*' --fail --silent --show-error --max-time 3 \
        "$base_url/healthz" >"$health_file"; then
        ready=true
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
test "$ready" = "true" || fail "health endpoint did not become ready"
require_text "$health_file" '"ok":true'
require_text "$health_file" '"runtime":"rust"'
require_text "$health_file" '"trading_enabled":true'
require_text "$health_file" '"runtime_ready":true'
require_text "$health_file" "\"active_strategies\":$expected_active"

auth_file=$temporary_directory/auth.json
curl --noproxy '*' --fail --silent --show-error --max-time 3 \
    "$base_url/api/auth/status" >"$auth_file" \
    || fail "authentication status is unavailable"
require_text "$auth_file" '"required":true'
require_text "$auth_file" '"configured":true'
require_text "$auth_file" '"authenticated":false'

protected_file=$temporary_directory/protected.json
protected_status=$(curl --noproxy '*' --silent --show-error --max-time 3 \
    --output "$protected_file" --write-out '%{http_code}' "$base_url/api/grid/status")
test "$protected_status" = "401" || fail "anonymous strategy API request was not rejected"
require_text "$protected_file" '"code":"authentication_required"'

index_file=$temporary_directory/index.html
curl --noproxy '*' --fail --silent --show-error --max-time 3 "$base_url/" >"$index_file" \
    || fail "Vue entrypoint is unavailable"
require_text "$index_file" '<div id="app"></div>'

docker exec "$container_id" sh -eu -c '
    test "$(id -u)" = "10001"
    test -x /usr/local/bin/grid-trading-server
    test -f /app/web/index.html
    test -d /app/data/rust-control/idempotency
    test -d /app/data/rust-control/strategies
    test -w /app/data
    probe=/app/data/.production-write-probe-$$
    umask 077
    : >"$probe"
    rm -f "$probe"
    ! command -v python >/dev/null 2>&1
    ! command -v node >/dev/null 2>&1
' || fail "production image composition or data permissions are invalid"

test "$(docker inspect -f '{{.RestartCount}}' "$container_id")" = "0" \
    || fail "Rust production container restarted during verification"

case "$expected_legacy_state" in
    running)
        test "$(docker inspect -f '{{.State.Running}}' "$legacy_container" 2>/dev/null || true)" = "true" \
            || fail "legacy container was expected to remain running"
        ;;
    stopped)
        test "$(docker inspect -f '{{.State.Running}}' "$legacy_container" 2>/dev/null || true)" != "true" \
            || fail "legacy and Rust production containers are running together"
        ;;
    ignore)
        ;;
    *)
        fail "invalid GRID_RUST_EXPECT_LEGACY_STATE: $expected_legacy_state"
        ;;
esac

printf 'Rust production verified: container=%s commit=%s url=%s active_strategies=%s\n' \
    "$container_id" "${expected_commit:-unchecked}" "$base_url" "$expected_active"
