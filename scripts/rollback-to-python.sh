#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PRODUCTION_COMPOSE:-docker-compose.rust-production.yml}
production_project=${GRID_RUST_PRODUCTION_PROJECT:-grid-trading-rust-production}
production_container=${GRID_RUST_PRODUCTION_CONTAINER:-grid-trading-rust}
legacy_container=${GRID_LEGACY_CONTAINER:-grid-trading}

fail() {
    printf 'Rust rollback failed: %s\n' "$1" >&2
    exit 1
}

test "${GRID_RUST_ROLLBACK_CONFIRM:-}" = "stop-rust-start-python" \
    || fail "set GRID_RUST_ROLLBACK_CONFIRM=stop-rust-start-python"
command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
test -n "$(docker ps -q --filter name="^/${production_container}$")" \
    || fail "Rust production container is not running"
test -n "$(docker ps -aq --filter name="^/${legacy_container}$")" \
    || fail "legacy Python container is unavailable"

health=$(mktemp)
trap 'rm -f "$health"' EXIT HUP INT TERM
curl --noproxy '*' --fail --silent --show-error --max-time 5 \
    http://127.0.0.1:8000/healthz >"$health" \
    || fail "Rust health endpoint is unavailable"
grep -F '"runtime":"rust"' "$health" >/dev/null \
    || fail "port 8000 is not served by Rust"
grep -F '"active_strategies":0' "$health" >/dev/null \
    || fail "Rust still owns an active strategy; stop it and wait for terminal cleanup first"

docker compose --project-name "$production_project" -f "$compose_file" down --remove-orphans
docker start "$legacy_container" >/dev/null

ready=false
attempt=1
while test "$attempt" -le 60; do
    if curl --noproxy '*' --silent --show-error --max-time 3 \
        http://127.0.0.1:8000/api/auth/status >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
    attempt=$((attempt + 1))
done
test "$ready" = "true" || fail "legacy Python service did not recover"
printf 'Rollback completed: Rust is stopped and legacy container %s is running.\n' \
    "$legacy_container"
