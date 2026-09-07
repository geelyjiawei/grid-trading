#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PRODUCTION_COMPOSE:-docker-compose.rust-production.yml}
production_container=${GRID_RUST_PRODUCTION_CONTAINER:-grid-trading-rust}
production_data=${GRID_RUST_PRODUCTION_DATA:-$repository_root/data-rust-production}
production_port=${GRID_RUST_PRODUCTION_PORT:-8000}
preflight_container=${GRID_RUST_RELEASE_PREFLIGHT_CONTAINER:-grid-trading-rust-release-preflight}
preflight_port=${GRID_RUST_RELEASE_PREFLIGHT_PORT:-18001}
image_verify_script=$repository_root/scripts/ensure-prebuilt-image.sh

fail() {
    printf 'Rust release deployment failed: %s\n' "$1" >&2
    exit 1
}

for command_name in awk curl docker flock git mktemp; do
    command -v "$command_name" >/dev/null 2>&1 \
        || fail "$command_name is unavailable"
done
test -f .env || fail ".env is required and must remain outside Git"
test -f "$compose_file" || fail "$compose_file does not exist"
test -f "$image_verify_script" || fail "$image_verify_script does not exist"

rendered_compose=$(mktemp)
temporary_directory=
preflight_started=false
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if test "$preflight_started" = true; then
        docker rm --force "$preflight_container" >/dev/null 2>&1 || true
    fi
    rm -f "$rendered_compose"
    if test -n "$temporary_directory"; then
        rm -rf "$temporary_directory"
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

GRID_RUST_IMAGE=${GRID_RUST_IMAGE:-grid-trading-vue-rust:production} \
GRID_RUST_PRODUCTION_DATA=$production_data \
GRID_RUST_PRODUCTION_PORT=$production_port \
GRID_RUST_PRODUCTION_CONTAINER=$production_container \
    docker compose --file "$compose_file" config >"$rendered_compose"
if grep -E '^ *build:' "$rendered_compose" >/dev/null; then
    fail "production Compose permits an on-host image build"
fi
grep -F 'GRID_RUST_TRADING_ENABLED: "true"' "$rendered_compose" >/dev/null \
    || fail "production Compose does not force trading writes on"
grep -F 'host_ip: 127.0.0.1' "$rendered_compose" >/dev/null \
    || fail "production Compose is not bound to localhost"

if test "${GRID_RUST_RELEASE_VALIDATE_ONLY:-false}" = true; then
    printf 'Rust release deployment guards validated; no container was changed.\n'
    exit 0
fi

expected_commit=${GRID_RUST_PRODUCTION_EXPECTED_COMMIT:-}
image=${GRID_RUST_IMAGE:-}
image_archive=${GRID_RUST_IMAGE_ARCHIVE:-}
test -n "$expected_commit" || fail "GRID_RUST_PRODUCTION_EXPECTED_COMMIT is required"
test -n "$image" || fail "GRID_RUST_IMAGE must name a prebuilt immutable image"
expected_commit=$(git rev-parse --verify "${expected_commit}^{commit}" 2>/dev/null) \
    || fail "the expected Git commit is unavailable in this checkout"
test "$(git rev-parse --verify HEAD)" = "$expected_commit" \
    || fail "the checkout does not match the expected commit"
test -z "$(git status --porcelain --untracked-files=all)" \
    || fail "the production worktree contains uncommitted source files"

exec 9>/run/lock/grid-trading-deploy.lock
flock -n 9 || fail "another grid deployment is already running"

available_mib=$(( $(awk '/^MemAvailable:/{print $2}' /proc/meminfo) / 1024 ))
test "$available_mib" -ge 1024 \
    || fail "host has less than 1024 MiB available memory"
available_disk_kib=$(df -Pk / | awk 'NR == 2 { print $4 }')
test "$available_disk_kib" -ge 5242880 \
    || fail "host has less than 5 GiB free disk space"

test "$(docker inspect -f '{{.State.Running}}' "$production_container" 2>/dev/null || true)" = true \
    || fail "production container is not running"
test "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$production_container")" = healthy \
    || fail "production container is not healthy"

health=$(curl --fail --silent --show-error \
    "http://127.0.0.1:$production_port/healthz") \
    || fail "production health endpoint is unavailable"
printf '%s' "$health" | grep -F '"active_strategies":0' >/dev/null \
    || fail "all strategies must be stopped before a release deployment"

sh "$image_verify_script" "$image" "$expected_commit" "$image_archive"

temporary_directory=$(mktemp -d)
preflight_data=$temporary_directory/data
mkdir -p "$preflight_data/rust-control/idempotency" \
    "$preflight_data/rust-control/strategies"
chown -R 10001:10001 "$preflight_data"

docker rm --force "$preflight_container" >/dev/null 2>&1 || true
docker run --detach \
    --name "$preflight_container" \
    --env-file .env \
    --env GRID_BIND=0.0.0.0:8000 \
    --env GRID_WEB_ROOT=/app/web \
    --env GRID_CONFIG_FILE=/app/data/api_config.json \
    --env GRID_RUST_CONTROL_ROOT=/app/data/rust-control/idempotency \
    --env GRID_RUST_STRATEGY_ROOT=/app/data/rust-control/strategies \
    --env GRID_RUST_TRADING_ENABLED=false \
    --publish "127.0.0.1:$preflight_port:8000" \
    --mount "type=bind,src=$preflight_data,dst=/app/data" \
    --read-only \
    --tmpfs /tmp:size=16m,mode=1777 \
    --cap-drop ALL \
    --security-opt no-new-privileges:true \
    --memory 512m \
    --memory-swap 512m \
    --cpus 0.5 \
    --pids-limit 128 \
    "$image" >/dev/null
preflight_started=true

attempt=0
while test "$attempt" -lt 30; do
    candidate_health=$(curl --fail --silent \
        "http://127.0.0.1:$preflight_port/healthz" 2>/dev/null || true)
    if printf '%s' "$candidate_health" | grep -F '"ok":true' >/dev/null \
        && printf '%s' "$candidate_health" | grep -F '"trading_enabled":false' >/dev/null; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
test "$attempt" -lt 30 || fail "isolated candidate did not become healthy"
docker rm --force "$preflight_container" >/dev/null
preflight_started=false

old_image=$(docker inspect -f '{{.Config.Image}}' "$production_container")
production_project=${GRID_RUST_PRODUCTION_PROJECT:-$(docker inspect -f \
    '{{index .Config.Labels "com.docker.compose.project"}}' "$production_container")}
test -n "$production_project" && test "$production_project" != '<no value>' \
    || fail "production Compose project could not be determined"

rollback() {
    status=$?
    trap - EXIT HUP INT TERM
    if test "$status" -ne 0; then
        printf 'Candidate failed; restoring image %s.\n' "$old_image" >&2
        GRID_RUST_IMAGE=$old_image \
        GRID_RUST_PRODUCTION_DATA=$production_data \
        GRID_RUST_PRODUCTION_PORT=$production_port \
        GRID_RUST_PRODUCTION_CONTAINER=$production_container \
            docker compose --project-name "$production_project" \
            --file "$compose_file" up --detach --no-build --force-recreate \
            >/dev/null 2>&1 || true
    fi
    rm -f "$rendered_compose"
    rm -rf "$temporary_directory"
    exit "$status"
}
trap rollback EXIT HUP INT TERM

GRID_RUST_IMAGE=$image \
GRID_RUST_PRODUCTION_DATA=$production_data \
GRID_RUST_PRODUCTION_PORT=$production_port \
GRID_RUST_PRODUCTION_CONTAINER=$production_container \
    docker compose --project-name "$production_project" --file "$compose_file" \
    up --detach --no-build --force-recreate

attempt=0
while test "$attempt" -lt 30; do
    if curl --fail --silent "http://127.0.0.1:$production_port/healthz" \
        | grep -F '"ok":true' >/dev/null 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
test "$attempt" -lt 30 || fail "updated production container did not become healthy"
test "$(docker inspect -f '{{.RestartCount}}' "$production_container")" = 0 \
    || fail "updated production container restarted during verification"
test "$(docker inspect -f '{{.Image}}' "$production_container")" = \
    "$(docker image inspect -f '{{.Id}}' "$image")" \
    || fail "production container does not use the verified image"

trap - EXIT HUP INT TERM
rm -f "$rendered_compose"
rm -rf "$temporary_directory"
printf 'Rust production updated from %s to %s at commit %s.\n' \
    "$old_image" "$image" "$expected_commit"
