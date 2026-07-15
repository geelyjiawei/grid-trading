#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PRODUCTION_COMPOSE:-docker-compose.rust-production.yml}
production_project=${GRID_RUST_PRODUCTION_PROJECT:-grid-trading-rust-production}
production_container=${GRID_RUST_PRODUCTION_CONTAINER:-grid-trading-rust}
production_data=${GRID_RUST_PRODUCTION_DATA:-$repository_root/data-rust-production}
legacy_root=${GRID_LEGACY_ROOT:-/opt/grid-trading}
legacy_container=${GRID_LEGACY_CONTAINER:-grid-trading}
preflight_project=${GRID_RUST_PREFLIGHT_PROJECT:-grid-trading-rust-preflight}
preflight_container=${GRID_RUST_PREFLIGHT_CONTAINER:-grid-trading-rust-preflight}
preflight_port=${GRID_RUST_PREFLIGHT_PORT:-18001}
audit_script=$repository_root/scripts/legacy-cutover-audit.py
verify_script=$repository_root/scripts/verify-rust-production.sh
login_verify_script=$repository_root/scripts/verify-rust-login.py
credentials_file=${GRID_RUST_CREDENTIALS_FILE:-$HOME/.grid-trading/rust-admin.txt}

fail() {
    printf 'Rust production deployment failed: %s\n' "$1" >&2
    exit 1
}

require_env_value() {
    key=$1
    value=$(awk -v key="$key" '
        index($0, key "=") == 1 { value=substr($0, length(key) + 2) }
        END { print value }
    ' .env)
    test -n "$value" || fail "$key must be configured in .env"
}

command -v awk >/dev/null 2>&1 || fail "awk is unavailable"
command -v cmp >/dev/null 2>&1 || fail "cmp is unavailable"
command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
command -v git >/dev/null 2>&1 || fail "git is unavailable"
command -v mktemp >/dev/null 2>&1 || fail "mktemp is unavailable"
test -f .env || fail ".env is required and must remain outside Git"
test -f "$compose_file" || fail "$compose_file does not exist"
test -f "$audit_script" || fail "$audit_script does not exist"
test -f "$verify_script" || fail "$verify_script does not exist"
test -f "$login_verify_script" || fail "$login_verify_script does not exist"

if git ls-files --error-unmatch .env >/dev/null 2>&1; then
    fail ".env is tracked by Git"
fi
git check-ignore .env >/dev/null 2>&1 || fail ".env is not protected by .gitignore"
grep -E '^AUTH_REQUIRED=true$' .env >/dev/null \
    || fail "AUTH_REQUIRED=true is mandatory for Rust production"
require_env_value ADMIN_USERNAME
require_env_value ADMIN_PASSWORD_HASH
require_env_value TOTP_SECRET
require_env_value GRID_CONFIG_KEY
grep -E "^ADMIN_PASSWORD_HASH='pbkdf2_sha256\\\$[0-9]+\\\$[^']+\\\$[^']+'$" .env >/dev/null \
    || fail "ADMIN_PASSWORD_HASH must be a single-quoted PBKDF2 value; run provision-rust-auth.py"

umask 077
rendered_compose=$(mktemp)
cleanup_rendered() {
    rm -f "$rendered_compose"
}
trap cleanup_rendered EXIT HUP INT TERM
GRID_RUST_PRODUCTION_DATA=$production_data \
GRID_RUST_PRODUCTION_PORT=8000 \
GRID_RUST_PRODUCTION_CONTAINER=$production_container \
    docker compose --project-name "$production_project" -f "$compose_file" config \
    >"$rendered_compose"
grep -F 'GRID_RUST_TRADING_ENABLED: "true"' "$rendered_compose" >/dev/null \
    || fail "production Compose does not force trading writes on"
grep -F 'host_ip: 127.0.0.1' "$rendered_compose" >/dev/null \
    || fail "production Compose is not bound to localhost"
grep -F 'published: "8000"' "$rendered_compose" >/dev/null \
    || fail "production Compose does not publish the expected port"
grep -F 'target: 8000' "$rendered_compose" >/dev/null \
    || fail "production Compose does not map the Rust service port"
grep -F 'target: /app/data' "$rendered_compose" >/dev/null \
    || fail "production Compose does not mount durable data"

if test "${GRID_RUST_PRODUCTION_VALIDATE_ONLY:-false}" = "true"; then
    printf 'Rust production deployment guards validated; no container or data was changed.\n'
    exit 0
fi

test "${GRID_RUST_CUTOVER_CONFIRM:-}" = "rust-replaces-python" \
    || fail "set GRID_RUST_CUTOVER_CONFIRM=rust-replaces-python for an explicit cutover"
expected_commit=${GRID_RUST_PRODUCTION_EXPECTED_COMMIT:-}
test -n "$expected_commit" || fail "GRID_RUST_PRODUCTION_EXPECTED_COMMIT is required"
expected_commit=$(git rev-parse --verify "${expected_commit}^{commit}" 2>/dev/null) \
    || fail "the expected Git commit is unavailable in this checkout"
actual_commit=$(git rev-parse --verify HEAD) \
    || fail "the current Git commit cannot be resolved"
test "$actual_commit" = "$expected_commit" \
    || fail "checkout $actual_commit does not match expected commit $expected_commit"
worktree_status=$(git status --porcelain --untracked-files=all) \
    || fail "the worktree status cannot be inspected"
test -z "$worktree_status" || fail "the production worktree contains uncommitted source files"
test -f "$credentials_file" \
    || fail "one-time credentials file is required for login proof: $credentials_file"

test -d "$legacy_root" || fail "legacy root does not exist: $legacy_root"
test -f "$legacy_root/.env" || fail "legacy .env does not exist"
test -d "$legacy_root/data" || fail "legacy data directory does not exist"
test "$(docker inspect -f '{{.State.Running}}' "$legacy_container" 2>/dev/null || true)" = "true" \
    || fail "legacy container is not running; the guarded cutover requires a known rollback source"
test -z "$(docker ps -q --filter name="^/${production_container}$")" \
    || fail "Rust production container already exists; this script is only for the initial cutover"

temporary_directory=$(mktemp -d)
state_audit=$temporary_directory/legacy-state.json
before_exposure=$temporary_directory/exposure-before.json
pre_cutover_exposure=$temporary_directory/exposure-pre-cutover.json
after_exposure=$temporary_directory/exposure-after.json

cleanup_temporary() {
    rm -rf "$temporary_directory"
    cleanup_rendered
}
trap cleanup_temporary EXIT HUP INT TERM

if ! docker exec -i "$legacy_container" python - state \
    <"$audit_script" >"$state_audit"; then
    cat "$state_audit" >&2
    fail "legacy strategy ownership is not clean"
fi
if ! docker exec -i "$legacy_container" python - exposure \
    <"$audit_script" >"$before_exposure"; then
    cat "$before_exposure" >&2
    fail "legacy exchange exposure could not be snapshotted"
fi

mkdir -p "$production_data/rust-control/idempotency" "$production_data/rust-control/strategies"
if test -f "$legacy_root/data/api_config.json"; then
    if test -f "$production_data/api_config.json"; then
        cmp -s "$legacy_root/data/api_config.json" "$production_data/api_config.json" \
            || fail "existing Rust API configuration differs from the legacy encrypted file"
    else
        cp "$legacy_root/data/api_config.json" "$production_data/api_config.json"
    fi
fi
chmod 700 "$production_data" "$production_data/rust-control" \
    "$production_data/rust-control/idempotency" "$production_data/rust-control/strategies"
test ! -f "$production_data/api_config.json" || chmod 600 "$production_data/api_config.json"
chown -R 10001:10001 "$production_data"

image="grid-trading-vue-rust:$expected_commit"
export GRID_RUST_IMAGE=$image
export GRID_RUST_PRODUCTION_EXPECTED_COMMIT=$expected_commit
export GRID_RUST_PRODUCTION_DATA=$production_data

printf 'Building Rust production commit %s while the legacy service remains online.\n' \
    "$expected_commit"
docker compose --project-name "$production_project" -f "$compose_file" build

preflight_started=false
production_started=false
legacy_stopped=false
rollback_on_failure() {
    status=$?
    trap - EXIT HUP INT TERM
    if test "$preflight_started" = "true"; then
        GRID_RUST_PRODUCTION_PORT=$preflight_port \
        GRID_RUST_PRODUCTION_CONTAINER=$preflight_container \
            docker compose --project-name "$preflight_project" -f "$compose_file" down \
            --remove-orphans >/dev/null 2>&1 || true
    fi
    if test "$production_started" = "true"; then
        GRID_RUST_PRODUCTION_PORT=8000 \
        GRID_RUST_PRODUCTION_CONTAINER=$production_container \
            docker compose --project-name "$production_project" -f "$compose_file" down \
            --remove-orphans >/dev/null 2>&1 || true
    fi
    if test "$legacy_stopped" = "true"; then
        docker start "$legacy_container" >/dev/null 2>&1 || true
    fi
    cleanup_temporary
    exit "$status"
}
trap rollback_on_failure EXIT HUP INT TERM

printf 'Starting a localhost-only Rust preflight on port %s.\n' "$preflight_port"
GRID_RUST_PRODUCTION_PORT=$preflight_port \
GRID_RUST_PRODUCTION_CONTAINER=$preflight_container \
    docker compose --project-name "$preflight_project" -f "$compose_file" up \
    -d --no-build --remove-orphans
preflight_started=true
GRID_RUST_PRODUCTION_PROJECT=$preflight_project \
GRID_RUST_PRODUCTION_CONTAINER=$preflight_container \
GRID_RUST_PRODUCTION_PORT=$preflight_port \
GRID_RUST_PRODUCTION_URL=http://127.0.0.1:$preflight_port \
GRID_RUST_PRODUCTION_EXPECTED_BINDING=127.0.0.1:$preflight_port \
GRID_RUST_EXPECT_LEGACY_STATE=running \
    sh "$verify_script"
python3 "$login_verify_script" \
    --credentials-file "$credentials_file" \
    --base-url "http://127.0.0.1:$preflight_port"

if ! docker exec -i "$legacy_container" python - exposure \
    <"$audit_script" >"$pre_cutover_exposure"; then
    cat "$pre_cutover_exposure" >&2
    fail "exchange exposure could not be checked after Rust preflight"
fi
cmp -s "$before_exposure" "$pre_cutover_exposure" \
    || fail "exchange exposure changed during Rust preflight"

GRID_RUST_PRODUCTION_PORT=$preflight_port \
GRID_RUST_PRODUCTION_CONTAINER=$preflight_container \
    docker compose --project-name "$preflight_project" -f "$compose_file" down \
    --remove-orphans
preflight_started=false

if ! docker exec -i "$legacy_container" python - state \
    <"$audit_script" >"$state_audit"; then
    cat "$state_audit" >&2
    fail "legacy strategy state changed while the candidate was built"
fi
if ! docker exec -i "$legacy_container" python - exposure \
    <"$audit_script" >"$pre_cutover_exposure"; then
    cat "$pre_cutover_exposure" >&2
    fail "final pre-cutover exposure snapshot failed"
fi

backup_root=$repository_root/release-backups/$(date -u +%Y%m%dT%H%M%SZ)-$expected_commit
mkdir -p "$backup_root"
chmod 700 "$repository_root/release-backups" "$backup_root"
cp "$state_audit" "$backup_root/legacy-state.json"
cp "$pre_cutover_exposure" "$backup_root/exposure-before.json"
docker inspect "$legacy_container" >"$backup_root/legacy-container.json"
git -C "$legacy_root" rev-parse HEAD >"$backup_root/legacy-git-head.txt"
printf '%s\n' "$expected_commit" >"$backup_root/rust-git-head.txt"

legacy_image=$(docker inspect -f '{{.Config.Image}}' "$legacy_container")
printf 'Stopping legacy container only after the Rust preflight passed.\n'
docker stop --time 30 "$legacy_container" >/dev/null
legacy_stopped=true

GRID_RUST_PRODUCTION_PORT=8000 \
GRID_RUST_PRODUCTION_CONTAINER=$production_container \
    docker compose --project-name "$production_project" -f "$compose_file" up \
    -d --no-build --remove-orphans
production_started=true
GRID_RUST_PRODUCTION_PROJECT=$production_project \
GRID_RUST_PRODUCTION_CONTAINER=$production_container \
GRID_RUST_PRODUCTION_PORT=8000 \
GRID_RUST_PRODUCTION_URL=http://127.0.0.1:8000 \
GRID_RUST_PRODUCTION_EXPECTED_BINDING=127.0.0.1:8000 \
GRID_RUST_EXPECT_LEGACY_STATE=stopped \
    sh "$verify_script"
python3 "$login_verify_script" \
    --credentials-file "$credentials_file" \
    --base-url http://127.0.0.1:8000

if ! docker run --rm -i \
    --env-file "$legacy_root/.env" \
    -e GRID_CONFIG_FILE=/app/data/api_config.json \
    -e GRID_STATE_FILE=/tmp/grid-state-read-only.json \
    -e GRID_HISTORY_FILE=/tmp/grid-history-read-only.json \
    --mount "type=bind,src=$legacy_root/data,dst=/app/data,readonly" \
    "$legacy_image" python - exposure <"$audit_script" >"$after_exposure"; then
    cat "$after_exposure" >&2
    fail "post-cutover exchange exposure snapshot failed"
fi
cmp -s "$pre_cutover_exposure" "$after_exposure" \
    || fail "positions or open orders changed during the production cutover"
cp "$after_exposure" "$backup_root/exposure-after.json"

trap - EXIT HUP INT TERM
cleanup_temporary
printf 'Rust production cutover succeeded at commit %s.\n' "$expected_commit"
printf 'Legacy container %s remains stopped for rollback.\n' "$legacy_container"
printf 'Cutover evidence: %s\n' "$backup_root"
