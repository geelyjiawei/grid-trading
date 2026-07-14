#!/bin/sh

set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

compose_file=${GRID_RUST_PREVIEW_COMPOSE:-docker-compose.rust-vue.yml}
preview_project=${GRID_RUST_PREVIEW_PROJECT:-grid-trading-rust-preview}

fail() {
    printf 'preview deployment failed: %s\n' "$1" >&2
    exit 1
}

command -v git >/dev/null 2>&1 || fail "git is unavailable"
command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
test -f .env || fail ".env is required and must remain outside Git"
test -f "$compose_file" || fail "$compose_file does not exist"

if git ls-files --error-unmatch .env >/dev/null 2>&1; then
    fail ".env is tracked by Git"
fi
git check-ignore .env >/dev/null 2>&1 || fail ".env is not protected by .gitignore"

docker compose --project-name "$preview_project" -f "$compose_file" config --quiet
rendered_compose=$(docker compose --project-name "$preview_project" -f "$compose_file" config)
printf '%s\n' "$rendered_compose" | grep -F 'GRID_RUST_TRADING_ENABLED: "false"' >/dev/null \
    || fail "candidate compose does not force trading writes off"
printf '%s\n' "$rendered_compose" | grep -F 'host_ip: 127.0.0.1' >/dev/null \
    || fail "candidate compose does not bind to localhost"
printf '%s\n' "$rendered_compose" | grep -F 'published: "8001"' >/dev/null \
    || fail "candidate compose does not publish port 8001"
printf '%s\n' "$rendered_compose" | grep -F 'target: 8000' >/dev/null \
    || fail "candidate compose does not map the Rust service port"

if test "${GRID_RUST_PREVIEW_VALIDATE_ONLY:-false}" = "true"; then
    printf 'Rust preview deployment guards validated; no container was changed.\n'
    exit 0
fi

production_ids_before=$(docker compose ps -q | sort)

docker compose --project-name "$preview_project" -f "$compose_file" up -d --build

production_ids_after=$(docker compose ps -q | sort)
test "$production_ids_before" = "$production_ids_after" \
    || fail "the production compose container set changed during candidate deployment"

sh "$repository_root/scripts/verify-rust-preview.sh"

printf 'Production containers were unchanged. Candidate remains isolated and read-only.\n'
