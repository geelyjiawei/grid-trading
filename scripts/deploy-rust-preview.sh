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

command -v cmp >/dev/null 2>&1 || fail "cmp is unavailable"
command -v mktemp >/dev/null 2>&1 || fail "mktemp is unavailable"

temporary_directory=$(mktemp -d)
non_preview_before=$temporary_directory/non-preview-before.txt
non_preview_after=$temporary_directory/non-preview-after.txt
cleanup() {
    rm -f "$non_preview_before" "$non_preview_after"
    rmdir "$temporary_directory" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

snapshot_non_preview_containers() {
    output_file=$1
    : >"$output_file"
    for container_id in $(docker ps --no-trunc --all --quiet); do
        compose_project=$(docker inspect --format \
            '{{if .Config.Labels}}{{index .Config.Labels "com.docker.compose.project"}}{{end}}' \
            "$container_id") || fail "could not inspect container $container_id"
        if test "$compose_project" = "$preview_project"; then
            continue
        fi
        docker inspect --format \
            '{{.Id}}|{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}' \
            "$container_id" >>"$output_file" \
            || fail "could not snapshot container $container_id"
    done
    sort "$output_file" -o "$output_file"
}

snapshot_non_preview_containers "$non_preview_before"

docker compose --project-name "$preview_project" -f "$compose_file" up -d --build

snapshot_non_preview_containers "$non_preview_after"
cmp -s "$non_preview_before" "$non_preview_after" \
    || fail "a non-preview container changed during candidate deployment"

sh "$repository_root/scripts/verify-rust-preview.sh"

printf 'All non-preview containers were unchanged. Candidate remains isolated and read-only.\n'
