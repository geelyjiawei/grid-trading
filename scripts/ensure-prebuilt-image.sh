#!/bin/sh

set -eu

fail() {
    printf 'prebuilt image verification failed: %s\n' "$1" >&2
    exit 1
}

test "$#" -ge 2 || fail "usage: ensure-prebuilt-image.sh IMAGE EXPECTED_COMMIT [ARCHIVE]"

image=$1
expected_commit=$2
archive=${3:-}

command -v docker >/dev/null 2>&1 || fail "docker is unavailable"

if test -n "$archive"; then
    test -f "$archive" || fail "image archive does not exist: $archive"
    printf 'Loading verified image archive %s.\n' "$archive"
    docker load --input "$archive" >/dev/null \
        || fail "could not load image archive"
fi

if ! docker image inspect "$image" >/dev/null 2>&1; then
    printf 'Pulling immutable image %s.\n' "$image"
    docker pull "$image" >/dev/null \
        || fail "image is neither loaded locally nor pullable: $image"
fi

revision=$(docker image inspect --format \
    '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image" 2>/dev/null) \
    || fail "image metadata is unavailable: $image"

test -n "$revision" && test "$revision" != '<no value>' \
    || fail "image has no immutable Git revision label"
test "$revision" = "$expected_commit" \
    || fail "image revision $revision does not match expected commit $expected_commit"

printf 'Prebuilt image verified: image=%s commit=%s\n' "$image" "$expected_commit"
