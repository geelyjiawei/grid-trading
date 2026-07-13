# Vue + Rust migration

## Required production architecture

- Frontend: Vue 3 + TypeScript from `vue-frontend/`.
- Backend: Rust + Axum from `rust-backend/`.
- The final production image must not contain Python, Node.js, `backend/`, or `frontend/`.
- Exchange writes stay disabled until read-only shadow audits and cutover checks pass.

## Current safety boundary

The existing `Dockerfile` and `docker-compose.yml` still describe the legacy
production service. They are intentionally unchanged so this migration branch
cannot replace a running trading process by accident.

The candidate stack is isolated:

```text
Dockerfile.rust-vue
docker-compose.rust-vue.yml
127.0.0.1:8001
data-rust-preview/
```

It builds Vue in a Node build stage, compiles the Rust server in a Rust build
stage, and copies only the Vue assets and Rust binary into the runtime image.
The Rust server serves both `/healthz` and the Vue static files. Unknown API
routes are never rewritten to the Vue index. They return `404` when web
authentication is disabled or a valid session is present; otherwise they fail
closed with `401` or `503`.

The Rust backend now owns `/api/auth/status`, `/api/auth/login`, and
`/api/auth/logout`. It accepts the existing `pbkdf2_sha256` password hashes and
six-digit TOTP setup, but it does not copy the legacy signed-cookie session
format. Successful logins receive a 12-hour opaque random cookie; Rust stores
only the SHA-256 token digest in bounded process memory. Logout, expiry, or a
Rust restart revokes the session. `SESSION_SECRET` remains a legacy Python-only
setting during migration and is not used by Rust.

The preview stack can be built after creating a local `.env` file:

```bash
docker compose -f docker-compose.rust-vue.yml up -d --build
```

Do not expose or deploy this preview as the trading service. The Rust binary
currently refuses to start when `GRID_RUST_TRADING_ENABLED=true`.

## Cutover gates

The legacy production entrypoint may be replaced only when all of these are
true:

1. Every Vue API dependency has a tested Rust implementation.
2. Authentication, encrypted exchange configuration, status, preview, start,
   stop, history, orders, trades, positions, and risk views have contract tests.
3. Restart and crash recovery tests prove no duplicate or missing orders.
4. Consecutive read-only shadow samples show exact order and position parity.
5. The candidate image passes the GitHub container build and smoke test.
6. A production backup, rollback command, and explicit deployment approval are
   recorded before any server switch.
