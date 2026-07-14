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
rust-preview-data (Docker named volume)
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

The candidate stack can be built after creating a local `.env` file:

```bash
docker compose -f docker-compose.rust-vue.yml up -d --build
```

Keep it isolated on port `8001` until shadow verification passes. With
`GRID_RUST_TRADING_ENABLED=false`, all write endpoints fail closed. Setting it
to `true` is accepted only when web authentication is fully configured; startup
also performs durable runtime recovery and aborts on discovery anomalies or
unknown recovery outcomes.

The Rust candidate reads exchange credentials only from `.env` at process
startup. `/api/config` returns masked connection status and never returns a
secret. The Vue configuration dialog is intentionally read-only; changing a
credential requires updating the server `.env` and rebuilding or restarting
the candidate container. This avoids transmitting production private keys
through browser requests before encrypted dynamic configuration and safe
gateway hot-reload exist end to end.

Runtime status is attributed by exact strategy identity. A durable strategy and
an in-memory runtime must agree on exchange, symbol, run ID, armed/active kind,
and lifecycle. `/api/grid/status` fails closed when they disagree. `/api/risk`
continues to return a read-only exchange snapshot for diagnosis, but sets
`has_risk=true` and `runtime_state_error=runtime_catalog_mismatch`; it never
repairs, places, or cancels an order. When trading is intentionally disabled,
the absent runtime is reported as unconfigured rather than as a mismatch.

Order recovery preserves one immutable exchange order ID from acknowledgement
through terminal execution accounting. A terminal intent is durable evidence,
not just a status label: it retains the authoritative exchange order ID even if
the process crashes before strategy state is committed. Legacy terminal records
without that ID remain readable for migration, but the runtime must
authoritatively enrich them before it can account a fill or advance the grid.
Every tick reconciles intents and executions before position comparison or new
placement, so a crash cannot turn an accepted or filled order into a duplicate
submission or an unbooked position change. A temporary `NotFound` from the order
lookup cannot regress a terminal execution that has already passed complete trade
accounting. When client ID, exchange order ID, shape, and terminal status remain
exact, the runtime first converges the intent ledger to that accounted terminal
state and only then materializes the counter order. Any identity or terminal-status
conflict remains fail-closed.

Inventory accounting is independently replayable. Every positive execution
delta appends an immutable event containing its strategy order identity,
quantity, quote value, and application time. Before an active strategy can be
loaded, these events must exactly reproduce the opening allocation, every
per-level directional lot, neutral FIFO lots, net grid position, and gross
realized profit. Aggregate totals alone are never accepted as proof because
quantity and cost can otherwise drift between levels while the overall sum
still appears correct. A legacy state that already contains executions but no
complete event chain is retained for diagnosis and rejected rather than being
silently reconstructed from guesses.

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
