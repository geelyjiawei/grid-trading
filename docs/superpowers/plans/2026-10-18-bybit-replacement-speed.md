# Bybit Replacement Recovery and Low-Latency Submission

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans; use TDD for each implementation task.

**Goal:** Recover Bybit fills that currently stall on exact-order lookup, reduce execution lock contention, and add official WebSocket order submission without weakening quantity or manual-position protection.

**Architecture:** Keep the existing Rust grid state machine and durable prepared-order ledger. Repair Bybit-specific evidence collection, use the existing prepare/place/complete split for Bybit, and prefer an authenticated persistent official trading WebSocket with fail-closed unknown outcomes.

**Tech Stack:** Rust, Tokio, rust_decimal, tokio-tungstenite, existing Vue frontend unchanged.

**Spec:** User-approved implementation request in this conversation, 2026-10-18. Before-change evidence is in the task workspace `.local/bybit-speed-20261018-confirmed.json`: 33 successful replacements, median 103 ms; two filled 500-Q orders remain unaccounted; the 480-ms sample waited 378 ms for the strategy mutex.

## Global Constraints

- Do not close, cancel, modify, adopt, or assign manual positions/orders to the strategy.
- Retain exact exchange/client identity, quantity, grid-capacity, reduce-only, and idempotency checks.
- A request possibly sent to WebSocket must never be replayed or retried through REST merely because its acknowledgement is missing.
- Keep production backend Rust; Python task helpers are observation/deployment tools only.
- Preserve existing unrelated dirty Vue files; do not include them in this release.
- No compilation on the resource-constrained production host; deploy the exact green-CI image with checksum and revision verification.

## Review Focus

- A valid pagination token is not a duplicate order; actual conflicting identities remain errors.
- Fills before placement acknowledgement or New events must not be silently lost or counted twice.
- Disconnects, stale generations, late full execution, and missing ACKs must recover without fabricated quantities/fees.
- Stop concurrent with unlocked placement must persist all sent outcomes and never submit new unreserved orders.
- Deployment must preserve current strategy ownership/baselines and manual account data; never restore stale ledgers.

## Tasks

### 1. Exact Lookup and Official WebSocket Submission

Files: `rust-backend/src/exchange/bybit.rs`, `bybit_codec.rs`, new `bybit/order_ws.rs`; factory integration in `configured.rs`.

- [ ] Add and observe failing tests for exact lookup with one valid row and a cursor, true duplicates/foreign rows, and non-advancing cursors.
- [ ] Implement bounded pagination and strict identity verification for exact-order lookups; leave general list completeness checks intact.
- [ ] Add failing transport tests for WebSocket authentication, create-frame fields, acknowledgements, wrong identities, disconnect/timeout ambiguity, and safe REST fallback only before sending.
- [ ] Implement `spawn_bybit_order_stream(testnet, api_key, api_secret, lifetime) -> Option<BybitOrderRelay>` and adapter relay attachment.
- [ ] Preserve account-shared 110-ms pacing while changing transport; verify no double writes after unknown results.

### 2. Bybit Execution Evidence and Wakeups

File: `rust-backend/src/exchange/realtime.rs`.

- [ ] Add failing tests for immediate fills, out-of-order events, duplicate fills, foreign identities, reconnect, stale partial snapshots, and full evidence arriving after fast wakeup.
- [ ] Add `BybitExecutionCache::register_placement(&OrderIntent, &PlacementAcknowledgement, u64)` to bind only exact owned order identities.
- [ ] Preserve bounded, session-safe evidence and exact cumulative quantity/quote assembly; publish deduplicated wakeups when authoritative evidence advances.
- [ ] Coordinate any REST seeding interface with the adapter; never fabricate missing fee or quantity fields.

### 3. Lock Contention and Concurrent Stop

Files: `rust-backend/src/engine/supervisor.rs`, relevant runtime tests, and `rust-backend/src/lib.rs` only if concurrency requires it.

- [ ] Write a failing regression proving Bybit placement does not retain the strategy mutex while awaiting network ACK.
- [ ] Reuse the TradeXyz prepare/place/complete path for Bybit; retain execution reservation while the lock is released.
- [ ] Enable bounded read-only periodic preemption for Bybit so GET work can yield to execution/stop without abandoning writes or starving audits.
- [ ] Test same-level fill bursts, stop during ACK, exact quantities, and unchanged manual baseline.

### 4. Verification, Release, and Measurement

- [ ] Run focused tests, Rust formatting/strict Clippy, and full Linux workspace CI; obtain independent review and resolve significant findings.
- [ ] Commit only this task's Rust/tests/plan; push the existing branch and verify the exact CI source SHA.
- [ ] Verify candidate state compatibility in a network-disabled container and read-only official-account probes before replacement.
- [ ] Back up quiesced current data, replace only the Rust service using the verified image, and confirm strategy set/lifecycle/manual ownership remain valid.
- [ ] Reconcile the two previously stuck fills through the normal Rust engine, never by manual ledger edits or extra direct exchange trades.
- [ ] Measure official Bybit `execTime` to replacement `createdTime`, deduplicating target orders, separating backlog catch-up from genuinely post-release fills, and disclosing unresolved/pending samples.
