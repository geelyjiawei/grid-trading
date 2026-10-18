# Durable Fast Counter Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for bounded independent changes and integration verification.

**Goal:** Reduce Rust fill-to-counter latency by removing redundant whole-ledger commits and idle maintenance lock holds without weakening pre-send quantity checks or crash recovery.

**Architecture:** Keep the existing JSON ledgers and durable prepare-before-send protocol. Combine execution accounting and replacement materialization in one validated strategy transaction; batch intent synchronization. Allow only explicitly idle, read-only maintenance waits to defer to execution/stop priority, with bounded audit admission deferral for the Binance-focused release.

**Tech Stack:** Existing Rust 1.90, Tokio, serde, atomic-write-file; no new dependencies or Python trading backend.

**Spec:** User request in this task: prioritize speed, retain quantity checks, test and deploy, measure actual QUSDT replacement latency, never operate manual positions.

## Global Constraints

- Preserve durable intent preparation before any exchange submission and all cell/exposure/identity validations.
- No asynchronous unflushed ledger, WAL format migration, manual trading, strategy restart, or position adjustment.
- Keep existing Vue work untouched. Work in the existing linked feature worktree.
- Build production images in GitHub CI, not on the production host.
- Current healthy QUSDT run discovered read-only: `b4e851ed881e`, 40 cells, 500 Q per cell. Do not revive stopped run `8efd8c5927e1`.
- Operational exchange calls are signed GET observations only. Production strategy continues its existing owned trading.

## Review Focus

- Failed atomic writes expose neither partial execution accounting nor replacement children.
- Duplicate/partial/malformed fills cannot exceed cell capacity or replay inventory.
- Stop precedes queued fills when it owns FIFO priority; never abandon placement or acknowledgement persistence.
- Binance continuous execution must not indefinitely starve risk/rules/position maintenance admission. Three read deferrals or five seconds since the last completed audit protect the next scheduled admission; this is not an audit-completion deadline.
- Restart after each durable boundary must reuse IDs and reconcile unknown outcomes, not resubmit blindly.

## Tasks

### 1. Atomic Engine Operations

Files: `rust-backend/src/engine/strategy_machine.rs`.

- [x] Red tests for one write per execution/materialization batch and one write per intent synchronization batch.
- [x] Implement `apply_valued_executions_and_materialize(...)->Result<(Vec<StrategyTransition>, StrategyTransition), StrategyMachineError>` using the existing state helpers and one final validated commit.
- [x] Implement `synchronize_intents(&[OrderIntent], now_ms)` with one final commit, preserving per-intent results.
- [x] Cover failed writes, duplicates, malformed evidence, stopped/failed lifecycle, and exact child quantity.

### 2. Read-Only Maintenance Priority

Files: `engine/periodic_priority.rs`, `engine/supervisor.rs`, module export, parent-owned `engine/runtime.rs` integration, and the periodic-only enumeration call in `engine/coordinator.rs`.

- [x] Red tests for a pending maintenance GET yielding to execution/stop, no missed notification, and waiter cancellation cleanup.
- [x] Pass a per-advance `PeriodicReadPriority` context; interrupt only explicitly safe read waits.
- [x] Bound deferral; do not claim an interrupted/erroring read completed a risk audit.
- [x] Preserve placement/acknowledgement completion and existing serialization for Binance.

### 3. Runtime Integration

File: `rust-backend/src/engine/runtime.rs`.

- [x] Red regression proves the old hot path performs redundant strategy writes and blocks on an idle GET.
- [x] Use the combined accounting/materialization operation and batch pre-/post-submit strategy synchronization.
- [x] Keep cross-ledger ownership and prepared-intent persistence before sending.
- [x] Gate priority deferral to Running maintenance without recovery, execution, submission or cancellation progress.
- [x] Test replay, write failure, stop ordering, unknown placement, partial fills, capacity limits and foreign IDs.

### 4. Verify, Publish And Measure

- [ ] Independent review; strict fmt/clippy and full Rust workspace tests on Linux CI.
- [ ] Report Windows's known directory-publication PermissionDenied failures separately, never as passes.
- [ ] Verify image revision/checksum, offline validate current state, back up ledgers, and replace only the Rust container.
- [ ] Assert protected positions/orders and immutable historical evidence remain unchanged.
- [ ] Measure exact exchange fill timestamps to corresponding new order creation timestamps, deduplicated by target order. Report pending/unmatched fills, cold/warm cohorts, full latency and WebSocket roundtrip separately.

## Decisions

- Retain synchronous durable writes. Optimize commit count, not safety boundaries.
- No new ledger format in this release: existing inspection, recovery and backup readers remain compatible.
- Older stopped runs are evidence only; deployment guards target the currently active run discovered before rollout.
- TradeXyz retains its pre-existing reservation across unlocked placement/acknowledgement and is excluded from bounded audit admission. Sustained-fill maintenance starvation on that pipeline is a known pre-existing limitation; removing the reservation would allow unsafe reconciliation of in-flight Prepared intents, so a separate drain design is deliberately out of this Binance/QUSDT change.
- Independent review found no new P1/P2 issues after narrowing that fairness contract; the TradeXyz reservation remains covered by regression testing and Linux full-suite gates.

## Local Verification Before Linux Release Gate

- Strict formatting and workspace/all-target Clippy passed.
- Windows GNU full library run: 808 passed, 23 failed. All 23 are the independently reproduced pre-existing Windows directory-publication `Publish(PermissionDenied)` failures; they are not counted as passes. Linux CI must pass the complete workspace before rollout.
- New regressions cover atomic batch commit counts, duplicate/partial execution capacity, failed writes, safe periodic GET preemption, bounded audit fairness, stop priority, and completion of an already in-flight placement acknowledgement.
- Deployment tooling has GET-only exchange access, validates the image and existing strategy offline, quiesces only the Rust process, backs up current ledgers, and verifies protected account state without compensating trades or restoring stale data.
