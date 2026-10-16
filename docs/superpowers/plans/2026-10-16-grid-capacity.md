# Grid Capacity Repair Implementation Plan

> **For agentic workers:** Use test-driven development and verification-before-completion. Execute inline, with a separate read-only safety review.

**Goal:** A 20-cell fixed-quantity grid owns at most 0.2 of circulating order capacity in each cell; remove the confirmed legacy excess without changing manual positions.

**Architecture:** Allocate a cell once, version deterministic plan snapshots so historical data remains readable, validate circulating capacity before replacement submission, and retire legacy cancelled excess explicitly rather than rewriting executions.

**Tech Stack:** Rust 1.90, Decimal, existing durable JSON ledgers, Binance official API, Vue unchanged.

**Spec:** User request in this task, 2026-10-16. Current run dab0d666a310 has an unfilled bot-owned 1932 SELL 0.4; only its extra 0.2 is authorized for removal.

## Global Constraints
- No market orders or position-close requests during repair.
- Preserve baselines, manual orders, immutable execution history and other grid cells.
- No compiler/build on the production server; deploy a CI-verified immutable image.
- Preserve existing unrelated uncommitted Vue drawer work.

## Review Focus
- References exactly on or strictly between cell boundaries, all directions and grid modes.
- Partial fills, repeated reconciliation and restart must conserve cell capacity.
- Historical version-one plans must remain readable, not silently reinterpreted.
- Cancellation timeout or fill races must not authorize blind replacement or retirement of executed quantity.
- Failed deployment or interrupted repair must retain a backup and fail closed.

### Task 1: Allocation and Capacity Invariants
- [x] Add a realistic ANTHROPIC regression and observe the old 21-leg / 4.2 behavior fail.
- [x] Allocate each new cell once and validate the versioned plan and circulating per-cell quantity.
- [x] Exercise boundaries, long/short/neutral, arithmetic/geometric, fragments, cancel/restore and restarts.

### Task 2: Auditable Legacy Repair
- [x] Add a narrow retirement operation that requires authoritative cancellation of unfilled legacy excess.
- [x] Keep historical order and obligation shapes immutable; explicitly record retired capacity.
- [x] Test identity mismatches, partial-fill races, idempotence and preservation of manual baselines.

Evidence before release: original regression failed 21 != 20; corrected grid matrix has 1,194 cases; all 105 strategy-machine and 29 shadow tests pass; both replay integrations and Clippy pass. The actual production ledger copy passes offline candidate validation with all 162 inventory events retained. Local full-suite Windows baseline contains directory-publication PermissionDenied failures; final authority is Linux CI. Local bundled Python lacks legacy application dependencies, so its full-suite attempt cannot replace the isolated CI install.

Ruling: final maintenance-only review fixes (signal cleanup, backup fsync, and signal propagation through network recovery) are separate operational commits. Runtime image inputs remain byte-identical to release `efd5e1b9d2b693509baf569c0365d98812c23982`, whose Linux Rust/Python/Vue/container checks passed. Deploy that verified immutable runtime image with the separately reviewed `10f66fa` maintenance script after all nine maintenance tests pass on Linux; verify the runtime input diff is empty. This avoids rebuilding an unchanged binary, not a test exemption for changed code.

### Task 3: Release and Production Verification
- [x] Independent safety review, complete Linux CI and immutable image verification.
- [x] Re-read exchange orders and positions, quiesce only the bot runtime, back up durable data.
- [x] Cancel/replace only the identified excess using a guarded durable repair; preserve all other order identities.
- [x] Deploy and compare exchange/state quantities, confirm 20 cells total 4.0 and manual exposure unchanged.

## Production Result

- 2026-10-16 19:44 CST: deployed immutable runtime image `grid-trading-vue-rust:efd5e1b`, image ID `sha256:862431870af19dd19728591635f13e2bad001c22e2b237254e99b7889ad51626`.
- Full CI: https://github.com/geelyjiawei/grid-trading/actions/runs/38894763546 ; 768 Rust tests, Python 3.12/3.13, Vue, container/Compose gates passed. Nine final maintenance tests passed separately on Linux.
- Private backup and durable phase journal: `/opt/grid-trading-rust-preview/backups/grid-capacity-20261016T114416Z`.
- Cancelled exactly exchange order `235064031` (1932 Sell 0.4, zero fills). Runtime accepted `235269469`, client `r_dab0d666a310_5_S_106`, at 1932 Sell 0.2 GTC.
- Other 37 exchange order identities and quantities unchanged. 38 physical orders represent 20 cells; each cell totals 0.2, aggregate 4.0. Historical fragments remain intentionally untouched.
- Before/after ANTHROPIC position flat; no position-close request, baseline or historical execution change. All 162 inventory events, 39 completed pairs and financial aggregates retained.
- Fresh post-deploy exchange/state audit: no identity/shape mismatches, no pending obligations, lifecycle Running, failure null. `/healthz` ready/active=1, container healthy/restarts=0, frontend HTTP 200.
