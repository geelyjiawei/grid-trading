# Manual Position Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Support evidence-backed manual inventory changes without changing owned grid exposure.

**Architecture:** Introduce a read-only normalized account position history contract on PositionSnapshotGateway. Persist external execution identities separately from grid inventory, replay the manual budget, and reconcile only against a fresh exact exchange quantity. Keep the fast replacement path free of history requests; latch observed conflicts across recovery.

**Tech Stack:** Rust 1.90, Tokio, rust_decimal, Serde, existing append-only WAL; no new dependency.

**Spec:** docs/superpowers/specs/2026-11-11-manual-position-compatibility-design.md

## Global Constraints

- Preserve quantity, ownership, deduplication, and WAL-before-network guarantees.
- No live trades, cancellations, leverage changes, production builds or deployment.
- Existing immutable baselines and grid inventory events must not be rewritten.
- Unsupported/ambiguous evidence must fail closed, not become an empty successful history.
- Reuse the existing linked worktree; retain all unrelated .local evidence.

## Review Focus

- History/position observations race with an owned fill: reject pending evidence, never classify it as manual.
- Equal-and-opposite manual fills hide a prefix overclose: replay prefixes, not only the final net.
- Restart bypasses an observed mismatch: persist the blocker and honor it on targeted and ordinary submission paths.
- Old WAL headers omit the new map: serde defaults plus map delta replay must retain complete ownership validation.
- Receipt attribution spans manual activity: exact cash receipts and full execution provenance, not account PnL or guessed fees.

### Task 1: Account history contract and official adapter readers

**Files:** exchange.rs, exchange/funding.rs, exchange/{binance,aster,bybit,trade_xyz,configured}.rs; adapter-owned tests.

**Interfaces:** `AccountPositionHistory { exchange: Exchange, symbol: String, from_ms: u64, through_ms: u64, fills: Vec<FundingAccountFill> }`; `PositionSnapshotGateway::account_position_history(exchange, symbol, from_ms, through_ms) -> Result<AccountPositionHistory, SnapshotError>` defaults to unavailable.

- [ ] Write failing fixture tests for exact opaque identities, sides, time bounds, complete pagination, conflicting duplicates and history limits.
- [ ] Implement bounded official read-only REST readers; never use order-scoped or cached partial history as account completeness.
- [ ] Forward the method through configured/shared gateways without introducing a trading mutation.
- [ ] Run all adapter tests and record outcomes.

### Task 2: Durable manual inventory and reconciliation

**Files:** engine/external_inventory.rs (new), engine/{mod,strategy_machine,runtime}.rs, persistence/journal_delta.rs, persistence/strategy_state.rs tests.

**Interfaces:** State holds an external fill map, optional verified-through timestamp and optional blocker; `manual_position_quantity()` adds the immutable baseline to external deltas. Machine reconciliation consumes complete AccountPositionHistory plus exact observed quantity and commits atomically.

- [ ] Add failing manual 0.5 -> 0.3, full close, add/reclose, duplicate/conflict, overclose/opposite, unknown-owned and boundary tests.
- [ ] Implement full identity/owned-execution/prefix checks; preserve grid state and reject contradictory history before committing.
- [ ] Add state validation, serde defaults and map deltas; test disk WAL recovery and injected persistence failure.
- [ ] Reconcile periodic/exit mismatches with a second position read, persist errors, and block all submission paths while unresolved; allow owned-only stop cleanup.
- [ ] Prove normal targeted replacement does not query account history and grid-only risk exit leaves remaining manual inventory.

### Task 3: Profit/funding integration and release verification

**Files:** engine/strategy_profit.rs, exchange/funding.rs and focused tests; documentation/evidence.

- [ ] Add regression tests for unchanged grid fees/PnL and receipt shares before/after external closes; missing evidence stays unavailable.
- [ ] Permit proven complete external histories in funding attribution while rejecting boundary or netted-budget ambiguity.
- [ ] Run Rust full workspace tests, fmt, Clippy and Vue regressions (response contracts unchanged).
- [ ] Obtain a fresh whole-change review; fix material findings with failing regressions first.
- [ ] Record actual passing/failing counts and remaining operational limits. Do not claim deployed/live-tested.
