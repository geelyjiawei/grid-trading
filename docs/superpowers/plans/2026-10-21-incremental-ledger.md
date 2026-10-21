# Incremental Durable Ledger

## Contract

Rust only. Preserve quantity/capacity, ownership, deduplication, fee handling,
idempotent client IDs, and durable prepared intents before network submission.
No production orders, positions, credentials, or unrelated Vue edits are changed.
Baseline: e7fcecc. Existing linked worktree is the isolated implementation area.

## Design

Keep legacy JSON readable. On the first active mutation, migrate to a versioned
checkpoint plus checksummed, revision-fenced append-only journal. Each mutation
appends and fsyncs a typed delta before publishing its in-memory successor.
Complete snapshots are captured immutably and written by one background worker.
Do not truncate committed journal history in this revision. Read-only loading
replays committed records and validates the complete recovered state. An incomplete
EOF frame is recoverable; corrupt complete frames fail closed. Old binaries must
reject the new checkpoint format rather than silently load stale state.

## Tasks

### Task 1: Journal and Store Integration

Implement framed checksummed WAL, lazy legacy migration, typed map deltas,
revision fencing, durable append, background checkpoint, and read-only replay.
Write regression tests first for checkpoint-not-rewritten and recovery; then add
torn tail, corruption, stale writers, concurrent readers, and checkpoint tests.
Files: rust-backend/src/persistence/* and the strategy persistence error type.
Expected: focused persistence tests pass; no full JSON rewrite per warm commit.

### Task 2: Validation Proof Reuse

Cache only successful cross-ledger validation for an unchanged pair of revisions;
invalidate before mutable references escape. New revisions still fully validate.
Files: rust-backend/src/engine/runtime.rs. TDD: same-pair reuse, changed revisions,
and same-revision mutable tampering. Do not remove quantity or ownership checks.
Expected: runtime regressions pass, injected bad ownership is still rejected.

### Task 3: Operations and Verification

Add read-only native exports for journal-aware operational inspection. Benchmark
warm persistence against legacy writes on representative large history, report
cold migration separately. Run fmt, strict Clippy, focused and full tests, then
fresh-context review. Do not deploy unless native Linux crash/recovery and full
regression gates pass. Document rollback compatibility and retained WAL growth.

## Shared Interfaces

Task 1 preserves StrategyStateStore and IntentStore snapshot/revision contracts.
Task 2 consumes those contracts without changing persistence schemas. Task 3
uses journal-aware store loaders, never raw checkpoint JSON for current state.

## Review Focus

Durability before send, ambiguous write failures, crash boundaries, malformed WAL,
stale writer fencing, readonly concurrent replay, missing WAL, background worker
lifetime, directory relocation, downgrade rejection, quantity and dedup invariants.
