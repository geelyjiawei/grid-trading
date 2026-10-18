# Optional Execution Evidence Repair

> Execute inline with TDD and an independent final review. User request: fix the Rust missing-versus-zero bug while retaining quantity validation.

**Goal:** A missing exchange PnL field must never erase known evidence or fail a real partial-fill audit; explicit conflicting values and execution changes must remain rejected.

**Architecture:** Represent reported realized PnL as `Option<Decimal>`. Reconcile optional metadata by exact immutable trade identity, preserve previously known values, and allow only absent-to-known enrichment. Strategy accounting continues to derive grid profit from owned lots, never from optional reported PnL. Existing decimal ledger values deserialize as known values; do not infer whether historical zeroes were originally missing.

**Scope:** Rust model/parsers, execution caches, strategy audit merge, API projection and regression tests. Keep the existing isolated feature worktree. Leave unrelated Vue edits untouched. Do not change production data, restart a failed strategy, place/cancel orders, or alter/adopt manual positions.

## Evidence

- Production run d9dbca8439d0 failed at 2026-10-18 23:08:13.394 +08:00 after a 210 Q partial fill of a 500 Q reduce-only order.
- Persisted WS trade fa4268e8-4994-541c-9709-18423acb3515 reports realized PnL 0.54026203. Official REST returns the identical economic trade without execPnl; the parser converted that absence to zero.
- Strict audit prefix equality and cache duplicate equality consequently treated the same trade as conflicting. A further 290 Q fill was left unaccounted. This plan repairs code, not the live failed ledger.

## Tasks

- [x] Add RED parser tests for missing/null/empty, explicit zero, nonzero and invalid execPnl.
- [x] Add a RED partial-then-REST regression, including 210/290 quantity progression, repeat idempotency and unchanged manual baseline.
- [x] Introduce optional reported PnL with backward-compatible serde, and a strict shared optional-evidence compatibility/merge operation.
- [x] Update Bybit, Binance/Aster and TradeXyz parsers without inventing zero; adapt test constructors to explicit known values.
- [x] Merge missing/known metadata in execution caches and strategy audit extension while keeping all other trade fields exact. Preserve previously known metadata on REST fallback and after serialization/restart.
- [x] Prove explicit zero versus nonzero conflicts, quantity/price/fee/identity conflicts, duplicate fills and cumulative over-allocation are still rejected.
- [x] Project unknown PnL/profit as unavailable in the Rust API, not fake zero or fee-only loss; preserve existing Vue optional-field contract.
- [ ] Run focused regressions, formatting, strict Clippy and full test suite. Verify native Linux independently of known Windows file-publication failures.
- [ ] Obtain independent review; retain raw evidence and report deployment/recovery status accurately. Do not silently deploy or restart the failed strategy.

## Review Focus

- Known zero is evidence and cannot be silently replaced by nonzero.
- A missing metadata field may be filled in; economic execution fields may not change.
- Previously persisted decimal values, including zero and negative PnL, remain readable and are not guessed to be missing.
- Missing/null/empty optional fields are distinct from malformed numeric/object/bool input; malformed evidence still fails.
- A metadata-only refresh must not create inventory deltas, replacement obligations or extra fees.
- Unknown PnL must not be presented as a real zero or negative fee-only profit.

## Verification Ledger

- Parser RED: omitted execPnl serialized as a reported zero. Model-only migration then exposed the real cache rejection and `Failed { message: "valued execution audit is invalid" }` in a same-trade REST refresh. Both passed after optional-evidence merging.
- Production-shaped replay passes: Bybit 500 Q reduce-only close at 0.1162; the actual 210/290 split, execution UUIDs, timestamps and fees; optional PnL 0.54026203; repeated REST absence; serde restart. Counter obligations remain exactly 210 + 290 = 500, baseline -0.5 is unchanged, grid quantity returns to zero, and fees are counted once. A minimal independent strategy owns this closing lot; it is not a copy or alteration of the live strategy.
- Independent review found an in-flight REST/WS metadata handoff gap. A real-adapter/real-frame-parser test reproduced returning None after known WS PnL arrived during REST. The merged snapshot is now returned before its WS generation is consumed, including terminal fills.
- Follow-up RED showed explicit REST zero could bypass the cache conflict boolean. Proven identity/trade/quarantine conflicts now return recoverable snapshot errors to the adapter; ordinary cache misses or stale sessions are not treated as false quantity evidence.
- Capacity RED reproduced that 4,097-fill REST snapshots could skip overlap validation. Cache admission is now separate from evidence validation/enrichment: large complete REST snapshots remain usable without unbounded caching, and explicit conflicts or existing quarantine still reject the read.
- Ruling: retain every assertion in the existing cancellation-race test, but derive its synthetic clock from the committed strategy timestamp. Its fixed 1,250 stop request could precede the prior batch ACK timestamp under test-runner load; no production timestamp guard was removed or changed.
- Final native Linux whole-workspace tests and exact revision are the remaining completion gate; raw RED/GREEN and Windows platform-failure evidence is retained in the task workspace `.local` directory.
- Frozen Windows whole-workspace invocation completed with 873 passing library tests and the same 24 file-publication failures as baseline (failure-name comparison is identical). All new regressions, including the production-shaped replay and large REST guard, passed. The independent final review closed both P2 findings and found no additional confirmed introduced defect. Formatting and strict whole-workspace Clippy passed after a test-assertion style correction.

## Compatibility and Operations

Existing persisted decimal values deserialize as Some(value), including zero and negatives. Historical zeroes cannot be retroactively classified as omitted without fresh evidence and are not rewritten. Newly persisted unknown values use null; an older binary that only accepts Decimal must not be blindly restored onto new evidence. Validate state compatibility before any future deployment or rollback. This change does not reset Failed lifecycle, rewrite live ledgers, adopt external trades, or resume any strategy.
