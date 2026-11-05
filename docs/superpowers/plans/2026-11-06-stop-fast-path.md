# Stop Fast Path Implementation Plan

> Execute the user's approved November 6 proposal inline with TDD and one independent final review. Reuse the existing linked worktree and preserve `.local/`.

**Goal:** Remove full historical catalog replay from stopping an already owned run, wake cancellation immediately, and expose true stop-request timings without weakening order ownership or persistence.

**Architecture:** The recovered runtime registry and each live run's exclusive file lease identify the stop target. Stop mutates that exact run through the existing durable state machine, independently of unrelated start/catalog mutations. A notification plus bounded 125 ms exit retries uses the existing serialized runtime, exact-ID cancellation ledger, and native exchange batches.

**Tech Stack:** Rust 1.90, Tokio, existing incremental strategy/intent journals, Vue unchanged unless a compatibility defect requires a fix.

**Spec:** Approved chat proposal: prioritize the measured 6.86-10.86 second pre-stop delay; retain quantity, ownership, deduplication, pre-dispatch durability, manual-position isolation, and authoritative terminal confirmation. Stage broader lock splitting only if the measured first-stage fix leaves a demonstrated bottleneck.

## Global Constraints

- No production test trades, cancels, closes, leverage changes, or manual-position operations.
- Do not compile on production. Release only a verified prebuilt image.
- Keep all existing order-ID, quantity, execution-accounting and persistence checks.
- No symbol/account-wide cancel-all. Retain Binance 10-target batches and 3 concurrent requests.
- A running strategy owns its exclusive runtime lease until authoritative cleanup finishes.
- Existing startup recovery and new-strategy catalog checks remain strict.
- Do not cut over a live trading process merely to obtain a test sample; use the zero-active deployment guard.
- Engineering targets are not live guarantees: stop durable <=200 ms, first dispatch <=300 ms, normal terminal convergence 1-2 s. Log actual failures and unfinished samples.

## Review Focus

- An unrelated start owns the catalog mutation guard/lease: an already owned run must still stop, with no new strategy admitted into the old market.
- Duplicate/late stops and run replacement: never redirect a selected stop to a newer owner.
- In-flight writes or accounting/read failures: persist stop, prevent new replacement planning, retain uncertain orders and reconcile them; never fabricate terminals.
- Stop retries must not execute normal opening/grid work for unrelated running strategies or pile overlapping work onto the same run.
- Stop metrics must retain API/request wall-clock identity, not a business timestamp advanced by an intervening tick.

### Task 1: Registry-owned stop control

**Files:** `rust-backend/src/engine/coordinator.rs`, `rust-backend/src/engine/supervisor.rs`, tests in `rust-backend/src/engine/runtime.rs`.

**Interfaces:** Keep `RuntimeCoordinator::request_stop(exchange, symbol, now_ms)` and `RuntimeRegistry::request_stop(run_id, now_ms)` signatures. Preserve the exact selected `run_id`; the existing per-run lease and journal append fence remain authoritative.

- [x] Add tests using real file stores and recovered runtimes: unrelated historical corruption does not block a known owned stop; absent market never falls back to disk; a held catalog lease/unrelated blocked start does not block an existing run's stop; repeated stops are durable/idempotent and do not place orders.
- [x] Observe targeted RED failures against the old coordinator.
- [x] Resolve a single owned registry run without global catalog scanning or start mutation locking; record selection, strategy-lock, persistence, total and original request timestamps by run ID.
- [x] Run targeted GREEN tests and existing start/recovery guards.

### Task 2: Immediate exit scheduling and precise cancellation telemetry

**Files:** `rust-backend/src/engine/coordinator.rs`, `rust-backend/src/engine/runtime.rs`, `rust-backend/src/engine/cancellation.rs`, `rust-backend/src/lib.rs`.

**Interfaces:** Add `RuntimeCoordinator::stop_wakeup()` and `advance_stopping(now_ms)`. Only durable stop success signals the wakeup. The existing per-run advancement reservation prevents overlapping exit sweeps.

- [x] RED tests: successful stop leaves an observable wakeup without a periodic tick; exit-only advancement completes the selected run without submitting an unrelated opening; a failed stop cannot publish a success wakeup; duplicate stop/retry preserves one owned cleanup.
- [x] Signal immediately after persistence, add an immediate scheduler branch and bounded 125 ms exit admission with per-run backoff, retaining normal periodic fallback.
- [x] Add run identity to exit/final logs and log prepared-target durability before network dispatch; separate cancellation network time from local commits. Never relabel a cancellation ACK as terminal success.
- [x] GREEN targeted stop, cancellation, in-flight placement, manual baseline, unknown-result and recovery regressions.

### Task 3: Verification, review and guarded delivery

**Files:** this plan, release evidence in ignored `.local/releases/<revision>/`.

- [ ] Run formatter, all-target Clippy, full Rust workspace tests and Vue tests/typecheck/build. Report the existing Windows atomic-directory-publish failures separately; require all Linux CI tests green.
- [ ] Independent final review, fix important findings with RED/GREEN tests.
- [ ] Commit/push the existing feature branch and run the pinned Linux/container release workflow; download and hash the immutable image, never compile production.
- [ ] Read current production state. If all strategies are terminal, perform the existing zero-active backed-up guarded cutover and verify auth, image identity, unchanged strategy/config hashes and manual-position/order observations. If still live, leave the current service untouched and clearly report the staged release.
- [ ] Collect genuine subsequent user stop samples if available; no artificial production trading sample. Report stage timings and any unfinished stops, not only successful ones.

## Execution Ledger

- Initial HEAD: `49390ab7ac3cad94772eea7e2e3403fcafbf0b6c`; existing linked worktree on `codex/fast-replacement-submit`, only untracked `.local/`.
- Production observation at start: image `49390ab`, healthy, zero restarts, one active strategy; disk 36% used. No service restart or financial command performed.
- Ruling: implement the measured entry-path fix and immediate exit scheduling first; avoid speculative broad lock-free cancellation/UI rewrites before new timing evidence. This reduces recovery/concurrency risk; deeper network-lock splitting remains contingent on residual latency.
- Baseline targeted ownership-proof tests: 8 passed. Entry RED reproduced unrelated-history rejection and unrelated-start timeout; entry GREEN: 3 passed. Wakeup interfaces RED (missing methods), then 5 coordinator stop tests passed.
- First full Windows GNU suite: 1037 passed, 25 existing atomic-directory-publish failures, one optional benchmark ignored. The failure set is a subset of the prior 26 failures; the renamed catalog-lease stop test now uses real recovered files and passes on Windows. With exactly that known baseline excluded: 1037 library + 10 auxiliary tests pass, one ignored. Pinned formatter, all-target Clippy, Vue 164 tests, typecheck and build pass.
- Independent reviewer Zeno identified overdue periodic audits returning lifecycle=None and hiding stops from exit admission. Reproduced by `coordinator_overdue_stop_exits_while_an_unrelated_tick_is_blocked`: expected one exit, got zero after 5.1 s. Fix exit admission to use live lifecycle metadata without periodic-audit masking; retain per-run write/advance reservations.
- Ruling: add per-run monotonic exit retry backoff (125, 250, 500, then capped 1000 ms) rather than sustained 8 Hz network polling during an outage. Immediate first dispatch is unchanged; periodic reads still use the existing exchange governor. Cost: uncertain exits may wait up to one second between dedicated retries instead of 125 ms, protecting exchange limits and the 2-core server.
- Final review fix: overdue-audit regression RED (zero exits) -> GREEN (one stopped run while another exchange's tick remains blocked). The retry-admission regression RED (immediate second read) -> GREEN (no second poll); a deterministic monotonic-time test covers 125/250/500/1000 ms delays and the one-second cap. Nine focused coordinator tests and the retry-cap test pass; final formatter/Clippy and Vue verification pass.
