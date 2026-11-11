# Query and Execution Isolation

> Approved user request: repair the profit query chain and optimize long replacement latency. Execute using TDD with disjoint workers, scoped review, then full Linux/container verification. Base d4eea1f; production is still 49390ab.

**Goal:** Make profit reads bounded and independent from the execution-critical path while retaining truthful settled funding, exact order quantities/ownership, duplicate protection and pre-send durability.

**Architecture:** Read immutable validated runtime snapshots instead of replaying every live ledger from disk per UI request. Limit/coalesce expensive read work, query only the selected run, and clearly distinguish a current validated net profit from older or incomplete observations. Improve bounded funding-history reads and reuse validated immutable history where independently provable; never replace unavailable money values with zero. Optimize repeated execution validation only through invariant-preserving proofs and exhaustive adversarial tests, not by dropping checks.

**Tech Stack:** Rust 1.90/Tokio; existing incrementally persisted journals; Vue/TypeScript.

## Global Constraints

- No production test orders, cancellations, closes, leverage edits, strategy parameter edits, or manual-position actions.
- No production compilation. Use prebuilt CI artifacts and preserve the current process while active strategies exist unless the user explicitly changes the deployment window.
- Acknowledgements are not terminal evidence. Quantity, run identity, ownership, deduplication, WAL-before-network and crash recovery remain mandatory.
- Financial results must retain the exact run, observation time and funding cutoff; stale/partial history cannot be reported current or interpreted as zero.
- Keep existing stop-fast-path changes; do not revert the untracked `.local/` evidence or alter unrelated files.
- Scope reads to the requested run; startup/new-owner checks remain strict. No blanket limit/concurrency increases.

## Evidence and Review Focus

- Three live strategies INITUSDT/ANTHROPICUSDT/SNDKUSDT. All three risk reads returned 429 while global risk collection was busy; logs show 12-second funding-history timeouts and pending-accounting attribution failures.
- Production CPU sample 128.20%, Rust RSS 1.155 GiB / 1.5 GiB. Heavy history reads must not add unbounded copies, detached jobs or memory.
- Recent 153 stage rows: mean accounting 562.7 ms, submission 1770.8 ms, total 3013.8 ms. State validation mean 351.7 ms/max1998 ms; persistence mean113.1 ms. Execution-lock mean912.9 ms/max12024 ms. These are diagnosis samples, not post-fix performance claims.
- Profit needs fees and settled funding. Wrong-run reuse, stale revisions/cutoffs, external fills, missing values, future rows, duplicate pages and incomplete history must fail closed.
- Snapshot ownership must survive stop/terminal transitions and in-flight writes without authorizing new writes; readers must not hold a strategy lock during network or disk replay.
- Every optimization must preserve manual baselines, exact cell capacity and recovery validation. No fabricated successful net profit or promised fixed exchange latency.

## Work Items

### 1. Funding reader/cache (worker)
**Own:** exchange/binance.rs funding reader/tests; exchange/configured.rs funding cache/tests if necessary; funding.rs only if strictly necessary and coordinated.
- [x] RED: excessive post-settlement trade reads, duplicate cold fetches/error poisoning or a demonstrable timeout mechanism.
- [x] GREEN: bounded independently complete funding/account-fill history, short failure retry vs successful TTL, exact keys/cutoffs and no false zero; preserve existing parser/pagination tests.
- [x] Record test commands/results. Do not edit API, engine or Vue.

### 2. Execution validation cost (worker)
**Own:** engine/strategy_machine.rs and narrowly scoped new validation helper/tests; coordinate before touching persistence/engine exports.
- [x] Profile existing full-history validation and immutable transition behavior; write RED reproducing unnecessary full validation work on a proven successor.
- [x] Optimize only invariants that can be proven from a validated predecessor and exact changed evidence. Keep full validation on untrusted load/recovery/mutable escape and uncertain transitions. Adversarial quantities, ownership, duplicated fills, changed lots and same-revision replacement must still fail.
- [ ] If a safe local optimization cannot be proven, report the limit rather than silently skipping checks.

### 3. Read path / truthful UI (main)
**Own:** api.rs; engine/coordinator.rs/supervisor.rs runtime read bridge; Vue query/profit components/tests; new read-only query helper module if needed.
- [x] Baseline/failing tests for repeated disk reads, unrelated-run work, cached stale/current distinction and duplicate/in-flight requests.
- [x] Immutable Arc runtime read bridge; isolated per-run catalog fallback; bounded query coalescing and clear reason/time fields.
- [x] Do not let a 12-second funding call monopolize unrelated query capacity; preserve exact attribution and deny-current results if collection becomes invalid.
- [x] Old values may be shown only explicitly marked stale with their own run/time, never mixed into current net/protection decisions.

### 4. Verify / deliver
- [x] Review each worker's changes, adversarial regressions, independent final review and fixes.
- [ ] Pinned fmt/Clippy; full Rust workspace Linux tests; Vue tests/typecheck/build; authenticated container and artifact identity checks.
- [ ] Push existing feature branch; stage checksummed prebuilt package. Read live readiness before any cutover; do not stop live strategies for a deployment sample.
- [ ] Measure genuine subsequent fills after cutover only if a safe window exists; otherwise clearly report not deployed and no live post-fix metrics.

## Ledger

- Initial worktree clean except intentionally untracked `.local/`; already isolated feature branch. No order/position command sent.
- Native stats above collected read-only from bounded Docker log tails; do not reuse them as performance targets or a successful rollout.
- Funding worker: 70 funding-history and 95 funding/attribution/protection tests passed. Success TTL stays 60 seconds, error TTL is 5 seconds; cold fetches coalesce within eight keys, protection fresh reads bypass cache/coalescing.
- Execution worker: 136 strategy-machine tests passed (one optional profile ignored), nine validation-reuse tests include 18 adversarial mutations. Synthetic debug median for 20 iterations of 128 audits: old successor 73.53 ms, new successor 65.69 ms; one sample was slower. This is approximately 11% in that synthetic CPU slice, NOT a live replacement latency claim. Global history scans and exact comparisons remain.
- Main regression RED: slow funding held the authoritative risk snapshot slot. GREEN after bounded two-query/per-market admission and moving history outside the one-at-a-time shadow slot, followed by run recapture and post-shadow revision fence.
- Runtime query tests cover Arc sharing while a mutation lock is held, committed stop publication, market isolation and queries succeeding with an intentionally nonexistent API disk catalog path. Recovery/new-owner validation stays unchanged; orphan cleanup rejects advancing/changed snapshot revisions before returning any targets.
- Local integrated validation: Windows workspace 1062 library plus 10 auxiliary tests passed, two optional profiles ignored, exactly 25 pre-existing Windows atomic-directory-publish tests excluded. Full Linux verification remains mandatory with no exclusions. Vue 203 tests, typecheck and production build passed. Evidence: `.local/query-execution-20261111/`.
- Independent review found a P2: incomplete profit data was also hiding valid fresh risk warnings. Reproduced RED with actual new metadata, separated `risk_observed_at_ms`/risk freshness from profit completeness, and retained warnings/actual fee and position evidence while net remains unknown. Added RED/GREEN hung-query expiry plus wrong-run/expired/future/missing/stale evidence checks. Follow-up review reports no new P0/P1/P2 findings; reviewer independently exercised the Vue component/timer but did not run Rust crash fault injection or production performance measurements.
- The initial runtime Arc is explicitly released before funding I/O; only run/from/cutoff identity is held until recapture. Final local Vue test count is 205. Production read-only recheck at 2026-11-11 13:38 CST: 49390ab, three active strategies, zero container restarts, 1802 MiB MemAvailable and 38 GiB disk available. No cutover while active.
- Extra fault regression reproduced a missing publication fence: an individually valid state could be published after an unsynchronized intent append. Publication now calls the existing exact revision-pair cached cross-ledger verification; mismatches publish no current snapshot. RED/GREEN completed; two publication tests and full local suite (1063 library + 10 auxiliary passed, same 25 Windows exclusions, two ignored) and Clippy passed. Independent follow-up found no P0/P1/P2 in this helper/test; task-abort integration was not independently executed.
- First Linux CI run 42967784990 on 77a2879: Vue, compatibility and isolated production container checks passed; Rust had 1095 passed, one failed, two ignored. Failure: unchanged `concurrent_same_run_start_has_one_durable_winner` received StartReservation(ENOENT). Read-only review identified pre-existing removable staging-root / renamed-lease inode races in unchanged reservation code, predating this task. No failures are skipped or accepted more broadly; a narrowly scoped reservation fix/regression is in progress before another full Linux gate. No verified release artifact or deployment from this failed run.
- Reservation fix: keep the shared staging root and a per-run stable guard outside the renamed payload. Acquire it before payload access, recheck the final destination under guard, retain it through publication and completed cleanup, never unlink it. Existing runtime lease relocation is unchanged. Five deterministic lifecycle tests RED then GREEN, 12 adjacent discovery/lease tests passed; existing 12-contender test is byte-identical to 77a2879 and successful-publication/winner-drop assertions are expanded. Independent narrow review found no actionable P0/P1/P2; Linux concurrency/publication verification remains required. Cost: one persistent small guard file/inode per run; no guard cleanup automation.
- Post-reservation local gates: Windows 1068 library plus 10 auxiliary tests passed, same exact 25 known platform exclusions, two optional tests ignored; pinned formatting and all-target Clippy passed. Linux and release results will be recorded with the exact release SHA in the ignored release verification artifact, without mutating a tested release commit just to check off this plan.
