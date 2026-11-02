# Strategy Net-Profit Protection Implementation Plan

> Execute approved work in this session using TDD and disjoint workers, followed by an independent whole-change review.

**Goal:** Enable per-run amount-based net-profit take profit/stop loss, editable while running, with owned-order cancellation and owned-inventory exit only.

**Architecture:** Optional persisted protection settings and trigger evidence extend the existing Rust strategy state. An independent background monitor collects the same authoritative, funding-inclusive strategy profit used by the UI without holding execution locks; a fresh, unchanged run/state/config snapshot may latch the existing risk-exit lifecycle. All four exchanges share this engine; exact-ID cancellation uses exchange-native batches where supported.

**Tech Stack:** Existing Rust Decimal/serde/Tokio, journaled state and Vue 3. No new service, language, secret or dependency.

**Spec:** User approval in this chat: amount-based per-run net income, runtime editing, cancel only owned orders and close only owned inventory, preserve manual positions, test and deploy. Additional request: improve apparently batched cancellation without cancelling manual orders.

## Global Constraints
- Default disabled for legacy and newly created strategies until explicitly configured. This release provides the setting on the running strategy; it never invents limits.
- Positive TP and SL amount magnitudes in the exchange settlement asset. TP is net >= TP; SL is net <= -SL; either may be null. Zero/negative/overflow invalid.
- Net = gross realized - actual execution fees + owned unrealized + attributable settled funding. No whole-account PnL, forecast funding or fabricated zero; future exit fees/slippage are not guaranteed.
- Setting a limit must not directly submit, cancel or close. The next verified background observation may trigger immediately if a newly set threshold is already crossed; show this warning.
- Separate protection revision and mandatory run identity prevent lost updates/ABA. Reject changes after exit is latched; disabling is an explicit full replacement with both fields null.
- At trigger, persist evidence before any cancellation; reuse the current RiskExitRequested flow. Await authoritative owned order terminals and fills, then use exact owned quantity and reduce-only market exits. Current account/ledger mismatch blocks closing and does not alter baseline/manual orders.
- No network waits while holding the strategy lock in the monitor. Bounded single monitor per run, no overlapping sweeps, only enabled strategies; stale/changed snapshots cannot trigger.
- Preserve all quantity, ownership, deduplication and durable-before-send safeguards. No production test trades or manual-position adjustments.
- Build Linux image in GitHub CI, never on production; deploy only if live strategies remain zero. Preserve data/configuration and compare read-only account evidence.

## Interfaces
- `engine::ProfitProtection { revision: u64, take_profit_amount: Option<Decimal>, stop_loss_amount: Option<Decimal>, updated_at_ms: Option<u64> }`, serde default, disabled when both amounts are null.
- `engine::ProfitProtectionObservation { status: String, observed_at_ms: Option<u64>, net_profit: Option<Decimal>, error: Option<String> }`.
- `engine::ProfitProtectionTrigger { net_profit: Decimal, mark_price: Decimal, observed_at_ms: u64, protection_revision: u64 }`.
- `engine::ProfitProtectionStatus { run_id: StrategyRunId, exchange: Exchange, symbol: String, lifecycle: PreparedStrategyLifecycle, profit_protection: ProfitProtection, observation: ProfitProtectionObservation, trigger: Option<ProfitProtectionTrigger> }`.
- `RuntimeCoordinator::profit_protection_status(exchange, symbol, &run_id) -> Result<ProfitProtectionStatus, ProfitProtectionControlError>`.
- `RuntimeCoordinator::update_profit_protection(exchange, symbol, &run_id, expected_revision, take_profit_amount, stop_loss_amount, now_ms) -> Result<ProfitProtectionStatus, ProfitProtectionControlError>`.
- Error variants: `NotFound`, `RunMismatch`, `NotRunning`, `RevisionConflict`, `InvalidLimits`, `State(StrategyMachineError)`.
- Authenticated GET/POST `/api/grid/profit-protection/{symbol}?exchange=...`; GET includes required `run_id`; POST full body `{run_id, expected_revision, take_profit_amount, stop_loss_amount}`. Amounts are exact decimal strings or null; both amount keys mandatory. Authorization and body constraints follow existing controls.
- API grid status includes `profit_protection`, `risk_exit_reason`, `profit_protection_trigger`. Vue also polls the dedicated status only for the selected enabled strategy, serially and without a new high-frequency account query.

## Tasks
### 1. Durable engine protection and shared profit calculation (main)
- [ ] Add defaulted settings/evidence and journal header coverage. RED/GREEN: positive bounds, disabled defaults, inclusive TP/SL, negative net, both directions, legacy serialization, CAS, exit latching, retry/restart without repeated close.
- [ ] Extract existing read-only profit/funding calculations into a shared engine module; retain API wrappers/tests and identical financial arithmetic.
- [ ] Add prepared-runtime bridge, registry controls and bounded monitoring outside the execution lock; validate run/config/state identity and collection age before latching. Test pending fees/funding, partial fills, ownership mismatch, in-flight placements, stale snapshots and configuration races.
- [ ] Reuse existing exact-quantity risk-close flow. Dispatch owned cancellations for the whole current set in one runtime sweep; exchange adapters bound wire batches/concurrency. Test late fills before final closing, preserved nonzero baseline, manual orders excluded and unknown acknowledgements reconciled rather than guessed.

### 2. Authenticated API and Vue controls (worker, disjoint files)
- [ ] Test exact route auth, run identity, invalid/missing amounts, CAS conflicts, terminal rejection and explicit disable.
- [ ] Add current-strategy panel for TP/SL settlement amounts, save/disable, configuration version and monitoring/trigger/error feedback. Do not restart or recreate a grid to edit limits.
- [ ] Test decimal inputs, duplicate clicks, changing selection, stale responses, errors, no submit on render and no order/position mutation endpoints. Run the full Vue suite/typecheck/build.

### 3. Exact-ID native batch cancellation (worker, adapter files only)
- [ ] Binance: preserve 10-order native batches; bounded concurrent dispatch and stable per-target outcomes.
- [ ] Bybit: 20-order linear cancel-batch, validate both per-row identities and per-row retExtInfo results; ACK is not a terminal state.
- [ ] Aster: verify official v3 batch support/limit before implementing; use existing wallet signing and strict acknowledgement parsing.
- [ ] TRADE.XYZ: signed cancel array of exact asset/order IDs, bounded batches and strict per-target statuses; no scheduleCancel or account-wide cancellation.
- [ ] RED/GREEN tests: fewer/more than one batch, partial failures, missing/extra/wrong identities, timeout/unknown outcomes, duplicate targets, foreign exchange and manual-order exclusion.

### 4. Review, verification and rollout (main)
- [ ] Independent review focused on unintended closes, races, deadlocks and stale profits. Address material findings before release.
- [ ] Focused Windows GNU tests, formatter and Clippy; full pinned Linux Rust/Vue/contract/container CI and dependency audit.
- [ ] Prebuilt image checksum/revision, isolated non-trading startup, read-only legacy-ledger validation, full backup, start-only maintenance barrier and zero-live cutover.
- [ ] Verify protected account observations and ledger/configuration hashes, authenticated controls, disabled legacy defaults and served frontend. Never enable a real protection setting or create a trade just to test.

## Review Focus
- An old/slow response arrives after limits changed or a new run reused the symbol: discard, never close the new run.
- A batch cancellation races a fill: account the new fill, recompute owned quantity, never reuse the old close amount.
- Funding or fee valuation is unknown: visibly unavailable; do not infer profit or silently substitute zero.
- Exchange accepts a cancel request but order is not terminal: retain reconciliation and do not report exit complete prematurely.
- Baseline/manual exposure is nonzero or changes during exit: preserve exact baseline, refuse uncertain close; reduce-only alone is not ownership proof.

## Pre-CI Verification

- Vue: 164 tests pass; typecheck and production build pass. Runtime dependencies
  have no reported vulnerabilities. The unchanged development dependency tree has
  four moderate advisories; the high-severity audit gate passes.
- Rust: pinned formatting and Windows GNU all-target Clippy pass. Native batch
  cancellation tests pass 20/20. Protection/read-failure regressions were observed
  failing before their fixes and passing afterward.
- Independent reviews found and closed stale-state starvation, funding-age gaps,
  stale/error funding-cache reuse, unfair admission, and read-blocked protective
  cancellation. Full-suite checks also caught and corrected two cleanup regressions.
- Windows GNU full library run: 1031 passed, 26 failed, one ignored. The failures
  below occur at the existing Windows atomic directory-publish boundary or its
  dependent start-count assertions. Linux full-suite success remains mandatory
  before deployment; these failures are not counted as a green local suite.
- No production order, position, protection limit, or trading configuration was
  modified during development. The observed live image remains `d37efb5` with zero
  active strategies before release validation.

### Windows File-Publish Failures

All names have the prefix `engine::runtime::tests::`:

- `abandoned_hidden_payload_is_reset_before_same_run_retry`
- `armed_advance_keeps_lease_while_waiting_and_activates_without_placing`
- `bybit_registry_pipelines_replacements_without_changing_manual_baseline`
- `cancelled_start_removes_its_hidden_reservation_and_allows_retry`
- `cancelling_an_armed_runtime_releases_market_ownership_atomically`
- `catalog_lease_blocks_stop_without_changing_the_strategy`
- `concurrent_same_run_start_has_one_durable_winner`
- `coordinator_bootstrap_failure_leaves_clean_catalog_and_allows_retry`
- `coordinator_scheduler_does_not_let_one_slow_market_block_another`
- `coordinator_scheduler_submits_one_opening_and_never_duplicates_it`
- `coordinator_serializes_same_market_start_through_persist_and_registration`
- `coordinator_start_freshness_includes_time_waiting_for_mutation_guard`
- `coordinator_stop_before_tick_is_durable_zero_write_and_allows_clean_restart`
- `duplicate_registry_entry_returns_the_rejected_runtime_without_replacing_owner`
- `duplicate_start_is_rejected_before_any_exchange_call`
- `in_flight_start_is_invisible_until_atomic_publish`
- `profit_protection_prepared_bridge_preserves_identity_and_maps_control_errors`
- `profit_protection_prepared_bridge_rejects_armed_updates_and_evaluation`
- `registry_pipelines_trade_xyz_replacements_and_preserves_stop_during_ack`
- `registry_rejects_a_second_live_strategy_for_the_same_market`
- `registry_rejects_overlapping_same_run_but_does_not_serialize_other_runs`
- `separate_coordinators_cannot_create_two_live_runs_in_one_catalog`
- `simultaneous_separate_coordinator_starts_never_both_win`
- `start_service_arms_with_only_one_public_market_read`
- `start_service_persists_active_runtime_without_placing_any_order`
- `stop_before_first_tick_never_submits_the_opening_order`
