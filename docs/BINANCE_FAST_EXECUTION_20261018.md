# Binance execution latency: fee decoupling and complete-stream wakeups

## Observed baseline

Read-only production audit, QUSDT run `8efd8c5927e1`, deployed revision
`f8a097937d8413b08d821ad6f7e817c215b4fc90`:

- Window: 2026-10-18 08:38:42.207 through 08:44:33.226, Asia/Shanghai.
- 196 strategy trades, 155 counter obligations, 153 unique replacement orders.
- First contributing exchange fill to replacement exchange creation: minimum
  40 ms, median 567 ms, mean 689.24 ms, P95 1584 ms, maximum 5416 ms.
- 29 replacements exceeded 1000 ms; seven exceeded 2000 ms.
- The 5416 ms sample waited for execution totals and then historical fee prices.
  Its placement log recorded zero pacer wait and a 4 ms WebSocket round trip.
- The 4 ms figure measures an already-submitted request, not end-to-end latency
  and not a guaranteed future service level.

## Correctness boundary

The exchange trade ID, immutable order identity, exact cumulative quantity,
quote totals, canonical chronology, per-cell capacity and durable intent
acknowledgement remain required. No synthetic trade, guessed commission price,
counter submission before durable inventory evidence, or cross-batch Binance
placement pipeline is introduced.

An incomplete price is not an incomplete fill. Foreign-asset commissions are
persisted with their exact raw amount and an explicit
`pending_historical_price` valuation. Only the known fee subtotal is included
in the internal numeric accumulator. API fee and net-profit values are null
while relevant pricing is pending; `fee_valuation_pending_count` distinguishes
this from zero fees. Gross volume and owned inventory remain exact.

The hot execution path performs no historical-price network request. Cached
validated minute prices and already-persisted valuations may be reused.
Background settlement snapshots a bounded batch, performs price requests
outside the strategy mutex, then commits only pending-to-valued changes. It
never replays inventory, resubmits an order, decreases a known fee, rewrites a
previously valued trade or changes the strategy position baseline. Work based
on an older partial fill is merged into the current audit without regressing
later fills. Failed prices remain pending across process restart and manual
strategy stop. Maintenance-only stopped runs must not own a trading market.

Settlement commits only the fee accumulator and fee audit. It does not call
the execution state transition again: terminal intent reconciliation may have
advanced while the older partial-fill audit is still the latest exact fill
evidence. Already-valued fees travel with background work and are never priced
again after restart. Conflicting common REST/stream trade IDs invalidate the
cache and remain inconclusive rather than silently choosing a commission.

## Stream/REST convergence

Late trades are retained even when their enclosing order header is older than
the latest header. Header totals never move backward. Authoritative REST
binding preserves newer session-local evidence and the immutable creation
time. A full update that advances a previously seen trade wakes execution
again; exact duplicate events do not.

A lagging REST trade page may be supplemented only by genuine fills from the
same uninterrupted user-stream session and immutable order identity. The union
must still exactly satisfy the untouched authoritative totals. If the stream
has already advanced beyond an in-flight REST read, a fully validated newer
bound snapshot is preferred. Incomplete/conflicting evidence remains blocked.

## Validation and release boundaries

Regression coverage includes missing prices with immediate counter creation,
invalid quantities staying blocked, idempotent fee settlement, stale partial
work, file reload, stop/restart, API null reporting, late stream trades,
same-trade full progress, conflicting REST/stream evidence and immutable
creation-time binding.

Release must use a tested prebuilt Rust/Vue image, not compile on production.
Do not restore stale strategy files after real exchange fills. An old Rust
image must validate the current state before rollback because it cannot read
the new pending valuation enum. Python is not a trading runtime or fallback.
Deployment may restart the Rust process without requesting strategy stop;
existing exchange orders remain live. Direct operational exchange access is
GET-only. Record protected non-strategy orders, unrelated positions, position
baseline and existing inventory evidence around deployment.

Post-deployment latency must use only fills after the verified image start,
exact source trade timestamps and unique corresponding exchange order creation
timestamps. Report outstanding/unmatched fills separately. Do not create test
trades to manufacture a favorable production latency sample.

### Local Windows validation caveat

The first complete Windows GNU run returned 755 passing and 23 failing tests.
The final Windows run after the review fixes returned 759 passing and the same
23 failures, with all new regression tests passing.
These failures share the existing strategy-directory publication/lease issue
(`Publish(PermissionDenied)`). The unchanged `f8a0979` source independently
reproduced `start_service_persists_active_runtime_without_placing_any_order`
with the same error (757-test baseline). Linux CI must pass the complete suite
before production deployment; these Windows failures are not counted as passes
or hidden by adding skips. The affected names in that run are:

```text
abandoned_hidden_payload_is_reset_before_same_run_retry
armed_advance_keeps_lease_while_waiting_and_activates_without_placing
cancelled_start_removes_its_hidden_reservation_and_allows_retry
cancelling_an_armed_runtime_releases_market_ownership_atomically
catalog_lease_blocks_stop_without_changing_the_strategy
concurrent_same_run_start_has_one_durable_winner
coordinator_bootstrap_failure_leaves_clean_catalog_and_allows_retry
coordinator_scheduler_does_not_let_one_slow_market_block_another
coordinator_scheduler_submits_one_opening_and_never_duplicates_it
coordinator_serializes_same_market_start_through_persist_and_registration
coordinator_start_freshness_includes_time_waiting_for_mutation_guard
coordinator_stop_before_tick_is_durable_zero_write_and_allows_clean_restart
duplicate_registry_entry_returns_the_rejected_runtime_without_replacing_owner
duplicate_start_is_rejected_before_any_exchange_call
in_flight_start_is_invisible_until_atomic_publish
registry_pipelines_trade_xyz_replacements_and_preserves_stop_during_ack
registry_rejects_a_second_live_strategy_for_the_same_market
registry_rejects_overlapping_same_run_but_does_not_serialize_other_runs
separate_coordinators_cannot_create_two_live_runs_in_one_catalog
simultaneous_separate_coordinator_starts_never_both_win
start_service_arms_with_only_one_public_market_read
start_service_persists_active_runtime_without_placing_any_order
stop_before_first_tick_never_submits_the_opening_order
```
