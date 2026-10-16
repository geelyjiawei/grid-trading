# Grid Cell Capacity

## Invariant and Root Cause

New plans use `allocation_version: 2`. A grid with N intervals contains exactly N initial limit allocations, one per interval. In fixed quantity mode, the circulating capacity of each interval is at most the configured per-grid quantity. For N=20 and quantity=0.2 the total is 4.0, not 4.2.

The original planner independently tested the two ends of an interval against the reference price. A reference strictly inside an interval satisfied both tests. This produced a profit allocation and an add allocation in the same interval; checking only distinct interval IDs hid the duplicate. The duplicated chains could later merge into one 0.4 replacement.

Directional grids assign the crossing interval to the opening inventory/profit leg, never also to an add leg. Neutral grids select one side at the interval midpoint (ties select Buy). References exactly on a level remain fully covered.

Circulating capacity is the sum of all nonterminal grid order remainders and all unassigned effective replacement obligations in the interval. Assigned obligations are represented by their order and are not counted twice. Partial fills may create several exchange orders, but their aggregate quantity cannot exceed the interval capacity. Sub-minimum deferred obligations still count.

## Historical Compatibility

Missing `allocation_version` means version 1. Historical plan replay remains deterministic; existing executions, initial orders and accounting events are not rewritten. The frozen cross-language v1 replay intentionally retains the old Python semantics. Version 2 replay and production starts use the corrected planner and have independent uniqueness tests.

Legacy fixed-quantity strategies cannot submit new grid orders while a cell exceeds the configured quantity. Merely loading an old snapshot does not cancel its accepted orders. Stop and risk-close paths retain their existing semantics.

## Authorized Legacy Retirement

A `RestoreCancelledRemainder` obligation can explicitly retire quantity only in strategy schema version 2. Historical source and obligation shapes stay immutable; effective replacement quantity equals original obligation quantity minus `retired_quantity`.

Retirement requires a confirmed cancelled non-reduce-only limit order with zero executions and a valid exchange execution audit. Only the excess proven by a duplicated directional version-one crossing cell can be retired. Retirement is quantity-step aligned, nonnegative and bounded cumulatively across descendants. Counter-fill obligations cannot be retired. Old binaries reject strategy schema 2 instead of silently ignoring retirement and recreating excess orders.

`scripts/repair-anthropic-capacity.py` is deliberately restricted to run `dab0d666a310`, client `r_dab0d666a310_5_S_80`, exchange order `235064031`, Sell 1932, quantity 0.4, retaining 0.2. It defaults to read-only and refuses changed identity, fills, other strategies, positions or order sets.

The operator workflow verifies the immutable image, quiesces the bot (without cancelling other exchange orders), backs up data and fsyncs both archive and directory entries, validates candidate ledgers in a no-network container, journals the exact cancellation before issuing it, confirms the result by exchange GET, validates again and commits both ledgers while no runtime can read intermediate state. Atomic replacements preserve existing ownership and permissions. The new runtime submits only the retained Ready order through its normal GTC path.

On any failed post-quiesce verification or caught SIGTERM/SIGHUP/SIGINT the runtime remains paused; there is no compensating position trade. Uncatchable power loss/SIGKILL still requires inspection of the durable phase journal before any restart. Interrupted repairs require inspection of the private backup receipt and phase journal before any restart. Do not restore an old binary against repaired schema-2 data. Never infer cancellation success from a missing order or retry placement blindly.

## Verification

- Original production regression: 21 allocations instead of 20, observed failing before the fix.
- Fixed-grid matrix: three directions, arithmetic and geometric grids, 199 reference prices each (1,194 cases), 20 unique intervals, 0.2 each.
- Durable partial-fill/cancellation/restart and retirement bounds, audit identity and immutable baseline checks.
- Frozen v1 and corrected v2 replay integration tests.
- Maintenance guards, Python optimization, file ownership preservation and failure quiescence tests.
- Release gate: complete Linux Rust/Python/Vue suites, Clippy, formatting, container and Compose smoke checks.

Production cancellation uses Binance's official `DELETE /fapi/v1/order` for one exact order ID, followed by `GET /fapi/v1/order`; it never uses symbol-wide cancellation or a market/close-position request. Official reference: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade
