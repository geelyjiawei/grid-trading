# Per-Run Net-Profit Protection

## Operator Controls

Select a live strategy, then use the net-profit protection panel. The initial
grid creation form and existing price-based protection settings are unchanged.
Amount settings are disabled by default, including for existing strategies.

- Enter a positive take-profit amount, stop-loss amount, or both, in the market's
  settlement asset (USDT or USDC). A stop-loss input of `10` means net <= `-10`.
- Leave one field blank to disable that side. Use the explicit disable button to
  clear both settings. Zero, negative, rounded, or overflowing values are rejected.
- Saving does not restart the strategy or synchronously place/cancel an order.
  A limit already crossed can trigger on the next verified background check.
- A separate settings revision prevents stale pages overwriting newer settings.
  Run identity is mandatory: a later run reusing the symbol is not interchangeable.
- Settings become locked once any exit has latched. A manual stop still means
  cancel strategy orders only; amount protection additionally exits owned inventory.

## Profit Definition

`net = gross realized - actual execution fees + owned unrealized + settled funding`

The calculation is shared with the account-facing API. Unrealized profit uses
the strategy's inventory, not the whole account's position PnL. Settled funding
requires attributable exchange receipts and execution ownership evidence. Unknown
fees, incomplete funding history, position deltas, or unmatched orders are not zero.
They make the protection observation unavailable and prevent a speculative close.

The trigger amount is not a guaranteed final liquidation profit. Subsequent
price changes, market execution, close fees, and slippage can change the result.
This is server-side monitoring, not an exchange-hosted stop order. Server failure,
network loss, exchange downtime, or missing authoritative data can delay or prevent
the exit. It is not a substitute for account-level isolation or exchange-native
emergency risk controls.

## Monitor and Exit Invariants

- Monitoring has a two-second per-run cooldown and at most two concurrent checks.
  Admission rotates between runs; this is not a fixed end-to-end latency guarantee.
- Immutable shared snapshots avoid deep-copying complete ledgers under the
  execution lock. Funding and exchange reads happen without holding that lock.
- After funding I/O, the monitor recaptures the same run/settings identity and
  reattributes raw funding against the current ledger. The short final exchange
  collection and durable trigger still require an unchanged state revision.
- Funding and position evidence must each be no more than five seconds old.
  Stale funding and cached funding errors force a bounded fresh read, including
  when the cached profit has not crossed a limit. A candidate crossing receives
  fresh funding verification before any exit is latched.
- Fees, ownership, exact quantities, deduplication, and durable-before-send
  requirements remain mandatory. Repeatedly changing/incomplete data is visibly
  unavailable/retrying; the monitor never pauses executions to manufacture a
  matching snapshot.
- A trigger and its amount, mark, time, and settings revision are persisted before
  cancellation. Existing risk-exit recovery handles restarts and repeated ticks.
- Only exact accepted order identities from this run are cancellation targets.
  An exchange cancellation acknowledgement is not terminal/fill confirmation.
- Once all relevant orders and racing fills are reconciled, the existing exit
  state machine derives the exact remaining grid-owned quantity from the ledger,
  verifies the account position, and uses reduce-only market close orders.
- The startup manual-position baseline and external orders are not exit targets.
  Opposite-direction baselines are rejected by existing validation. Arbitrary
  simultaneous external netting can make ownership unverifiable; then closing is
  blocked rather than guessing. Separate accounts/subaccounts provide stronger
  isolation than attribution in one net position.

## Batch Cancellation

The engine sends the complete currently known owned target set in one exit sweep;
the placement-per-tick cap no longer throttles cancellation. Adapters split wire
requests according to their contracts:

| Adapter | Targets Per Request | Dispatch |
| --- | --- | --- |
| Binance USD-M | 10 | Up to three requests concurrently |
| Aster wallet v3 | 10 | Native exact-ID batches |
| Bybit linear | 20 | Native exact-ID batches with per-row status checks |
| Hyperliquid / TRADE.XYZ | 100 local safety bound | Signed cancel arrays, existing WS relay |

There is no whole-symbol cancel-all fallback. Batches are not atomic across the
exchange; partial/unknown outcomes remain individually reconcilable. One user
action can initiate every owned cancellation without implying one wire request or
instantaneous completion.

Official endpoint contracts:
[Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Multiple-Orders),
[Aster v3](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md#cancel-multiple-orders-trade),
[Bybit](https://bybit-exchange.github.io/docs/v5/order/batch-cancel),
[Hyperliquid](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#cancel-order-s).

## Authenticated API

`GET /api/grid/profit-protection/{symbol}?exchange=binance&run_id=RUN00001`

`POST /api/grid/profit-protection/{symbol}?exchange=binance`

```json
{
  "run_id": "RUN00001",
  "expected_revision": 0,
  "take_profit_amount": "100.000000000000000001",
  "stop_loss_amount": "50"
}
```

Both amount keys are required and must be exact decimal strings or null. Responses
include configuration, current observation, lifecycle, and persisted trigger.
Conflicting run/settings versions return 409; absent runs return 404. Controls
require the existing authenticated web session and retain request-body limits.

## Deployment Boundaries

Use the CI-built immutable Linux image, never a production-host Rust compilation.
Release requires Linux full-suite success, an isolated non-trading startup and
read-only legacy-ledger validation. Take a backup and refuse cutover if any live
strategy exists or protected ledger/config hashes change. Preserve manual account
observations; do not create real test orders or enable protection during release.
