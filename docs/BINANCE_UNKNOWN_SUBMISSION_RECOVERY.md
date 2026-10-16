# Binance unknown submission recovery

An order WebSocket disconnect after writing a request is an unknown outcome,
not a rejection. Previously, an opening intent stayed unknown indefinitely even
when Binance could not find it. That blocked both grid deployment and a later
manual stop.

## Recovery contract

- Normal accepted orders keep the existing fast path.
- Reconciliation is scoped to the persisted exchange, symbol, and client order ID.
- An unknown or prepared Binance intent waits at least its receive window plus
  five seconds, with a ten-second minimum, before absence confirmation.
- Absence requires an exact order lookup, a complete `allOrders` time window
  covering the submission, and another exact lookup after the history read.
- A matching live or filled historical order is recovered and accounted instead
  of being submitted again. Truncated, malformed, unavailable, or expired history
  cannot authorize a retry. The diagnostic retains the actual lookup error.
- History-based absence is restricted to intents created within 24 hours,
  inside Binance's retention window for unfilled cancelled orders.
- Confirmed absence is persisted as the existing `retryable_not_submitted`
  state; it does not fabricate an exchange order ID or a cancellation.
- A running startup may retry the exact intent only through the existing full
  instrument, market, and position reconciliation path. An execution wakeup
  cannot bypass that check.
- A persisted manual stop finishes once every owned order is accounted or
  confirmed absent. It never retries the opening or sends a position close.
- A manual position difference does not prevent that manual stop from finishing.
  It still prevents a startup retry from trading into an unexpected position.

Unknown submission timestamps include time spent awaiting the gateway, so a
slow submission cannot consume the subsequent confirmation grace in advance.
This applies to individual and batched submissions. No database migration or
manual editing of strategy state is required.

## Verification

Regression tests cover absent opening recovery, unchanged identity on retry,
stop convergence with an external 0.5 short and a manual pending order, position
drift blocking retries, late visibility, filled orders found in history, extended
receive windows, API failure, invalid history, pagination limits, retention
limits, and slow submission timestamps.

Windows GNU runs hit pre-existing directory-publish permission failures in 23
runtime/catalog tests. The release gate uses the complete Linux Rust suite,
Clippy, formatting, frontend checks, and container smoke tests in GitHub Actions.
Production must use a prebuilt immutable image; do not compile Rust on the
trading server.

## API references

- [Binance All Orders](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/All-Orders)
- [Binance Query Order](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Order)
- [Binance WebSocket New Order](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/New-Order)

The client order ID is unique among open orders, not a permanent deduplication
key for filled orders. Reusing it alone is not sufficient evidence for a retry.
