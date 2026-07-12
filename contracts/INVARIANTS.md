# Trading invariants

These rules are part of the compatibility contract even when they are not visible in
the OpenAPI schema.

## Numeric safety

- Price, quantity, notional, fee, and PnL calculations use decimal arithmetic.
- Exchange `tickSize`, `qtyStep`, `minQty`, and minimum notional are authoritative.
- Quantity is never guessed, inflated, or rounded upward to repair local state.

## Order ownership

- One client order ID represents exactly one immutable side/price/quantity/reduce-only
  shape.
- A write intent is durable before the exchange request.
- Timeout or malformed acknowledgement is `SUBMIT_UNKNOWN`, never a safe retry.
- An order is removed only after authoritative terminal status and complete execution
  accounting.
- A cancelled order is not equivalent to a filled order.

## Position ownership

- Position at grid start is the baseline and is never silently absorbed by the grid.
- Grid-owned position equals confirmed opening fills minus confirmed reducing fills.
- Manual or unexplained exchange position changes fail closed and never rewrite the
  local ledger.
- Stopping a grid does not market-close a retained position unless an explicit risk
  action requires it.

## Grid behavior

- Fixed quantity mode preserves the configured quantity for every grid level.
- Partial fills preserve exact remainders, including valid sub-minimum fragments.
- Completed legs restore the exact opposite order, including outside the configured
  range when that is the defined grid transition.
- A strategy is ready only when the entire initial target plan is represented.

## Persistence and recovery

- State/history updates are atomic and durable.
- Initial deployment ownership is retained until every linked exchange order is
  reconciled and terminal.
- Failed, closed, stopped, and saved are distinct lifecycle states.
- A restart cannot resume normal placement before fresh exchange rules and
  authoritative state are validated.
- Corrupt or incomplete storage fails closed and is retained for audit.
