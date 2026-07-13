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
- A missing client order ID lookup is inconclusive and never authorizes a replacement
  order.
- Reconciliation accepts an exchange order only when its exchange, client ID, order
  ID, side, price, quantity, reduce-only flag, type, and time-in-force are consistent
  with the immutable intent.
- Any reconciliation identity or shape mismatch is a durable ownership conflict that
  stops automatic handling for that intent.
- An order is removed only after authoritative terminal status and complete execution
  accounting.
- A cancelled order is not equivalent to a filled order.
- Intent state is monotonic. Accepted or terminal orders can never regress to prepared
  or unknown states.
- A terminal exchange label without cumulative execution accounting remains unresolved
  and blocks final strategy shutdown.

## Position ownership

- Position at grid start is the baseline and is never silently absorbed by the grid.
- Grid-owned position equals confirmed opening fills minus confirmed reducing fills.
- Manual or unexplained exchange position changes fail closed and never rewrite the
  local ledger.
- Stopping a grid does not market-close a retained position unless an explicit risk
  action requires it.

## Grid behavior

- Fixed quantity mode preserves the configured quantity for every grid level.
- A fixed quantity that is not an exact exchange quantity step is rejected before
  submission; it is never silently reduced or redistributed.
- Directional opening quantity equals the exact sum of the profit-side grid legs at
  the exchange-normalized opening reference price.
- A limit opening reference is the actual exchange-quantized order price, not the
  unrounded user input or a later ticker value.
- Grid levels that collapse after exchange tick-size quantization are rejected before
  any opening exposure is created.
- Partial fills preserve exact remainders, including valid sub-minimum fragments.
- Completed legs restore the exact opposite order, including outside the configured
  range when that is the defined grid transition.
- A strategy is ready only when the entire initial target plan is represented.
- If an initial target is cancelled, the strategy remains in deployment until the exact
  replacement chain is accepted; the cancelled order itself never counts as coverage.
- Replacement orders exactly equal their assigned durable obligations. Quantity, side,
  price, reduce-only, level, and exchange rules are revalidated on every state write.

## Persistence and recovery

- State/history updates are atomic and durable.
- Initial deployment ownership is retained until every linked exchange order is
  reconciled and terminal.
- Failed, closed, stopped, and saved are distinct lifecycle states.
- A normal stop creates no market order and does not change baseline or grid-owned
  position. It waits for every submitted or uncertain order to become authoritative.
- A late fill after a stopped or closed state is still booked to owned position and then
  escalated durably as a failure; it is never discarded.
- A restart cannot resume normal placement before fresh exchange rules and
  authoritative state are validated.
- Corrupt or incomplete storage fails closed and is retained for audit.
