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

## Exchange protocols

- Binance order writes are HMAC-SHA256 signed, carry the immutable client order ID,
  and use the same canonical parameter bytes for signing and transport.
- Aster V3 order writes use one production wallet/private key. The wallet address is
  derived locally and used as both `user` and `signer`; only the EIP-712 signature is
  transmitted.
- Aster EIP-712 signs the canonical form body with domain `AsterSignTransaction`,
  version `1`, chain ID `1666`, and the zero verifying-contract address.
- Private keys and API secrets are never serialized, logged, included in request debug
  output, or stored in strategy state.
- Signed requests never follow HTTP redirects, because redirects could disclose signed
  parameters or authentication headers to another origin.
- A write transport error, HTTP timeout/rate limit/server error, redirect response,
  unknown-execution exchange code, malformed success body, or mismatched acknowledgement
  is an unknown outcome. The same client order ID must be reconciled before any retry.
- Only the exchange's definitive order-not-found code is `NotFound`; transport and
  malformed lookup responses remain inconclusive.
- A cancellation acknowledgement must match both immutable client and exchange order
  IDs. It confirms only that cancellation was accepted; terminal state still requires
  authoritative cumulative execution accounting.
- An execution snapshot is accepted only when one exact order lookup and the complete
  bounded account-trade pagination agree on exchange order ID, client order ID, symbol,
  side, cumulative quantity, and cumulative quote.
- A full account-trade page always requires a strictly advancing follow-up query. A
  duplicate trade ID, backward page, malformed row, pagination cap, or cumulative total
  mismatch remains inconclusive and can never be interpreted as a fill.
- Exchange commission is retained per trade in its original signed value and asset.
  Positive fee cost is aggregated by asset; BNB or any non-quote fee is never silently
  relabelled as USDT or included in quote-currency profit without explicit valuation.

## Position ownership

- Position at grid start is the baseline and is never silently absorbed by the grid.
- Strategy initialization obtains market price, instrument rules, and position directly
  from the selected exchange and accepts the bundle only when exchange and symbol
  identities all match.
- Existing one-way position quantity and entry price become an immutable baseline.
  Hedge-mode `LONG` and `SHORT` legs remain separate and cannot be netted into that
  baseline while the strategy ledger is one-way only.
- In one-way position mode, an opposite-side baseline and a directional grid cannot be
  isolated. That configuration is rejected instead of netting through the old position.
- A neutral grid requires a flat baseline unless the exchange adapter later provides an
  explicit hedge-position identity.
- Neutral fills maintain durable FIFO directional cost lots. Opposite fills close those
  lots first, realize exact PnL, and only an excess quantity opens a new direction.
- Neutral inventory may never retain long and short lots at the same time; its signed lot
  sum must exactly equal the exchange-facing grid-owned net quantity.
- Grid-owned position equals confirmed opening fills minus confirmed reducing fills.
- Manual or unexplained exchange position changes fail closed and never rewrite the
  local ledger.
- Stopping a grid does not market-close a retained position unless an explicit risk
  action requires it.
- Every authoritative position check must exactly equal baseline plus grid-owned position;
  mismatch fails closed and never rewrites either ledger component.

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
- Last price and mark price come from their distinct exchange fields. A missing mark
  price never falls back to last price, and stale or future-dated market snapshots are
  rejected before planning or risk evaluation.
- Tick size, limit quantity rules, market quantity rules, and minimum notional are parsed
  from one exact, currently trading symbol row. Missing, duplicate, malformed, or
  non-trading exchange rules fail closed without local defaults.
- Counter and cancelled-remainder obligations are created only while a strategy is
  deploying or running. Fills observed during stop, risk exit, failure, or finalization
  are still booked exactly, but can never schedule a normal grid replacement.

## Persistence and recovery

- State/history updates are atomic and durable.
- A waiting trigger has no grid plan, baseline, or order intent. Trigger activation uses
  fresh market data, fresh exchange rules, and the authoritative position at trigger time.
- Armed-to-active activation replaces one durable runtime state atomically. Any planning,
  rule, or baseline failure leaves the armed bytes unchanged and creates no order.
- Trigger direction is derived from the trigger price relative to the arming market, not
  guessed from long, short, or neutral grid direction.
- Initial deployment ownership is retained until every linked exchange order is
  reconciled and terminal.
- Failed, closed, stopped, and saved are distinct lifecycle states.
- A normal stop creates no market order and does not change baseline or grid-owned
  position. It waits for every submitted or uncertain order to become authoritative.
- Grid boundaries never trigger a market close. Only configured stop-loss or take-profit
  prices create a risk-exit request.
- A risk exit disables normal placement, waits for every submitted order to become
  authoritative, revalidates exchange rules and actual position, then prepares only the
  exact grid-owned quantity as an immutable reduce-only market intent.
- A partial or cancelled risk close never recreates its planned quantity. A subsequent
  intent can cover only the exact remaining grid-owned quantity.
- A late fill after a stopped or closed state is still booked to owned position and then
  escalated durably as a failure; it is never discarded.
- A restart cannot resume normal placement before fresh exchange rules and
  authoritative state are validated.
- Corrupt or incomplete storage fails closed and is retained for audit.
