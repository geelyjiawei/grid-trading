# Trading invariants

These rules are part of the compatibility contract even when they are not visible in
the OpenAPI schema.

## Numeric safety

- Price, quantity, notional, fee, and PnL calculations use decimal arithmetic.
- Exchange `tickSize`, `qtyStep`, `minQty`, and minimum notional are authoritative.
- Quantity is never guessed, inflated, or rounded upward to repair local state.
- Maker and taker rates used for a new strategy preview come from one exact signed
  account-rate response for the selected exchange and symbol. Browser-supplied rates
  are estimates only and are replaced before an effective strategy config exists.
- Missing, ambiguous, foreign-symbol, or malformed fee-rate responses block activation.
  Completed-trade accounting continues to use each exchange execution's actual fee and
  fee asset; configured rates never overwrite historical execution evidence.

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
- Cancellation has its own write-ahead record in the same atomic intent ledger. A retry
  is allowed only for the identical exchange/client/order target after an exact lookup
  still shows that order active; an acknowledged cancellation is never sent twice.
- Acknowledged cancellations that remain active do not consume the bounded dispatch
  slots needed by later orders. Every active order eventually receives its own durable
  cancellation attempt without queue starvation.
- An execution snapshot is accepted only when one exact order lookup and the complete
  bounded account-trade pagination agree on exchange order ID, client order ID, symbol,
  side, cumulative quantity, and cumulative quote.
- A full account-trade page always requires a strictly advancing follow-up query. A
  duplicate trade ID, backward page, malformed row, pagination cap, or cumulative total
  mismatch remains inconclusive and can never be interpreted as a fill.
- Exchange commission is retained per trade in its original signed value and asset.
  Positive fee cost is aggregated by asset; BNB or any non-quote fee is never silently
  relabelled as USDT or included in quote-currency profit without explicit valuation.
- A non-quote commission is valued only from the exact one-minute candle containing that
  trade, on the same exchange and against the configured quote asset. Missing or
  mismatched historical candles block accounting; current prices are never substituted.
- The execution-to-strategy bridge independently revalidates order identity, lifecycle,
  trade IDs, quantities, quotes, fee assets, and per-asset totals before producing one
  cumulative quote-currency fee value.
- Execution lookup and fee valuation complete before the strategy state is cloned for its
  single atomic transition. A transport, identity, valuation, or persistence failure
  leaves position, fees, lots, pair counts, and replacement obligations unchanged.
- Retrying the same complete cumulative execution snapshot after a persistence failure is
  idempotent; it cannot double-count a fill, fee, lot, completed pair, or replacement.
- Complete cumulative exchange fills and their per-trade fee valuations are embedded in
  the same atomic strategy-state file as the resulting position and PnL transition.
  There is no second audit file that can commit before or after the strategy state.
- A later snapshot may append new trade IDs but can never alter or remove an already
  audited trade or valuation. Evidence mutation fails the strategy closed while retaining
  the last valid audit record.

## Position ownership

- Configured leverage is not trusted from the browser request alone. Before activation,
  the selected exchange and symbol must report the exact requested one-way leverage.
- Leverage is changed only when the authoritative position snapshot differs. A successful
  change acknowledgement is followed by another authoritative snapshot before any order
  may be prepared. A timeout is resolved only by observing the exact requested leverage;
  otherwise activation remains blocked.
- An empty Bybit position list for an explicitly requested symbol is malformed, not proof
  of a flat position. Missing leverage and hedge-mode leverage cannot be silently converted
  into one-way strategy settings.
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
- A position mismatch observed between execution and position reads blocks all placement
  for that tick but does not immediately corrupt or permanently fail the strategy. The
  next tick replays authoritative executions first; normal fill/read races can converge,
  while a persistent unexplained delta remains blocked for operator review.

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
- Every runtime tick reconciles all durable order intents and applies complete execution
  accounting before it reads fresh market/rule/position inputs or submits another order.
- A prepared, unknown, rejected, conflicted, or incompletely accounted intent blocks all
  later submissions in that tick. Batch placement stops at the first unknown or rejected
  result; later grid orders are not sent speculatively.
- If the exchange accepts an order but either the intent ledger or strategy-state commit
  fails, the next tick performs authoritative client-ID reconciliation before submission.
  The immutable client order ID is never submitted a second time.
- Fresh exchange instrument rules must exactly match the rules that produced the durable
  plan. Any change fails the strategy before a new order is placed; an existing plan is
  never silently requantized under new rules.
- A waiting trigger has no grid plan, baseline, order intent, private account read, or
  leverage write. Trigger activation first confirms the condition from fresh public market
  data, then uses fresh account fee rates, verified leverage, exchange rules, and the
  authoritative position at trigger time.
- A non-triggered strategy can produce its first durable state only after authoritative
  fee replacement, leverage verification, fresh market/rules, and baseline validation all
  succeed. Failure in any stage produces no order intent.
- First persistence uses an operating-system exclusive create and can never overwrite an
  existing run. A failed or interrupted first write is retained as blocking evidence and is
  never reset to an empty strategy. The runtime can observe a new strategy only after the
  complete prepared state has been durably written.
- Every active or armed strategy has one independent operating-system runtime lease. A second
  process must fail before loading or mutating either ledger, and a crashed owner releases the
  lease automatically without deleting audit evidence.
- Runtime state, order-intent ledger, and lease paths are derived from one validated run ID.
  Loading acquires the lease first, then verifies the persisted run ID and cross-ledger ownership;
  any mismatch prevents the runtime from becoming visible and performs no exchange operation.
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
