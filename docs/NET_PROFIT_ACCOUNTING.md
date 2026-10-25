# Strategy Net Equity

The active-strategy card and real-time strategy view consume the same matched Rust risk snapshot. Neither view falls back to realized-only profit or the account's merged PnL.

```
total_equity_profit = gross_realized_profit - total_fee
                    + grid_unrealised_pnl + funding_net_income
```

- Fees are actual or reconciled strategy execution fees, deducted exactly once. Pending fee valuations prevent a final total.
- Unrealized PnL values only this run's owned inventory, not the baseline or manual positions.
- Settled funding is receive-positive and pay-negative. No forecast funding rate, account-wide balance change, or future closing fee is substituted.
- Both views show an unknown value when ownership, exact executions, funding history, fees or the current risk identity cannot be verified. Unknown is not zero.
- The selected card uses exactly the risk object supplied to the real-time view. Other cards refresh serially, at most every 30 seconds. Profit snapshots expire after 60 seconds.

## Funding Evidence

Binance and Aster read signed income history (`FUNDING_FEE`) plus all account executions in the interval. Bybit reads the unified account transaction log's signed `funding` and settlement `size`. TRADE.XYZ reads Hyperliquid `userFunding` for the master account and exact resolved `xyz:` coin, using `delta.usdc` and `delta.szi`.

At every settlement, the strategy's inventory is reconstructed using exact exchange execution timestamps and trade/order identities. Actual funding is shared according to the strategy's same-direction quantity, excluding baseline inventory. Receipts and executions are deduplicated; conflicting evidence, a saturated/truncated page, missing fields and ambiguous equal-millisecond ordering fail closed.

Legacy strategy baselines do not include an exchange observation timestamp. Therefore Binance/Aster funding attribution rejects external executions between creation and the last settlement rather than replaying them from an uncertain starting balance. Existing manual positions with no such executions can still be excluded proportionally. Receipt-position exchanges require the settlement position to match baseline plus owned inventory. Mixed/opposite exposure or incomplete owned executions cannot be assigned silently.

Funding history is cached for 60 seconds (including errors), limited to eight entries per shared gateway. Bulk Binance funding/trade history uses the normal request lane with documented weights; exact order execution reads keep their trading-critical priority. Normal reads cannot consume the last 150 weight of the conservative shared budget, preserving capacity for critical execution checks. A local weight refusal rejects only that request; actual exchange bans/rate limits and all rolling order limits remain enforced. The inner 12-second deadline caches a timeout before the API's 15-second outer guard. Funding is not part of order placement, cancellation, quantity validation, durable order intent or execution accounting. A funding display failure does not trigger a strategy risk exit.

History retention and pagination bounds are explicit. An unavailable exchange history is never treated as an empty verified result. Long-running runs outside exchange retention need a future durable funding ledger; this release does not manufacture historical funding.

## Read-Only Audit

`GET /api/grid/funding/{run_id}` requires the existing authenticated web session. It loads one exact terminal run and returns only attributable settled funding through its stop/update timestamp. Nonterminal runs return 409: live strategies must use the authoritative risk view, not a standalone possibly lagging ledger audit. No endpoint in this change places, cancels or adjusts an order or position.

## Verification And Release

Tests cover both funding signs, fee non-duplication, baseline exclusion, unknown values, exact identity, receipt/fill conflicts, pagination, request priorities and cache bounds. Vue integration tests compare both profit displays, including losses and delayed/failed snapshots. Linux CI runs the full Rust and Vue suites and isolated container checks before publishing a revision-labelled image.

Production rollout uses that prebuilt image, never an on-host Rust build. It refuses nonzero active strategies, backs up production data, preflights with trading disabled, rechecks immediately before cutover, changes only the Rust service, and compares protected ledger/configuration hashes and read-only account observations. No strategy is stopped or resumed to make a rollout pass.

The funding feature does not rewrite legacy strategy snapshots, historical realized-profit fields or previous strategy history rows. Those remain realized-only unless a response explicitly supplies the funding-inclusive total.
