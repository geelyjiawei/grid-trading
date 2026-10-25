# Funding-Inclusive Net Profit Implementation Plan

> Use executing-plans for integration and focused, disjoint adapter workers.

**Goal:** Deploy matching active-card and strategy net equity, including attributable settled funding, without touching orders or manual positions.

**Architecture:** Rust read-only account history adapters supply signed funding receipts. A pure attribution function replays strategy-owned fills at each settlement and excludes baseline/manual exposure. Existing risk snapshots publish one funding-inclusive total for both Vue views. No strategy schema or execution-loop changes.

**Tech Stack:** Existing Rust Decimal, async gateways and request governors; Vue 3 and Vitest. Build Linux image in CI, never on production.

## Constraints
- No placement, cancellation, position/leverage change, or automatic reconciliation.
- No fabricated zero funding on missing, truncated, conflicting or unsupported history.
- Positive funding is received income; negative funding is paid cost.
- Attribute only same-direction owned exposure not exceeding the confirmed account exposure. Mixed/ambiguous exposure fails closed.
- Funding reads are cached, bounded and outside execution locks.
- Production currently has zero active strategies. Check again immediately before cutover; refuse a restart if any strategy has started.
- Preserve all existing UI changes and production credentials/data.

## Tasks
- [ ] Add funding history types and default unavailable account-history capability; prove attribution regression fails.
- [ ] Add Binance/Aster signed read-only income and account-fill pagination with tests.
- [ ] Add Bybit/Hyperliquid settled-funding pagination with tests.
- [ ] Implement exact Decimal attribution, deduplication and ambiguity safeguards; test baseline/manual exposure and both payment signs.
- [ ] Cache and integrate Rust funding into risk responses; add an authenticated run-level funding audit endpoint for stopped-run verification.
- [ ] Show settled funding and identical funding-inclusive totals in Vue; reject incomplete data and test.
- [ ] Run formatter, focused Rust tests, full Linux CI and Vue tests/build; request independent review.
- [ ] Back up production data, verify image revision and stopped strategies, preflight with trading disabled, then deploy with rollback.
- [ ] Verify auth, health, API funding arithmetic, UI consistency, and unchanged manual orders/positions; report limits explicitly.

## Sources
- Binance signed income: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account
- Bybit account settlement log: https://bybit-exchange.github.io/docs/v5/account/transaction-log
- Aster income history: https://docs.asterdex.com/for-developers/aster-api/api-documentation.md
- Hyperliquid user funding: official API documentation and official SDK request schemas, to be verified before implementation.
