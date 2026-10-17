# Binance replacement tail latency investigation (2026-10-17)

## Scope and evidence

Only Binance `ANTHROPICUSDT` run `dab0d666a310` is the production observation
target. No manual order cancellation, position closing, or position adoption is
part of this work. Existing Vue work is unrelated and excluded from the release.

The 2026-10-17 08:15 CST read-only audit matched Binance `userTrades` trade IDs,
quantities, timestamps and source order IDs against durable inventory events.
Their `(source_client_order_id, applied_at_ms)` groups exactly matched all 99
counter obligations. Those obligations map to 83 distinct replacement orders in
Binance `allOrders`. Opening and cancellation-restoration orders are excluded.
Neither response reached the 1,000-row page limit (106 orders, 162 trades).

Latency is replacement `allOrders.time` minus the earliest contributing
`userTrades.time`, counted once per replacement order. All samples predate the
2026-10-16 19:44 deployment. Median 160 ms; mean 438.2 ms; nearest-rank P95
1,432 ms; maximum 3,725 ms. Nineteen orders exceeded one second.

| New order ID | Total ms | Fill to ledger reference ms | Reference to order ms |
| --- | ---: | ---: | ---: |
| 234709095 | 1432 | 1053 | 379 |
| 234734806 | 1315 | 1056 | 259 |
| 235045139 | 1175 | 5 | 1170 |
| 235045156 | 1234 | 5 | 1229 |
| 235045330 | 1294 | 5 | 1289 |
| 235045493 | 1354 | 5 | 1349 |
| 235045554 | 1413 | 5 | 1408 |
| 235045574 | 1474 | 5 | 1469 |
| 235045584 | 1534 | 5 | 1529 |
| 235046245 | 3725 | 3459 | 266 |
| 235052170 | 1112 | 5 | 1107 |
| 235052309 | 1172 | 5 | 1167 |
| 235052628 | 1413 | 5 | 1408 |
| 235052802 | 1473 | 5 | 1468 |
| 235052437 | 1234 | 6 | 1228 |
| 235052510 | 1293 | 5 | 1288 |
| 235052590 | 1353 | 5 | 1348 |
| 235053702 | 1149 | 810 | 339 |
| 235055215 | 1043 | 661 | 382 |

Ledger timestamps are the processing tick's reference time, NOT independent
measurements of cache arrival, accounting completion, or socket writes. The old
container was replaced and its detailed logs are unavailable. These values
cannot establish the exact network/queue/persistence split for every order.

## Reproduced causes

1. Fourteen slow orders belong to two same-fill bursts with approximately 60 ms
   steps. `SharedConfiguredExchangeGateway` imposed a 60 ms placement pacer on
   top of Binance's existing shared REST/WebSocket rolling-window budget. Twenty
   reservations at one instant mechanically added 1,140 ms to the last slot.
2. Execution-critical REST order/trade snapshots also shared the ordinary-query
   60 ms pacing deadline despite having separate bounded network lanes and the
   same weighted rolling-window protection. A cold 20-order fill burst can need
   40 detail/trade reads, mechanically adding about 2.4 seconds before accounting
   and replacement placement even after the outer placement pacer is removed.
3. Trade-ID deduplication suppressed `ORDER_TRADE_UPDATE` after `TRADE_LITE` for
   the same fill. The lite wakeup can observe the previous cached snapshot; when
   the full cache update arrives its wakeup was discarded, leaving a periodic
   scan to detect progress. This event-ordering defect is reproduced by a failing
   regression test; historical logs cannot prove which of the five other samples
   encountered it.

## Narrow fix and safety boundaries

- Remove the redundant Binance placement pacer only. Aster, Bybit and TRADE.XYZ
  pacing are unchanged. Binance's shared 10-second/minute budgets, 75% headroom,
  exchange-provided limit updates and cooldowns remain enforced below the adapter.
  REST fallback still uses its existing request governor.
- Apply the REST governor's fixed minimum interval only to ordinary queries.
  Critical execution/order requests still reserve every weight/order unit, check
  the same cooldowns and rolling windows, and use the same four-request in-flight
  cap. They do not consume or wait for the ordinary-query spacing deadline.
- Keep Binance event batches serialized per market so a child fill cannot
  overtake persistence of its parent's placement acknowledgement. The existing
  within-batch placement concurrency remains four; without the redundant pacer
  this can submit a valid burst without adding 60 ms for every later order.
  Cross-batch pipelining was evaluated but rejected at review because an early
  child fill could otherwise be dropped before its exchange order ID is bound.
  TRADE.XYZ's pre-existing dispatch pipeline is not expanded by this release.
- Permit one full-update wakeup after an earlier lite wakeup; suppress repeated
  lite/full events and late lite events after a full update. Trade-ID accounting,
  obligation allocation and cell-capacity validation remain unchanged.
- Add order/request identity and wakeup timestamps to existing timing logs. Never
  log API keys, signatures or full signed request payloads.

Official reference: [Binance USD-M WebSocket order API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade).
Limits are window budgets, not a mandatory 60 ms interval between every order.

## Verification and release gates

Regression tests cover a 20-order pacer burst, Binance child-fill deferral until
the parent acknowledgement, lite/full ordering and duplicates, and TRADE.XYZ's
existing stop and duplicate-fill behavior during acknowledgement. Additional
tests exercise a 20-snapshot critical burst behind a reserved normal query
deadline and prove ordinary-query spacing is unchanged. Existing
quantity conservation, ownership, unknown-outcome and rate-limit suites remain
release gates. Local Windows file-backed tests have an existing atomic directory
publication permission failure; the full Linux CI suite is authoritative for
production deployment, not a reason to silently skip those tests.

Build the immutable image in GitHub CI, not on the 2-core/4-GB production server.
Validate the real persisted strategy with writes/network disabled before restart.
Back up after quiescing the process, without requesting strategy stop or touching
exchange orders. Compare exchange open-order identities/quantities, positions,
state accounting and per-cell capacity before/after deployment.

Do not claim a new production latency distribution until actual new fills exist.
Synthetic tests prove the removed waits and safety properties, not a guaranteed
sub-second exchange response under disconnects, throttling or network failures.
