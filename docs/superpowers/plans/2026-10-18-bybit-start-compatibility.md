# Bybit Startup Compatibility Repair

## Evidence And Scope

- Production `b39ba76` rejected Bybit QUSDT startup before scheduling: `invalid Bybit position snapshot: Bybit response contains duplicate records`.
- Signed GET returns one exact-symbol, one-way flat row (`positionIdx=0`) with nonempty cursor metadata and fractional leverage `12.5`. Cursor presence is not evidence of duplicate identities.
- Follow-up cursor GET returns an empty-position placeholder for that same symbol/index. Do not merge it over the first authoritative row or silently overwrite leverage.
- No Bybit strategy was created. Existing Binance QUSDT and all manual orders/positions must remain untouched by diagnostic actions.

## Implementation

1. Reproduce cursor and fractional leverage failures with real codec inputs and leverage-preflight regression tests.
2. For the existing exact-symbol position query, validate metadata types but identify duplicates using actual symbol/index records. Preserve all foreign-symbol, duplicate-index, mixed-mode, empty-payload and malformed-value rejection.
3. Store observed leverage losslessly as Decimal. Keep requested grid leverage and leverage writes integer-valued; compare exact observed leverage before any change and after acknowledgement. Never truncate fractional values or change leverage on a nonzero position.
4. Preserve legacy integer deserialization and public integer JSON representation. Add regression coverage for flat fractional leverage setup, nonzero manual-position protection, wrong acknowledgement and verification mismatch.
5. Run focused RED/GREEN, full Linux workspace CI, formatting, strict Clippy and independent review. Build in CI, never on the production server.
6. Deploy only the verified Rust image with backups and account-state guards. Validate live Bybit read-only snapshots through the Rust adapter without starting a grid, submitting orders, changing account mode/leverage, or cancelling manual orders.

## Acceptance

- Real cursor metadata and `12.5` leverage parse correctly; fake duplicates and unsupported hedge mode remain fail-closed.
- Integer legacy snapshots retain their meaning and JSON compatibility.
- Existing-position fractional leverage mismatch performs zero exchange writes.
- Production positions/orders are observed, not adjusted to make checks pass.
- Read-only startup prerequisites passing is not presented as proof that a live strategy was started.

## Local Verification

- Four initial `bybit_start_` regressions failed on the original implementation for the reported cursor and precision reasons.
- Ten focused compatibility/safety tests now pass, including the combined signed Bybit adapter fixture, malformed input, hedge/manual-position protections, exact post-write verification, and legacy integer serialization.
- Windows GNU full workspace library run: 818 passed, 23 known Windows directory-publication `Publish(PermissionDenied)` failures. These are not counted as passes; the Linux full-workspace CI remains the release gate.
- Strict workspace/all-target Clippy, formatting, and independent static review passed with no new P1/P2 findings.
- The exact-symbol first response is a bounded compatibility decision, not an official guarantee that all cursor-bearing responses are complete. This exception must not be reused for symbol-less positions, orders, executions, or instruments.
