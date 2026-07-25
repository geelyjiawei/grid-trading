# Exchange testnet verification

The testnet runner is isolated from the production server:

- It always builds `ExchangeEnvironment::Testnet`.
- It reads only dedicated `*_TESTNET_*` credential names.
- It does not mount production state or configuration files.
- Order writes are disabled unless `GRID_TESTNET_CONFIRM=TESTNET_ORDER_CYCLE`.
- Every order cycle places one post-only limit buy below mark price, confirms the
  exact exchange identity, cancels that exact order, confirms `Cancelled`, checks
  that it is absent from open orders, and verifies that position exposure did not
  change.

## Public connectivity

This does not need credentials and does not write to an exchange:

```sh
sh scripts/testnet-public-smoke.sh
```

It checks the official testnet REST endpoints for Binance Futures, Bybit, Aster,
and Hyperliquid, which backs TRADE.XYZ.

## Private read-only verification

Create the ignored secret file on the server:

```sh
cp .env.testnet.example .env.testnet
chmod 600 .env.testnet
```

Edit `.env.testnet` on the server. Do not send its contents in chat and do not
add it to Git. Select exactly one exchange and provide only that exchange's
dedicated testnet credentials.

Use a symbol that is listed on the selected testnet. Binance, Bybit, and Aster
symbols normally use a `USDT` suffix. TRADE.XYZ symbols use a `USDC` suffix.

Run the private read-only snapshots:

```sh
docker compose -f docker-compose.testnet.yml run --rm exchange-testnet-smoke
```

This verifies the actual account, market, contract rules, position, fee-rate,
and open-order responses through the production Rust adapter without placing an
order.

## Real order and cancellation cycle

Set these values only in `.env.testnet`:

```text
GRID_TESTNET_MODE=order_cycle
GRID_TESTNET_CONFIRM=TESTNET_ORDER_CYCLE
GRID_TESTNET_ITERATIONS=3
```

Then run:

```sh
docker compose -f docker-compose.testnet.yml run --rm exchange-testnet-smoke
```

The JSON result includes placement, active confirmation, cancellation request,
terminal confirmation, and total elapsed milliseconds for every iteration.
The command exits nonzero on an identity mismatch, unexpected fill, lingering
open order, inconclusive cancellation, or position exposure change.

## Testnet credentials

- Binance requires a Binance Futures Testnet API key and secret.
- Bybit requires a Bybit Testnet API key and secret.
- Aster requires a testnet wallet private key and may require testnet access to
  be approved by Aster.
- TRADE.XYZ requires a Hyperliquid testnet account address and an authorized
  testnet Agent private key.

Production credentials are not accepted as a substitute for testnet
credentials. Keep withdrawal permissions disabled where the platform exposes
such a permission.
