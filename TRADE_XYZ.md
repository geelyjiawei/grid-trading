# TRADE.XYZ (Hyperliquid HIP-3) Guide

TRADE.XYZ is integrated through Hyperliquid's public API as the HIP-3 perpetual DEX named `xyz`. It is not treated as a separate HMAC exchange.

## Configuration

- Select `TRADE.XYZ` in the exchange settings dialog.
- Enter the Hyperliquid master account address (`0x...`).
- Enter a private key for an Agent wallet authorized by that account. Using an Agent is recommended so the account wallet key does not need to be stored on the server.
- Choose Mainnet or Testnet before saving.
- Symbols use the local `BASEUSDC` form. For example, `MUUSDC` maps to Hyperliquid coin `xyz:MU`.

The server verifies the Agent relationship and reads an account balance before it encrypts and saves the configuration. Credentials for every exchange are independent; Binance, AsterDEX, Bybit, and TRADE.XYZ can be configured and used separately.

Environment configuration is also supported:

```dotenv
GRID_EXCHANGE=trade_xyz
TRADE_XYZ_ACCOUNT_ADDRESS=
TRADE_XYZ_AGENT_PRIVATE_KEY=
TRADE_XYZ_TESTNET=false
```

Real values belong only in the server's untracked `.env` or encrypted `GRID_CONFIG_FILE`. They must never be committed to GitHub.

## Account Modes

- Standard accounts are supported and use the isolated `xyz` clearinghouse balance.
- Unified accounts are supported and use the authoritative spot USDC balance/holds plus `xyz` unrealized profit.
- Portfolio Margin is rejected before trading because a USDC-only grid cannot safely value the full collateral portfolio.
- Legacy DEX abstraction is accepted for compatibility but is not recommended by Hyperliquid.
- Subaccounts and vaults are rejected by the current integration; they require an explicit `vaultAddress` signing path and must never be silently treated as the master account.

## Order Safety

- Limit orders support GTC and Post Only. Market orders use IOC with a bounded protection price.
- Exchange asset IDs, quantity precision, price precision, leverage limits, and the minimum 10 USDC notional are loaded from Hyperliquid metadata.
- Strategy client IDs are encoded into reversible Hyperliquid `cloid` values. Manual orders are never adopted by the grid.
- A cancellation or rejection is never counted as a fill. Ambiguous write responses stop local progress until the authoritative order state is reconciled.
- Partial and terminal fills must reconcile exactly to Hyperliquid fill history before strategy accounting advances.
- The shared request governor stays below Hyperliquid's published IP weight limit and applies a cooldown after HTTP 429.

Official references:

- <https://docs.trade.xyz/api/overview>
- <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids>
- <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint>
- <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes>

Production order placement must still be validated first with the user's own authorized credentials and a deliberately small test strategy. Public endpoints can verify metadata, but they cannot prove private account authorization or live execution without those credentials.
