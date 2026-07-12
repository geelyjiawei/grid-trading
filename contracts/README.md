# Compatibility contract

This directory freezes the behavior that the Vue/Rust migration must preserve.
The Python service remains the behavioral oracle until every mutating endpoint
passes deterministic replay and fault-injection tests against the Rust engine.

## Contract sources

- `openapi-v1.json`: deterministic FastAPI request and route schema.
- `frontend-api-v1.json`: endpoints used by the current browser application.
- `INVARIANTS.md`: state, ownership, and persistence rules that OpenAPI cannot express.

Regenerate the machine-readable files with:

```powershell
$env:PYTHONPATH = "backend"
python contracts/export_contract.py
```

The export contains no credentials, balances, orders, positions, or production
responses. It is derived only from source-level route metadata and static frontend
request strings.

## Migration gate

The Rust service must not be enabled for exchange writes until all of these are true:

1. Every existing API route has a compatible request and response contract.
2. Existing `api_config.json`, `grid_state.json`, and `grid_history.json` files load
   without lossy conversion.
3. Binance, Aster, and Bybit adapters pass the same malformed-response and
   lost-acknowledgement fixtures as the Python clients.
4. Python and Rust produce identical order intents for replayed grid transitions.
5. Restart, cancellation, partial-fill, rate-limit, and fsync fault tests pass.
6. A read-only shadow run reports zero state, order, position, and PnL divergence.
