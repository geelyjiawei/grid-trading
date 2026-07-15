from decimal import Decimal, InvalidOperation


MAX_FEE_RATE = Decimal("0.01")


def normalize_fee_rate(value, field: str) -> str:
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError(f"Exchange returned an invalid {field}") from exc
    if not rate.is_finite() or rate < 0 or rate > MAX_FEE_RATE:
        raise RuntimeError(f"Exchange returned an invalid {field}")
    return format(rate, "f")


def fee_rate_response(
    symbol: str,
    maker_fee_rate,
    taker_fee_rate,
    *,
    source: str,
    fetched_at: int,
) -> dict:
    return {
        "retCode": 0,
        "result": {
            "symbol": str(symbol or "").upper(),
            "makerFeeRate": normalize_fee_rate(maker_fee_rate, "maker fee rate"),
            "takerFeeRate": normalize_fee_rate(taker_fee_rate, "taker fee rate"),
            "source": source,
            "fetchedAt": int(fetched_at),
        },
    }
