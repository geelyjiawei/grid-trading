import math
import re
from decimal import Decimal
from typing import Any


def snapshot_decimal(
    value: Any,
    *,
    context: str,
    row_index: int,
    field: str,
    allow_blank: bool = False,
) -> Decimal | None:
    if value is None or isinstance(value, bool):
        if allow_blank and value is None:
            return None
        raise RuntimeError(f"{context} row {row_index} has invalid {field}")
    text = str(value).strip()
    if not text:
        if allow_blank:
            return None
        raise RuntimeError(f"{context} row {row_index} has invalid {field}")
    try:
        parsed = Decimal(text)
    except Exception as exc:
        raise RuntimeError(f"{context} row {row_index} has invalid {field}") from exc
    if not parsed.is_finite():
        raise RuntimeError(f"{context} row {row_index} has non-finite {field}")
    try:
        float_value = float(parsed)
    except (OverflowError, ValueError) as exc:
        raise RuntimeError(
            f"{context} row {row_index} has out-of-range {field}"
        ) from exc
    if not math.isfinite(float_value) or (parsed != 0 and float_value == 0):
        raise RuntimeError(f"{context} row {row_index} has out-of-range {field}")
    return parsed


def validate_positive_decimal(
    value: Any,
    *,
    context: str,
    field: str,
    row_index: int = 0,
) -> Decimal:
    parsed = snapshot_decimal(
        value,
        context=context,
        row_index=row_index,
        field=field,
    )
    assert parsed is not None
    if parsed <= 0:
        raise RuntimeError(f"{context} row {row_index} has non-positive {field}")
    return parsed


def snapshot_text(
    value: Any,
    *,
    context: str,
    row_index: int,
    field: str,
) -> str:
    if value is None or isinstance(value, bool):
        raise RuntimeError(f"{context} row {row_index} has invalid {field}")
    text = str(value).strip()
    if not text:
        raise RuntimeError(f"{context} row {row_index} has invalid {field}")
    return text


def snapshot_boolean(
    value: Any,
    *,
    context: str,
    row_index: int,
    field: str,
    allow_strings: bool = False,
) -> bool:
    if isinstance(value, bool):
        return value
    if allow_strings and isinstance(value, str):
        text = value.strip().lower()
        if text == "true":
            return True
        if text == "false":
            return False
    raise RuntimeError(f"{context} row {row_index} has invalid {field}")


def validate_positive_integer(
    value: Any,
    *,
    context: str,
    field: str,
    row_index: int = 0,
) -> int:
    parsed = validate_positive_decimal(
        value,
        context=context,
        field=field,
        row_index=row_index,
    )
    if parsed != parsed.to_integral_value():
        raise RuntimeError(f"{context} row {row_index} has non-integer {field}")
    return int(parsed)


ORDER_STATUSES = frozenset(
    {
        "ACTIVE",
        "CANCELED",
        "CREATED",
        "DEACTIVATED",
        "EXPIRED",
        "EXPIRED_IN_MATCH",
        "FILLED",
        "NEW",
        "PARTIALLY_FILLED",
        "PARTIALLY_FILLED_CANCELED",
        "PENDING_CANCEL",
        "REJECTED",
        "TRIGGERED",
        "UNTRIGGERED",
    }
)
OPEN_ORDER_STATUSES = frozenset(
    {
        "ACTIVE",
        "CREATED",
        "NEW",
        "PARTIALLY_FILLED",
        "PENDING_CANCEL",
        "TRIGGERED",
        "UNTRIGGERED",
    }
)
MARKET_ORDER_TYPES = frozenset(
    {
        "MARKET",
        "STOP_MARKET",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    }
)


def _enum_key(value: Any, *, context: str, row_index: int, field: str) -> str:
    text = snapshot_text(
        value,
        context=context,
        row_index=row_index,
        field=field,
    )
    return re.sub(r"[^A-Za-z0-9]", "", text).upper()


def canonical_order_status(
    value: Any,
    *,
    context: str,
    row_index: int,
) -> str:
    aliases = {
        "ACTIVE": "ACTIVE",
        "CANCELED": "CANCELED",
        "CANCELLED": "CANCELED",
        "CREATED": "CREATED",
        "DEACTIVATED": "DEACTIVATED",
        "EXPIRED": "EXPIRED",
        "EXPIREDINMATCH": "EXPIRED_IN_MATCH",
        "FILLED": "FILLED",
        "NEW": "NEW",
        "PARTIALLYFILLED": "PARTIALLY_FILLED",
        "PARTIALFILLED": "PARTIALLY_FILLED",
        "FILLEDPARTIALLY": "PARTIALLY_FILLED",
        "PARTIALLYFILLEDCANCELED": "PARTIALLY_FILLED_CANCELED",
        "PARTIALLYFILLEDCANCELLED": "PARTIALLY_FILLED_CANCELED",
        # Kept for the misspelling published by one Bybit API explorer page.
        "PARTILLYFILLEDCANCELLED": "PARTIALLY_FILLED_CANCELED",
        "PENDINGCANCEL": "PENDING_CANCEL",
        "REJECTED": "REJECTED",
        "TRIGGERED": "TRIGGERED",
        "UNTRIGGERED": "UNTRIGGERED",
    }
    key = _enum_key(
        value,
        context=context,
        row_index=row_index,
        field="orderStatus",
    )
    status = aliases.get(key, "")
    if status not in ORDER_STATUSES:
        raise RuntimeError(f"{context} row {row_index} has invalid orderStatus")
    return status


def canonical_order_type(
    value: Any,
    *,
    context: str,
    row_index: int,
) -> str:
    aliases = {
        "LIMIT": "LIMIT",
        "LIMITMAKER": "LIMIT_MAKER",
        "MARKET": "MARKET",
        "STOP": "STOP",
        "STOPMARKET": "STOP_MARKET",
        "TAKEPROFIT": "TAKE_PROFIT",
        "TAKEPROFITMARKET": "TAKE_PROFIT_MARKET",
        "TRAILINGSTOPMARKET": "TRAILING_STOP_MARKET",
        "UNKNOWN": "UNKNOWN",
    }
    key = _enum_key(
        value,
        context=context,
        row_index=row_index,
        field="orderType",
    )
    order_type = aliases.get(key, "")
    if not order_type:
        raise RuntimeError(f"{context} row {row_index} has invalid orderType")
    return order_type


def canonical_time_in_force(
    value: Any,
    *,
    context: str,
    row_index: int,
) -> str:
    aliases = {
        "FOK": "FOK",
        "FILLORKILL": "FOK",
        "GTC": "GTC",
        "GOODTILCANCEL": "GTC",
        "GOODTILCANCELED": "GTC",
        "GOODTILCANCELLED": "GTC",
        "GTX": "GTX",
        "GTD": "GTD",
        "GOODTILLDATE": "GTD",
        "IOC": "IOC",
        "IMMEDIATEORCANCEL": "IOC",
        "POSTONLY": "POST_ONLY",
        "RPI": "RPI",
    }
    key = _enum_key(
        value,
        context=context,
        row_index=row_index,
        field="timeInForce",
    )
    time_in_force = aliases.get(key, "")
    if not time_in_force:
        raise RuntimeError(f"{context} row {row_index} has invalid timeInForce")
    return time_in_force


def _order_context(symbol: str, context: str | None) -> str:
    if context:
        return context
    label = str(symbol or "").upper().strip()
    return f"{label} order snapshot" if label else "order snapshot"


def _optional_order_text(
    row: dict,
    field: str,
    *,
    context: str,
    row_index: int,
) -> str:
    value = row.get(field)
    if value is None or value == "":
        return ""
    return snapshot_text(
        value,
        context=context,
        row_index=row_index,
        field=field,
    )


def _nonnegative_order_decimal(
    row: dict,
    field: str,
    *,
    context: str,
    row_index: int,
    required: bool,
    allow_blank: bool = False,
) -> Decimal | None:
    if field not in row:
        if required:
            raise RuntimeError(f"{context} row {row_index} is missing {field}")
        return None
    parsed = snapshot_decimal(
        row.get(field),
        context=context,
        row_index=row_index,
        field=field,
        allow_blank=allow_blank,
    )
    if parsed is None:
        parsed = Decimal("0")
    if parsed < 0:
        raise RuntimeError(f"{context} row {row_index} has negative {field}")
    return parsed


def validate_order_row(
    row: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    expected_link_id: str = "",
    require_details: bool = True,
    allowed_statuses: frozenset[str] | set[str] | None = None,
    row_index: int = 0,
    context: str | None = None,
) -> dict:
    label = str(expected_symbol or "").upper().strip()
    expected_order = str(expected_order_id or "").strip()
    expected_link = str(expected_link_id or "").strip()
    snapshot_context = _order_context(label, context)
    if not isinstance(row, dict):
        raise RuntimeError(f"{snapshot_context} row {row_index} must be an object")

    order_id = snapshot_text(
        row.get("orderId"),
        context=snapshot_context,
        row_index=row_index,
        field="orderId",
    )
    link_id = _optional_order_text(
        row,
        "orderLinkId",
        context=snapshot_context,
        row_index=row_index,
    )
    if expected_order and order_id != expected_order:
        raise RuntimeError(
            f"{snapshot_context} row {row_index} belongs to order {order_id}"
        )
    if expected_link and link_id != expected_link:
        raise RuntimeError(
            f"{snapshot_context} row {row_index} belongs to client order "
            f"{link_id or 'an empty client order ID'}"
        )

    symbol = ""
    if require_details or "symbol" in row:
        symbol = snapshot_text(
            row.get("symbol"),
            context=snapshot_context,
            row_index=row_index,
            field="symbol",
        ).upper()
        if label and symbol != label:
            raise RuntimeError(
                f"{snapshot_context} row {row_index} belongs to {symbol}"
            )

    side = ""
    if require_details or "side" in row:
        side_key = _enum_key(
            row.get("side"),
            context=snapshot_context,
            row_index=row_index,
            field="side",
        )
        side = {"BUY": "Buy", "SELL": "Sell"}.get(side_key, "")
        if not side:
            raise RuntimeError(f"{snapshot_context} row {row_index} has invalid side")

    order_type = ""
    if require_details or "orderType" in row:
        order_type = canonical_order_type(
            row.get("orderType"),
            context=snapshot_context,
            row_index=row_index,
        )

    reduce_only = None
    if require_details or "reduceOnly" in row:
        reduce_only = snapshot_boolean(
            row.get("reduceOnly"),
            context=snapshot_context,
            row_index=row_index,
            field="reduceOnly",
            allow_strings=True,
        )
    close_on_trigger = None
    if "closeOnTrigger" in row:
        close_on_trigger = snapshot_boolean(
            row.get("closeOnTrigger"),
            context=snapshot_context,
            row_index=row_index,
            field="closeOnTrigger",
            allow_strings=True,
        )
    close_position = None
    if "closePosition" in row:
        close_position = snapshot_boolean(
            row.get("closePosition"),
            context=snapshot_context,
            row_index=row_index,
            field="closePosition",
            allow_strings=True,
        )

    qty = None
    if require_details or "qty" in row:
        if "qty" not in row:
            raise RuntimeError(f"{snapshot_context} row {row_index} is missing qty")
        qty = snapshot_decimal(
            row.get("qty"),
            context=snapshot_context,
            row_index=row_index,
            field="qty",
        )
        assert qty is not None
        if qty < 0 or (
            qty == 0
            and not ((reduce_only and close_on_trigger) or close_position)
        ):
            raise RuntimeError(
                f"{snapshot_context} row {row_index} has non-positive qty"
            )
    close_all_position = bool(
        qty == 0 and ((reduce_only and close_on_trigger) or close_position)
    )

    price = _nonnegative_order_decimal(
        row,
        "price",
        context=snapshot_context,
        row_index=row_index,
        required=require_details,
        allow_blank=order_type in MARKET_ORDER_TYPES,
    )
    if price is not None and order_type and order_type not in MARKET_ORDER_TYPES and price <= 0:
        raise RuntimeError(f"{snapshot_context} row {row_index} has non-positive price")

    avg_price = _nonnegative_order_decimal(
        row,
        "avgPrice",
        context=snapshot_context,
        row_index=row_index,
        required=require_details,
        allow_blank=True,
    )
    executed_qty = _nonnegative_order_decimal(
        row,
        "executedQty",
        context=snapshot_context,
        row_index=row_index,
        required=require_details,
        allow_blank=False,
    )
    cum_quote = _nonnegative_order_decimal(
        row,
        "cumQuote",
        context=snapshot_context,
        row_index=row_index,
        required=require_details,
        allow_blank=False,
    )
    if (
        qty is not None
        and executed_qty is not None
        and not close_all_position
        and executed_qty > qty
    ):
        raise RuntimeError(
            f"{snapshot_context} row {row_index} executedQty exceeds qty"
        )

    status = ""
    if require_details or "orderStatus" in row:
        status = canonical_order_status(
            row.get("orderStatus"),
            context=snapshot_context,
            row_index=row_index,
        )
        if allowed_statuses is not None and status not in allowed_statuses:
            raise RuntimeError(
                f"{snapshot_context} row {row_index} has non-open orderStatus {status}"
            )
        if status == "PARTIALLY_FILLED" and (
            qty is None
            or executed_qty is None
            or executed_qty <= 0
            or (not close_all_position and executed_qty >= qty)
        ):
            raise RuntimeError(
                f"{snapshot_context} row {row_index} has inconsistent partial execution"
            )
        if status == "FILLED" and (
            qty is None
            or executed_qty is None
            or (close_all_position and executed_qty <= 0)
            or (not close_all_position and executed_qty != qty)
        ):
            raise RuntimeError(
                f"{snapshot_context} row {row_index} has inconsistent filled execution"
            )

    time_in_force = ""
    if require_details or "timeInForce" in row:
        time_in_force = canonical_time_in_force(
            row.get("timeInForce"),
            context=snapshot_context,
            row_index=row_index,
        )

    created_time = None
    if require_details or "createdTime" in row:
        created_time = validate_positive_integer(
            row.get("createdTime"),
            context=snapshot_context,
            field="createdTime",
            row_index=row_index,
        )

    return {
        "raw": row,
        "symbol": symbol,
        "order_id": order_id,
        "link_id": link_id,
        "side": side,
        "price": price,
        "qty": qty,
        "avg_price": avg_price,
        "executed_qty": executed_qty,
        "cum_quote": cum_quote,
        "status": status,
        "reduce_only": reduce_only,
        "close_on_trigger": close_on_trigger,
        "close_position": close_position,
        "close_all_position": close_all_position,
        "time_in_force": time_in_force,
        "order_type": order_type,
        "created_time": created_time,
    }


def validate_order_rows(
    rows: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    expected_link_id: str = "",
    require_details: bool = True,
    allowed_statuses: frozenset[str] | set[str] | None = None,
    unique_link_ids: bool = False,
    require_single: bool = False,
    allow_empty: bool = True,
    context: str | None = None,
) -> list[dict]:
    snapshot_context = _order_context(expected_symbol, context)
    if not isinstance(rows, list):
        raise RuntimeError(f"{snapshot_context} list must be an array")
    if require_single and len(rows) != 1:
        raise RuntimeError(f"{snapshot_context} must contain exactly one order")
    if not allow_empty and not rows:
        raise RuntimeError(f"{snapshot_context} must contain an order")

    validated: list[dict] = []
    seen_order_ids: set[str] = set()
    seen_link_ids: set[str] = set()
    for row_index, row in enumerate(rows):
        item = validate_order_row(
            row,
            expected_symbol=expected_symbol,
            expected_order_id=expected_order_id,
            expected_link_id=expected_link_id,
            require_details=require_details,
            allowed_statuses=allowed_statuses,
            row_index=row_index,
            context=snapshot_context,
        )
        order_id = item["order_id"]
        if order_id in seen_order_ids:
            raise RuntimeError(
                f"{snapshot_context} contains duplicate exchange order ID {order_id}"
            )
        seen_order_ids.add(order_id)
        link_id = item["link_id"]
        if unique_link_ids and link_id:
            if link_id in seen_link_ids:
                raise RuntimeError(
                    f"{snapshot_context} contains duplicate client order ID {link_id}"
                )
            seen_link_ids.add(link_id)
        validated.append(item)
    return validated


def normalize_binance_style_order_rows(
    rows: Any,
    *,
    expected_symbol: str,
    expected_order_id: str = "",
    expected_link_id: str = "",
    allowed_statuses: frozenset[str] | set[str] | None = None,
    unique_link_ids: bool = False,
    require_single: bool = False,
) -> list[dict]:
    mapped: Any = rows
    if isinstance(rows, list):
        mapped = []
        for item in rows:
            if not isinstance(item, dict):
                mapped.append(item)
                continue
            raw_side = item.get("side")
            side_key = str(raw_side or "").upper()
            side = {"BUY": "Buy", "SELL": "Sell"}.get(side_key, raw_side)
            snapshot = {
                "symbol": item.get("symbol"),
                "orderId": item.get("orderId"),
                "orderLinkId": item.get("clientOrderId"),
                "side": side,
                "price": item.get("price"),
                "qty": item.get("origQty"),
                "avgPrice": item.get("avgPrice"),
                "executedQty": item.get("executedQty"),
                "cumQuote": item.get("cumQuote"),
                "orderStatus": item.get("status"),
                "reduceOnly": item.get("reduceOnly"),
                "timeInForce": item.get("timeInForce"),
                "orderType": item.get("type"),
                "createdTime": item.get("time"),
            }
            if "closePosition" in item:
                snapshot["closePosition"] = item.get("closePosition")
            mapped.append(snapshot)

    validated = validate_order_rows(
        mapped,
        expected_symbol=expected_symbol,
        expected_order_id=expected_order_id,
        expected_link_id=expected_link_id,
        require_details=True,
        allowed_statuses=allowed_statuses,
        unique_link_ids=unique_link_ids,
        require_single=require_single,
    )
    normalized: list[dict] = []
    for item in validated:
        row = dict(item["raw"])
        row.update(
            {
                "symbol": item["symbol"],
                "orderId": item["order_id"],
                "orderLinkId": item["link_id"],
                "side": item["side"],
                "orderStatus": item["status"],
                "reduceOnly": item["reduce_only"],
                "timeInForce": item["time_in_force"],
                "orderType": item["order_type"],
                "createdTime": str(item["created_time"]),
            }
        )
        normalized.append(row)
    return normalized


def normalize_order_ack_row(
    row: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    expected_link_id: str = "",
    context: str | None = None,
) -> dict:
    validated = validate_order_row(
        row,
        expected_symbol=expected_symbol,
        expected_order_id=expected_order_id,
        expected_link_id=expected_link_id,
        require_details=False,
        context=context,
    )
    normalized = dict(validated["raw"])
    normalized["orderId"] = validated["order_id"]
    normalized["orderLinkId"] = validated["link_id"]
    for output, source in (
        ("symbol", "symbol"),
        ("side", "side"),
        ("orderStatus", "status"),
        ("timeInForce", "time_in_force"),
        ("orderType", "order_type"),
    ):
        if validated[source]:
            normalized[output] = validated[source]
    if validated["reduce_only"] is not None:
        normalized["reduceOnly"] = validated["reduce_only"]
    if validated["created_time"] is not None:
        normalized["createdTime"] = str(validated["created_time"])
    return normalized


def normalize_binance_style_order_ack(
    row: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    expected_link_id: str = "",
) -> dict:
    context = _order_context(expected_symbol, None)
    if not isinstance(row, dict):
        raise RuntimeError(f"{context} acknowledgement must be an object")
    mapped: dict[str, Any] = {
        "orderId": row.get("orderId"),
        "orderLinkId": row.get("clientOrderId"),
    }
    field_map = {
        "symbol": "symbol",
        "side": "side",
        "price": "price",
        "origQty": "qty",
        "avgPrice": "avgPrice",
        "executedQty": "executedQty",
        "cumQuote": "cumQuote",
        "status": "orderStatus",
        "reduceOnly": "reduceOnly",
        "closePosition": "closePosition",
        "timeInForce": "timeInForce",
        "type": "orderType",
        "time": "createdTime",
    }
    for source, target in field_map.items():
        if source in row:
            mapped[target] = row.get(source)
    raw_side = mapped.get("side")
    if raw_side is not None:
        side_key = str(raw_side or "").upper()
        mapped["side"] = {"BUY": "Buy", "SELL": "Sell"}.get(
            side_key,
            raw_side,
        )
    return normalize_order_ack_row(
        mapped,
        expected_symbol=expected_symbol,
        expected_order_id=expected_order_id,
        expected_link_id=expected_link_id,
        context=context,
    )


def normalize_binance_style_cancel_ack(
    row: Any,
    *,
    expected_symbol: str,
    expected_order_id: str,
) -> dict:
    context = f"{expected_symbol.upper()} cancellation acknowledgement"
    if not isinstance(row, dict):
        raise RuntimeError(f"{context} must be an object")
    required_fields = {
        "symbol",
        "orderId",
        "clientOrderId",
        "side",
        "price",
        "origQty",
        "executedQty",
        "status",
        "reduceOnly",
        "timeInForce",
        "type",
    }
    missing = sorted(field for field in required_fields if field not in row)
    if missing:
        raise RuntimeError(f"{context} is missing {', '.join(missing)}")
    normalized = normalize_binance_style_order_ack(
        row,
        expected_symbol=expected_symbol,
        expected_order_id=expected_order_id,
    )
    if normalized.get("orderStatus") != "CANCELED":
        raise RuntimeError(f"{context} does not confirm a cancelled order")
    return normalized


def _execution_context(symbol: str, context: str | None) -> str:
    if context:
        return context
    label = str(symbol or "").upper().strip()
    return f"{label} execution snapshot" if label else "execution snapshot"


def validate_execution_row(
    row: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    require_identity: bool = True,
    row_index: int = 0,
    context: str | None = None,
) -> dict:
    label = str(expected_symbol or "").upper().strip()
    order_label = str(expected_order_id or "").strip()
    snapshot_context = _execution_context(label, context)
    if not isinstance(row, dict):
        raise RuntimeError(
            f"{snapshot_context} row {row_index} must be an object"
        )

    def identity_text(field: str, *, upper: bool = False) -> str:
        value = row.get(field)
        if not require_identity and (value is None or not str(value).strip()):
            return ""
        text = snapshot_text(
            value,
            context=snapshot_context,
            row_index=row_index,
            field=field,
        )
        return text.upper() if upper else text

    symbol = identity_text("symbol", upper=True)
    order_id = identity_text("orderId")
    trade_id = identity_text("tradeId")
    side = identity_text("side")
    if symbol and label and symbol != label:
        raise RuntimeError(
            f"{snapshot_context} row {row_index} belongs to {symbol}"
        )
    if order_id and order_label and order_id != order_label:
        raise RuntimeError(
            f"{snapshot_context} row {row_index} belongs to order {order_id}"
        )
    if side and side not in {"Buy", "Sell"}:
        raise RuntimeError(f"{snapshot_context} row {row_index} has invalid side")

    price = validate_positive_decimal(
        row.get("price"),
        context=snapshot_context,
        field="price",
        row_index=row_index,
    )
    qty = validate_positive_decimal(
        row.get("qty"),
        context=snapshot_context,
        field="qty",
        row_index=row_index,
    )
    volume_value = row.get("volume")
    if require_identity:
        volume = validate_positive_decimal(
            volume_value,
            context=snapshot_context,
            field="volume",
            row_index=row_index,
        )
    else:
        volume = snapshot_decimal(
            volume_value,
            context=snapshot_context,
            row_index=row_index,
            field="volume",
            allow_blank=True,
        )
        if volume is None:
            volume = price * qty
        if volume <= 0:
            raise RuntimeError(
                f"{snapshot_context} row {row_index} has non-positive volume"
            )

    fee_value = row.get("fee")
    if not require_identity and (fee_value is None or not str(fee_value).strip()):
        fee = Decimal("0")
    else:
        fee = snapshot_decimal(
            fee_value,
            context=snapshot_context,
            row_index=row_index,
            field="fee",
        )
        assert fee is not None

    fee_usdt = snapshot_decimal(
        row.get("feeUsdt"),
        context=snapshot_context,
        row_index=row_index,
        field="feeUsdt",
        allow_blank=True,
    )
    realized_pnl = snapshot_decimal(
        row.get("realizedPnl", "0"),
        context=snapshot_context,
        row_index=row_index,
        field="realizedPnl",
        allow_blank=True,
    )
    if realized_pnl is None:
        realized_pnl = Decimal("0")

    fee_asset_value = row.get("feeAsset")
    if not require_identity and (
        fee_asset_value is None or not str(fee_asset_value).strip()
    ):
        fee_asset = "USDT"
    else:
        fee_asset = snapshot_text(
            fee_asset_value,
            context=snapshot_context,
            row_index=row_index,
            field="feeAsset",
        ).upper()

    maker_value = row.get("isMaker")
    if not require_identity and maker_value is None:
        is_maker = False
    else:
        is_maker = snapshot_boolean(
            maker_value,
            context=snapshot_context,
            row_index=row_index,
            field="isMaker",
        )

    time_value = row.get("time")
    if not require_identity and (time_value is None or not str(time_value).strip()):
        execution_time = None
    else:
        execution_time = validate_positive_integer(
            time_value,
            context=snapshot_context,
            field="time",
            row_index=row_index,
        )

    fee_source = str(row.get("feeUsdtSource") or "").strip()
    fingerprint = (
        symbol,
        order_id,
        trade_id,
        side,
        price,
        qty,
        volume,
        fee,
        fee_asset,
        fee_usdt,
        fee_source,
        realized_pnl,
        is_maker,
        execution_time,
    )
    return {
        "raw": row,
        "symbol": symbol,
        "order_id": order_id,
        "trade_id": trade_id,
        "side": side,
        "price": price,
        "qty": qty,
        "volume": volume,
        "fee": fee,
        "fee_asset": fee_asset,
        "fee_usdt": fee_usdt,
        "fee_usdt_source": fee_source,
        "realized_pnl": realized_pnl,
        "is_maker": is_maker,
        "time": execution_time,
        "fingerprint": fingerprint,
    }


def validate_execution_response(
    response: Any,
    *,
    expected_symbol: str = "",
    expected_order_id: str = "",
    require_identity: bool = True,
) -> list[dict]:
    context = _execution_context(expected_symbol, None)
    if not isinstance(response, dict):
        raise RuntimeError(f"{context} response must be an object")
    if response.get("retCode") != 0:
        message = str(response.get("retMsg") or "exchange rejected the request")
        raise RuntimeError(f"{context} request failed: {message}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{context} result must be an object")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise RuntimeError(f"{context} list must be an array")

    validated: list[dict] = []
    by_trade_id: dict[tuple[str, str], dict] = {}
    for row_index, row in enumerate(rows):
        item = validate_execution_row(
            row,
            expected_symbol=expected_symbol,
            expected_order_id=expected_order_id,
            require_identity=require_identity,
            row_index=row_index,
            context=context,
        )
        trade_id = item["trade_id"]
        if not trade_id:
            validated.append(item)
            continue
        key = (item["symbol"], trade_id)
        previous = by_trade_id.get(key)
        if previous is None:
            by_trade_id[key] = item
            validated.append(item)
            continue
        if previous["fingerprint"] != item["fingerprint"]:
            raise RuntimeError(
                f"{context} contains conflicting duplicate tradeId {trade_id}"
            )
    return validated


def _single_snapshot_row(response: Any, *, symbol: str, kind: str) -> tuple[str, dict]:
    label = str(symbol or "").upper().strip()
    context = f"{label} {kind} snapshot" if label else f"{kind} snapshot"
    if not isinstance(response, dict):
        raise RuntimeError(f"{context} response must be an object")
    if response.get("retCode") != 0:
        message = str(response.get("retMsg") or "exchange rejected the request")
        raise RuntimeError(f"{context} request failed: {message}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{context} result must be an object")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise RuntimeError(f"{context} list must be an array")
    if len(rows) != 1:
        raise RuntimeError(f"{context} must contain exactly one row")
    row = rows[0]
    if not isinstance(row, dict):
        raise RuntimeError(f"{context} row must be an object")
    if "symbol" not in row:
        raise RuntimeError(f"{context} row is missing symbol")
    row_symbol = str(row.get("symbol") or "").upper().strip()
    if row_symbol != label:
        raise RuntimeError(f"{context} row belongs to {row_symbol or 'an empty symbol'}")
    return context, row


def validate_ticker_response(response: Any, *, symbol: str) -> dict:
    context, row = _single_snapshot_row(response, symbol=symbol, kind="ticker")
    last_price = validate_positive_decimal(
        row.get("lastPrice"),
        context=context,
        field="lastPrice",
    )
    mark_price = snapshot_decimal(
        row.get("markPrice"),
        context=context,
        row_index=0,
        field="markPrice",
        allow_blank=True,
    )
    if mark_price is None:
        mark_price = last_price
    elif mark_price <= 0:
        raise RuntimeError(f"{context} row has non-positive markPrice")
    return {
        "raw": row,
        "symbol": str(symbol).upper().strip(),
        "last_price": last_price,
        "mark_price": mark_price,
    }


def validate_symbol_price_row(
    row: Any,
    *,
    symbol: str,
    price_field: str = "price",
    kind: str = "fee asset price",
) -> Decimal:
    label = str(symbol or "").upper().strip()
    context = f"{label} {kind} snapshot" if label else f"{kind} snapshot"
    if not isinstance(row, dict):
        raise RuntimeError(f"{context} row must be an object")
    if "symbol" not in row:
        raise RuntimeError(f"{context} row is missing symbol")
    row_symbol = str(row.get("symbol") or "").upper().strip()
    if row_symbol != label:
        raise RuntimeError(f"{context} row belongs to {row_symbol or 'an empty symbol'}")
    return validate_positive_decimal(
        row.get(price_field),
        context=context,
        field=price_field,
    )


def validate_price_cache_entry(
    entry: Any,
    *,
    symbol: str,
    now: float,
    ttl_seconds: float,
) -> tuple[Decimal, bool]:
    context = f"{str(symbol or '').upper().strip()} fee asset price cache"
    if not isinstance(entry, tuple) or len(entry) != 2:
        raise RuntimeError(f"{context} entry has an invalid shape")
    value, cached_at_value = entry
    cached_at = snapshot_decimal(
        cached_at_value,
        context=context,
        row_index=0,
        field="timestamp",
    )
    assert cached_at is not None
    age = Decimal(str(now)) - cached_at
    fresh = Decimal("0") <= age < Decimal(str(ttl_seconds))
    price = validate_positive_decimal(
        value,
        context=context,
        field="price",
    )
    return price, fresh


def validate_instrument_response(response: Any, *, symbol: str) -> dict:
    context, row = _single_snapshot_row(response, symbol=symbol, kind="instrument")
    price_filter = row.get("priceFilter")
    lot_filter = row.get("lotSizeFilter")
    if not isinstance(price_filter, dict):
        raise RuntimeError(f"{context} priceFilter must be an object")
    if not isinstance(lot_filter, dict):
        raise RuntimeError(f"{context} lotSizeFilter must be an object")

    validate_positive_decimal(
        price_filter.get("tickSize"),
        context=context,
        field="tickSize",
    )
    validate_positive_decimal(
        lot_filter.get("qtyStep"),
        context=context,
        field="qtyStep",
    )
    min_qty = validate_positive_decimal(
        lot_filter.get("minOrderQty"),
        context=context,
        field="minOrderQty",
    )

    min_notional = snapshot_decimal(
        lot_filter.get("minNotionalValue", row.get("minNotionalValue")),
        context=context,
        row_index=0,
        field="minNotionalValue",
        allow_blank=True,
    )
    if min_notional is None:
        min_notional = Decimal("0")
    elif min_notional < 0:
        raise RuntimeError(f"{context} has negative minNotionalValue")

    if "marketLotSizeFilter" in row:
        market_filter = row.get("marketLotSizeFilter")
        if not isinstance(market_filter, dict):
            raise RuntimeError(f"{context} marketLotSizeFilter must be an object")
        validate_positive_decimal(
            market_filter.get("qtyStep"),
            context=context,
            field="market qtyStep",
        )
        market_min_qty = validate_positive_decimal(
            market_filter.get("minOrderQty"),
            context=context,
            field="market minOrderQty",
        )
        max_market_value = market_filter.get("maxOrderQty")
    else:
        market_filter = lot_filter
        market_min_qty = min_qty
        max_market_value = (
            lot_filter.get("maxMktOrderQty")
            or lot_filter.get("maxMarketOrderQty")
            or lot_filter.get("maxOrderQty")
        )
    max_market_qty = snapshot_decimal(
        max_market_value,
        context=context,
        row_index=0,
        field="market maxOrderQty",
        allow_blank=True,
    )
    if max_market_qty is None:
        max_market_qty = Decimal("0")
    elif max_market_qty < 0:
        raise RuntimeError(f"{context} has negative market maxOrderQty")

    return {
        "raw": row,
        "symbol": str(symbol).upper().strip(),
        "tick_size": str(price_filter.get("tickSize")).strip(),
        "qty_step": str(lot_filter.get("qtyStep")).strip(),
        "min_qty": min_qty,
        "min_notional": min_notional,
        "market_qty_step": str(market_filter.get("qtyStep")).strip(),
        "market_min_qty": market_min_qty,
        "max_market_qty": max_market_qty,
    }


def validate_position_response(response: Any, *, symbol: str = "") -> list[dict]:
    label = str(symbol or "").upper().strip()
    context = f"{label} position snapshot" if label else "position snapshot"
    if not isinstance(response, dict):
        raise RuntimeError(f"{context} response must be an object")
    if response.get("retCode") != 0:
        message = str(response.get("retMsg") or "exchange rejected the request")
        raise RuntimeError(f"{context} request failed: {message}")

    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{context} result must be an object")
    raw_positions = result.get("list")
    if not isinstance(raw_positions, list):
        raise RuntimeError(f"{context} list must be an array")

    positions: list[dict] = []
    positive_sides: set[str] = set()
    for row_index, raw_position in enumerate(raw_positions):
        if not isinstance(raw_position, dict):
            raise RuntimeError(f"{context} row {row_index} must be an object")
        if label:
            if "symbol" not in raw_position:
                raise RuntimeError(f"{context} row {row_index} is missing symbol")
            row_symbol = str(raw_position.get("symbol") or "").upper().strip()
            if row_symbol != label:
                raise RuntimeError(
                    f"{context} row {row_index} belongs to {row_symbol or 'an empty symbol'}"
                )
        if "size" not in raw_position:
            raise RuntimeError(f"{context} row {row_index} is missing size")

        qty = snapshot_decimal(
            raw_position.get("size"),
            context=context,
            row_index=row_index,
            field="size",
        )
        assert qty is not None
        if qty < 0:
            raise RuntimeError(f"{context} row {row_index} has negative size")

        side_value = raw_position.get("side")
        side = "" if side_value is None else str(side_value).strip()
        if side not in {"", "Buy", "Sell"}:
            raise RuntimeError(f"{context} row {row_index} has invalid side")
        if qty > 0 and side not in {"Buy", "Sell"}:
            raise RuntimeError(f"{context} row {row_index} has size without a position side")
        if qty > 0:
            if side in positive_sides:
                raise RuntimeError(f"{context} contains duplicate positive {side} rows")
            positive_sides.add(side)

        entry_price = None
        for field in ("avgPrice", "entryPrice", "entry_price"):
            if field not in raw_position:
                continue
            candidate = snapshot_decimal(
                raw_position.get(field),
                context=context,
                row_index=row_index,
                field=field,
                allow_blank=True,
            )
            if candidate is None:
                continue
            if candidate < 0:
                raise RuntimeError(f"{context} row {row_index} has negative {field}")
            if entry_price is not None and candidate != entry_price:
                raise RuntimeError(f"{context} row {row_index} has conflicting entry prices")
            entry_price = candidate

        mark_price = None
        if "markPrice" in raw_position:
            mark_price = snapshot_decimal(
                raw_position.get("markPrice"),
                context=context,
                row_index=row_index,
                field="markPrice",
                allow_blank=True,
            )
            if mark_price is not None and mark_price < 0:
                raise RuntimeError(f"{context} row {row_index} has negative markPrice")

        unrealised_pnl = None
        if "unrealisedPnl" in raw_position:
            unrealised_pnl = snapshot_decimal(
                raw_position.get("unrealisedPnl"),
                context=context,
                row_index=row_index,
                field="unrealisedPnl",
                allow_blank=True,
            )

        positions.append(
            {
                "raw": raw_position,
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "unrealised_pnl": unrealised_pnl,
            }
        )
    return positions


def validate_balance_response(response: Any, *, coin: str = "USDT") -> dict:
    label = str(coin or "").upper().strip()
    context = f"{label} balance snapshot" if label else "balance snapshot"
    if not isinstance(response, dict):
        raise RuntimeError(f"{context} response must be an object")
    if response.get("retCode") != 0:
        message = str(response.get("retMsg") or "exchange rejected the request")
        raise RuntimeError(f"{context} request failed: {message}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{context} result must be an object")
    accounts = result.get("list")
    if not isinstance(accounts, list):
        raise RuntimeError(f"{context} list must be an array")
    if len(accounts) != 1:
        raise RuntimeError(f"{context} must contain exactly one account row")
    account = accounts[0]
    if not isinstance(account, dict):
        raise RuntimeError(f"{context} account row must be an object")
    assets = account.get("coin")
    if not isinstance(assets, list):
        raise RuntimeError(f"{context} coin list must be an array")

    matches: list[dict] = []
    for row_index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            raise RuntimeError(f"{context} row {row_index} must be an object")
        if "coin" not in asset:
            raise RuntimeError(f"{context} row {row_index} is missing coin")
        asset_label = str(asset.get("coin") or "").upper().strip()
        if not asset_label:
            raise RuntimeError(f"{context} row {row_index} has an empty coin")
        if asset_label == label:
            matches.append(asset)
    if len(matches) > 1:
        raise RuntimeError(f"{context} contains duplicate {label} rows")
    if not matches:
        zero = Decimal("0")
        return {
            "raw": None,
            "account": account,
            "coin": label,
            "present": False,
            "available": zero,
            "equity": zero,
            "unrealised_pnl": zero,
        }

    asset = matches[0]
    row_index = assets.index(asset)
    available_value = asset.get("availableToWithdraw")
    if available_value is None or not str(available_value).strip():
        available_value = asset.get("walletBalance")
    available = snapshot_decimal(
        available_value,
        context=context,
        row_index=row_index,
        field="available balance",
    )
    equity = snapshot_decimal(
        asset.get("equity"),
        context=context,
        row_index=row_index,
        field="equity",
    )
    unrealised_pnl = snapshot_decimal(
        asset.get("unrealisedPnl"),
        context=context,
        row_index=row_index,
        field="unrealisedPnl",
    )
    assert available is not None and equity is not None and unrealised_pnl is not None
    return {
        "raw": asset,
        "account": account,
        "coin": label,
        "present": True,
        "available": available,
        "equity": equity,
        "unrealised_pnl": unrealised_pnl,
    }


def normalize_futures_balance_rows(rows: Any, *, coin: str = "USDT") -> dict:
    label = str(coin or "").upper().strip()
    context = f"{label} balance snapshot" if label else "balance snapshot"
    if not isinstance(rows, list):
        raise RuntimeError(f"{context} response must be an array")

    matches: list[tuple[int, dict]] = []
    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise RuntimeError(f"{context} row {row_index} must be an object")
        asset = str(row.get("asset") or "").upper().strip()
        if not asset:
            raise RuntimeError(f"{context} row {row_index} has an empty asset")
        if asset == label:
            matches.append((row_index, row))
    if len(matches) > 1:
        raise RuntimeError(f"{context} contains duplicate {label} rows")

    assets = []
    if matches:
        row_index, row = matches[0]
        for field in ("availableBalance", "balance", "crossUnPnl"):
            snapshot_decimal(
                row.get(field),
                context=context,
                row_index=row_index,
                field=field,
            )
        assets.append(
            {
                "coin": label,
                "availableToWithdraw": row.get("availableBalance"),
                "walletBalance": row.get("balance"),
                "equity": row.get("balance"),
                "unrealisedPnl": row.get("crossUnPnl"),
            }
        )

    response = {
        "retCode": 0,
        "result": {"list": [{"coin": assets}]},
    }
    validate_balance_response(response, coin=label)
    return response
