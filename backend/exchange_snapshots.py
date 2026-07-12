import math
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
