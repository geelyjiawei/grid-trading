use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::{
    domain::{
        ClientOrderId, Exchange, InstrumentRules, OrderKind, OrderShape, OrderSide, QuantityRules,
        TerminalOrderStatus, TimeInForce,
    },
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, CancellationAcknowledgement, ExchangeMarketSnapshot,
        HistoricalMinutePrice, LeverageAcknowledgement, OrderLifecycle, PlacementAcknowledgement,
        PositionLeg, PositionSide, PositionSnapshot, TradeFill, TradingFeeRates,
        execution::OrderExecutionHeader, is_valid_trade_id,
    },
};

const CATEGORY: &str = "linear";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BybitErrorBody {
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum BybitCodecError {
    #[error("Bybit response is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("Bybit response field {0} is missing or invalid")]
    InvalidField(&'static str),
    #[error("Bybit response is not successful")]
    ExchangeRejected,
    #[error("Bybit response identity does not match the request")]
    IdentityMismatch,
    #[error("Bybit response contains duplicate records")]
    DuplicateRecord,
}

pub(super) fn parse_error(body: &str) -> BybitErrorBody {
    let Ok(root) = serde_json::from_str::<Value>(body) else {
        return BybitErrorBody {
            code: None,
            message: "Bybit returned a non-JSON response".into(),
        };
    };
    BybitErrorBody {
        code: root.get("retCode").and_then(scalar_text),
        message: root
            .get("retMsg")
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
            .unwrap_or("Bybit rejected the request")
            .to_owned(),
    }
}

pub(super) fn parse_placement_acknowledgement(
    body: &str,
    expected_client_order_id: &ClientOrderId,
) -> Result<PlacementAcknowledgement, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    let exchange_order_id = required_string(result, "orderId")?;
    let client_order_id = required_string(result, "orderLinkId")?;
    if client_order_id != expected_client_order_id.as_str() || exchange_order_id.trim().is_empty() {
        return Err(BybitCodecError::IdentityMismatch);
    }
    Ok(PlacementAcknowledgement {
        client_order_id: expected_client_order_id.clone(),
        exchange_order_id: exchange_order_id.into(),
    })
}

pub(super) fn parse_cancellation_acknowledgement(
    body: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: &str,
) -> Result<CancellationAcknowledgement, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    if required_string(result, "orderId")? != expected_exchange_order_id
        || required_string(result, "orderLinkId")? != expected_client_order_id.as_str()
    {
        return Err(BybitCodecError::IdentityMismatch);
    }
    Ok(CancellationAcknowledgement {
        client_order_id: expected_client_order_id.clone(),
        exchange_order_id: expected_exchange_order_id.into(),
    })
}

pub(super) fn parse_leverage_acknowledgement(
    body: &str,
    expected_symbol: &str,
    expected_leverage: u16,
) -> Result<LeverageAcknowledgement, BybitCodecError> {
    let root: Value = serde_json::from_str(body)
        .map_err(|error| BybitCodecError::InvalidJson(error.to_string()))?;
    let code = required_i64(&root, "retCode")?;
    if !matches!(code, 0 | 110043) {
        return Err(BybitCodecError::ExchangeRejected);
    }
    if code == 0 {
        result_object(&root)?;
    } else {
        required_string(&root, "retMsg")?;
    }
    if expected_leverage == 0 {
        return Err(BybitCodecError::InvalidField("leverage"));
    }
    Ok(LeverageAcknowledgement {
        exchange: Exchange::Bybit,
        symbol: expected_symbol.to_ascii_uppercase(),
        leverage: expected_leverage,
    })
}

pub(super) fn parse_trading_fee_rates(
    body: &str,
    expected_symbol: &str,
) -> Result<TradingFeeRates, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    let rows = required_array(result, "list")?;
    if rows.len() != 1
        || !required_string(&rows[0], "symbol")?.eq_ignore_ascii_case(expected_symbol)
    {
        return Err(BybitCodecError::IdentityMismatch);
    }
    let rates = TradingFeeRates {
        exchange: Exchange::Bybit,
        symbol: expected_symbol.to_ascii_uppercase(),
        maker_rate: required_decimal(&rows[0], "makerFeeRate")?,
        taker_rate: required_decimal(&rows[0], "takerFeeRate")?,
    };
    rates
        .validate()
        .map_err(|_| BybitCodecError::InvalidField("feeRate"))?;
    Ok(rates)
}

pub(super) fn parse_exact_order_record(
    body: &str,
    expected_symbol: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: Option<&str>,
) -> Result<Option<OrderExecutionHeader>, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    require_category(result)?;
    let cursor = optional_string(result, "nextPageCursor")?.unwrap_or_default();
    if !cursor.is_empty() {
        return Err(BybitCodecError::DuplicateRecord);
    }
    let rows = required_array(result, "list")?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1 {
        return Err(BybitCodecError::DuplicateRecord);
    }
    parse_order_row(
        &rows[0],
        expected_symbol,
        expected_client_order_id,
        expected_exchange_order_id,
    )
    .map(Some)
}

fn parse_order_row(
    row: &Value,
    expected_symbol: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: Option<&str>,
) -> Result<OrderExecutionHeader, BybitCodecError> {
    let symbol = required_string(row, "symbol")?.to_ascii_uppercase();
    let client_order_id = required_string(row, "orderLinkId")?;
    let exchange_order_id = required_string(row, "orderId")?;
    if symbol != expected_symbol.to_ascii_uppercase()
        || client_order_id != expected_client_order_id.as_str()
        || expected_exchange_order_id.is_some_and(|expected| expected != exchange_order_id)
        || required_u64(row, "positionIdx")? != 0
    {
        return Err(BybitCodecError::IdentityMismatch);
    }
    let side = parse_side(required_string(row, "side")?)?;
    let kind = match required_string(row, "orderType")?
        .to_ascii_uppercase()
        .as_str()
    {
        "LIMIT" => OrderKind::Limit,
        "MARKET" => OrderKind::Market,
        _ => return Err(BybitCodecError::InvalidField("orderType")),
    };
    let quantity = required_decimal(row, "qty")?;
    let (price, time_in_force) = match kind {
        OrderKind::Limit => {
            let time_in_force = match required_string(row, "timeInForce")?
                .to_ascii_uppercase()
                .as_str()
            {
                "GTC" => TimeInForce::Gtc,
                "POSTONLY" => TimeInForce::PostOnly,
                _ => return Err(BybitCodecError::InvalidField("timeInForce")),
            };
            (Some(required_decimal(row, "price")?), time_in_force)
        }
        OrderKind::Market => (None, TimeInForce::Gtc),
    };
    let shape = OrderShape {
        symbol,
        side,
        price,
        quantity,
        reduce_only: required_bool(row, "reduceOnly")?,
        kind,
        time_in_force,
    };
    shape
        .validate()
        .map_err(|_| BybitCodecError::InvalidField("orderShape"))?;
    let lifecycle = parse_lifecycle(required_string(row, "orderStatus")?)?;
    let cumulative_quantity = required_decimal(row, "cumExecQty")?;
    let cumulative_quote = required_decimal(row, "cumExecValue")?;
    let order_time_ms = required_u64(row, "createdTime")?;
    let update_time_ms = required_u64(row, "updatedTime")?;
    if cumulative_quantity < Decimal::ZERO
        || cumulative_quantity > quantity
        || cumulative_quote < Decimal::ZERO
        || order_time_ms == 0
        || update_time_ms < order_time_ms
        || (cumulative_quantity.is_zero() && !cumulative_quote.is_zero())
        || (cumulative_quantity > Decimal::ZERO && cumulative_quote <= Decimal::ZERO)
    {
        return Err(BybitCodecError::InvalidField("executionTotals"));
    }
    match lifecycle {
        OrderLifecycle::Active(ActiveOrderStatus::New) if !cumulative_quantity.is_zero() => {
            return Err(BybitCodecError::InvalidField("orderStatus"));
        }
        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
            if cumulative_quantity <= Decimal::ZERO || cumulative_quantity >= quantity =>
        {
            return Err(BybitCodecError::InvalidField("orderStatus"));
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
            if cumulative_quantity != quantity =>
        {
            return Err(BybitCodecError::InvalidField("orderStatus"));
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)
            if !cumulative_quantity.is_zero() =>
        {
            return Err(BybitCodecError::InvalidField("orderStatus"));
        }
        _ => {}
    }
    Ok(OrderExecutionHeader {
        order: AuthoritativeOrder {
            client_order_id: ClientOrderId::parse(client_order_id)
                .map_err(|_| BybitCodecError::InvalidField("orderLinkId"))?,
            exchange_order_id: exchange_order_id.into(),
            exchange: Exchange::Bybit,
            shape,
            lifecycle,
        },
        cumulative_quantity,
        cumulative_quote,
        order_time_ms,
        update_time_ms,
    })
}

#[derive(Debug, PartialEq)]
pub(super) struct ExecutionPage {
    pub trades: Vec<TradeFill>,
    pub next_cursor: Option<String>,
}

pub(super) fn parse_execution_page(
    body: &str,
    expected_symbol: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: &str,
) -> Result<ExecutionPage, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    require_category(result)?;
    let cursor = optional_string(result, "nextPageCursor")?.unwrap_or_default();
    if !cursor.is_empty()
        && (cursor.len() > 2_048 || !cursor.bytes().all(|byte| byte.is_ascii_graphic()))
    {
        return Err(BybitCodecError::InvalidField("nextPageCursor"));
    }
    let rows = required_array(result, "list")?;
    let mut ids = BTreeSet::new();
    let mut trades = Vec::with_capacity(rows.len());
    for row in rows {
        let symbol = required_string(row, "symbol")?.to_ascii_uppercase();
        let exchange_order_id = required_string(row, "orderId")?;
        let client_order_id = required_string(row, "orderLinkId")?;
        if symbol != expected_symbol.to_ascii_uppercase()
            || exchange_order_id != expected_exchange_order_id
            || client_order_id != expected_client_order_id.as_str()
        {
            return Err(BybitCodecError::IdentityMismatch);
        }
        if !required_string(row, "execType")?.eq_ignore_ascii_case("Trade") {
            return Err(BybitCodecError::InvalidField("execType"));
        }
        let trade_id = required_string(row, "execId")?.to_owned();
        if !is_valid_trade_id(&trade_id) || !ids.insert(trade_id.clone()) {
            return Err(BybitCodecError::DuplicateRecord);
        }
        let price = required_decimal(row, "execPrice")?;
        let quantity = required_decimal(row, "execQty")?;
        let quote_quantity = required_decimal(row, "execValue")?;
        let raw_commission = required_decimal(row, "execFee")?;
        let trade_time_ms = required_u64(row, "execTime")?;
        if price <= Decimal::ZERO
            || quantity <= Decimal::ZERO
            || quote_quantity <= Decimal::ZERO
            || raw_commission < Decimal::ZERO
            || trade_time_ms == 0
        {
            return Err(BybitCodecError::InvalidField("execution"));
        }
        let commission_asset = match optional_string(row, "feeCurrency")? {
            Some(asset) if !asset.is_empty() => asset.to_ascii_uppercase(),
            _ if symbol.ends_with("USDT") => "USDT".into(),
            _ if symbol.ends_with("USDC") => "USDC".into(),
            _ => return Err(BybitCodecError::InvalidField("feeCurrency")),
        };
        if !commission_asset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(BybitCodecError::InvalidField("feeCurrency"));
        }
        let realized_profit = optional_decimal(row, "execPnl")?.unwrap_or(Decimal::ZERO);
        trades.push(TradeFill {
            trade_id,
            exchange_order_id: exchange_order_id.into(),
            symbol,
            side: parse_side(required_string(row, "side")?)?,
            price,
            quantity,
            quote_quantity,
            raw_commission,
            commission_cost: raw_commission,
            commission_asset,
            realized_profit,
            is_maker: required_bool(row, "isMaker")?,
            trade_time_ms,
        });
    }
    Ok(ExecutionPage {
        trades,
        next_cursor: (!cursor.is_empty()).then_some(cursor),
    })
}

pub(super) fn parse_market_snapshot(
    body: &str,
    expected_symbol: &str,
) -> Result<ExchangeMarketSnapshot, BybitCodecError> {
    let root = success_root(body)?;
    let observed_at_ms = required_u64(&root, "time")?;
    let result = result_object(&root)?;
    require_category(result)?;
    let rows = required_array(result, "list")?;
    if rows.len() != 1
        || !required_string(&rows[0], "symbol")?.eq_ignore_ascii_case(expected_symbol)
    {
        return Err(BybitCodecError::IdentityMismatch);
    }
    let last_price = required_decimal(&rows[0], "lastPrice")?;
    let mark_price = required_decimal(&rows[0], "markPrice")?;
    if observed_at_ms == 0 || last_price <= Decimal::ZERO || mark_price <= Decimal::ZERO {
        return Err(BybitCodecError::InvalidField("marketSnapshot"));
    }
    Ok(ExchangeMarketSnapshot {
        exchange: Exchange::Bybit,
        symbol: expected_symbol.to_ascii_uppercase(),
        last_price,
        mark_price,
        observed_at_ms,
    })
}

pub(super) fn parse_instrument_rules(
    body: &str,
    expected_symbol: &str,
) -> Result<InstrumentRules, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    require_category(result)?;
    if optional_string(result, "nextPageCursor")?.is_some_and(|cursor| !cursor.is_empty()) {
        return Err(BybitCodecError::DuplicateRecord);
    }
    let rows = required_array(result, "list")?;
    if rows.len() != 1
        || !required_string(&rows[0], "symbol")?.eq_ignore_ascii_case(expected_symbol)
    {
        return Err(BybitCodecError::IdentityMismatch);
    }
    let row = &rows[0];
    if !required_string(row, "status")?.eq_ignore_ascii_case("Trading") {
        return Err(BybitCodecError::InvalidField("status"));
    }
    let price_filter = required_object(row, "priceFilter")?;
    let lot_filter = required_object(row, "lotSizeFilter")?;
    let min = required_decimal(lot_filter, "minOrderQty")?;
    let step = required_decimal(lot_filter, "qtyStep")?;
    let rules = InstrumentRules {
        tick_size: required_decimal(price_filter, "tickSize")?,
        limit_quantity: QuantityRules {
            step,
            min,
            max: optional_positive_max(lot_filter, "maxOrderQty")?,
        },
        market_quantity: QuantityRules {
            step,
            min,
            max: optional_positive_max(lot_filter, "maxMktOrderQty")?,
        },
        min_notional: required_decimal(lot_filter, "minNotionalValue")?,
    };
    rules
        .validate()
        .map_err(|_| BybitCodecError::InvalidField("instrumentRules"))?;
    Ok(rules)
}

pub(super) fn parse_position_snapshot(
    body: &str,
    expected_symbol: &str,
    fallback_mark_price: Decimal,
) -> Result<PositionSnapshot, BybitCodecError> {
    if fallback_mark_price <= Decimal::ZERO {
        return Err(BybitCodecError::InvalidField("fallbackMarkPrice"));
    }
    let root = success_root(body)?;
    let result = result_object(&root)?;
    require_category(result)?;
    if optional_string(result, "nextPageCursor")?.is_some_and(|cursor| !cursor.is_empty()) {
        return Err(BybitCodecError::DuplicateRecord);
    }
    let rows = required_array(result, "list")?;
    let mut legs = Vec::new();
    let mut indexes = BTreeSet::new();
    for row in rows {
        if !required_string(row, "symbol")?.eq_ignore_ascii_case(expected_symbol) {
            return Err(BybitCodecError::IdentityMismatch);
        }
        let position_index = required_u64(row, "positionIdx")?;
        if !matches!(position_index, 0..=2) || !indexes.insert(position_index) {
            return Err(BybitCodecError::DuplicateRecord);
        }
        let size = required_decimal(row, "size")?;
        if size < Decimal::ZERO {
            return Err(BybitCodecError::InvalidField("size"));
        }
        let raw_side = optional_string(row, "side")?.unwrap_or_default();
        let (side, signed_quantity) = match (position_index, raw_side.as_str(), size.is_zero()) {
            (0, "", true) => (PositionSide::Both, Decimal::ZERO),
            (0, "Buy", _) => (PositionSide::Both, size),
            (0, "Sell", _) => (PositionSide::Both, -size),
            (1, "", true) => (PositionSide::Long, Decimal::ZERO),
            (1, "Buy", _) => (PositionSide::Long, size),
            (2, "", true) => (PositionSide::Short, Decimal::ZERO),
            (2, "Sell", _) => (PositionSide::Short, -size),
            _ => return Err(BybitCodecError::InvalidField("positionSide")),
        };
        let raw_entry_price = optional_decimal(row, "avgPrice")?;
        let entry_price = if size.is_zero() {
            if raw_entry_price.is_some_and(|price| price < Decimal::ZERO) {
                return Err(BybitCodecError::InvalidField("avgPrice"));
            }
            None
        } else {
            match raw_entry_price {
                Some(price) if price > Decimal::ZERO => Some(price),
                _ => return Err(BybitCodecError::InvalidField("avgPrice")),
            }
        };
        let mark_price = match optional_decimal(row, "markPrice")? {
            Some(price) if price > Decimal::ZERO => price,
            Some(price) if price.is_zero() && size.is_zero() => fallback_mark_price,
            None if size.is_zero() => fallback_mark_price,
            _ => return Err(BybitCodecError::InvalidField("markPrice")),
        };
        let unrealized_profit = optional_decimal(row, "unrealisedPnl")?.unwrap_or(Decimal::ZERO);
        let leverage = optional_positive_u16(row, "leverage")?;
        legs.push(PositionLeg {
            side,
            signed_quantity,
            entry_price,
            mark_price,
            unrealized_profit,
            leverage,
        });
    }
    if legs.is_empty() {
        return Err(BybitCodecError::InvalidField("positions"));
    }
    if indexes.contains(&0) && indexes.len() != 1 {
        return Err(BybitCodecError::InvalidField("positionIdx"));
    }
    legs.sort_by_key(|leg| match leg.side {
        PositionSide::Both => 0,
        PositionSide::Long => 1,
        PositionSide::Short => 2,
    });
    Ok(PositionSnapshot {
        exchange: Exchange::Bybit,
        symbol: expected_symbol.to_ascii_uppercase(),
        legs,
    })
}

pub(super) fn parse_historical_minute_open(
    body: &str,
    expected_symbol: &str,
    expected_minute_start_ms: u64,
) -> Result<HistoricalMinutePrice, BybitCodecError> {
    let root = success_root(body)?;
    let result = result_object(&root)?;
    require_category(result)?;
    if !required_string(result, "symbol")?.eq_ignore_ascii_case(expected_symbol) {
        return Err(BybitCodecError::IdentityMismatch);
    }
    let rows = required_array(result, "list")?;
    if rows.len() != 1 {
        return Err(BybitCodecError::InvalidField("kline"));
    }
    let row = rows[0]
        .as_array()
        .filter(|row| row.len() >= 2)
        .ok_or(BybitCodecError::InvalidField("kline"))?;
    let minute = scalar_text(&row[0])
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(BybitCodecError::InvalidField("kline.start"))?;
    let open = scalar_text(&row[1])
        .and_then(|value| value.parse::<Decimal>().ok())
        .ok_or(BybitCodecError::InvalidField("kline.open"))?;
    if minute != expected_minute_start_ms || open <= Decimal::ZERO {
        return Err(BybitCodecError::InvalidField("kline"));
    }
    Ok(HistoricalMinutePrice {
        exchange: Exchange::Bybit,
        symbol: expected_symbol.to_ascii_uppercase(),
        minute_start_ms: minute,
        open_price: open,
    })
}

fn success_root(body: &str) -> Result<Value, BybitCodecError> {
    let root: Value = serde_json::from_str(body)
        .map_err(|error| BybitCodecError::InvalidJson(error.to_string()))?;
    if required_i64(&root, "retCode")? != 0 {
        return Err(BybitCodecError::ExchangeRejected);
    }
    Ok(root)
}

fn result_object(root: &Value) -> Result<&Value, BybitCodecError> {
    required_object(root, "result")
}

fn require_category(result: &Value) -> Result<(), BybitCodecError> {
    if required_string(result, "category")?.eq_ignore_ascii_case(CATEGORY) {
        Ok(())
    } else {
        Err(BybitCodecError::IdentityMismatch)
    }
}

fn parse_side(value: &str) -> Result<OrderSide, BybitCodecError> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(BybitCodecError::InvalidField("side")),
    }
}

fn parse_lifecycle(value: &str) -> Result<OrderLifecycle, BybitCodecError> {
    match value.to_ascii_uppercase().as_str() {
        "NEW" => Ok(OrderLifecycle::Active(ActiveOrderStatus::New)),
        "PARTIALLYFILLED" => Ok(OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)),
        "FILLED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Filled)),
        "CANCELLED" | "CANCELED" | "PARTIALLYFILLEDCANCELED" => {
            Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled))
        }
        "REJECTED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)),
        "DEACTIVATED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Expired)),
        _ => Err(BybitCodecError::InvalidField("orderStatus")),
    }
}

fn optional_positive_max(
    value: &Value,
    field: &'static str,
) -> Result<Option<Decimal>, BybitCodecError> {
    let maximum = required_decimal(value, field)?;
    if maximum.is_zero() {
        Ok(None)
    } else if maximum > Decimal::ZERO {
        Ok(Some(maximum))
    } else {
        Err(BybitCodecError::InvalidField(field))
    }
}

fn required_object<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a Value, BybitCodecError> {
    value
        .get(field)
        .filter(|item| item.is_object())
        .ok_or(BybitCodecError::InvalidField(field))
}

fn required_array<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a [Value], BybitCodecError> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or(BybitCodecError::InvalidField(field))
}

fn required_string<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, BybitCodecError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or(BybitCodecError::InvalidField(field))
}

fn optional_string(value: &Value, field: &'static str) -> Result<Option<String>, BybitCodecError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        _ => Err(BybitCodecError::InvalidField(field)),
    }
}

fn required_decimal(value: &Value, field: &'static str) -> Result<Decimal, BybitCodecError> {
    value
        .get(field)
        .and_then(scalar_text)
        .and_then(|text| text.parse::<Decimal>().ok())
        .ok_or(BybitCodecError::InvalidField(field))
}

fn optional_decimal(
    value: &Value,
    field: &'static str,
) -> Result<Option<Decimal>, BybitCodecError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(item) => scalar_text(item)
            .and_then(|text| text.parse::<Decimal>().ok())
            .map(Some)
            .ok_or(BybitCodecError::InvalidField(field)),
    }
}

fn optional_positive_u16(
    value: &Value,
    field: &'static str,
) -> Result<Option<u16>, BybitCodecError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(item) => {
            let leverage = scalar_text(item)
                .and_then(|text| text.parse::<u16>().ok())
                .filter(|leverage| *leverage > 0)
                .ok_or(BybitCodecError::InvalidField(field))?;
            Ok(Some(leverage))
        }
    }
}

fn required_u64(value: &Value, field: &'static str) -> Result<u64, BybitCodecError> {
    value
        .get(field)
        .and_then(scalar_text)
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or(BybitCodecError::InvalidField(field))
}

fn required_i64(value: &Value, field: &'static str) -> Result<i64, BybitCodecError> {
    value
        .get(field)
        .and_then(scalar_text)
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(BybitCodecError::InvalidField(field))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, BybitCodecError> {
    match value.get(field) {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(BybitCodecError::InvalidField(field)),
    }
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_id() -> ClientOrderId {
        ClientOrderId::parse("g_0_S_bybit-test").unwrap()
    }

    fn order_response(status: &str, executed: &str, quote: &str) -> String {
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"category":"linear","nextPageCursor":"","list":[{{"orderId":"order-42","orderLinkId":"g_0_S_bybit-test","symbol":"MUUSDT","price":"1014","qty":"2.8","side":"Sell","positionIdx":0,"orderStatus":"{status}","cumExecQty":"{executed}","cumExecValue":"{quote}","timeInForce":"GTC","orderType":"Limit","reduceOnly":false,"createdTime":"1700000000000","updatedTime":"1700000001000"}}]}},"time":1700000001001}}"#
        )
    }

    #[test]
    fn text_execution_identity_and_exact_order_shape_are_preserved() {
        let header = parse_exact_order_record(
            &order_response("Filled", "2.8", "2839.2"),
            "MUUSDT",
            &client_id(),
            Some("order-42"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(header.order.shape.quantity, Decimal::new(28, 1));
        assert_eq!(header.order.shape.price, Some(Decimal::new(1014, 0)));

        let page = parse_execution_page(
            r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"cursor%3A2","list":[{"symbol":"MUUSDT","orderId":"order-42","orderLinkId":"g_0_S_bybit-test","side":"Sell","execType":"Trade","execFee":"0.5","execId":"e0cbe81d-0f18-5866-9415-cf319b5dab3b","execPrice":"1014","execQty":"2.8","execValue":"2839.2","execTime":"1700000000500","feeCurrency":"USDT","isMaker":true}]},"time":1700000001001}"#,
            "MUUSDT",
            &client_id(),
            "order-42",
        )
        .unwrap();
        assert_eq!(
            page.trades[0].trade_id,
            "e0cbe81d-0f18-5866-9415-cf319b5dab3b"
        );
        assert_eq!(page.next_cursor.as_deref(), Some("cursor%3A2"));
    }

    #[test]
    fn order_and_execution_identity_mismatches_fail_closed() {
        assert_eq!(
            parse_exact_order_record(
                &order_response("New", "0", "0"),
                "OTHERUSDT",
                &client_id(),
                None,
            ),
            Err(BybitCodecError::IdentityMismatch)
        );
        assert!(parse_execution_page(
            r#"{"retCode":0,"result":{"category":"linear","nextPageCursor":"","list":[{"symbol":"MUUSDT","orderId":"foreign","orderLinkId":"g_0_S_bybit-test","side":"Sell","execType":"Trade","execFee":"0","execId":"id-1","execPrice":"1014","execQty":"1","execValue":"1014","execTime":"1700000000500","feeCurrency":"USDT","isMaker":true}]}}"#,
            "MUUSDT",
            &client_id(),
            "order-42",
        )
        .is_err());
    }

    #[test]
    fn partial_cancellation_keeps_the_exact_executed_quantity() {
        let header = parse_exact_order_record(
            &order_response("Cancelled", "1.4", "1419.6"),
            "MUUSDT",
            &client_id(),
            Some("order-42"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            header.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled)
        );
        assert_eq!(header.cumulative_quantity, Decimal::new(14, 1));
        assert_eq!(header.cumulative_quote, Decimal::new(14196, 1));
    }

    #[test]
    fn instrument_and_position_parsers_preserve_distinct_exchange_rules() {
        let rules = parse_instrument_rules(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","status":"Trading","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"maxOrderQty":"100","minOrderQty":"0.1","qtyStep":"0.1","maxMktOrderQty":"50","minNotionalValue":"5"}}],"nextPageCursor":""}}"#,
            "MUUSDT",
        )
        .unwrap();
        assert_eq!(rules.limit_quantity.max, Some(Decimal::new(100, 0)));
        assert_eq!(rules.market_quantity.max, Some(Decimal::new(50, 0)));

        let position = parse_position_snapshot(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","positionIdx":0,"side":"Sell","size":"3","avgPrice":"1011","markPrice":"1000","unrealisedPnl":"33","leverage":"5"}]}}"#,
            "MUUSDT",
            Decimal::new(999, 0),
        )
        .unwrap();
        assert_eq!(
            position.one_way_position().unwrap(),
            (Decimal::new(-3, 0), Some(Decimal::new(1011, 0)))
        );
        assert_eq!(position.one_way_leverage().unwrap(), 5);
    }

    #[test]
    fn hedge_mode_and_empty_position_are_not_conflated() {
        let hedge = parse_position_snapshot(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","positionIdx":1,"side":"","size":"0","avgPrice":"","markPrice":"","unrealisedPnl":""},{"symbol":"MUUSDT","positionIdx":2,"side":"","size":"0","avgPrice":"","markPrice":"","unrealisedPnl":""}]}}"#,
            "MUUSDT",
            Decimal::new(1000, 0),
        )
        .unwrap();
        assert!(hedge.one_way_position().is_err());

        let flat = parse_position_snapshot(
            r#"{"retCode":0,"result":{"category":"linear","list":[]}}"#,
            "MUUSDT",
            Decimal::new(1000, 0),
        );
        assert_eq!(flat, Err(BybitCodecError::InvalidField("positions")));

        assert!(parse_position_snapshot(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","positionIdx":0,"side":"Sell","size":"1","avgPrice":"1011","markPrice":"-1","unrealisedPnl":"0"}]}}"#,
            "MUUSDT",
            Decimal::new(1000, 0),
        )
        .is_err());
    }

    #[test]
    fn leverage_acknowledgement_accepts_only_success_or_already_configured() {
        let changed = parse_leverage_acknowledgement(
            r#"{"retCode":0,"retMsg":"OK","result":{}}"#,
            "MUUSDT",
            5,
        )
        .unwrap();
        let unchanged = parse_leverage_acknowledgement(
            r#"{"retCode":110043,"retMsg":"Set leverage not modified"}"#,
            "MUUSDT",
            5,
        )
        .unwrap();
        assert_eq!(changed, unchanged);
        assert!(
            parse_leverage_acknowledgement(
                r#"{"retCode":10001,"retMsg":"bad leverage"}"#,
                "MUUSDT",
                5,
            )
            .is_err()
        );
    }

    #[test]
    fn fee_rate_parser_requires_one_exact_symbol_row() {
        let rates = parse_trading_fee_rates(
            r#"{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"MUUSDT","takerFeeRate":"0.0005","makerFeeRate":"0.0002"}]}}"#,
            "MUUSDT",
        )
        .unwrap();
        assert_eq!(rates.maker_rate, Decimal::new(2, 4));
        assert_eq!(rates.taker_rate, Decimal::new(5, 4));
        assert!(
            parse_trading_fee_rates(r#"{"retCode":0,"result":{"list":[]}}"#, "MUUSDT",).is_err()
        );
    }

    #[test]
    fn malformed_cursor_and_duplicate_execution_ids_are_rejected() {
        let duplicate = r#"{"retCode":0,"result":{"category":"linear","nextPageCursor":"","list":[{"symbol":"MUUSDT","orderId":"order-42","orderLinkId":"g_0_S_bybit-test","side":"Sell","execType":"Trade","execFee":"0","execId":"same","execPrice":"1014","execQty":"1","execValue":"1014","execTime":"1700000000500","feeCurrency":"USDT","isMaker":true},{"symbol":"MUUSDT","orderId":"order-42","orderLinkId":"g_0_S_bybit-test","side":"Sell","execType":"Trade","execFee":"0","execId":"same","execPrice":"1014","execQty":"1","execValue":"1014","execTime":"1700000000600","feeCurrency":"USDT","isMaker":true}]}}"#;
        assert_eq!(
            parse_execution_page(duplicate, "MUUSDT", &client_id(), "order-42"),
            Err(BybitCodecError::DuplicateRecord)
        );
    }
}
