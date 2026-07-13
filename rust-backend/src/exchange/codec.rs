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
        LeverageAcknowledgement, OrderLifecycle, PlacementAcknowledgement, PositionLeg,
        PositionSide, PositionSnapshot, SnapshotError, TradingFeeRates, protocol::Parameters,
        strategy_client_order_id,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExchangeErrorBody {
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum CodecError {
    #[error("response is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("response field {0} is missing or invalid")]
    InvalidField(&'static str),
    #[error("response client order ID does not match the request")]
    ClientOrderIdMismatch,
    #[error("response symbol does not match the request")]
    SymbolMismatch,
}

pub(super) fn parse_exchange_error(body: &str) -> ExchangeErrorBody {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return ExchangeErrorBody {
            code: None,
            message: "exchange returned a non-JSON error response".into(),
        };
    };
    ExchangeErrorBody {
        code: value.get("code").and_then(json_scalar_text),
        message: value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
            .unwrap_or("exchange rejected the request")
            .to_owned(),
    }
}

pub(super) fn parse_market_snapshot(
    ticker_body: &str,
    premium_body: &str,
    exchange: Exchange,
    expected_symbol: &str,
) -> Result<ExchangeMarketSnapshot, CodecError> {
    let ticker = parse_json(ticker_body)?;
    let premium = parse_json(premium_body)?;
    require_symbol(&ticker, expected_symbol)?;
    require_symbol(&premium, expected_symbol)?;
    let last_price = decimal_from_first(&ticker, &["lastPrice", "price"])?;
    let mark_price = required_decimal(&premium, "markPrice")?;
    if last_price <= Decimal::ZERO || mark_price <= Decimal::ZERO {
        return Err(CodecError::InvalidField("marketPrice"));
    }
    let observed_at_ms = required_scalar_text(&premium, "time")?
        .parse::<u64>()
        .map_err(|_| CodecError::InvalidField("time"))?;
    if observed_at_ms == 0 {
        return Err(CodecError::InvalidField("time"));
    }
    Ok(ExchangeMarketSnapshot {
        exchange,
        symbol: expected_symbol.to_ascii_uppercase(),
        last_price,
        mark_price,
        observed_at_ms,
    })
}

pub(super) fn parse_instrument_rules(
    body: &str,
    expected_symbol: &str,
) -> Result<InstrumentRules, CodecError> {
    let root = parse_json(body)?;
    let symbols = root
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(CodecError::InvalidField("symbols"))?;
    let matches = symbols
        .iter()
        .filter(|row| {
            row.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.eq_ignore_ascii_case(expected_symbol))
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(CodecError::InvalidField("symbols"));
    }
    let instrument = matches[0];
    if !required_string(instrument, "status")?.eq_ignore_ascii_case("TRADING") {
        return Err(CodecError::InvalidField("status"));
    }
    let filters = instrument
        .get("filters")
        .and_then(Value::as_array)
        .ok_or(CodecError::InvalidField("filters"))?;
    let price_filter = unique_filter(filters, "PRICE_FILTER")?;
    let lot_filter = unique_filter(filters, "LOT_SIZE")?;
    let market_filter = optional_unique_filter(filters, "MARKET_LOT_SIZE")?.unwrap_or(lot_filter);
    let legacy_notional = optional_unique_filter(filters, "MIN_NOTIONAL")?;
    let current_notional = optional_unique_filter(filters, "NOTIONAL")?;
    let notional_filter = match (legacy_notional, current_notional) {
        (Some(_), Some(_)) => return Err(CodecError::InvalidField("notionalFilter")),
        (legacy, current) => legacy.or(current),
    };
    let min_notional = match notional_filter {
        Some(filter) => decimal_from_first(filter, &["notional", "minNotional"])?,
        None => Decimal::ZERO,
    };
    let rules = InstrumentRules {
        tick_size: required_decimal(price_filter, "tickSize")?,
        limit_quantity: quantity_rules(lot_filter)?,
        market_quantity: quantity_rules(market_filter)?,
        min_notional,
    };
    rules
        .validate()
        .map_err(|_| CodecError::InvalidField("instrumentRules"))?;
    Ok(rules)
}

pub(super) fn parse_position_snapshot(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
) -> Result<PositionSnapshot, CodecError> {
    let root = parse_json(body)?;
    let rows = root
        .as_array()
        .ok_or(CodecError::InvalidField("positions"))?;
    let mut legs = Vec::new();
    for row in rows {
        let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
            return Err(CodecError::InvalidField("symbol"));
        };
        if !symbol.eq_ignore_ascii_case(expected_symbol) {
            continue;
        }
        let side = match required_string(row, "positionSide")?
            .to_ascii_uppercase()
            .as_str()
        {
            "BOTH" => PositionSide::Both,
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ => return Err(CodecError::InvalidField("positionSide")),
        };
        if legs.iter().any(|leg: &PositionLeg| leg.side == side) {
            return Err(CodecError::InvalidField("positionSide"));
        }
        let signed_quantity = required_decimal(row, "positionAmt")?;
        if (side == PositionSide::Long && signed_quantity < Decimal::ZERO)
            || (side == PositionSide::Short && signed_quantity > Decimal::ZERO)
        {
            return Err(CodecError::InvalidField("positionAmt"));
        }
        let raw_entry_price = required_decimal(row, "entryPrice")?;
        let entry_price = if signed_quantity.is_zero() {
            if raw_entry_price < Decimal::ZERO {
                return Err(CodecError::InvalidField("entryPrice"));
            }
            None
        } else if raw_entry_price > Decimal::ZERO {
            Some(raw_entry_price)
        } else {
            return Err(CodecError::InvalidField("entryPrice"));
        };
        let mark_price = required_decimal(row, "markPrice")?;
        if mark_price <= Decimal::ZERO {
            return Err(CodecError::InvalidField("markPrice"));
        }
        let unrealized_profit = decimal_from_first(
            row,
            &["unRealizedProfit", "unrealizedProfit", "unrealisedPnl"],
        )?;
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
        return Err(CodecError::InvalidField("positions"));
    }
    legs.sort_by_key(|leg| match leg.side {
        PositionSide::Both => 0,
        PositionSide::Long => 1,
        PositionSide::Short => 2,
    });
    Ok(PositionSnapshot {
        exchange,
        symbol: expected_symbol.to_ascii_uppercase(),
        legs,
    })
}

pub(super) fn parse_leverage_acknowledgement(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
    expected_leverage: u16,
) -> Result<LeverageAcknowledgement, CodecError> {
    let root = parse_json(body)?;
    require_symbol(&root, expected_symbol)?;
    let leverage = required_scalar_text(&root, "leverage")?
        .parse::<u16>()
        .map_err(|_| CodecError::InvalidField("leverage"))?;
    if leverage == 0 || leverage != expected_leverage {
        return Err(CodecError::InvalidField("leverage"));
    }
    Ok(LeverageAcknowledgement {
        exchange,
        symbol: expected_symbol.to_ascii_uppercase(),
        leverage,
    })
}

pub(super) fn parse_trading_fee_rates(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
) -> Result<TradingFeeRates, CodecError> {
    let root = parse_json(body)?;
    require_symbol(&root, expected_symbol)?;
    let rates = TradingFeeRates {
        exchange,
        symbol: expected_symbol.to_ascii_uppercase(),
        maker_rate: required_decimal(&root, "makerCommissionRate")?,
        taker_rate: required_decimal(&root, "takerCommissionRate")?,
    };
    rates
        .validate()
        .map_err(|_| CodecError::InvalidField("commissionRate"))?;
    Ok(rates)
}

pub(super) fn execution_status_is_unknown(code: Option<&str>) -> bool {
    matches!(code, Some("-1006" | "-1007"))
}

pub(super) fn order_is_definitively_absent(code: Option<&str>) -> bool {
    matches!(code, Some("-2013"))
}

pub(super) fn validate_snapshot_request(
    actual_exchange: Exchange,
    expected_exchange: Exchange,
    symbol: &str,
) -> Result<(), SnapshotError> {
    if actual_exchange != expected_exchange {
        return Err(SnapshotError::new("snapshot belongs to another exchange"));
    }
    if symbol.trim().is_empty() || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(SnapshotError::new("snapshot symbol is invalid"));
    }
    Ok(())
}

pub(super) fn build_order_parameters(
    client_order_id: &ClientOrderId,
    shape: &OrderShape,
) -> Result<Parameters, CodecError> {
    let mut parameters = vec![
        ("symbol".into(), shape.symbol.clone()),
        (
            "side".into(),
            match shape.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            }
            .into(),
        ),
        (
            "type".into(),
            match shape.kind {
                OrderKind::Limit => "LIMIT",
                OrderKind::Market => "MARKET",
            }
            .into(),
        ),
        ("quantity".into(), shape.quantity.to_string()),
        ("reduceOnly".into(), shape.reduce_only.to_string()),
    ];
    if shape.kind == OrderKind::Limit {
        parameters.push((
            "price".into(),
            shape
                .price
                .ok_or(CodecError::InvalidField("orderShape.price"))?
                .to_string(),
        ));
        parameters.push((
            "timeInForce".into(),
            match shape.time_in_force {
                TimeInForce::Gtc => "GTC",
                TimeInForce::PostOnly => "GTX",
            }
            .into(),
        ));
    }
    parameters.push(("newClientOrderId".into(), client_order_id.as_str().into()));
    Ok(parameters)
}

pub(super) fn parse_placement_acknowledgement(
    body: &str,
    expected_client_order_id: &ClientOrderId,
) -> Result<PlacementAcknowledgement, CodecError> {
    let value: Value =
        serde_json::from_str(body).map_err(|error| CodecError::InvalidJson(error.to_string()))?;
    let exchange_order_id = required_scalar_text(&value, "orderId")?;
    let returned_client_id = required_scalar_text(&value, "clientOrderId")?;
    if returned_client_id != expected_client_order_id.as_str() {
        return Err(CodecError::ClientOrderIdMismatch);
    }

    Ok(PlacementAcknowledgement {
        client_order_id: expected_client_order_id.clone(),
        exchange_order_id,
    })
}

pub(super) fn parse_cancellation_acknowledgement(
    body: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: &str,
) -> Result<CancellationAcknowledgement, CodecError> {
    let value: Value =
        serde_json::from_str(body).map_err(|error| CodecError::InvalidJson(error.to_string()))?;
    let exchange_order_id = required_scalar_text(&value, "orderId")?;
    if exchange_order_id != expected_exchange_order_id {
        return Err(CodecError::InvalidField("orderId"));
    }
    let returned_client_id = required_scalar_text(&value, "clientOrderId")?;
    if returned_client_id != expected_client_order_id.as_str() {
        return Err(CodecError::ClientOrderIdMismatch);
    }
    if !matches!(
        required_string(&value, "status")?
            .to_ascii_uppercase()
            .as_str(),
        "CANCELED" | "CANCELLED"
    ) {
        return Err(CodecError::InvalidField("status"));
    }
    Ok(CancellationAcknowledgement {
        client_order_id: expected_client_order_id.clone(),
        exchange_order_id,
    })
}

pub(super) fn parse_authoritative_order(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
    expected_client_order_id: &ClientOrderId,
) -> Result<AuthoritativeOrder, CodecError> {
    let value = parse_json(body)?;
    parse_authoritative_order_value(
        &value,
        exchange,
        expected_symbol,
        Some(expected_client_order_id),
    )
}

pub(super) fn parse_open_orders(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
) -> Result<Vec<AuthoritativeOrder>, CodecError> {
    const MAX_OPEN_ORDERS: usize = 1_000;

    let value = parse_json(body)?;
    let rows = value
        .as_array()
        .ok_or(CodecError::InvalidField("openOrders"))?;
    if rows.len() > MAX_OPEN_ORDERS {
        return Err(CodecError::InvalidField("openOrders"));
    }
    let mut client_order_ids = BTreeSet::new();
    let mut exchange_order_ids = BTreeSet::new();
    let mut orders = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(raw_client_order_id) = row
            .get("clientOrderId")
            .and_then(json_scalar_text)
            .filter(|value| !value.trim().is_empty())
        else {
            continue;
        };
        if strategy_client_order_id(&raw_client_order_id)
            .map_err(|_| CodecError::InvalidField("clientOrderId"))?
            .is_none()
        {
            continue;
        }
        let order = parse_authoritative_order_value(row, exchange, expected_symbol, None)?;
        if !matches!(order.lifecycle, OrderLifecycle::Active(_))
            || !client_order_ids.insert(order.client_order_id.clone())
            || !exchange_order_ids.insert(order.exchange_order_id.clone())
        {
            return Err(CodecError::InvalidField("openOrders"));
        }
        orders.push(order);
    }
    orders.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
    Ok(orders)
}

fn parse_authoritative_order_value(
    value: &Value,
    exchange: Exchange,
    expected_symbol: &str,
    expected_client_order_id: Option<&ClientOrderId>,
) -> Result<AuthoritativeOrder, CodecError> {
    let symbol = required_string(value, "symbol")?.to_ascii_uppercase();
    if symbol != expected_symbol.to_ascii_uppercase() {
        return Err(CodecError::SymbolMismatch);
    }

    let returned_client_id = required_scalar_text(value, "clientOrderId")?;
    if expected_client_order_id.is_some_and(|expected| returned_client_id != expected.as_str()) {
        return Err(CodecError::ClientOrderIdMismatch);
    }
    let client_order_id = ClientOrderId::parse(returned_client_id)
        .map_err(|_| CodecError::InvalidField("clientOrderId"))?;
    let exchange_order_id = required_scalar_text(value, "orderId")?;
    let side = match required_string(value, "side")?
        .to_ascii_uppercase()
        .as_str()
    {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(CodecError::InvalidField("side")),
    };
    let kind = match required_string(value, "type")?
        .to_ascii_uppercase()
        .as_str()
    {
        "LIMIT" => OrderKind::Limit,
        "MARKET" => OrderKind::Market,
        _ => return Err(CodecError::InvalidField("type")),
    };
    let quantity = required_decimal(value, "origQty")?;
    let reduce_only = required_bool(value, "reduceOnly")?;
    let (price, time_in_force) = match kind {
        OrderKind::Limit => {
            let price = required_decimal(value, "price")?;
            let time_in_force = match required_string(value, "timeInForce")?
                .to_ascii_uppercase()
                .as_str()
            {
                "GTC" => TimeInForce::Gtc,
                "GTX" => TimeInForce::PostOnly,
                _ => return Err(CodecError::InvalidField("timeInForce")),
            };
            (Some(price), time_in_force)
        }
        OrderKind::Market => (None, TimeInForce::Gtc),
    };
    let shape = OrderShape {
        symbol,
        side,
        price,
        quantity,
        reduce_only,
        kind,
        time_in_force,
    };
    shape
        .validate()
        .map_err(|_| CodecError::InvalidField("orderShape"))?;

    Ok(AuthoritativeOrder {
        client_order_id,
        exchange_order_id,
        exchange,
        shape,
        lifecycle: parse_lifecycle(required_string(value, "status")?)?,
    })
}

fn parse_lifecycle(status: &str) -> Result<OrderLifecycle, CodecError> {
    match status.to_ascii_uppercase().as_str() {
        "NEW" => Ok(OrderLifecycle::Active(ActiveOrderStatus::New)),
        "PARTIALLY_FILLED" => Ok(OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)),
        "FILLED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Filled)),
        "CANCELED" | "CANCELLED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled)),
        "REJECTED" => Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)),
        "EXPIRED" | "EXPIRED_IN_MATCH" => {
            Ok(OrderLifecycle::Terminal(TerminalOrderStatus::Expired))
        }
        _ => Err(CodecError::InvalidField("status")),
    }
}

fn parse_json(body: &str) -> Result<Value, CodecError> {
    serde_json::from_str(body).map_err(|error| CodecError::InvalidJson(error.to_string()))
}

fn require_symbol(value: &Value, expected_symbol: &str) -> Result<(), CodecError> {
    if required_string(value, "symbol")?.eq_ignore_ascii_case(expected_symbol) {
        Ok(())
    } else {
        Err(CodecError::SymbolMismatch)
    }
}

fn decimal_from_first(value: &Value, fields: &[&'static str]) -> Result<Decimal, CodecError> {
    for field in fields {
        if value.get(field).is_some() {
            return required_decimal(value, field);
        }
    }
    Err(CodecError::InvalidField(
        fields.first().copied().unwrap_or("decimal"),
    ))
}

fn unique_filter<'a>(
    filters: &'a [Value],
    filter_type: &'static str,
) -> Result<&'a Value, CodecError> {
    optional_unique_filter(filters, filter_type)?.ok_or(CodecError::InvalidField(filter_type))
}

fn optional_unique_filter<'a>(
    filters: &'a [Value],
    filter_type: &'static str,
) -> Result<Option<&'a Value>, CodecError> {
    let mut found = None;
    for filter in filters {
        let current_type = required_string(filter, "filterType")?;
        if current_type.eq_ignore_ascii_case(filter_type) {
            if found.is_some() {
                return Err(CodecError::InvalidField(filter_type));
            }
            found = Some(filter);
        }
    }
    Ok(found)
}

fn quantity_rules(filter: &Value) -> Result<QuantityRules, CodecError> {
    let maximum = required_decimal(filter, "maxQty")?;
    let max = if maximum.is_zero() {
        None
    } else if maximum > Decimal::ZERO {
        Some(maximum)
    } else {
        return Err(CodecError::InvalidField("maxQty"));
    };
    Ok(QuantityRules {
        step: required_decimal(filter, "stepSize")?,
        min: required_decimal(filter, "minQty")?,
        max,
    })
}

fn required_string<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, CodecError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or(CodecError::InvalidField(field))
}

fn required_scalar_text(value: &Value, field: &'static str) -> Result<String, CodecError> {
    value
        .get(field)
        .and_then(json_scalar_text)
        .filter(|text| !text.trim().is_empty())
        .ok_or(CodecError::InvalidField(field))
}

fn json_scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn required_decimal(value: &Value, field: &'static str) -> Result<Decimal, CodecError> {
    required_scalar_text(value, field)?
        .parse::<Decimal>()
        .map_err(|_| CodecError::InvalidField(field))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, CodecError> {
    match value.get(field) {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(CodecError::InvalidField(field)),
    }
}

fn optional_positive_u16(value: &Value, field: &'static str) -> Result<Option<u16>, CodecError> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    let leverage = json_scalar_text(raw)
        .filter(|text| !text.trim().is_empty())
        .ok_or(CodecError::InvalidField(field))?
        .parse::<u16>()
        .map_err(|_| CodecError::InvalidField(field))?;
    if leverage == 0 {
        return Err(CodecError::InvalidField(field));
    }
    Ok(Some(leverage))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoritative_parser_preserves_exchange_quantity_exactly() {
        let order = parse_authoritative_order(
            r#"{
                "symbol":"ANSEMUSDT","orderId":4770039,"clientOrderId":"g_0_B_test",
                "side":"BUY","price":"0.3800000","origQty":"70","status":"NEW",
                "reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"
            }"#,
            Exchange::Aster,
            "ANSEMUSDT",
            &ClientOrderId::parse("g_0_B_test").unwrap(),
        )
        .unwrap();

        assert_eq!(order.shape.quantity, Decimal::new(70, 0));
        assert_eq!(order.shape.price, Some(Decimal::new(38, 2)));
        assert_eq!(
            order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        );
    }

    #[test]
    fn open_order_snapshot_is_complete_sorted_and_active_only() {
        let orders = parse_open_orders(
            r#"[
                {"symbol":"ANSEMUSDT","orderId":2,"clientOrderId":"g_RUN00001_2_S_2","side":"SELL","price":"0.382","origQty":"100","status":"PARTIALLY_FILLED","reduceOnly":false,"timeInForce":"GTC","type":"LIMIT"},
                {"symbol":"ANSEMUSDT","orderId":1,"clientOrderId":"g_RUN00001_1_B_1","side":"BUY","price":"0.380","origQty":"100","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"}
            ]"#,
            Exchange::Aster,
            "ANSEMUSDT",
        )
        .unwrap();

        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].client_order_id.as_str(), "g_RUN00001_1_B_1");
        assert_eq!(orders[1].shape.quantity, Decimal::new(100, 0));
        assert_eq!(
            orders[1].lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
        );
    }

    #[test]
    fn open_order_snapshot_rejects_terminal_duplicate_and_foreign_rows() {
        let base = r#"{"symbol":"MUUSDT","orderId":1,"clientOrderId":"g_RUN00001_1_B_1","side":"BUY","price":"1010","origQty":"1","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"}"#;
        let terminal = base.replace("\"NEW\"", "\"FILLED\"");
        let foreign = base.replace("MUUSDT", "ANSEMUSDT");
        assert!(parse_open_orders(&format!("[{terminal}]"), Exchange::Binance, "MUUSDT").is_err());
        assert!(
            parse_open_orders(&format!("[{base},{base}]"), Exchange::Binance, "MUUSDT").is_err()
        );
        assert!(parse_open_orders(&format!("[{foreign}]"), Exchange::Binance, "MUUSDT").is_err());
    }

    #[test]
    fn unrelated_manual_order_ids_do_not_block_owned_order_snapshot() {
        let orders = parse_open_orders(
            r#"[
                {"symbol":"MUUSDT","orderId":8,"clientOrderId":"manual_1","side":"BUY","price":"1000","origQty":"1","status":"NEW","reduceOnly":false,"timeInForce":"GTC","type":"LIMIT"},
                {"symbol":"MUUSDT","orderId":9,"clientOrderId":"manual:id/with.dots","side":"BUY","price":"1000","origQty":"1","status":"NEW","reduceOnly":false,"timeInForce":"GTC","type":"LIMIT"},
                {"symbol":"MUUSDT","orderId":1,"clientOrderId":"g_RUN00001_1_B_1","side":"BUY","price":"1010","origQty":"1","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"}
            ]"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].client_order_id.as_str(), "g_RUN00001_1_B_1");
    }

    #[test]
    fn malformed_strategy_order_id_fails_the_complete_snapshot() {
        assert!(
            parse_open_orders(
                r#"[{"symbol":"MUUSDT","orderId":1,"clientOrderId":"g_bad:id","side":"BUY","price":"1010","origQty":"1","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"}]"#,
                Exchange::Binance,
                "MUUSDT",
            )
            .is_err()
        );
    }

    #[test]
    fn leverage_acknowledgement_requires_exact_symbol_and_value() {
        let acknowledgement = parse_leverage_acknowledgement(
            r#"{"symbol":"MUUSDT","leverage":5,"maxNotionalValue":"100000"}"#,
            Exchange::Binance,
            "MUUSDT",
            5,
        )
        .unwrap();
        assert_eq!(acknowledgement.leverage, 5);
        assert!(
            parse_leverage_acknowledgement(
                r#"{"symbol":"MUUSDT","leverage":3}"#,
                Exchange::Binance,
                "MUUSDT",
                5,
            )
            .is_err()
        );
    }

    #[test]
    fn fee_rate_snapshot_preserves_exact_account_rates_and_identity() {
        let rates = parse_trading_fee_rates(
            r#"{"symbol":"MUUSDT","makerCommissionRate":"0.0002","takerCommissionRate":"0.0005"}"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .unwrap();
        assert_eq!(rates.maker_rate, Decimal::new(2, 4));
        assert_eq!(rates.taker_rate, Decimal::new(5, 4));
        assert!(parse_trading_fee_rates(
            r#"{"symbol":"OTHERUSDT","makerCommissionRate":"0.0002","takerCommissionRate":"0.0005"}"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .is_err());
    }

    #[test]
    fn malformed_or_foreign_identity_is_never_authoritative() {
        let expected = ClientOrderId::parse("g_0_B_expected").unwrap();
        assert_eq!(
            parse_placement_acknowledgement(
                r#"{"orderId":12,"clientOrderId":"g_0_B_other"}"#,
                &expected,
            ),
            Err(CodecError::ClientOrderIdMismatch)
        );
    }

    #[test]
    fn market_snapshot_requires_matching_positive_ticker_and_mark_price() {
        let snapshot = parse_market_snapshot(
            r#"{"symbol":"MUUSDT","lastPrice":"1011.25"}"#,
            r#"{"symbol":"MUUSDT","markPrice":"1011.20","time":1700000000000}"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .unwrap();

        assert_eq!(snapshot.last_price, Decimal::new(101125, 2));
        assert_eq!(snapshot.mark_price, Decimal::new(101120, 2));
        assert_eq!(snapshot.observed_at_ms, 1_700_000_000_000);
        assert_eq!(
            parse_market_snapshot(
                r#"{"symbol":"OTHERUSDT","lastPrice":"1011.25"}"#,
                r#"{"symbol":"MUUSDT","markPrice":"1011.20"}"#,
                Exchange::Binance,
                "MUUSDT",
            ),
            Err(CodecError::SymbolMismatch)
        );
    }

    #[test]
    fn instrument_snapshot_preserves_distinct_limit_and_market_quantity_rules() {
        let rules = parse_instrument_rules(
            r#"{
                "symbols":[{
                    "symbol":"MUUSDT","status":"TRADING","filters":[
                        {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                        {"filterType":"LOT_SIZE","stepSize":"0.01","minQty":"0.01","maxQty":"100"},
                        {"filterType":"MARKET_LOT_SIZE","stepSize":"0.1","minQty":"0.1","maxQty":"50"},
                        {"filterType":"MIN_NOTIONAL","notional":"5"}
                    ]
                }]
            }"#,
            "MUUSDT",
        )
        .unwrap();

        assert_eq!(rules.tick_size, Decimal::new(1, 2));
        assert_eq!(rules.limit_quantity.step, Decimal::new(1, 2));
        assert_eq!(rules.market_quantity.step, Decimal::new(1, 1));
        assert_eq!(rules.market_quantity.max, Some(Decimal::new(50, 0)));
        assert_eq!(rules.min_notional, Decimal::new(5, 0));
    }

    #[test]
    fn instrument_snapshot_rejects_duplicate_or_non_trading_contracts() {
        let duplicate = r#"{
            "symbols":[{
                "symbol":"MUUSDT","status":"TRADING","filters":[
                    {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                    {"filterType":"PRICE_FILTER","tickSize":"0.02"},
                    {"filterType":"LOT_SIZE","stepSize":"0.1","minQty":"0.1","maxQty":"10"}
                ]
            }]
        }"#;
        assert!(parse_instrument_rules(duplicate, "MUUSDT").is_err());

        let paused = r#"{
            "symbols":[{
                "symbol":"MUUSDT","status":"BREAK","filters":[
                    {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                    {"filterType":"LOT_SIZE","stepSize":"0.1","minQty":"0.1","maxQty":"10"}
                ]
            }]
        }"#;
        assert!(parse_instrument_rules(paused, "MUUSDT").is_err());
    }

    #[test]
    fn one_way_position_preserves_old_short_baseline_exactly() {
        let snapshot = parse_position_snapshot(
            r#"[{
                "symbol":"MUUSDT","positionSide":"BOTH","positionAmt":"-3",
                "entryPrice":"1011.25","markPrice":"1008.10","unRealizedProfit":"9.45"
            }]"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .unwrap();

        assert_eq!(
            snapshot.one_way_position().unwrap(),
            (Decimal::new(-3, 0), Some(Decimal::new(101125, 2)))
        );
    }

    #[test]
    fn hedge_position_is_visible_but_cannot_be_netted_into_one_way_baseline() {
        let snapshot = parse_position_snapshot(
            r#"[
                {"symbol":"MUUSDT","positionSide":"LONG","positionAmt":"2","entryPrice":"1000","markPrice":"1010","unRealizedProfit":"20"},
                {"symbol":"MUUSDT","positionSide":"SHORT","positionAmt":"-1","entryPrice":"1020","markPrice":"1010","unRealizedProfit":"10"}
            ]"#,
            Exchange::Aster,
            "MUUSDT",
        )
        .unwrap();

        assert_eq!(snapshot.legs.len(), 2);
        assert!(snapshot.one_way_position().is_err());
    }

    #[test]
    fn flat_position_has_no_fake_entry_price() {
        let snapshot = parse_position_snapshot(
            r#"[{
                "symbol":"MUUSDT","positionSide":"BOTH","positionAmt":"0",
                "entryPrice":"0","markPrice":"1010","unRealizedProfit":"0"
            }]"#,
            Exchange::Binance,
            "MUUSDT",
        )
        .unwrap();
        assert_eq!(snapshot.one_way_position().unwrap(), (Decimal::ZERO, None));
    }
}
