use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::{
    domain::{ClientOrderId, Exchange, OrderSide, TerminalOrderStatus},
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, HistoricalMinutePrice, OrderExecutionSnapshot,
        OrderLifecycle, TradeFill, codec::parse_authoritative_order, compare_trade_chronology,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommissionConvention {
    PositiveCost,
    SignedBalanceDeltaOrPositiveCost,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct OrderExecutionHeader {
    pub order: AuthoritativeOrder,
    pub cumulative_quantity: Decimal,
    pub cumulative_quote: Decimal,
    pub order_time_ms: u64,
    pub update_time_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum ExecutionCodecError {
    #[error("execution response is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("execution response field {0} is missing or invalid")]
    InvalidField(&'static str),
    #[error("execution response order identity does not match the request")]
    OrderIdentityMismatch,
    #[error("execution response contains duplicate trade IDs")]
    DuplicateTradeId,
    #[error("execution response trade totals do not match the authoritative order totals")]
    TotalsMismatch,
}

pub(super) fn parse_order_execution_header(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
    expected_client_order_id: &ClientOrderId,
    expected_exchange_order_id: &str,
) -> Result<OrderExecutionHeader, ExecutionCodecError> {
    if expected_exchange_order_id.trim().is_empty() {
        return Err(ExecutionCodecError::OrderIdentityMismatch);
    }
    let order =
        parse_authoritative_order(body, exchange, expected_symbol, expected_client_order_id)
            .map_err(|_| ExecutionCodecError::InvalidField("order"))?;
    if order.exchange_order_id != expected_exchange_order_id {
        return Err(ExecutionCodecError::OrderIdentityMismatch);
    }
    let value = parse_json(body)?;
    let cumulative_quantity = required_decimal(&value, "executedQty")?;
    let cumulative_quote = required_decimal(&value, "cumQuote")?;
    let order_time_ms = required_u64(&value, "time")?;
    let update_time_ms = required_u64(&value, "updateTime")?;
    if cumulative_quantity < Decimal::ZERO
        || cumulative_quote < Decimal::ZERO
        || cumulative_quantity > order.shape.quantity
        || order_time_ms == 0
        || update_time_ms < order_time_ms
        || (cumulative_quantity.is_zero() && !cumulative_quote.is_zero())
        || (cumulative_quantity > Decimal::ZERO && cumulative_quote <= Decimal::ZERO)
    {
        return Err(ExecutionCodecError::InvalidField("orderExecutionTotals"));
    }
    match order.lifecycle {
        OrderLifecycle::Active(ActiveOrderStatus::New) if !cumulative_quantity.is_zero() => {
            return Err(ExecutionCodecError::InvalidField("status"));
        }
        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
            if cumulative_quantity <= Decimal::ZERO
                || cumulative_quantity >= order.shape.quantity =>
        {
            return Err(ExecutionCodecError::InvalidField("status"));
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
            if cumulative_quantity != order.shape.quantity =>
        {
            return Err(ExecutionCodecError::InvalidField("status"));
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)
            if !cumulative_quantity.is_zero() =>
        {
            return Err(ExecutionCodecError::InvalidField("status"));
        }
        _ => {}
    }
    Ok(OrderExecutionHeader {
        order,
        cumulative_quantity,
        cumulative_quote,
        order_time_ms,
        update_time_ms,
    })
}

pub(super) fn parse_trade_page(
    body: &str,
    expected_symbol: &str,
    convention: CommissionConvention,
) -> Result<Vec<TradeFill>, ExecutionCodecError> {
    let root = parse_json(body)?;
    let rows = root
        .as_array()
        .ok_or(ExecutionCodecError::InvalidField("trades"))?;
    let mut trades = Vec::with_capacity(rows.len());
    let mut page_ids = BTreeSet::new();
    for row in rows {
        let symbol = required_string(row, "symbol")?.to_ascii_uppercase();
        if symbol != expected_symbol.to_ascii_uppercase() {
            return Err(ExecutionCodecError::InvalidField("symbol"));
        }
        let numeric_trade_id = required_u64(row, "id")?;
        if !page_ids.insert(numeric_trade_id) {
            return Err(ExecutionCodecError::DuplicateTradeId);
        }
        let exchange_order_id = required_scalar_text(row, "orderId")?;
        let side = match required_string(row, "side")?.to_ascii_uppercase().as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => return Err(ExecutionCodecError::InvalidField("side")),
        };
        if let Some(buyer) = optional_bool(row, "buyer")?
            && buyer != (side == OrderSide::Buy)
        {
            return Err(ExecutionCodecError::InvalidField("buyer"));
        }
        let price = required_decimal(row, "price")?;
        let quantity = required_decimal(row, "qty")?;
        let quote_quantity = required_decimal(row, "quoteQty")?;
        let raw_commission = required_decimal(row, "commission")?;
        let commission_cost = match convention {
            CommissionConvention::PositiveCost => {
                if raw_commission < Decimal::ZERO {
                    return Err(ExecutionCodecError::InvalidField("commission"));
                }
                raw_commission
            }
            CommissionConvention::SignedBalanceDeltaOrPositiveCost => raw_commission.abs(),
        };
        let commission_asset = required_string(row, "commissionAsset")?.to_ascii_uppercase();
        if !commission_asset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(ExecutionCodecError::InvalidField("commissionAsset"));
        }
        let realized_profit = optional_decimal(row, "realizedPnl")?.unwrap_or(Decimal::ZERO);
        let is_maker = required_bool(row, "maker")?;
        let trade_time_ms = required_u64(row, "time")?;
        if numeric_trade_id == 0
            || exchange_order_id.trim().is_empty()
            || price <= Decimal::ZERO
            || quantity <= Decimal::ZERO
            || quote_quantity <= Decimal::ZERO
            || trade_time_ms == 0
        {
            return Err(ExecutionCodecError::InvalidField("trade"));
        }
        trades.push(TradeFill {
            trade_id: numeric_trade_id.to_string(),
            exchange_order_id,
            symbol,
            side,
            price,
            quantity,
            quote_quantity,
            raw_commission,
            commission_cost,
            commission_asset,
            realized_profit,
            is_maker,
            trade_time_ms,
        });
    }
    Ok(trades)
}

pub(super) fn parse_historical_minute_open(
    body: &str,
    exchange: Exchange,
    expected_symbol: &str,
    expected_minute_start_ms: u64,
) -> Result<HistoricalMinutePrice, ExecutionCodecError> {
    if expected_minute_start_ms == 0 || !expected_minute_start_ms.is_multiple_of(60_000) {
        return Err(ExecutionCodecError::InvalidField("minuteStart"));
    }
    let root = parse_json(body)?;
    let rows = root
        .as_array()
        .ok_or(ExecutionCodecError::InvalidField("klines"))?;
    if rows.len() != 1 {
        return Err(ExecutionCodecError::InvalidField("klines"));
    }
    let row = rows[0]
        .as_array()
        .filter(|row| row.len() >= 2)
        .ok_or(ExecutionCodecError::InvalidField("kline"))?;
    let minute_start_ms = scalar_text(&row[0])
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(ExecutionCodecError::InvalidField("kline.openTime"))?;
    let open_price = scalar_text(&row[1])
        .and_then(|value| value.parse::<Decimal>().ok())
        .ok_or(ExecutionCodecError::InvalidField("kline.open"))?;
    if minute_start_ms != expected_minute_start_ms || open_price <= Decimal::ZERO {
        return Err(ExecutionCodecError::InvalidField("kline"));
    }
    Ok(HistoricalMinutePrice {
        exchange,
        symbol: expected_symbol.to_ascii_uppercase(),
        minute_start_ms,
        open_price,
    })
}

pub(super) fn assemble_execution_snapshot(
    header: OrderExecutionHeader,
    mut trades: Vec<TradeFill>,
) -> Result<OrderExecutionSnapshot, ExecutionCodecError> {
    trades.sort_by(|left, right| {
        compare_trade_chronology(
            left.trade_time_ms,
            &left.trade_id,
            right.trade_time_ms,
            &right.trade_id,
        )
    });
    let mut ids = BTreeSet::new();
    let mut quantity = Decimal::ZERO;
    let mut quote = Decimal::ZERO;
    let mut fees_by_asset = BTreeMap::new();
    for trade in &trades {
        if !ids.insert(trade.trade_id.clone()) {
            return Err(ExecutionCodecError::DuplicateTradeId);
        }
        if trade.exchange_order_id != header.order.exchange_order_id
            || trade.symbol != header.order.shape.symbol
            || trade.side != header.order.shape.side
        {
            return Err(ExecutionCodecError::OrderIdentityMismatch);
        }
        quantity = quantity
            .checked_add(trade.quantity)
            .ok_or(ExecutionCodecError::InvalidField("tradeQuantity"))?;
        quote = quote
            .checked_add(trade.quote_quantity)
            .ok_or(ExecutionCodecError::InvalidField("tradeQuote"))?;
        let current = fees_by_asset
            .entry(trade.commission_asset.clone())
            .or_insert(Decimal::ZERO);
        *current = current
            .checked_add(trade.commission_cost)
            .ok_or(ExecutionCodecError::InvalidField("commission"))?;
    }
    if quantity != header.cumulative_quantity || quote != header.cumulative_quote {
        return Err(ExecutionCodecError::TotalsMismatch);
    }
    Ok(OrderExecutionSnapshot {
        order: header.order,
        cumulative_quantity: header.cumulative_quantity,
        cumulative_quote: header.cumulative_quote,
        fees_by_asset,
        trades,
        order_time_ms: header.order_time_ms,
        update_time_ms: header.update_time_ms,
    })
}

pub(super) fn numeric_trade_id(trade: &TradeFill) -> Result<u64, ExecutionCodecError> {
    let value = trade
        .trade_id
        .parse::<u64>()
        .map_err(|_| ExecutionCodecError::InvalidField("trade.id"))?;
    if value == 0 || value.to_string() != trade.trade_id {
        return Err(ExecutionCodecError::InvalidField("trade.id"));
    }
    Ok(value)
}

fn parse_json(body: &str) -> Result<Value, ExecutionCodecError> {
    serde_json::from_str(body).map_err(|error| ExecutionCodecError::InvalidJson(error.to_string()))
}

fn required_string<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a str, ExecutionCodecError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or(ExecutionCodecError::InvalidField(field))
}

fn required_scalar_text(value: &Value, field: &'static str) -> Result<String, ExecutionCodecError> {
    value
        .get(field)
        .and_then(scalar_text)
        .filter(|text| !text.trim().is_empty())
        .ok_or(ExecutionCodecError::InvalidField(field))
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn required_decimal(value: &Value, field: &'static str) -> Result<Decimal, ExecutionCodecError> {
    required_scalar_text(value, field)?
        .parse::<Decimal>()
        .map_err(|_| ExecutionCodecError::InvalidField(field))
}

fn optional_decimal(
    value: &Value,
    field: &'static str,
) -> Result<Option<Decimal>, ExecutionCodecError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_decimal(value, field).map(Some),
    }
}

fn required_u64(value: &Value, field: &'static str) -> Result<u64, ExecutionCodecError> {
    required_scalar_text(value, field)?
        .parse::<u64>()
        .map_err(|_| ExecutionCodecError::InvalidField(field))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, ExecutionCodecError> {
    optional_bool(value, field)?.ok_or(ExecutionCodecError::InvalidField(field))
}

fn optional_bool(value: &Value, field: &'static str) -> Result<Option<bool>, ExecutionCodecError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("false") => Ok(Some(false)),
        _ => Err(ExecutionCodecError::InvalidField(field)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(status: &str, executed: &str, quote: &str) -> OrderExecutionHeader {
        parse_order_execution_header(
            &format!(
                r#"{{"symbol":"MUUSDT","clientOrderId":"g_0_S_fixed","orderId":42,"side":"SELL","type":"LIMIT","origQty":"3.14","executedQty":"{executed}","cumQuote":"{quote}","reduceOnly":false,"price":"15.95","timeInForce":"GTC","status":"{status}","time":1000,"updateTime":1100}}"#
            ),
            Exchange::Binance,
            "MUUSDT",
            &ClientOrderId::parse("g_0_S_fixed").unwrap(),
            "42",
        )
        .unwrap()
    }

    #[test]
    fn partial_execution_requires_exact_trade_and_fee_asset_totals() {
        let trades = parse_trade_page(
            r#"[
                {"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","buyer":false,"price":"15.95","qty":"2","quoteQty":"31.90","commission":"0.00638","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1050},
                {"symbol":"MUUSDT","id":8,"orderId":42,"side":"SELL","buyer":false,"price":"15.95","qty":"1.14","quoteQty":"18.183","commission":"0.0036366","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1060}
            ]"#,
            "MUUSDT",
            CommissionConvention::PositiveCost,
        )
        .unwrap();
        let snapshot =
            assemble_execution_snapshot(header("FILLED", "3.14", "50.083"), trades).unwrap();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(314, 2));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::new(100166, 7));
        assert_eq!(snapshot.trades.len(), 2);
        assert_eq!(snapshot.trades[0].trade_id, "7");
        assert_eq!(snapshot.trades[1].trade_id, "8");
        assert_eq!(numeric_trade_id(&snapshot.trades[0]), Ok(7));
    }

    #[test]
    fn same_millisecond_numeric_trade_ids_keep_exchange_sequence() {
        let trades = parse_trade_page(
            r#"[
                {"symbol":"MUUSDT","id":10,"orderId":42,"side":"SELL","price":"15.95","qty":"1.14","quoteQty":"18.183","commission":"0.0036366","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1050},
                {"symbol":"MUUSDT","id":9,"orderId":42,"side":"SELL","price":"15.95","qty":"2","quoteQty":"31.90","commission":"0.00638","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1050}
            ]"#,
            "MUUSDT",
            CommissionConvention::PositiveCost,
        )
        .unwrap();

        let snapshot =
            assemble_execution_snapshot(header("FILLED", "3.14", "50.083"), trades).unwrap();

        assert_eq!(
            snapshot
                .trades
                .iter()
                .map(|trade| trade.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["9", "10"]
        );
    }

    #[test]
    fn non_quote_fees_remain_separate_assets() {
        let trades = parse_trade_page(
            r#"[
                {"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","price":"15.95","qty":"2","quoteQty":"31.90","commission":"0.0001","commissionAsset":"BNB","realizedPnl":"0","maker":true,"time":1050},
                {"symbol":"MUUSDT","id":8,"orderId":42,"side":"SELL","price":"15.95","qty":"1.14","quoteQty":"18.183","commission":"0.0036366","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1060}
            ]"#,
            "MUUSDT",
            CommissionConvention::PositiveCost,
        )
        .unwrap();
        let snapshot =
            assemble_execution_snapshot(header("FILLED", "3.14", "50.083"), trades).unwrap();

        assert_eq!(snapshot.fees_by_asset.len(), 2);
        assert_eq!(snapshot.fees_by_asset["BNB"], Decimal::new(1, 4));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::new(36366, 7));
    }

    #[test]
    fn duplicate_or_incomplete_trades_never_become_a_snapshot() {
        let duplicate = parse_trade_page(
            r#"[
                {"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","price":"15.95","qty":"1","quoteQty":"15.95","commission":"0","commissionAsset":"USDT","maker":true,"time":1050},
                {"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","price":"15.95","qty":"2.14","quoteQty":"34.133","commission":"0","commissionAsset":"USDT","maker":true,"time":1060}
            ]"#,
            "MUUSDT",
            CommissionConvention::PositiveCost,
        );
        assert_eq!(duplicate, Err(ExecutionCodecError::DuplicateTradeId));

        let incomplete = parse_trade_page(
            r#"[{"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","price":"15.95","qty":"3","quoteQty":"47.85","commission":"0","commissionAsset":"USDT","maker":true,"time":1050}]"#,
            "MUUSDT",
            CommissionConvention::PositiveCost,
        )
        .unwrap();
        assert_eq!(
            assemble_execution_snapshot(header("FILLED", "3.14", "50.083"), incomplete),
            Err(ExecutionCodecError::TotalsMismatch)
        );
    }

    #[test]
    fn aster_preserves_signed_commission_but_normalizes_fee_cost() {
        let trades = parse_trade_page(
            r#"[{"symbol":"MUUSDT","id":7,"orderId":42,"side":"SELL","price":"15.95","qty":"3.14","quoteQty":"50.083","commission":"-0.0100166","commissionAsset":"USDT","maker":true,"time":1050}]"#,
            "MUUSDT",
            CommissionConvention::SignedBalanceDeltaOrPositiveCost,
        )
        .unwrap();
        assert_eq!(trades[0].raw_commission, Decimal::new(-100166, 7));
        assert_eq!(trades[0].commission_cost, Decimal::new(100166, 7));
    }

    #[test]
    fn invalid_lifecycle_totals_are_rejected() {
        let invalid = parse_order_execution_header(
            r#"{"symbol":"MUUSDT","clientOrderId":"g_0_S_fixed","orderId":42,"side":"SELL","type":"LIMIT","origQty":"3.14","executedQty":"3","cumQuote":"47.85","reduceOnly":false,"price":"15.95","timeInForce":"GTC","status":"FILLED","time":1000,"updateTime":1100}"#,
            Exchange::Binance,
            "MUUSDT",
            &ClientOrderId::parse("g_0_S_fixed").unwrap(),
            "42",
        );
        assert_eq!(invalid, Err(ExecutionCodecError::InvalidField("status")));
    }

    #[test]
    fn historical_price_requires_one_exact_minute_and_positive_open() {
        let price = parse_historical_minute_open(
            r#"[[1020000,"602.25","603","601","602","100"]]"#,
            Exchange::Binance,
            "BNBUSDT",
            1_020_000,
        )
        .unwrap();
        assert_eq!(price.open_price, Decimal::new(60225, 2));

        assert!(
            parse_historical_minute_open(
                r#"[[960000,"602.25"]]"#,
                Exchange::Binance,
                "BNBUSDT",
                1_020_000,
            )
            .is_err()
        );
        assert!(
            parse_historical_minute_open(
                r#"[[1020000,"0"]]"#,
                Exchange::Binance,
                "BNBUSDT",
                1_020_000,
            )
            .is_err()
        );
    }
}
