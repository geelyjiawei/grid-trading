use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::{
    domain::{
        ClientOrderId, Exchange, OrderKind, OrderShape, OrderSide, TerminalOrderStatus, TimeInForce,
    },
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, CancellationAcknowledgement, OrderLifecycle,
        PlacementAcknowledgement, protocol::Parameters,
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

pub(super) fn execution_status_is_unknown(code: Option<&str>) -> bool {
    matches!(code, Some("-1006" | "-1007"))
}

pub(super) fn order_is_definitively_absent(code: Option<&str>) -> bool {
    matches!(code, Some("-2013"))
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
    let value: Value =
        serde_json::from_str(body).map_err(|error| CodecError::InvalidJson(error.to_string()))?;
    let symbol = required_string(&value, "symbol")?.to_ascii_uppercase();
    if symbol != expected_symbol.to_ascii_uppercase() {
        return Err(CodecError::SymbolMismatch);
    }

    let returned_client_id = required_scalar_text(&value, "clientOrderId")?;
    if returned_client_id != expected_client_order_id.as_str() {
        return Err(CodecError::ClientOrderIdMismatch);
    }
    let client_order_id = ClientOrderId::parse(returned_client_id)
        .map_err(|_| CodecError::InvalidField("clientOrderId"))?;
    let exchange_order_id = required_scalar_text(&value, "orderId")?;
    let side = match required_string(&value, "side")?
        .to_ascii_uppercase()
        .as_str()
    {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(CodecError::InvalidField("side")),
    };
    let kind = match required_string(&value, "type")?
        .to_ascii_uppercase()
        .as_str()
    {
        "LIMIT" => OrderKind::Limit,
        "MARKET" => OrderKind::Market,
        _ => return Err(CodecError::InvalidField("type")),
    };
    let quantity = required_decimal(&value, "origQty")?;
    let reduce_only = required_bool(&value, "reduceOnly")?;
    let (price, time_in_force) = match kind {
        OrderKind::Limit => {
            let price = required_decimal(&value, "price")?;
            let time_in_force = match required_string(&value, "timeInForce")?
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
        lifecycle: parse_lifecycle(required_string(&value, "status")?)?,
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
}
