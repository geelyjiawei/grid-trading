use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::Exchange;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    pub fn parse(value: impl Into<String>) -> Result<Self, OrderIntentError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 36
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(OrderIntentError::InvalidClientOrderId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKind {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    PostOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminalOrderStatus {
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderShape {
    pub symbol: String,
    pub side: OrderSide,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub reduce_only: bool,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
}

impl OrderShape {
    pub fn validate(&self) -> Result<(), OrderIntentError> {
        if self.symbol.is_empty()
            || !self
                .symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(OrderIntentError::InvalidSymbol);
        }
        if self.quantity <= Decimal::ZERO {
            return Err(OrderIntentError::InvalidQuantity);
        }
        match (self.kind, self.price) {
            (OrderKind::Limit, Some(price)) if price > Decimal::ZERO => {}
            (OrderKind::Limit, _) => return Err(OrderIntentError::InvalidLimitPrice),
            (OrderKind::Market, None) => {}
            (OrderKind::Market, Some(_)) => return Err(OrderIntentError::MarketHasPrice),
        }
        if self.kind == OrderKind::Market && self.time_in_force == TimeInForce::PostOnly {
            return Err(OrderIntentError::MarketPostOnly);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntentState {
    Prepared,
    RetryableNotSubmitted {
        message: String,
    },
    SubmitUnknown {
        message: String,
    },
    Accepted {
        exchange_order_id: String,
    },
    Rejected {
        code: Option<String>,
        message: String,
    },
    OwnershipConflict {
        message: String,
    },
    Terminal {
        status: TerminalOrderStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exchange_order_id: Option<String>,
    },
}

impl IntentState {
    pub fn validate(&self) -> Result<(), OrderIntentError> {
        let valid = match self {
            Self::Prepared
            | Self::Terminal {
                exchange_order_id: None,
                ..
            } => true,
            Self::Terminal {
                exchange_order_id: Some(exchange_order_id),
                ..
            } => !exchange_order_id.trim().is_empty(),
            Self::RetryableNotSubmitted { message }
            | Self::SubmitUnknown { message }
            | Self::OwnershipConflict { message } => !message.trim().is_empty(),
            Self::Accepted { exchange_order_id } => !exchange_order_id.trim().is_empty(),
            Self::Rejected { message, .. } => !message.trim().is_empty(),
        };
        if valid {
            Ok(())
        } else {
            Err(OrderIntentError::InvalidStateMetadata)
        }
    }

    pub fn exchange_order_id(&self) -> Option<&str> {
        match self {
            Self::Accepted { exchange_order_id } => Some(exchange_order_id),
            Self::Terminal {
                exchange_order_id: Some(exchange_order_id),
                ..
            } => Some(exchange_order_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub client_order_id: ClientOrderId,
    pub exchange: Exchange,
    pub shape: OrderShape,
    pub state: IntentState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl OrderIntent {
    pub fn prepare(
        client_order_id: ClientOrderId,
        exchange: Exchange,
        shape: OrderShape,
        now_ms: u64,
    ) -> Result<Self, OrderIntentError> {
        shape.validate()?;
        Ok(Self {
            client_order_id,
            exchange,
            shape,
            state: IntentState::Prepared,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }

    pub fn validate(&self) -> Result<(), OrderIntentError> {
        ClientOrderId::parse(self.client_order_id.as_str())?;
        self.shape.validate()?;
        self.state.validate()?;
        if self.updated_at_ms < self.created_at_ms {
            return Err(OrderIntentError::InvalidTimestamp);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OrderIntentError {
    #[error("client order ID must be 1-36 ASCII letters, digits, underscores, or hyphens")]
    InvalidClientOrderId,
    #[error("symbol must contain only uppercase ASCII letters and digits")]
    InvalidSymbol,
    #[error("quantity must be positive")]
    InvalidQuantity,
    #[error("limit order price must be positive")]
    InvalidLimitPrice,
    #[error("market order must not contain a price")]
    MarketHasPrice,
    #[error("market order cannot be post-only")]
    MarketPostOnly,
    #[error("updated timestamp precedes created timestamp")]
    InvalidTimestamp,
    #[error("order intent state metadata is missing")]
    InvalidStateMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_identity_is_bounded_and_portable() {
        assert!(ClientOrderId::parse("g_12_S_abcd-1234").is_ok());
        assert!(ClientOrderId::parse("a".repeat(36)).is_ok());
        assert_eq!(
            ClientOrderId::parse("a".repeat(37)),
            Err(OrderIntentError::InvalidClientOrderId)
        );
        assert_eq!(
            ClientOrderId::parse("bad id"),
            Err(OrderIntentError::InvalidClientOrderId)
        );
    }

    #[test]
    fn order_shape_rejects_ambiguous_market_price() {
        let shape = OrderShape {
            symbol: "MUUSDT".into(),
            side: OrderSide::Sell,
            price: Some(Decimal::new(1011, 0)),
            quantity: Decimal::new(2, 1),
            reduce_only: false,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        };
        assert_eq!(shape.validate(), Err(OrderIntentError::MarketHasPrice));
    }

    #[test]
    fn accepted_intent_requires_an_exchange_order_identity() {
        let mut intent = OrderIntent::prepare(
            ClientOrderId::parse("g_1_S_missing").unwrap(),
            Exchange::Binance,
            OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(1011, 0)),
                quantity: Decimal::new(2, 1),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            100,
        )
        .unwrap();
        intent.state = IntentState::Accepted {
            exchange_order_id: " ".into(),
        };

        assert_eq!(
            intent.validate(),
            Err(OrderIntentError::InvalidStateMetadata)
        );
    }
}
