use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{ClientOrderId, Exchange, TerminalOrderStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CancellationState {
    Prepared,
    SubmitUnknown { message: String },
    Acknowledged,
    Rejected { message: String },
    Resolved { status: TerminalOrderStatus },
}

impl CancellationState {
    pub fn validate(&self) -> Result<(), CancellationIntentError> {
        match self {
            Self::SubmitUnknown { message } | Self::Rejected { message }
                if message.trim().is_empty() =>
            {
                Err(CancellationIntentError::InvalidStateMetadata)
            }
            _ => Ok(()),
        }
    }

    pub fn is_resolved(&self) -> bool {
        matches!(self, Self::Resolved { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationIntent {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub state: CancellationState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl CancellationIntent {
    pub fn prepare(
        client_order_id: ClientOrderId,
        exchange_order_id: impl Into<String>,
        exchange: Exchange,
        symbol: impl Into<String>,
        now_ms: u64,
    ) -> Result<Self, CancellationIntentError> {
        let intent = Self {
            client_order_id,
            exchange_order_id: exchange_order_id.into(),
            exchange,
            symbol: symbol.into(),
            state: CancellationState::Prepared,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        intent.validate()?;
        Ok(intent)
    }

    pub fn validate(&self) -> Result<(), CancellationIntentError> {
        ClientOrderId::parse(self.client_order_id.as_str())?;
        if self.exchange_order_id.trim().is_empty() {
            return Err(CancellationIntentError::MissingExchangeOrderId);
        }
        if self.symbol.is_empty()
            || !self
                .symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(CancellationIntentError::InvalidSymbol);
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err(CancellationIntentError::InvalidTimestamp);
        }
        self.state.validate()
    }

    pub fn has_same_target(&self, other: &Self) -> bool {
        self.client_order_id == other.client_order_id
            && self.exchange_order_id == other.exchange_order_id
            && self.exchange == other.exchange
            && self.symbol == other.symbol
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CancellationIntentError {
    #[error(transparent)]
    InvalidClientOrderId(#[from] crate::domain::OrderIntentError),
    #[error("exchange order ID is required")]
    MissingExchangeOrderId,
    #[error("symbol must contain only uppercase ASCII letters and digits")]
    InvalidSymbol,
    #[error("updated timestamp precedes created timestamp")]
    InvalidTimestamp,
    #[error("cancellation state metadata is missing")]
    InvalidStateMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_target_is_immutable_and_validated() {
        let intent = CancellationIntent::prepare(
            ClientOrderId::parse("g_1_S_cancel").unwrap(),
            "exchange-1",
            Exchange::Binance,
            "MUUSDT",
            100,
        )
        .unwrap();
        assert!(intent.has_same_target(&intent));

        let mut changed = intent.clone();
        changed.exchange_order_id = "exchange-2".into();
        assert!(!intent.has_same_target(&changed));
    }

    #[test]
    fn malformed_cancellation_never_reaches_an_exchange_adapter() {
        assert!(matches!(
            CancellationIntent::prepare(
                ClientOrderId::parse("g_1_S_cancel").unwrap(),
                "",
                Exchange::Aster,
                "MUUSDT",
                100,
            ),
            Err(CancellationIntentError::MissingExchangeOrderId)
        ));
    }
}
