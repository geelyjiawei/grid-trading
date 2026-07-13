use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    ClientOrderId, Exchange, InstrumentRules, OrderIntent, OrderShape, TerminalOrderStatus,
};

pub mod aster;
pub mod binance;
mod codec;
pub mod protocol;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementAcknowledgement {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlacementError {
    #[error("exchange definitively rejected the order: {message}")]
    Definitive {
        code: Option<String>,
        message: String,
    },
    #[error("exchange write outcome is unknown: {message}")]
    Unknown { message: String },
}

#[async_trait]
pub trait OrderPlacementGateway: Send + Sync {
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationAcknowledgement {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CancellationError {
    #[error("order cancellation request is invalid: {message}")]
    Invalid { message: String },
    #[error("order cancellation outcome is unknown: {message}")]
    Unknown { message: String },
}

#[async_trait]
pub trait OrderCancellationGateway: Send + Sync {
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActiveOrderStatus {
    New,
    PartiallyFilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderLifecycle {
    Active(ActiveOrderStatus),
    Terminal(TerminalOrderStatus),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeOrder {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
    pub exchange: Exchange,
    pub shape: OrderShape,
    pub lifecycle: OrderLifecycle,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderLookup {
    Found(AuthoritativeOrder),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("exchange order lookup is inconclusive: {message}")]
pub struct LookupError {
    pub message: String,
}

#[async_trait]
pub trait OrderLookupGateway: Send + Sync {
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeMarketSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub last_price: Decimal,
    pub mark_price: Decimal,
    pub observed_at_ms: u64,
}

impl ExchangeMarketSnapshot {
    pub fn ensure_fresh(
        &self,
        now_ms: u64,
        maximum_age_ms: u64,
        maximum_future_skew_ms: u64,
    ) -> Result<(), SnapshotError> {
        if maximum_age_ms == 0 {
            return Err(SnapshotError::new(
                "market snapshot maximum age must be positive",
            ));
        }
        if self.observed_at_ms
            > now_ms
                .checked_add(maximum_future_skew_ms)
                .ok_or_else(|| SnapshotError::new("market snapshot clock range overflowed"))?
        {
            return Err(SnapshotError::new(
                "market snapshot timestamp is too far in the future",
            ));
        }
        if now_ms.saturating_sub(self.observed_at_ms) > maximum_age_ms {
            return Err(SnapshotError::new("market snapshot is stale"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionLeg {
    pub side: PositionSide,
    pub signed_quantity: Decimal,
    pub entry_price: Option<Decimal>,
    pub mark_price: Decimal,
    pub unrealized_profit: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub legs: Vec<PositionLeg>,
}

impl PositionSnapshot {
    pub fn one_way_position(&self) -> Result<(Decimal, Option<Decimal>), SnapshotError> {
        if self.legs.len() != 1 || self.legs[0].side != PositionSide::Both {
            return Err(SnapshotError::new(
                "hedge-mode position cannot be represented by the one-way strategy ledger",
            ));
        }
        Ok((self.legs[0].signed_quantity, self.legs[0].entry_price))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("exchange snapshot is inconclusive: {message}")]
pub struct SnapshotError {
    pub message: String,
}

impl SnapshotError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait MarketSnapshotGateway: Send + Sync {
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError>;
}

#[async_trait]
pub trait InstrumentRulesGateway: Send + Sync {
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<InstrumentRules, SnapshotError>;
}

#[async_trait]
pub trait PositionSnapshotGateway: Send + Sync {
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError>;
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    fn snapshot(observed_at_ms: u64) -> ExchangeMarketSnapshot {
        ExchangeMarketSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            last_price: Decimal::new(1011, 0),
            mark_price: Decimal::new(1010, 0),
            observed_at_ms,
        }
    }

    #[test]
    fn market_freshness_rejects_stale_and_future_snapshots() {
        assert!(snapshot(10_000).ensure_fresh(10_500, 1_000, 100).is_ok());
        assert!(snapshot(9_000).ensure_fresh(10_500, 1_000, 100).is_err());
        assert!(snapshot(10_601).ensure_fresh(10_500, 1_000, 100).is_err());
    }
}
