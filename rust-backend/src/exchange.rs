use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{ClientOrderId, Exchange, OrderIntent, OrderShape, TerminalOrderStatus};

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
