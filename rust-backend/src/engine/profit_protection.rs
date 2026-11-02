use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::Exchange;

use super::{
    PreparedStrategyLifecycle, RiskExitReason, StrategyMachineError, StrategyRunId,
    StrategyStateError,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfitProtection {
    pub revision: u64,
    pub take_profit_amount: Option<Decimal>,
    pub stop_loss_amount: Option<Decimal>,
    pub updated_at_ms: Option<u64>,
}

impl ProfitProtection {
    pub(crate) fn validate_limits(&self) -> Result<(), StrategyStateError> {
        if [self.take_profit_amount, self.stop_loss_amount]
            .into_iter()
            .flatten()
            .any(|amount| amount <= Decimal::ZERO)
        {
            return Err(StrategyStateError::InvalidProfitProtection);
        }
        Ok(())
    }

    pub(crate) fn triggered_reason(&self, net_profit: Decimal) -> Option<RiskExitReason> {
        if self
            .stop_loss_amount
            .is_some_and(|amount| net_profit <= -amount)
        {
            Some(RiskExitReason::ProfitStopLoss)
        } else if self
            .take_profit_amount
            .is_some_and(|amount| net_profit >= amount)
        {
            Some(RiskExitReason::ProfitTakeProfit)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfitProtectionObservation {
    pub status: String,
    pub observed_at_ms: Option<u64>,
    pub net_profit: Option<Decimal>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfitProtectionTrigger {
    pub net_profit: Decimal,
    pub mark_price: Decimal,
    pub observed_at_ms: u64,
    pub protection_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfitProtectionStatus {
    pub run_id: StrategyRunId,
    pub exchange: Exchange,
    pub symbol: String,
    pub lifecycle: PreparedStrategyLifecycle,
    pub profit_protection: ProfitProtection,
    pub observation: ProfitProtectionObservation,
    pub trigger: Option<ProfitProtectionTrigger>,
}

#[derive(Debug, Error)]
pub enum ProfitProtectionControlError {
    #[error("strategy was not found")]
    NotFound,
    #[error("strategy run identity changed")]
    RunMismatch,
    #[error("strategy does not allow profit protection updates")]
    NotRunning,
    #[error("profit protection revision changed")]
    RevisionConflict,
    #[error("profit protection limits must be positive amounts")]
    InvalidLimits,
    #[error(transparent)]
    State(#[from] StrategyMachineError),
}
