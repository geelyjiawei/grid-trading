use std::{collections::BTreeMap, sync::Arc};

use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::exchange::{
    ExchangeIdentityGateway, ExecutionSnapshotGateway, HistoricalPriceGateway,
    InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway, OrderCancellationGateway,
    OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway, TradingFeeRateGateway,
};

use super::{
    PreparedLeasedFileStrategy, PreparedStrategyKind, PreparedStrategyStep,
    PreparedStrategyStepError, StrategyRunId,
};

type RuntimeSlot<G> = Arc<Mutex<PreparedLeasedFileStrategy<G>>>;

pub struct RuntimeRegistry<G> {
    entries: RwLock<BTreeMap<StrategyRunId, RuntimeSlot<G>>>,
}

impl<G> Default for RuntimeRegistry<G> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
        }
    }
}

pub enum RuntimeRegistration<G> {
    Registered,
    Duplicate(PreparedLeasedFileStrategy<G>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRegistryEntry {
    pub run_id: StrategyRunId,
    pub kind: Option<PreparedStrategyKind>,
    pub advancing: bool,
}

impl<G> RuntimeRegistry<G> {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        strategy: PreparedLeasedFileStrategy<G>,
    ) -> RuntimeRegistration<G> {
        let run_id = strategy.run_id().clone();
        let mut entries = self.entries.write().await;
        if entries.contains_key(&run_id) {
            return RuntimeRegistration::Duplicate(strategy);
        }
        entries.insert(run_id, Arc::new(Mutex::new(strategy)));
        RuntimeRegistration::Registered
    }

    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.entries.read().await.is_empty()
    }

    pub async fn contains(&self, run_id: &StrategyRunId) -> bool {
        self.entries.read().await.contains_key(run_id)
    }

    pub async fn entries(&self) -> Vec<RuntimeRegistryEntry> {
        let entries = self
            .entries
            .read()
            .await
            .iter()
            .map(|(run_id, slot)| (run_id.clone(), Arc::clone(slot)))
            .collect::<Vec<_>>();
        entries
            .into_iter()
            .map(|(run_id, slot)| match slot.try_lock() {
                Ok(strategy) => RuntimeRegistryEntry {
                    run_id,
                    kind: Some(strategy.kind()),
                    advancing: false,
                },
                Err(_) => RuntimeRegistryEntry {
                    run_id,
                    kind: None,
                    advancing: true,
                },
            })
            .collect()
    }

    pub async fn advance(
        &self,
        run_id: &StrategyRunId,
        now_ms: u64,
    ) -> Result<PreparedStrategyStep, RuntimeRegistryAdvanceError>
    where
        G: ExchangeIdentityGateway
            + TradingFeeRateGateway
            + LeverageGateway
            + PositionSnapshotGateway
            + MarketSnapshotGateway
            + InstrumentRulesGateway
            + OrderPlacementGateway
            + OrderCancellationGateway
            + OrderLookupGateway
            + ExecutionSnapshotGateway
            + HistoricalPriceGateway,
    {
        let slot = self
            .entries
            .read()
            .await
            .get(run_id)
            .cloned()
            .ok_or_else(|| RuntimeRegistryAdvanceError::NotFound(run_id.clone()))?;
        let mut strategy = slot
            .try_lock()
            .map_err(|_| RuntimeRegistryAdvanceError::AlreadyAdvancing(run_id.clone()))?;
        strategy.advance(now_ms).await.map_err(Into::into)
    }
}

#[derive(Debug, Error)]
pub enum RuntimeRegistryAdvanceError {
    #[error("strategy {0:?} is not registered")]
    NotFound(StrategyRunId),
    #[error("strategy {0:?} already has a tick in progress")]
    AlreadyAdvancing(StrategyRunId),
    #[error(transparent)]
    Strategy(#[from] PreparedStrategyStepError),
}
