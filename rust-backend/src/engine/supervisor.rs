use std::{collections::BTreeMap, sync::Arc};

use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::domain::Exchange;
use crate::exchange::{
    ExchangeIdentityGateway, ExecutionSnapshotGateway, HistoricalPriceGateway,
    InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway, OrderCancellationGateway,
    OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway, TradingFeeRateGateway,
};
use crate::persistence::{StrategyDiscoveryAnomaly, StrategyDiscoveryReport, StrategyFilePaths};

use super::{
    FileStrategyRecoveryError, PreparedLeasedFileStrategy, PreparedStrategyKind,
    PreparedStrategyStep, PreparedStrategyStepError, RuntimeSettings, StrategyRunId,
    claim_leased_file_strategy,
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

pub trait RuntimeRecoveryProvider {
    type Gateway: ExchangeIdentityGateway;
    type Error;

    fn runtime_for(
        &self,
        exchange: Exchange,
        run_id: &StrategyRunId,
    ) -> Result<(Self::Gateway, RuntimeSettings), Self::Error>;
}

#[derive(Debug)]
pub struct RuntimeStartupReport<E> {
    pub registered: Vec<StrategyRunId>,
    pub discovery_anomalies: Vec<StrategyDiscoveryAnomaly>,
    pub failures: Vec<RuntimeStartupFailure<E>>,
}

#[derive(Debug)]
pub enum RuntimeStartupFailure<E> {
    Claim {
        paths: StrategyFilePaths,
        error: FileStrategyRecoveryError,
    },
    Provider {
        run_id: StrategyRunId,
        exchange: Exchange,
        error: E,
    },
    Attach {
        run_id: StrategyRunId,
        error: FileStrategyRecoveryError,
    },
    Duplicate {
        run_id: StrategyRunId,
    },
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

pub async fn recover_discovered_strategies<P>(
    registry: &RuntimeRegistry<P::Gateway>,
    discovery: StrategyDiscoveryReport,
    provider: &P,
) -> RuntimeStartupReport<P::Error>
where
    P: RuntimeRecoveryProvider,
{
    let mut report = RuntimeStartupReport {
        registered: Vec::new(),
        discovery_anomalies: discovery.anomalies,
        failures: Vec::new(),
    };
    for paths in discovery.strategies {
        let claim = match claim_leased_file_strategy(paths.clone()) {
            Ok(claim) => claim,
            Err(error) => {
                report
                    .failures
                    .push(RuntimeStartupFailure::Claim { paths, error });
                continue;
            }
        };
        let run_id = claim.run_id().clone();
        let exchange = claim.exchange();
        let (gateway, settings) = match provider.runtime_for(exchange, &run_id) {
            Ok(runtime) => runtime,
            Err(error) => {
                report.failures.push(RuntimeStartupFailure::Provider {
                    run_id,
                    exchange,
                    error,
                });
                continue;
            }
        };
        let strategy = match claim.attach_gateway(gateway, settings) {
            Ok(strategy) => strategy,
            Err(error) => {
                report
                    .failures
                    .push(RuntimeStartupFailure::Attach { run_id, error });
                continue;
            }
        };
        match registry.register(strategy).await {
            RuntimeRegistration::Registered => report.registered.push(run_id),
            RuntimeRegistration::Duplicate(rejected) => {
                drop(rejected);
                report
                    .failures
                    .push(RuntimeStartupFailure::Duplicate { run_id });
            }
        }
    }
    report.registered.sort();
    report
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
