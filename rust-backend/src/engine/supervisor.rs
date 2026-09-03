use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::domain::Exchange;
use crate::exchange::{
    AccountBalanceSnapshotGateway, ExchangeIdentityGateway, ExecutionSnapshotGateway,
    HistoricalPriceGateway, InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway,
    OrderCancellationGateway, OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway,
    TradingFeeRateGateway,
};
use crate::persistence::{StrategyDiscoveryAnomaly, StrategyDiscoveryReport, StrategyFilePaths};

use super::exchange_inputs::advance_reference_time_ms;
use super::runtime::PreparedExecutionEventStep;
use super::{
    FileStrategyRecoveryError, PreparedLeasedFileStrategy, PreparedStrategyKind,
    PreparedStrategyLifecycle, PreparedStrategyStep, PreparedStrategyStepError,
    PreparedStrategyStopError, PreparedStrategyStopOutcome, RuntimeSettings, StrategyRunId,
    claim_leased_file_strategy,
};

struct RuntimeSlot<G> {
    exchange: Exchange,
    symbol: String,
    execution_waiters: AtomicUsize,
    strategy: Mutex<PreparedLeasedFileStrategy<G>>,
}

struct ExecutionWaiter<'a>(&'a AtomicUsize);

impl<'a> ExecutionWaiter<'a> {
    fn register(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self(waiters)
    }
}

impl Drop for ExecutionWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

type RuntimeSlotHandle<G> = Arc<RuntimeSlot<G>>;

pub struct RuntimeRegistry<G> {
    entries: RwLock<BTreeMap<StrategyRunId, RuntimeSlotHandle<G>>>,
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
    Terminal(PreparedLeasedFileStrategy<G>),
    Duplicate(PreparedLeasedFileStrategy<G>),
    MarketAlreadyOwned {
        owner_run_id: StrategyRunId,
        rejected: PreparedLeasedFileStrategy<G>,
    },
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
    pub skipped_terminal: Vec<StrategyRunId>,
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
    MarketAlreadyOwned {
        run_id: StrategyRunId,
        exchange: Exchange,
        symbol: String,
        owner_run_id: StrategyRunId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRegistryEntry {
    pub run_id: StrategyRunId,
    pub exchange: Exchange,
    pub symbol: String,
    pub kind: Option<PreparedStrategyKind>,
    pub lifecycle: Option<PreparedStrategyLifecycle>,
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
        let exchange = strategy.exchange();
        let symbol = strategy.symbol().to_owned();
        if strategy.is_terminal() {
            return RuntimeRegistration::Terminal(strategy);
        }
        let mut entries = self.entries.write().await;
        prune_terminal_entries(&mut entries);
        if entries.contains_key(&run_id) {
            return RuntimeRegistration::Duplicate(strategy);
        }
        if let Some((owner_run_id, _)) = entries
            .iter()
            .find(|(_, slot)| slot.exchange == exchange && slot.symbol == symbol)
        {
            return RuntimeRegistration::MarketAlreadyOwned {
                owner_run_id: owner_run_id.clone(),
                rejected: strategy,
            };
        }
        entries.insert(
            run_id,
            Arc::new(RuntimeSlot {
                exchange,
                symbol,
                execution_waiters: AtomicUsize::new(0),
                strategy: Mutex::new(strategy),
            }),
        );
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

    pub async fn owner_for_market(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Option<StrategyRunId> {
        self.entries
            .read()
            .await
            .iter()
            .find(|(_, slot)| slot.exchange == exchange && slot.symbol == symbol)
            .map(|(run_id, _)| run_id.clone())
    }

    pub async fn prune_terminal(&self) -> Vec<StrategyRunId> {
        let mut entries = self.entries.write().await;
        let removed = entries
            .iter()
            .filter_map(|(run_id, slot)| {
                slot.strategy
                    .try_lock()
                    .ok()
                    .filter(|strategy| strategy.is_terminal())
                    .map(|_| run_id.clone())
            })
            .collect::<Vec<_>>();
        for run_id in &removed {
            entries.remove(run_id);
        }
        removed
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
            .map(|(run_id, slot)| {
                if slot.execution_waiters.load(Ordering::Acquire) > 0 {
                    return RuntimeRegistryEntry {
                        run_id,
                        exchange: slot.exchange,
                        symbol: slot.symbol.clone(),
                        kind: None,
                        lifecycle: None,
                        advancing: true,
                    };
                }
                match slot.strategy.try_lock() {
                    Ok(strategy) => RuntimeRegistryEntry {
                        run_id,
                        exchange: slot.exchange,
                        symbol: slot.symbol.clone(),
                        kind: Some(strategy.kind()),
                        lifecycle: Some(strategy.lifecycle()),
                        advancing: false,
                    },
                    Err(_) => RuntimeRegistryEntry {
                        run_id,
                        exchange: slot.exchange,
                        symbol: slot.symbol.clone(),
                        kind: None,
                        lifecycle: None,
                        advancing: true,
                    },
                }
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
            + AccountBalanceSnapshotGateway
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
        if slot.execution_waiters.load(Ordering::Acquire) > 0 {
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
        let mut strategy = slot
            .strategy
            .try_lock()
            .map_err(|_| RuntimeRegistryAdvanceError::AlreadyAdvancing(run_id.clone()))?;
        if slot.execution_waiters.load(Ordering::Acquire) > 0 {
            drop(strategy);
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
        strategy.advance(now_ms).await.map_err(Into::into)
    }

    pub async fn advance_execution_events(
        &self,
        exchange: Exchange,
        symbol: &str,
        exchange_order_ids: Option<&[String]>,
        now_ms: u64,
    ) -> Option<(
        StrategyRunId,
        Result<PreparedStrategyStep, RuntimeRegistryAdvanceError>,
    )>
    where
        G: Clone
            + ExchangeIdentityGateway
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
        let (run_id, slot) = self
            .entries
            .read()
            .await
            .iter()
            .find(|(_, slot)| slot.exchange == exchange && slot.symbol == symbol)
            .map(|(run_id, slot)| (run_id.clone(), Arc::clone(slot)))?;
        let lock_started = Instant::now();
        let waiter = ExecutionWaiter::register(&slot.execution_waiters);
        let mut strategy = slot.strategy.lock().await;
        let strategy_lock_wait_ms =
            u64::try_from(lock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::info!(
            run_id = run_id.as_str(),
            exchange = ?exchange,
            symbol,
            strategy_lock_wait_ms,
            "execution event acquired strategy lock"
        );
        let effective_now_ms = now_ms.max(strategy.updated_at_ms());
        if exchange == Exchange::TradeXyz
            && let Some(exchange_order_ids) = exchange_order_ids
        {
            let prepared = match strategy
                .prepare_targeted_execution_events(effective_now_ms, exchange_order_ids)
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => return Some((run_id, Err(error.into()))),
            };
            let prepared = match prepared {
                PreparedExecutionEventStep::Complete(step) => {
                    return Some((run_id, Ok(step)));
                }
                PreparedExecutionEventStep::Submit(prepared) => prepared,
            };
            drop(strategy);
            let placement_results = prepared.place().await;
            let completion_lock_started = Instant::now();
            let mut strategy = slot.strategy.lock().await;
            let completion_lock_wait_ms =
                u64::try_from(completion_lock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            tracing::info!(
                run_id = run_id.as_str(),
                exchange = ?exchange,
                symbol,
                completion_lock_wait_ms,
                "execution placement acquired strategy lock for durable acknowledgement"
            );
            let completion_now_ms = advance_reference_time_ms(now_ms, lock_started.elapsed())
                .max(strategy.updated_at_ms());
            let result = strategy
                .complete_targeted_submission(prepared, placement_results, completion_now_ms)
                .map_err(Into::into);
            return Some((run_id, result));
        }
        drop(waiter);
        let result = strategy
            .advance_execution_events(effective_now_ms, exchange_order_ids)
            .await
            .map_err(Into::into);
        Some((run_id, result))
    }

    pub async fn request_stop(
        &self,
        run_id: &StrategyRunId,
        now_ms: u64,
    ) -> Result<PreparedStrategyStopOutcome, RuntimeRegistryStopError> {
        let slot = self
            .entries
            .read()
            .await
            .get(run_id)
            .cloned()
            .ok_or_else(|| RuntimeRegistryStopError::NotFound(run_id.clone()))?;
        // A stop request is safety-critical: wait for the current atomic tick instead of
        // returning a transient busy error that would force the caller to guess or retry.
        let mut strategy = slot.strategy.lock().await;
        let effective_now_ms = now_ms.max(strategy.updated_at_ms());
        strategy.request_stop(effective_now_ms).map_err(Into::into)
    }
}

fn prune_terminal_entries<G>(entries: &mut BTreeMap<StrategyRunId, RuntimeSlotHandle<G>>) {
    let removable = entries
        .iter()
        .filter_map(|(run_id, slot)| {
            slot.strategy
                .try_lock()
                .ok()
                .filter(|strategy| strategy.is_terminal())
                .map(|_| run_id.clone())
        })
        .collect::<Vec<_>>();
    for run_id in removable {
        entries.remove(&run_id);
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
        skipped_terminal: Vec::new(),
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
        if claim.is_terminal() {
            drop(claim);
            report.skipped_terminal.push(run_id);
            continue;
        }
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
            RuntimeRegistration::Terminal(rejected) => {
                drop(rejected);
                report.skipped_terminal.push(run_id);
            }
            RuntimeRegistration::Duplicate(rejected) => {
                drop(rejected);
                report
                    .failures
                    .push(RuntimeStartupFailure::Duplicate { run_id });
            }
            RuntimeRegistration::MarketAlreadyOwned {
                owner_run_id,
                rejected,
            } => {
                let exchange = rejected.exchange();
                let symbol = rejected.symbol().to_owned();
                drop(rejected);
                report
                    .failures
                    .push(RuntimeStartupFailure::MarketAlreadyOwned {
                        run_id,
                        exchange,
                        symbol,
                        owner_run_id,
                    });
            }
        }
    }
    report.registered.sort();
    report.skipped_terminal.sort();
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

#[derive(Debug, Error)]
pub enum RuntimeRegistryStopError {
    #[error("strategy {0:?} is not registered")]
    NotFound(StrategyRunId),
    #[error(transparent)]
    Strategy(#[from] PreparedStrategyStopError),
}
