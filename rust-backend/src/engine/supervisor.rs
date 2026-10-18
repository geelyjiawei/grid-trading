use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, stream};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::domain::{ClientOrderId, Exchange};
use crate::exchange::{
    AccountBalanceSnapshotGateway, ExchangeIdentityGateway, ExecutionSnapshotGateway,
    HistoricalPriceGateway, InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway,
    OrderCancellationGateway, OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway,
    TradingFeeRateGateway,
};
use crate::persistence::{StrategyDiscoveryAnomaly, StrategyDiscoveryReport, StrategyFilePaths};

use super::exchange_inputs::advance_reference_time_ms;
use super::periodic_priority::{PeriodicPriority, PriorityWaiter as ExecutionWaiter};
use super::runtime::PreparedExecutionEventStep;
use super::{
    FileStrategyRecoveryError, PendingFeeValuation, PreparedLeasedFileStrategy,
    PreparedStrategyKind, PreparedStrategyLifecycle, PreparedStrategyStep,
    PreparedStrategyStepError, PreparedStrategyStopError, PreparedStrategyStopOutcome,
    RuntimeSettings, RuntimeTickReport, StrategyRunId, claim_leased_file_strategy,
};

const FEE_SETTLEMENT_BATCH_SIZE: usize = 8;
const FEE_SETTLEMENT_CONCURRENCY: usize = 2;
const FEE_SETTLEMENT_RETRY_DELAY: Duration = Duration::from_secs(5);
const FEE_SETTLEMENT_PRICE_TIMEOUT: Duration = Duration::from_secs(10);

struct RuntimeSlot<G> {
    exchange: Exchange,
    symbol: String,
    owns_market: AtomicBool,
    execution_waiters: PeriodicPriority,
    fee_settlement: std::sync::Mutex<FeeSettlementSchedule>,
    strategy: Mutex<PreparedLeasedFileStrategy<G>>,
}

#[derive(Default)]
struct FeeSettlementSchedule {
    in_flight: bool,
    not_before: Option<Instant>,
    after: Option<ClientOrderId>,
}

struct FeeSettlementLease<G>(RuntimeSlotHandle<G>);

impl<G> Drop for FeeSettlementLease<G> {
    fn drop(&mut self) {
        let mut schedule = self
            .0
            .fee_settlement
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        schedule.in_flight = false;
        schedule.not_before = Some(Instant::now() + FEE_SETTLEMENT_RETRY_DELAY);
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
        let owns_market = !strategy.is_terminal();
        if !owns_market && !strategy.has_pending_fee_valuations() {
            return RuntimeRegistration::Terminal(strategy);
        }
        let mut entries = self.entries.write().await;
        prune_terminal_entries(&mut entries);
        if entries.contains_key(&run_id) {
            return RuntimeRegistration::Duplicate(strategy);
        }
        if owns_market
            && let Some((owner_run_id, _)) = entries.iter().find(|(_, slot)| {
                slot.owns_market.load(Ordering::Acquire)
                    && slot.exchange == exchange
                    && slot.symbol == symbol
            })
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
                owns_market: AtomicBool::new(owns_market),
                execution_waiters: PeriodicPriority::default(),
                fee_settlement: std::sync::Mutex::new(FeeSettlementSchedule::default()),
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
            .find(|(_, slot)| {
                slot.owns_market.load(Ordering::Acquire)
                    && slot.exchange == exchange
                    && slot.symbol == symbol
            })
            .map(|(run_id, _)| run_id.clone())
    }

    pub async fn prune_terminal(&self) -> Vec<StrategyRunId> {
        let mut entries = self.entries.write().await;
        prune_terminal_entries(&mut entries)
    }

    pub async fn entries(&self) -> Vec<RuntimeRegistryEntry> {
        self.registry_entries(false, false).await
    }

    pub(crate) async fn periodic_entries(&self) -> Vec<RuntimeRegistryEntry> {
        self.registry_entries(false, true).await
    }

    pub(crate) async fn market_entries(&self) -> Vec<RuntimeRegistryEntry> {
        self.registry_entries(true, false).await
    }

    async fn registry_entries(
        &self,
        owners_only: bool,
        periodic: bool,
    ) -> Vec<RuntimeRegistryEntry> {
        let entries = self
            .entries
            .read()
            .await
            .iter()
            .filter(|(_, slot)| !owners_only || slot.owns_market.load(Ordering::Acquire))
            .map(|(run_id, slot)| (run_id.clone(), Arc::clone(slot)))
            .collect::<Vec<_>>();
        entries
            .into_iter()
            .map(|(run_id, slot)| {
                // Bounded fairness is Binance-only, not an audit-completion deadline.
                // Other exchanges retain admission reservations, including TradeXyz ACKs.
                let audit_due = periodic
                    && slot.exchange == Exchange::Binance
                    && slot.owns_market.load(Ordering::Acquire)
                    && slot.execution_waiters.audit_due()
                    && !slot.execution_waiters.is_advancing();
                if audit_due {
                    return RuntimeRegistryEntry {
                        run_id,
                        exchange: slot.exchange,
                        symbol: slot.symbol.clone(),
                        kind: None,
                        lifecycle: None,
                        advancing: false,
                    };
                }
                if slot.execution_waiters.has_waiters() {
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

    pub(crate) async fn spawn_fee_settlements(&self, now_ms: u64) -> usize
    where
        G: Clone + HistoricalPriceGateway + Send + Sync + 'static,
    {
        let slots = self
            .entries
            .read()
            .await
            .iter()
            .map(|(run_id, slot)| (run_id.clone(), Arc::clone(slot)))
            .collect::<Vec<_>>();
        let mut spawned = 0;
        for (run_id, slot) in slots {
            if slot.execution_waiters.has_waiters() {
                continue;
            }
            let work = {
                let mut schedule = slot
                    .fee_settlement
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if schedule.in_flight
                    || schedule
                        .not_before
                        .is_some_and(|until| Instant::now() < until)
                {
                    continue;
                }
                let Ok(strategy) = slot.strategy.try_lock() else {
                    continue;
                };
                if slot.execution_waiters.has_waiters() {
                    continue;
                }
                let work = strategy.prepare_pending_fee_work_after(
                    FEE_SETTLEMENT_BATCH_SIZE,
                    schedule.after.as_ref(),
                );
                if work.is_empty() {
                    continue;
                }
                schedule.after = work
                    .last()
                    .map(|pending| pending.snapshot.order.client_order_id.clone());
                schedule.in_flight = true;
                work
            };
            let lease = FeeSettlementLease(Arc::clone(&slot));
            tokio::spawn(async move {
                let _lease = lease;
                settle_fee_batch(slot, run_id, work, now_ms).await;
            });
            spawned += 1;
        }
        spawned
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
        let started = Instant::now();
        let slot = self
            .entries
            .read()
            .await
            .get(run_id)
            .cloned()
            .ok_or_else(|| RuntimeRegistryAdvanceError::NotFound(run_id.clone()))?;
        let priority = slot
            .execution_waiters
            .begin()
            .ok_or_else(|| RuntimeRegistryAdvanceError::AlreadyAdvancing(run_id.clone()))?;
        let bounded_priority = slot.exchange == Exchange::Binance;
        let protected = bounded_priority && priority.is_protected();
        if !protected && slot.execution_waiters.has_waiters() {
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
        let mut strategy = if protected {
            slot.strategy.lock().await
        } else {
            slot.strategy
                .try_lock()
                .map_err(|_| RuntimeRegistryAdvanceError::AlreadyAdvancing(run_id.clone()))?
        };
        if !protected && slot.execution_waiters.has_waiters() {
            drop(strategy);
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
        if strategy.is_terminal() {
            slot.owns_market.store(false, Ordering::Release);
            return Ok(PreparedStrategyStep::Active(RuntimeTickReport {
                ledger_reconciliations: 0,
                execution_syncs: 0,
                submissions: Vec::new(),
                cancellations: Vec::new(),
                blockers: Vec::new(),
            }));
        }
        let effective_now_ms =
            advance_reference_time_ms(now_ms, started.elapsed()).max(strategy.updated_at_ms());
        let result = if bounded_priority {
            strategy
                .advance_with_priority(effective_now_ms, &priority)
                .await
        } else {
            strategy.advance(effective_now_ms).await
        }
        .map_err(Into::into);
        priority.complete(matches!(
            &result,
            Ok(PreparedStrategyStep::Active(report)) if !report.is_blocked()
        ));
        slot.owns_market
            .store(!strategy.is_terminal(), Ordering::Release);
        result
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
            .find(|(_, slot)| {
                slot.owns_market.load(Ordering::Acquire)
                    && slot.exchange == exchange
                    && slot.symbol == symbol
            })
            .map(|(run_id, slot)| (run_id.clone(), Arc::clone(slot)))?;
        let lock_started = Instant::now();
        let waiter = ExecutionWaiter::register(&slot.execution_waiters);
        let mut strategy = slot.strategy.lock().await;
        if strategy.is_terminal() {
            return None;
        }
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
                    slot.owns_market
                        .store(!strategy.is_terminal(), Ordering::Release);
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
            slot.owns_market
                .store(!strategy.is_terminal(), Ordering::Release);
            return Some((run_id, result));
        }
        drop(waiter);
        let result = strategy
            .advance_execution_events(effective_now_ms, exchange_order_ids)
            .await
            .map_err(Into::into);
        slot.owns_market
            .store(!strategy.is_terminal(), Ordering::Release);
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
        let _waiter = ExecutionWaiter::register_stop(&slot.execution_waiters);
        let mut strategy = slot.strategy.lock().await;
        let effective_now_ms = now_ms.max(strategy.updated_at_ms());
        let result = strategy.request_stop(effective_now_ms).map_err(Into::into);
        slot.owns_market
            .store(!strategy.is_terminal(), Ordering::Release);
        result
    }
}

async fn settle_fee_batch<G>(
    slot: RuntimeSlotHandle<G>,
    run_id: StrategyRunId,
    work: Vec<PendingFeeValuation<G>>,
    now_ms: u64,
) where
    G: HistoricalPriceGateway,
{
    let started = Instant::now();
    let mut resolutions = stream::iter(work)
        .map(|pending| async move {
            tokio::time::timeout(FEE_SETTLEMENT_PRICE_TIMEOUT, pending.resolve()).await
        })
        .buffer_unordered(FEE_SETTLEMENT_CONCURRENCY);
    while let Some(result) = resolutions.next().await {
        let resolved = match result {
            Ok(Ok(resolved)) => resolved,
            Ok(Err(error)) => {
                tracing::warn!(run_id = run_id.as_str(), %error, "fee valuation remains pending until a later sweep");
                continue;
            }
            Err(_) => {
                tracing::warn!(
                    run_id = run_id.as_str(),
                    "fee valuation timed out; retry deferred to a later sweep"
                );
                continue;
            }
        };
        // Never queue behind trading work or execution waiters. The current audit
        // remains pending if the short state-only commit cannot run right now.
        if slot.execution_waiters.has_waiters() {
            continue;
        }
        let Ok(mut strategy) = slot.strategy.try_lock() else {
            continue;
        };
        if slot.execution_waiters.has_waiters() {
            continue;
        }
        let effective_now_ms =
            advance_reference_time_ms(now_ms, started.elapsed()).max(strategy.updated_at_ms());
        let client_order_id = resolved.client_order_id.clone();
        if let Err(error) = strategy.complete_fee_valuation(resolved, effective_now_ms) {
            tracing::warn!(run_id = run_id.as_str(), client_order_id = client_order_id.as_str(),
                %error, "fee valuation commit failed; current audit remains authoritative");
        }
    }
}

fn prune_terminal_entries<G>(
    entries: &mut BTreeMap<StrategyRunId, RuntimeSlotHandle<G>>,
) -> Vec<StrategyRunId> {
    let removable = entries
        .iter()
        .filter_map(|(run_id, slot)| {
            slot.strategy
                .try_lock()
                .ok()
                .filter(|strategy| {
                    if !strategy.is_terminal() {
                        return false;
                    }
                    slot.owns_market.store(false, Ordering::Release);
                    !strategy.has_pending_fee_valuations()
                        && !slot
                            .fee_settlement
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .in_flight
                })
                .map(|_| run_id.clone())
        })
        .collect::<Vec<_>>();
    for run_id in &removable {
        entries.remove(run_id);
    }
    removable
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
        if claim.is_terminal() && !claim.has_pending_fee_valuations() {
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

#[cfg(test)]
mod fee_settlement_tests {
    use super::*;
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use std::{sync::atomic::AtomicUsize, time::Duration};
    use tempfile::tempdir;
    use tokio::sync::Semaphore;

    use crate::{
        domain::{
            ClientOrderId, Direction, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            IntentState, OrderIntent, PositionSizingMode, QuantityRules, TerminalOrderStatus,
        },
        engine::{
            ExecutionSyncService, MarketSnapshot, MemoryStrategyStateStore, PositionBaseline,
            RuntimeCoordinator, StrategyLifecycle, StrategyMachine, StrategyOrderTracking,
            StrategyState, StrategyStateStore, build_grid_plan, recover_leased_file_strategy,
        },
        exchange::{
            AuthoritativeOrder, ExecutionSnapshotError, HistoricalMinutePrice,
            OrderExecutionSnapshot, OrderLifecycle, SnapshotError, TradeFill,
        },
        persistence::{FileOrderIntentStore, FileStrategyStateStore, IntentStore},
    };

    #[derive(Clone)]
    struct FeeGateway {
        exchange: Exchange,
        prices: Arc<PriceGate>,
        snapshot: Option<OrderExecutionSnapshot>,
    }

    struct PriceGate {
        permits: Semaphore,
        calls: AtomicUsize,
        active: AtomicUsize,
        maximum_active: AtomicUsize,
        failures: AtomicUsize,
        reject_before_minute: AtomicUsize,
    }

    impl FeeGateway {
        fn new() -> Self {
            Self {
                exchange: Exchange::Binance,
                prices: Arc::new(PriceGate {
                    permits: Semaphore::new(0),
                    calls: AtomicUsize::new(0),
                    active: AtomicUsize::new(0),
                    maximum_active: AtomicUsize::new(0),
                    failures: AtomicUsize::new(0),
                    reject_before_minute: AtomicUsize::new(0),
                }),
                snapshot: None,
            }
        }

        async fn wait_for_calls(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while self.prices.calls.load(Ordering::Acquire) < count {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("fee lookup must start");
        }
    }

    impl ExchangeIdentityGateway for FeeGateway {
        fn exchange(&self) -> Exchange {
            self.exchange
        }
    }

    #[async_trait]
    impl ExecutionSnapshotGateway for FeeGateway {
        async fn execution_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            _client_order_id: &ClientOrderId,
            _exchange_order_id: &str,
        ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
            Ok(self
                .snapshot
                .clone()
                .expect("only fixture setup reads executions"))
        }
    }

    #[async_trait]
    impl HistoricalPriceGateway for FeeGateway {
        async fn historical_minute_open(
            &self,
            exchange: Exchange,
            symbol: &str,
            minute_start_ms: u64,
        ) -> Result<HistoricalMinutePrice, SnapshotError> {
            assert_eq!(exchange, Exchange::Binance);
            assert_eq!(symbol, "BNBUSDT");
            self.prices.calls.fetch_add(1, Ordering::AcqRel);
            let active = self.prices.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.prices
                .maximum_active
                .fetch_max(active, Ordering::AcqRel);
            self.prices.permits.acquire().await.unwrap().forget();
            self.prices.active.fetch_sub(1, Ordering::AcqRel);
            if minute_start_ms < self.prices.reject_before_minute.load(Ordering::Acquire) as u64 {
                return Err(SnapshotError::new("unavailable old fee-price minute"));
            }
            if self
                .prices
                .failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(SnapshotError::new("temporary fee-price outage"));
            }
            Ok(HistoricalMinutePrice {
                exchange,
                symbol: symbol.to_owned(),
                minute_start_ms,
                open_price: Decimal::new(300, 0),
            })
        }
    }

    async fn pending_strategy(
        root: &std::path::Path,
        gateway: &FeeGateway,
        run: &str,
        count: usize,
        stopped: bool,
    ) -> PreparedLeasedFileStrategy<FeeGateway> {
        let config = GridConfig {
            exchange: Some(gateway.exchange()),
            symbol: if gateway.exchange() == Exchange::TradeXyz {
                "MUUSDC".into()
            } else {
                "MUUSDT".into()
            },
            direction: Direction::Neutral,
            upper_price: Decimal::new(1020, 0),
            lower_price: Decimal::new(1000, 0),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(2, 1)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        };
        let quantity = QuantityRules {
            step: Decimal::new(1, 1),
            min: Decimal::new(1, 1),
            max: Some(Decimal::new(100, 0)),
        };
        let rules = InstrumentRules {
            tick_size: Decimal::ONE,
            max_price_significant_digits: None,
            limit_quantity: quantity.clone(),
            market_quantity: quantity,
            min_notional: Decimal::ONE,
        };
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(1014, 0),
                mark_price: Decimal::new(1014, 0),
            },
            &rules,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse(run).unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            1_000,
        )
        .unwrap();
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state));
        let orders = machine
            .store()
            .snapshot()
            .orders
            .values()
            .take(count)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(orders.len(), count);
        for (index, order) in orders.into_iter().enumerate() {
            let id = order.client_order_id;
            let exchange_order_id = format!("fee-order-{index}");
            machine
                .synchronize_intent(
                    &OrderIntent {
                        client_order_id: id.clone(),
                        exchange: Exchange::Binance,
                        shape: order.shape.clone(),
                        state: IntentState::Accepted {
                            exchange_order_id: exchange_order_id.clone(),
                        },
                        created_at_ms: 1_000,
                        updated_at_ms: 1_100,
                    },
                    1_200,
                )
                .unwrap();
            let price = order.shape.price.unwrap();
            let quote = order.shape.quantity * price;
            let trade_time_ms = (index as u64 + 1) * 60_000 + 1_150;
            let snapshot = OrderExecutionSnapshot {
                order: AuthoritativeOrder {
                    client_order_id: id.clone(),
                    exchange_order_id: exchange_order_id.clone(),
                    exchange: Exchange::Binance,
                    shape: order.shape.clone(),
                    lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
                    executed_quantity: Some(order.shape.quantity),
                },
                cumulative_quantity: order.shape.quantity,
                cumulative_quote: quote,
                fees_by_asset: [("BNB".into(), Decimal::new(1, 2))].into_iter().collect(),
                trades: vec![TradeFill {
                    trade_id: format!("fee-trade-{index}"),
                    exchange_order_id,
                    symbol: "MUUSDT".into(),
                    side: order.shape.side,
                    price,
                    quantity: order.shape.quantity,
                    quote_quantity: quote,
                    raw_commission: Decimal::new(1, 2),
                    commission_cost: Decimal::new(1, 2),
                    commission_asset: "BNB".into(),
                    realized_profit: Decimal::ZERO,
                    is_maker: true,
                    trade_time_ms,
                }],
                order_time_ms: trade_time_ms - 50,
                update_time_ms: trade_time_ms,
            };
            let fixture = FeeGateway {
                snapshot: Some(snapshot),
                ..gateway.clone()
            };
            ExecutionSyncService::new("USDT")
                .unwrap()
                .synchronize(&fixture, &mut machine, &id, 1_200)
                .await
                .unwrap();
        }
        if stopped {
            machine.request_stop(1_200).unwrap();
            machine.mark_stopped(1_200).unwrap();
        }
        let state = machine.store().snapshot().clone();
        let paths = StrategyFilePaths::new(root, state.run_id.clone()).unwrap();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        for order in state.orders.values() {
            if let StrategyOrderTracking::Intent {
                state: intent_state,
            } = &order.tracking
            {
                intents
                    .insert_prepared(
                        OrderIntent::prepare(
                            order.client_order_id.clone(),
                            Exchange::Binance,
                            order.shape.clone(),
                            1_000,
                        )
                        .unwrap(),
                    )
                    .unwrap();
                intents
                    .transition(
                        &order.client_order_id,
                        IntentState::Accepted {
                            exchange_order_id: order.exchange_order_id.clone().unwrap(),
                        },
                        1_100,
                    )
                    .unwrap();
                intents
                    .transition(&order.client_order_id, intent_state.clone(), 1_200)
                    .unwrap();
            }
        }
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        recover_leased_file_strategy(
            gateway.clone(),
            paths,
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        )
        .unwrap()
    }

    async fn wait_for_settlement(registry: &RuntimeRegistry<FeeGateway>, run: &StrategyRunId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let slot = registry.entries.read().await.get(run).unwrap().clone();
                if !slot.fee_settlement.lock().unwrap().in_flight {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("fee batch must finish");
    }

    async fn total_fee(registry: &RuntimeRegistry<FeeGateway>, run: &StrategyRunId) -> Decimal {
        let slot = registry.entries.read().await.get(run).unwrap().clone();
        slot.strategy
            .lock()
            .await
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .total_fee
    }

    #[tokio::test]
    async fn queued_stop_announces_priority_and_cancellation_cleans_it_up() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "priority1", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let strategy = slot.strategy.lock().await;
        let before = strategy
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .clone();
        let mut stop = Box::pin(registry.request_stop(&run, 1_300));
        std::future::poll_fn(|cx| {
            assert!(stop.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(slot.execution_waiters.has_waiters());
        drop(stop);
        assert!(!slot.execution_waiters.has_waiters());
        assert_eq!(
            strategy
                .active_runtime()
                .unwrap()
                .runtime()
                .machine()
                .store()
                .snapshot(),
            &before
        );
    }

    #[tokio::test]
    async fn overdue_audit_is_offered_once_even_with_execution_queued() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "priority2", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let _waiter = ExecutionWaiter::register(&slot.execution_waiters);
        for _ in 0..3 {
            let context = slot.execution_waiters.begin().unwrap();
            assert_eq!(context.read(std::future::ready(())).await, None);
        }
        let _strategy = slot.strategy.lock().await;
        assert!(registry.entries().await[0].advancing);
        assert!(!registry.periodic_entries().await[0].advancing);
        let audit = slot.execution_waiters.begin().unwrap();
        assert!(audit.is_protected());
        assert!(registry.periodic_entries().await[0].advancing);
        drop(audit);
        assert!(!registry.periodic_entries().await[0].advancing);
    }

    #[tokio::test]
    async fn trade_xyz_prepared_intent_reservation_blocks_an_overdue_periodic_audit() {
        let directory = tempdir().unwrap();
        let mut gateway = FeeGateway::new();
        gateway.exchange = Exchange::TradeXyz;
        let strategy = pending_strategy(directory.path(), &gateway, "priority3", 0, false).await;
        let run = strategy.run_id().clone();
        let paths = strategy.paths().clone();
        let intent = strategy
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .ready_intents(1_200)
            .unwrap()
            .remove(0);
        drop(strategy);
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        intents.insert_prepared(intent.clone()).unwrap();
        let mut machine =
            StrategyMachine::new(FileStrategyStateStore::load(paths.state()).unwrap());
        machine.synchronize_intent(&intent, 1_200).unwrap();
        drop(machine);
        drop(intents);
        let strategy = recover_leased_file_strategy(
            gateway,
            paths,
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        )
        .unwrap();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        // The real TradeXyz path keeps this reservation while placement/ACK is unlocked.
        let acknowledgement = ExecutionWaiter::register(&slot.execution_waiters);
        for _ in 0..3 {
            let context = slot.execution_waiters.begin().unwrap();
            assert_eq!(context.read(std::future::ready(())).await, None);
        }
        assert!(slot.execution_waiters.audit_due());
        let before = {
            let strategy = slot
                .strategy
                .try_lock()
                .expect("ACK must not hold the mutex");
            let runtime = strategy.active_runtime().unwrap().runtime();
            assert!(matches!(
                runtime.intent_store().snapshot().intents[&intent.client_order_id].state,
                IntentState::Prepared
            ));
            (
                runtime.machine().store().snapshot().clone(),
                runtime.intent_store().snapshot().clone(),
            )
        };
        assert!(registry.periodic_entries().await[0].advancing);
        {
            let strategy = slot.strategy.try_lock().unwrap();
            let runtime = strategy.active_runtime().unwrap().runtime();
            assert_eq!(runtime.machine().store().snapshot(), &before.0);
            assert_eq!(runtime.intent_store().snapshot(), &before.1);
        }
        drop(acknowledgement);
        let entry = registry.periodic_entries().await.remove(0);
        assert!(!entry.advancing);
        assert_eq!(entry.kind, Some(PreparedStrategyKind::Active));
    }

    #[tokio::test]
    async fn background_fee_settlement_deduplicates_and_does_not_block_stop() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "fees0001", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        assert!(matches!(
            registry.register(strategy).await,
            RuntimeRegistration::Registered
        ));

        assert_eq!(registry.spawn_fee_settlements(1_300).await, 1);
        gateway.wait_for_calls(1).await;
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 0);
        tokio::time::timeout(
            Duration::from_millis(200),
            registry.request_stop(&run, 1_301),
        )
        .await
        .expect("stop must not wait for historical fee HTTP")
        .unwrap();
        assert_eq!(total_fee(&registry, &run).await, Decimal::ZERO);

        gateway.prices.permits.add_permits(1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(total_fee(&registry, &run).await, Decimal::new(3, 0));
        assert_eq!(
            registry.entries().await[0].lifecycle,
            Some(PreparedStrategyLifecycle::StopRequested)
        );
        assert_eq!(registry.spawn_fee_settlements(1_400).await, 0);
    }

    #[tokio::test]
    async fn background_fee_settlement_bounds_the_batch_and_concurrent_prices() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "fees0002", 10, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 1);
        gateway.wait_for_calls(2).await;
        assert_eq!(gateway.prices.calls.load(Ordering::Acquire), 2);
        gateway.prices.permits.add_permits(8);
        wait_for_settlement(&registry, &run).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let strategy = slot.strategy.lock().await;
        let state = strategy
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot();
        assert_eq!(state.total_fee, Decimal::new(24, 0));
        assert!(strategy.has_pending_fee_valuations());
        assert_eq!(gateway.prices.maximum_active.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn background_fee_settlement_failure_retries_only_on_a_later_sweep() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "fees0003", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        gateway.prices.failures.store(1, Ordering::Release);
        gateway.prices.permits.add_permits(1);
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(total_fee(&registry, &run).await, Decimal::ZERO);
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 0);
        assert_eq!(gateway.prices.calls.load(Ordering::Acquire), 1);
        tokio::time::sleep(Duration::from_secs(5)).await;
        gateway.prices.permits.add_permits(1);
        assert_eq!(registry.spawn_fee_settlements(6_300).await, 1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(total_fee(&registry, &run).await, Decimal::new(3, 0));
    }

    #[tokio::test]
    async fn background_fee_settlement_yields_snapshot_and_commit_to_execution_waiters() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "fees0004", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let waiter = ExecutionWaiter::register(&slot.execution_waiters);
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 0);
        drop(waiter);
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 1);
        gateway.wait_for_calls(1).await;
        let waiter = ExecutionWaiter::register(&slot.execution_waiters);
        gateway.prices.permits.add_permits(1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(total_fee(&registry, &run).await, Decimal::ZERO);
        drop(waiter);
        assert!(slot.strategy.lock().await.has_pending_fee_valuations());
    }

    #[tokio::test]
    async fn background_fee_settlement_rotates_past_permanently_unavailable_orders() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "fees0007", 10, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        gateway
            .prices
            .reject_before_minute
            .store(540_000, Ordering::Release);
        gateway.prices.permits.add_permits(8);
        assert_eq!(registry.spawn_fee_settlements(1_300).await, 1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(total_fee(&registry, &run).await, Decimal::ZERO);

        tokio::time::sleep(Duration::from_secs(5)).await;
        gateway.prices.permits.add_permits(8);
        assert_eq!(registry.spawn_fee_settlements(6_300).await, 1);
        wait_for_settlement(&registry, &run).await;
        assert_eq!(
            total_fee(&registry, &run).await,
            Decimal::new(6, 0),
            "the ninth and tenth orders must settle even while the oldest eight fail"
        );
    }

    struct FeeRecoveryProvider(FeeGateway);

    impl RuntimeRecoveryProvider for FeeRecoveryProvider {
        type Gateway = FeeGateway;
        type Error = ();

        fn runtime_for(
            &self,
            _: Exchange,
            _: &StrategyRunId,
        ) -> Result<(FeeGateway, RuntimeSettings), ()> {
            Ok((
                self.0.clone(),
                RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
            ))
        }
    }

    #[tokio::test]
    async fn background_fee_settlement_recovers_stopped_runs_without_owning_the_market() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let stopped = pending_strategy(directory.path(), &gateway, "fees0005", 1, true).await;
        let stopped_run = stopped.run_id().clone();
        drop(stopped);
        let coordinator = RuntimeCoordinator::new(
            directory.path().to_owned(),
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        );
        let registry = coordinator.registry();
        let report = coordinator
            .recover(&FeeRecoveryProvider(gateway.clone()))
            .await
            .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert_eq!(report.registered, vec![stopped_run.clone()]);
        assert!(registry.prune_terminal().await.is_empty());
        assert_eq!(
            registry.owner_for_market(Exchange::Binance, "MUUSDT").await,
            None
        );

        let live = pending_strategy(directory.path(), &gateway, "fees0006", 0, false).await;
        let live_run = live.run_id().clone();
        assert!(matches!(
            registry.register(live).await,
            RuntimeRegistration::Registered
        ));
        assert_eq!(
            registry.owner_for_market(Exchange::Binance, "MUUSDT").await,
            Some(live_run)
        );
        assert_eq!(
            coordinator.entries().await.len(),
            1,
            "maintenance slots are not API trading owners"
        );
        let stopped_slot = registry
            .entries
            .read()
            .await
            .get(&stopped_run)
            .unwrap()
            .clone();
        let committing = stopped_slot.strategy.lock().await;
        assert_eq!(
            coordinator.entries().await.len(),
            1,
            "busy maintenance must not look like a trading owner"
        );
        drop(committing);
        gateway.prices.permits.add_permits(1);
        assert_eq!(coordinator.spawn_fee_settlements(1_300).await, 1);
        wait_for_settlement(&registry, &stopped_run).await;
        assert_eq!(total_fee(&registry, &stopped_run).await, Decimal::new(3, 0));
        assert_eq!(
            registry
                .entries()
                .await
                .iter()
                .find(|entry| entry.run_id == stopped_run)
                .unwrap()
                .lifecycle,
            Some(PreparedStrategyLifecycle::Stopped)
        );
        assert_eq!(registry.prune_terminal().await, vec![stopped_run]);
        let persisted =
            FileStrategyStateStore::load(directory.path().join("fees0005/strategy.json")).unwrap();
        assert_eq!(persisted.snapshot().lifecycle, StrategyLifecycle::Stopped);
    }
}
