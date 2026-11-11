use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, stream};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, Semaphore};

use crate::domain::{ClientOrderId, Exchange};
use crate::exchange::{
    AccountBalanceSnapshotGateway, ExchangeIdentityGateway, ExecutionSnapshotGateway,
    HistoricalPriceGateway, InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway,
    OpenOrderSnapshotGateway, OrderCancellationGateway, OrderLookupGateway, OrderPlacementGateway,
    PositionSnapshotGateway, SnapshotError, TradingFeeRateGateway,
};
use crate::persistence::{
    StrategyCatalog, StrategyCatalogSnapshot, StrategyDiscoveryAnomaly, StrategyDiscoveryReport,
    StrategyFilePaths,
};

use super::exchange_inputs::advance_reference_time_ms;
use super::periodic_priority::{PeriodicPriority, PriorityWaiter as ExecutionWaiter};
use super::runtime::PreparedExecutionEventStep;
use super::{
    FileStrategyRecoveryError, PendingFeeValuation, PreparedLeasedFileStrategy,
    PreparedStrategyKind, PreparedStrategyLifecycle, PreparedStrategyStep,
    PreparedStrategyStepError, PreparedStrategyStopError, PreparedStrategyStopOutcome,
    ProfitProtectionControlError, ProfitProtectionObservation, ProfitProtectionStatus,
    RuntimeSettings, RuntimeTickReport, StrategyRunId, StrategyState, StrategyStateStore,
    StrategyTransition, attribute_strategy_funding, claim_leased_file_strategy,
    collect_strategy_shadow_view, strategy_profit_metrics,
};

const FEE_SETTLEMENT_BATCH_SIZE: usize = 8;
const FEE_SETTLEMENT_CONCURRENCY: usize = 2;
const FEE_SETTLEMENT_RETRY_DELAY: Duration = Duration::from_secs(5);
const FEE_SETTLEMENT_PRICE_TIMEOUT: Duration = Duration::from_secs(10);
const PROFIT_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const PROFIT_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const PROFIT_CHECK_CONCURRENCY: usize = 2;

struct RuntimeSlot<G> {
    exchange: Exchange,
    symbol: String,
    owns_market: AtomicBool,
    unlocked_placements: AtomicUsize,
    execution_waiters: PeriodicPriority,
    fee_settlement: std::sync::Mutex<FeeSettlementSchedule>,
    profit_check: std::sync::Mutex<ProfitCheckSchedule>,
    strategy: PublishedStrategy<G>,
}

// Queries share only committed immutable state. Publishing on guard release
// covers success, errors and cancellation without queueing behind trading work.
struct PublishedStrategy<G> {
    inner: Mutex<PreparedLeasedFileStrategy<G>>,
    snapshot: std::sync::RwLock<Option<StrategyCatalogSnapshot>>,
}

impl<G> PublishedStrategy<G> {
    fn new(strategy: PreparedLeasedFileStrategy<G>) -> Self {
        let snapshot = committed_query_snapshot(&strategy);
        Self {
            inner: Mutex::new(strategy),
            snapshot: std::sync::RwLock::new(snapshot),
        }
    }

    async fn lock(&self) -> PublishedStrategyGuard<'_, G> {
        PublishedStrategyGuard {
            guard: self.inner.lock().await,
            published: &self.snapshot,
        }
    }

    fn try_lock(&self) -> Result<PublishedStrategyGuard<'_, G>, tokio::sync::TryLockError> {
        self.inner.try_lock().map(|guard| PublishedStrategyGuard {
            guard,
            published: &self.snapshot,
        })
    }

    fn snapshot(&self) -> Option<StrategyCatalogSnapshot> {
        self.snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

fn committed_query_snapshot<G>(
    strategy: &PreparedLeasedFileStrategy<G>,
) -> Option<StrategyCatalogSnapshot> {
    if let Some(runtime) = strategy.active_runtime() {
        committed_active_query_snapshot(runtime)
    } else {
        strategy
            .armed_strategy()
            .map(|armed| StrategyCatalogSnapshot::Armed(Box::new(armed.snapshot().clone())))
    }
}

fn committed_active_query_snapshot<G>(
    runtime: &super::LeasedFileStrategyRuntime<G>,
) -> Option<StrategyCatalogSnapshot> {
    // Reuses the concrete stores' exact revision-pair proof. A partially
    // committed intent/state update must never become current query evidence.
    runtime.runtime().verify_ledger_ownership().ok()?;
    Some(StrategyCatalogSnapshot::Active(
        runtime.runtime().machine().store().shared_snapshot(),
    ))
}

struct PublishedStrategyGuard<'a, G> {
    guard: tokio::sync::MutexGuard<'a, PreparedLeasedFileStrategy<G>>,
    published: &'a std::sync::RwLock<Option<StrategyCatalogSnapshot>>,
}

impl<G> Deref for PublishedStrategyGuard<'_, G> {
    type Target = PreparedLeasedFileStrategy<G>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<G> DerefMut for PublishedStrategyGuard<'_, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<G> Drop for PublishedStrategyGuard<'_, G> {
    fn drop(&mut self) {
        *self
            .published
            .write()
            .unwrap_or_else(|error| error.into_inner()) = committed_query_snapshot(&self.guard);
    }
}

#[derive(Default)]
struct FeeSettlementSchedule {
    in_flight: bool,
    not_before: Option<Instant>,
    after: Option<ClientOrderId>,
}

#[derive(Default)]
struct ProfitCheckSchedule {
    in_flight: bool,
    not_before: Option<Instant>,
    observation: ProfitProtectionObservation,
}

struct ProfitCheckLease<G>(RuntimeSlotHandle<G>);

impl<G> Drop for ProfitCheckLease<G> {
    fn drop(&mut self) {
        let mut schedule = self
            .0
            .profit_check
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        schedule.in_flight = false;
        schedule.not_before = Some(Instant::now() + PROFIT_CHECK_INTERVAL);
        if schedule.observation.status == "checking" {
            schedule.observation = ProfitProtectionObservation {
                status: "unavailable".into(),
                error: Some("strategy changed during verification; retry pending".into()),
                ..Default::default()
            };
        }
    }
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

struct UnlockedPlacementReservation<'a>(&'a AtomicUsize);

impl<'a> UnlockedPlacementReservation<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count)
    }
}

impl Drop for UnlockedPlacementReservation<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct RuntimeRegistry<G> {
    entries: RwLock<BTreeMap<StrategyRunId, RuntimeSlotHandle<G>>>,
    profit_check_capacity: Arc<Semaphore>,
    profit_check_after: std::sync::Mutex<Option<StrategyRunId>>,
}

impl<G> Default for RuntimeRegistry<G> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            profit_check_capacity: Arc::new(Semaphore::new(PROFIT_CHECK_CONCURRENCY)),
            profit_check_after: std::sync::Mutex::new(None),
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

    pub async fn profit_protection_status(
        &self,
        exchange: Exchange,
        symbol: &str,
        run_id: &StrategyRunId,
    ) -> Result<ProfitProtectionStatus, ProfitProtectionControlError> {
        let slot = self
            .profit_protection_slot(exchange, symbol, run_id)
            .await?;
        let strategy = slot.strategy.lock().await;
        profit_status(&slot, &strategy)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_profit_protection(
        &self,
        exchange: Exchange,
        symbol: &str,
        run_id: &StrategyRunId,
        expected_revision: u64,
        take_profit_amount: Option<Decimal>,
        stop_loss_amount: Option<Decimal>,
        now_ms: u64,
    ) -> Result<ProfitProtectionStatus, ProfitProtectionControlError> {
        let started = Instant::now();
        let slot = self
            .profit_protection_slot(exchange, symbol, run_id)
            .await?;
        let _waiter = ExecutionWaiter::register_stop(&slot.execution_waiters);
        let mut strategy = slot.strategy.lock().await;
        let effective_now =
            advance_reference_time_ms(now_ms, started.elapsed()).max(strategy.updated_at_ms());
        let transition = strategy.update_profit_protection(
            expected_revision,
            take_profit_amount,
            stop_loss_amount,
            effective_now,
        )?;
        if transition != StrategyTransition::NoChange {
            let mut schedule = slot
                .profit_check
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            schedule.observation = ProfitProtectionObservation::default();
            schedule.not_before = None;
        }
        profit_status(&slot, &strategy)
    }

    async fn profit_protection_slot(
        &self,
        exchange: Exchange,
        symbol: &str,
        run_id: &StrategyRunId,
    ) -> Result<RuntimeSlotHandle<G>, ProfitProtectionControlError> {
        let entries = self.entries.read().await;
        let slot = entries.get(run_id).ok_or_else(|| {
            if entries.values().any(|slot| {
                slot.exchange == exchange
                    && slot.symbol == symbol
                    && slot.owns_market.load(Ordering::Acquire)
            }) {
                ProfitProtectionControlError::RunMismatch
            } else {
                ProfitProtectionControlError::NotFound
            }
        })?;
        if slot.exchange != exchange || slot.symbol != symbol {
            return Err(ProfitProtectionControlError::RunMismatch);
        }
        Ok(Arc::clone(slot))
    }

    pub(crate) async fn spawn_profit_protection_checks(&self, now_ms: u64) -> usize
    where
        G: Clone
            + AccountBalanceSnapshotGateway
            + PositionSnapshotGateway
            + OpenOrderSnapshotGateway
            + Send
            + Sync
            + 'static,
    {
        let mut slots = self
            .entries
            .read()
            .await
            .iter()
            .map(|(id, slot)| (id.clone(), Arc::clone(slot)))
            .collect::<Vec<_>>();
        rotate_profit_slots(
            &mut slots,
            self.profit_check_after
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref(),
        );
        let mut spawned = 0;
        for (run_id, slot) in slots {
            if !slot.owns_market.load(Ordering::Acquire)
                || slot.execution_waiters.has_waiters()
                || slot.unlocked_placements.load(Ordering::Acquire) > 0
            {
                continue;
            }
            let Ok(permit) = Arc::clone(&self.profit_check_capacity).try_acquire_owned() else {
                break;
            };
            let work = {
                let Ok(strategy) = slot.strategy.try_lock() else {
                    continue;
                };
                let Some(runtime) = strategy.active_runtime() else {
                    continue;
                };
                let state = runtime.runtime().machine().store().snapshot();
                if !profit_check_enabled(state)
                    || slot.execution_waiters.has_waiters()
                    || slot.unlocked_placements.load(Ordering::Acquire) > 0
                {
                    continue;
                }
                let mut schedule = slot
                    .profit_check
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if schedule.in_flight || schedule.not_before.is_some_and(|at| Instant::now() < at) {
                    continue;
                }
                schedule.in_flight = true;
                schedule.observation = ProfitProtectionObservation {
                    status: "checking".into(),
                    ..Default::default()
                };
                (
                    runtime.runtime().gateway().clone(),
                    runtime.runtime().machine().store().shared_snapshot(),
                )
            };
            let lease = ProfitCheckLease(Arc::clone(&slot));
            tokio::spawn(async move {
                let _lease = lease;
                let _permit = permit;
                check_profit_protection(slot, work.0, work.1, now_ms).await;
            });
            *self
                .profit_check_after
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(run_id);
            spawned += 1;
        }
        spawned
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
                unlocked_placements: AtomicUsize::new(0),
                execution_waiters: PeriodicPriority::default(),
                fee_settlement: std::sync::Mutex::new(FeeSettlementSchedule::default()),
                profit_check: std::sync::Mutex::new(ProfitCheckSchedule::default()),
                strategy: PublishedStrategy::new(strategy),
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

    pub(crate) async fn query_catalog(
        &self,
        market: Option<(Exchange, &str)>,
    ) -> Result<(StrategyCatalog, Vec<RuntimeRegistryEntry>), SnapshotError> {
        let slots = self.entries.read().await;
        let mut snapshots = Vec::new();
        let mut entries = Vec::new();
        for (run_id, slot) in slots.iter().filter(|(_, slot)| {
            slot.owns_market.load(Ordering::Acquire)
                && market.is_none_or(|(exchange, symbol)| {
                    slot.exchange == exchange && slot.symbol == symbol
                })
        }) {
            // Never queue a dashboard read behind a trading mutation.
            let guard = slot.strategy.try_lock().ok();
            let snapshot = if let Some(strategy) = guard.as_ref() {
                committed_query_snapshot(strategy)
            } else {
                slot.strategy.snapshot()
            }
            .ok_or_else(|| {
                SnapshotError::new("strategy transition has no committed query snapshot")
            })?;
            if snapshot.run_id() != run_id.as_str()
                || snapshot.exchange() != slot.exchange
                || snapshot.symbol() != slot.symbol
            {
                return Err(SnapshotError::new(
                    "runtime query snapshot identity mismatch",
                ));
            }
            let advancing = guard.is_none()
                || slot.execution_waiters.has_waiters()
                || slot.unlocked_placements.load(Ordering::Acquire) > 0;
            entries.push(RuntimeRegistryEntry {
                run_id: run_id.clone(),
                exchange: slot.exchange,
                symbol: slot.symbol.clone(),
                kind: guard.as_ref().map(|strategy| strategy.kind()),
                lifecycle: guard.as_ref().map(|strategy| strategy.lifecycle()),
                advancing,
            });
            snapshots.push(snapshot);
        }
        Ok((StrategyCatalog::from_runtime_snapshots(snapshots), entries))
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
                if slot.unlocked_placements.load(Ordering::Acquire) > 0 {
                    return RuntimeRegistryEntry {
                        run_id,
                        exchange: slot.exchange,
                        symbol: slot.symbol.clone(),
                        kind: None,
                        lifecycle: None,
                        advancing: true,
                    };
                }
                // Periodic reads may yield, but an unlocked write must finish before
                // an overdue audit can inspect its durably prepared intents.
                let audit_due = periodic
                    && matches!(slot.exchange, Exchange::Binance | Exchange::Bybit)
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
        if slot.unlocked_placements.load(Ordering::Acquire) > 0 {
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
        let bounded_priority = matches!(slot.exchange, Exchange::Binance | Exchange::Bybit);
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
        if slot.unlocked_placements.load(Ordering::Acquire) > 0 {
            return Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(
                run_id.clone(),
            ));
        }
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
        let effective_now_ms =
            advance_reference_time_ms(now_ms, lock_started.elapsed()).max(strategy.updated_at_ms());
        if matches!(exchange, Exchange::TradeXyz | Exchange::Bybit)
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
            let _reservation = UnlockedPlacementReservation::new(&slot.unlocked_placements);
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
        let started = Instant::now();
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
        let strategy_lock_wait_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let effective_now_ms = now_ms.max(strategy.updated_at_ms());
        let persistence_started = Instant::now();
        let result = strategy.request_stop(effective_now_ms).map_err(Into::into);
        tracing::info!(
            run_id = run_id.as_str(),
            exchange = ?slot.exchange,
            symbol = slot.symbol,
            request_received_at_ms = now_ms,
            strategy_reference_time_ms = effective_now_ms,
            strategy_lock_wait_ms,
            persistence_ms = u64::try_from(persistence_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            durable = result.is_ok(),
            "strategy stop control stages completed"
        );
        slot.owns_market
            .store(!strategy.is_terminal(), Ordering::Release);
        result
    }
}

fn profit_check_enabled(state: &StrategyState) -> bool {
    matches!(
        state.lifecycle,
        super::StrategyLifecycle::AwaitingOpening
            | super::StrategyLifecycle::DeployingGrid
            | super::StrategyLifecycle::Running
    ) && state.profit_protection_trigger.is_none()
        && (state.profit_protection.take_profit_amount.is_some()
            || state.profit_protection.stop_loss_amount.is_some())
}

fn rotate_profit_slots<T>(slots: &mut [(StrategyRunId, T)], after: Option<&StrategyRunId>) {
    if let Some(after) = after {
        let start = slots.iter().position(|(run, _)| run > after).unwrap_or(0);
        slots.rotate_left(start);
    }
}

fn profit_status<G>(
    slot: &RuntimeSlot<G>,
    strategy: &PreparedLeasedFileStrategy<G>,
) -> Result<ProfitProtectionStatus, ProfitProtectionControlError> {
    let mut observation = slot
        .profit_check
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .observation
        .clone();
    let mut status = strategy.profit_protection_status(observation.clone())?;
    if let Some(trigger) = &status.trigger {
        observation = ProfitProtectionObservation {
            status: "triggered".into(),
            observed_at_ms: Some(trigger.observed_at_ms),
            net_profit: Some(trigger.net_profit),
            error: None,
        };
    } else if status.profit_protection.take_profit_amount.is_none()
        && status.profit_protection.stop_loss_amount.is_none()
    {
        observation = ProfitProtectionObservation {
            status: "disabled".into(),
            ..Default::default()
        };
    } else if observation.status.is_empty() {
        observation.status = "checking".into();
    }
    status.observation = observation;
    Ok(status)
}

struct VerifiedProfitSample {
    net_profit: Decimal,
    mark_price: Decimal,
    observed_at_ms: u64,
    funding_through_ms: u64,
}

async fn profit_sample<G>(
    gateway: &G,
    state: &StrategyState,
    funding: crate::exchange::funding::FundingAttribution,
    now_ms: u64,
) -> Result<VerifiedProfitSample, SnapshotError>
where
    G: OpenOrderSnapshotGateway + PositionSnapshotGateway,
{
    let collected = tokio::time::timeout(
        PROFIT_CHECK_TIMEOUT,
        collect_strategy_shadow_view(gateway, state),
    )
    .await
    .map_err(|_| SnapshotError::new("profit protection exchange verification timed out"))?
    .map_err(|error| SnapshotError::new(error.to_string()))?;
    if !collected.report.clean {
        return Err(SnapshotError::new(
            "profit protection requires exact order and inventory ownership",
        ));
    }
    let metrics = strategy_profit_metrics(state, &collected.position)?;
    Ok(VerifiedProfitSample {
        net_profit: metrics
            .total_equity_profit
            .checked_add(funding.net_income)
            .ok_or_else(|| SnapshotError::new("strategy net profit arithmetic overflowed"))?,
        mark_price: metrics.mark_price,
        observed_at_ms: now_ms,
        funding_through_ms: funding.through_ms,
    })
}

async fn collect_profit_sample<G>(
    slot: &RuntimeSlot<G>,
    gateway: &G,
    identity: &StrategyState,
    now_ms: u64,
) -> Result<(Arc<StrategyState>, VerifiedProfitSample), SnapshotError>
where
    G: AccountBalanceSnapshotGateway + OpenOrderSnapshotGateway + PositionSnapshotGateway,
{
    if identity.pending_fee_valuation_count() != 0 {
        return Err(SnapshotError::new(
            "actual execution fees are not settled yet",
        ));
    }
    let started = Instant::now();
    let cached = tokio::time::timeout(
        PROFIT_CHECK_TIMEOUT,
        gateway.funding_history(
            identity.exchange,
            &identity.symbol,
            identity.created_at_ms,
            now_ms,
        ),
    )
    .await
    .unwrap_or_else(|_| Err(SnapshotError::new("funding history verification timed out")));
    let observed = advance_reference_time_ms(now_ms, started.elapsed());
    let refresh = cached.as_ref().map_or(true, |history| {
        !profit_evidence_fresh(history.through_ms, observed)
    });
    let history = if refresh {
        fresh_profit_funding(gateway, identity, observed).await?
    } else {
        cached?
    };
    let history = super::complete_funding_account_history(
        gateway,
        history,
        !identity.external_fills.is_empty(),
    )
    .await?;
    let state = capture_profit_state(slot, identity)?;
    let funding = attribute_strategy_funding(&state, &history)?;
    let sample = profit_sample(
        gateway,
        &state,
        funding,
        advance_reference_time_ms(now_ms, started.elapsed()),
    )
    .await?;
    if refresh
        || state
            .profit_protection
            .triggered_reason(sample.net_profit)
            .is_none()
    {
        return Ok((state, sample));
    }
    // Rebase only after funding I/O, keeping the final exact-revision fence short.
    let through = advance_reference_time_ms(now_ms, started.elapsed());
    let history = fresh_profit_funding(gateway, identity, through).await?;
    let state = capture_profit_state(slot, identity)?;
    let funding = attribute_strategy_funding(&state, &history)?;
    let sample = profit_sample(
        gateway,
        &state,
        funding,
        advance_reference_time_ms(now_ms, started.elapsed()),
    )
    .await?;
    Ok((state, sample))
}

fn profit_evidence_fresh(at_ms: u64, now_ms: u64) -> bool {
    now_ms.checked_sub(at_ms).is_some_and(|age| age <= 5_000)
}

fn capture_profit_state<G>(
    slot: &RuntimeSlot<G>,
    identity: &StrategyState,
) -> Result<Arc<StrategyState>, SnapshotError> {
    if slot.execution_waiters.has_waiters() || slot.unlocked_placements.load(Ordering::Acquire) > 0
    {
        return Err(SnapshotError::new(
            "trading work has priority; profit verification will retry",
        ));
    }
    let strategy = slot
        .strategy
        .try_lock()
        .map_err(|_| SnapshotError::new("strategy is advancing; profit verification will retry"))?;
    let runtime = strategy
        .active_runtime()
        .ok_or_else(|| SnapshotError::new("strategy is no longer active"))?;
    let state = runtime.runtime().machine().store().shared_snapshot();
    if state.run_id != identity.run_id
        || state.profit_protection.revision != identity.profit_protection.revision
        || !profit_check_enabled(&state)
    {
        return Err(SnapshotError::new(
            "profit protection configuration or lifecycle changed",
        ));
    }
    if state.pending_fee_valuation_count() != 0 {
        return Err(SnapshotError::new(
            "actual execution fees are not settled yet",
        ));
    }
    Ok(state)
}

async fn fresh_profit_funding<G: AccountBalanceSnapshotGateway + PositionSnapshotGateway>(
    gateway: &G,
    state: &StrategyState,
    through: u64,
) -> Result<crate::exchange::funding::FundingHistorySnapshot, SnapshotError> {
    let history = tokio::time::timeout(
        PROFIT_CHECK_TIMEOUT,
        gateway.fresh_funding_history(state.exchange, &state.symbol, state.created_at_ms, through),
    )
    .await
    .map_err(|_| SnapshotError::new("fresh funding verification timed out"))??;
    if history.through_ms != through {
        return Err(SnapshotError::new(
            "fresh funding does not cover the decision interval",
        ));
    }
    super::complete_funding_account_history(gateway, history, !state.external_fills.is_empty())
        .await
}

async fn check_profit_protection<G>(
    slot: RuntimeSlotHandle<G>,
    gateway: G,
    captured: Arc<StrategyState>,
    now_ms: u64,
) where
    G: AccountBalanceSnapshotGateway + OpenOrderSnapshotGateway + PositionSnapshotGateway,
{
    let started = Instant::now();
    let result = collect_profit_sample(&slot, &gateway, &captured, now_ms).await;
    if slot.execution_waiters.has_waiters() || slot.unlocked_placements.load(Ordering::Acquire) > 0
    {
        return;
    }
    let Ok(mut strategy) = slot.strategy.try_lock() else {
        return;
    };
    if slot.execution_waiters.has_waiters() || slot.unlocked_placements.load(Ordering::Acquire) > 0
    {
        return;
    }
    let Some(runtime) = strategy.active_runtime() else {
        return;
    };
    let state = runtime.runtime().machine().store().snapshot();
    if state.run_id != captured.run_id
        || state.profit_protection.revision != captured.profit_protection.revision
        || !profit_check_enabled(state)
    {
        return;
    }
    let effective_now =
        advance_reference_time_ms(now_ms, started.elapsed()).max(state.updated_at_ms);
    let result = result.and_then(|(observed_state, sample)| {
        if observed_state.revision != state.revision {
            return Err(SnapshotError::new(
                "strategy changed during the final profit snapshot; retry pending",
            ));
        }
        if !profit_evidence_fresh(sample.observed_at_ms, effective_now)
            || !profit_evidence_fresh(sample.funding_through_ms, effective_now)
        {
            return Err(SnapshotError::new(
                "profit or funding evidence is older than five seconds; retry pending",
            ));
        }
        Ok(sample)
    });
    let observation = match result {
        Ok(sample) => {
            match strategy.evaluate_profit_protection(
                sample.net_profit,
                sample.mark_price,
                sample.observed_at_ms,
                captured.profit_protection.revision,
                effective_now,
            ) {
                Ok(transition) => ProfitProtectionObservation {
                    status: if matches!(transition, StrategyTransition::RiskExitRequested { .. }) {
                        "triggered"
                    } else {
                        "verified"
                    }
                    .into(),
                    observed_at_ms: Some(sample.observed_at_ms),
                    net_profit: Some(sample.net_profit),
                    error: None,
                },
                Err(error) => ProfitProtectionObservation {
                    status: "unavailable".into(),
                    observed_at_ms: Some(effective_now),
                    error: Some(error.to_string()),
                    ..Default::default()
                },
            }
        }
        Err(error) => ProfitProtectionObservation {
            status: "unavailable".into(),
            observed_at_ms: Some(effective_now),
            error: Some(error.to_string()),
            ..Default::default()
        },
    };
    slot.profit_check
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .observation = observation;
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
        profit: Arc<std::sync::Mutex<ProfitFixture>>,
        profit_calls: Arc<AtomicUsize>,
        profit_gate: Arc<Semaphore>,
    }

    #[derive(Default)]
    struct ProfitFixture {
        orders: Vec<AuthoritativeOrder>,
        position_quantity: Decimal,
        hold_funding: bool,
        funding_unavailable: bool,
        cached_funding_error: bool,
        cached_funding_lag: u64,
        fresh_funding_lag: u64,
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
                profit: Arc::new(std::sync::Mutex::new(ProfitFixture::default())),
                profit_calls: Arc::new(AtomicUsize::new(0)),
                profit_gate: Arc::new(Semaphore::new(0)),
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

        async fn profit_history(
            &self,
            exchange: Exchange,
            symbol: &str,
            from_ms: u64,
            through_ms: u64,
            fresh: bool,
        ) -> Result<crate::exchange::funding::FundingHistorySnapshot, SnapshotError> {
            self.profit_calls.fetch_add(1, Ordering::AcqRel);
            let hold = self.profit.lock().unwrap().hold_funding;
            if hold {
                self.profit_gate.acquire().await.unwrap().forget();
            }
            let state = self.profit.lock().unwrap();
            if state.funding_unavailable || (!fresh && state.cached_funding_error) {
                return Err(SnapshotError::new("funding unavailable"));
            }
            Ok(crate::exchange::funding::FundingHistorySnapshot {
                exchange,
                symbol: symbol.to_owned(),
                from_ms,
                through_ms: through_ms.saturating_sub(if fresh {
                    state.fresh_funding_lag
                } else {
                    state.cached_funding_lag
                }),
                payments: Vec::new(),
                account_fills: Some(Vec::new()),
            })
        }
    }

    impl ExchangeIdentityGateway for FeeGateway {
        fn exchange(&self) -> Exchange {
            self.exchange
        }
    }

    #[async_trait]
    impl crate::exchange::OpenOrderSnapshotGateway for FeeGateway {
        async fn open_orders_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<Vec<AuthoritativeOrder>, SnapshotError> {
            Ok(self.profit.lock().unwrap().orders.clone())
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for FeeGateway {
        async fn position_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<crate::exchange::PositionSnapshot, SnapshotError> {
            let quantity = self.profit.lock().unwrap().position_quantity;
            Ok(crate::exchange::PositionSnapshot {
                exchange,
                symbol: symbol.to_owned(),
                legs: vec![crate::exchange::PositionLeg {
                    side: crate::exchange::PositionSide::Both,
                    signed_quantity: quantity,
                    entry_price: (!quantity.is_zero()).then_some(Decimal::new(1014, 0)),
                    mark_price: Decimal::new(1014, 0),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(Decimal::new(5, 0)),
                }],
            })
        }
    }

    #[async_trait]
    impl AccountBalanceSnapshotGateway for FeeGateway {
        async fn account_balance_snapshot(
            &self,
            _exchange: Exchange,
        ) -> Result<crate::exchange::AccountBalanceSnapshot, SnapshotError> {
            panic!("profit checks must not request unrelated account balance");
        }

        async fn funding_history(
            &self,
            exchange: Exchange,
            symbol: &str,
            from_ms: u64,
            through_ms: u64,
        ) -> Result<crate::exchange::funding::FundingHistorySnapshot, SnapshotError> {
            self.profit_history(exchange, symbol, from_ms, through_ms, false)
                .await
        }

        async fn fresh_funding_history(
            &self,
            exchange: Exchange,
            symbol: &str,
            from_ms: u64,
            through_ms: u64,
        ) -> Result<crate::exchange::funding::FundingHistorySnapshot, SnapshotError> {
            self.profit_history(exchange, symbol, from_ms, through_ms, true)
                .await
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
                    realized_profit: Some(Decimal::ZERO),
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
    async fn profit_protection_controls_pin_run_and_revision_without_changing_inventory() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "pctrl001", 0, false).await;
        let run = strategy.run_id().clone();
        let before = strategy
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .clone();
        let registry = RuntimeRegistry::new();
        assert!(matches!(
            registry.register(strategy).await,
            RuntimeRegistration::Registered
        ));
        let initial = registry
            .profit_protection_status(Exchange::Binance, "MUUSDT", &run)
            .await
            .unwrap();
        assert_eq!(initial.profit_protection.revision, 0);
        assert_eq!(initial.observation.status, "disabled");
        assert!(matches!(
            registry
                .profit_protection_status(Exchange::Bybit, "MUUSDT", &run)
                .await,
            Err(ProfitProtectionControlError::RunMismatch)
        ));
        assert!(matches!(
            registry
                .profit_protection_status(Exchange::Binance, "OTHERUSDT", &run)
                .await,
            Err(ProfitProtectionControlError::RunMismatch)
        ));
        let updated = registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                Some(Decimal::TEN),
                Some(Decimal::ONE),
                1_300,
            )
            .await
            .unwrap();
        assert_eq!(updated.profit_protection.revision, 1);
        assert_eq!(
            updated.profit_protection.take_profit_amount,
            Some(Decimal::TEN)
        );
        assert_eq!(updated.observation.status, "checking");
        assert!(matches!(
            registry
                .update_profit_protection(Exchange::Binance, "MUUSDT", &run, 0, None, None, 1_400)
                .await,
            Err(ProfitProtectionControlError::RevisionConflict)
        ));
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let after = slot
            .strategy
            .lock()
            .await
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .clone();
        assert_eq!(after.baseline, before.baseline);
        assert_eq!(after.orders, before.orders);
        assert_eq!(
            after.grid_position_net_quantity,
            before.grid_position_net_quantity
        );
        let disabled = registry
            .update_profit_protection(Exchange::Binance, "MUUSDT", &run, 1, None, None, 1_500)
            .await
            .unwrap();
        assert_eq!(disabled.observation.status, "disabled");
        registry.request_stop(&run, 1_600).await.unwrap();
        assert!(matches!(
            registry
                .update_profit_protection(
                    Exchange::Binance,
                    "MUUSDT",
                    &run,
                    2,
                    Some(Decimal::TEN),
                    None,
                    1_700
                )
                .await,
            Err(ProfitProtectionControlError::NotRunning)
        ));
        assert_eq!(gateway.prices.calls.load(Ordering::Acquire), 0);
    }

    async fn profit_strategy(
        root: &std::path::Path,
        gateway: &FeeGateway,
        run: &str,
    ) -> PreparedLeasedFileStrategy<FeeGateway> {
        profit_strategy_with_fills(root, gateway, run, 0).await
    }

    async fn profit_strategy_with_fills(
        root: &std::path::Path,
        gateway: &FeeGateway,
        run: &str,
        fills: usize,
    ) -> PreparedLeasedFileStrategy<FeeGateway> {
        let mut strategy = pending_strategy(root, gateway, run, fills, false).await;
        gateway.prices.permits.add_permits(fills);
        for work in strategy.prepare_pending_fee_work(8) {
            strategy
                .complete_fee_valuation(work.resolve().await.unwrap(), 1_300)
                .unwrap();
        }
        let paths = strategy.paths().clone();
        drop(strategy);
        let mut machine =
            StrategyMachine::new(FileStrategyStateStore::load(paths.state()).unwrap());
        let rules = machine.store().snapshot().instrument_rules.clone();
        machine.materialize_replacements(&rules, 1_400).unwrap();
        let ready = machine
            .store()
            .snapshot()
            .orders
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        for (index, order) in ready.into_iter().enumerate() {
            if order.tracking != StrategyOrderTracking::Ready {
                continue;
            }
            let intent = OrderIntent::prepare(
                order.client_order_id.clone(),
                gateway.exchange(),
                order.shape.clone(),
                1_000,
            )
            .unwrap();
            intents.insert_prepared(intent).unwrap();
            intents
                .transition(
                    &order.client_order_id,
                    IntentState::Accepted {
                        exchange_order_id: format!("profit-order-{index}"),
                    },
                    1_200,
                )
                .unwrap();
            let accepted = intents
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .clone();
            machine.synchronize_intent(&accepted, 1_400).unwrap();
            gateway
                .profit
                .lock()
                .unwrap()
                .orders
                .push(AuthoritativeOrder {
                    client_order_id: order.client_order_id,
                    exchange_order_id: format!("profit-order-{index}"),
                    exchange: gateway.exchange(),
                    shape: order.shape,
                    lifecycle: OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::New),
                    executed_quantity: Some(Decimal::ZERO),
                });
        }
        gateway.profit.lock().unwrap().position_quantity = machine
            .store()
            .snapshot()
            .expected_exchange_position()
            .unwrap();
        drop(machine);
        recover_leased_file_strategy(
            gateway.clone(),
            paths,
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        )
        .unwrap()
    }

    async fn wait_profit(
        registry: &RuntimeRegistry<FeeGateway>,
        run: &StrategyRunId,
    ) -> ProfitProtectionStatus {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = registry
                    .profit_protection_status(Exchange::Binance, "MUUSDT", run)
                    .await
                    .unwrap();
                if status.observation.status != "checking" {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("profit check must finish")
    }

    #[tokio::test]
    async fn profit_protection_background_verifies_exact_ownership_and_stays_off_when_disabled() {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = profit_strategy(dir.path(), &gateway, "pmon0001").await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 0);
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                Some(Decimal::TEN),
                None,
                1_500,
            )
            .await
            .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 1);
        let status = wait_profit(&registry, &run).await;
        assert_eq!(status.observation.status, "verified", "{status:?}");
        assert_eq!(status.observation.net_profit, Some(Decimal::ZERO));
        assert!(status.trigger.is_none());
        assert_eq!(registry.spawn_profit_protection_checks(2_001).await, 0);
        registry
            .update_profit_protection(Exchange::Binance, "MUUSDT", &run, 1, None, None, 2_100)
            .await
            .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(5_000).await, 0);
        assert_eq!(gateway.profit_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn profit_protection_background_network_does_not_block_edits_or_stop_or_apply_old_config()
    {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        gateway.profit.lock().unwrap().hold_funding = true;
        let strategy = profit_strategy(dir.path(), &gateway, "pmon0002").await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                Some(Decimal::TEN),
                None,
                1_500,
            )
            .await
            .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 1);
        tokio::time::timeout(Duration::from_secs(5), async {
            while gateway.profit_calls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 0);
        let disabled = tokio::time::timeout(
            Duration::from_secs(1),
            registry.update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                1,
                None,
                None,
                2_100,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(disabled.observation.status, "disabled");
        tokio::time::timeout(Duration::from_secs(1), registry.request_stop(&run, 2_200))
            .await
            .unwrap()
            .unwrap();
        gateway.profit_gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let status = registry
            .profit_protection_status(Exchange::Binance, "MUUSDT", &run)
            .await
            .unwrap();
        assert_eq!(status.observation.status, "disabled");
        assert!(status.trigger.is_none());
    }

    #[tokio::test]
    async fn profit_protection_background_never_guesses_missing_funding_or_external_inventory() {
        for bad_funding in [true, false] {
            let dir = tempdir().unwrap();
            let gateway = FeeGateway::new();
            let strategy = profit_strategy(dir.path(), &gateway, "pmon0003").await;
            if bad_funding {
                gateway.profit.lock().unwrap().funding_unavailable = true;
            } else {
                gateway.profit.lock().unwrap().position_quantity = Decimal::ONE;
            }
            let run = strategy.run_id().clone();
            let registry = RuntimeRegistry::new();
            registry.register(strategy).await;
            registry
                .update_profit_protection(
                    Exchange::Binance,
                    "MUUSDT",
                    &run,
                    0,
                    Some(Decimal::ONE),
                    Some(Decimal::ONE),
                    1_500,
                )
                .await
                .unwrap();
            assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 1);
            let status = wait_profit(&registry, &run).await;
            assert_eq!(status.observation.status, "unavailable", "{status:?}");
            assert!(status.observation.net_profit.is_none());
            assert!(status.trigger.is_none());
            assert_ne!(
                status.lifecycle,
                PreparedStrategyLifecycle::RiskExitRequested
            );
        }
    }

    #[tokio::test]
    async fn profit_protection_background_latches_only_after_fresh_funding_and_durable_exact_snapshot()
     {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = profit_strategy_with_fills(dir.path(), &gateway, "pmon0004", 1).await;
        let run = strategy.run_id().clone();
        let paths = strategy.paths().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                None,
                Some(Decimal::new(1, 2)),
                1_500,
            )
            .await
            .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 1);
        let status = wait_profit(&registry, &run).await;
        assert_eq!(status.observation.status, "triggered", "{status:?}");
        assert_eq!(
            status.lifecycle,
            PreparedStrategyLifecycle::RiskExitRequested
        );
        assert!(status.trigger.as_ref().unwrap().net_profit < Decimal::ZERO);
        assert_eq!(gateway.profit_calls.load(Ordering::Acquire), 2);
        let persisted = FileStrategyStateStore::load(paths.state()).unwrap();
        assert_eq!(
            persisted.snapshot().profit_protection_trigger,
            status.trigger
        );
        assert_eq!(
            persisted.snapshot().lifecycle,
            StrategyLifecycle::RiskExitRequested
        );
        assert_eq!(registry.spawn_profit_protection_checks(2_001).await, 0);
    }

    #[tokio::test]
    async fn profit_protection_background_rejects_pending_fees_without_account_or_funding_reads() {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(dir.path(), &gateway, "pmon0005", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                None,
                Some(Decimal::new(1, 2)),
                1_500,
            )
            .await
            .unwrap();
        assert_eq!(registry.spawn_profit_protection_checks(2_000).await, 1);
        let status = wait_profit(&registry, &run).await;
        assert_eq!(status.observation.status, "unavailable");
        assert!(status.trigger.is_none());
        assert_eq!(gateway.profit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn profit_protection_admission_rotates_past_always_due_early_runs() {
        let source = (1..=5)
            .map(|n| (StrategyRunId::parse(format!("fair000{n}")).unwrap(), ()))
            .collect::<Vec<_>>();
        let mut cursor = None;
        let mut visited = Vec::new();
        for _ in 0..3 {
            let mut slots = source.clone();
            rotate_profit_slots(&mut slots, cursor.as_ref());
            for (id, _) in slots.into_iter().take(2) {
                visited.push(id.clone());
                cursor = Some(id);
            }
        }
        assert_eq!(
            visited
                .iter()
                .map(StrategyRunId::as_str)
                .collect::<Vec<_>>(),
            vec![
                "fair0001", "fair0002", "fair0003", "fair0004", "fair0005", "fair0001"
            ]
        );
    }

    #[test]
    fn profit_protection_evidence_deadline_includes_elapsed_request_time_and_future_cutoffs() {
        for (cutoff, decision, expected) in [
            (1_000, 6_000, true),
            (1_000, 6_001, false),
            (6_001, 6_000, false),
        ] {
            assert_eq!(profit_evidence_fresh(cutoff, decision), expected);
        }
    }

    #[tokio::test]
    async fn profit_protection_recaptures_accounting_after_funding_before_short_shadow_fence() {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = profit_strategy(dir.path(), &gateway, "pmon0006").await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                Some(Decimal::TEN),
                None,
                1_500,
            )
            .await
            .unwrap();
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let mut captured = slot
            .strategy
            .lock()
            .await
            .active_runtime()
            .unwrap()
            .runtime()
            .machine()
            .store()
            .snapshot()
            .clone();
        captured.revision -= 1;
        check_profit_protection(Arc::clone(&slot), gateway, Arc::new(captured), 10_000).await;
        let status = registry
            .profit_protection_status(Exchange::Binance, "MUUSDT", &run)
            .await
            .unwrap();
        assert_eq!(status.observation.status, "verified");
        assert!(status.trigger.is_none());
    }

    #[tokio::test]
    async fn profit_protection_refreshes_stale_noncrossing_funding_and_recovered_cached_errors() {
        for cached_error in [true, false] {
            let dir = tempdir().unwrap();
            let gateway = FeeGateway::new();
            let strategy = profit_strategy(dir.path(), &gateway, "pmon0007").await;
            gateway.profit.lock().unwrap().cached_funding_error = cached_error;
            gateway.profit.lock().unwrap().cached_funding_lag = 6_000;
            let run = strategy.run_id().clone();
            let registry = RuntimeRegistry::new();
            registry.register(strategy).await;
            registry
                .update_profit_protection(
                    Exchange::Binance,
                    "MUUSDT",
                    &run,
                    0,
                    Some(Decimal::TEN),
                    None,
                    1_500,
                )
                .await
                .unwrap();
            registry.spawn_profit_protection_checks(10_000).await;
            let status = wait_profit(&registry, &run).await;
            assert_eq!(status.observation.status, "verified", "{status:?}");
            assert_eq!(gateway.profit_calls.load(Ordering::Acquire), 2);
        }
    }

    #[tokio::test]
    async fn profit_protection_expired_funding_cannot_be_made_fresh_by_a_later_position_read() {
        let dir = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = profit_strategy(dir.path(), &gateway, "pmon0008").await;
        gateway.profit.lock().unwrap().cached_funding_lag = 6_000;
        gateway.profit.lock().unwrap().fresh_funding_lag = 6_000;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        registry
            .update_profit_protection(
                Exchange::Binance,
                "MUUSDT",
                &run,
                0,
                Some(Decimal::TEN),
                None,
                1_500,
            )
            .await
            .unwrap();
        registry.spawn_profit_protection_checks(10_000).await;
        let status = wait_profit(&registry, &run).await;
        assert_eq!(status.observation.status, "unavailable", "{status:?}");
        assert!(status.trigger.is_none());
    }

    #[tokio::test]
    async fn published_query_snapshot_rejects_interrupted_cross_ledger_commit() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "query002", 0, false).await;
        let paths = strategy.paths().clone();
        drop(strategy);
        let mut active = super::super::LeasedFileStrategyRuntime::load(
            gateway,
            paths,
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        )
        .unwrap();
        assert!(committed_active_query_snapshot(&active).is_some());
        let shape = active
            .runtime()
            .machine()
            .store()
            .snapshot()
            .orders
            .values()
            .next()
            .unwrap()
            .shape
            .clone();
        // Simulate a durable intent append followed by failed strategy synchronization.
        active
            .runtime_mut()
            .intent_store_mut()
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("query-foreign-001").unwrap(),
                    Exchange::Binance,
                    shape,
                    1_300,
                )
                .unwrap(),
            )
            .unwrap();
        active
            .runtime()
            .machine()
            .store()
            .snapshot()
            .validate()
            .unwrap();
        assert!(
            committed_active_query_snapshot(&active).is_none(),
            "a valid individual state is not a cross-ledger ownership certificate"
        );
    }

    #[tokio::test]
    async fn published_query_catalog_never_waits_for_trading_and_tracks_committed_stops() {
        let directory = tempdir().unwrap();
        let gateway = FeeGateway::new();
        let strategy = pending_strategy(directory.path(), &gateway, "query001", 1, false).await;
        let run = strategy.run_id().clone();
        let registry = RuntimeRegistry::new();
        registry.register(strategy).await;
        let slot = registry.entries.read().await.get(&run).unwrap().clone();
        let guard = slot.strategy.lock().await;
        let (before, entries) = registry.query_catalog(None).await.unwrap();
        assert_eq!(before.entries().len(), 1);
        assert!(entries[0].advancing);
        let crate::persistence::StrategyCatalogSnapshot::Active(first) = &before.entries()[0]
        else {
            panic!("expected active snapshot");
        };
        assert!(Arc::ptr_eq(
            first,
            &guard
                .active_runtime()
                .unwrap()
                .runtime()
                .machine()
                .store()
                .shared_snapshot()
        ));
        let (unrelated, _) = registry
            .query_catalog(Some((Exchange::Bybit, "MUUSDT")))
            .await
            .unwrap();
        assert!(unrelated.entries().is_empty());
        drop(guard);
        registry.request_stop(&run, 1_500).await.unwrap();
        let (after, entries) = registry.query_catalog(None).await.unwrap();
        assert!(!entries[0].advancing);
        let crate::persistence::StrategyCatalogSnapshot::Active(state) = &after.entries()[0] else {
            panic!("expected active snapshot");
        };
        assert_eq!(state.lifecycle, StrategyLifecycle::StopRequested);
        assert_eq!(first.lifecycle, StrategyLifecycle::DeployingGrid);
        assert!(!Arc::ptr_eq(first, state));
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
        assert_prepared_intent_reservation_blocks_overdue_audit(Exchange::TradeXyz).await;
    }

    #[tokio::test]
    async fn bybit_prepared_intent_reservation_blocks_an_overdue_periodic_audit() {
        assert_prepared_intent_reservation_blocks_overdue_audit(Exchange::Bybit).await;
    }

    async fn assert_prepared_intent_reservation_blocks_overdue_audit(exchange: Exchange) {
        let directory = tempdir().unwrap();
        let mut gateway = FeeGateway::new();
        gateway.exchange = exchange;
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
        // Real split placement retains both reservations until the ACK is durable.
        let acknowledgement = ExecutionWaiter::register(&slot.execution_waiters);
        let reservation = UnlockedPlacementReservation::new(&slot.unlocked_placements);
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
        assert!(registry.periodic_entries().await[0].advancing);
        drop(reservation);
        let entry = registry.periodic_entries().await.remove(0);
        assert!(!entry.advancing);
        // Bybit explicitly offers the overdue audit without waiting to inspect
        // strategy metadata; TradeXyz retains its ordinary admission path.
        assert_eq!(
            entry.kind,
            (exchange != Exchange::Bybit).then_some(PreparedStrategyKind::Active)
        );
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
