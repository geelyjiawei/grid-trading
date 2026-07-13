use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::{CancellationIntent, CancellationState, ClientOrderId, IntentState},
    engine::{
        ArmedStrategyState, CancellationResult, CancellationServiceError, ExecutionAccountingError,
        ExecutionSyncService, ReconciliationError, ReconciliationResult, StrategyBootstrapError,
        StrategyLifecycle, StrategyMachine, StrategyMachineError, StrategyOrderPurpose,
        StrategyOrderTracking, StrategyStateError, StrategyStateStore, StrategyStoreError,
        StrategyTransition, SubmissionError, SubmissionResult, activate_armed_strategy,
        cancel_with, load_strategy_inputs, reconcile_with, resolve_cancellation_with, submit_with,
    },
    exchange::{
        ExecutionSnapshotGateway, HistoricalPriceGateway, InstrumentRulesGateway, LeverageGateway,
        MarketSnapshotGateway, OrderCancellationGateway, OrderLookupGateway, OrderPlacementGateway,
        PositionSnapshotGateway, TradingFeeRateGateway,
    },
    persistence::{
        FileArmedStrategyStateStore, FileOrderIntentStore, FileStrategyStateStore, IntentStore,
        LedgerError, RuntimeLeaseError, StrategyFilePaths, StrategyRuntimeLease,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStage {
    LedgerReconciliation,
    ExecutionAccounting,
    ExchangeInputs,
    PositionReconciliation,
    InstrumentRules,
    RiskExit,
    Stop,
    StrategyFailed,
    SubmissionUnknown,
    SubmissionRejected,
    CancellationPending,
    CancellationUnknown,
    CancellationRejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBlocker {
    pub stage: RuntimeStage,
    pub client_order_id: Option<ClientOrderId>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSubmission {
    pub client_order_id: ClientOrderId,
    pub result: SubmissionResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCancellation {
    pub client_order_id: ClientOrderId,
    pub result: CancellationResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTickReport {
    pub ledger_reconciliations: usize,
    pub execution_syncs: usize,
    pub submissions: Vec<RuntimeSubmission>,
    pub cancellations: Vec<RuntimeCancellation>,
    pub blockers: Vec<RuntimeBlocker>,
}

impl RuntimeTickReport {
    fn new() -> Self {
        Self {
            ledger_reconciliations: 0,
            execution_syncs: 0,
            submissions: Vec::new(),
            cancellations: Vec::new(),
            blockers: Vec::new(),
        }
    }

    pub fn is_blocked(&self) -> bool {
        !self.blockers.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum RuntimeBuildError {
    #[error("market freshness window must be positive")]
    InvalidFreshnessWindow,
    #[error("maximum submissions per tick must be positive")]
    InvalidSubmissionLimit,
    #[error(transparent)]
    ExecutionAccounting(#[from] ExecutionAccountingError),
}

#[derive(Debug, Error)]
pub enum RuntimeTickError {
    #[error("strategy and order-intent ledgers disagree")]
    IntentLedgerMismatch,
    #[error(transparent)]
    Reconciliation(#[from] ReconciliationError),
    #[error(transparent)]
    Strategy(#[from] StrategyMachineError),
    #[error(transparent)]
    Submission(#[from] SubmissionError),
    #[error(transparent)]
    Cancellation(#[from] CancellationServiceError),
    #[error(transparent)]
    State(#[from] StrategyStateError),
}

pub struct StrategyRuntime<G, I, S> {
    gateway: G,
    intent_store: I,
    machine: StrategyMachine<S>,
    execution_sync: ExecutionSyncService,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
    maximum_submissions_per_tick: usize,
}

pub struct LeasedFileStrategyRuntime<G> {
    paths: StrategyFilePaths,
    _lease: StrategyRuntimeLease,
    runtime: StrategyRuntime<G, FileOrderIntentStore, FileStrategyStateStore>,
}

pub struct LeasedFileArmedStrategy {
    paths: StrategyFilePaths,
    lease: StrategyRuntimeLease,
    store: FileArmedStrategyStateStore,
}

impl LeasedFileArmedStrategy {
    pub fn load(paths: StrategyFilePaths) -> Result<Self, FileArmedLoadError> {
        let lease = StrategyRuntimeLease::acquire(paths.lease())?;
        let store = FileArmedStrategyStateStore::load(paths.state())?;
        if &store.snapshot().run_id != paths.run_id() {
            return Err(FileArmedLoadError::RunIdentityMismatch);
        }
        Ok(Self {
            paths,
            lease,
            store,
        })
    }

    pub fn paths(&self) -> &StrategyFilePaths {
        &self.paths
    }

    pub fn snapshot(&self) -> &ArmedStrategyState {
        self.store.snapshot()
    }

    pub fn cancel(&mut self, now_ms: u64) -> Result<(), StrategyStoreError> {
        self.store.cancel(now_ms)
    }

    pub async fn activate<G>(
        self,
        gateway: G,
        now_ms: u64,
        quote_asset: &str,
        maximum_market_age_ms: u64,
        maximum_future_skew_ms: u64,
        maximum_submissions_per_tick: usize,
    ) -> Result<LeasedFileStrategyRuntime<G>, FileArmedActivationError>
    where
        G: TradingFeeRateGateway
            + LeverageGateway
            + PositionSnapshotGateway
            + MarketSnapshotGateway
            + InstrumentRulesGateway,
    {
        validate_runtime_settings(
            quote_asset,
            maximum_market_age_ms,
            maximum_submissions_per_tick,
        )?;
        let intent_store = FileOrderIntentStore::load(self.paths.intents())?;
        if !intent_store.snapshot().intents.is_empty()
            || !intent_store.snapshot().cancellations.is_empty()
        {
            return Err(FileArmedActivationError::IntentLedgerMismatch);
        }
        let active = activate_armed_strategy(
            &gateway,
            self.store.snapshot(),
            now_ms,
            maximum_market_age_ms,
            maximum_future_skew_ms,
        )
        .await?;
        let Self {
            paths,
            lease,
            store,
        } = self;
        let state_store = store.activate_prepared(active)?;
        let runtime = StrategyRuntime::new(
            gateway,
            intent_store,
            StrategyMachine::new(state_store),
            quote_asset,
            maximum_market_age_ms,
            maximum_future_skew_ms,
            maximum_submissions_per_tick,
        )?;
        runtime
            .verify_ledger_ownership()
            .map_err(|_| FileArmedActivationError::IntentLedgerMismatch)?;
        Ok(LeasedFileStrategyRuntime {
            paths,
            _lease: lease,
            runtime,
        })
    }
}

#[derive(Debug, Error)]
pub enum FileArmedLoadError {
    #[error(transparent)]
    Lease(#[from] RuntimeLeaseError),
    #[error(transparent)]
    StrategyState(#[from] StrategyStoreError),
    #[error("armed strategy run identity does not match its file directory")]
    RunIdentityMismatch,
}

#[derive(Debug, Error)]
pub enum FileArmedActivationError {
    #[error(transparent)]
    Bootstrap(#[from] StrategyBootstrapError),
    #[error(transparent)]
    StrategyState(#[from] StrategyStoreError),
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
    #[error(transparent)]
    Runtime(#[from] RuntimeBuildError),
    #[error("armed strategy cannot activate with a non-empty or foreign intent ledger")]
    IntentLedgerMismatch,
}

impl<G> LeasedFileStrategyRuntime<G> {
    pub fn load(
        gateway: G,
        paths: StrategyFilePaths,
        quote_asset: &str,
        maximum_market_age_ms: u64,
        maximum_future_skew_ms: u64,
        maximum_submissions_per_tick: usize,
    ) -> Result<Self, FileRuntimeLoadError> {
        let lease = StrategyRuntimeLease::acquire(paths.lease())?;
        let state_store = FileStrategyStateStore::load(paths.state())?;
        if &state_store.snapshot().run_id != paths.run_id() {
            return Err(FileRuntimeLoadError::RunIdentityMismatch);
        }
        let intent_store = FileOrderIntentStore::load(paths.intents())?;
        let runtime = StrategyRuntime::new(
            gateway,
            intent_store,
            StrategyMachine::new(state_store),
            quote_asset,
            maximum_market_age_ms,
            maximum_future_skew_ms,
            maximum_submissions_per_tick,
        )?;
        runtime
            .verify_ledger_ownership()
            .map_err(|_| FileRuntimeLoadError::IntentLedgerMismatch)?;
        Ok(Self {
            paths,
            _lease: lease,
            runtime,
        })
    }

    pub fn paths(&self) -> &StrategyFilePaths {
        &self.paths
    }

    pub fn runtime(&self) -> &StrategyRuntime<G, FileOrderIntentStore, FileStrategyStateStore> {
        &self.runtime
    }

    pub fn runtime_mut(
        &mut self,
    ) -> &mut StrategyRuntime<G, FileOrderIntentStore, FileStrategyStateStore> {
        &mut self.runtime
    }
}

#[derive(Debug, Error)]
pub enum FileRuntimeLoadError {
    #[error(transparent)]
    Lease(#[from] RuntimeLeaseError),
    #[error(transparent)]
    StrategyState(#[from] StrategyStoreError),
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
    #[error(transparent)]
    Runtime(#[from] RuntimeBuildError),
    #[error("strategy state run identity does not match its file directory")]
    RunIdentityMismatch,
    #[error("strategy and order-intent ledgers disagree")]
    IntentLedgerMismatch,
}

impl<G, I, S> StrategyRuntime<G, I, S>
where
    I: IntentStore,
    S: StrategyStateStore,
{
    pub fn new(
        gateway: G,
        intent_store: I,
        machine: StrategyMachine<S>,
        quote_asset: &str,
        maximum_market_age_ms: u64,
        maximum_future_skew_ms: u64,
        maximum_submissions_per_tick: usize,
    ) -> Result<Self, RuntimeBuildError> {
        validate_runtime_settings(
            quote_asset,
            maximum_market_age_ms,
            maximum_submissions_per_tick,
        )?;
        let execution_sync = ExecutionSyncService::new(quote_asset)?;
        Ok(Self {
            gateway,
            intent_store,
            machine,
            execution_sync,
            maximum_market_age_ms,
            maximum_future_skew_ms,
            maximum_submissions_per_tick,
        })
    }

    pub fn gateway(&self) -> &G {
        &self.gateway
    }

    pub fn intent_store(&self) -> &I {
        &self.intent_store
    }

    pub fn machine(&self) -> &StrategyMachine<S> {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut StrategyMachine<S> {
        &mut self.machine
    }

    pub fn verify_ledger_ownership(&self) -> Result<(), RuntimeTickError> {
        self.validate_ledger_ownership()
    }

    fn validate_ledger_ownership(&self) -> Result<(), RuntimeTickError> {
        let strategy = self.machine.store().snapshot();
        for (client_order_id, intent) in &self.intent_store.snapshot().intents {
            let Some(order) = strategy.orders.get(client_order_id) else {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            };
            if intent.client_order_id != *client_order_id
                || intent.exchange != strategy.exchange
                || intent.shape != order.shape
                || order.tracking == StrategyOrderTracking::Dormant
            {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
        }
        for order in strategy.orders.values() {
            if matches!(order.tracking, StrategyOrderTracking::Intent { .. })
                && self
                    .intent_store
                    .snapshot()
                    .intents
                    .get(&order.client_order_id)
                    .is_none_or(|intent| {
                        intent.exchange != strategy.exchange || intent.shape != order.shape
                    })
            {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
        }
        for (client_order_id, cancellation) in &self.intent_store.snapshot().cancellations {
            let Some(order) = strategy.orders.get(client_order_id) else {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            };
            let Some(intent) = self.intent_store.snapshot().intents.get(client_order_id) else {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            };
            if cancellation.client_order_id != *client_order_id
                || cancellation.exchange != strategy.exchange
                || cancellation.symbol != strategy.symbol
                || cancellation.symbol != order.shape.symbol
                || order.exchange_order_id.as_deref()
                    != Some(cancellation.exchange_order_id.as_str())
                || intent.exchange != cancellation.exchange
                || intent.shape.symbol != cancellation.symbol
                || matches!(
                    order.tracking,
                    StrategyOrderTracking::Dormant | StrategyOrderTracking::Ready
                )
            {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
            if let CancellationState::Resolved { status } = cancellation.state
                && (!matches!(intent.state, IntentState::Terminal { status: order_status } if order_status == status)
                    || !matches!(order.tracking, StrategyOrderTracking::Intent { state: IntentState::Terminal { status: order_status } } if order_status == status))
            {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
        }
        Ok(())
    }
}

fn validate_runtime_settings(
    quote_asset: &str,
    maximum_market_age_ms: u64,
    maximum_submissions_per_tick: usize,
) -> Result<(), RuntimeBuildError> {
    if maximum_market_age_ms == 0 {
        return Err(RuntimeBuildError::InvalidFreshnessWindow);
    }
    if maximum_submissions_per_tick == 0 {
        return Err(RuntimeBuildError::InvalidSubmissionLimit);
    }
    ExecutionSyncService::new(quote_asset)?;
    Ok(())
}

impl<G, I, S> StrategyRuntime<G, I, S>
where
    G: OrderPlacementGateway
        + OrderCancellationGateway
        + OrderLookupGateway
        + ExecutionSnapshotGateway
        + HistoricalPriceGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway
        + PositionSnapshotGateway,
    I: IntentStore,
    S: StrategyStateStore,
{
    pub async fn tick(&mut self, now_ms: u64) -> Result<RuntimeTickReport, RuntimeTickError> {
        self.validate_ledger_ownership()?;
        let mut report = RuntimeTickReport::new();
        let ledger_ids = self
            .intent_store
            .snapshot()
            .intents
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for client_order_id in &ledger_ids {
            let result = reconcile_with(
                &self.gateway,
                &mut self.intent_store,
                client_order_id,
                now_ms,
            )
            .await?;
            report.ledger_reconciliations += 1;
            let intent = self
                .intent_store
                .snapshot()
                .intents
                .get(client_order_id)
                .cloned()
                .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
            let transition = self.machine.synchronize_intent(&intent, now_ms)?;
            if matches!(result, ReconciliationResult::StillUnknown { .. }) {
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::LedgerReconciliation,
                    client_order_id: Some(client_order_id.clone()),
                    message: "order submission remains unknown".into(),
                });
            }
            if matches!(result, ReconciliationResult::OwnershipConflict { .. })
                || matches!(transition, StrategyTransition::Failed { .. })
            {
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::StrategyFailed,
                    client_order_id: Some(client_order_id.clone()),
                    message: "order ownership reconciliation failed the strategy".into(),
                });
            }
        }
        let terminal_cancellations = self
            .intent_store
            .snapshot()
            .cancellations
            .values()
            .filter_map(|cancellation| {
                if matches!(
                    cancellation.state,
                    CancellationState::Resolved { .. } | CancellationState::Rejected { .. }
                ) {
                    return None;
                }
                self.intent_store
                    .snapshot()
                    .intents
                    .get(&cancellation.client_order_id)
                    .and_then(|intent| match intent.state {
                        IntentState::Terminal { status } => {
                            Some((cancellation.client_order_id.clone(), status))
                        }
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();
        for (client_order_id, status) in terminal_cancellations {
            resolve_cancellation_with(&mut self.intent_store, &client_order_id, status, now_ms)?;
        }
        self.validate_ledger_ownership()?;

        let execution_ids = self
            .machine
            .store()
            .snapshot()
            .orders
            .values()
            .filter(|order| order.exchange_order_id.is_some() && !order.terminal_processed)
            .map(|order| order.client_order_id.clone())
            .collect::<Vec<_>>();
        for client_order_id in &execution_ids {
            match self
                .execution_sync
                .synchronize(&self.gateway, &mut self.machine, client_order_id, now_ms)
                .await
            {
                Ok(result) => {
                    report.execution_syncs += 1;
                    if matches!(result.transition, StrategyTransition::Failed { .. }) {
                        report.blockers.push(RuntimeBlocker {
                            stage: RuntimeStage::StrategyFailed,
                            client_order_id: Some(client_order_id.clone()),
                            message: "execution accounting failed the strategy".into(),
                        });
                    }
                }
                Err(error) => report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::ExecutionAccounting,
                    client_order_id: Some(client_order_id.clone()),
                    message: error.to_string(),
                }),
            }
        }
        if report.is_blocked() {
            return Ok(report);
        }

        let (exchange, symbol, lifecycle) = {
            let state = self.machine.store().snapshot();
            (state.exchange, state.symbol.clone(), state.lifecycle)
        };
        if lifecycle == StrategyLifecycle::Failed {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::StrategyFailed,
                client_order_id: None,
                message: "strategy is failed".into(),
            });
            return Ok(report);
        }
        if matches!(
            lifecycle,
            StrategyLifecycle::StopRequested | StrategyLifecycle::RiskExitRequested
        ) {
            return self.drive_exit(report, lifecycle, now_ms).await;
        }

        let inputs = match load_strategy_inputs(
            &self.gateway,
            exchange,
            &symbol,
            now_ms,
            self.maximum_market_age_ms,
            self.maximum_future_skew_ms,
        )
        .await
        {
            Ok(inputs) => inputs,
            Err(error) => {
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::ExchangeInputs,
                    client_order_id: None,
                    message: error.to_string(),
                });
                return Ok(report);
            }
        };
        let rules_transition = self
            .machine
            .reconcile_instrument_rules(&inputs.instrument_rules, now_ms)?;
        if matches!(rules_transition, StrategyTransition::Failed { .. }) {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::InstrumentRules,
                client_order_id: None,
                message: "exchange instrument rules changed".into(),
            });
            return Ok(report);
        }
        let expected_position = self
            .machine
            .store()
            .snapshot()
            .expected_exchange_position()?;
        if inputs.baseline.signed_quantity != expected_position {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: format!(
                    "position snapshot is not yet consistent with execution accounting: expected {expected_position}, actual {}",
                    inputs.baseline.signed_quantity
                ),
            });
            return Ok(report);
        }
        let risk_transition = self
            .machine
            .evaluate_risk_price(inputs.market.mark_price, now_ms)?;
        if matches!(
            risk_transition,
            StrategyTransition::RiskExitRequested { .. }
        ) {
            return self
                .drive_exit(report, StrategyLifecycle::RiskExitRequested, now_ms)
                .await;
        }
        self.machine
            .materialize_replacements(&inputs.instrument_rules, now_ms)?;
        self.submit_ready_orders(&mut report, now_ms).await?;
        self.validate_ledger_ownership()?;
        Ok(report)
    }

    async fn submit_ready_orders(
        &mut self,
        report: &mut RuntimeTickReport,
        now_ms: u64,
    ) -> Result<(), RuntimeTickError> {
        let ready = self.machine.store().snapshot().ready_intents(now_ms)?;
        for intent in ready.into_iter().take(self.maximum_submissions_per_tick) {
            let client_order_id = intent.client_order_id.clone();
            let result = submit_with(&self.gateway, &mut self.intent_store, intent, now_ms).await?;
            let persisted = self
                .intent_store
                .snapshot()
                .intents
                .get(&client_order_id)
                .cloned()
                .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
            let transition = self.machine.synchronize_intent(&persisted, now_ms)?;
            report.submissions.push(RuntimeSubmission {
                client_order_id: client_order_id.clone(),
                result: result.clone(),
            });
            match result {
                SubmissionResult::Accepted { .. } => {}
                SubmissionResult::SubmitUnknown => {
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::SubmissionUnknown,
                        client_order_id: Some(client_order_id),
                        message: "placement outcome is unknown; later orders were not sent".into(),
                    });
                    break;
                }
                SubmissionResult::Rejected => {
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::SubmissionRejected,
                        client_order_id: Some(client_order_id),
                        message: "exchange definitively rejected the order".into(),
                    });
                    break;
                }
            }
            if matches!(transition, StrategyTransition::Failed { .. }) {
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::StrategyFailed,
                    client_order_id: Some(client_order_id),
                    message: "accepted intent failed strategy synchronization".into(),
                });
                break;
            }
        }
        Ok(())
    }

    async fn drive_exit(
        &mut self,
        mut report: RuntimeTickReport,
        lifecycle: StrategyLifecycle,
        now_ms: u64,
    ) -> Result<RuntimeTickReport, RuntimeTickError> {
        let (exchange, symbol, cancellation_targets) = {
            let state = self.machine.store().snapshot();
            let targets = state
                .orders
                .values()
                .filter(|order| {
                    !matches!(order.purpose, StrategyOrderPurpose::RiskClose)
                        && matches!(
                            order.tracking,
                            StrategyOrderTracking::Intent {
                                state: IntentState::Accepted { .. }
                            }
                        )
                })
                .map(|order| {
                    let exchange_order_id = order
                        .exchange_order_id
                        .clone()
                        .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
                    CancellationIntent::prepare(
                        order.client_order_id.clone(),
                        exchange_order_id,
                        state.exchange,
                        state.symbol.clone(),
                        now_ms,
                    )
                    .map_err(CancellationServiceError::from)
                    .map_err(RuntimeTickError::from)
                })
                .collect::<Result<Vec<_>, _>>()?;
            (state.exchange, state.symbol.clone(), targets)
        };

        let mut dispatch_targets = Vec::new();
        for target in &cancellation_targets {
            match self
                .intent_store
                .snapshot()
                .cancellations
                .get(&target.client_order_id)
                .map(|cancellation| cancellation.state.clone())
            {
                Some(CancellationState::Acknowledged) => {
                    report.cancellations.push(RuntimeCancellation {
                        client_order_id: target.client_order_id.clone(),
                        result: CancellationResult::AlreadyAcknowledged,
                    });
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::CancellationPending,
                        client_order_id: Some(target.client_order_id.clone()),
                        message: "cancellation was acknowledged; authoritative terminal status is pending"
                            .into(),
                    });
                }
                Some(CancellationState::Rejected { .. }) => {
                    report.cancellations.push(RuntimeCancellation {
                        client_order_id: target.client_order_id.clone(),
                        result: CancellationResult::Rejected,
                    });
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::CancellationRejected,
                        client_order_id: Some(target.client_order_id.clone()),
                        message:
                            "cancellation request was rejected before authoritative terminal status"
                                .into(),
                    });
                }
                Some(CancellationState::Resolved { .. }) => {
                    return Err(RuntimeTickError::IntentLedgerMismatch);
                }
                Some(CancellationState::Prepared)
                | Some(CancellationState::SubmitUnknown { .. })
                | None => dispatch_targets.push(target.clone()),
            }
        }

        for target in dispatch_targets
            .iter()
            .take(self.maximum_submissions_per_tick)
            .cloned()
        {
            let client_order_id = target.client_order_id.clone();
            let result = cancel_with(&self.gateway, &mut self.intent_store, target, now_ms).await?;
            report.cancellations.push(RuntimeCancellation {
                client_order_id: client_order_id.clone(),
                result: result.clone(),
            });
            let (stage, message) = match result {
                CancellationResult::Acknowledged | CancellationResult::AlreadyAcknowledged => (
                    RuntimeStage::CancellationPending,
                    "cancellation was acknowledged; authoritative terminal status is pending",
                ),
                CancellationResult::SubmitUnknown => (
                    RuntimeStage::CancellationUnknown,
                    "cancellation outcome is unknown; the exact order will be reconciled before retry",
                ),
                CancellationResult::Rejected => (
                    RuntimeStage::CancellationRejected,
                    "cancellation request was rejected before authoritative terminal status",
                ),
                CancellationResult::AlreadyResolved { .. } => (
                    RuntimeStage::StrategyFailed,
                    "resolved cancellation still points to an active strategy order",
                ),
            };
            report.blockers.push(RuntimeBlocker {
                stage,
                client_order_id: Some(client_order_id),
                message: message.into(),
            });
        }
        if dispatch_targets.len() > self.maximum_submissions_per_tick {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::CancellationPending,
                client_order_id: None,
                message: "additional active orders remain queued for cancellation".into(),
            });
        }
        if !cancellation_targets.is_empty() {
            self.validate_ledger_ownership()?;
            return Ok(report);
        }

        let inputs = match load_strategy_inputs(
            &self.gateway,
            exchange,
            &symbol,
            now_ms,
            self.maximum_market_age_ms,
            self.maximum_future_skew_ms,
        )
        .await
        {
            Ok(inputs) => inputs,
            Err(error) => {
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::ExchangeInputs,
                    client_order_id: None,
                    message: error.to_string(),
                });
                return Ok(report);
            }
        };
        let rules_transition = self
            .machine
            .reconcile_instrument_rules(&inputs.instrument_rules, now_ms)?;
        if matches!(rules_transition, StrategyTransition::Failed { .. }) {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::InstrumentRules,
                client_order_id: None,
                message: "exchange instrument rules changed during strategy exit".into(),
            });
            return Ok(report);
        }
        let expected_position = self
            .machine
            .store()
            .snapshot()
            .expected_exchange_position()?;
        if inputs.baseline.signed_quantity != expected_position {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: format!(
                    "exit position snapshot is not yet consistent with execution accounting: expected {expected_position}, actual {}",
                    inputs.baseline.signed_quantity
                ),
            });
            return Ok(report);
        }

        match lifecycle {
            StrategyLifecycle::StopRequested => {
                self.machine.mark_stopped(now_ms)?;
            }
            StrategyLifecycle::RiskExitRequested => {
                let transition = self.machine.prepare_risk_close(
                    inputs.baseline.signed_quantity,
                    &inputs.instrument_rules,
                    now_ms,
                )?;
                if matches!(transition, StrategyTransition::Failed { .. }) {
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::StrategyFailed,
                        client_order_id: None,
                        message: "risk-close preparation failed".into(),
                    });
                    return Ok(report);
                }
                self.submit_ready_orders(&mut report, now_ms).await?;
                if self.machine.store().snapshot().lifecycle == StrategyLifecycle::RiskExitRequested
                {
                    report.blockers.push(RuntimeBlocker {
                        stage: RuntimeStage::RiskExit,
                        client_order_id: None,
                        message: "risk close remains pending authoritative execution".into(),
                    });
                }
            }
            _ => return Err(StrategyStateError::InvalidLifecycleTransition.into()),
        }
        self.validate_ledger_ownership()?;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            Direction, Exchange, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            OrderIntent, OrderKind, OrderShape, OrderSide, PositionSizingMode, QuantityRules,
            TerminalOrderStatus, TimeInForce,
        },
        engine::{
            GridOrderRole, MarketSnapshot, MemoryStrategyStateStore, PositionBaseline,
            StrategyLifecycle, StrategyOrderPurpose, StrategyRunId, StrategyState,
            StrategyStateStore, build_grid_plan,
        },
        exchange::{
            ActiveOrderStatus, AuthoritativeOrder, CancellationAcknowledgement, CancellationError,
            ExchangeMarketSnapshot, ExecutionSnapshotError, HistoricalMinutePrice,
            LeverageAcknowledgement, LeverageError, LookupError, OrderExecutionSnapshot,
            OrderLifecycle, OrderLookup, PlacementAcknowledgement, PlacementError, PositionLeg,
            PositionSide, PositionSnapshot, SnapshotError, TradeFill, TradingFeeRates,
        },
        persistence::{
            FileArmedStrategyStateStore, FileOrderIntentStore, FileStrategyStateStore, IntentStore,
            MemoryOrderIntentStore, StrategyFilePaths, StrategyRuntimeLease,
        },
    };

    #[derive(Clone)]
    struct MockGateway {
        state: Arc<Mutex<MockGatewayState>>,
    }

    struct MockGatewayState {
        placement_calls: Vec<OrderIntent>,
        next_placement_error: Option<PlacementError>,
        cancellation_calls: Vec<(ClientOrderId, String)>,
        next_cancellation_error: Option<CancellationError>,
        cancellation_marks_terminal: bool,
        orders: BTreeMap<ClientOrderId, AuthoritativeOrder>,
        executions: BTreeMap<ClientOrderId, OrderExecutionSnapshot>,
        market: ExchangeMarketSnapshot,
        rules: InstrumentRules,
        position_quantity: Decimal,
        position_entry_price: Option<Decimal>,
        market_snapshot_calls: usize,
        rules_snapshot_calls: usize,
        position_snapshot_calls: usize,
        fee_rate_calls: usize,
        leverage_write_calls: usize,
    }

    impl MockGateway {
        fn new(rules: InstrumentRules, observed_at_ms: u64) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockGatewayState {
                    placement_calls: Vec::new(),
                    next_placement_error: None,
                    cancellation_calls: Vec::new(),
                    next_cancellation_error: None,
                    cancellation_marks_terminal: false,
                    orders: BTreeMap::new(),
                    executions: BTreeMap::new(),
                    market: ExchangeMarketSnapshot {
                        exchange: Exchange::Binance,
                        symbol: "MUUSDT".into(),
                        last_price: Decimal::new(1014, 0),
                        mark_price: Decimal::new(1014, 0),
                        observed_at_ms,
                    },
                    rules,
                    position_quantity: Decimal::ZERO,
                    position_entry_price: None,
                    market_snapshot_calls: 0,
                    rules_snapshot_calls: 0,
                    position_snapshot_calls: 0,
                    fee_rate_calls: 0,
                    leverage_write_calls: 0,
                })),
            }
        }

        fn placement_call_count(&self) -> usize {
            self.state.lock().unwrap().placement_calls.len()
        }

        fn placement_ids(&self) -> Vec<ClientOrderId> {
            self.state
                .lock()
                .unwrap()
                .placement_calls
                .iter()
                .map(|intent| intent.client_order_id.clone())
                .collect()
        }

        fn fail_next_placement(&self, error: PlacementError) {
            self.state.lock().unwrap().next_placement_error = Some(error);
        }

        fn cancellation_call_count(&self) -> usize {
            self.state.lock().unwrap().cancellation_calls.len()
        }

        fn market_snapshot_call_count(&self) -> usize {
            self.state.lock().unwrap().market_snapshot_calls
        }

        fn account_preflight_call_count(&self) -> usize {
            let state = self.state.lock().unwrap();
            state.position_snapshot_calls + state.fee_rate_calls + state.leverage_write_calls
        }

        fn all_bootstrap_call_count(&self) -> usize {
            let state = self.state.lock().unwrap();
            state.market_snapshot_calls
                + state.rules_snapshot_calls
                + state.position_snapshot_calls
                + state.fee_rate_calls
                + state.leverage_write_calls
        }

        fn cancellation_ids(&self) -> Vec<ClientOrderId> {
            self.state
                .lock()
                .unwrap()
                .cancellation_calls
                .iter()
                .map(|(client_order_id, _)| client_order_id.clone())
                .collect()
        }

        fn fail_next_cancellation(&self, error: CancellationError) {
            self.state.lock().unwrap().next_cancellation_error = Some(error);
        }

        fn set_cancellation_marks_terminal(&self, enabled: bool) {
            self.state.lock().unwrap().cancellation_marks_terminal = enabled;
        }

        fn mark_order_cancelled(&self, client_order_id: &ClientOrderId) {
            let mut state = self.state.lock().unwrap();
            let order = state
                .orders
                .get_mut(client_order_id)
                .expect("order must have been placed");
            order.lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled);
            let order = order.clone();
            let execution = state
                .executions
                .get_mut(client_order_id)
                .expect("placed order must have an execution snapshot");
            execution.order = order;
            execution.update_time_ms = execution.update_time_ms.max(1_150);
        }

        fn hide_order_from_lookup(&self, client_order_id: &ClientOrderId) {
            self.state.lock().unwrap().orders.remove(client_order_id);
        }

        fn set_rules(&self, rules: InstrumentRules) {
            self.state.lock().unwrap().rules = rules;
        }

        fn set_market_price(&self, price: Decimal, observed_at_ms: u64) {
            let mut state = self.state.lock().unwrap();
            state.market.last_price = price;
            state.market.mark_price = price;
            state.market.observed_at_ms = observed_at_ms;
        }

        fn set_position(&self, quantity: Decimal, entry_price: Option<Decimal>) {
            let mut state = self.state.lock().unwrap();
            state.position_quantity = quantity;
            state.position_entry_price = entry_price;
        }

        fn fill_order(&self, client_order_id: &ClientOrderId, price: Decimal, fee: Decimal) {
            let mut state = self.state.lock().unwrap();
            let previous = state
                .executions
                .get(client_order_id)
                .cloned()
                .expect("placed order must have an execution snapshot");
            let order = state
                .orders
                .get_mut(client_order_id)
                .expect("order must have been placed");
            order.lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Filled);
            let order = order.clone();
            let quantity = order.shape.quantity;
            let quote_quantity = quantity * price;
            let exchange_order_id = order.exchange_order_id.clone();
            let trade_time_ms = previous.update_time_ms + 1;
            let trade = TradeFill {
                trade_id: "1".into(),
                exchange_order_id,
                symbol: order.shape.symbol.clone(),
                side: order.shape.side,
                price,
                quantity,
                quote_quantity,
                raw_commission: fee,
                commission_cost: fee,
                commission_asset: "USDT".into(),
                realized_profit: Decimal::ZERO,
                is_maker: true,
                trade_time_ms,
            };
            state.executions.insert(
                client_order_id.clone(),
                OrderExecutionSnapshot {
                    order,
                    cumulative_quantity: quantity,
                    cumulative_quote: quote_quantity,
                    fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
                    trades: vec![trade],
                    order_time_ms: previous.order_time_ms,
                    update_time_ms: trade_time_ms,
                },
            );
        }

        fn partially_fill_order(
            &self,
            client_order_id: &ClientOrderId,
            quantity: Decimal,
            price: Decimal,
            fee: Decimal,
        ) {
            let mut state = self.state.lock().unwrap();
            let previous = state
                .executions
                .get(client_order_id)
                .cloned()
                .expect("placed order must have an execution snapshot");
            let order = state
                .orders
                .get_mut(client_order_id)
                .expect("order must have been placed");
            assert!(quantity > Decimal::ZERO && quantity < order.shape.quantity);
            order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
            let order = order.clone();
            let quote_quantity = quantity * price;
            let trade_time_ms = previous.update_time_ms + 1;
            let trade = TradeFill {
                trade_id: "1".into(),
                exchange_order_id: order.exchange_order_id.clone(),
                symbol: order.shape.symbol.clone(),
                side: order.shape.side,
                price,
                quantity,
                quote_quantity,
                raw_commission: fee,
                commission_cost: fee,
                commission_asset: "USDT".into(),
                realized_profit: Decimal::ZERO,
                is_maker: true,
                trade_time_ms,
            };
            state.executions.insert(
                client_order_id.clone(),
                OrderExecutionSnapshot {
                    order,
                    cumulative_quantity: quantity,
                    cumulative_quote: quote_quantity,
                    fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
                    trades: vec![trade],
                    order_time_ms: previous.order_time_ms,
                    update_time_ms: trade_time_ms,
                },
            );
        }
    }

    #[async_trait]
    impl OrderPlacementGateway for MockGateway {
        async fn place_order(
            &self,
            intent: &OrderIntent,
        ) -> Result<PlacementAcknowledgement, PlacementError> {
            let mut state = self.state.lock().unwrap();
            state.placement_calls.push(intent.clone());
            if let Some(error) = state.next_placement_error.take() {
                return Err(error);
            }
            let exchange_order_id = format!("exchange-{}", state.placement_calls.len());
            let order = AuthoritativeOrder {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: exchange_order_id.clone(),
                exchange: intent.exchange,
                shape: intent.shape.clone(),
                lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
            };
            state
                .orders
                .insert(intent.client_order_id.clone(), order.clone());
            state.executions.insert(
                intent.client_order_id.clone(),
                OrderExecutionSnapshot {
                    order,
                    cumulative_quantity: Decimal::ZERO,
                    cumulative_quote: Decimal::ZERO,
                    fees_by_asset: BTreeMap::new(),
                    trades: Vec::new(),
                    order_time_ms: intent.created_at_ms,
                    update_time_ms: intent.updated_at_ms,
                },
            );
            Ok(PlacementAcknowledgement {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id,
            })
        }
    }

    #[async_trait]
    impl OrderCancellationGateway for MockGateway {
        async fn cancel_order(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
            exchange_order_id: &str,
        ) -> Result<CancellationAcknowledgement, CancellationError> {
            let mut state = self.state.lock().unwrap();
            state
                .cancellation_calls
                .push((client_order_id.clone(), exchange_order_id.to_owned()));
            if let Some(error) = state.next_cancellation_error.take() {
                return Err(error);
            }
            let order = state
                .orders
                .get(client_order_id)
                .filter(|order| order.exchange_order_id == exchange_order_id)
                .cloned()
                .ok_or_else(|| CancellationError::Unknown {
                    message: "order is not visible".into(),
                })?;
            if state.cancellation_marks_terminal {
                let mut cancelled = order.clone();
                cancelled.lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled);
                state
                    .orders
                    .insert(client_order_id.clone(), cancelled.clone());
                let execution = state
                    .executions
                    .get_mut(client_order_id)
                    .expect("placed order must have an execution snapshot");
                execution.order = cancelled;
                execution.update_time_ms = execution.update_time_ms.max(1_150);
            }
            Ok(CancellationAcknowledgement {
                client_order_id: client_order_id.clone(),
                exchange_order_id: exchange_order_id.to_owned(),
            })
        }
    }

    #[async_trait]
    impl OrderLookupGateway for MockGateway {
        async fn lookup_order_by_client_id(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
        ) -> Result<OrderLookup, LookupError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .orders
                .get(client_order_id)
                .cloned()
                .map(OrderLookup::Found)
                .unwrap_or(OrderLookup::NotFound))
        }
    }

    #[async_trait]
    impl ExecutionSnapshotGateway for MockGateway {
        async fn execution_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
            _exchange_order_id: &str,
        ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
            self.state
                .lock()
                .unwrap()
                .executions
                .get(client_order_id)
                .cloned()
                .ok_or_else(|| ExecutionSnapshotError::new("execution is not visible"))
        }
    }

    #[async_trait]
    impl HistoricalPriceGateway for MockGateway {
        async fn historical_minute_open(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            _minute_start_ms: u64,
        ) -> Result<HistoricalMinutePrice, SnapshotError> {
            Err(SnapshotError::new(
                "historical pricing must not be used for quote-asset fees",
            ))
        }
    }

    #[async_trait]
    impl MarketSnapshotGateway for MockGateway {
        async fn market_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.market_snapshot_calls += 1;
            Ok(state.market.clone())
        }
    }

    #[async_trait]
    impl InstrumentRulesGateway for MockGateway {
        async fn instrument_rules(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<InstrumentRules, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.rules_snapshot_calls += 1;
            Ok(state.rules.clone())
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for MockGateway {
        async fn position_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.position_snapshot_calls += 1;
            Ok(PositionSnapshot {
                exchange: Exchange::Binance,
                symbol: "MUUSDT".into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: state.position_quantity,
                    entry_price: state.position_entry_price,
                    mark_price: state.market.mark_price,
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(5),
                }],
            })
        }
    }

    #[async_trait]
    impl TradingFeeRateGateway for MockGateway {
        async fn trading_fee_rates(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<TradingFeeRates, SnapshotError> {
            self.state.lock().unwrap().fee_rate_calls += 1;
            Ok(TradingFeeRates {
                exchange,
                symbol: symbol.into(),
                maker_rate: Decimal::new(2, 4),
                taker_rate: Decimal::new(5, 4),
            })
        }
    }

    #[async_trait]
    impl LeverageGateway for MockGateway {
        async fn set_leverage(
            &self,
            exchange: Exchange,
            symbol: &str,
            leverage: u16,
        ) -> Result<LeverageAcknowledgement, LeverageError> {
            self.state.lock().unwrap().leverage_write_calls += 1;
            Ok(LeverageAcknowledgement {
                exchange,
                symbol: symbol.into(),
                leverage,
            })
        }
    }

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::ONE,
            limit_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: Some(Decimal::new(100, 0)),
            },
            market_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: Some(Decimal::new(100, 0)),
            },
            min_notional: Decimal::ONE,
        }
    }

    fn config(stop_loss_price: Option<Decimal>) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction: Direction::Short,
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
            initial_order_price: Some(Decimal::new(1014, 0)),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price,
            take_profit_price: None,
        }
    }

    fn machine(
        config: GridConfig,
        rules: &InstrumentRules,
    ) -> StrategyMachine<MemoryStrategyStateStore> {
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(1014, 0),
                mark_price: Decimal::new(1014, 0),
            },
            rules,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("runtime01").unwrap(),
            config,
            rules.clone(),
            plan,
            PositionBaseline::flat(),
            1_000,
        )
        .unwrap();
        StrategyMachine::new(MemoryStrategyStateStore::new(state))
    }

    fn runtime(
        gateway: MockGateway,
        intent_store: MemoryOrderIntentStore,
        machine: StrategyMachine<MemoryStrategyStateStore>,
    ) -> StrategyRuntime<MockGateway, MemoryOrderIntentStore, MemoryStrategyStateStore> {
        StrategyRuntime::new(gateway, intent_store, machine, "USDT", 10_000, 100, 100).unwrap()
    }

    fn opening_id<S: StrategyStateStore>(machine: &StrategyMachine<S>) -> ClientOrderId {
        machine
            .store()
            .snapshot()
            .orders
            .values()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap()
            .client_order_id
            .clone()
    }

    fn file_state() -> StrategyState {
        machine(config(None), &rules()).store().snapshot().clone()
    }

    fn armed_file_state() -> ArmedStrategyState {
        let mut config = config(None);
        config.trigger_price = Some(Decimal::new(1014, 0));
        ArmedStrategyState::new(
            StrategyRunId::parse("armed001").unwrap(),
            config,
            &MarketSnapshot {
                last_price: Decimal::new(1010, 0),
                mark_price: Decimal::new(1010, 0),
            },
            1_000,
        )
        .unwrap()
    }

    #[test]
    fn leased_file_runtime_has_one_owner_and_releases_on_drop() {
        let directory = tempdir().unwrap();
        let state = file_state();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();

        let first =
            LeasedFileStrategyRuntime::load((), paths.clone(), "USDT", 10_000, 100, 100).unwrap();
        assert_eq!(first.paths(), &paths);
        assert!(matches!(
            LeasedFileStrategyRuntime::load((), paths.clone(), "USDT", 10_000, 100, 100),
            Err(FileRuntimeLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));

        drop(first);
        assert!(LeasedFileStrategyRuntime::load((), paths, "USDT", 10_000, 100, 100).is_ok());
    }

    #[test]
    fn file_runtime_rejects_state_from_another_run_and_releases_lease() {
        let directory = tempdir().unwrap();
        let paths =
            StrategyFilePaths::new(directory.path(), StrategyRunId::parse("OTHER001").unwrap())
                .unwrap();
        FileStrategyStateStore::create(paths.state(), file_state()).unwrap();

        assert!(matches!(
            LeasedFileStrategyRuntime::load((), paths.clone(), "USDT", 10_000, 100, 100),
            Err(FileRuntimeLoadError::RunIdentityMismatch)
        ));
        assert!(StrategyRuntimeLease::acquire(paths.lease()).is_ok());
    }

    #[test]
    fn file_runtime_rejects_foreign_intent_ledger_before_becoming_visible() {
        let directory = tempdir().unwrap();
        let state = file_state();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        intents
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("foreign_1").unwrap(),
                    Exchange::Binance,
                    OrderShape {
                        symbol: "MUUSDT".into(),
                        side: OrderSide::Sell,
                        price: Some(Decimal::new(1015, 0)),
                        quantity: Decimal::new(2, 1),
                        reduce_only: false,
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::Gtc,
                    },
                    1_000,
                )
                .unwrap(),
            )
            .unwrap();

        assert!(matches!(
            LeasedFileStrategyRuntime::load((), paths.clone(), "USDT", 10_000, 100, 100),
            Err(FileRuntimeLoadError::IntentLedgerMismatch)
        ));
        assert!(StrategyRuntimeLease::acquire(paths.lease()).is_ok());
    }

    #[test]
    fn leased_armed_strategy_has_one_owner_and_cancel_is_durable() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();

        let mut first = LeasedFileArmedStrategy::load(paths.clone()).unwrap();
        assert!(matches!(
            LeasedFileArmedStrategy::load(paths.clone()),
            Err(FileArmedLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
        first.cancel(1_001).unwrap();
        drop(first);

        let restored = LeasedFileArmedStrategy::load(paths).unwrap();
        assert_eq!(
            restored.snapshot().lifecycle,
            crate::engine::ArmedStrategyLifecycle::Cancelled
        );
    }

    #[tokio::test]
    async fn trigger_activation_atomically_hands_the_same_lease_to_active_runtime() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        let active = leased
            .activate(gateway.clone(), 1_100, "USDT", 10_000, 100, 100)
            .await
            .unwrap();

        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(active.runtime().machine().store().snapshot().revision, 1);
        assert_eq!(
            active
                .runtime()
                .machine()
                .store()
                .snapshot()
                .config
                .maker_fee_rate,
            Some(Decimal::new(2, 4))
        );
        assert!(FileStrategyStateStore::load(paths.state()).is_ok());
        assert!(matches!(
            LeasedFileStrategyRuntime::load(gateway, paths, "USDT", 10_000, 100, 100),
            Err(FileRuntimeLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
    }

    #[tokio::test]
    async fn unhit_trigger_keeps_armed_file_unchanged_and_releases_lease() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        gateway.set_market_price(Decimal::new(1013, 0), 1_100);

        assert!(matches!(
            leased
                .activate(gateway.clone(), 1_100, "USDT", 10_000, 100, 100)
                .await,
            Err(FileArmedActivationError::Bootstrap(
                StrategyBootstrapError::TriggerNotReached
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.market_snapshot_call_count(), 1);
        assert_eq!(gateway.account_preflight_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths).is_ok());
    }

    #[tokio::test]
    async fn invalid_runtime_settings_block_before_armed_state_transition() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        assert!(matches!(
            leased
                .activate(gateway.clone(), 1_100, "USDT", 0, 100, 100)
                .await,
            Err(FileArmedActivationError::Runtime(
                RuntimeBuildError::InvalidFreshnessWindow
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths).is_ok());
    }

    #[tokio::test]
    async fn non_empty_intent_ledger_blocks_armed_activation_before_exchange_reads() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        intents
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("foreign_1").unwrap(),
                    Exchange::Binance,
                    OrderShape {
                        symbol: "MUUSDT".into(),
                        side: OrderSide::Sell,
                        price: Some(Decimal::new(1015, 0)),
                        quantity: Decimal::new(2, 1),
                        reduce_only: false,
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::Gtc,
                    },
                    1_000,
                )
                .unwrap(),
            )
            .unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        assert!(matches!(
            leased
                .activate(gateway.clone(), 1_100, "USDT", 10_000, 100, 100)
                .await,
            Err(FileArmedActivationError::IntentLedgerMismatch)
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths).is_ok());
    }

    #[tokio::test]
    async fn accepted_opening_is_never_submitted_twice_on_later_ticks() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );

        let first = runtime.tick(1_100).await.unwrap();
        let second = runtime.tick(1_200).await.unwrap();

        assert_eq!(first.submissions.len(), 1);
        assert!(!first.is_blocked());
        assert!(second.submissions.is_empty());
        assert!(!second.is_blocked());
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn unknown_placement_is_reconciled_and_never_resubmitted_when_not_found() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        gateway.fail_next_placement(PlacementError::Unknown {
            message: "connection reset after request body".into(),
        });
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );

        let first = runtime.tick(1_100).await.unwrap();
        let second = runtime.tick(1_200).await.unwrap();

        assert_eq!(first.blockers[0].stage, RuntimeStage::SubmissionUnknown);
        assert_eq!(second.blockers[0].stage, RuntimeStage::LedgerReconciliation);
        assert!(second.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn accepted_exchange_write_with_failed_local_commit_recovers_without_resubmission() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut intent_store = MemoryOrderIntentStore::default();
        intent_store.fail_on_write(2);
        let mut runtime = runtime(gateway.clone(), intent_store, machine(config(None), &rules));

        assert!(matches!(
            runtime.tick(1_100).await,
            Err(RuntimeTickError::Submission(SubmissionError::Persistence(
                _
            )))
        ));
        assert_eq!(gateway.placement_call_count(), 1);
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .values()
                .next()
                .unwrap()
                .state,
            crate::domain::IntentState::Prepared
        ));

        let recovered = runtime.tick(1_200).await.unwrap();
        assert!(!recovered.is_blocked());
        assert!(recovered.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn accepted_intent_with_failed_strategy_commit_recovers_without_resubmission() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut machine = machine(config(None), &rules);
        machine.store_mut().fail_next_write();
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        assert!(matches!(
            runtime.tick(1_100).await,
            Err(RuntimeTickError::Strategy(
                StrategyMachineError::Persistence(_)
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 1);
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .values()
                .next()
                .unwrap()
                .state,
            crate::domain::IntentState::Accepted { .. }
        ));

        let recovered = runtime.tick(1_200).await.unwrap();
        assert!(!recovered.is_blocked());
        assert!(recovered.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn failed_partial_execution_commit_retries_exactly_without_new_orders() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let partial_quantity = opening_quantity / Decimal::new(2, 0);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        gateway.partially_fill_order(
            &opening_id,
            partial_quantity,
            Decimal::new(1014, 0),
            Decimal::new(2, 2),
        );
        gateway.set_position(-partial_quantity, Some(Decimal::new(1014, 0)));
        runtime.machine_mut().store_mut().fail_next_write();

        let failed = runtime.tick(1_200).await.unwrap();
        assert_eq!(failed.blockers[0].stage, RuntimeStage::ExecutionAccounting);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            Decimal::ZERO
        );
        assert_eq!(gateway.placement_call_count(), 1);

        let recovered = runtime.tick(1_300).await.unwrap();
        assert!(!recovered.is_blocked());
        assert!(recovered.submissions.is_empty());
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -partial_quantity
        );
        assert_eq!(
            runtime.machine().store().snapshot().total_fee,
            Decimal::new(2, 2)
        );
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn position_mismatch_blocks_without_permanent_failure_and_recovers_when_consistent() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        gateway.set_position(-Decimal::ONE, Some(Decimal::new(1014, 0)));
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );

        let report = runtime.tick(1_100).await.unwrap();

        assert_eq!(
            report.blockers[0].stage,
            RuntimeStage::PositionReconciliation
        );
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::AwaitingOpening
        );

        gateway.set_position(Decimal::ZERO, None);
        let recovered = runtime.tick(1_200).await.unwrap();
        assert!(!recovered.is_blocked());
        assert_eq!(recovered.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn fill_between_execution_and_position_reads_blocks_then_reconciles_next_tick() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let partial_quantity = opening_quantity / Decimal::new(2, 0);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();

        gateway.set_position(-partial_quantity, Some(Decimal::new(1014, 0)));
        let raced = runtime.tick(1_200).await.unwrap();
        assert_eq!(
            raced.blockers[0].stage,
            RuntimeStage::PositionReconciliation
        );
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::AwaitingOpening
        );
        assert_eq!(gateway.placement_call_count(), 1);

        gateway.partially_fill_order(
            &opening_id,
            partial_quantity,
            Decimal::new(1014, 0),
            Decimal::new(2, 2),
        );
        let reconciled = runtime.tick(1_300).await.unwrap();
        assert!(!reconciled.is_blocked());
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -partial_quantity
        );
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn changed_instrument_rules_fail_before_any_exchange_write() {
        let original_rules = rules();
        let gateway = MockGateway::new(original_rules.clone(), 1_100);
        let mut changed_rules = original_rules.clone();
        changed_rules.tick_size = Decimal::new(5, 1);
        gateway.set_rules(changed_rules);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &original_rules),
        );

        let report = runtime.tick(1_100).await.unwrap();

        assert_eq!(report.blockers[0].stage, RuntimeStage::InstrumentRules);
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Failed
        );
    }

    #[tokio::test]
    async fn filled_opening_is_accounted_before_initial_grid_is_submitted_exactly_once() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let initial_grid_count = machine
            .store()
            .snapshot()
            .orders
            .values()
            .filter(|order| {
                matches!(
                    order.purpose,
                    StrategyOrderPurpose::InitialGrid {
                        role: GridOrderRole::Profit | GridOrderRole::Add,
                        ..
                    }
                )
            })
            .count();
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        let deployment = runtime.tick(1_200).await.unwrap();

        assert!(!deployment.is_blocked());
        assert_eq!(deployment.submissions.len(), initial_grid_count);
        assert_eq!(gateway.placement_call_count(), initial_grid_count + 1);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -opening_quantity
        );
        assert_eq!(
            runtime.machine().store().snapshot().total_fee,
            Decimal::new(5, 2)
        );

        let next = runtime.tick(1_300).await.unwrap();
        assert!(next.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), initial_grid_count + 1);
        let placed_ids = gateway.placement_ids();
        let unique_ids = placed_ids.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(placed_ids.len(), unique_ids.len());
    }

    #[tokio::test]
    async fn unknown_first_grid_placement_stops_the_remaining_batch_and_never_retries() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        gateway.fail_next_placement(PlacementError::Unknown {
            message: "timeout after sending the first grid order".into(),
        });

        let interrupted = runtime.tick(1_200).await.unwrap();
        assert_eq!(
            interrupted.blockers[0].stage,
            RuntimeStage::SubmissionUnknown
        );
        assert_eq!(interrupted.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), 2);

        let next = runtime.tick(1_300).await.unwrap();
        assert_eq!(next.blockers[0].stage, RuntimeStage::LedgerReconciliation);
        assert!(next.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 2);
    }

    #[tokio::test]
    async fn foreign_intent_ledger_is_rejected_before_any_exchange_write() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut intent_store = MemoryOrderIntentStore::default();
        intent_store
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("foreign_1").unwrap(),
                    Exchange::Binance,
                    OrderShape {
                        symbol: "MUUSDT".into(),
                        side: OrderSide::Sell,
                        price: Some(Decimal::new(1015, 0)),
                        quantity: Decimal::new(2, 1),
                        reduce_only: false,
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::Gtc,
                    },
                    1_000,
                )
                .unwrap(),
            )
            .unwrap();
        let mut runtime = runtime(gateway.clone(), intent_store, machine(config(None), &rules));

        assert!(matches!(
            runtime.tick(1_100).await,
            Err(RuntimeTickError::IntentLedgerMismatch)
        ));
        assert_eq!(gateway.placement_call_count(), 0);
    }

    #[tokio::test]
    async fn unsubmitted_stop_finishes_without_creating_or_cancelling_orders() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut machine = machine(config(None), &rules);
        machine.request_stop(1_050).unwrap();
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        let report = runtime.tick(1_100).await.unwrap();

        assert!(!report.is_blocked());
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.cancellation_call_count(), 0);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Stopped
        );
    }

    #[tokio::test]
    async fn flat_risk_trigger_closes_without_creating_an_order() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        gateway.set_market_price(Decimal::new(1022, 0), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(Some(Decimal::new(1021, 0))), &rules),
        );

        let report = runtime.tick(1_100).await.unwrap();

        assert!(!report.is_blocked());
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Closed
        );
    }

    #[tokio::test]
    async fn stop_waits_for_terminal_cancellation_and_complete_execution_accounting() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        gateway.set_cancellation_marks_terminal(true);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        runtime.machine_mut().request_stop(1_150).unwrap();

        let cancellation = runtime.tick(1_200).await.unwrap();
        assert_eq!(cancellation.cancellations.len(), 1);
        assert_eq!(
            cancellation.blockers[0].stage,
            RuntimeStage::CancellationPending
        );
        assert_eq!(gateway.cancellation_call_count(), 1);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::StopRequested
        );

        let stopped = runtime.tick(1_300).await.unwrap();
        assert!(!stopped.is_blocked(), "{stopped:?}");
        assert_eq!(gateway.cancellation_call_count(), 1);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Stopped
        );
        assert_eq!(
            runtime
                .intent_store()
                .snapshot()
                .cancellations
                .get(&opening_id)
                .unwrap()
                .state,
            CancellationState::Resolved {
                status: TerminalOrderStatus::Cancelled
            }
        );
        assert!(
            runtime
                .machine()
                .store()
                .snapshot()
                .orders
                .get(&opening_id)
                .unwrap()
                .terminal_processed
        );
    }

    #[tokio::test]
    async fn acknowledged_cancellation_is_not_repeated_while_terminal_status_is_delayed() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        runtime.machine_mut().request_stop(1_150).unwrap();

        runtime.tick(1_200).await.unwrap();
        let delayed = runtime.tick(1_300).await.unwrap();
        assert_eq!(gateway.cancellation_call_count(), 1);
        assert_eq!(delayed.cancellations.len(), 1);
        assert_eq!(
            delayed.cancellations[0].result,
            CancellationResult::AlreadyAcknowledged
        );
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::StopRequested
        );

        gateway.mark_order_cancelled(&opening_id);
        runtime.tick(1_400).await.unwrap();
        assert_eq!(gateway.cancellation_call_count(), 1);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Stopped
        );
    }

    #[tokio::test]
    async fn unknown_cancellation_retries_only_after_exact_active_order_reconciliation() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        runtime.machine_mut().request_stop(1_150).unwrap();
        gateway.fail_next_cancellation(CancellationError::Unknown {
            message: "timeout after cancellation request".into(),
        });

        let unknown = runtime.tick(1_200).await.unwrap();
        assert_eq!(unknown.blockers[0].stage, RuntimeStage::CancellationUnknown);
        assert_eq!(gateway.cancellation_call_count(), 1);

        let retried = runtime.tick(1_300).await.unwrap();
        assert_eq!(retried.blockers[0].stage, RuntimeStage::CancellationPending);
        assert_eq!(gateway.cancellation_call_count(), 2);
        gateway.mark_order_cancelled(&opening_id);
        runtime.tick(1_400).await.unwrap();
        assert_eq!(gateway.cancellation_call_count(), 2);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Stopped
        );
    }

    #[tokio::test]
    async fn unknown_cancellation_is_not_retried_when_exact_lookup_is_not_found() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        runtime.machine_mut().request_stop(1_150).unwrap();
        gateway.fail_next_cancellation(CancellationError::Unknown {
            message: "timeout after cancellation request".into(),
        });
        runtime.tick(1_200).await.unwrap();
        assert_eq!(gateway.cancellation_call_count(), 1);

        gateway.hide_order_from_lookup(&opening_id);
        let inconclusive = runtime.tick(1_300).await.unwrap();

        assert!(
            inconclusive
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::LedgerReconciliation)
        );
        assert_eq!(gateway.cancellation_call_count(), 1);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::StopRequested
        );
    }

    #[tokio::test]
    async fn acknowledged_cancellations_never_starve_later_orders_under_a_small_batch_limit() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        runtime.tick(1_200).await.unwrap();
        runtime.machine_mut().request_stop(1_250).unwrap();
        runtime.maximum_submissions_per_tick = 1;

        runtime.tick(1_300).await.unwrap();
        runtime.tick(1_400).await.unwrap();

        let ids = gateway.cancellation_ids();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    #[tokio::test]
    async fn fill_winning_the_cancellation_race_is_accounted_without_a_replacement() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(None), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        runtime.tick(1_200).await.unwrap();
        runtime.machine_mut().request_stop(1_250).unwrap();
        runtime.maximum_submissions_per_tick = 1;
        runtime.tick(1_300).await.unwrap();

        let raced_id = gateway.cancellation_ids()[0].clone();
        let raced_shape = gateway
            .state
            .lock()
            .unwrap()
            .orders
            .get(&raced_id)
            .unwrap()
            .shape
            .clone();
        gateway.fill_order(&raced_id, raced_shape.price.unwrap(), Decimal::new(1, 2));
        let signed_delta = match raced_shape.side {
            OrderSide::Buy => raced_shape.quantity,
            OrderSide::Sell => -raced_shape.quantity,
        };
        let expected_position = -opening_quantity + signed_delta;
        gateway.set_position(expected_position, Some(Decimal::new(1014, 0)));
        gateway.set_cancellation_marks_terminal(true);
        runtime.maximum_submissions_per_tick = 100;

        let race_reconciliation = runtime.tick(1_400).await.unwrap();
        assert!(
            !race_reconciliation
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::StrategyFailed),
            "{race_reconciliation:?}; failure={:?}",
            runtime.machine().store().snapshot().failure
        );
        let stopped = runtime.tick(1_500).await.unwrap();

        assert!(!stopped.is_blocked(), "{stopped:?}");
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Stopped
        );
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            expected_position
        );
        assert!(
            runtime
                .machine()
                .store()
                .snapshot()
                .replacement_obligations
                .is_empty()
        );
        assert_eq!(
            runtime
                .intent_store()
                .snapshot()
                .cancellations
                .get(&raced_id)
                .unwrap()
                .state,
            CancellationState::Resolved {
                status: TerminalOrderStatus::Filled
            }
        );
    }

    #[tokio::test]
    async fn risk_exit_cancels_grid_then_submits_and_accounts_exact_reduce_only_close() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let machine = machine(config(Some(Decimal::new(1021, 0))), &rules);
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        let initial_grid_count = machine
            .store()
            .snapshot()
            .orders
            .values()
            .filter(|order| matches!(order.purpose, StrategyOrderPurpose::InitialGrid { .. }))
            .count();
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);
        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        runtime.tick(1_200).await.unwrap();
        assert_eq!(gateway.placement_call_count(), initial_grid_count + 1);

        gateway.set_cancellation_marks_terminal(true);
        gateway.set_market_price(Decimal::new(1022, 0), 1_300);
        let cancelling = runtime.tick(1_300).await.unwrap();
        assert_eq!(cancelling.cancellations.len(), initial_grid_count);
        assert_eq!(gateway.cancellation_call_count(), initial_grid_count);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::RiskExitRequested
        );

        let closing = runtime.tick(1_400).await.unwrap();
        assert_eq!(closing.submissions.len(), 1);
        let close_intent = gateway
            .state
            .lock()
            .unwrap()
            .placement_calls
            .last()
            .unwrap()
            .clone();
        assert_eq!(close_intent.shape.side, OrderSide::Buy);
        assert_eq!(close_intent.shape.quantity, opening_quantity);
        assert!(close_intent.shape.reduce_only);
        assert_eq!(close_intent.shape.kind, OrderKind::Market);
        assert_eq!(close_intent.shape.price, None);
        assert_eq!(gateway.cancellation_call_count(), initial_grid_count);

        gateway.fill_order(
            &close_intent.client_order_id,
            Decimal::new(1022, 0),
            Decimal::new(5, 2),
        );
        gateway.set_position(Decimal::ZERO, None);
        let closed = runtime.tick(1_500).await.unwrap();
        assert!(
            !closed
                .submissions
                .iter()
                .any(|submission| submission.client_order_id != close_intent.client_order_id)
        );
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Closed
        );
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            Decimal::ZERO
        );
    }
}
