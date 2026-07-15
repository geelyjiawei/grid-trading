use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::{
        CancellationIntent, CancellationState, ClientOrderId, Exchange, GridConfig, IntentState,
        TerminalOrderStatus,
    },
    engine::{
        ArmedStrategyLifecycle, ArmedStrategyState, CancellationResult, CancellationServiceError,
        ExecutionAccountingError, ExecutionSyncService, ReconciliationError, ReconciliationResult,
        StrategyBootstrapError, StrategyLifecycle, StrategyMachine, StrategyMachineError,
        StrategyOrderPurpose, StrategyOrderRecord, StrategyOrderTracking, StrategyRunId,
        StrategyState, StrategyStateError, StrategyStateStore, StrategyStoreError,
        StrategyTransition, SubmissionError, SubmissionResult, activate_armed_strategy,
        cancel_with, load_strategy_inputs, prepare_new_strategy, reconcile_with,
        resolve_cancellation_with, submit_with,
    },
    exchange::{
        ActiveOrderStatus, ExchangeIdentityGateway, ExecutionSnapshotGateway,
        HistoricalPriceGateway, InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway,
        OpenOrderExecutionProgress, OrderCancellationGateway, OrderLifecycle, OrderLookupGateway,
        OrderPlacementGateway, PositionSnapshotGateway, TradingFeeRateGateway,
    },
    persistence::{
        FileArmedStrategyStateStore, FileOrderIntentStore, FilePreparedStrategyStore,
        FileStrategyStateStore, IntentStore, LedgerError, LedgerSnapshot, RuntimeLeaseError,
        StrategyFilePathError, StrategyFilePaths, StrategyRuntimeLease,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSettings {
    quote_asset: String,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
    maximum_submissions_per_tick: usize,
}

impl RuntimeSettings {
    pub fn new(
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
        Ok(Self {
            quote_asset: quote_asset.to_owned(),
            maximum_market_age_ms,
            maximum_future_skew_ms,
            maximum_submissions_per_tick,
        })
    }

    pub fn quote_asset(&self) -> &str {
        &self.quote_asset
    }

    pub fn maximum_market_age_ms(&self) -> u64 {
        self.maximum_market_age_ms
    }

    pub fn maximum_future_skew_ms(&self) -> u64 {
        self.maximum_future_skew_ms
    }

    pub fn maximum_submissions_per_tick(&self) -> usize {
        self.maximum_submissions_per_tick
    }
}

#[derive(Debug, Error)]
pub enum RuntimeTickError {
    #[error("strategy and order-intent ledgers disagree")]
    IntentLedgerMismatch,
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
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

enum PreparedLeasedFileStrategyInner<G> {
    Armed {
        strategy: Box<LeasedFileArmedStrategy>,
        gateway: Box<G>,
    },
    Active(Box<LeasedFileStrategyRuntime<G>>),
}

pub struct PreparedLeasedFileStrategy<G> {
    inner: Option<PreparedLeasedFileStrategyInner<G>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedStrategyKind {
    Armed,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedStrategyLifecycle {
    WaitingTrigger,
    Cancelled,
    AwaitingOpening,
    DeployingGrid,
    Running,
    RiskExitRequested,
    StopRequested,
    Stopped,
    Failed,
    Closed,
}

impl PreparedStrategyLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Stopped | Self::Closed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedStrategyStopOutcome {
    ArmedCancelled,
    Active(StrategyTransition),
}

#[derive(Debug, Error)]
pub enum PreparedStrategyStopError {
    #[error("failed to persist armed strategy cancellation: {0}")]
    Armed(StrategyStoreError),
    #[error("failed to persist active strategy stop request: {0}")]
    Active(StrategyMachineError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedStrategyStep {
    WaitingForTrigger,
    Activated,
    Active(RuntimeTickReport),
}

#[derive(Debug, Error)]
pub enum PreparedStrategyStepError {
    #[error(transparent)]
    Activation(#[from] FileArmedActivationError),
    #[error(transparent)]
    Runtime(#[from] RuntimeTickError),
}

impl<G> PreparedLeasedFileStrategy<G> {
    fn armed(strategy: LeasedFileArmedStrategy, gateway: G) -> Self {
        Self {
            inner: Some(PreparedLeasedFileStrategyInner::Armed {
                strategy: Box::new(strategy),
                gateway: Box::new(gateway),
            }),
        }
    }

    fn active(runtime: LeasedFileStrategyRuntime<G>) -> Self {
        Self {
            inner: Some(PreparedLeasedFileStrategyInner::Active(Box::new(runtime))),
        }
    }

    pub fn paths(&self) -> &StrategyFilePaths {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => strategy.paths(),
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => runtime.paths(),
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn run_id(&self) -> &StrategyRunId {
        self.paths().run_id()
    }

    pub fn kind(&self) -> PreparedStrategyKind {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { .. }) => PreparedStrategyKind::Armed,
            Some(PreparedLeasedFileStrategyInner::Active(_)) => PreparedStrategyKind::Active,
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn exchange(&self) -> Exchange {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => {
                strategy.snapshot().exchange
            }
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => {
                runtime.runtime().machine().store().snapshot().exchange
            }
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn symbol(&self) -> &str {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => {
                &strategy.snapshot().symbol
            }
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => {
                &runtime.runtime().machine().store().snapshot().symbol
            }
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn lifecycle(&self) -> PreparedStrategyLifecycle {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => {
                match strategy.snapshot().lifecycle {
                    ArmedStrategyLifecycle::WaitingTrigger => {
                        PreparedStrategyLifecycle::WaitingTrigger
                    }
                    ArmedStrategyLifecycle::Cancelled => PreparedStrategyLifecycle::Cancelled,
                }
            }
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => {
                match runtime.runtime().machine().store().snapshot().lifecycle {
                    StrategyLifecycle::AwaitingOpening => {
                        PreparedStrategyLifecycle::AwaitingOpening
                    }
                    StrategyLifecycle::DeployingGrid => PreparedStrategyLifecycle::DeployingGrid,
                    StrategyLifecycle::Running => PreparedStrategyLifecycle::Running,
                    StrategyLifecycle::RiskExitRequested => {
                        PreparedStrategyLifecycle::RiskExitRequested
                    }
                    StrategyLifecycle::StopRequested => PreparedStrategyLifecycle::StopRequested,
                    StrategyLifecycle::Stopped => PreparedStrategyLifecycle::Stopped,
                    StrategyLifecycle::Failed => PreparedStrategyLifecycle::Failed,
                    StrategyLifecycle::Closed => PreparedStrategyLifecycle::Closed,
                }
            }
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.lifecycle().is_terminal()
    }

    pub fn request_stop(
        &mut self,
        now_ms: u64,
    ) -> Result<PreparedStrategyStopOutcome, PreparedStrategyStopError> {
        match self.inner.as_mut() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => {
                strategy
                    .cancel(now_ms)
                    .map_err(PreparedStrategyStopError::Armed)?;
                Ok(PreparedStrategyStopOutcome::ArmedCancelled)
            }
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => runtime
                .runtime_mut()
                .machine_mut()
                .request_stop(now_ms)
                .map(PreparedStrategyStopOutcome::Active)
                .map_err(PreparedStrategyStopError::Active),
            None => unreachable!("strategy transition is synchronous"),
        }
    }

    pub fn armed_strategy(&self) -> Option<&LeasedFileArmedStrategy> {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, .. }) => Some(strategy),
            Some(PreparedLeasedFileStrategyInner::Active(_)) | None => None,
        }
    }

    pub fn active_runtime(&self) -> Option<&LeasedFileStrategyRuntime<G>> {
        match self.inner.as_ref() {
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => Some(runtime),
            Some(PreparedLeasedFileStrategyInner::Armed { .. }) | None => None,
        }
    }

    pub async fn advance(
        &mut self,
        now_ms: u64,
    ) -> Result<PreparedStrategyStep, PreparedStrategyStepError>
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
        let prepared = match self.inner.as_mut() {
            Some(PreparedLeasedFileStrategyInner::Active(runtime)) => {
                return runtime
                    .runtime_mut()
                    .tick(now_ms)
                    .await
                    .map(PreparedStrategyStep::Active)
                    .map_err(PreparedStrategyStepError::from);
            }
            Some(PreparedLeasedFileStrategyInner::Armed { strategy, gateway }) => {
                match strategy.prepare_activation(gateway.as_ref(), now_ms).await {
                    Ok(prepared) => prepared,
                    Err(FileArmedActivationError::Bootstrap(
                        StrategyBootstrapError::TriggerNotReached,
                    )) => return Ok(PreparedStrategyStep::WaitingForTrigger),
                    Err(error) => return Err(error.into()),
                }
            }
            None => unreachable!("strategy transition is synchronous"),
        };

        let Some(PreparedLeasedFileStrategyInner::Armed { strategy, gateway }) = self.inner.take()
        else {
            unreachable!("only an armed strategy can finish activation")
        };
        match strategy.commit_activation(*gateway, prepared) {
            Ok(runtime) => {
                let ownership = runtime.runtime().verify_ledger_ownership();
                self.inner = Some(PreparedLeasedFileStrategyInner::Active(Box::new(runtime)));
                ownership
                    .map(|()| PreparedStrategyStep::Activated)
                    .map_err(|_| FileArmedActivationError::IntentLedgerMismatch.into())
            }
            Err(failure) => {
                self.inner = Some(PreparedLeasedFileStrategyInner::Armed {
                    strategy: Box::new(failure.strategy),
                    gateway: Box::new(failure.gateway),
                });
                Err(failure.error.into())
            }
        }
    }
}

pub async fn prepare_leased_file_strategy<G>(
    gateway: G,
    root: impl Into<PathBuf>,
    run_id: StrategyRunId,
    config: GridConfig,
    now_ms: u64,
    settings: RuntimeSettings,
) -> Result<PreparedLeasedFileStrategy<G>, FileStrategyStartError>
where
    G: ExchangeIdentityGateway
        + TradingFeeRateGateway
        + LeverageGateway
        + PositionSnapshotGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway,
{
    config
        .validate()
        .map_err(FileStrategyStartError::InvalidConfig)?;
    let expected_exchange = config
        .exchange
        .ok_or(FileStrategyStartError::MissingExchange)?;
    let actual_exchange = gateway.exchange();
    if actual_exchange != expected_exchange {
        return Err(FileStrategyStartError::GatewayMismatch {
            expected: expected_exchange,
            actual: actual_exchange,
        });
    }
    let paths = StrategyFilePaths::new(root, run_id.clone())?;
    let lease = StrategyRuntimeLease::acquire(paths.lease())?;
    if paths
        .state()
        .try_exists()
        .map_err(FileStrategyStartError::InspectPath)?
    {
        return Err(FileStrategyStartError::StateAlreadyExists);
    }
    if paths
        .intents()
        .try_exists()
        .map_err(FileStrategyStartError::InspectPath)?
    {
        return Err(FileStrategyStartError::UnexpectedIntentLedger);
    }

    let prepared = prepare_new_strategy(
        &gateway,
        run_id,
        config,
        now_ms,
        settings.maximum_market_age_ms,
        settings.maximum_future_skew_ms,
    )
    .await?;
    let persisted = FilePreparedStrategyStore::create(paths.state(), prepared)?;
    match persisted {
        FilePreparedStrategyStore::Armed(store) => {
            let strategy = LeasedFileArmedStrategy {
                paths,
                lease,
                store: *store,
                settings,
            };
            Ok(PreparedLeasedFileStrategy::armed(strategy, gateway))
        }
        FilePreparedStrategyStore::Active(store) => {
            let intent_store = FileOrderIntentStore::load(paths.intents())?;
            let runtime = StrategyRuntime::new(
                gateway,
                intent_store,
                StrategyMachine::new(*store),
                &settings.quote_asset,
                settings.maximum_market_age_ms,
                settings.maximum_future_skew_ms,
                settings.maximum_submissions_per_tick,
            )?;
            runtime
                .verify_ledger_ownership()
                .map_err(|_| FileStrategyStartError::IntentLedgerMismatch)?;
            Ok(PreparedLeasedFileStrategy::active(
                LeasedFileStrategyRuntime {
                    paths,
                    _lease: lease,
                    runtime,
                },
            ))
        }
    }
}

pub fn recover_leased_file_strategy<G>(
    gateway: G,
    paths: StrategyFilePaths,
    settings: RuntimeSettings,
) -> Result<PreparedLeasedFileStrategy<G>, FileStrategyRecoveryError>
where
    G: ExchangeIdentityGateway,
{
    claim_leased_file_strategy(paths)?.attach_gateway(gateway, settings)
}

enum LeasedFileStrategyRecoveryInner {
    Armed(Box<FileArmedStrategyStateStore>),
    Active {
        store: Box<FileStrategyStateStore>,
        intents: FileOrderIntentStore,
    },
}

pub struct LeasedFileStrategyRecovery {
    paths: StrategyFilePaths,
    lease: StrategyRuntimeLease,
    inner: LeasedFileStrategyRecoveryInner,
}

impl LeasedFileStrategyRecovery {
    pub fn paths(&self) -> &StrategyFilePaths {
        &self.paths
    }

    pub fn run_id(&self) -> &StrategyRunId {
        self.paths.run_id()
    }

    pub fn exchange(&self) -> Exchange {
        match &self.inner {
            LeasedFileStrategyRecoveryInner::Armed(store) => store.snapshot().exchange,
            LeasedFileStrategyRecoveryInner::Active { store, .. } => store.snapshot().exchange,
        }
    }

    pub fn kind(&self) -> PreparedStrategyKind {
        match self.inner {
            LeasedFileStrategyRecoveryInner::Armed(_) => PreparedStrategyKind::Armed,
            LeasedFileStrategyRecoveryInner::Active { .. } => PreparedStrategyKind::Active,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match &self.inner {
            LeasedFileStrategyRecoveryInner::Armed(store) => {
                store.snapshot().lifecycle == ArmedStrategyLifecycle::Cancelled
            }
            LeasedFileStrategyRecoveryInner::Active { store, .. } => matches!(
                store.snapshot().lifecycle,
                StrategyLifecycle::Stopped | StrategyLifecycle::Closed
            ),
        }
    }

    pub fn attach_gateway<G>(
        self,
        gateway: G,
        settings: RuntimeSettings,
    ) -> Result<PreparedLeasedFileStrategy<G>, FileStrategyRecoveryError>
    where
        G: ExchangeIdentityGateway,
    {
        verify_gateway_identity(&gateway, self.exchange())?;
        match self.inner {
            LeasedFileStrategyRecoveryInner::Armed(store) => Ok(PreparedLeasedFileStrategy::armed(
                LeasedFileArmedStrategy {
                    paths: self.paths,
                    lease: self.lease,
                    store: *store,
                    settings,
                },
                gateway,
            )),
            LeasedFileStrategyRecoveryInner::Active { store, intents } => {
                let runtime = StrategyRuntime::new(
                    gateway,
                    intents,
                    StrategyMachine::new(*store),
                    &settings.quote_asset,
                    settings.maximum_market_age_ms,
                    settings.maximum_future_skew_ms,
                    settings.maximum_submissions_per_tick,
                )?;
                runtime
                    .verify_ledger_ownership()
                    .map_err(|_| FileStrategyRecoveryError::IntentLedgerMismatch)?;
                Ok(PreparedLeasedFileStrategy::active(
                    LeasedFileStrategyRuntime {
                        paths: self.paths,
                        _lease: self.lease,
                        runtime,
                    },
                ))
            }
        }
    }
}

pub fn claim_leased_file_strategy(
    paths: StrategyFilePaths,
) -> Result<LeasedFileStrategyRecovery, FileStrategyRecoveryError> {
    let lease = StrategyRuntimeLease::acquire(paths.lease())?;
    let persisted = FilePreparedStrategyStore::load(paths.state())?;
    let inner = match persisted {
        FilePreparedStrategyStore::Armed(store) => {
            if &store.snapshot().run_id != paths.run_id() {
                return Err(FileStrategyRecoveryError::RunIdentityMismatch);
            }
            match load_empty_intent_store(paths.intents()) {
                Ok(_) => {}
                Err(EmptyIntentLedgerError::Ledger(error)) => {
                    return Err(FileStrategyRecoveryError::IntentLedger(error));
                }
                Err(EmptyIntentLedgerError::NotEmpty) => {
                    return Err(FileStrategyRecoveryError::IntentLedgerMismatch);
                }
            }
            LeasedFileStrategyRecoveryInner::Armed(store)
        }
        FilePreparedStrategyStore::Active(store) => {
            if &store.snapshot().run_id != paths.run_id() {
                return Err(FileStrategyRecoveryError::RunIdentityMismatch);
            }
            let intents = FileOrderIntentStore::load(paths.intents())?;
            validate_cross_ledger_ownership(store.snapshot(), intents.snapshot())
                .map_err(|_| FileStrategyRecoveryError::IntentLedgerMismatch)?;
            LeasedFileStrategyRecoveryInner::Active { store, intents }
        }
    };
    Ok(LeasedFileStrategyRecovery {
        paths,
        lease,
        inner,
    })
}

fn verify_gateway_identity<G>(
    gateway: &G,
    expected: Exchange,
) -> Result<(), FileStrategyRecoveryError>
where
    G: ExchangeIdentityGateway,
{
    let actual = gateway.exchange();
    if actual == expected {
        Ok(())
    } else {
        Err(FileStrategyRecoveryError::GatewayMismatch { expected, actual })
    }
}

#[derive(Debug, Error)]
pub enum FileStrategyRecoveryError {
    #[error("configured gateway is for {actual:?}, but persisted strategy requires {expected:?}")]
    GatewayMismatch {
        expected: Exchange,
        actual: Exchange,
    },
    #[error("persisted strategy run identity does not match its file directory")]
    RunIdentityMismatch,
    #[error("strategy and order-intent ledgers disagree during recovery")]
    IntentLedgerMismatch,
    #[error(transparent)]
    Lease(#[from] RuntimeLeaseError),
    #[error(transparent)]
    StrategyState(#[from] StrategyStoreError),
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
    #[error(transparent)]
    Runtime(#[from] RuntimeBuildError),
}

#[derive(Debug, Error)]
pub enum FileStrategyStartError {
    #[error("grid configuration is invalid: {0}")]
    InvalidConfig(crate::domain::GridConfigError),
    #[error("grid configuration must identify an exchange")]
    MissingExchange,
    #[error("configured gateway is for {actual:?}, but strategy requires {expected:?}")]
    GatewayMismatch {
        expected: crate::domain::Exchange,
        actual: crate::domain::Exchange,
    },
    #[error("strategy state already exists for this run ID")]
    StateAlreadyExists,
    #[error("an intent ledger exists without its strategy state")]
    UnexpectedIntentLedger,
    #[error("failed to inspect strategy files: {0}")]
    InspectPath(std::io::Error),
    #[error("strategy and order-intent ledgers disagree after creation")]
    IntentLedgerMismatch,
    #[error(transparent)]
    Paths(#[from] StrategyFilePathError),
    #[error(transparent)]
    Lease(#[from] RuntimeLeaseError),
    #[error(transparent)]
    Bootstrap(#[from] StrategyBootstrapError),
    #[error(transparent)]
    StrategyState(#[from] StrategyStoreError),
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
    #[error(transparent)]
    Runtime(#[from] RuntimeBuildError),
}

pub struct LeasedFileArmedStrategy {
    paths: StrategyFilePaths,
    lease: StrategyRuntimeLease,
    store: FileArmedStrategyStateStore,
    settings: RuntimeSettings,
}

struct PreparedArmedActivation {
    active: StrategyState,
    intent_store: FileOrderIntentStore,
    execution_sync: ExecutionSyncService,
}

struct ActivationCommitFailure<G> {
    strategy: LeasedFileArmedStrategy,
    gateway: G,
    error: FileArmedActivationError,
}

#[derive(Debug, Error)]
enum EmptyIntentLedgerError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error("armed strategy intent ledger is not empty")]
    NotEmpty,
}

fn load_empty_intent_store(
    path: impl Into<PathBuf>,
) -> Result<FileOrderIntentStore, EmptyIntentLedgerError> {
    let store = FileOrderIntentStore::load(path)?;
    if store.snapshot().intents.is_empty() && store.snapshot().cancellations.is_empty() {
        Ok(store)
    } else {
        Err(EmptyIntentLedgerError::NotEmpty)
    }
}

impl LeasedFileArmedStrategy {
    pub fn load(
        paths: StrategyFilePaths,
        settings: RuntimeSettings,
    ) -> Result<Self, FileArmedLoadError> {
        let lease = StrategyRuntimeLease::acquire(paths.lease())?;
        let store = FileArmedStrategyStateStore::load(paths.state())?;
        if &store.snapshot().run_id != paths.run_id() {
            return Err(FileArmedLoadError::RunIdentityMismatch);
        }
        match load_empty_intent_store(paths.intents()) {
            Ok(_) => {}
            Err(EmptyIntentLedgerError::Ledger(error)) => {
                return Err(FileArmedLoadError::IntentLedger(error));
            }
            Err(EmptyIntentLedgerError::NotEmpty) => {
                return Err(FileArmedLoadError::IntentLedgerMismatch);
            }
        }
        Ok(Self {
            paths,
            lease,
            store,
            settings,
        })
    }

    pub fn paths(&self) -> &StrategyFilePaths {
        &self.paths
    }

    pub fn snapshot(&self) -> &ArmedStrategyState {
        self.store.snapshot()
    }

    pub fn settings(&self) -> &RuntimeSettings {
        &self.settings
    }

    pub fn cancel(&mut self, now_ms: u64) -> Result<(), StrategyStoreError> {
        self.store.cancel(now_ms)
    }

    pub async fn activate<G>(
        self,
        gateway: G,
        now_ms: u64,
    ) -> Result<LeasedFileStrategyRuntime<G>, FileArmedActivationError>
    where
        G: ExchangeIdentityGateway
            + TradingFeeRateGateway
            + LeverageGateway
            + PositionSnapshotGateway
            + MarketSnapshotGateway
            + InstrumentRulesGateway,
    {
        let prepared = self.prepare_activation(&gateway, now_ms).await?;
        match self.commit_activation(gateway, prepared) {
            Ok(runtime) => {
                runtime
                    .runtime()
                    .verify_ledger_ownership()
                    .map_err(|_| FileArmedActivationError::IntentLedgerMismatch)?;
                Ok(runtime)
            }
            Err(failure) => Err(failure.error),
        }
    }

    async fn prepare_activation<G>(
        &self,
        gateway: &G,
        now_ms: u64,
    ) -> Result<PreparedArmedActivation, FileArmedActivationError>
    where
        G: ExchangeIdentityGateway
            + TradingFeeRateGateway
            + LeverageGateway
            + PositionSnapshotGateway
            + MarketSnapshotGateway
            + InstrumentRulesGateway,
    {
        let expected = self.store.snapshot().exchange;
        let actual = gateway.exchange();
        if expected != actual {
            return Err(FileArmedActivationError::GatewayMismatch { expected, actual });
        }
        match load_empty_intent_store(self.paths.intents()) {
            Ok(_) => {}
            Err(EmptyIntentLedgerError::Ledger(error)) => {
                return Err(FileArmedActivationError::IntentLedger(error));
            }
            Err(EmptyIntentLedgerError::NotEmpty) => {
                return Err(FileArmedActivationError::IntentLedgerMismatch);
            }
        }
        let active = activate_armed_strategy(
            gateway,
            self.store.snapshot(),
            now_ms,
            self.settings.maximum_market_age_ms,
            self.settings.maximum_future_skew_ms,
        )
        .await?;
        let intent_store = match load_empty_intent_store(self.paths.intents()) {
            Ok(store) => store,
            Err(EmptyIntentLedgerError::Ledger(error)) => {
                return Err(FileArmedActivationError::IntentLedger(error));
            }
            Err(EmptyIntentLedgerError::NotEmpty) => {
                return Err(FileArmedActivationError::IntentLedgerMismatch);
            }
        };
        let execution_sync = ExecutionSyncService::new(&self.settings.quote_asset)
            .map_err(RuntimeBuildError::from)?;
        Ok(PreparedArmedActivation {
            active,
            intent_store,
            execution_sync,
        })
    }

    fn commit_activation<G>(
        mut self,
        gateway: G,
        prepared: PreparedArmedActivation,
    ) -> Result<LeasedFileStrategyRuntime<G>, Box<ActivationCommitFailure<G>>> {
        let state_store = match self.store.try_activate_prepared(&prepared.active) {
            Ok(store) => store,
            Err(error) => {
                return Err(Box::new(ActivationCommitFailure {
                    strategy: self,
                    gateway,
                    error: FileArmedActivationError::StrategyState(error),
                }));
            }
        };
        let Self {
            paths,
            lease,
            store: _,
            settings,
        } = self;
        let runtime = StrategyRuntime {
            gateway,
            intent_store: prepared.intent_store,
            machine: StrategyMachine::new(state_store),
            execution_sync: prepared.execution_sync,
            maximum_market_age_ms: settings.maximum_market_age_ms,
            maximum_future_skew_ms: settings.maximum_future_skew_ms,
            maximum_submissions_per_tick: settings.maximum_submissions_per_tick,
        };
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
    #[error(transparent)]
    IntentLedger(#[from] LedgerError),
    #[error("armed strategy run identity does not match its file directory")]
    RunIdentityMismatch,
    #[error("armed strategy cannot load with a non-empty or foreign intent ledger")]
    IntentLedgerMismatch,
}

#[derive(Debug, Error)]
pub enum FileArmedActivationError {
    #[error("configured gateway is for {actual:?}, but armed strategy requires {expected:?}")]
    GatewayMismatch {
        expected: Exchange,
        actual: Exchange,
    },
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
        settings: RuntimeSettings,
    ) -> Result<Self, FileRuntimeLoadError>
    where
        G: ExchangeIdentityGateway,
    {
        let lease = StrategyRuntimeLease::acquire(paths.lease())?;
        let state_store = FileStrategyStateStore::load(paths.state())?;
        if &state_store.snapshot().run_id != paths.run_id() {
            return Err(FileRuntimeLoadError::RunIdentityMismatch);
        }
        let expected = state_store.snapshot().exchange;
        let actual = gateway.exchange();
        if expected != actual {
            return Err(FileRuntimeLoadError::GatewayMismatch { expected, actual });
        }
        let intent_store = FileOrderIntentStore::load(paths.intents())?;
        let runtime = StrategyRuntime::new(
            gateway,
            intent_store,
            StrategyMachine::new(state_store),
            &settings.quote_asset,
            settings.maximum_market_age_ms,
            settings.maximum_future_skew_ms,
            settings.maximum_submissions_per_tick,
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
    #[error("configured gateway is for {actual:?}, but persisted strategy requires {expected:?}")]
    GatewayMismatch {
        expected: Exchange,
        actual: Exchange,
    },
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
        validate_cross_ledger_ownership(
            self.machine.store().snapshot(),
            self.intent_store.snapshot(),
        )
    }

    fn converge_accounted_terminal_intents(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<ClientOrderId>, RuntimeTickError> {
        let candidates = self
            .machine
            .store()
            .snapshot()
            .orders
            .values()
            .filter_map(|order| {
                let status = order.terminal_status?;
                let exchange_order_id = order.exchange_order_id.clone()?;
                order.terminal_processed.then_some((
                    order.client_order_id.clone(),
                    status,
                    exchange_order_id,
                ))
            })
            .collect::<Vec<_>>();
        let mut converged = Vec::new();
        for (client_order_id, status, exchange_order_id) in candidates {
            let intent = self
                .intent_store
                .snapshot()
                .intents
                .get(&client_order_id)
                .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
            let target = IntentState::Terminal {
                status,
                exchange_order_id: Some(exchange_order_id.clone()),
            };
            if intent.state == target {
                continue;
            }
            let can_converge = matches!(
                &intent.state,
                IntentState::Accepted {
                    exchange_order_id: accepted_id,
                } if accepted_id == &exchange_order_id
            ) || matches!(
                &intent.state,
                IntentState::Terminal {
                    status: legacy_status,
                    exchange_order_id: None,
                } if *legacy_status == status
            );
            if !can_converge {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
            self.intent_store
                .transition(&client_order_id, target, now_ms)?;
            converged.push(client_order_id);
        }
        Ok(converged)
    }
}

pub(crate) fn validate_cross_ledger_ownership(
    strategy: &StrategyState,
    ledger: &LedgerSnapshot,
) -> Result<(), RuntimeTickError> {
    for (client_order_id, intent) in &ledger.intents {
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
            && ledger
                .intents
                .get(&order.client_order_id)
                .is_none_or(|intent| {
                    intent.exchange != strategy.exchange || intent.shape != order.shape
                })
        {
            return Err(RuntimeTickError::IntentLedgerMismatch);
        }
    }
    for (client_order_id, cancellation) in &ledger.cancellations {
        let Some(order) = strategy.orders.get(client_order_id) else {
            return Err(RuntimeTickError::IntentLedgerMismatch);
        };
        let Some(intent) = ledger.intents.get(client_order_id) else {
            return Err(RuntimeTickError::IntentLedgerMismatch);
        };
        if cancellation.client_order_id != *client_order_id
            || cancellation.exchange != strategy.exchange
            || cancellation.symbol != strategy.symbol
            || cancellation.symbol != order.shape.symbol
            || order.exchange_order_id.as_deref() != Some(cancellation.exchange_order_id.as_str())
            || intent.exchange != cancellation.exchange
            || intent.shape.symbol != cancellation.symbol
            || matches!(
                order.tracking,
                StrategyOrderTracking::Dormant | StrategyOrderTracking::Ready
            )
        {
            return Err(RuntimeTickError::IntentLedgerMismatch);
        }
        if let CancellationState::Resolved { status } = cancellation.state {
            let intent_matches = terminal_resolution_identity_matches(
                &intent.state,
                status,
                &cancellation.exchange_order_id,
            );
            let strategy_matches = match &order.tracking {
                StrategyOrderTracking::Intent { state } => terminal_resolution_identity_matches(
                    state,
                    status,
                    &cancellation.exchange_order_id,
                ),
                _ => false,
            };
            if !intent_matches || !strategy_matches {
                return Err(RuntimeTickError::IntentLedgerMismatch);
            }
        }
    }
    Ok(())
}

fn terminal_resolution_identity_matches(
    state: &IntentState,
    status: TerminalOrderStatus,
    exchange_order_id: &str,
) -> bool {
    match state {
        IntentState::Terminal {
            status: order_status,
            exchange_order_id: recorded_id,
        } if *order_status == status => recorded_id
            .as_deref()
            .is_none_or(|recorded_id| recorded_id == exchange_order_id),
        _ => false,
    }
}

fn index_open_order_progress(
    progress: Vec<OpenOrderExecutionProgress>,
) -> Option<BTreeMap<ClientOrderId, OpenOrderExecutionProgress>> {
    let mut indexed = BTreeMap::new();
    for item in progress {
        let valid_quantity = match item.order.lifecycle {
            OrderLifecycle::Active(ActiveOrderStatus::New) => item.cumulative_quantity.is_zero(),
            OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled) => {
                item.cumulative_quantity > rust_decimal::Decimal::ZERO
                    && item.cumulative_quantity < item.order.shape.quantity
            }
            OrderLifecycle::Terminal(_) => false,
        };
        if !valid_quantity
            || indexed
                .insert(item.order.client_order_id.clone(), item)
                .is_some()
        {
            return None;
        }
    }
    Some(indexed)
}

fn accepted_progress_matches(
    intent: &crate::domain::OrderIntent,
    progress: &OpenOrderExecutionProgress,
) -> bool {
    progress.order.client_order_id == intent.client_order_id
        && progress.order.exchange == intent.exchange
        && progress.order.shape == intent.shape
        && matches!(
            &intent.state,
            IntentState::Accepted { exchange_order_id }
                if exchange_order_id == &progress.order.exchange_order_id
        )
}

fn execution_progress_matches(
    exchange: Exchange,
    order: &StrategyOrderRecord,
    progress: &OpenOrderExecutionProgress,
) -> bool {
    progress.order.client_order_id == order.client_order_id
        && progress.order.exchange == exchange
        && progress.order.shape == order.shape
        && order.exchange_order_id.as_deref() == Some(progress.order.exchange_order_id.as_str())
        && matches!(
            &order.tracking,
            StrategyOrderTracking::Intent {
                state: IntentState::Accepted { exchange_order_id }
            } if exchange_order_id == &progress.order.exchange_order_id
        )
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
        self.converge_accounted_terminal_intents(now_ms)?;
        self.validate_ledger_ownership()?;
        let mut report = RuntimeTickReport::new();
        let (exchange, symbol) = {
            let state = self.machine.store().snapshot();
            (state.exchange, state.symbol.clone())
        };
        let open_progress = match self
            .gateway
            .open_order_execution_progress_snapshot(exchange, &symbol)
            .await
        {
            Ok(Some(progress)) => index_open_order_progress(progress),
            Ok(None) | Err(_) => None,
        };
        let ledger_ids = self
            .intent_store
            .snapshot()
            .intents
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for client_order_id in &ledger_ids {
            let current_intent = self
                .intent_store
                .snapshot()
                .intents
                .get(client_order_id)
                .cloned()
                .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
            let accepted_progress = open_progress
                .as_ref()
                .and_then(|progress| progress.get(client_order_id))
                .filter(|progress| accepted_progress_matches(&current_intent, progress));
            let result = if let Some(progress) = accepted_progress {
                ReconciliationResult::Accepted {
                    exchange_order_id: progress.order.exchange_order_id.clone(),
                }
            } else {
                reconcile_with(
                    &self.gateway,
                    &mut self.intent_store,
                    client_order_id,
                    now_ms,
                )
                .await?
            };
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
                        IntentState::Terminal { status, .. } => {
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
            let unchanged_open_order = {
                let state = self.machine.store().snapshot();
                let order = state
                    .orders
                    .get(client_order_id)
                    .ok_or(RuntimeTickError::IntentLedgerMismatch)?;
                open_progress
                    .as_ref()
                    .and_then(|progress| progress.get(client_order_id))
                    .is_some_and(|progress| {
                        execution_progress_matches(state.exchange, order, progress)
                            && progress.cumulative_quantity == order.cumulative_quantity
                    })
            };
            if unchanged_open_order {
                continue;
            }
            match self
                .execution_sync
                .synchronize(&self.gateway, &mut self.machine, client_order_id, now_ms)
                .await
            {
                Ok(result) => {
                    report.execution_syncs += 1;
                    // A validated execution snapshot proves ownership and current
                    // exchange visibility more strongly than a transiently absent
                    // per-order lookup. Do not let the weaker read stall unrelated
                    // ready grid orders in this tick.
                    report.blockers.retain(|blocker| {
                        blocker.stage != RuntimeStage::LedgerReconciliation
                            || blocker.client_order_id.as_ref() != Some(client_order_id)
                    });
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
        let converged_terminal_ids = self.converge_accounted_terminal_intents(now_ms)?;
        if !converged_terminal_ids.is_empty() {
            report.blockers.retain(|blocker| {
                blocker.stage != RuntimeStage::LedgerReconciliation
                    || blocker
                        .client_order_id
                        .as_ref()
                        .is_none_or(|client_order_id| {
                            !converged_terminal_ids.contains(client_order_id)
                        })
            });
        }
        self.validate_ledger_ownership()?;
        let (exchange, symbol, lifecycle) = {
            let state = self.machine.store().snapshot();
            (state.exchange, state.symbol.clone(), state.lifecycle)
        };
        if lifecycle == StrategyLifecycle::Failed {
            return self.drive_exit(report, lifecycle, now_ms).await;
        }
        if report.is_blocked() {
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
            return self
                .drive_exit(report, StrategyLifecycle::Failed, now_ms)
                .await;
        }
        let expected_position = self
            .machine
            .store()
            .snapshot()
            .expected_exchange_position()?;
        if inputs.position.signed_quantity != expected_position {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: format!(
                    "position snapshot is not yet consistent with execution accounting: expected {expected_position}, actual {}",
                    inputs.position.signed_quantity
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
        let replacement_transition = self
            .machine
            .materialize_replacements(&inputs.instrument_rules, now_ms)?;
        if matches!(replacement_transition, StrategyTransition::Failed { .. }) {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::StrategyFailed,
                client_order_id: None,
                message: "replacement planning failed before order submission".into(),
            });
            self.validate_ledger_ownership()?;
            return self
                .drive_exit(report, StrategyLifecycle::Failed, now_ms)
                .await;
        }
        self.submit_ready_orders(&mut report, now_ms).await?;
        self.validate_ledger_ownership()?;
        if self.machine.store().snapshot().lifecycle == StrategyLifecycle::Failed {
            return self
                .drive_exit(report, StrategyLifecycle::Failed, now_ms)
                .await;
        }
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
                    (lifecycle == StrategyLifecycle::Failed
                        || !matches!(order.purpose, StrategyOrderPurpose::RiskClose))
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

        if lifecycle == StrategyLifecycle::Failed {
            let cleanup = self.machine.mark_failed_stopped(now_ms)?;
            if !report
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::StrategyFailed)
            {
                let message = if matches!(
                    cleanup,
                    StrategyTransition::LifecycleChanged {
                        lifecycle: StrategyLifecycle::Stopped
                    }
                ) {
                    "strategy failed; owned orders are terminal and market ownership was released"
                } else {
                    "strategy failed; cleanup is waiting for every uncertain order to become authoritative terminal"
                };
                report.blockers.push(RuntimeBlocker {
                    stage: RuntimeStage::StrategyFailed,
                    client_order_id: None,
                    message: message.into(),
                });
            }
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
        if inputs.position.signed_quantity != expected_position {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: format!(
                    "exit position snapshot is not yet consistent with execution accounting: expected {expected_position}, actual {}",
                    inputs.position.signed_quantity
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
                    inputs.position.signed_quantity,
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
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use tempfile::tempdir;
    use tokio::sync::{Barrier as AsyncBarrier, Semaphore};

    use super::*;
    use crate::{
        domain::{
            Direction, Exchange, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            OrderIntent, OrderKind, OrderShape, OrderSide, PositionSizingMode, QuantityRules,
            TerminalOrderStatus, TimeInForce,
        },
        engine::{
            GridOrderRole, MarketSnapshot, MemoryStrategyStateStore, PositionBaseline,
            ReplacementObligationKind, RuntimeCoordinator, RuntimeCoordinatorError,
            RuntimeRecoveryError, RuntimeRecoveryProvider, RuntimeRegistration, RuntimeRegistry,
            RuntimeRegistryAdvanceError, RuntimeStartupFailure, StrategyLifecycle,
            StrategyOrderPurpose, StrategyRunId, StrategyState, StrategyStateStore,
            build_grid_plan, recover_discovered_strategies,
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
            MemoryOrderIntentStore, STRATEGY_CATALOG_LEASE_FILE_NAME, StrategyFilePaths,
            StrategyRuntimeLease, discover_strategy_files, load_strategy_catalog,
        },
    };

    #[test]
    fn resolved_cancellation_accepts_only_exact_or_legacy_missing_terminal_identity() {
        let exact = IntentState::Terminal {
            status: TerminalOrderStatus::Cancelled,
            exchange_order_id: Some("exchange-1".into()),
        };
        let legacy = IntentState::Terminal {
            status: TerminalOrderStatus::Cancelled,
            exchange_order_id: None,
        };
        let changed = IntentState::Terminal {
            status: TerminalOrderStatus::Cancelled,
            exchange_order_id: Some("exchange-2".into()),
        };

        assert!(terminal_resolution_identity_matches(
            &exact,
            TerminalOrderStatus::Cancelled,
            "exchange-1",
        ));
        assert!(terminal_resolution_identity_matches(
            &legacy,
            TerminalOrderStatus::Cancelled,
            "exchange-1",
        ));
        assert!(!terminal_resolution_identity_matches(
            &changed,
            TerminalOrderStatus::Cancelled,
            "exchange-1",
        ));
        assert!(!terminal_resolution_identity_matches(
            &exact,
            TerminalOrderStatus::Filled,
            "exchange-1",
        ));
    }

    #[derive(Clone)]
    struct MockGateway {
        exchange: Exchange,
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
        order_lookup_calls: usize,
        execution_snapshot_calls: usize,
        open_progress_enabled: bool,
        open_progress_calls: usize,
        fee_rate_calls: usize,
        leverage_write_calls: usize,
        market_gate: Option<Arc<MarketGate>>,
    }

    struct MarketGate {
        entered: AsyncBarrier,
        release: Semaphore,
    }

    impl MarketGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: AsyncBarrier::new(2),
                release: Semaphore::new(0),
            })
        }

        async fn wait_until_entered(&self) {
            self.entered.wait().await;
        }

        fn release(&self) {
            self.release.add_permits(1);
        }
    }

    impl MockGateway {
        fn new(rules: InstrumentRules, observed_at_ms: u64) -> Self {
            Self {
                exchange: Exchange::Binance,
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
                    order_lookup_calls: 0,
                    execution_snapshot_calls: 0,
                    open_progress_enabled: false,
                    open_progress_calls: 0,
                    fee_rate_calls: 0,
                    leverage_write_calls: 0,
                    market_gate: None,
                })),
            }
        }

        fn with_exchange(mut self, exchange: Exchange) -> Self {
            self.exchange = exchange;
            self
        }

        fn with_symbol(self, symbol: &str) -> Self {
            self.state.lock().unwrap().market.symbol = symbol.to_owned();
            self
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

        fn placement_intents(&self) -> Vec<OrderIntent> {
            self.state.lock().unwrap().placement_calls.clone()
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

        fn enable_open_progress(&self) {
            self.state.lock().unwrap().open_progress_enabled = true;
        }

        fn order_lookup_call_count(&self) -> usize {
            self.state.lock().unwrap().order_lookup_calls
        }

        fn execution_snapshot_call_count(&self) -> usize {
            self.state.lock().unwrap().execution_snapshot_calls
        }

        fn open_progress_call_count(&self) -> usize {
            self.state.lock().unwrap().open_progress_calls
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

        fn block_market_snapshot(&self) -> Arc<MarketGate> {
            let gate = MarketGate::new();
            self.state.lock().unwrap().market_gate = Some(Arc::clone(&gate));
            gate
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
            let trade_id = format!("trade-{exchange_order_id}");
            let trade_time_ms = previous.update_time_ms + 1;
            let trade = TradeFill {
                trade_id,
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
            let trade_id = format!("trade-{}", order.exchange_order_id);
            let trade_time_ms = previous.update_time_ms + 1;
            let trade = TradeFill {
                trade_id,
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

    impl ExchangeIdentityGateway for MockGateway {
        fn exchange(&self) -> Exchange {
            self.exchange
        }
    }

    struct MockRecoveryProvider {
        gateway: MockGateway,
        requests: Arc<Mutex<Vec<(Exchange, StrategyRunId)>>>,
        fail_run: Option<StrategyRunId>,
    }

    impl MockRecoveryProvider {
        fn new(gateway: MockGateway) -> Self {
            Self {
                gateway,
                requests: Arc::new(Mutex::new(Vec::new())),
                fail_run: None,
            }
        }

        fn failing_for(mut self, run_id: StrategyRunId) -> Self {
            self.fail_run = Some(run_id);
            self
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl RuntimeRecoveryProvider for MockRecoveryProvider {
        type Gateway = MockGateway;
        type Error = &'static str;

        fn runtime_for(
            &self,
            exchange: Exchange,
            run_id: &StrategyRunId,
        ) -> Result<(Self::Gateway, RuntimeSettings), Self::Error> {
            self.requests
                .lock()
                .unwrap()
                .push((exchange, run_id.clone()));
            if self.fail_run.as_ref() == Some(run_id) {
                return Err("credentials unavailable");
            }
            Ok((self.gateway.clone(), runtime_settings()))
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
            let mut state = self.state.lock().unwrap();
            state.order_lookup_calls += 1;
            Ok(state
                .orders
                .get(client_order_id)
                .cloned()
                .map(OrderLookup::Found)
                .unwrap_or(OrderLookup::NotFound))
        }
    }

    #[async_trait]
    impl ExecutionSnapshotGateway for MockGateway {
        async fn open_order_execution_progress_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<Option<Vec<OpenOrderExecutionProgress>>, ExecutionSnapshotError> {
            let mut state = self.state.lock().unwrap();
            if !state.open_progress_enabled {
                return Ok(None);
            }
            state.open_progress_calls += 1;
            let progress = state
                .orders
                .values()
                .filter(|order| matches!(order.lifecycle, OrderLifecycle::Active(_)))
                .map(|order| {
                    let cumulative_quantity = state
                        .executions
                        .get(&order.client_order_id)
                        .map_or(Decimal::ZERO, |snapshot| snapshot.cumulative_quantity);
                    OpenOrderExecutionProgress {
                        order: order.clone(),
                        cumulative_quantity,
                    }
                })
                .collect();
            Ok(Some(progress))
        }

        async fn execution_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
            _exchange_order_id: &str,
        ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.execution_snapshot_calls += 1;
            state
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
            let (market, gate) = {
                let mut state = self.state.lock().unwrap();
                state.market_snapshot_calls += 1;
                (state.market.clone(), state.market_gate.clone())
            };
            if let Some(gate) = gate {
                gate.entered.wait().await;
                gate.release
                    .acquire()
                    .await
                    .expect("test market gate must remain open")
                    .forget();
            }
            Ok(market)
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
                exchange: state.market.exchange,
                symbol: state.market.symbol.clone(),
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
        machine_with_baseline(config, rules, PositionBaseline::flat())
    }

    fn machine_with_baseline(
        config: GridConfig,
        rules: &InstrumentRules,
        baseline: PositionBaseline,
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
            baseline,
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

    async fn deploy_running_short_grid(
        runtime: &mut StrategyRuntime<
            MockGateway,
            MemoryOrderIntentStore,
            MemoryStrategyStateStore,
        >,
        gateway: &MockGateway,
    ) -> Decimal {
        let opening_id = opening_id(runtime.machine());
        let opening_quantity = runtime
            .machine()
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;

        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        let deployment = runtime.tick(1_200).await.unwrap();

        assert!(!deployment.is_blocked(), "{deployment:?}");
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Running
        );
        opening_quantity
    }

    fn accepted_add_order<S: StrategyStateStore>(
        machine: &StrategyMachine<S>,
    ) -> (ClientOrderId, OrderShape, u16) {
        machine
            .store()
            .snapshot()
            .orders
            .values()
            .find_map(|order| match (&order.purpose, &order.tracking) {
                (
                    StrategyOrderPurpose::InitialGrid {
                        level_index,
                        role: GridOrderRole::Add,
                    },
                    StrategyOrderTracking::Intent {
                        state: IntentState::Accepted { .. },
                    },
                ) => Some((
                    order.client_order_id.clone(),
                    order.shape.clone(),
                    *level_index,
                )),
                _ => None,
            })
            .expect("running short grid must have an accepted add order")
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

    fn triggered_config() -> GridConfig {
        let mut config = config(None);
        config.trigger_price = Some(Decimal::new(1014, 0));
        config
    }

    fn runtime_settings() -> RuntimeSettings {
        RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap()
    }

    #[tokio::test]
    async fn start_service_persists_active_runtime_without_placing_any_order() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let run_id = StrategyRunId::parse("start001").unwrap();

        let prepared = prepare_leased_file_strategy(
            gateway.clone(),
            directory.path(),
            run_id,
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let runtime = prepared
            .active_runtime()
            .expect("immediate strategy should be active");

        assert_eq!(gateway.placement_call_count(), 0);
        assert!(runtime.paths().state().is_file());
        assert!(
            runtime
                .runtime()
                .intent_store()
                .snapshot()
                .intents
                .is_empty()
        );
        assert!(matches!(
            LeasedFileStrategyRuntime::load(gateway, runtime.paths().clone(), runtime_settings()),
            Err(FileRuntimeLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
    }

    #[tokio::test]
    async fn start_service_arms_with_only_one_public_market_read() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        gateway.set_market_price(Decimal::new(1010, 0), 1_100);
        let run_id = StrategyRunId::parse("start002").unwrap();

        let prepared = prepare_leased_file_strategy(
            gateway.clone(),
            directory.path(),
            run_id,
            triggered_config(),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let strategy = prepared
            .armed_strategy()
            .expect("triggered strategy should be armed");

        assert_eq!(gateway.market_snapshot_call_count(), 1);
        assert_eq!(gateway.account_preflight_call_count(), 0);
        assert_eq!(gateway.placement_call_count(), 0);
        assert!(strategy.paths().state().is_file());
        assert!(matches!(
            LeasedFileArmedStrategy::load(strategy.paths().clone(), runtime_settings()),
            Err(FileArmedLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
    }

    #[tokio::test]
    async fn duplicate_start_is_rejected_before_any_exchange_call() {
        let directory = tempdir().unwrap();
        let first_gateway = MockGateway::new(rules(), 1_100);
        first_gateway.set_market_price(Decimal::new(1010, 0), 1_100);
        let run_id = StrategyRunId::parse("start003").unwrap();
        let first = prepare_leased_file_strategy(
            first_gateway,
            directory.path(),
            run_id.clone(),
            triggered_config(),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        drop(first);
        let duplicate_gateway = MockGateway::new(rules(), 1_100);

        assert!(matches!(
            prepare_leased_file_strategy(
                duplicate_gateway.clone(),
                directory.path(),
                run_id,
                triggered_config(),
                1_100,
                runtime_settings(),
            )
            .await,
            Err(FileStrategyStartError::StateAlreadyExists)
        ));
        assert_eq!(duplicate_gateway.all_bootstrap_call_count(), 0);
        assert_eq!(duplicate_gateway.placement_call_count(), 0);
    }

    #[tokio::test]
    async fn coordinator_serializes_same_market_start_through_persist_and_registration() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let coordinator = Arc::new(RuntimeCoordinator::new(
            directory.path().to_path_buf(),
            runtime_settings(),
        ));
        let left_coordinator = Arc::clone(&coordinator);
        let right_coordinator = Arc::clone(&coordinator);
        let left_gateway = gateway.clone();
        let right_gateway = gateway.clone();

        let (left, right) = tokio::join!(
            left_coordinator.start(left_gateway, config(None), 1_100),
            right_coordinator.start(right_gateway, config(None), 1_100),
        );

        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let failure = if left.is_err() { left } else { right };
        assert!(matches!(
            failure,
            Err(RuntimeCoordinatorError::MarketAlreadyOwned {
                exchange: Exchange::Binance,
                symbol,
                ..
            }) if symbol == "MUUSDT"
        ));
        assert_eq!(coordinator.entries().await.len(), 1);
        assert_eq!(gateway.market_snapshot_call_count(), 1);
        assert_eq!(gateway.placement_call_count(), 0);

        let catalog = load_strategy_catalog(directory.path()).unwrap();
        assert!(catalog.anomalies().is_empty());
        assert_eq!(
            catalog
                .entries()
                .iter()
                .filter(|entry| entry.is_live())
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn separate_coordinators_cannot_create_two_live_runs_in_one_catalog() {
        let directory = tempdir().unwrap();
        let first_gateway = MockGateway::new(rules(), 1_100);
        let second_gateway = MockGateway::new(rules(), 1_100);
        let first_coordinator = Arc::new(RuntimeCoordinator::new(
            directory.path().to_path_buf(),
            runtime_settings(),
        ));
        let second_coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());
        let gate = first_gateway.block_market_snapshot();
        let starting_coordinator = Arc::clone(&first_coordinator);
        let first_start = tokio::spawn(async move {
            starting_coordinator
                .start(first_gateway, config(None), 1_100)
                .await
        });
        gate.wait_until_entered().await;

        let second = second_coordinator
            .start(second_gateway.clone(), config(None), 1_100)
            .await;
        gate.release();
        let first = first_start.await.unwrap();

        assert!(matches!(
            &second,
            Err(RuntimeCoordinatorError::CatalogLease(
                RuntimeLeaseError::AlreadyHeld
            ))
        ));
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(second_gateway.all_bootstrap_call_count(), 0);
        let catalog = load_strategy_catalog(directory.path()).unwrap();
        assert!(catalog.anomalies().is_empty());
        assert_eq!(
            catalog
                .entries()
                .iter()
                .filter(|entry| entry.is_live())
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn simultaneous_separate_coordinator_starts_never_both_win() {
        for attempt in 0..200 {
            let directory = tempdir().unwrap();
            let first_gateway = MockGateway::new(rules(), 1_100);
            let second_gateway = MockGateway::new(rules(), 1_100);
            let first_coordinator =
                RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());
            let second_coordinator =
                RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());

            let (first, second) = tokio::join!(
                first_coordinator.start(first_gateway, config(None), 1_100),
                second_coordinator.start(second_gateway, config(None), 1_100),
            );

            assert_eq!(
                usize::from(first.is_ok()) + usize::from(second.is_ok()),
                1,
                "attempt={attempt} first={first:?} second={second:?}"
            );
        }
    }

    #[tokio::test]
    async fn catalog_lease_blocks_recovery_before_provider_access() {
        let directory = tempdir().unwrap();
        let _catalog_lease =
            StrategyRuntimeLease::acquire(directory.path().join(STRATEGY_CATALOG_LEASE_FILE_NAME))
                .unwrap();
        let provider = MockRecoveryProvider::new(MockGateway::new(rules(), 1_100));
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());

        assert!(matches!(
            coordinator.recover(&provider).await,
            Err(RuntimeRecoveryError::CatalogLease(
                RuntimeLeaseError::AlreadyHeld
            ))
        ));
        assert_eq!(provider.request_count(), 0);
        assert!(coordinator.entries().await.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn catalog_lease_rejects_a_symbolic_link_strategy_root_without_touching_its_target() {
        use std::{fs, os::unix::fs::symlink};

        let directory = tempdir().unwrap();
        let target = directory.path().join("external-strategies");
        let root = directory.path().join("strategies");
        fs::create_dir(&target).unwrap();
        symlink(&target, &root).unwrap();
        let provider = MockRecoveryProvider::new(MockGateway::new(rules(), 1_100));
        let coordinator = RuntimeCoordinator::new(root, runtime_settings());

        assert!(matches!(
            coordinator.recover(&provider).await,
            Err(RuntimeRecoveryError::CatalogLease(
                RuntimeLeaseError::ParentSymbolicLink
            ))
        ));
        assert_eq!(provider.request_count(), 0);
        assert!(!target.join(STRATEGY_CATALOG_LEASE_FILE_NAME).exists());
    }

    #[tokio::test]
    async fn catalog_lease_blocks_stop_without_changing_the_strategy() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());
        let started = coordinator
            .start(gateway.clone(), config(None), 1_100)
            .await
            .unwrap();
        let catalog_lease =
            StrategyRuntimeLease::acquire(directory.path().join(STRATEGY_CATALOG_LEASE_FILE_NAME))
                .unwrap();

        assert!(matches!(
            coordinator
                .request_stop(Exchange::Binance, "MUUSDT", 1_101)
                .await,
            Err(RuntimeCoordinatorError::CatalogLease(
                RuntimeLeaseError::AlreadyHeld
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(coordinator.entries().await.len(), 1);
        assert_eq!(
            coordinator.entries().await[0].lifecycle,
            Some(PreparedStrategyLifecycle::AwaitingOpening)
        );

        drop(catalog_lease);
        let stopped = coordinator
            .request_stop(Exchange::Binance, "MUUSDT", 1_102)
            .await
            .unwrap();
        assert_eq!(stopped.run_id, started.run_id);
        assert!(matches!(
            stopped.outcome,
            PreparedStrategyStopOutcome::Active(StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested,
            })
        ));
    }

    #[tokio::test]
    async fn coordinator_catalog_anomaly_blocks_before_every_exchange_call() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("unexpected-file"), b"do not ignore").unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());

        assert!(matches!(
            coordinator
                .start(gateway.clone(), config(None), 1_100)
                .await,
            Err(RuntimeCoordinatorError::CatalogAnomalies { count: 1 })
        ));
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(gateway.placement_call_count(), 0);
        assert!(coordinator.entries().await.is_empty());
    }

    #[tokio::test]
    async fn coordinator_scheduler_submits_one_opening_and_never_duplicates_it() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());
        let receipt = coordinator
            .start(gateway.clone(), config(None), 1_100)
            .await
            .unwrap();

        assert_eq!(receipt.exchange, Exchange::Binance);
        assert_eq!(receipt.symbol, "MUUSDT");
        assert_eq!(
            receipt.lifecycle,
            PreparedStrategyLifecycle::AwaitingOpening
        );
        assert_eq!(gateway.placement_call_count(), 0);

        let first = coordinator.advance_all(1_200).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].run_id, receipt.run_id);
        assert!(first[0].result.is_ok());
        assert_eq!(gateway.placement_call_count(), 1);

        let second = coordinator.advance_all(1_300).await;
        assert_eq!(second.len(), 1);
        assert!(second[0].result.is_ok());
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coordinator_scheduler_does_not_let_one_slow_market_block_another() {
        let first_directory = tempdir().unwrap();
        let second_directory = tempdir().unwrap();
        let coordinator_directory = tempdir().unwrap();
        let first_gateway = MockGateway::new(rules(), 1_100);
        let second_gateway = MockGateway::new(rules(), 1_100).with_symbol("ALTUSDT");
        let first_run = StrategyRunId::parse("batch001").unwrap();
        let second_run = StrategyRunId::parse("batch002").unwrap();
        let first = prepare_leased_file_strategy(
            first_gateway.clone(),
            first_directory.path(),
            first_run.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let mut second_config = config(None);
        second_config.symbol = "ALTUSDT".into();
        let second = prepare_leased_file_strategy(
            second_gateway.clone(),
            second_directory.path(),
            second_run.clone(),
            second_config,
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let coordinator = Arc::new(RuntimeCoordinator::new(
            coordinator_directory.path().to_path_buf(),
            runtime_settings(),
        ));
        assert!(matches!(
            coordinator.registry().register(first).await,
            RuntimeRegistration::Registered
        ));
        assert!(matches!(
            coordinator.registry().register(second).await,
            RuntimeRegistration::Registered
        ));

        let gate = first_gateway.block_market_snapshot();
        let second_market_calls_before = second_gateway.market_snapshot_call_count();
        let advancing = Arc::clone(&coordinator);
        let batch = tokio::spawn(async move { advancing.advance_all(1_200).await });
        gate.wait_until_entered().await;
        let unrelated_progressed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while second_gateway.market_snapshot_call_count() == second_market_calls_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();

        assert!(
            unrelated_progressed,
            "a slow strategy must not delay an unrelated market's tick"
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let entries = coordinator.entries().await;
                if entries
                    .iter()
                    .find(|entry| entry.run_id == second_run)
                    .is_some_and(|entry| !entry.advancing)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the unrelated strategy should finish its first tick");
        let second_market_calls_after_first = second_gateway.market_snapshot_call_count();
        let overlapping = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coordinator.advance_all(1_250),
        )
        .await
        .expect("a new scheduler batch must not wait for the slow strategy");
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].run_id, second_run);
        assert!(overlapping[0].result.is_ok());
        assert!(second_gateway.market_snapshot_call_count() > second_market_calls_after_first);

        gate.release();
        let results = batch.await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].run_id, first_run);
        assert_eq!(results[1].run_id, second_run);
        assert!(results.iter().all(|result| result.result.is_ok()));
        assert_eq!(first_gateway.placement_call_count(), 1);
        assert_eq!(second_gateway.placement_call_count(), 1);
    }

    #[tokio::test]
    async fn coordinator_stop_before_tick_is_durable_zero_write_and_allows_clean_restart() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());
        let first = coordinator
            .start(gateway.clone(), config(None), 1_100)
            .await
            .unwrap();

        let stopped = coordinator
            .request_stop(Exchange::Binance, "MUUSDT", 1_101)
            .await
            .unwrap();
        assert_eq!(stopped.run_id, first.run_id);
        assert!(matches!(
            stopped.outcome,
            PreparedStrategyStopOutcome::Active(StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested,
            })
        ));

        let reports = coordinator.advance_all(1_102).await;
        assert_eq!(reports.len(), 1);
        assert!(reports[0].result.is_ok());
        assert_eq!(gateway.placement_call_count(), 0);
        assert!(coordinator.entries().await.is_empty());
        let catalog = load_strategy_catalog(directory.path()).unwrap();
        assert!(
            catalog
                .select_live(Exchange::Binance, "MUUSDT")
                .unwrap()
                .is_none()
        );

        let second = coordinator
            .start(gateway.clone(), config(None), 1_103)
            .await
            .unwrap();
        assert_ne!(second.run_id, first.run_id);
        assert_eq!(coordinator.entries().await.len(), 1);
        assert_eq!(gateway.placement_call_count(), 0);
    }

    #[tokio::test]
    async fn coordinator_recovery_skips_terminal_state_without_credentials() {
        let directory = tempdir().unwrap();
        let terminal = armed_file_state().cancelled(1_100).unwrap();
        let run_id = terminal.run_id.clone();
        let paths = StrategyFilePaths::new(directory.path(), run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), terminal).unwrap();
        let provider = MockRecoveryProvider::new(MockGateway::new(rules(), 1_100));
        let coordinator =
            RuntimeCoordinator::new(directory.path().to_path_buf(), runtime_settings());

        let report = coordinator.recover(&provider).await.unwrap();

        assert!(report.registered.is_empty());
        assert_eq!(report.skipped_terminal, vec![run_id]);
        assert!(report.discovery_anomalies.is_empty());
        assert!(report.failures.is_empty());
        assert_eq!(provider.request_count(), 0);
        assert!(coordinator.entries().await.is_empty());
        assert!(StrategyRuntimeLease::acquire(paths.lease()).is_ok());
    }

    #[tokio::test]
    async fn gateway_identity_and_orphan_intent_ledger_fail_before_exchange_calls() {
        let directory = tempdir().unwrap();
        let wrong_gateway = MockGateway::new(rules(), 1_100).with_exchange(Exchange::Aster);
        assert!(matches!(
            prepare_leased_file_strategy(
                wrong_gateway.clone(),
                directory.path(),
                StrategyRunId::parse("start004").unwrap(),
                triggered_config(),
                1_100,
                runtime_settings(),
            )
            .await,
            Err(FileStrategyStartError::GatewayMismatch {
                expected: Exchange::Binance,
                actual: Exchange::Aster
            })
        ));
        assert_eq!(wrong_gateway.all_bootstrap_call_count(), 0);

        let run_id = StrategyRunId::parse("start005").unwrap();
        let paths = StrategyFilePaths::new(directory.path(), run_id.clone()).unwrap();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        intents
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("orphan_01").unwrap(),
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
        let gateway = MockGateway::new(rules(), 1_100);
        assert!(matches!(
            prepare_leased_file_strategy(
                gateway.clone(),
                directory.path(),
                run_id,
                triggered_config(),
                1_100,
                runtime_settings(),
            )
            .await,
            Err(FileStrategyStartError::UnexpectedIntentLedger)
        ));
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_same_run_start_has_one_durable_winner() {
        let directory = tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let gateway = MockGateway::new(rules(), 1_100);
        gateway.set_market_price(Decimal::new(1010, 0), 1_100);
        let run_id = StrategyRunId::parse("start006").unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..12 {
            let gateway = gateway.clone();
            let root = root.clone();
            let run_id = run_id.clone();
            tasks.spawn(async move {
                prepare_leased_file_strategy(
                    gateway,
                    root,
                    run_id,
                    triggered_config(),
                    1_100,
                    runtime_settings(),
                )
                .await
            });
        }

        let mut successes = 0;
        let mut blocked = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(_) => successes += 1,
                Err(FileStrategyStartError::Lease(RuntimeLeaseError::AlreadyHeld))
                | Err(FileStrategyStartError::StateAlreadyExists) => blocked += 1,
                Err(error) => panic!("unexpected concurrent start result: {error}"),
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(blocked, 11);
        assert_eq!(gateway.market_snapshot_call_count(), 1);
        let paths = StrategyFilePaths::new(root, run_id).unwrap();
        assert!(FileArmedStrategyStateStore::load(paths.state()).is_ok());
    }

    #[tokio::test]
    async fn armed_advance_keeps_lease_while_waiting_and_activates_without_placing() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        gateway.set_market_price(Decimal::new(1010, 0), 1_100);
        let mut prepared = prepare_leased_file_strategy(
            gateway.clone(),
            directory.path(),
            StrategyRunId::parse("advance1").unwrap(),
            triggered_config(),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let paths = prepared.paths().clone();

        assert_eq!(
            prepared.advance(1_100).await.unwrap(),
            PreparedStrategyStep::WaitingForTrigger
        );
        assert_eq!(prepared.kind(), PreparedStrategyKind::Armed);
        assert_eq!(gateway.account_preflight_call_count(), 0);
        assert!(matches!(
            LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()),
            Err(FileArmedLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));

        gateway.set_market_price(Decimal::new(1014, 0), 1_200);
        assert_eq!(
            prepared.advance(1_200).await.unwrap(),
            PreparedStrategyStep::Activated
        );
        assert_eq!(prepared.kind(), PreparedStrategyKind::Active);
        assert_eq!(gateway.placement_call_count(), 0);

        let PreparedStrategyStep::Active(report) = prepared.advance(1_300).await.unwrap() else {
            panic!("the first active tick must return an active report");
        };
        assert_eq!(report.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), 1);
    }

    #[test]
    fn recovery_loads_under_one_lease_without_calling_the_exchange() {
        let directory = tempdir().unwrap();
        let state = file_state();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        let claim = claim_leased_file_strategy(paths.clone()).unwrap();

        assert_eq!(claim.run_id(), paths.run_id());
        assert_eq!(claim.exchange(), Exchange::Binance);
        assert_eq!(claim.kind(), PreparedStrategyKind::Active);
        assert!(matches!(
            claim_leased_file_strategy(paths.clone()),
            Err(FileStrategyRecoveryError::Lease(
                RuntimeLeaseError::AlreadyHeld
            ))
        ));
        let recovered = claim
            .attach_gateway(gateway.clone(), runtime_settings())
            .unwrap();

        assert_eq!(recovered.kind(), PreparedStrategyKind::Active);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(gateway.placement_call_count(), 0);
        assert!(matches!(
            recover_leased_file_strategy(gateway, paths, runtime_settings()),
            Err(FileStrategyRecoveryError::Lease(
                RuntimeLeaseError::AlreadyHeld
            ))
        ));
    }

    #[test]
    fn active_claim_rejects_foreign_ledger_before_gateway_selection() {
        let directory = tempdir().unwrap();
        let state = file_state();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        let mut intents = FileOrderIntentStore::load(paths.intents()).unwrap();
        intents
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("claimbad").unwrap(),
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
            claim_leased_file_strategy(paths.clone()),
            Err(FileStrategyRecoveryError::IntentLedgerMismatch)
        ));
        assert!(StrategyRuntimeLease::acquire(paths.lease()).is_ok());
    }

    #[tokio::test]
    async fn startup_recovery_registers_valid_runs_and_reports_each_failed_claim() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        let active = file_state();
        let active_run = active.run_id.clone();
        let active_paths = StrategyFilePaths::new(&root, active_run.clone()).unwrap();
        FileStrategyStateStore::create(active_paths.state(), active).unwrap();
        let armed = armed_file_state();
        let armed_run = armed.run_id.clone();
        let armed_paths = StrategyFilePaths::new(&root, armed_run.clone()).unwrap();
        FileArmedStrategyStateStore::create(armed_paths.state(), armed).unwrap();
        std::fs::create_dir(root.join("bad-name")).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let provider = MockRecoveryProvider::new(gateway.clone());
        let registry = RuntimeRegistry::new();

        let report = recover_discovered_strategies(
            &registry,
            discover_strategy_files(&root).unwrap(),
            &provider,
        )
        .await;

        assert_eq!(report.registered, vec![armed_run.clone()]);
        assert_eq!(report.discovery_anomalies.len(), 1);
        assert!(matches!(
            report.failures.as_slice(),
            [RuntimeStartupFailure::MarketAlreadyOwned {
                run_id,
                exchange: Exchange::Binance,
                symbol,
                owner_run_id,
            }] if run_id == &active_run
                && symbol == "MUUSDT"
                && owner_run_id == &armed_run
        ));
        assert_eq!(provider.request_count(), 2);
        assert_eq!(registry.len().await, 1);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(gateway.placement_call_count(), 0);

        let repeated = recover_discovered_strategies(
            &registry,
            discover_strategy_files(&root).unwrap(),
            &provider,
        )
        .await;
        assert!(repeated.registered.is_empty());
        assert_eq!(repeated.failures.len(), 2);
        assert!(repeated.failures.iter().any(|failure| matches!(
            failure,
            RuntimeStartupFailure::Claim {
                paths,
                error: FileStrategyRecoveryError::Lease(RuntimeLeaseError::AlreadyHeld),
            } if paths.run_id() == &armed_run
        )));
        assert!(repeated.failures.iter().any(|failure| matches!(
            failure,
            RuntimeStartupFailure::MarketAlreadyOwned {
                run_id,
                exchange: Exchange::Binance,
                symbol,
                owner_run_id,
            } if run_id == &active_run
                && symbol == "MUUSDT"
                && owner_run_id == &armed_run
        )));
        assert_eq!(provider.request_count(), 3);
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn startup_provider_or_gateway_failure_releases_claim_for_retry() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        let state = file_state();
        let run_id = state.run_id.clone();
        let paths = StrategyFilePaths::new(&root, run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        let registry = RuntimeRegistry::new();
        let unavailable =
            MockRecoveryProvider::new(MockGateway::new(rules(), 1_100)).failing_for(run_id.clone());

        let provider_failure = recover_discovered_strategies(
            &registry,
            discover_strategy_files(&root).unwrap(),
            &unavailable,
        )
        .await;
        assert!(matches!(
            provider_failure.failures.as_slice(),
            [RuntimeStartupFailure::Provider {
                run_id: failed_run,
                exchange: Exchange::Binance,
                error: "credentials unavailable"
            }] if failed_run == &run_id
        ));
        assert!(claim_leased_file_strategy(paths.clone()).is_ok());

        let wrong_gateway = MockGateway::new(rules(), 1_100).with_exchange(Exchange::Aster);
        let mismatched = MockRecoveryProvider::new(wrong_gateway.clone());
        let attach_failure = recover_discovered_strategies(
            &registry,
            discover_strategy_files(&root).unwrap(),
            &mismatched,
        )
        .await;
        assert!(matches!(
            attach_failure.failures.as_slice(),
            [RuntimeStartupFailure::Attach {
                run_id: failed_run,
                error: FileStrategyRecoveryError::GatewayMismatch {
                    expected: Exchange::Binance,
                    actual: Exchange::Aster
                }
            }] if failed_run == &run_id
        ));
        assert_eq!(wrong_gateway.all_bootstrap_call_count(), 0);
        assert!(claim_leased_file_strategy(paths).is_ok());
        assert!(registry.is_empty().await);
    }

    #[test]
    fn recovery_rejects_gateway_mismatch_and_dirty_armed_ledger_before_visibility() {
        let directory = tempdir().unwrap();
        let active = file_state();
        let active_paths = StrategyFilePaths::new(directory.path(), active.run_id.clone()).unwrap();
        FileStrategyStateStore::create(active_paths.state(), active).unwrap();
        let wrong_gateway = MockGateway::new(rules(), 1_100).with_exchange(Exchange::Aster);
        assert!(matches!(
            recover_leased_file_strategy(
                wrong_gateway.clone(),
                active_paths.clone(),
                runtime_settings()
            ),
            Err(FileStrategyRecoveryError::GatewayMismatch {
                expected: Exchange::Binance,
                actual: Exchange::Aster
            })
        ));
        assert_eq!(wrong_gateway.all_bootstrap_call_count(), 0);
        assert!(StrategyRuntimeLease::acquire(active_paths.lease()).is_ok());

        let armed = armed_file_state();
        let armed_paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(armed_paths.state(), armed).unwrap();
        let mut intents = FileOrderIntentStore::load(armed_paths.intents()).unwrap();
        intents
            .insert_prepared(
                OrderIntent::prepare(
                    ClientOrderId::parse("dirty_arm").unwrap(),
                    Exchange::Binance,
                    OrderShape {
                        symbol: "MUUSDT".into(),
                        side: OrderSide::Sell,
                        price: Some(Decimal::new(1014, 0)),
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
        let gateway = MockGateway::new(rules(), 1_100);
        assert!(matches!(
            recover_leased_file_strategy(gateway.clone(), armed_paths, runtime_settings()),
            Err(FileStrategyRecoveryError::IntentLedgerMismatch)
        ));
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registry_rejects_overlapping_same_run_but_does_not_serialize_other_runs() {
        let first_directory = tempdir().unwrap();
        let second_directory = tempdir().unwrap();
        let first_gateway = MockGateway::new(rules(), 1_100);
        let second_gateway = MockGateway::new(rules(), 1_100).with_symbol("ALTUSDT");
        let first_run = StrategyRunId::parse("sched001").unwrap();
        let second_run = StrategyRunId::parse("sched002").unwrap();
        let first = prepare_leased_file_strategy(
            first_gateway.clone(),
            first_directory.path(),
            first_run.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let mut second_config = config(None);
        second_config.symbol = "ALTUSDT".into();
        let second = prepare_leased_file_strategy(
            second_gateway.clone(),
            second_directory.path(),
            second_run.clone(),
            second_config,
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let registry = Arc::new(RuntimeRegistry::new());
        assert!(matches!(
            registry.register(first).await,
            RuntimeRegistration::Registered
        ));
        assert!(matches!(
            registry.register(second).await,
            RuntimeRegistration::Registered
        ));
        let gate = first_gateway.block_market_snapshot();
        let advancing_registry = Arc::clone(&registry);
        let advancing_run = first_run.clone();
        let first_tick =
            tokio::spawn(async move { advancing_registry.advance(&advancing_run, 1_200).await });
        gate.wait_until_entered().await;

        assert!(matches!(
            registry.advance(&first_run, 1_200).await,
            Err(RuntimeRegistryAdvanceError::AlreadyAdvancing(run_id)) if run_id == first_run
        ));
        let entries = registry.entries().await;
        assert!(
            entries.iter().any(|entry| {
                entry.run_id == first_run && entry.advancing && entry.kind.is_none()
            })
        );

        let PreparedStrategyStep::Active(second_report) =
            registry.advance(&second_run, 1_200).await.unwrap()
        else {
            panic!("the unrelated runtime must advance independently");
        };
        assert_eq!(second_report.submissions.len(), 1);
        assert_eq!(second_gateway.placement_call_count(), 1);

        let stopping_registry = Arc::clone(&registry);
        let stopping_run = first_run.clone();
        let stop =
            tokio::spawn(async move { stopping_registry.request_stop(&stopping_run, 1_201).await });
        tokio::task::yield_now().await;
        assert!(
            !stop.is_finished(),
            "stop must wait for the in-flight atomic tick instead of failing transiently"
        );

        gate.release();
        let PreparedStrategyStep::Active(first_report) = first_tick.await.unwrap().unwrap() else {
            panic!("the blocked runtime must finish after release");
        };
        assert_eq!(first_report.submissions.len(), 1);
        assert_eq!(first_gateway.placement_call_count(), 1);
        assert!(matches!(
            stop.await.unwrap().unwrap(),
            PreparedStrategyStopOutcome::Active(StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested,
            })
        ));
    }

    #[tokio::test]
    async fn registry_rejects_a_second_live_strategy_for_the_same_market() {
        let first_directory = tempdir().unwrap();
        let second_directory = tempdir().unwrap();
        let first_run = StrategyRunId::parse("market01").unwrap();
        let second_run = StrategyRunId::parse("market02").unwrap();
        let first = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            first_directory.path(),
            first_run.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let second = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            second_directory.path(),
            second_run.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let registry = RuntimeRegistry::new();

        assert!(matches!(
            registry.register(first).await,
            RuntimeRegistration::Registered
        ));
        let RuntimeRegistration::MarketAlreadyOwned {
            owner_run_id,
            rejected,
        } = registry.register(second).await
        else {
            panic!("one exchange market must have exactly one runtime owner");
        };

        assert_eq!(owner_run_id, first_run);
        assert_eq!(rejected.run_id(), &second_run);
        assert_eq!(registry.len().await, 1);
        assert_eq!(
            registry.owner_for_market(Exchange::Binance, "MUUSDT").await,
            Some(first_run)
        );
    }

    #[tokio::test]
    async fn cancelling_an_armed_runtime_releases_market_ownership_atomically() {
        let first_directory = tempdir().unwrap();
        let second_directory = tempdir().unwrap();
        let first_run = StrategyRunId::parse("armed101").unwrap();
        let second_run = StrategyRunId::parse("armed102").unwrap();
        let mut armed_config = config(None);
        armed_config.trigger_price = Some(Decimal::new(1015, 0));
        let first = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            first_directory.path(),
            first_run.clone(),
            armed_config.clone(),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let second = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            second_directory.path(),
            second_run.clone(),
            armed_config,
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let registry = RuntimeRegistry::new();

        assert!(matches!(
            registry.register(first).await,
            RuntimeRegistration::Registered
        ));
        assert!(matches!(
            registry.request_stop(&first_run, 1_101).await.unwrap(),
            PreparedStrategyStopOutcome::ArmedCancelled
        ));
        assert!(matches!(
            registry.register(second).await,
            RuntimeRegistration::Registered
        ));

        assert!(!registry.contains(&first_run).await);
        assert!(registry.contains(&second_run).await);
        assert_eq!(
            registry.owner_for_market(Exchange::Binance, "MUUSDT").await,
            Some(second_run)
        );
    }

    #[tokio::test]
    async fn stop_before_first_tick_never_submits_the_opening_order() {
        let directory = tempdir().unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        let run_id = StrategyRunId::parse("stop0001").unwrap();
        let strategy = prepare_leased_file_strategy(
            gateway.clone(),
            directory.path(),
            run_id.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let registry = RuntimeRegistry::new();
        assert!(matches!(
            registry.register(strategy).await,
            RuntimeRegistration::Registered
        ));

        assert!(matches!(
            registry.request_stop(&run_id, 1_101).await.unwrap(),
            PreparedStrategyStopOutcome::Active(StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested,
            })
        ));
        let PreparedStrategyStep::Active(report) = registry.advance(&run_id, 1_102).await.unwrap()
        else {
            panic!("an active runtime must process its durable stop request");
        };

        assert!(report.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 0);
        let entries = registry.entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].lifecycle,
            Some(PreparedStrategyLifecycle::Stopped)
        );
        assert_eq!(registry.prune_terminal().await, vec![run_id.clone()]);
        assert!(!registry.contains(&run_id).await);
    }

    #[tokio::test]
    async fn duplicate_registry_entry_returns_the_rejected_runtime_without_replacing_owner() {
        let first_directory = tempdir().unwrap();
        let duplicate_directory = tempdir().unwrap();
        let run_id = StrategyRunId::parse("duprun01").unwrap();
        let first = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            first_directory.path(),
            run_id.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let duplicate = prepare_leased_file_strategy(
            MockGateway::new(rules(), 1_100),
            duplicate_directory.path(),
            run_id.clone(),
            config(None),
            1_100,
            runtime_settings(),
        )
        .await
        .unwrap();
        let duplicate_paths = duplicate.paths().clone();
        let registry = RuntimeRegistry::new();

        assert!(matches!(
            registry.register(first).await,
            RuntimeRegistration::Registered
        ));
        let RuntimeRegistration::Duplicate(rejected) = registry.register(duplicate).await else {
            panic!("the existing run ID must remain the sole registry owner");
        };

        assert_eq!(registry.len().await, 1);
        assert!(registry.contains(&run_id).await);
        assert_eq!(rejected.paths(), &duplicate_paths);
        assert!(matches!(
            LeasedFileStrategyRuntime::load(
                MockGateway::new(rules(), 1_100),
                duplicate_paths,
                runtime_settings()
            ),
            Err(FileRuntimeLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
    }

    #[test]
    fn leased_file_runtime_has_one_owner_and_releases_on_drop() {
        let directory = tempdir().unwrap();
        let state = file_state();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();

        let first = LeasedFileStrategyRuntime::load(
            MockGateway::new(rules(), 1_100),
            paths.clone(),
            runtime_settings(),
        )
        .unwrap();
        assert_eq!(first.paths(), &paths);
        assert!(matches!(
            LeasedFileStrategyRuntime::load(
                MockGateway::new(rules(), 1_100),
                paths.clone(),
                runtime_settings()
            ),
            Err(FileRuntimeLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));

        drop(first);
        assert!(
            LeasedFileStrategyRuntime::load(
                MockGateway::new(rules(), 1_100),
                paths,
                runtime_settings()
            )
            .is_ok()
        );
    }

    #[test]
    fn file_runtime_rejects_state_from_another_run_and_releases_lease() {
        let directory = tempdir().unwrap();
        let paths =
            StrategyFilePaths::new(directory.path(), StrategyRunId::parse("OTHER001").unwrap())
                .unwrap();
        FileStrategyStateStore::create(paths.state(), file_state()).unwrap();

        assert!(matches!(
            LeasedFileStrategyRuntime::load(
                MockGateway::new(rules(), 1_100),
                paths.clone(),
                runtime_settings()
            ),
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
            LeasedFileStrategyRuntime::load(
                MockGateway::new(rules(), 1_100),
                paths.clone(),
                runtime_settings()
            ),
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

        let mut first = LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()).unwrap();
        assert!(matches!(
            LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()),
            Err(FileArmedLoadError::Lease(RuntimeLeaseError::AlreadyHeld))
        ));
        first.cancel(1_001).unwrap();
        drop(first);

        let restored = LeasedFileArmedStrategy::load(paths, runtime_settings()).unwrap();
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
        let leased = LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        let active = leased.activate(gateway.clone(), 1_100).await.unwrap();

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
            LeasedFileStrategyRuntime::load(gateway, paths, runtime_settings()),
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
        let leased = LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);
        gateway.set_market_price(Decimal::new(1013, 0), 1_100);

        assert!(matches!(
            leased.activate(gateway.clone(), 1_100).await,
            Err(FileArmedActivationError::Bootstrap(
                StrategyBootstrapError::TriggerNotReached
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.market_snapshot_call_count(), 1);
        assert_eq!(gateway.account_preflight_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths, runtime_settings()).is_ok());
    }

    #[tokio::test]
    async fn low_level_armed_activation_rejects_wrong_exchange_before_any_gateway_call() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100).with_exchange(Exchange::Aster);

        assert!(matches!(
            leased.activate(gateway.clone(), 1_100).await,
            Err(FileArmedActivationError::GatewayMismatch {
                expected: Exchange::Binance,
                actual: Exchange::Aster
            })
        ));
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths, runtime_settings()).is_ok());
    }

    #[test]
    fn invalid_runtime_settings_cannot_construct_an_armed_runtime() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let gateway = MockGateway::new(rules(), 1_100);

        assert!(matches!(
            RuntimeSettings::new("USDT", 0, 100, 100),
            Err(RuntimeBuildError::InvalidFreshnessWindow)
        ));
        assert!(matches!(
            RuntimeSettings::new("USDT", 10_000, 100, 0),
            Err(RuntimeBuildError::InvalidSubmissionLimit)
        ));
        assert!(matches!(
            RuntimeSettings::new("USDT/BNB", 10_000, 100, 100),
            Err(RuntimeBuildError::ExecutionAccounting(
                ExecutionAccountingError::InvalidQuoteAsset
            ))
        ));
        let valid = runtime_settings();
        assert_eq!(valid.quote_asset(), "USDT");
        assert_eq!(valid.maximum_market_age_ms(), 10_000);
        assert_eq!(valid.maximum_future_skew_ms(), 100);
        assert_eq!(valid.maximum_submissions_per_tick(), 100);
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(LeasedFileArmedStrategy::load(paths, runtime_settings()).is_ok());
    }

    #[tokio::test]
    async fn non_empty_intent_ledger_blocks_armed_activation_before_exchange_reads() {
        let directory = tempdir().unwrap();
        let armed = armed_file_state();
        let paths = StrategyFilePaths::new(directory.path(), armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let bytes_before = std::fs::read(paths.state()).unwrap();
        let leased = LeasedFileArmedStrategy::load(paths.clone(), runtime_settings()).unwrap();
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
        let gateway = MockGateway::new(rules(), 1_100);

        assert!(matches!(
            leased.activate(gateway.clone(), 1_100).await,
            Err(FileArmedActivationError::IntentLedgerMismatch)
        ));
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(gateway.all_bootstrap_call_count(), 0);
        assert_eq!(std::fs::read(paths.state()).unwrap(), bytes_before);
        assert!(matches!(
            LeasedFileArmedStrategy::load(paths, runtime_settings()),
            Err(FileArmedLoadError::IntentLedgerMismatch)
        ));
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
    async fn batched_open_progress_skips_idle_orders_but_syncs_the_exact_partial_fill() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        gateway.enable_open_progress();

        let lookup_before_idle = gateway.order_lookup_call_count();
        let execution_before_idle = gateway.execution_snapshot_call_count();
        let idle = runtime.tick(1_300).await.unwrap();

        assert!(!idle.is_blocked(), "{idle:?}");
        assert_eq!(gateway.open_progress_call_count(), 1);
        assert_eq!(gateway.order_lookup_call_count(), lookup_before_idle);
        assert_eq!(
            gateway.execution_snapshot_call_count(),
            execution_before_idle
        );
        assert_eq!(idle.execution_syncs, 0);

        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let partial_quantity = source_shape.quantity / Decimal::new(2, 0);
        gateway.partially_fill_order(
            &source_id,
            partial_quantity,
            source_shape.price.unwrap(),
            Decimal::new(1, 3),
        );
        gateway.set_position(
            -(opening_quantity + partial_quantity),
            Some(Decimal::new(1014, 0)),
        );

        let lookup_before_fill = gateway.order_lookup_call_count();
        let execution_before_fill = gateway.execution_snapshot_call_count();
        let filled = runtime.tick(1_400).await.unwrap();

        assert!(!filled.is_blocked(), "{filled:?}");
        assert_eq!(gateway.open_progress_call_count(), 2);
        assert_eq!(gateway.order_lookup_call_count(), lookup_before_fill);
        assert_eq!(
            gateway.execution_snapshot_call_count(),
            execution_before_fill + 1
        );
        assert_eq!(filled.execution_syncs, 1);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .replacement_obligations
                .values()
                .filter(|obligation| {
                    obligation.kind == ReplacementObligationKind::Counter
                        && obligation.source_client_order_id == source_id
                })
                .map(|obligation| obligation.shape.quantity)
                .sum::<Decimal>(),
            partial_quantity
        );
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
    async fn terminal_fill_after_failed_accept_commit_is_recovered_and_accounted() {
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
        let mut intent_store = MemoryOrderIntentStore::default();
        intent_store.fail_on_write(2);
        let mut runtime = runtime(gateway.clone(), intent_store, machine);

        assert!(matches!(
            runtime.tick(1_100).await,
            Err(RuntimeTickError::Submission(SubmissionError::Persistence(
                _
            )))
        ));
        assert_eq!(gateway.placement_ids(), vec![opening_id.clone()]);
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(2, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));

        let recovered = runtime.tick(1_200).await.unwrap();

        assert!(!recovered.is_blocked());
        assert_eq!(recovered.execution_syncs, 1);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -opening_quantity
        );
        assert_eq!(
            gateway
                .placement_ids()
                .iter()
                .filter(|client_order_id| **client_order_id == opening_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn terminal_identity_survives_a_second_crash_before_strategy_commit() {
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
        let mut intent_store = MemoryOrderIntentStore::default();
        intent_store.fail_on_write(2);
        let mut runtime = runtime(gateway.clone(), intent_store, machine);

        assert!(runtime.tick(1_100).await.is_err());
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(2, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        runtime.machine_mut().store_mut().fail_next_write();

        assert!(matches!(
            runtime.tick(1_200).await,
            Err(RuntimeTickError::Strategy(
                StrategyMachineError::Persistence(_)
            ))
        ));
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .get(&opening_id)
                .unwrap()
                .state,
            IntentState::Terminal {
                status: TerminalOrderStatus::Filled,
                exchange_order_id: Some(_),
            }
        ));
        assert!(
            runtime
                .machine()
                .store()
                .snapshot()
                .orders
                .get(&opening_id)
                .unwrap()
                .exchange_order_id
                .is_none()
        );

        let recovered = runtime.tick(1_300).await.unwrap();

        assert!(!recovered.is_blocked());
        assert_eq!(recovered.execution_syncs, 1);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -opening_quantity
        );
        assert_eq!(
            gateway
                .placement_ids()
                .iter()
                .filter(|client_order_id| **client_order_id == opening_id)
                .count(),
            1
        );
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
    async fn partial_terminal_opening_remainder_is_exact_and_never_resubmitted_after_commit_failure()
     {
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
        let expected_remainder = opening_quantity - partial_quantity;
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        runtime.tick(1_100).await.unwrap();
        gateway.partially_fill_order(
            &opening_id,
            partial_quantity,
            Decimal::new(1014, 0),
            Decimal::new(2, 2),
        );
        gateway.set_position(-partial_quantity, Some(Decimal::new(1014, 0)));
        runtime.tick(1_200).await.unwrap();
        assert_eq!(gateway.placement_call_count(), 1);

        runtime.maximum_submissions_per_tick = 0;
        gateway.mark_order_cancelled(&opening_id);
        runtime.tick(1_300).await.unwrap();
        let remainder = runtime
            .machine()
            .store()
            .snapshot()
            .orders
            .values()
            .find(|order| {
                order.purpose == StrategyOrderPurpose::Opening
                    && order.tracking == StrategyOrderTracking::Ready
            })
            .unwrap()
            .clone();
        assert_eq!(remainder.shape.quantity, expected_remainder);
        assert_eq!(gateway.placement_call_count(), 1);

        runtime.maximum_submissions_per_tick = 100;
        runtime.machine_mut().store_mut().fail_next_write();
        assert!(matches!(
            runtime.tick(1_400).await,
            Err(RuntimeTickError::Strategy(
                StrategyMachineError::Persistence(_)
            ))
        ));
        assert_eq!(gateway.placement_call_count(), 2);
        assert_eq!(
            gateway.placement_intents()[1].client_order_id,
            remainder.client_order_id
        );
        assert_eq!(
            gateway.placement_intents()[1].shape.quantity,
            expected_remainder
        );

        let recovered = runtime.tick(1_500).await.unwrap();
        assert!(!recovered.is_blocked());
        assert!(recovered.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), 2);
        assert!(matches!(
            runtime
                .machine()
                .store()
                .snapshot()
                .orders
                .get(&remainder.client_order_id)
                .unwrap()
                .tracking,
            StrategyOrderTracking::Intent {
                state: IntentState::Accepted { .. }
            }
        ));

        gateway.fill_order(
            &remainder.client_order_id,
            Decimal::new(1014, 0),
            Decimal::new(2, 2),
        );
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        let completed = runtime.tick(1_600).await.unwrap();

        assert!(!completed.is_blocked());
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Running
        );
        assert_eq!(
            runtime.machine().store().snapshot().opening_filled_quantity,
            opening_quantity
        );
        assert_eq!(gateway.placement_call_count(), 22);
        assert_eq!(
            gateway
                .placement_ids()
                .iter()
                .filter(|client_order_id| **client_order_id == remainder.client_order_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn partial_terminal_opening_waits_for_position_snapshot_before_remainder_submission() {
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
        gateway.mark_order_cancelled(&opening_id);

        let lagged = runtime.tick(1_200).await.unwrap();

        assert_eq!(
            lagged.blockers[0].stage,
            RuntimeStage::PositionReconciliation
        );
        assert_eq!(gateway.placement_call_count(), 1);
        let ready = runtime
            .machine()
            .store()
            .snapshot()
            .ready_intents(1_201)
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].shape.quantity, opening_quantity - partial_quantity);

        gateway.set_position(-partial_quantity, Some(Decimal::new(1014, 0)));
        let recovered = runtime.tick(1_300).await.unwrap();

        assert!(!recovered.is_blocked());
        assert_eq!(recovered.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), 2);
        assert_eq!(
            gateway.placement_intents()[1].shape.quantity,
            opening_quantity - partial_quantity
        );
    }

    #[tokio::test]
    async fn failed_grid_partial_commit_creates_one_exact_counter_only_after_retry() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, level_index) = accepted_add_order(runtime.machine());
        let partial_quantity = source_shape.quantity / Decimal::new(2, 0);
        let source_price = source_shape.price.unwrap();
        let placements_before_fill = gateway.placement_call_count();
        gateway.partially_fill_order(
            &source_id,
            partial_quantity,
            source_price,
            Decimal::new(1, 2),
        );
        gateway.set_position(
            -(opening_quantity + partial_quantity),
            Some(Decimal::new(1014, 0)),
        );
        runtime.machine_mut().store_mut().fail_next_write();

        let failed = runtime.tick(1_300).await.unwrap();

        assert!(
            failed
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::ExecutionAccounting)
        );
        assert_eq!(gateway.placement_call_count(), placements_before_fill);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -opening_quantity
        );
        assert!(
            runtime
                .machine()
                .store()
                .snapshot()
                .replacement_obligations
                .is_empty()
        );

        let recovered = runtime.tick(1_400).await.unwrap();
        assert!(!recovered.is_blocked(), "{recovered:?}");
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);
        let counter = gateway
            .state
            .lock()
            .unwrap()
            .placement_calls
            .last()
            .unwrap()
            .clone();
        assert_eq!(counter.shape.side, OrderSide::Buy);
        assert_eq!(counter.shape.quantity, partial_quantity);
        assert!(counter.shape.reduce_only);
        assert_eq!(
            counter.shape.price,
            Some(runtime.machine().store().snapshot().plan.levels[usize::from(level_index)])
        );

        let stable = runtime.tick(1_500).await.unwrap();
        assert!(!stable.is_blocked(), "{stable:?}");
        assert!(stable.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);
    }

    #[tokio::test]
    async fn unsubmitable_reduce_only_fragment_blocks_the_same_tick_without_placing_an_order() {
        let mut strict_rules = rules();
        strict_rules.limit_quantity.min = Decimal::new(2, 1);
        strict_rules.market_quantity.min = Decimal::new(2, 1);
        let mut exact_config = config(None);
        exact_config.grid_order_qty = Some(Decimal::new(4, 1));
        let gateway = MockGateway::new(strict_rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(exact_config, &strict_rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let partial_quantity = Decimal::new(1, 1);
        let placements_before_fill = gateway.placement_call_count();
        gateway.partially_fill_order(
            &source_id,
            partial_quantity,
            source_shape.price.unwrap(),
            Decimal::new(1, 2),
        );
        gateway.set_position(
            -(opening_quantity + partial_quantity),
            Some(Decimal::new(1014, 0)),
        );
        let accepted_before_failure = runtime
            .machine()
            .store()
            .snapshot()
            .orders
            .values()
            .filter(|order| {
                matches!(
                    order.tracking,
                    StrategyOrderTracking::Intent {
                        state: IntentState::Accepted { .. }
                    }
                )
            })
            .map(|order| order.client_order_id.clone())
            .collect::<BTreeSet<_>>();
        gateway.set_cancellation_marks_terminal(true);

        let failed = runtime.tick(1_300).await.unwrap();

        assert!(failed.is_blocked(), "{failed:?}");
        assert!(
            failed
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::StrategyFailed)
        );
        assert_eq!(gateway.placement_call_count(), placements_before_fill);
        let state = runtime.machine().store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(
            state.grid_position_net_quantity,
            -(opening_quantity + partial_quantity)
        );
        assert_eq!(state.replacement_obligations.len(), 1);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );
        assert!(state.orders.values().all(|order| {
            order.shape.quantity != partial_quantity
                || !matches!(order.purpose, StrategyOrderPurpose::Replacement { .. })
        }));
        assert_eq!(
            gateway
                .cancellation_ids()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            accepted_before_failure
        );
        assert!(
            state
                .orders
                .values()
                .all(|order| !matches!(order.purpose, StrategyOrderPurpose::RiskClose))
        );

        let cancellations_after_failure = gateway.cancellation_call_count();
        let settled = runtime.tick(1_400).await.unwrap();
        assert!(settled.is_blocked(), "{settled:?}");
        assert_eq!(
            gateway.cancellation_call_count(),
            cancellations_after_failure
        );
        assert_eq!(gateway.placement_call_count(), placements_before_fill);
        let settled_state = runtime.machine().store().snapshot();
        assert_eq!(settled_state.lifecycle, StrategyLifecycle::Stopped);
        assert!(settled_state.failure.is_some());
        assert!(accepted_before_failure.iter().all(|client_order_id| {
            settled_state
                .orders
                .get(client_order_id)
                .is_some_and(|order| order.terminal_processed)
        }));
    }

    #[tokio::test]
    async fn failed_replacement_materialization_keeps_obligation_unassigned_until_exact_retry() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let partial_quantity = source_shape.quantity / Decimal::new(2, 0);
        gateway.partially_fill_order(
            &source_id,
            partial_quantity,
            source_shape.price.unwrap(),
            Decimal::new(1, 2),
        );
        gateway.set_position(
            -(opening_quantity + partial_quantity),
            Some(Decimal::new(1014, 0)),
        );
        let sync = ExecutionSyncService::new("USDT").unwrap();
        sync.synchronize(&gateway, runtime.machine_mut(), &source_id, 1_300)
            .await
            .unwrap();
        let placements_before_retry = gateway.placement_call_count();
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .replacement_obligations
                .values()
                .filter(|obligation| obligation.assigned_client_order_id.is_none())
                .count(),
            1
        );
        runtime.machine_mut().store_mut().fail_next_write();

        assert!(matches!(
            runtime.tick(1_400).await,
            Err(RuntimeTickError::Strategy(
                StrategyMachineError::Persistence(_)
            ))
        ));
        assert_eq!(gateway.placement_call_count(), placements_before_retry);
        assert!(
            runtime
                .machine()
                .store()
                .snapshot()
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );

        let recovered = runtime.tick(1_500).await.unwrap();
        assert!(!recovered.is_blocked(), "{recovered:?}");
        assert_eq!(recovered.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), placements_before_retry + 1);
        assert_eq!(
            recovered.submissions[0].client_order_id,
            gateway.placement_ids().last().unwrap().clone()
        );
    }

    #[tokio::test]
    async fn immediately_filled_counter_after_failed_strategy_commit_recovers_full_cycle_once() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, level_index) = accepted_add_order(runtime.machine());
        let partial_quantity = source_shape.quantity / Decimal::new(2, 0);
        gateway.partially_fill_order(
            &source_id,
            partial_quantity,
            source_shape.price.unwrap(),
            Decimal::new(1, 2),
        );
        gateway.set_position(
            -(opening_quantity + partial_quantity),
            Some(Decimal::new(1014, 0)),
        );
        let sync = ExecutionSyncService::new("USDT").unwrap();
        sync.synchronize(&gateway, runtime.machine_mut(), &source_id, 1_300)
            .await
            .unwrap();
        runtime
            .machine_mut()
            .materialize_replacements(&rules, 1_350)
            .unwrap();
        let placements_before_counter = gateway.placement_call_count();
        runtime.machine_mut().store_mut().fail_next_write();

        assert!(matches!(
            runtime.tick(1_400).await,
            Err(RuntimeTickError::Strategy(
                StrategyMachineError::Persistence(_)
            ))
        ));
        assert_eq!(
            gateway.placement_call_count(),
            placements_before_counter + 1
        );
        let first_counter_id = gateway.placement_ids().last().unwrap().clone();
        let first_counter = gateway
            .state
            .lock()
            .unwrap()
            .orders
            .get(&first_counter_id)
            .unwrap()
            .clone();
        assert_eq!(first_counter.shape.side, OrderSide::Buy);
        assert_eq!(first_counter.shape.quantity, partial_quantity);
        assert!(first_counter.shape.reduce_only);
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .orders
                .get(&first_counter_id)
                .unwrap()
                .tracking,
            StrategyOrderTracking::Ready
        );

        gateway.fill_order(
            &first_counter_id,
            first_counter.shape.price.unwrap(),
            Decimal::new(1, 2),
        );
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        let recovered = runtime.tick(1_500).await.unwrap();

        assert!(!recovered.is_blocked(), "{recovered:?}");
        assert_eq!(recovered.submissions.len(), 1);
        assert_eq!(
            gateway.placement_call_count(),
            placements_before_counter + 2
        );
        assert_eq!(
            gateway
                .placement_ids()
                .iter()
                .filter(|client_order_id| **client_order_id == first_counter_id)
                .count(),
            1
        );
        let reopened = gateway
            .state
            .lock()
            .unwrap()
            .placement_calls
            .last()
            .unwrap()
            .clone();
        assert_eq!(reopened.shape.side, OrderSide::Sell);
        assert_eq!(reopened.shape.quantity, partial_quantity);
        assert!(!reopened.shape.reduce_only);
        assert_eq!(
            reopened.shape.price,
            Some(runtime.machine().store().snapshot().plan.levels[usize::from(level_index) + 1])
        );
        assert_eq!(
            runtime
                .machine()
                .store()
                .snapshot()
                .grid_position_net_quantity,
            -opening_quantity
        );

        let stable = runtime.tick(1_600).await.unwrap();
        assert!(!stable.is_blocked(), "{stable:?}");
        assert!(stable.submissions.is_empty());
        assert_eq!(
            gateway.placement_call_count(),
            placements_before_counter + 2
        );
    }

    #[tokio::test]
    async fn terminal_execution_supersedes_temporary_not_found_without_state_regression() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let placements_before_fill = gateway.placement_call_count();
        gateway.fill_order(&source_id, source_shape.price.unwrap(), Decimal::new(1, 2));
        gateway.set_position(
            -(opening_quantity + source_shape.quantity),
            Some(Decimal::new(1014, 0)),
        );
        gateway.hide_order_from_lookup(&source_id);

        let first = runtime.tick(1_300).await.unwrap();

        assert!(!first.is_blocked(), "{first:?}");
        assert_eq!(first.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .get(&source_id)
                .unwrap()
                .state,
            IntentState::Terminal {
                status: TerminalOrderStatus::Filled,
                exchange_order_id: Some(_),
            }
        ));
        let accounted = runtime
            .machine()
            .store()
            .snapshot()
            .orders
            .get(&source_id)
            .unwrap();
        assert_eq!(accounted.terminal_status, Some(TerminalOrderStatus::Filled));
        assert!(accounted.terminal_processed);

        let stable = runtime.tick(1_400).await.unwrap();

        assert!(!stable.is_blocked(), "{stable:?}");
        assert!(stable.submissions.is_empty());
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Running
        );
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .get(&source_id)
                .unwrap()
                .state,
            IntentState::Terminal {
                status: TerminalOrderStatus::Filled,
                exchange_order_id: Some(_),
            }
        ));
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);
    }

    #[tokio::test]
    async fn failed_terminal_ledger_convergence_places_nothing_and_retries_exactly_once() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let placements_before_fill = gateway.placement_call_count();
        gateway.fill_order(&source_id, source_shape.price.unwrap(), Decimal::new(1, 2));
        gateway.set_position(
            -(opening_quantity + source_shape.quantity),
            Some(Decimal::new(1014, 0)),
        );
        let sync = ExecutionSyncService::new("USDT").unwrap();
        sync.synchronize(&gateway, runtime.machine_mut(), &source_id, 1_300)
            .await
            .unwrap();
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .get(&source_id)
                .unwrap()
                .state,
            IntentState::Accepted { .. }
        ));
        runtime.intent_store.fail_next_write();

        assert!(matches!(
            runtime.tick(1_400).await,
            Err(RuntimeTickError::IntentLedger(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert_eq!(gateway.placement_call_count(), placements_before_fill);
        assert!(matches!(
            runtime
                .intent_store()
                .snapshot()
                .intents
                .get(&source_id)
                .unwrap()
                .state,
            IntentState::Accepted { .. }
        ));

        let recovered = runtime.tick(1_500).await.unwrap();
        assert!(!recovered.is_blocked(), "{recovered:?}");
        assert_eq!(recovered.submissions.len(), 1);
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);

        let stable = runtime.tick(1_600).await.unwrap();
        assert!(!stable.is_blocked(), "{stable:?}");
        assert!(stable.submissions.is_empty());
        assert_eq!(gateway.placement_call_count(), placements_before_fill + 1);
    }

    #[tokio::test]
    async fn conflicting_terminal_ledgers_fail_closed_without_any_replacement() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(None), &rules),
        );
        let opening_quantity = deploy_running_short_grid(&mut runtime, &gateway).await;
        let primed = runtime.tick(1_250).await.unwrap();
        assert!(!primed.is_blocked(), "{primed:?}");
        let (source_id, source_shape, _) = accepted_add_order(runtime.machine());
        let placements_before_fill = gateway.placement_call_count();
        gateway.fill_order(&source_id, source_shape.price.unwrap(), Decimal::new(1, 2));
        gateway.set_position(
            -(opening_quantity + source_shape.quantity),
            Some(Decimal::new(1014, 0)),
        );
        let sync = ExecutionSyncService::new("USDT").unwrap();
        sync.synchronize(&gateway, runtime.machine_mut(), &source_id, 1_300)
            .await
            .unwrap();
        let exchange_order_id = runtime
            .machine()
            .store()
            .snapshot()
            .orders
            .get(&source_id)
            .unwrap()
            .exchange_order_id
            .clone()
            .unwrap();
        runtime
            .intent_store
            .transition(
                &source_id,
                IntentState::Terminal {
                    status: TerminalOrderStatus::Cancelled,
                    exchange_order_id: Some(exchange_order_id),
                },
                1_350,
            )
            .unwrap();

        assert!(matches!(
            runtime.tick(1_400).await,
            Err(RuntimeTickError::IntentLedgerMismatch)
        ));
        assert_eq!(gateway.placement_call_count(), placements_before_fill);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Running
        );
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
        let state = runtime.machine().store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Stopped);
        assert!(state.failure.is_some());
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
    async fn authoritative_active_execution_allows_deployment_after_transient_lookup_absence() {
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
        runtime.maximum_submissions_per_tick = 1;

        let opening = runtime.tick(1_100).await.unwrap();
        assert_eq!(opening.submissions.len(), 1);
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));

        let first_grid = runtime.tick(1_200).await.unwrap();
        assert!(!first_grid.is_blocked(), "{first_grid:?}");
        assert_eq!(first_grid.submissions.len(), 1);
        let visible_through_execution = first_grid.submissions[0].client_order_id.clone();
        gateway.hide_order_from_lookup(&visible_through_execution);

        let continued = runtime.tick(1_300).await.unwrap();

        assert!(
            !continued.is_blocked(),
            "an exact active execution snapshot must supersede transient lookup absence: {continued:?}"
        );
        assert_eq!(continued.submissions.len(), 1);
        assert_ne!(
            continued.submissions[0].client_order_id,
            visible_through_execution
        );
        assert_eq!(gateway.placement_call_count(), 3);
    }

    #[tokio::test]
    async fn foreign_execution_never_clears_transient_lookup_absence_or_allows_more_orders() {
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
        runtime.maximum_submissions_per_tick = 1;

        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(-opening_quantity, Some(Decimal::new(1014, 0)));
        let first_grid = runtime.tick(1_200).await.unwrap();
        let hidden = first_grid.submissions[0].client_order_id.clone();
        gateway.hide_order_from_lookup(&hidden);
        gateway
            .state
            .lock()
            .unwrap()
            .executions
            .get_mut(&hidden)
            .unwrap()
            .order
            .shape
            .quantity += Decimal::new(1, 1);

        let blocked = runtime.tick(1_300).await.unwrap();

        assert!(blocked.is_blocked(), "{blocked:?}");
        assert!(blocked.submissions.is_empty());
        assert!(blocked.blockers.iter().any(|blocker| {
            blocker.stage == RuntimeStage::LedgerReconciliation
                && blocker.client_order_id.as_ref() == Some(&hidden)
        }));
        assert!(blocked.blockers.iter().any(|blocker| {
            blocker.stage == RuntimeStage::ExecutionAccounting
                && blocker.client_order_id.as_ref() == Some(&hidden)
        }));
        assert_eq!(gateway.placement_call_count(), 2);
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
    async fn unknown_cancellation_retries_when_exact_execution_proves_the_order_is_active() {
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
        let retried = runtime.tick(1_300).await.unwrap();

        assert!(
            retried
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::CancellationUnknown)
        );
        assert!(
            !retried
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::LedgerReconciliation)
        );
        assert_eq!(gateway.cancellation_call_count(), 2);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::StopRequested
        );
    }

    #[tokio::test]
    async fn unknown_cancellation_is_not_retried_without_any_authoritative_active_evidence() {
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

        {
            let mut state = gateway.state.lock().unwrap();
            state.orders.remove(&opening_id);
            state.executions.remove(&opening_id);
        }
        let inconclusive = runtime.tick(1_300).await.unwrap();

        assert!(inconclusive.blockers.iter().any(|blocker| {
            blocker.stage == RuntimeStage::LedgerReconciliation
                && blocker.client_order_id.as_ref() == Some(&opening_id)
        }));
        assert!(inconclusive.blockers.iter().any(|blocker| {
            blocker.stage == RuntimeStage::ExecutionAccounting
                && blocker.client_order_id.as_ref() == Some(&opening_id)
        }));
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

    #[tokio::test]
    async fn failed_cleanup_cancels_an_accepted_risk_close_before_releasing_the_market() {
        let original_rules = rules();
        let gateway = MockGateway::new(original_rules.clone(), 1_100);
        let machine = machine(config(Some(Decimal::new(1021, 0))), &original_rules);
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

        gateway.set_cancellation_marks_terminal(true);
        gateway.set_market_price(Decimal::new(1022, 0), 1_300);
        runtime.tick(1_300).await.unwrap();
        let closing = runtime.tick(1_400).await.unwrap();
        assert_eq!(closing.submissions.len(), 1, "{closing:?}");
        let risk_close = gateway
            .state
            .lock()
            .unwrap()
            .placement_calls
            .last()
            .unwrap()
            .clone();
        assert!(risk_close.shape.reduce_only);
        assert_eq!(risk_close.shape.kind, OrderKind::Market);
        let placements_before_failure = gateway.placement_call_count();
        let cancellations_before_failure = gateway.cancellation_call_count();

        let mut changed_rules = original_rules;
        changed_rules.tick_size = Decimal::new(5, 1);
        gateway.set_rules(changed_rules);
        let failed = runtime.tick(1_500).await.unwrap();
        assert!(
            failed
                .blockers
                .iter()
                .any(|blocker| blocker.stage == RuntimeStage::InstrumentRules),
            "{failed:?}"
        );
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::Failed
        );
        assert_eq!(
            gateway.cancellation_call_count(),
            cancellations_before_failure
        );

        let cleanup = runtime.tick(1_600).await.unwrap();
        assert!(
            cleanup
                .cancellations
                .iter()
                .any(|cancellation| cancellation.client_order_id == risk_close.client_order_id),
            "{cleanup:?}"
        );
        assert_eq!(
            gateway.cancellation_call_count(),
            cancellations_before_failure + 1
        );
        assert_eq!(
            gateway.cancellation_ids().last(),
            Some(&risk_close.client_order_id)
        );
        assert_eq!(gateway.placement_call_count(), placements_before_failure);

        runtime.tick(1_700).await.unwrap();
        let settled = runtime.machine().store().snapshot();
        assert_eq!(settled.lifecycle, StrategyLifecycle::Stopped);
        assert!(settled.failure.is_some());
        assert_eq!(gateway.placement_call_count(), placements_before_failure);
        assert_eq!(
            gateway.cancellation_call_count(),
            cancellations_before_failure + 1
        );
        assert!(
            settled
                .orders
                .get(&risk_close.client_order_id)
                .is_some_and(|order| order.terminal_processed)
        );
    }

    #[tokio::test]
    async fn risk_exit_accounts_a_fill_winning_the_cancel_race_and_preserves_the_baseline() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let baseline_quantity = Decimal::new(-3, 0);
        let baseline = PositionBaseline::from_authoritative_position(
            baseline_quantity,
            Some(Decimal::new(1010, 0)),
        )
        .unwrap();
        let machine = machine_with_baseline(
            config(Some(Decimal::new(1021, 0))),
            &rules,
            baseline.clone(),
        );
        let opening_id = opening_id(&machine);
        let opening_quantity = machine
            .store()
            .snapshot()
            .orders
            .get(&opening_id)
            .unwrap()
            .shape
            .quantity;
        gateway.set_position(baseline_quantity, Some(Decimal::new(1010, 0)));
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        runtime.tick(1_100).await.unwrap();
        gateway.fill_order(&opening_id, Decimal::new(1014, 0), Decimal::new(5, 2));
        gateway.set_position(
            baseline_quantity - opening_quantity,
            Some(Decimal::new(1012, 0)),
        );
        runtime.tick(1_200).await.unwrap();

        gateway.set_market_price(Decimal::new(1022, 0), 1_300);
        let cancelling = runtime.tick(1_300).await.unwrap();
        assert!(!cancelling.cancellations.is_empty());
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::RiskExitRequested
        );

        let cancellation_ids = gateway.cancellation_ids();
        let raced_id = cancellation_ids[0].clone();
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
        for client_order_id in cancellation_ids
            .iter()
            .filter(|client_order_id| **client_order_id != raced_id)
        {
            gateway.mark_order_cancelled(client_order_id);
        }
        let raced_delta = match raced_shape.side {
            OrderSide::Buy => raced_shape.quantity,
            OrderSide::Sell => -raced_shape.quantity,
        };
        let expected_grid_quantity = -opening_quantity + raced_delta;
        gateway.set_position(
            baseline_quantity + expected_grid_quantity,
            Some(Decimal::new(1012, 0)),
        );

        let closing = runtime.tick(1_400).await.unwrap();
        assert_eq!(closing.submissions.len(), 1, "{closing:?}");
        let close_intent = gateway
            .state
            .lock()
            .unwrap()
            .placement_calls
            .last()
            .unwrap()
            .clone();
        assert_eq!(close_intent.shape.quantity, expected_grid_quantity.abs());
        assert_eq!(close_intent.shape.side, OrderSide::Buy);
        assert!(close_intent.shape.reduce_only);
        assert_eq!(close_intent.shape.kind, OrderKind::Market);

        gateway.fill_order(
            &close_intent.client_order_id,
            Decimal::new(1022, 0),
            Decimal::new(5, 2),
        );
        gateway.set_position(baseline_quantity, Some(Decimal::new(1010, 0)));
        let closed = runtime.tick(1_500).await.unwrap();

        assert!(!closed.is_blocked(), "{closed:?}");
        let state = runtime.machine().store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Closed);
        assert_eq!(state.baseline, baseline);
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert_eq!(
            state.expected_exchange_position().unwrap(),
            baseline_quantity
        );
    }
}
