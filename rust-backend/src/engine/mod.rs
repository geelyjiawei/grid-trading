mod armed_strategy;
mod bootstrap;
mod cancellation;
mod exchange_inputs;
mod execution_accounting;
mod execution_sync;
mod fee_rates;
mod grid_plan;
mod leverage;
mod reconciliation;
mod runtime;
mod shadow_audit;
mod shadow_collector;
mod strategy_machine;
mod submission;
mod supervisor;

pub use armed_strategy::{
    ArmedStrategyError, ArmedStrategyLifecycle, ArmedStrategyState, TriggerCondition,
};
pub use bootstrap::{
    PreparedStrategy, StrategyBootstrapError, activate_armed_strategy, prepare_new_strategy,
};
pub use cancellation::{
    CancellationResult, CancellationServiceError, cancel_with, resolve_cancellation_with,
};
pub use exchange_inputs::{
    AuthoritativeStrategyInputs, StrategyInputError, StrategyInputService, load_strategy_inputs,
};
pub use execution_accounting::{
    ExecutionAccountingError, ExecutionAccountingService, ExecutionAuditRecord, FeeValuation,
    FeeValuationSource, ValuedExecutionReport,
};
pub use execution_sync::{ExecutionSyncError, ExecutionSyncResult, ExecutionSyncService};
pub use fee_rates::{AuthoritativeFeeConfig, FeeRateConfigError, load_authoritative_fee_config};
pub use grid_plan::{
    GridOrderRole, GridPlan, GridPlanError, MarketSnapshot, PlannedGridOrder, PlannedOpeningOrder,
    build_grid_plan,
};
pub use leverage::{LeveragePreflightError, LeveragePreflightResult, ensure_symbol_leverage};
pub use reconciliation::{
    ReconciliationError, ReconciliationResult, ReconciliationService, reconcile_with,
};
pub(crate) use runtime::validate_cross_ledger_ownership;
pub use runtime::{
    FileArmedActivationError, FileArmedLoadError, FileRuntimeLoadError, FileStrategyRecoveryError,
    FileStrategyStartError, LeasedFileArmedStrategy, LeasedFileStrategyRecovery,
    LeasedFileStrategyRuntime, PreparedLeasedFileStrategy, PreparedStrategyKind,
    PreparedStrategyStep, PreparedStrategyStepError, RuntimeBlocker, RuntimeBuildError,
    RuntimeCancellation, RuntimeSettings, RuntimeStage, RuntimeSubmission, RuntimeTickError,
    RuntimeTickReport, StrategyRuntime, claim_leased_file_strategy, prepare_leased_file_strategy,
    recover_leased_file_strategy,
};
pub use shadow_audit::{
    ShadowAuditIssue, ShadowAuditReport, ShadowExpectedLifecycle, ShadowLevelCoverage,
    ShadowOrderAuditSummary, ShadowPositionAudit, audit_strategy_shadow,
};
pub use shadow_collector::{
    CollectedStrategyShadow, ShadowCollectionError, StableExchangeView,
    collect_stable_exchange_view, collect_strategy_shadow, collect_strategy_shadow_view,
};
pub(crate) use strategy_machine::TriggerActivation;
pub use strategy_machine::{
    ExecutionReport, LevelLot, MemoryStrategyStateStore, NeutralLot, PositionBaseline,
    ReplacementObligation, ReplacementObligationKind, RiskExitReason, StrategyLifecycle,
    StrategyMachine, StrategyMachineError, StrategyOrderPurpose, StrategyOrderRecord,
    StrategyOrderTracking, StrategyRunId, StrategyState, StrategyStateError, StrategyStateStore,
    StrategyStoreError, StrategyTransition,
};
pub use submission::{SubmissionError, SubmissionResult, SubmissionService, submit_with};
pub use supervisor::{
    RuntimeRecoveryProvider, RuntimeRegistration, RuntimeRegistry, RuntimeRegistryAdvanceError,
    RuntimeRegistryEntry, RuntimeStartupFailure, RuntimeStartupReport,
    recover_discovered_strategies,
};
