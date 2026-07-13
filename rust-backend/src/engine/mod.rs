mod armed_strategy;
mod cancellation;
mod exchange_inputs;
mod execution_accounting;
mod execution_sync;
mod grid_plan;
mod reconciliation;
mod runtime;
mod strategy_machine;
mod submission;

pub use armed_strategy::{
    ArmedStrategyError, ArmedStrategyLifecycle, ArmedStrategyState, TriggerCondition,
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
pub use grid_plan::{
    GridOrderRole, GridPlan, GridPlanError, MarketSnapshot, PlannedGridOrder, PlannedOpeningOrder,
    build_grid_plan,
};
pub use reconciliation::{
    ReconciliationError, ReconciliationResult, ReconciliationService, reconcile_with,
};
pub use runtime::{
    RuntimeBlocker, RuntimeBuildError, RuntimeCancellation, RuntimeStage, RuntimeSubmission,
    RuntimeTickError, RuntimeTickReport, StrategyRuntime,
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
