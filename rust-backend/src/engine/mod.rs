mod armed_strategy;
mod exchange_inputs;
mod execution_accounting;
mod execution_sync;
mod grid_plan;
mod reconciliation;
mod strategy_machine;
mod submission;

pub use armed_strategy::{
    ArmedStrategyError, ArmedStrategyLifecycle, ArmedStrategyState, TriggerCondition,
};
pub use exchange_inputs::{AuthoritativeStrategyInputs, StrategyInputError, StrategyInputService};
pub use execution_accounting::{
    ExecutionAccountingError, ExecutionAccountingService, ExecutionAuditRecord, FeeValuation,
    FeeValuationSource, ValuedExecutionReport,
};
pub use execution_sync::{ExecutionSyncError, ExecutionSyncResult, ExecutionSyncService};
pub use grid_plan::{
    GridOrderRole, GridPlan, GridPlanError, MarketSnapshot, PlannedGridOrder, PlannedOpeningOrder,
    build_grid_plan,
};
pub use reconciliation::{ReconciliationError, ReconciliationResult, ReconciliationService};
pub(crate) use strategy_machine::TriggerActivation;
pub use strategy_machine::{
    ExecutionReport, LevelLot, MemoryStrategyStateStore, NeutralLot, PositionBaseline,
    ReplacementObligation, ReplacementObligationKind, RiskExitReason, StrategyLifecycle,
    StrategyMachine, StrategyMachineError, StrategyOrderPurpose, StrategyOrderRecord,
    StrategyOrderTracking, StrategyRunId, StrategyState, StrategyStateError, StrategyStateStore,
    StrategyStoreError, StrategyTransition,
};
pub use submission::{SubmissionError, SubmissionResult, SubmissionService};
