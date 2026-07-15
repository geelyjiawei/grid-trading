use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    ClientOrderId, Direction, Exchange, GridConfig, InstrumentRules, IntentState, OrderIntent,
    OrderKind, OrderShape, OrderSide, TerminalOrderStatus,
};
use crate::exchange::{
    OrderLifecycle, compare_trade_chronology, is_valid_trade_id, trades_are_canonically_ordered,
};

use super::{
    GridOrderRole, GridPlan, GridPlanError, PlannedGridOrder,
    execution_accounting::{ExecutionAuditRecord, FeeValuationSource, ValuedExecutionReport},
};

fn one_u64() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StrategyRunId(String);

impl StrategyRunId {
    pub fn parse(value: impl Into<String>) -> Result<Self, StrategyStateError> {
        let value = value.into();
        if !(8..=12).contains(&value.len())
            || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(StrategyStateError::InvalidRunId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionBaseline {
    pub signed_quantity: Decimal,
    pub entry_price: Option<Decimal>,
}

impl PositionBaseline {
    pub fn flat() -> Self {
        Self {
            signed_quantity: Decimal::ZERO,
            entry_price: None,
        }
    }

    pub fn from_authoritative_position(
        signed_quantity: Decimal,
        entry_price: Option<Decimal>,
    ) -> Result<Self, StrategyStateError> {
        let baseline = Self {
            signed_quantity,
            entry_price,
        };
        baseline.validate()?;
        Ok(baseline)
    }

    fn validate(&self) -> Result<(), StrategyStateError> {
        if self.signed_quantity.is_zero() {
            if self.entry_price.is_some_and(|price| price <= Decimal::ZERO) {
                return Err(StrategyStateError::InvalidBaseline);
            }
            return Ok(());
        }
        if self.entry_price.is_none_or(|price| price <= Decimal::ZERO) {
            return Err(StrategyStateError::InvalidBaseline);
        }
        Ok(())
    }

    fn validate_for_direction(&self, direction: Direction) -> Result<(), StrategyStateError> {
        let compatible = match direction {
            Direction::Long => self.signed_quantity >= Decimal::ZERO,
            Direction::Short => self.signed_quantity <= Decimal::ZERO,
            Direction::Neutral => self.signed_quantity.is_zero(),
        };
        if compatible {
            Ok(())
        } else {
            Err(StrategyStateError::BaselineDirectionConflict)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyLifecycle {
    AwaitingOpening,
    DeployingGrid,
    Running,
    RiskExitRequested,
    StopRequested,
    Stopped,
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskExitReason {
    StopLoss,
    TakeProfit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StrategyOrderPurpose {
    Opening,
    RiskClose,
    InitialGrid {
        level_index: u16,
        role: GridOrderRole,
    },
    Replacement {
        level_index: u16,
        obligation_ids: Vec<u64>,
    },
}

impl StrategyOrderPurpose {
    fn level_index(&self) -> Option<u16> {
        match self {
            Self::Opening | Self::RiskClose => None,
            Self::InitialGrid { level_index, .. } | Self::Replacement { level_index, .. } => {
                Some(*level_index)
            }
        }
    }

    fn is_initial_grid(&self) -> bool {
        matches!(self, Self::InitialGrid { .. })
    }

    fn is_risk_close(&self) -> bool {
        matches!(self, Self::RiskClose)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StrategyOrderTracking {
    Dormant,
    Ready,
    Intent { state: IntentState },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyOrderRecord {
    pub client_order_id: ClientOrderId,
    pub shape: OrderShape,
    pub purpose: StrategyOrderPurpose,
    pub tracking: StrategyOrderTracking,
    pub exchange_order_id: Option<String>,
    pub cumulative_quantity: Decimal,
    pub cumulative_quote: Decimal,
    pub cumulative_fee: Decimal,
    #[serde(default)]
    pub execution_audit: Option<ExecutionAuditRecord>,
    pub terminal_status: Option<TerminalOrderStatus>,
    pub terminal_processed: bool,
    pub completed_pair_counted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LevelLot {
    pub quantity: Decimal,
    pub entry_value: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NeutralLot {
    pub id: u64,
    pub signed_quantity: Decimal,
    pub entry_value: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryExecutionEvent {
    pub sequence: u64,
    pub source_client_order_id: ClientOrderId,
    pub quantity: Decimal,
    pub quote: Decimal,
    #[serde(default)]
    pub exchange_trade_id: Option<String>,
    #[serde(default)]
    pub execution_time_ms: Option<u64>,
    pub applied_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementObligationKind {
    Counter,
    RestoreCancelledRemainder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplacementObligation {
    pub id: u64,
    pub kind: ReplacementObligationKind,
    pub source_client_order_id: ClientOrderId,
    pub level_index: u16,
    pub shape: OrderShape,
    pub created_at_ms: u64,
    pub assigned_client_order_id: Option<ClientOrderId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyState {
    pub version: u8,
    pub revision: u64,
    pub run_id: StrategyRunId,
    pub config: GridConfig,
    pub instrument_rules: InstrumentRules,
    pub exchange: Exchange,
    pub symbol: String,
    pub direction: Direction,
    pub plan: GridPlan,
    pub lifecycle: StrategyLifecycle,
    pub baseline: PositionBaseline,
    pub grid_position_net_quantity: Decimal,
    pub opening_filled_quantity: Decimal,
    pub opening_filled_value: Decimal,
    pub orders: BTreeMap<ClientOrderId, StrategyOrderRecord>,
    pub lots_by_level: BTreeMap<u16, LevelLot>,
    #[serde(default)]
    pub neutral_lots: BTreeMap<u64, NeutralLot>,
    #[serde(default)]
    pub inventory_events: BTreeMap<u64, InventoryExecutionEvent>,
    pub replacement_obligations: BTreeMap<u64, ReplacementObligation>,
    pub next_order_sequence: u64,
    pub next_obligation_sequence: u64,
    #[serde(default = "one_u64")]
    pub next_neutral_lot_sequence: u64,
    #[serde(default = "one_u64")]
    pub next_inventory_event_sequence: u64,
    pub initial_deployment_complete: bool,
    #[serde(default)]
    pub risk_exit_reason: Option<RiskExitReason>,
    #[serde(default)]
    pub risk_trigger_mark_price: Option<Decimal>,
    #[serde(default)]
    pub trigger_armed_price: Option<Decimal>,
    #[serde(default)]
    pub trigger_observed_price: Option<Decimal>,
    #[serde(default)]
    pub triggered_at_ms: Option<u64>,
    pub completed_pairs: u64,
    pub gross_realized_profit: Decimal,
    pub total_volume: Decimal,
    pub total_fee: Decimal,
    pub failure: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TriggerActivation {
    pub(crate) armed_price: Decimal,
    pub(crate) observed_price: Decimal,
    pub(crate) triggered_at_ms: u64,
}

impl StrategyState {
    pub fn from_plan(
        run_id: StrategyRunId,
        config: GridConfig,
        instrument_rules: InstrumentRules,
        plan: GridPlan,
        baseline: PositionBaseline,
        now_ms: u64,
    ) -> Result<Self, StrategyStateError> {
        Self::from_plan_internal(
            run_id,
            config,
            instrument_rules,
            plan,
            baseline,
            None,
            now_ms,
        )
    }

    pub(crate) fn from_triggered_plan(
        run_id: StrategyRunId,
        config: GridConfig,
        instrument_rules: InstrumentRules,
        plan: GridPlan,
        baseline: PositionBaseline,
        trigger_activation: TriggerActivation,
    ) -> Result<Self, StrategyStateError> {
        let now_ms = trigger_activation.triggered_at_ms;
        Self::from_plan_internal(
            run_id,
            config,
            instrument_rules,
            plan,
            baseline,
            Some(trigger_activation),
            now_ms,
        )
    }

    fn from_plan_internal(
        run_id: StrategyRunId,
        config: GridConfig,
        instrument_rules: InstrumentRules,
        plan: GridPlan,
        baseline: PositionBaseline,
        trigger_activation: Option<TriggerActivation>,
        now_ms: u64,
    ) -> Result<Self, StrategyStateError> {
        config
            .validate()
            .map_err(StrategyStateError::InvalidConfig)?;
        instrument_rules
            .validate()
            .map_err(StrategyStateError::InvalidInstrument)?;
        let exchange = config.exchange.ok_or(StrategyStateError::MissingExchange)?;
        let symbol = config.symbol.clone();
        let direction = config.direction;
        validate_symbol(&symbol)?;
        baseline.validate()?;
        baseline.validate_for_direction(direction)?;
        plan.validate_snapshot(&config, &instrument_rules)
            .map_err(StrategyStateError::InvalidPlan)?;
        validate_risk_prices(&config, plan.reference_price)?;
        match (config.trigger_price, trigger_activation) {
            (None, None) => {}
            (Some(_), Some(activation))
                if activation.armed_price > Decimal::ZERO
                    && activation.observed_price > Decimal::ZERO => {}
            _ => return Err(StrategyStateError::InvalidTriggerActivation),
        }
        let lifecycle = if plan.opening_order.is_some() {
            StrategyLifecycle::AwaitingOpening
        } else {
            StrategyLifecycle::DeployingGrid
        };
        let mut state = Self {
            version: 1,
            revision: 0,
            run_id,
            config,
            instrument_rules,
            exchange,
            symbol,
            direction,
            plan,
            lifecycle,
            baseline,
            grid_position_net_quantity: Decimal::ZERO,
            opening_filled_quantity: Decimal::ZERO,
            opening_filled_value: Decimal::ZERO,
            orders: BTreeMap::new(),
            lots_by_level: BTreeMap::new(),
            neutral_lots: BTreeMap::new(),
            inventory_events: BTreeMap::new(),
            replacement_obligations: BTreeMap::new(),
            next_order_sequence: 1,
            next_obligation_sequence: 1,
            next_neutral_lot_sequence: 1,
            next_inventory_event_sequence: 1,
            initial_deployment_complete: false,
            risk_exit_reason: None,
            risk_trigger_mark_price: None,
            trigger_armed_price: trigger_activation.map(|activation| activation.armed_price),
            trigger_observed_price: trigger_activation.map(|activation| activation.observed_price),
            triggered_at_ms: trigger_activation.map(|activation| activation.triggered_at_ms),
            completed_pairs: 0,
            gross_realized_profit: Decimal::ZERO,
            total_volume: Decimal::ZERO,
            total_fee: Decimal::ZERO,
            failure: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        state.prepare_initial_orders()?;
        state.validate()?;
        Ok(state)
    }

    pub fn expected_exchange_position(&self) -> Result<Decimal, StrategyStateError> {
        self.baseline
            .signed_quantity
            .checked_add(self.grid_position_net_quantity)
            .ok_or(StrategyStateError::NumericOverflow(
                "expected exchange position",
            ))
    }

    /// Values only inventory created by this strategy. The pre-existing account
    /// baseline is deliberately excluded because one-way exchanges merge both
    /// positions into one exchange-level average entry price.
    pub fn grid_unrealized_profit(
        &self,
        mark_price: Decimal,
    ) -> Result<Decimal, StrategyStateError> {
        if mark_price <= Decimal::ZERO {
            return Err(StrategyStateError::InvalidMarketPrice);
        }
        match self.direction {
            Direction::Long | Direction::Short => {
                if (self.direction == Direction::Long
                    && self.grid_position_net_quantity < Decimal::ZERO)
                    || (self.direction == Direction::Short
                        && self.grid_position_net_quantity > Decimal::ZERO)
                {
                    return Err(StrategyStateError::GridPositionDirectionMismatch);
                }
                let mut quantity = Decimal::ZERO;
                let mut entry_value = Decimal::ZERO;
                for lot in self.lots_by_level.values() {
                    if lot.quantity <= Decimal::ZERO || lot.entry_value <= Decimal::ZERO {
                        return Err(StrategyStateError::InvalidLevelLot);
                    }
                    quantity = quantity.checked_add(lot.quantity).ok_or(
                        StrategyStateError::NumericOverflow("grid unrealized quantity"),
                    )?;
                    entry_value = entry_value.checked_add(lot.entry_value).ok_or(
                        StrategyStateError::NumericOverflow("grid unrealized entry value"),
                    )?;
                }
                if quantity != self.grid_position_net_quantity.abs() {
                    return Err(StrategyStateError::LevelLotCoverageMismatch);
                }
                let mark_value =
                    mark_price
                        .checked_mul(quantity)
                        .ok_or(StrategyStateError::NumericOverflow(
                            "grid unrealized mark value",
                        ))?;
                if self.direction == Direction::Long {
                    mark_value.checked_sub(entry_value)
                } else {
                    entry_value.checked_sub(mark_value)
                }
                .ok_or(StrategyStateError::NumericOverflow(
                    "grid unrealized profit",
                ))
            }
            Direction::Neutral => {
                if self.grid_position_net_quantity.is_zero() && !self.neutral_lots.is_empty() {
                    return Err(StrategyStateError::NeutralLotCoverageMismatch);
                }
                let mut signed_quantity = Decimal::ZERO;
                let mut unrealized_profit = Decimal::ZERO;
                for lot in self.neutral_lots.values() {
                    if lot.signed_quantity.is_zero() || lot.entry_value <= Decimal::ZERO {
                        return Err(StrategyStateError::InvalidNeutralLot);
                    }
                    if (self.grid_position_net_quantity > Decimal::ZERO
                        && lot.signed_quantity <= Decimal::ZERO)
                        || (self.grid_position_net_quantity < Decimal::ZERO
                            && lot.signed_quantity >= Decimal::ZERO)
                    {
                        return Err(StrategyStateError::NeutralLotCoverageMismatch);
                    }
                    signed_quantity = signed_quantity.checked_add(lot.signed_quantity).ok_or(
                        StrategyStateError::NumericOverflow("neutral unrealized quantity"),
                    )?;
                    let mark_value = mark_price.checked_mul(lot.signed_quantity.abs()).ok_or(
                        StrategyStateError::NumericOverflow("neutral unrealized mark value"),
                    )?;
                    let lot_profit = if lot.signed_quantity > Decimal::ZERO {
                        mark_value.checked_sub(lot.entry_value)
                    } else {
                        lot.entry_value.checked_sub(mark_value)
                    }
                    .ok_or(StrategyStateError::NumericOverflow(
                        "neutral unrealized lot profit",
                    ))?;
                    unrealized_profit = unrealized_profit.checked_add(lot_profit).ok_or(
                        StrategyStateError::NumericOverflow("neutral unrealized profit"),
                    )?;
                }
                if signed_quantity != self.grid_position_net_quantity {
                    return Err(StrategyStateError::NeutralLotCoverageMismatch);
                }
                Ok(unrealized_profit)
            }
        }
    }

    pub fn ready_intents(&self, now_ms: u64) -> Result<Vec<OrderIntent>, StrategyStateError> {
        if matches!(
            self.lifecycle,
            StrategyLifecycle::StopRequested
                | StrategyLifecycle::Stopped
                | StrategyLifecycle::Failed
                | StrategyLifecycle::Closed
        ) {
            return Ok(Vec::new());
        }
        self.orders
            .values()
            .filter(|order| {
                order.tracking == StrategyOrderTracking::Ready
                    && (self.lifecycle != StrategyLifecycle::RiskExitRequested
                        || order.purpose.is_risk_close())
            })
            .map(|order| {
                OrderIntent::prepare(
                    order.client_order_id.clone(),
                    self.exchange,
                    order.shape.clone(),
                    now_ms,
                )
                .map_err(StrategyStateError::InvalidOrderIntent)
            })
            .collect()
    }

    pub fn validate(&self) -> Result<(), StrategyStateError> {
        if self.version != 1 {
            return Err(StrategyStateError::UnsupportedVersion(self.version));
        }
        StrategyRunId::parse(self.run_id.as_str())?;
        self.config
            .validate()
            .map_err(StrategyStateError::InvalidConfig)?;
        self.instrument_rules
            .validate()
            .map_err(StrategyStateError::InvalidInstrument)?;
        validate_symbol(&self.symbol)?;
        if self.config.exchange != Some(self.exchange)
            || self.config.symbol != self.symbol
            || self.config.direction != self.direction
        {
            return Err(StrategyStateError::ConfigIdentityMismatch);
        }
        self.baseline.validate()?;
        self.baseline.validate_for_direction(self.direction)?;
        match self.failure.as_deref() {
            Some(message) if message.trim().is_empty() => {
                return Err(StrategyStateError::InvalidFailureState);
            }
            Some(_)
                if !matches!(
                    self.lifecycle,
                    StrategyLifecycle::Failed
                        | StrategyLifecycle::Stopped
                        | StrategyLifecycle::Closed
                ) =>
            {
                return Err(StrategyStateError::InvalidFailureState);
            }
            None if self.lifecycle == StrategyLifecycle::Failed => {
                return Err(StrategyStateError::InvalidFailureState);
            }
            _ => {}
        }
        self.plan
            .validate_snapshot(&self.config, &self.instrument_rules)
            .map_err(StrategyStateError::InvalidPlan)?;
        validate_risk_prices(&self.config, self.plan.reference_price)?;
        if self
            .risk_trigger_mark_price
            .is_some_and(|price| price <= Decimal::ZERO)
            || (self.lifecycle == StrategyLifecycle::RiskExitRequested
                && (self.risk_exit_reason.is_none() || self.risk_trigger_mark_price.is_none()))
        {
            return Err(StrategyStateError::InvalidRiskExitState);
        }
        let trigger_metadata_complete = self.trigger_armed_price.is_some()
            && self.trigger_observed_price.is_some()
            && self.triggered_at_ms.is_some();
        if self.config.trigger_price.is_some() != trigger_metadata_complete
            || self
                .trigger_armed_price
                .is_some_and(|price| price <= Decimal::ZERO)
            || self
                .trigger_observed_price
                .is_some_and(|price| price <= Decimal::ZERO)
            || self
                .triggered_at_ms
                .is_some_and(|triggered_at| triggered_at > self.updated_at_ms)
        {
            return Err(StrategyStateError::InvalidTriggerActivation);
        }
        if self.lifecycle == StrategyLifecycle::Closed
            && (!self.grid_position_net_quantity.is_zero()
                || !self.lots_by_level.is_empty()
                || !self.neutral_lots.is_empty()
                || self.orders.values().any(order_may_still_be_live))
        {
            return Err(StrategyStateError::CannotCloseStrategy);
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err(StrategyStateError::TimestampRegression);
        }
        match self.direction {
            Direction::Long if self.grid_position_net_quantity < Decimal::ZERO => {
                return Err(StrategyStateError::GridPositionDirectionMismatch);
            }
            Direction::Short if self.grid_position_net_quantity > Decimal::ZERO => {
                return Err(StrategyStateError::GridPositionDirectionMismatch);
            }
            _ => {}
        }

        for (key, order) in &self.orders {
            if key != &order.client_order_id || order.shape.symbol != self.symbol {
                return Err(StrategyStateError::OrderIdentityMismatch);
            }
            order
                .shape
                .validate()
                .map_err(StrategyStateError::InvalidOrderIntent)?;
            validate_order_against_instrument(order, &self.instrument_rules)?;
            if let StrategyOrderTracking::Intent { state } = &order.tracking {
                state
                    .validate()
                    .map_err(StrategyStateError::InvalidOrderIntent)?;
            }
            if order.cumulative_quantity < Decimal::ZERO
                || order.cumulative_quantity > order.shape.quantity
                || order.cumulative_quote < Decimal::ZERO
                || order.cumulative_fee < Decimal::ZERO
                || order.cumulative_quantity.is_zero() != order.cumulative_quote.is_zero()
            {
                return Err(StrategyStateError::InvalidExecutionTotals);
            }
            if let Some(audit) = &order.execution_audit {
                validate_execution_audit_payload(
                    self.exchange,
                    order,
                    audit,
                    order.cumulative_quantity,
                    order.cumulative_quote,
                    order.cumulative_fee,
                    order.terminal_status,
                )?;
                if audit.synced_at_ms > self.updated_at_ms {
                    return Err(StrategyStateError::InvalidExecutionAudit);
                }
            }
            if order.terminal_processed != order.terminal_status.is_some() {
                return Err(StrategyStateError::TerminalProcessingMismatch);
            }
            match (
                &order.tracking,
                order.terminal_status,
                order.terminal_processed,
            ) {
                (
                    StrategyOrderTracking::Intent {
                        state:
                            IntentState::Terminal {
                                status: tracked, ..
                            },
                    },
                    Some(recorded),
                    true,
                ) if *tracked == recorded => {}
                (
                    StrategyOrderTracking::Intent {
                        state: IntentState::Terminal { .. },
                    },
                    None,
                    false,
                ) => {}
                (
                    StrategyOrderTracking::Intent {
                        state: IntentState::Terminal { .. },
                    },
                    _,
                    _,
                )
                | (_, Some(_), true) => {
                    return Err(StrategyStateError::TerminalProcessingMismatch);
                }
                _ => {}
            }
            if order
                .exchange_order_id
                .as_ref()
                .is_some_and(|exchange_order_id| exchange_order_id.trim().is_empty())
                || (order.cumulative_quantity > Decimal::ZERO && order.exchange_order_id.is_none())
            {
                return Err(StrategyStateError::ExchangeOrderIdentityMismatch);
            }
            if let StrategyOrderTracking::Intent { state } = &order.tracking
                && let Some(exchange_order_id) = state.exchange_order_id()
                && order.exchange_order_id.as_deref() != Some(exchange_order_id)
            {
                return Err(StrategyStateError::ExchangeOrderIdentityMismatch);
            }
            if let StrategyOrderPurpose::Replacement {
                level_index,
                obligation_ids,
            } = &order.purpose
                && (obligation_ids.is_empty()
                    || combined_obligation_shape(self, obligation_ids).as_ref()
                        != Some(&order.shape)
                    || obligation_ids.iter().any(|id| {
                        self.replacement_obligations
                            .get(id)
                            .is_none_or(|obligation| {
                                obligation.level_index != *level_index
                                    || obligation.assigned_client_order_id.as_ref()
                                        != Some(&order.client_order_id)
                            })
                    }))
            {
                return Err(StrategyStateError::ReplacementOrderMismatch);
            }
        }
        validate_strategy_trade_id_ownership(self, None)?;
        validate_initial_grid_ledger(self)?;
        validate_initial_deployment_state(self)?;
        validate_append_only_sequences(self)?;
        let opening_orders = self
            .orders
            .values()
            .filter(|order| order.purpose == StrategyOrderPurpose::Opening)
            .collect::<Vec<_>>();
        let opening_quantity = opening_orders
            .iter()
            .map(|order| order.cumulative_quantity)
            .try_fold(Decimal::ZERO, |total, quantity| total.checked_add(quantity))
            .ok_or(StrategyStateError::NumericOverflow(
                "opening execution quantity",
            ))?;
        let opening_value = opening_orders
            .iter()
            .map(|order| order.cumulative_quote)
            .try_fold(Decimal::ZERO, |total, value| total.checked_add(value))
            .ok_or(StrategyStateError::NumericOverflow(
                "opening execution value",
            ))?;
        if opening_quantity != self.opening_filled_quantity
            || opening_value != self.opening_filled_value
            || self.opening_filled_quantity.is_zero() != self.opening_filled_value.is_zero()
        {
            return Err(StrategyStateError::OpeningAccountingMismatch);
        }
        match &self.plan.opening_order {
            None if !opening_orders.is_empty()
                || !self.opening_filled_quantity.is_zero()
                || !self.opening_filled_value.is_zero() =>
            {
                return Err(StrategyStateError::OpeningAccountingMismatch);
            }
            None => {}
            Some(planned) => {
                if opening_orders.is_empty()
                    || opening_orders.iter().any(|order| {
                        order.shape.side != planned.side
                            || order.shape.price != planned.price
                            || order.shape.quantity > planned.quantity
                            || order.shape.reduce_only
                            || order.shape.kind != planned.kind
                            || order.shape.time_in_force != planned.time_in_force
                    })
                {
                    return Err(StrategyStateError::OpeningOrderMismatch);
                }
                let unresolved = opening_orders
                    .iter()
                    .filter(|order| order.terminal_status.is_none())
                    .count();
                if unresolved > 1
                    || (self.lifecycle != StrategyLifecycle::Failed
                        && self.opening_filled_quantity > planned.quantity)
                {
                    return Err(StrategyStateError::OpeningAccountingMismatch);
                }
                if self.lifecycle == StrategyLifecycle::AwaitingOpening
                    && (self.opening_filled_quantity >= planned.quantity
                        || unresolved != 1
                        || self.initial_deployment_complete
                        || self.orders.values().any(|order| {
                            order.purpose.is_initial_grid()
                                && order.tracking != StrategyOrderTracking::Dormant
                        }))
                {
                    return Err(StrategyStateError::OpeningAccountingMismatch);
                }
                if matches!(
                    self.lifecycle,
                    StrategyLifecycle::DeployingGrid | StrategyLifecycle::Running
                ) && (self.opening_filled_quantity != planned.quantity
                    || unresolved != 0
                    || self.orders.values().any(|order| {
                        order.purpose.is_initial_grid()
                            && order.tracking == StrategyOrderTracking::Dormant
                    }))
                {
                    return Err(StrategyStateError::OpeningAccountingMismatch);
                }
            }
        }
        for lot in self.lots_by_level.values() {
            if lot.quantity <= Decimal::ZERO || lot.entry_value <= Decimal::ZERO {
                return Err(StrategyStateError::InvalidLevelLot);
            }
        }
        for (id, lot) in &self.neutral_lots {
            if id != &lot.id
                || lot.signed_quantity.is_zero()
                || lot.entry_value <= Decimal::ZERO
                || *id >= self.next_neutral_lot_sequence
            {
                return Err(StrategyStateError::InvalidNeutralLot);
            }
        }
        if self.direction == Direction::Neutral {
            if !self.lots_by_level.is_empty() {
                return Err(StrategyStateError::NeutralLotCoverageMismatch);
            }
            if self.lifecycle != StrategyLifecycle::Failed {
                let signed_quantity = self
                    .neutral_lots
                    .values()
                    .map(|lot| lot.signed_quantity)
                    .try_fold(Decimal::ZERO, |total, quantity| total.checked_add(quantity))
                    .ok_or(StrategyStateError::NumericOverflow("neutral lot quantity"))?;
                if signed_quantity != self.grid_position_net_quantity
                    || self.neutral_lots.values().any(|lot| {
                        (self.grid_position_net_quantity > Decimal::ZERO
                            && lot.signed_quantity <= Decimal::ZERO)
                            || (self.grid_position_net_quantity < Decimal::ZERO
                                && lot.signed_quantity >= Decimal::ZERO)
                    })
                {
                    return Err(StrategyStateError::NeutralLotCoverageMismatch);
                }
            }
        } else if !self.neutral_lots.is_empty() {
            return Err(StrategyStateError::NeutralLotCoverageMismatch);
        } else if self.lifecycle != StrategyLifecycle::Failed {
            let lot_quantity = self
                .lots_by_level
                .values()
                .map(|lot| lot.quantity)
                .try_fold(Decimal::ZERO, |total, quantity| total.checked_add(quantity))
                .ok_or(StrategyStateError::NumericOverflow("level lot quantity"))?;
            if lot_quantity != self.grid_position_net_quantity.abs() {
                return Err(StrategyStateError::LevelLotCoverageMismatch);
            }
        }
        for (id, obligation) in &self.replacement_obligations {
            if id != &obligation.id || obligation.shape.symbol != self.symbol {
                return Err(StrategyStateError::ObligationIdentityMismatch);
            }
            obligation
                .shape
                .validate()
                .map_err(StrategyStateError::InvalidOrderIntent)?;
            validate_obligation_against_instrument(obligation, &self.instrument_rules)?;
            if let Some(client_order_id) = &obligation.assigned_client_order_id {
                let Some(order) = self.orders.get(client_order_id) else {
                    return Err(StrategyStateError::MissingAssignedReplacement);
                };
                if !matches!(
                    &order.purpose,
                    StrategyOrderPurpose::Replacement {
                        level_index,
                        obligation_ids,
                    } if obligation_ids.contains(id) && *level_index == obligation.level_index
                ) {
                    return Err(StrategyStateError::MissingAssignedReplacement);
                }
            }
        }
        validate_replacement_obligation_ledger(self)?;
        validate_aggregate_accounting(self)?;
        validate_inventory_event_ledger(self)?;
        self.expected_exchange_position()?;
        Ok(())
    }

    fn prepare_initial_orders(&mut self) -> Result<(), StrategyStateError> {
        if let Some(opening) = self.plan.opening_order.clone() {
            let client_order_id = self.next_client_order_id("o", None, opening.side)?;
            self.insert_order(StrategyOrderRecord {
                client_order_id,
                shape: OrderShape {
                    symbol: self.symbol.clone(),
                    side: opening.side,
                    price: opening.price,
                    quantity: opening.quantity,
                    reduce_only: false,
                    kind: opening.kind,
                    time_in_force: opening.time_in_force,
                },
                purpose: StrategyOrderPurpose::Opening,
                tracking: StrategyOrderTracking::Ready,
                exchange_order_id: None,
                cumulative_quantity: Decimal::ZERO,
                cumulative_quote: Decimal::ZERO,
                cumulative_fee: Decimal::ZERO,
                execution_audit: None,
                terminal_status: None,
                terminal_processed: false,
                completed_pair_counted: false,
            })?;
        }

        for planned in self.plan.grid_orders.clone() {
            let client_order_id =
                self.next_client_order_id("g", Some(planned.level_index), planned.side)?;
            self.insert_order(StrategyOrderRecord {
                client_order_id,
                shape: OrderShape {
                    symbol: self.symbol.clone(),
                    side: planned.side,
                    price: Some(planned.price),
                    quantity: planned.quantity,
                    reduce_only: planned.reduce_only,
                    kind: OrderKind::Limit,
                    time_in_force: planned.time_in_force,
                },
                purpose: StrategyOrderPurpose::InitialGrid {
                    level_index: planned.level_index,
                    role: planned.role,
                },
                tracking: if self.plan.opening_order.is_some() {
                    StrategyOrderTracking::Dormant
                } else {
                    StrategyOrderTracking::Ready
                },
                exchange_order_id: None,
                cumulative_quantity: Decimal::ZERO,
                cumulative_quote: Decimal::ZERO,
                cumulative_fee: Decimal::ZERO,
                execution_audit: None,
                terminal_status: None,
                terminal_processed: false,
                completed_pair_counted: false,
            })?;
        }
        Ok(())
    }

    fn next_client_order_id(
        &mut self,
        prefix: &str,
        level_index: Option<u16>,
        side: OrderSide,
    ) -> Result<ClientOrderId, StrategyStateError> {
        let sequence = self.next_order_sequence;
        self.next_order_sequence = self
            .next_order_sequence
            .checked_add(1)
            .ok_or(StrategyStateError::NumericOverflow("order sequence"))?;
        generated_client_order_id(&self.run_id, prefix, level_index, side, sequence)
    }

    fn insert_order(&mut self, order: StrategyOrderRecord) -> Result<(), StrategyStateError> {
        if self.orders.contains_key(&order.client_order_id) {
            return Err(StrategyStateError::DuplicateOrderIdentity);
        }
        self.orders.insert(order.client_order_id.clone(), order);
        Ok(())
    }

    fn fail(&mut self, message: impl Into<String>) {
        self.lifecycle = StrategyLifecycle::Failed;
        self.failure = Some(message.into());
    }
}

fn validate_initial_grid_ledger(state: &StrategyState) -> Result<(), StrategyStateError> {
    let initial_orders = state
        .orders
        .values()
        .filter(|order| order.purpose.is_initial_grid())
        .collect::<Vec<_>>();
    if initial_orders.len() != state.plan.grid_orders.len()
        || initial_orders.iter().any(|order| {
            state
                .plan
                .grid_orders
                .iter()
                .filter(|planned| initial_grid_order_matches(state, order, planned))
                .count()
                != 1
        })
        || state.plan.grid_orders.iter().any(|planned| {
            initial_orders
                .iter()
                .filter(|order| initial_grid_order_matches(state, order, planned))
                .count()
                != 1
        })
    {
        return Err(StrategyStateError::InitialGridOrderMismatch);
    }
    Ok(())
}

fn initial_grid_order_matches(
    state: &StrategyState,
    order: &StrategyOrderRecord,
    planned: &PlannedGridOrder,
) -> bool {
    matches!(
        order.purpose,
        StrategyOrderPurpose::InitialGrid { level_index, role }
            if level_index == planned.level_index && role == planned.role
    ) && order.shape.symbol == state.symbol
        && order.shape.side == planned.side
        && order.shape.price == Some(planned.price)
        && order.shape.quantity == planned.quantity
        && order.shape.reduce_only == planned.reduce_only
        && order.shape.kind == OrderKind::Limit
        && order.shape.time_in_force == planned.time_in_force
}

fn validate_initial_deployment_state(state: &StrategyState) -> Result<(), StrategyStateError> {
    let lifecycle_flag_mismatch = match state.lifecycle {
        StrategyLifecycle::AwaitingOpening | StrategyLifecycle::DeployingGrid => {
            state.initial_deployment_complete
        }
        StrategyLifecycle::Running => !state.initial_deployment_complete,
        StrategyLifecycle::RiskExitRequested
        | StrategyLifecycle::StopRequested
        | StrategyLifecycle::Stopped
        | StrategyLifecycle::Failed
        | StrategyLifecycle::Closed => false,
    };
    let lifecycle_order_mismatch = (state.lifecycle == StrategyLifecycle::AwaitingOpening
        && state.plan.opening_order.is_none())
        || (state.lifecycle == StrategyLifecycle::Running
            && state.orders.values().any(|order| {
                order.purpose.is_initial_grid()
                    && matches!(
                        order.tracking,
                        StrategyOrderTracking::Dormant | StrategyOrderTracking::Ready
                    )
            }));
    if lifecycle_flag_mismatch || lifecycle_order_mismatch {
        return Err(StrategyStateError::InitialDeploymentStateMismatch);
    }
    Ok(())
}

fn validate_append_only_sequences(state: &StrategyState) -> Result<(), StrategyStateError> {
    let expected_order_sequence = u64::try_from(state.orders.len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(StrategyStateError::NumericOverflow("order ledger length"))?;
    if state.next_order_sequence != expected_order_sequence {
        return Err(StrategyStateError::OrderSequenceMismatch);
    }
    let mut observed_order_sequences = BTreeSet::new();
    for order in state.orders.values() {
        let sequence = validate_order_identity_sequence(state, order)?;
        if !observed_order_sequences.insert(sequence) {
            return Err(StrategyStateError::OrderSequenceMismatch);
        }
    }
    if observed_order_sequences
        .iter()
        .copied()
        .ne(1..state.next_order_sequence)
    {
        return Err(StrategyStateError::OrderSequenceMismatch);
    }
    let expected_obligation_sequence = u64::try_from(state.replacement_obligations.len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(StrategyStateError::NumericOverflow(
            "replacement obligation ledger length",
        ))?;
    if state.next_obligation_sequence != expected_obligation_sequence {
        return Err(StrategyStateError::ObligationSequenceMismatch);
    }
    if state
        .replacement_obligations
        .keys()
        .copied()
        .ne(1..state.next_obligation_sequence)
    {
        return Err(StrategyStateError::ObligationSequenceMismatch);
    }
    Ok(())
}

fn validate_order_identity_sequence(
    state: &StrategyState,
    order: &StrategyOrderRecord,
) -> Result<u64, StrategyStateError> {
    let identity = order.client_order_id.as_str();
    let sequence_text = identity
        .rsplit_once('_')
        .map(|(_, sequence)| sequence)
        .ok_or(StrategyStateError::OrderSequenceMismatch)?;
    let sequence = sequence_text
        .parse::<u64>()
        .map_err(|_| StrategyStateError::OrderSequenceMismatch)?;
    if sequence == 0 || sequence.to_string() != sequence_text {
        return Err(StrategyStateError::OrderSequenceMismatch);
    }
    let (prefix, level_index) = match &order.purpose {
        StrategyOrderPurpose::Opening => ("o", None),
        StrategyOrderPurpose::RiskClose => ("c", None),
        StrategyOrderPurpose::InitialGrid { level_index, .. } => ("g", Some(*level_index)),
        StrategyOrderPurpose::Replacement { level_index, .. } => ("r", Some(*level_index)),
    };
    let expected = generated_client_order_id(
        &state.run_id,
        prefix,
        level_index,
        order.shape.side,
        sequence,
    )
    .map_err(|_| StrategyStateError::OrderSequenceMismatch)?;
    if order.client_order_id != expected {
        return Err(StrategyStateError::OrderSequenceMismatch);
    }
    Ok(sequence)
}

fn generated_client_order_id(
    run_id: &StrategyRunId,
    prefix: &str,
    level_index: Option<u16>,
    side: OrderSide,
    sequence: u64,
) -> Result<ClientOrderId, StrategyStateError> {
    let side = match side {
        OrderSide::Buy => "B",
        OrderSide::Sell => "S",
    };
    let value = match level_index {
        Some(level) => format!("{prefix}_{}_{level}_{side}_{sequence}", run_id.as_str()),
        None => format!("{prefix}_{}_{side}_{sequence}", run_id.as_str()),
    };
    ClientOrderId::parse(value).map_err(StrategyStateError::InvalidOrderIntent)
}

fn validate_symbol(symbol: &str) -> Result<(), StrategyStateError> {
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(StrategyStateError::InvalidSymbol);
    }
    Ok(())
}

fn validate_risk_prices(
    config: &GridConfig,
    reference_price: Decimal,
) -> Result<(), StrategyStateError> {
    let stop_loss_valid = config
        .stop_loss_price
        .is_none_or(|price| match config.direction {
            Direction::Long => price < reference_price,
            Direction::Short => price > reference_price,
            Direction::Neutral => price < config.lower_price,
        });
    let take_profit_valid = config
        .take_profit_price
        .is_none_or(|price| match config.direction {
            Direction::Long => price > reference_price,
            Direction::Short => price < reference_price,
            Direction::Neutral => price > config.upper_price,
        });
    if stop_loss_valid && take_profit_valid {
        Ok(())
    } else {
        Err(StrategyStateError::InvalidRiskPriceDirection)
    }
}

fn validate_order_against_instrument(
    order: &StrategyOrderRecord,
    rules: &InstrumentRules,
) -> Result<(), StrategyStateError> {
    let quantity_rules = match order.shape.kind {
        OrderKind::Limit => &rules.limit_quantity,
        OrderKind::Market => &rules.market_quantity,
    };
    if !quantity_rules.is_aligned(order.shape.quantity)
        || quantity_rules
            .max
            .is_some_and(|maximum| order.shape.quantity > maximum)
        || order.shape.quantity < quantity_rules.min
    {
        return Err(StrategyStateError::OrderViolatesInstrumentRules);
    }
    if let Some(price) = order.shape.price {
        if rules.floor_price(price) != Some(price) {
            return Err(StrategyStateError::OrderViolatesInstrumentRules);
        }
        if !order.shape.reduce_only
            && price
                .checked_mul(order.shape.quantity)
                .is_none_or(|notional| notional < rules.min_notional)
        {
            return Err(StrategyStateError::OrderViolatesInstrumentRules);
        }
    }
    Ok(())
}

fn validate_obligation_against_instrument(
    obligation: &ReplacementObligation,
    rules: &InstrumentRules,
) -> Result<(), StrategyStateError> {
    let quantity = obligation.shape.quantity;
    if obligation.shape.kind != OrderKind::Limit
        || !rules.limit_quantity.is_aligned(quantity)
        || rules
            .limit_quantity
            .max
            .is_some_and(|maximum| quantity > maximum)
        || obligation
            .shape
            .price
            .is_none_or(|price| rules.floor_price(price) != Some(price))
    {
        return Err(StrategyStateError::OrderViolatesInstrumentRules);
    }
    Ok(())
}

fn validate_replacement_obligation_ledger(state: &StrategyState) -> Result<(), StrategyStateError> {
    let mut counter_quantities = BTreeMap::<ClientOrderId, Decimal>::new();
    let mut cancelled_remainder_sources = BTreeSet::<ClientOrderId>::new();

    for obligation in state.replacement_obligations.values() {
        if obligation.created_at_ms < state.created_at_ms
            || obligation.created_at_ms > state.updated_at_ms
        {
            return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
        }
        let source = state
            .orders
            .get(&obligation.source_client_order_id)
            .ok_or(StrategyStateError::ReplacementObligationLedgerMismatch)?;
        let source_level = source
            .purpose
            .level_index()
            .ok_or(StrategyStateError::ReplacementObligationLedgerMismatch)?;
        if source_level != obligation.level_index {
            return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
        }

        match obligation.kind {
            ReplacementObligationKind::Counter => {
                let expected = counter_shape(
                    state,
                    source_level,
                    &source.shape,
                    obligation.shape.quantity,
                )
                .map_err(|_| StrategyStateError::ReplacementObligationLedgerMismatch)?;
                if obligation.shape != expected {
                    return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
                }
                let total = counter_quantities
                    .entry(obligation.source_client_order_id.clone())
                    .or_insert(Decimal::ZERO);
                *total = total.checked_add(obligation.shape.quantity).ok_or(
                    StrategyStateError::NumericOverflow("replacement counter quantity"),
                )?;
            }
            ReplacementObligationKind::RestoreCancelledRemainder => {
                if !matches!(
                    source.terminal_status,
                    Some(TerminalOrderStatus::Cancelled | TerminalOrderStatus::Expired)
                ) || !cancelled_remainder_sources
                    .insert(obligation.source_client_order_id.clone())
                {
                    return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
                }
                let remaining = source
                    .shape
                    .quantity
                    .checked_sub(source.cumulative_quantity)
                    .filter(|quantity| *quantity > Decimal::ZERO)
                    .ok_or(StrategyStateError::ReplacementObligationLedgerMismatch)?;
                let mut expected = source.shape.clone();
                expected.quantity = remaining;
                if obligation.shape != expected {
                    return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
                }
            }
        }
    }

    let complete_normal_ledger = normal_grid_replacements_enabled(state.lifecycle);
    for order in state
        .orders
        .values()
        .filter(|order| order.purpose.level_index().is_some())
    {
        let counter_quantity = counter_quantities
            .get(&order.client_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        if counter_quantity > order.cumulative_quantity
            || (complete_normal_ledger && counter_quantity != order.cumulative_quantity)
        {
            return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
        }
        if complete_normal_ledger {
            let needs_remainder = matches!(
                order.terminal_status,
                Some(TerminalOrderStatus::Cancelled | TerminalOrderStatus::Expired)
            ) && order.cumulative_quantity < order.shape.quantity;
            if cancelled_remainder_sources.contains(&order.client_order_id) != needs_remainder {
                return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
            }
        }
    }

    let mut visiting = BTreeSet::new();
    let mut verified = BTreeSet::new();
    for order in state
        .orders
        .values()
        .filter(|order| matches!(order.purpose, StrategyOrderPurpose::Replacement { .. }))
    {
        if !replacement_order_has_initial_provenance(
            state,
            &order.client_order_id,
            &mut visiting,
            &mut verified,
        ) {
            return Err(StrategyStateError::ReplacementObligationLedgerMismatch);
        }
    }
    Ok(())
}

fn validate_aggregate_accounting(state: &StrategyState) -> Result<(), StrategyStateError> {
    let mut expected_volume = Decimal::ZERO;
    let mut expected_fee = Decimal::ZERO;
    let mut buy_quote = Decimal::ZERO;
    let mut sell_quote = Decimal::ZERO;
    let mut expected_grid_position = Decimal::ZERO;
    let mut expected_completed_pairs = 0_u64;

    for order in state.orders.values() {
        expected_volume = expected_volume.checked_add(order.cumulative_quote).ok_or(
            StrategyStateError::NumericOverflow("aggregate trade volume"),
        )?;
        expected_fee = expected_fee
            .checked_add(order.cumulative_fee)
            .ok_or(StrategyStateError::NumericOverflow("aggregate trade fee"))?;
        match order.shape.side {
            OrderSide::Buy => {
                expected_grid_position = expected_grid_position
                    .checked_add(order.cumulative_quantity)
                    .ok_or(StrategyStateError::NumericOverflow(
                        "aggregate buy quantity",
                    ))?;
                buy_quote = buy_quote
                    .checked_add(order.cumulative_quote)
                    .ok_or(StrategyStateError::NumericOverflow("aggregate buy quote"))?;
            }
            OrderSide::Sell => {
                expected_grid_position = expected_grid_position
                    .checked_sub(order.cumulative_quantity)
                    .ok_or(StrategyStateError::NumericOverflow(
                        "aggregate sell quantity",
                    ))?;
                sell_quote = sell_quote
                    .checked_add(order.cumulative_quote)
                    .ok_or(StrategyStateError::NumericOverflow("aggregate sell quote"))?;
            }
        }

        let should_count_completed_pair = !matches!(
            order.purpose,
            StrategyOrderPurpose::Opening | StrategyOrderPurpose::RiskClose
        ) && order.shape.reduce_only
            && order.terminal_status.is_some()
            && order.cumulative_quantity > Decimal::ZERO;
        if order.completed_pair_counted != should_count_completed_pair {
            return Err(StrategyStateError::AggregateAccountingMismatch);
        }
        if should_count_completed_pair {
            expected_completed_pairs = expected_completed_pairs.checked_add(1).ok_or(
                StrategyStateError::NumericOverflow("aggregate completed pairs"),
            )?;
        }
    }

    if expected_volume != state.total_volume
        || expected_fee != state.total_fee
        || expected_completed_pairs != state.completed_pairs
        || expected_grid_position != state.grid_position_net_quantity
    {
        return Err(StrategyStateError::AggregateAccountingMismatch);
    }

    let remaining_entry_value = if state.direction == Direction::Neutral {
        state
            .neutral_lots
            .values()
            .map(|lot| lot.entry_value)
            .try_fold(Decimal::ZERO, |total, value| total.checked_add(value))
    } else {
        state
            .lots_by_level
            .values()
            .map(|lot| lot.entry_value)
            .try_fold(Decimal::ZERO, |total, value| total.checked_add(value))
    }
    .ok_or(StrategyStateError::NumericOverflow(
        "aggregate remaining entry value",
    ))?;
    let cash_flow =
        sell_quote
            .checked_sub(buy_quote)
            .ok_or(StrategyStateError::NumericOverflow(
                "aggregate execution cash flow",
            ))?;
    let expected_gross_profit = if state.grid_position_net_quantity > Decimal::ZERO {
        cash_flow.checked_add(remaining_entry_value)
    } else if state.grid_position_net_quantity < Decimal::ZERO {
        cash_flow.checked_sub(remaining_entry_value)
    } else {
        Some(cash_flow)
    }
    .ok_or(StrategyStateError::NumericOverflow(
        "aggregate gross realized profit",
    ))?;
    if expected_gross_profit != state.gross_realized_profit {
        return Err(StrategyStateError::AggregateAccountingMismatch);
    }
    Ok(())
}

fn validate_inventory_event_ledger(state: &StrategyState) -> Result<(), StrategyStateError> {
    if state.next_inventory_event_sequence == 0
        || state
            .inventory_events
            .keys()
            .copied()
            .ne(1..state.next_inventory_event_sequence)
    {
        return Err(StrategyStateError::InventoryEventLedgerMismatch);
    }

    let mut quantities_by_order = BTreeMap::<ClientOrderId, Decimal>::new();
    let mut quotes_by_order = BTreeMap::<ClientOrderId, Decimal>::new();
    let mut events_by_order = BTreeMap::<ClientOrderId, Vec<&InventoryExecutionEvent>>::new();
    let mut previous_applied_at_ms = state.created_at_ms;
    for (sequence, event) in &state.inventory_events {
        let evidence_is_valid = match (&event.exchange_trade_id, event.execution_time_ms) {
            (None, None) => true,
            (Some(trade_id), Some(execution_time_ms)) => {
                is_valid_trade_id(trade_id) && execution_time_ms > 0
            }
            _ => false,
        };
        if event.sequence != *sequence
            || event.quantity <= Decimal::ZERO
            || event.quote <= Decimal::ZERO
            || !evidence_is_valid
            || event.applied_at_ms < previous_applied_at_ms
            || event.applied_at_ms > state.updated_at_ms
            || !state.orders.contains_key(&event.source_client_order_id)
        {
            return Err(StrategyStateError::InventoryEventLedgerMismatch);
        }
        previous_applied_at_ms = event.applied_at_ms;
        let quantity = quantities_by_order
            .entry(event.source_client_order_id.clone())
            .or_insert(Decimal::ZERO);
        *quantity =
            quantity
                .checked_add(event.quantity)
                .ok_or(StrategyStateError::NumericOverflow(
                    "inventory event quantity",
                ))?;
        let quote = quotes_by_order
            .entry(event.source_client_order_id.clone())
            .or_insert(Decimal::ZERO);
        *quote = quote
            .checked_add(event.quote)
            .ok_or(StrategyStateError::NumericOverflow("inventory event quote"))?;
        events_by_order
            .entry(event.source_client_order_id.clone())
            .or_default()
            .push(event);
    }
    if state.orders.values().any(|order| {
        quantities_by_order
            .get(&order.client_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
            != order.cumulative_quantity
            || quotes_by_order
                .get(&order.client_order_id)
                .copied()
                .unwrap_or(Decimal::ZERO)
                != order.cumulative_quote
    }) {
        return Err(StrategyStateError::InventoryEventLedgerMismatch);
    }
    for order in state.orders.values() {
        let events = events_by_order
            .get(&order.client_order_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(audit) = &order.execution_audit {
            if events.len() != audit.snapshot.trades.len()
                || events
                    .iter()
                    .zip(&audit.snapshot.trades)
                    .any(|(event, trade)| {
                        event.exchange_trade_id.as_deref() != Some(trade.trade_id.as_str())
                            || event.execution_time_ms != Some(trade.trade_time_ms)
                            || event.quantity != trade.quantity
                            || event.quote != trade.quote_quantity
                    })
            {
                return Err(StrategyStateError::InventoryEventLedgerMismatch);
            }
        } else if events
            .iter()
            .any(|event| event.exchange_trade_id.is_some() || event.execution_time_ms.is_some())
        {
            return Err(StrategyStateError::InventoryEventLedgerMismatch);
        }
    }

    // A failed strategy is retained for diagnosis even when the execution that
    // caused the failure could not be represented by the inventory model.
    if state.lifecycle == StrategyLifecycle::Failed {
        return Ok(());
    }

    let accounting_events = inventory_events_in_accounting_order(state)
        .map_err(|_| StrategyStateError::InventoryEventLedgerMismatch)?;
    let mut replay = state.clone();
    InventoryAccountingSnapshot::empty().restore(&mut replay);
    for event in &accounting_events {
        let order = state
            .orders
            .get(&event.source_client_order_id)
            .ok_or(StrategyStateError::InventoryEventLedgerMismatch)?;
        apply_inventory_accounting(
            &mut replay,
            &order.purpose,
            &order.shape,
            event.quantity,
            event.quote,
        )
        .map_err(|_| StrategyStateError::InventoryEventLedgerMismatch)?;
    }

    if replay.grid_position_net_quantity != state.grid_position_net_quantity
        || replay.opening_filled_quantity != state.opening_filled_quantity
        || replay.opening_filled_value != state.opening_filled_value
        || replay.gross_realized_profit != state.gross_realized_profit
        || replay.next_neutral_lot_sequence != state.next_neutral_lot_sequence
    {
        return Err(StrategyStateError::InventoryEventLedgerMismatch);
    }
    if replay.lots_by_level != state.lots_by_level {
        return Err(StrategyStateError::LevelLotLedgerMismatch);
    }
    if replay.neutral_lots != state.neutral_lots {
        return Err(StrategyStateError::NeutralLotLedgerMismatch);
    }
    Ok(())
}

fn replacement_order_has_initial_provenance(
    state: &StrategyState,
    client_order_id: &ClientOrderId,
    visiting: &mut BTreeSet<ClientOrderId>,
    verified: &mut BTreeSet<ClientOrderId>,
) -> bool {
    if verified.contains(client_order_id) {
        return true;
    }
    if !visiting.insert(client_order_id.clone()) {
        return false;
    }
    let valid = state
        .orders
        .get(client_order_id)
        .is_some_and(|order| match &order.purpose {
            StrategyOrderPurpose::InitialGrid { .. } => true,
            StrategyOrderPurpose::Replacement { obligation_ids, .. } => {
                !obligation_ids.is_empty()
                    && obligation_ids.iter().all(|id| {
                        state
                            .replacement_obligations
                            .get(id)
                            .is_some_and(|obligation| {
                                obligation.assigned_client_order_id.as_ref()
                                    == Some(client_order_id)
                                    && replacement_order_has_initial_provenance(
                                        state,
                                        &obligation.source_client_order_id,
                                        visiting,
                                        verified,
                                    )
                            })
                    })
            }
            StrategyOrderPurpose::Opening | StrategyOrderPurpose::RiskClose => false,
        });
    visiting.remove(client_order_id);
    if valid {
        verified.insert(client_order_id.clone());
    }
    valid
}

fn validate_execution_audit_payload(
    strategy_exchange: Exchange,
    order: &StrategyOrderRecord,
    audit: &ExecutionAuditRecord,
    expected_quantity: Decimal,
    expected_quote: Decimal,
    expected_fee: Decimal,
    expected_terminal_status: Option<TerminalOrderStatus>,
) -> Result<(), StrategyStateError> {
    let snapshot = &audit.snapshot;
    if audit.synced_at_ms == 0
        || snapshot.order.exchange != strategy_exchange
        || snapshot.order.client_order_id != order.client_order_id
        || snapshot.order.exchange_order_id.trim().is_empty()
        || order.exchange_order_id.as_deref() != Some(&snapshot.order.exchange_order_id)
        || snapshot.order.shape != order.shape
        || snapshot
            .order
            .executed_quantity
            .is_some_and(|quantity| quantity != snapshot.cumulative_quantity)
        || snapshot.cumulative_quantity != expected_quantity
        || snapshot.cumulative_quote != expected_quote
        || snapshot.order_time_ms == 0
        || snapshot.update_time_ms < snapshot.order_time_ms
        || expected_quantity < Decimal::ZERO
        || expected_quote < Decimal::ZERO
        || expected_fee < Decimal::ZERO
        || !trades_are_canonically_ordered(&snapshot.trades)
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    let snapshot_terminal_status = match snapshot.order.lifecycle {
        OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::New) => {
            if !expected_quantity.is_zero() {
                return Err(StrategyStateError::InvalidExecutionAudit);
            }
            None
        }
        OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::PartiallyFilled) => {
            if expected_quantity <= Decimal::ZERO || expected_quantity >= order.shape.quantity {
                return Err(StrategyStateError::InvalidExecutionAudit);
            }
            None
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Filled) => {
            if expected_quantity != order.shape.quantity {
                return Err(StrategyStateError::InvalidExecutionAudit);
            }
            Some(TerminalOrderStatus::Filled)
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Rejected) => {
            if !expected_quantity.is_zero() {
                return Err(StrategyStateError::InvalidExecutionAudit);
            }
            Some(TerminalOrderStatus::Rejected)
        }
        OrderLifecycle::Terminal(status) => Some(status),
    };
    if snapshot_terminal_status != expected_terminal_status {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }

    let mut trades = BTreeMap::new();
    let mut trade_quantity = Decimal::ZERO;
    let mut trade_quote = Decimal::ZERO;
    let mut fees_by_asset = BTreeMap::new();
    for trade in &snapshot.trades {
        if trades.insert(trade.trade_id.as_str(), trade).is_some()
            || !is_valid_trade_id(&trade.trade_id)
            || trade.exchange_order_id != snapshot.order.exchange_order_id
            || trade.symbol != order.shape.symbol
            || trade.side != order.shape.side
            || trade.price <= Decimal::ZERO
            || trade.quantity <= Decimal::ZERO
            || trade.quote_quantity <= Decimal::ZERO
            || trade.commission_cost < Decimal::ZERO
            || trade.commission_asset.is_empty()
            || trade.trade_time_ms < snapshot.order_time_ms
            || trade.trade_time_ms > snapshot.update_time_ms
        {
            return Err(StrategyStateError::InvalidExecutionAudit);
        }
        trade_quantity = trade_quantity.checked_add(trade.quantity).ok_or(
            StrategyStateError::NumericOverflow("audited trade quantity"),
        )?;
        trade_quote = trade_quote
            .checked_add(trade.quote_quantity)
            .ok_or(StrategyStateError::NumericOverflow("audited trade quote"))?;
        let fee = fees_by_asset
            .entry(trade.commission_asset.clone())
            .or_insert(Decimal::ZERO);
        *fee = fee
            .checked_add(trade.commission_cost)
            .ok_or(StrategyStateError::NumericOverflow("audited fee asset"))?;
    }
    if trade_quantity != expected_quantity
        || trade_quote != expected_quote
        || fees_by_asset != snapshot.fees_by_asset
        || audit.fee_valuations.len() != trades.len()
        || audit
            .fee_valuations
            .iter()
            .zip(&snapshot.trades)
            .any(|(valuation, trade)| valuation.trade_id != trade.trade_id)
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }

    let mut valuation_trade_ids = BTreeSet::new();
    let mut valued_fee = Decimal::ZERO;
    let mut audit_quote_asset: Option<&str> = None;
    for valuation in &audit.fee_valuations {
        let Some(trade) = trades.get(valuation.trade_id.as_str()) else {
            return Err(StrategyStateError::InvalidExecutionAudit);
        };
        if !valuation_trade_ids.insert(valuation.trade_id.as_str())
            || valuation.fee_asset != trade.commission_asset
            || valuation.fee_amount != trade.commission_cost
            || valuation.quote_asset.is_empty()
            || !valuation
                .quote_asset
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            || !order.shape.symbol.ends_with(&valuation.quote_asset)
            || order.shape.symbol.len() == valuation.quote_asset.len()
            || valuation.quote_value < Decimal::ZERO
        {
            return Err(StrategyStateError::InvalidExecutionAudit);
        }
        if audit_quote_asset.is_some_and(|quote_asset| quote_asset != valuation.quote_asset) {
            return Err(StrategyStateError::InvalidExecutionAudit);
        }
        audit_quote_asset = Some(&valuation.quote_asset);
        let source_is_valid = match valuation.source {
            FeeValuationSource::ExchangeZero => {
                valuation.fee_amount.is_zero()
                    && valuation.quote_value.is_zero()
                    && valuation.valuation_symbol.is_none()
                    && valuation.valuation_minute_start_ms.is_none()
                    && valuation.valuation_price.is_none()
            }
            FeeValuationSource::QuoteAsset => {
                valuation.fee_asset == valuation.quote_asset
                    && valuation.quote_value == valuation.fee_amount
                    && valuation.valuation_symbol.is_none()
                    && valuation.valuation_minute_start_ms.is_none()
                    && valuation.valuation_price == Some(Decimal::ONE)
            }
            FeeValuationSource::HistoricalMinuteOpen => {
                let expected_minute = trade.trade_time_ms - (trade.trade_time_ms % 60_000);
                let expected_symbol = format!("{}{}", valuation.fee_asset, valuation.quote_asset);
                valuation.fee_amount > Decimal::ZERO
                    && valuation.fee_asset != valuation.quote_asset
                    && expected_minute > 0
                    && valuation.valuation_symbol.as_deref() == Some(&expected_symbol)
                    && valuation.valuation_minute_start_ms == Some(expected_minute)
                    && valuation.valuation_price.is_some_and(|price| {
                        price > Decimal::ZERO
                            && valuation.fee_amount.checked_mul(price)
                                == Some(valuation.quote_value)
                    })
            }
        };
        if !source_is_valid {
            return Err(StrategyStateError::InvalidExecutionAudit);
        }
        valued_fee = valued_fee
            .checked_add(valuation.quote_value)
            .ok_or(StrategyStateError::NumericOverflow("audited valued fee"))?;
    }
    if valued_fee != expected_fee {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    Ok(())
}

fn validate_execution_audit_extension(
    previous: &ExecutionAuditRecord,
    candidate: &ExecutionAuditRecord,
) -> Result<(), StrategyStateError> {
    if candidate.synced_at_ms < previous.synced_at_ms
        || candidate.snapshot.order_time_ms != previous.snapshot.order_time_ms
        || candidate.snapshot.update_time_ms < previous.snapshot.update_time_ms
        || candidate.snapshot.cumulative_quantity < previous.snapshot.cumulative_quantity
        || candidate.snapshot.cumulative_quote < previous.snapshot.cumulative_quote
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    if !candidate
        .snapshot
        .trades
        .starts_with(&previous.snapshot.trades)
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    if !candidate
        .fee_valuations
        .starts_with(&previous.fee_valuations)
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    if matches!(
        previous.snapshot.order.lifecycle,
        OrderLifecycle::Terminal(_)
    ) && previous.snapshot.order.lifecycle != candidate.snapshot.order.lifecycle
    {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    Ok(())
}

fn validate_strategy_trade_id_ownership(
    state: &StrategyState,
    candidate: Option<(&ClientOrderId, &ExecutionAuditRecord)>,
) -> Result<(), StrategyStateError> {
    let mut owners = BTreeMap::<String, ClientOrderId>::new();
    let mut candidate_seen = candidate.is_none();
    for (client_order_id, order) in &state.orders {
        let audit = match candidate {
            Some((candidate_order_id, candidate_audit))
                if client_order_id == candidate_order_id =>
            {
                candidate_seen = true;
                Some(candidate_audit)
            }
            _ => order.execution_audit.as_ref(),
        };
        let Some(audit) = audit else {
            continue;
        };
        for trade in &audit.snapshot.trades {
            if owners
                .insert(trade.trade_id.clone(), client_order_id.clone())
                .is_some()
            {
                return Err(StrategyStateError::InvalidExecutionAudit);
            }
        }
    }
    if !candidate_seen {
        return Err(StrategyStateError::InvalidExecutionAudit);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: String,
    pub cumulative_quantity: Decimal,
    pub cumulative_quote: Decimal,
    pub cumulative_fee: Decimal,
    pub terminal_status: Option<TerminalOrderStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyTransition {
    NoChange,
    Updated {
        new_obligation_ids: Vec<u64>,
    },
    ReplacementOrdersReady {
        client_order_ids: Vec<ClientOrderId>,
    },
    LifecycleChanged {
        lifecycle: StrategyLifecycle,
    },
    RiskExitRequested {
        reason: RiskExitReason,
        mark_price: Decimal,
    },
    RiskCloseOrderReady {
        client_order_id: ClientOrderId,
        quantity: Decimal,
    },
    Failed {
        message: String,
    },
}

pub trait StrategyStateStore {
    fn snapshot(&self) -> &StrategyState;
    fn replace(&mut self, next: StrategyState) -> Result<(), StrategyStoreError>;
}

#[derive(Debug, Clone)]
pub struct MemoryStrategyStateStore {
    snapshot: StrategyState,
    write_attempts: u64,
    fail_write_attempt: Option<u64>,
}

impl MemoryStrategyStateStore {
    pub fn new(snapshot: StrategyState) -> Self {
        Self {
            snapshot,
            write_attempts: 0,
            fail_write_attempt: None,
        }
    }

    pub fn fail_next_write(&mut self) {
        self.fail_write_attempt = Some(self.write_attempts + 1);
    }
}

impl StrategyStateStore for MemoryStrategyStateStore {
    fn snapshot(&self) -> &StrategyState {
        &self.snapshot
    }

    fn replace(&mut self, next: StrategyState) -> Result<(), StrategyStoreError> {
        self.write_attempts = self
            .write_attempts
            .checked_add(1)
            .ok_or(StrategyStoreError::WriteAttemptOverflow)?;
        if self.fail_write_attempt == Some(self.write_attempts) {
            self.fail_write_attempt = None;
            return Err(StrategyStoreError::InjectedWriteFailure);
        }
        if self.snapshot.revision.checked_add(1) != Some(next.revision) {
            return Err(StrategyStoreError::RevisionMismatch);
        }
        next.validate().map_err(StrategyStoreError::InvalidState)?;
        self.snapshot = next;
        Ok(())
    }
}

pub struct StrategyMachine<S> {
    store: S,
}

impl<S> StrategyMachine<S>
where
    S: StrategyStateStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn synchronize_intent(
        &mut self,
        intent: &OrderIntent,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        let transition = synchronize_intent_state(&mut next, intent);
        finalize_and_store(&mut self.store, next, now_ms, transition)
    }

    pub fn apply_execution(
        &mut self,
        report: &ExecutionReport,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        let transition = apply_execution_report(&mut next, report, None, now_ms);
        finalize_and_store(&mut self.store, next, now_ms, transition)
    }

    pub fn apply_valued_execution(
        &mut self,
        snapshot: &crate::exchange::OrderExecutionSnapshot,
        valued: &ValuedExecutionReport,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        let candidate = ExecutionAuditRecord {
            snapshot: snapshot.clone(),
            fee_valuations: valued.fee_valuations.clone(),
            synced_at_ms: now_ms,
        };
        let audit_validation = next
            .orders
            .get(&valued.report.client_order_id)
            .ok_or(StrategyStateError::InvalidExecutionAudit)
            .and_then(|order| {
                validate_execution_audit_payload(
                    next.exchange,
                    order,
                    &candidate,
                    valued.report.cumulative_quantity,
                    valued.report.cumulative_quote,
                    valued.report.cumulative_fee,
                    valued.report.terminal_status,
                )?;
                if let Some(previous) = &order.execution_audit {
                    validate_execution_audit_extension(previous, &candidate)?;
                }
                Ok(())
            })
            .and_then(|()| {
                validate_strategy_trade_id_ownership(
                    &next,
                    Some((&valued.report.client_order_id, &candidate)),
                )
            });
        if audit_validation.is_err() {
            let transition = fail_transition(&mut next, "valued execution audit is invalid");
            return finalize_and_store(&mut self.store, next, now_ms, transition);
        }
        let previous_trade_count = next
            .orders
            .get(&valued.report.client_order_id)
            .and_then(|order| order.execution_audit.as_ref())
            .map_or(0, |audit| audit.snapshot.trades.len());
        let inventory_deltas = candidate
            .snapshot
            .trades
            .iter()
            .skip(previous_trade_count)
            .map(|trade| InventoryDeltaEvidence {
                quantity: trade.quantity,
                quote: trade.quote_quantity,
                exchange_trade_id: Some(trade.trade_id.clone()),
                execution_time_ms: Some(trade.trade_time_ms),
            })
            .collect::<Vec<_>>();
        let audit_changed = next
            .orders
            .get(&valued.report.client_order_id)
            .and_then(|order| order.execution_audit.as_ref())
            .is_none_or(|previous| {
                previous.snapshot != candidate.snapshot
                    || previous.fee_valuations != candidate.fee_valuations
            });
        let report_was_already_applied = next
            .orders
            .get(&valued.report.client_order_id)
            .is_some_and(|order| order_matches_execution_report(order, &valued.report));
        let mut transition =
            apply_execution_report(&mut next, &valued.report, Some(&inventory_deltas), now_ms);
        let report_was_applied = next
            .orders
            .get(&valued.report.client_order_id)
            .is_some_and(|order| order_matches_execution_report(order, &valued.report));
        if audit_changed
            && (!matches!(transition, StrategyTransition::Failed { .. })
                || (report_was_applied && !report_was_already_applied))
        {
            let Some(order) = next.orders.get_mut(&valued.report.client_order_id) else {
                transition = fail_transition(
                    &mut next,
                    "valued execution order disappeared during audit persistence",
                );
                return finalize_and_store(&mut self.store, next, now_ms, transition);
            };
            order.execution_audit = Some(candidate);
            if transition == StrategyTransition::NoChange {
                transition = StrategyTransition::Updated {
                    new_obligation_ids: Vec::new(),
                };
            }
        }
        finalize_and_store(&mut self.store, next, now_ms, transition)
    }

    pub fn materialize_replacements(
        &mut self,
        fresh_rules: &InstrumentRules,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        fresh_rules
            .validate()
            .map_err(StrategyStateError::InvalidInstrument)?;
        let mut next = self.store.snapshot().clone();
        if matches!(
            next.lifecycle,
            StrategyLifecycle::RiskExitRequested
                | StrategyLifecycle::StopRequested
                | StrategyLifecycle::Stopped
                | StrategyLifecycle::Failed
                | StrategyLifecycle::Closed
        ) {
            return Ok(StrategyTransition::NoChange);
        }
        let transition = materialize_replacement_orders(&mut next, fresh_rules);
        finalize_and_store(&mut self.store, next, now_ms, transition)
    }

    pub fn reconcile_instrument_rules(
        &mut self,
        fresh_rules: &InstrumentRules,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        fresh_rules
            .validate()
            .map_err(StrategyStateError::InvalidInstrument)?;
        if fresh_rules == &self.store.snapshot().instrument_rules {
            return Ok(StrategyTransition::NoChange);
        }
        let mut next = self.store.snapshot().clone();
        let message = "authoritative exchange instrument rules changed".to_owned();
        next.fail(message.clone());
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::Failed { message },
        )
    }

    pub fn evaluate_risk_price(
        &mut self,
        mark_price: Decimal,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        if mark_price <= Decimal::ZERO {
            return Err(StrategyStateError::InvalidMarketPrice.into());
        }
        let snapshot = self.store.snapshot();
        if !matches!(
            snapshot.lifecycle,
            StrategyLifecycle::AwaitingOpening
                | StrategyLifecycle::DeployingGrid
                | StrategyLifecycle::Running
        ) {
            return Ok(StrategyTransition::NoChange);
        }
        let reason = match snapshot.direction {
            Direction::Long => {
                if snapshot
                    .config
                    .stop_loss_price
                    .is_some_and(|price| mark_price <= price)
                {
                    Some(RiskExitReason::StopLoss)
                } else if snapshot
                    .config
                    .take_profit_price
                    .is_some_and(|price| mark_price >= price)
                {
                    Some(RiskExitReason::TakeProfit)
                } else {
                    None
                }
            }
            Direction::Short => {
                if snapshot
                    .config
                    .stop_loss_price
                    .is_some_and(|price| mark_price >= price)
                {
                    Some(RiskExitReason::StopLoss)
                } else if snapshot
                    .config
                    .take_profit_price
                    .is_some_and(|price| mark_price <= price)
                {
                    Some(RiskExitReason::TakeProfit)
                } else {
                    None
                }
            }
            Direction::Neutral => {
                if snapshot
                    .config
                    .stop_loss_price
                    .is_some_and(|price| mark_price <= price)
                {
                    Some(RiskExitReason::StopLoss)
                } else if snapshot
                    .config
                    .take_profit_price
                    .is_some_and(|price| mark_price >= price)
                {
                    Some(RiskExitReason::TakeProfit)
                } else {
                    None
                }
            }
        };
        let Some(reason) = reason else {
            return Ok(StrategyTransition::NoChange);
        };
        let mut next = snapshot.clone();
        next.lifecycle = StrategyLifecycle::RiskExitRequested;
        next.risk_exit_reason = Some(reason);
        next.risk_trigger_mark_price = Some(mark_price);
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::RiskExitRequested { reason, mark_price },
        )
    }

    pub fn prepare_risk_close(
        &mut self,
        actual_signed_quantity: Decimal,
        fresh_rules: &InstrumentRules,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        fresh_rules
            .validate()
            .map_err(StrategyStateError::InvalidInstrument)?;
        let snapshot = self.store.snapshot();
        if snapshot.lifecycle == StrategyLifecycle::Closed {
            return Ok(StrategyTransition::NoChange);
        }
        if snapshot.lifecycle != StrategyLifecycle::RiskExitRequested {
            return Err(StrategyStateError::InvalidLifecycleTransition.into());
        }
        if snapshot
            .orders
            .values()
            .any(|order| order.purpose.is_risk_close() && !order.terminal_processed)
        {
            return Ok(StrategyTransition::NoChange);
        }

        let mut next = snapshot.clone();
        if fresh_rules != &next.instrument_rules {
            let transition = fail_transition(
                &mut next,
                "exchange instrument rules changed before the risk close",
            );
            return finalize_and_store(&mut self.store, next, now_ms, transition);
        }
        if next.orders.values().any(order_may_still_be_live) {
            return Err(StrategyStateError::OrdersNotTerminal.into());
        }
        let expected = next.expected_exchange_position()?;
        if actual_signed_quantity != expected {
            let transition = fail_transition(
                &mut next,
                format!(
                    "risk close position mismatch: expected {expected}, actual {actual_signed_quantity}"
                ),
            );
            return finalize_and_store(&mut self.store, next, now_ms, transition);
        }
        if next.grid_position_net_quantity.is_zero() {
            next.lifecycle = StrategyLifecycle::Closed;
            return finalize_and_store(
                &mut self.store,
                next,
                now_ms,
                StrategyTransition::LifecycleChanged {
                    lifecycle: StrategyLifecycle::Closed,
                },
            );
        }
        let total_quantity = next.grid_position_net_quantity.abs();
        let close_quantity = if let Some(maximum) = fresh_rules.market_quantity.max
            && total_quantity > maximum
        {
            fresh_rules
                .market_quantity
                .floor(maximum)
                .ok_or(StrategyStateError::RiskCloseQuantityInvalid)?
        } else {
            total_quantity
        };
        if !fresh_rules.market_quantity.accepts(close_quantity) {
            let transition = fail_transition(
                &mut next,
                "exact grid-owned risk close quantity is not accepted by market rules",
            );
            return finalize_and_store(&mut self.store, next, now_ms, transition);
        }
        let side = if next.grid_position_net_quantity > Decimal::ZERO {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };
        let client_order_id = next.next_client_order_id("c", None, side)?;
        next.insert_order(StrategyOrderRecord {
            client_order_id: client_order_id.clone(),
            shape: OrderShape {
                symbol: next.symbol.clone(),
                side,
                price: None,
                quantity: close_quantity,
                reduce_only: true,
                kind: OrderKind::Market,
                time_in_force: crate::domain::TimeInForce::Gtc,
            },
            purpose: StrategyOrderPurpose::RiskClose,
            tracking: StrategyOrderTracking::Ready,
            exchange_order_id: None,
            cumulative_quantity: Decimal::ZERO,
            cumulative_quote: Decimal::ZERO,
            cumulative_fee: Decimal::ZERO,
            execution_audit: None,
            terminal_status: None,
            terminal_processed: false,
            completed_pair_counted: false,
        })?;
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::RiskCloseOrderReady {
                client_order_id,
                quantity: close_quantity,
            },
        )
    }

    pub fn reconcile_position(
        &mut self,
        actual_signed_quantity: Decimal,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let expected = self.store.snapshot().expected_exchange_position()?;
        if actual_signed_quantity == expected {
            return Ok(StrategyTransition::NoChange);
        }
        let mut next = self.store.snapshot().clone();
        let message = format!(
            "authoritative position mismatch: expected {expected}, actual {actual_signed_quantity}"
        );
        next.fail(message.clone());
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::Failed { message },
        )
    }

    pub fn request_stop(
        &mut self,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        if matches!(
            next.lifecycle,
            StrategyLifecycle::RiskExitRequested
                | StrategyLifecycle::StopRequested
                | StrategyLifecycle::Stopped
                | StrategyLifecycle::Failed
                | StrategyLifecycle::Closed
        ) {
            return Ok(StrategyTransition::NoChange);
        }
        next.lifecycle = StrategyLifecycle::StopRequested;
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested,
            },
        )
    }

    pub fn mark_stopped(
        &mut self,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        if next.lifecycle == StrategyLifecycle::Stopped {
            return Ok(StrategyTransition::NoChange);
        }
        if next.lifecycle != StrategyLifecycle::StopRequested {
            return Err(StrategyStateError::InvalidLifecycleTransition.into());
        }
        if next.orders.values().any(order_may_still_be_live) {
            return Err(StrategyStateError::OrdersNotTerminal.into());
        }
        next.lifecycle = StrategyLifecycle::Stopped;
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Stopped,
            },
        )
    }

    pub fn mark_failed_stopped(
        &mut self,
        now_ms: u64,
    ) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        if next.lifecycle != StrategyLifecycle::Failed {
            return Err(StrategyStateError::InvalidLifecycleTransition.into());
        }
        if next.orders.values().any(order_may_still_be_live) {
            return Ok(StrategyTransition::NoChange);
        }
        if next
            .failure
            .as_deref()
            .is_none_or(|message| message.trim().is_empty())
        {
            return Err(StrategyStateError::InvalidFailureState.into());
        }
        next.lifecycle = StrategyLifecycle::Stopped;
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Stopped,
            },
        )
    }

    pub fn mark_closed(&mut self, now_ms: u64) -> Result<StrategyTransition, StrategyMachineError> {
        let mut next = self.store.snapshot().clone();
        if next.lifecycle == StrategyLifecycle::Closed {
            return Ok(StrategyTransition::NoChange);
        }
        if !matches!(
            next.lifecycle,
            StrategyLifecycle::StopRequested | StrategyLifecycle::Stopped
        ) || !next.grid_position_net_quantity.is_zero()
            || !next.lots_by_level.is_empty()
            || !next.neutral_lots.is_empty()
            || next.orders.values().any(order_may_still_be_live)
        {
            return Err(StrategyStateError::CannotCloseStrategy.into());
        }
        next.lifecycle = StrategyLifecycle::Closed;
        finalize_and_store(
            &mut self.store,
            next,
            now_ms,
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Closed,
            },
        )
    }
}

fn order_may_still_be_live(order: &StrategyOrderRecord) -> bool {
    match &order.tracking {
        StrategyOrderTracking::Intent {
            state:
                IntentState::Prepared | IntentState::SubmitUnknown { .. } | IntentState::Accepted { .. },
        } => true,
        StrategyOrderTracking::Intent {
            state: IntentState::Terminal { .. },
        } => !order.terminal_processed,
        StrategyOrderTracking::Dormant
        | StrategyOrderTracking::Ready
        | StrategyOrderTracking::Intent { .. } => false,
    }
}

fn finalize_and_store<S: StrategyStateStore>(
    store: &mut S,
    mut next: StrategyState,
    now_ms: u64,
    transition: StrategyTransition,
) -> Result<StrategyTransition, StrategyMachineError> {
    if transition == StrategyTransition::NoChange {
        return Ok(transition);
    }
    next.revision = next
        .revision
        .checked_add(1)
        .ok_or(StrategyStateError::NumericOverflow("strategy revision"))?;
    if now_ms < next.updated_at_ms {
        return Err(StrategyStateError::TimestampRegression.into());
    }
    next.updated_at_ms = now_ms;
    next.validate()?;
    store.replace(next)?;
    Ok(transition)
}

fn synchronize_intent_state(state: &mut StrategyState, intent: &OrderIntent) -> StrategyTransition {
    if let Err(error) = intent.validate() {
        let message = format!("order intent is invalid: {error}");
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    let Some(order) = state.orders.get_mut(&intent.client_order_id) else {
        let message = "order intent does not belong to this strategy".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    };
    if intent.exchange != state.exchange || intent.shape != order.shape {
        let message = "order intent shape or exchange does not match the strategy task".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    let authoritative_exchange_order_id = intent.state.exchange_order_id().map(str::to_owned);
    if let Some(exchange_order_id) = &authoritative_exchange_order_id
        && order
            .exchange_order_id
            .as_ref()
            .is_some_and(|current| current != exchange_order_id)
    {
        let message = "order intent exchange order identity changed".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if matches!(
        intent.state,
        IntentState::Terminal {
            exchange_order_id: None,
            ..
        }
    ) && order.exchange_order_id.is_none()
    {
        let message = "terminal order intent is missing its exchange order identity".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    let tracking = StrategyOrderTracking::Intent {
        state: intent.state.clone(),
    };
    if order.tracking == tracking {
        return StrategyTransition::NoChange;
    }
    if order.tracking == StrategyOrderTracking::Dormant {
        let message = "dormant order received a submission intent before activation".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if !strategy_intent_transition_allowed(&order.tracking, &intent.state) {
        let message = "order intent state regressed or changed after becoming final".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    order.tracking = tracking;
    if let Some(exchange_order_id) = authoritative_exchange_order_id {
        order.exchange_order_id = Some(exchange_order_id);
    }
    if matches!(
        intent.state,
        IntentState::Rejected { .. } | IntentState::OwnershipConflict { .. }
    ) {
        let message = "order intent reached a non-recoverable state".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    refresh_running_state(state);
    StrategyTransition::Updated {
        new_obligation_ids: Vec::new(),
    }
}

fn strategy_intent_transition_allowed(current: &StrategyOrderTracking, next: &IntentState) -> bool {
    match current {
        StrategyOrderTracking::Ready => true,
        StrategyOrderTracking::Dormant => false,
        StrategyOrderTracking::Intent { state: current } => {
            let ordinary_transition = matches!(
                (current, next),
                (
                    IntentState::Prepared,
                    IntentState::SubmitUnknown { .. }
                        | IntentState::Accepted { .. }
                        | IntentState::Rejected { .. }
                        | IntentState::OwnershipConflict { .. }
                        | IntentState::Terminal { .. }
                ) | (
                    IntentState::SubmitUnknown { .. },
                    IntentState::Accepted { .. }
                        | IntentState::Rejected { .. }
                        | IntentState::OwnershipConflict { .. }
                        | IntentState::Terminal { .. }
                ) | (
                    IntentState::Accepted { .. },
                    IntentState::Terminal { .. } | IntentState::OwnershipConflict { .. }
                )
            );
            let legacy_terminal_enrichment = matches!(
                (current, next),
                (
                    IntentState::Terminal {
                        status: current_status,
                        exchange_order_id: None,
                    },
                    IntentState::Terminal {
                        status: next_status,
                        exchange_order_id: Some(_),
                    }
                ) if current_status == next_status
            );
            ordinary_transition || legacy_terminal_enrichment
        }
    }
}

fn order_matches_execution_report(order: &StrategyOrderRecord, report: &ExecutionReport) -> bool {
    order.exchange_order_id.as_deref() == Some(report.exchange_order_id.as_str())
        && order.cumulative_quantity == report.cumulative_quantity
        && order.cumulative_quote == report.cumulative_quote
        && order.cumulative_fee == report.cumulative_fee
        && order.terminal_status == report.terminal_status
}

fn apply_execution_report(
    state: &mut StrategyState,
    report: &ExecutionReport,
    precise_inventory_deltas: Option<&[InventoryDeltaEvidence]>,
    now_ms: u64,
) -> StrategyTransition {
    let execution_after_final_state = matches!(
        state.lifecycle,
        StrategyLifecycle::Stopped | StrategyLifecycle::Closed
    );
    let Some(existing) = state.orders.get(&report.client_order_id) else {
        let message = "execution report references an unknown client order ID".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    };
    if report.exchange_order_id.is_empty()
        || existing
            .exchange_order_id
            .as_ref()
            .is_some_and(|order_id| order_id != &report.exchange_order_id)
    {
        let message = "execution report exchange order identity changed or is missing".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if report.cumulative_quantity < existing.cumulative_quantity
        || report.cumulative_quote < existing.cumulative_quote
        || report.cumulative_fee < existing.cumulative_fee
        || report.cumulative_quantity > existing.shape.quantity
        || report.cumulative_quantity < Decimal::ZERO
        || report.cumulative_quote < Decimal::ZERO
        || report.cumulative_fee < Decimal::ZERO
        || (report.cumulative_quantity > Decimal::ZERO && report.cumulative_quote <= Decimal::ZERO)
        || (report.cumulative_quantity > Decimal::ZERO
            && !(match existing.shape.kind {
                OrderKind::Limit => &state.instrument_rules.limit_quantity,
                OrderKind::Market => &state.instrument_rules.market_quantity,
            })
            .is_aligned(report.cumulative_quantity))
    {
        let message = "execution report cumulative totals are invalid or regressed".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if let Some(current) = existing.terminal_status
        && report.terminal_status != Some(current)
    {
        let message = "execution report terminal status changed".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if matches!(
        existing.tracking,
        StrategyOrderTracking::Intent {
            state: IntentState::Terminal { .. }
        }
    ) && report.terminal_status.is_none()
    {
        let message = "execution report regressed an authoritative terminal order".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if report.terminal_status == Some(TerminalOrderStatus::Filled)
        && report.cumulative_quantity != existing.shape.quantity
    {
        let message = "filled order does not report its complete planned quantity".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    if report.terminal_status == Some(TerminalOrderStatus::Rejected)
        && report.cumulative_quantity > Decimal::ZERO
    {
        let message = "rejected order unexpectedly reports an execution".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }

    let delta_quantity = report.cumulative_quantity - existing.cumulative_quantity;
    let delta_quote = report.cumulative_quote - existing.cumulative_quote;
    let delta_fee = report.cumulative_fee - existing.cumulative_fee;
    let terminal_changed = report.terminal_status.is_some() && existing.terminal_status.is_none();
    let purpose = existing.purpose.clone();
    let shape = existing.shape.clone();
    let was_terminal_processed = existing.terminal_processed;
    let previous_inventory = InventoryAccountingSnapshot::capture(state);
    if delta_quantity.is_zero() != delta_quote.is_zero() {
        let message =
            "execution quantity and quote deltas must become positive together".to_owned();
        state.fail(message.clone());
        return StrategyTransition::Failed { message };
    }
    let inventory_deltas =
        match normalize_inventory_deltas(delta_quantity, delta_quote, precise_inventory_deltas) {
            Ok(deltas) => deltas,
            Err(message) => return fail_transition(state, message),
        };
    if delta_quantity.is_zero() && delta_fee.is_zero() && !terminal_changed {
        return StrategyTransition::NoChange;
    }
    if let Err(message) =
        append_inventory_events(state, &report.client_order_id, &inventory_deltas, now_ms)
    {
        return fail_transition(state, message);
    }

    {
        let Some(order) = state.orders.get_mut(&report.client_order_id) else {
            return fail_transition(state, "execution order disappeared during processing");
        };
        order.exchange_order_id = Some(report.exchange_order_id.clone());
        order.cumulative_quantity = report.cumulative_quantity;
        order.cumulative_quote = report.cumulative_quote;
        order.cumulative_fee = report.cumulative_fee;
        if let Some(status) = report.terminal_status {
            order.terminal_status = Some(status);
            order.terminal_processed = true;
            order.tracking = StrategyOrderTracking::Intent {
                state: IntentState::Terminal {
                    status,
                    exchange_order_id: Some(report.exchange_order_id.clone()),
                },
            };
        } else {
            order.tracking = StrategyOrderTracking::Intent {
                state: IntentState::Accepted {
                    exchange_order_id: report.exchange_order_id.clone(),
                },
            };
        }
    }

    state.total_volume = match state.total_volume.checked_add(delta_quote) {
        Some(total) => total,
        None => return fail_transition(state, "trade volume overflowed"),
    };
    state.total_fee = match state.total_fee.checked_add(delta_fee) {
        Some(total) => total,
        None => return fail_transition(state, "trade fee overflowed"),
    };

    let mut obligation_ids = Vec::new();
    if let Err(replay_message) = rebuild_inventory_accounting(state) {
        // Preserve the prior fail-closed diagnostic shape when exact global
        // chronology cannot be reconstructed (for example, legacy aggregate
        // evidence mixed with exact trades).
        previous_inventory.restore(state);
        for delta in &inventory_deltas {
            if let Err(message) =
                apply_inventory_accounting(state, &purpose, &shape, delta.quantity, delta.quote)
            {
                return fail_transition(state, message);
            }
        }
        return fail_transition(state, replay_message);
    }
    if delta_quantity > Decimal::ZERO
        && let Err(message) = add_execution_counter_obligation(
            state,
            &report.client_order_id,
            &purpose,
            &shape,
            delta_quantity,
            now_ms,
            &mut obligation_ids,
        )
    {
        return fail_transition(state, message);
    }

    if terminal_changed
        && !was_terminal_processed
        && let Err(message) = process_terminal_order(
            state,
            &report.client_order_id,
            &purpose,
            &shape,
            report,
            now_ms,
            &mut obligation_ids,
        )
    {
        return fail_transition(state, message);
    }
    if execution_after_final_state && delta_quantity > Decimal::ZERO {
        return fail_transition(
            state,
            "authoritative execution arrived after the strategy was finalized",
        );
    }
    refresh_running_state(state);
    StrategyTransition::Updated {
        new_obligation_ids: obligation_ids,
    }
}

fn append_inventory_events(
    state: &mut StrategyState,
    source_client_order_id: &ClientOrderId,
    deltas: &[InventoryDeltaEvidence],
    applied_at_ms: u64,
) -> Result<(), String> {
    let event_count =
        u64::try_from(deltas.len()).map_err(|_| "inventory event count overflowed".to_owned())?;
    let first_sequence = state.next_inventory_event_sequence;
    let next_sequence = first_sequence
        .checked_add(event_count)
        .ok_or_else(|| "inventory event sequence overflowed".to_owned())?;
    if state
        .inventory_events
        .range(first_sequence..next_sequence)
        .next()
        .is_some()
    {
        return Err("inventory event sequence is duplicated".to_owned());
    }
    let events = deltas
        .iter()
        .enumerate()
        .map(|(offset, delta)| {
            let offset = u64::try_from(offset)
                .map_err(|_| "inventory event offset overflowed".to_owned())?;
            let sequence = first_sequence
                .checked_add(offset)
                .ok_or_else(|| "inventory event sequence overflowed".to_owned())?;
            Ok(InventoryExecutionEvent {
                sequence,
                source_client_order_id: source_client_order_id.clone(),
                quantity: delta.quantity,
                quote: delta.quote,
                exchange_trade_id: delta.exchange_trade_id.clone(),
                execution_time_ms: delta.execution_time_ms,
                applied_at_ms,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    state
        .inventory_events
        .extend(events.into_iter().map(|event| (event.sequence, event)));
    state.next_inventory_event_sequence = next_sequence;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct InventoryDeltaEvidence {
    quantity: Decimal,
    quote: Decimal,
    exchange_trade_id: Option<String>,
    execution_time_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
struct InventoryAccountingSnapshot {
    grid_position_net_quantity: Decimal,
    opening_filled_quantity: Decimal,
    opening_filled_value: Decimal,
    lots_by_level: BTreeMap<u16, LevelLot>,
    neutral_lots: BTreeMap<u64, NeutralLot>,
    next_neutral_lot_sequence: u64,
    gross_realized_profit: Decimal,
}

impl InventoryAccountingSnapshot {
    fn capture(state: &StrategyState) -> Self {
        Self {
            grid_position_net_quantity: state.grid_position_net_quantity,
            opening_filled_quantity: state.opening_filled_quantity,
            opening_filled_value: state.opening_filled_value,
            lots_by_level: state.lots_by_level.clone(),
            neutral_lots: state.neutral_lots.clone(),
            next_neutral_lot_sequence: state.next_neutral_lot_sequence,
            gross_realized_profit: state.gross_realized_profit,
        }
    }

    fn empty() -> Self {
        Self {
            grid_position_net_quantity: Decimal::ZERO,
            opening_filled_quantity: Decimal::ZERO,
            opening_filled_value: Decimal::ZERO,
            lots_by_level: BTreeMap::new(),
            neutral_lots: BTreeMap::new(),
            next_neutral_lot_sequence: 1,
            gross_realized_profit: Decimal::ZERO,
        }
    }

    fn restore(self, state: &mut StrategyState) {
        state.grid_position_net_quantity = self.grid_position_net_quantity;
        state.opening_filled_quantity = self.opening_filled_quantity;
        state.opening_filled_value = self.opening_filled_value;
        state.lots_by_level = self.lots_by_level;
        state.neutral_lots = self.neutral_lots;
        state.next_neutral_lot_sequence = self.next_neutral_lot_sequence;
        state.gross_realized_profit = self.gross_realized_profit;
    }
}

fn inventory_events_in_accounting_order(
    state: &StrategyState,
) -> Result<Vec<InventoryExecutionEvent>, String> {
    let mut events = state.inventory_events.values().cloned().collect::<Vec<_>>();
    let exact_event_count = events
        .iter()
        .filter(|event| event.exchange_trade_id.is_some() && event.execution_time_ms.is_some())
        .count();
    if exact_event_count != 0 && exact_event_count != events.len() {
        return Err(
            "exact exchange trades cannot be mixed with legacy aggregate inventory evidence"
                .to_owned(),
        );
    }
    if exact_event_count == events.len() {
        events.sort_by(|left, right| {
            match (
                left.execution_time_ms,
                left.exchange_trade_id.as_deref(),
                right.execution_time_ms,
                right.exchange_trade_id.as_deref(),
            ) {
                (Some(left_time), Some(left_id), Some(right_time), Some(right_id)) => {
                    compare_trade_chronology(left_time, left_id, right_time, right_id)
                }
                _ => left.sequence.cmp(&right.sequence),
            }
            .then_with(|| {
                left.source_client_order_id
                    .cmp(&right.source_client_order_id)
            })
            .then_with(|| left.sequence.cmp(&right.sequence))
        });
    }
    Ok(events)
}

fn rebuild_inventory_accounting(state: &mut StrategyState) -> Result<(), String> {
    let events = inventory_events_in_accounting_order(state)?;
    let mut replay = state.clone();
    InventoryAccountingSnapshot::empty().restore(&mut replay);
    for event in &events {
        let order = state
            .orders
            .get(&event.source_client_order_id)
            .ok_or_else(|| "inventory event references an unknown order".to_owned())?;
        apply_inventory_accounting(
            &mut replay,
            &order.purpose,
            &order.shape,
            event.quantity,
            event.quote,
        )?;
    }
    InventoryAccountingSnapshot::capture(&replay).restore(state);
    Ok(())
}

fn normalize_inventory_deltas(
    delta_quantity: Decimal,
    delta_quote: Decimal,
    precise_inventory_deltas: Option<&[InventoryDeltaEvidence]>,
) -> Result<Vec<InventoryDeltaEvidence>, String> {
    let deltas = match precise_inventory_deltas {
        Some(deltas) => deltas.to_vec(),
        None if delta_quantity > Decimal::ZERO => vec![InventoryDeltaEvidence {
            quantity: delta_quantity,
            quote: delta_quote,
            exchange_trade_id: None,
            execution_time_ms: None,
        }],
        None => Vec::new(),
    };
    let mut quantity = Decimal::ZERO;
    let mut quote = Decimal::ZERO;
    let mut trade_ids = BTreeSet::new();
    for delta in &deltas {
        let evidence_is_valid = match (&delta.exchange_trade_id, delta.execution_time_ms) {
            (Some(trade_id), Some(execution_time_ms)) if precise_inventory_deltas.is_some() => {
                is_valid_trade_id(trade_id)
                    && execution_time_ms > 0
                    && trade_ids.insert(trade_id.as_str())
            }
            (None, None) if precise_inventory_deltas.is_none() => true,
            _ => false,
        };
        if delta.quantity <= Decimal::ZERO || delta.quote <= Decimal::ZERO || !evidence_is_valid {
            return Err("execution inventory evidence is invalid".to_owned());
        }
        quantity = quantity
            .checked_add(delta.quantity)
            .ok_or_else(|| "execution inventory quantity overflowed".to_owned())?;
        quote = quote
            .checked_add(delta.quote)
            .ok_or_else(|| "execution inventory quote overflowed".to_owned())?;
    }
    if quantity != delta_quantity || quote != delta_quote {
        return Err("execution inventory evidence does not match cumulative deltas".to_owned());
    }
    Ok(deltas)
}

fn add_execution_counter_obligation(
    state: &mut StrategyState,
    source_client_order_id: &ClientOrderId,
    purpose: &StrategyOrderPurpose,
    shape: &OrderShape,
    quantity: Decimal,
    now_ms: u64,
    obligation_ids: &mut Vec<u64>,
) -> Result<(), String> {
    if matches!(
        purpose,
        StrategyOrderPurpose::Opening | StrategyOrderPurpose::RiskClose
    ) || !normal_grid_replacements_enabled(state.lifecycle)
    {
        return Ok(());
    }
    let level_index = purpose
        .level_index()
        .ok_or_else(|| "grid execution has no level identity".to_owned())?;
    let counter = counter_shape(state, level_index, shape, quantity)?;
    let id = add_obligation(
        state,
        ReplacementObligationKind::Counter,
        source_client_order_id.clone(),
        level_index,
        counter,
        now_ms,
    )?;
    obligation_ids.push(id);
    Ok(())
}

fn apply_inventory_accounting(
    state: &mut StrategyState,
    purpose: &StrategyOrderPurpose,
    shape: &OrderShape,
    quantity: Decimal,
    quote: Decimal,
) -> Result<(), String> {
    let signed_quantity = match shape.side {
        OrderSide::Buy => quantity,
        OrderSide::Sell => -quantity,
    };
    state.grid_position_net_quantity = state
        .grid_position_net_quantity
        .checked_add(signed_quantity)
        .ok_or_else(|| "grid position quantity overflowed".to_owned())?;

    if matches!(purpose, StrategyOrderPurpose::Opening) {
        state.opening_filled_quantity = state
            .opening_filled_quantity
            .checked_add(quantity)
            .ok_or_else(|| "opening quantity overflowed".to_owned())?;
        state.opening_filled_value = state
            .opening_filled_value
            .checked_add(quote)
            .ok_or_else(|| "opening value overflowed".to_owned())?;
        allocate_opening_delta(state, quantity, quote)?;
        return validate_directional_position(state, shape);
    }

    if matches!(purpose, StrategyOrderPurpose::RiskClose) {
        validate_directional_position(state, shape)?;
        let realized = consume_risk_close_lots(state, shape.side, quantity, quote)?;
        state.gross_realized_profit = state
            .gross_realized_profit
            .checked_add(realized)
            .ok_or_else(|| "risk close realized profit overflowed".to_owned())?;
        return Ok(());
    }

    validate_directional_position(state, shape)?;
    let level_index = purpose
        .level_index()
        .ok_or_else(|| "grid execution has no level identity".to_owned())?;
    if state.direction != Direction::Neutral {
        if shape.reduce_only {
            let realized = consume_level_lot(state, level_index, quantity, quote)?;
            state.gross_realized_profit = state
                .gross_realized_profit
                .checked_add(realized)
                .ok_or_else(|| "realized profit overflowed".to_owned())?;
        } else {
            add_level_lot(state, level_index, quantity, quote)?;
        }
    } else {
        let realized = apply_neutral_fill(state, shape.side, quantity, quote)?;
        state.gross_realized_profit = state
            .gross_realized_profit
            .checked_add(realized)
            .ok_or_else(|| "neutral realized profit overflowed".to_owned())?;
    }
    Ok(())
}

fn validate_directional_position(state: &StrategyState, shape: &OrderShape) -> Result<(), String> {
    match state.direction {
        Direction::Long => {
            let valid_shape = if shape.reduce_only {
                shape.side == OrderSide::Sell
            } else {
                shape.side == OrderSide::Buy
            };
            if !valid_shape || state.grid_position_net_quantity < Decimal::ZERO {
                return Err("long strategy execution violates owned-position direction".into());
            }
        }
        Direction::Short => {
            let valid_shape = if shape.reduce_only {
                shape.side == OrderSide::Buy
            } else {
                shape.side == OrderSide::Sell
            };
            if !valid_shape || state.grid_position_net_quantity > Decimal::ZERO {
                return Err("short strategy execution violates owned-position direction".into());
            }
        }
        Direction::Neutral => {}
    }
    Ok(())
}

fn add_level_lot(
    state: &mut StrategyState,
    level_index: u16,
    quantity: Decimal,
    entry_value: Decimal,
) -> Result<(), String> {
    if quantity <= Decimal::ZERO || entry_value <= Decimal::ZERO {
        return Err("opening grid execution has invalid quantity or value".into());
    }
    let lot = state.lots_by_level.entry(level_index).or_insert(LevelLot {
        quantity: Decimal::ZERO,
        entry_value: Decimal::ZERO,
    });
    lot.quantity = lot
        .quantity
        .checked_add(quantity)
        .ok_or_else(|| "level lot quantity overflowed".to_owned())?;
    lot.entry_value = lot
        .entry_value
        .checked_add(entry_value)
        .ok_or_else(|| "level lot value overflowed".to_owned())?;
    Ok(())
}

fn consume_level_lot(
    state: &mut StrategyState,
    level_index: u16,
    quantity: Decimal,
    exit_value: Decimal,
) -> Result<Decimal, String> {
    let Some(lot) = state.lots_by_level.get_mut(&level_index) else {
        return Err(format!(
            "reduce execution has no owned lot at level {level_index}"
        ));
    };
    if quantity > lot.quantity || lot.quantity <= Decimal::ZERO {
        return Err(format!(
            "reduce execution exceeds the owned lot at level {level_index}"
        ));
    }
    let consumed_entry_value = lot
        .entry_value
        .checked_mul(quantity)
        .and_then(|value| value.checked_div(lot.quantity))
        .ok_or_else(|| "level lot entry allocation overflowed".to_owned())?;
    lot.quantity -= quantity;
    lot.entry_value -= consumed_entry_value;
    if lot.quantity.is_zero() {
        state.lots_by_level.remove(&level_index);
    }
    match state.direction {
        Direction::Long => exit_value
            .checked_sub(consumed_entry_value)
            .ok_or_else(|| "long realized profit overflowed".to_owned()),
        Direction::Short => consumed_entry_value
            .checked_sub(exit_value)
            .ok_or_else(|| "short realized profit overflowed".to_owned()),
        Direction::Neutral => Ok(Decimal::ZERO),
    }
}

fn apply_neutral_fill(
    state: &mut StrategyState,
    side: OrderSide,
    quantity: Decimal,
    trade_value: Decimal,
) -> Result<Decimal, String> {
    if quantity <= Decimal::ZERO || trade_value <= Decimal::ZERO {
        return Err("neutral execution is invalid".into());
    }
    let opposing_ids = state
        .neutral_lots
        .iter()
        .filter_map(|(id, lot)| {
            ((lot.signed_quantity > Decimal::ZERO && side == OrderSide::Sell)
                || (lot.signed_quantity < Decimal::ZERO && side == OrderSide::Buy))
                .then_some(*id)
        })
        .collect::<Vec<_>>();
    let mut remaining_quantity = quantity;
    let mut remaining_trade_value = trade_value;
    let mut realized = Decimal::ZERO;

    for id in opposing_ids {
        if remaining_quantity.is_zero() {
            break;
        }
        let lot = state
            .neutral_lots
            .get(&id)
            .cloned()
            .ok_or_else(|| "neutral lot disappeared during matching".to_owned())?;
        let available = lot.signed_quantity.abs();
        let consumed = available.min(remaining_quantity);
        let consumed_trade_value = if consumed == remaining_quantity {
            remaining_trade_value
        } else {
            trade_value
                .checked_mul(consumed)
                .and_then(|value| value.checked_div(quantity))
                .ok_or_else(|| "neutral trade value allocation overflowed".to_owned())?
        };
        let consumed_entry_value = lot
            .entry_value
            .checked_mul(consumed)
            .and_then(|value| value.checked_div(available))
            .ok_or_else(|| "neutral entry allocation overflowed".to_owned())?;
        let lot_profit = if lot.signed_quantity > Decimal::ZERO {
            consumed_trade_value
                .checked_sub(consumed_entry_value)
                .ok_or_else(|| "neutral long profit overflowed".to_owned())?
        } else {
            consumed_entry_value
                .checked_sub(consumed_trade_value)
                .ok_or_else(|| "neutral short profit overflowed".to_owned())?
        };
        realized = realized
            .checked_add(lot_profit)
            .ok_or_else(|| "neutral realized profit overflowed".to_owned())?;

        if consumed == available {
            state.neutral_lots.remove(&id);
        } else {
            let current = state
                .neutral_lots
                .get_mut(&id)
                .ok_or_else(|| "neutral lot disappeared during update".to_owned())?;
            current.signed_quantity = match side {
                OrderSide::Buy => current.signed_quantity.checked_add(consumed),
                OrderSide::Sell => current.signed_quantity.checked_sub(consumed),
            }
            .ok_or_else(|| "neutral lot quantity overflowed".to_owned())?;
            current.entry_value -= consumed_entry_value;
        }
        remaining_quantity -= consumed;
        remaining_trade_value -= consumed_trade_value;
    }

    if remaining_quantity > Decimal::ZERO {
        let id = state.next_neutral_lot_sequence;
        state.next_neutral_lot_sequence = state
            .next_neutral_lot_sequence
            .checked_add(1)
            .ok_or_else(|| "neutral lot sequence overflowed".to_owned())?;
        state.neutral_lots.insert(
            id,
            NeutralLot {
                id,
                signed_quantity: match side {
                    OrderSide::Buy => remaining_quantity,
                    OrderSide::Sell => -remaining_quantity,
                },
                entry_value: remaining_trade_value,
            },
        );
    } else if !remaining_trade_value.is_zero() {
        return Err("neutral trade value allocation is incomplete".into());
    }
    Ok(realized)
}

fn consume_risk_close_lots(
    state: &mut StrategyState,
    side: OrderSide,
    quantity: Decimal,
    exit_value: Decimal,
) -> Result<Decimal, String> {
    if state.direction == Direction::Neutral {
        return apply_neutral_fill(state, side, quantity, exit_value);
    }
    let level_indices = state.lots_by_level.keys().copied().collect::<Vec<_>>();
    let mut remaining_quantity = quantity;
    let mut remaining_exit_value = exit_value;
    let mut realized = Decimal::ZERO;
    for level_index in level_indices {
        if remaining_quantity.is_zero() {
            break;
        }
        let available = state
            .lots_by_level
            .get(&level_index)
            .map_or(Decimal::ZERO, |lot| lot.quantity);
        if available.is_zero() {
            continue;
        }
        let consumed = available.min(remaining_quantity);
        let consumed_exit_value = if consumed == remaining_quantity {
            remaining_exit_value
        } else {
            exit_value
                .checked_mul(consumed)
                .and_then(|value| value.checked_div(quantity))
                .ok_or_else(|| "risk close exit allocation overflowed".to_owned())?
        };
        realized = realized
            .checked_add(consume_level_lot(
                state,
                level_index,
                consumed,
                consumed_exit_value,
            )?)
            .ok_or_else(|| "risk close realized profit overflowed".to_owned())?;
        remaining_quantity -= consumed;
        remaining_exit_value -= consumed_exit_value;
    }
    if !remaining_quantity.is_zero() || !remaining_exit_value.is_zero() {
        return Err("risk close quantity exceeds the owned directional lots".into());
    }
    Ok(realized)
}

fn counter_shape(
    state: &StrategyState,
    level_index: u16,
    source: &OrderShape,
    quantity: Decimal,
) -> Result<OrderShape, String> {
    let index = usize::from(level_index);
    let lower = *state
        .plan
        .levels
        .get(index)
        .ok_or_else(|| "counter level lower price is missing".to_owned())?;
    let upper = *state
        .plan
        .levels
        .get(index + 1)
        .ok_or_else(|| "counter level upper price is missing".to_owned())?;
    let (side, price, reduce_only) = match (state.direction, source.side) {
        (Direction::Long, OrderSide::Buy) => (OrderSide::Sell, upper, true),
        (Direction::Long, OrderSide::Sell) => (OrderSide::Buy, lower, false),
        (Direction::Short, OrderSide::Sell) => (OrderSide::Buy, lower, true),
        (Direction::Short, OrderSide::Buy) => (OrderSide::Sell, upper, false),
        (Direction::Neutral, OrderSide::Buy) => (OrderSide::Sell, upper, false),
        (Direction::Neutral, OrderSide::Sell) => (OrderSide::Buy, lower, false),
    };
    Ok(OrderShape {
        symbol: state.symbol.clone(),
        side,
        price: Some(price),
        quantity,
        reduce_only,
        kind: OrderKind::Limit,
        time_in_force: source.time_in_force,
    })
}

fn add_obligation(
    state: &mut StrategyState,
    kind: ReplacementObligationKind,
    source_client_order_id: ClientOrderId,
    level_index: u16,
    shape: OrderShape,
    now_ms: u64,
) -> Result<u64, String> {
    shape
        .validate()
        .map_err(|error| format!("replacement obligation is invalid: {error}"))?;
    let id = state.next_obligation_sequence;
    state.next_obligation_sequence = state
        .next_obligation_sequence
        .checked_add(1)
        .ok_or_else(|| "replacement obligation sequence overflowed".to_owned())?;
    state.replacement_obligations.insert(
        id,
        ReplacementObligation {
            id,
            kind,
            source_client_order_id,
            level_index,
            shape,
            created_at_ms: now_ms,
            assigned_client_order_id: None,
        },
    );
    Ok(id)
}

fn process_terminal_order(
    state: &mut StrategyState,
    source_client_order_id: &ClientOrderId,
    purpose: &StrategyOrderPurpose,
    shape: &OrderShape,
    report: &ExecutionReport,
    now_ms: u64,
    obligation_ids: &mut Vec<u64>,
) -> Result<(), String> {
    if matches!(purpose, StrategyOrderPurpose::Opening) {
        let target_quantity = state
            .plan
            .opening_order
            .as_ref()
            .ok_or_else(|| "opening execution has no planned opening target".to_owned())?
            .quantity;
        if state.opening_filled_quantity > target_quantity {
            return Err(format!(
                "opening executions exceeded the planned target: {} of {}",
                state.opening_filled_quantity, target_quantity
            ));
        }
        if state.opening_filled_quantity == target_quantity {
            if state.lifecycle != StrategyLifecycle::AwaitingOpening {
                return Ok(());
            }
            for order in state.orders.values_mut() {
                if order.purpose.is_initial_grid()
                    && order.tracking == StrategyOrderTracking::Dormant
                {
                    order.tracking = StrategyOrderTracking::Ready;
                }
            }
            state.lifecycle = StrategyLifecycle::DeployingGrid;
            return Ok(());
        }

        if state.lifecycle != StrategyLifecycle::AwaitingOpening {
            return Ok(());
        }
        if matches!(
            report.terminal_status,
            Some(TerminalOrderStatus::Cancelled | TerminalOrderStatus::Expired)
        ) {
            let mut remainder_shape = shape.clone();
            remainder_shape.quantity = target_quantity - state.opening_filled_quantity;
            let mut remainder = StrategyOrderRecord {
                client_order_id: source_client_order_id.clone(),
                shape: remainder_shape,
                purpose: StrategyOrderPurpose::Opening,
                tracking: StrategyOrderTracking::Ready,
                exchange_order_id: None,
                cumulative_quantity: Decimal::ZERO,
                cumulative_quote: Decimal::ZERO,
                cumulative_fee: Decimal::ZERO,
                execution_audit: None,
                terminal_status: None,
                terminal_processed: false,
                completed_pair_counted: false,
            };
            validate_order_against_instrument(&remainder, &state.instrument_rules).map_err(
                |error| format!("opening remainder cannot be submitted safely: {error}"),
            )?;
            remainder.client_order_id = state
                .next_client_order_id("o", None, remainder.shape.side)
                .map_err(|error| error.to_string())?;
            state
                .insert_order(remainder)
                .map_err(|error| error.to_string())?;
            return Ok(());
        }
        return Err(format!(
            "opening order ended as {:?} with retained grid quantity {} of {}",
            report.terminal_status, state.opening_filled_quantity, target_quantity
        ));
    }

    if matches!(purpose, StrategyOrderPurpose::RiskClose) {
        if report.terminal_status == Some(TerminalOrderStatus::Rejected) {
            return Err("risk close order was rejected and requires explicit review".into());
        }
        return Ok(());
    }

    if shape.reduce_only && report.cumulative_quantity > Decimal::ZERO {
        let order = state
            .orders
            .get_mut(source_client_order_id)
            .ok_or_else(|| "terminal order disappeared from the strategy".to_owned())?;
        if !order.completed_pair_counted {
            order.completed_pair_counted = true;
            state.completed_pairs = state
                .completed_pairs
                .checked_add(1)
                .ok_or_else(|| "completed pair counter overflowed".to_owned())?;
        }
    }

    if matches!(
        report.terminal_status,
        Some(TerminalOrderStatus::Cancelled | TerminalOrderStatus::Expired)
    ) && normal_grid_replacements_enabled(state.lifecycle)
    {
        let remaining = shape.quantity - report.cumulative_quantity;
        if remaining > Decimal::ZERO {
            let mut replacement = shape.clone();
            replacement.quantity = remaining;
            let level_index = purpose
                .level_index()
                .ok_or_else(|| "cancelled grid order has no level identity".to_owned())?;
            let id = add_obligation(
                state,
                ReplacementObligationKind::RestoreCancelledRemainder,
                source_client_order_id.clone(),
                level_index,
                replacement,
                now_ms,
            )?;
            obligation_ids.push(id);
        }
    } else if report.terminal_status == Some(TerminalOrderStatus::Rejected) {
        return Err("grid order was rejected and requires explicit review".into());
    }
    Ok(())
}

fn normal_grid_replacements_enabled(lifecycle: StrategyLifecycle) -> bool {
    matches!(
        lifecycle,
        StrategyLifecycle::DeployingGrid | StrategyLifecycle::Running
    )
}

fn allocate_opening_delta(
    state: &mut StrategyState,
    delta_quantity: Decimal,
    delta_quote: Decimal,
) -> Result<(), String> {
    if delta_quantity <= Decimal::ZERO || delta_quote <= Decimal::ZERO {
        return Err("opening execution delta is invalid".into());
    }
    let mut protected_orders = state
        .orders
        .values()
        .filter(|order| order.purpose.is_initial_grid() && order.shape.reduce_only)
        .map(|order| {
            order
                .purpose
                .level_index()
                .map(|level_index| (level_index, order.shape.quantity))
                .ok_or_else(|| "initial grid order has no level identity".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    protected_orders.sort_by_key(|(level_index, _)| *level_index);
    let mut remaining_quantity = delta_quantity;
    let mut remaining_quote = delta_quote;
    for (level_index, planned_quantity) in protected_orders {
        if remaining_quantity.is_zero() {
            break;
        }
        let already_allocated = state
            .lots_by_level
            .get(&level_index)
            .map_or(Decimal::ZERO, |lot| lot.quantity);
        let available = planned_quantity - already_allocated;
        if available <= Decimal::ZERO {
            continue;
        }
        let quantity = available.min(remaining_quantity);
        let entry_value = if quantity == remaining_quantity {
            remaining_quote
        } else {
            delta_quote
                .checked_mul(quantity)
                .and_then(|value| value.checked_div(delta_quantity))
                .ok_or_else(|| "opening lot value allocation overflowed".to_owned())?
        };
        add_level_lot(state, level_index, quantity, entry_value)?;
        remaining_quantity -= quantity;
        remaining_quote -= entry_value;
    }
    if !remaining_quantity.is_zero() || !remaining_quote.is_zero() {
        return Err("opening lot allocation is incomplete".into());
    }
    Ok(())
}

fn refresh_running_state(state: &mut StrategyState) {
    if state.lifecycle != StrategyLifecycle::DeployingGrid {
        return;
    }
    let initial_order_ids = state
        .orders
        .values()
        .filter(|order| order.purpose.is_initial_grid())
        .map(|order| order.client_order_id.clone())
        .collect::<Vec<_>>();
    let represented = initial_order_ids.iter().all(|client_order_id| {
        deployment_order_is_represented(state, client_order_id, &mut BTreeSet::new())
    });
    if represented {
        state.lifecycle = StrategyLifecycle::Running;
        state.initial_deployment_complete = true;
    }
}

fn deployment_order_is_represented(
    state: &StrategyState,
    client_order_id: &ClientOrderId,
    visiting: &mut BTreeSet<ClientOrderId>,
) -> bool {
    let Some(order) = state.orders.get(client_order_id) else {
        return false;
    };
    match &order.tracking {
        StrategyOrderTracking::Intent {
            state: IntentState::Accepted { .. },
        } => true,
        StrategyOrderTracking::Intent {
            state: IntentState::Terminal { status, .. },
        } if order.terminal_processed
            && order.terminal_status == Some(*status)
            && *status != TerminalOrderStatus::Rejected =>
        {
            if !visiting.insert(client_order_id.clone()) {
                return false;
            }
            let obligations = state
                .replacement_obligations
                .values()
                .filter(|obligation| obligation.source_client_order_id == *client_order_id)
                .collect::<Vec<_>>();
            let represented = !obligations.is_empty()
                && obligations.iter().all(|obligation| {
                    obligation
                        .assigned_client_order_id
                        .as_ref()
                        .is_some_and(|replacement_id| {
                            deployment_order_is_represented(state, replacement_id, visiting)
                        })
                });
            visiting.remove(client_order_id);
            represented
        }
        StrategyOrderTracking::Dormant
        | StrategyOrderTracking::Ready
        | StrategyOrderTracking::Intent { .. } => false,
    }
}

fn fail_transition(state: &mut StrategyState, message: impl Into<String>) -> StrategyTransition {
    let message = message.into();
    state.fail(message.clone());
    StrategyTransition::Failed { message }
}

fn materialize_replacement_orders(
    state: &mut StrategyState,
    fresh_rules: &InstrumentRules,
) -> StrategyTransition {
    if fresh_rules != &state.instrument_rules {
        return fail_transition(
            state,
            "exchange instrument rules changed after the strategy plan was created",
        );
    }
    let mut pending_ids = state
        .replacement_obligations
        .iter()
        .filter_map(|(id, obligation)| obligation.assigned_client_order_id.is_none().then_some(*id))
        .collect::<Vec<_>>();
    if pending_ids.is_empty() {
        return StrategyTransition::NoChange;
    }

    let mut created = Vec::new();
    while let Some(first_id) = pending_ids.first().copied() {
        let Some(first) = state.replacement_obligations.get(&first_id).cloned() else {
            return fail_transition(state, "replacement obligation disappeared during planning");
        };
        let compatible_ids = pending_ids
            .iter()
            .copied()
            .filter(|id| {
                state
                    .replacement_obligations
                    .get(id)
                    .is_some_and(|candidate| obligations_are_compatible(&first, candidate))
            })
            .collect::<Vec<_>>();
        pending_ids.retain(|id| !compatible_ids.contains(id));

        if first.shape.reduce_only {
            for id in compatible_ids {
                let Some(obligation) = state.replacement_obligations.get(&id) else {
                    return fail_transition(state, "replacement obligation is missing");
                };
                if !replacement_quantity_is_valid(&obligation.shape, fresh_rules) {
                    return fail_transition(
                        state,
                        "exact reduce-only replacement violates the exchange LOT_SIZE quantity filter",
                    );
                }
                if let Err(message) =
                    materialize_obligation_bucket(state, &[id], fresh_rules, &mut created)
                {
                    return fail_transition(state, message);
                }
            }
            continue;
        }

        if let Err(message) =
            materialize_non_reduce_obligations(state, compatible_ids, fresh_rules, &mut created)
        {
            return fail_transition(state, message);
        }
    }

    if created.is_empty() {
        StrategyTransition::NoChange
    } else {
        StrategyTransition::ReplacementOrdersReady {
            client_order_ids: created,
        }
    }
}

fn materialize_non_reduce_obligations(
    state: &mut StrategyState,
    obligation_ids: Vec<u64>,
    rules: &InstrumentRules,
    created: &mut Vec<ClientOrderId>,
) -> Result<(), String> {
    let planned_buckets = plan_non_reduce_obligation_buckets(state, obligation_ids, rules)?;
    for bucket in planned_buckets {
        materialize_obligation_bucket(state, &bucket, rules, created)?;
    }
    Ok(())
}

const MAX_EXACT_REPLACEMENT_PARTITION_ITEMS: usize = 64;
const MAX_EXACT_REPLACEMENT_PARTITION_STATES: usize = 1_000_000;

#[derive(Clone)]
struct PlannedObligationBucket {
    obligation_ids: Vec<u64>,
    quantity: Decimal,
}

struct ExactObligationPartition<'a> {
    template: OrderShape,
    rules: &'a InstrumentRules,
    items: Vec<(u64, Decimal)>,
    total_quantity: Decimal,
    best_quantity: Decimal,
    best_buckets: Vec<Vec<u64>>,
    visited: BTreeSet<(usize, Vec<Decimal>)>,
    explored_states: usize,
    search_limit_reached: bool,
}

impl ExactObligationPartition<'_> {
    fn explore(&mut self, index: usize, buckets: &mut Vec<PlannedObligationBucket>) {
        if self.best_quantity == self.total_quantity || self.search_limit_reached {
            return;
        }
        self.explored_states = self.explored_states.saturating_add(1);
        if self.explored_states > MAX_EXACT_REPLACEMENT_PARTITION_STATES {
            self.search_limit_reached = true;
            return;
        }

        // Every item in this search has the same immutable order shape, so future
        // feasibility depends on bucket quantities rather than source identities.
        let mut canonical_quantities = buckets
            .iter()
            .map(|bucket| bucket.quantity)
            .collect::<Vec<_>>();
        canonical_quantities.sort();
        if !self.visited.insert((index, canonical_quantities)) {
            return;
        }

        if index == self.items.len() {
            let mut valid_buckets = Vec::new();
            let mut assigned_quantity = Decimal::ZERO;
            for bucket in buckets.iter() {
                if replacement_quantity_is_valid(
                    &order_shape_with_quantity(&self.template, bucket.quantity),
                    self.rules,
                ) {
                    assigned_quantity += bucket.quantity;
                    valid_buckets.push(bucket.obligation_ids.clone());
                }
            }
            if assigned_quantity > self.best_quantity
                || (assigned_quantity == self.best_quantity
                    && !valid_buckets.is_empty()
                    && (self.best_buckets.is_empty()
                        || valid_buckets.len() < self.best_buckets.len()))
            {
                self.best_quantity = assigned_quantity;
                self.best_buckets = valid_buckets;
            }
            return;
        }

        let (obligation_id, quantity) = self.items[index];
        let maximum = self.rules.limit_quantity.max;
        let mut attempted_quantities = BTreeSet::new();
        for bucket_index in 0..buckets.len() {
            let current_quantity = buckets[bucket_index].quantity;
            if !attempted_quantities.insert(current_quantity) {
                continue;
            }
            let Some(combined_quantity) = current_quantity.checked_add(quantity) else {
                self.search_limit_reached = true;
                return;
            };
            if maximum.is_some_and(|limit| combined_quantity > limit) {
                continue;
            }
            buckets[bucket_index].quantity = combined_quantity;
            buckets[bucket_index].obligation_ids.push(obligation_id);
            self.explore(index + 1, buckets);
            buckets[bucket_index].obligation_ids.pop();
            buckets[bucket_index].quantity = current_quantity;
            if self.best_quantity == self.total_quantity || self.search_limit_reached {
                return;
            }
        }

        buckets.push(PlannedObligationBucket {
            obligation_ids: vec![obligation_id],
            quantity,
        });
        self.explore(index + 1, buckets);
        buckets.pop();
    }
}

fn order_shape_with_quantity(template: &OrderShape, quantity: Decimal) -> OrderShape {
    let mut shape = template.clone();
    shape.quantity = quantity;
    shape
}

fn plan_non_reduce_obligation_buckets(
    state: &StrategyState,
    obligation_ids: Vec<u64>,
    rules: &InstrumentRules,
) -> Result<Vec<Vec<u64>>, String> {
    if obligation_ids.is_empty() {
        return Ok(Vec::new());
    }
    let combined = combined_obligation_shape(state, &obligation_ids)
        .ok_or_else(|| "replacement obligation bucket is inconsistent".to_owned())?;
    let total_quantity = combined.quantity;
    if replacement_quantity_is_valid(&combined, rules) {
        return Ok(vec![obligation_ids]);
    }

    let greedy = greedy_non_reduce_obligation_buckets(state, &obligation_ids, rules)?;
    let greedy_quantity = greedy.iter().try_fold(Decimal::ZERO, |total, bucket| {
        combined_obligation_shape(state, bucket).and_then(|shape| total.checked_add(shape.quantity))
    });
    let Some(greedy_quantity) = greedy_quantity else {
        return Err("replacement obligation quantity overflowed during planning".into());
    };
    if greedy_quantity == combined.quantity {
        return Ok(greedy);
    }
    if obligation_ids.len() > MAX_EXACT_REPLACEMENT_PARTITION_ITEMS {
        return Err(format!(
            "replacement obligation partition requires more than {MAX_EXACT_REPLACEMENT_PARTITION_ITEMS} exact items"
        ));
    }

    let mut items = obligation_ids
        .iter()
        .map(|id| {
            state
                .replacement_obligations
                .get(id)
                .map(|obligation| (*id, obligation.shape.quantity))
                .ok_or_else(|| "replacement obligation is missing".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    items.sort_by(|(left_id, left_quantity), (right_id, right_quantity)| {
        right_quantity
            .cmp(left_quantity)
            .then_with(|| left_id.cmp(right_id))
    });
    let mut search = ExactObligationPartition {
        template: combined,
        rules,
        items,
        total_quantity,
        best_quantity: greedy_quantity,
        best_buckets: greedy,
        visited: BTreeSet::new(),
        explored_states: 0,
        search_limit_reached: false,
    };
    search.explore(0, &mut Vec::new());
    if search.search_limit_reached && search.best_quantity < search.total_quantity {
        return Err(format!(
            "replacement obligation partition exceeded {MAX_EXACT_REPLACEMENT_PARTITION_STATES} deterministic states"
        ));
    }
    for bucket in &mut search.best_buckets {
        bucket.sort_unstable();
    }
    search
        .best_buckets
        .sort_by_key(|bucket| bucket.first().copied().unwrap_or(u64::MAX));
    Ok(search.best_buckets)
}

fn greedy_non_reduce_obligation_buckets(
    state: &StrategyState,
    obligation_ids: &[u64],
    rules: &InstrumentRules,
) -> Result<Vec<Vec<u64>>, String> {
    let mut residual_buckets = Vec::<Vec<u64>>::new();
    let mut ready_buckets = Vec::<Vec<u64>>::new();
    for id in obligation_ids.iter().copied() {
        let mut submit_bucket = None;
        for (index, residual) in residual_buckets.iter().enumerate() {
            let mut candidate = residual.clone();
            candidate.push(id);
            let shape = combined_obligation_shape(state, &candidate)
                .ok_or_else(|| "replacement obligation bucket is inconsistent".to_owned())?;
            if replacement_quantity_is_valid(&shape, rules) {
                submit_bucket = Some((index, candidate));
                break;
            }
        }
        if let Some((index, candidate)) = submit_bucket {
            residual_buckets.remove(index);
            ready_buckets.push(candidate);
            continue;
        }

        if replacement_quantity_is_valid(
            &combined_obligation_shape(state, &[id])
                .ok_or_else(|| "replacement obligation bucket is inconsistent".to_owned())?,
            rules,
        ) {
            ready_buckets.push(vec![id]);
            continue;
        }

        let mut appended = false;
        for residual in &mut residual_buckets {
            let mut candidate = residual.clone();
            candidate.push(id);
            let shape = combined_obligation_shape(state, &candidate)
                .ok_or_else(|| "replacement obligation bucket is inconsistent".to_owned())?;
            if rules
                .limit_quantity
                .max
                .is_none_or(|maximum| shape.quantity <= maximum)
            {
                *residual = candidate;
                appended = true;
                break;
            }
        }
        if !appended {
            residual_buckets.push(vec![id]);
        }
    }
    Ok(ready_buckets)
}

fn obligations_are_compatible(
    first: &ReplacementObligation,
    candidate: &ReplacementObligation,
) -> bool {
    first.kind == candidate.kind
        && first.level_index == candidate.level_index
        && first.shape.symbol == candidate.shape.symbol
        && first.shape.side == candidate.shape.side
        && first.shape.price == candidate.shape.price
        && first.shape.reduce_only == candidate.shape.reduce_only
        && first.shape.kind == candidate.shape.kind
        && first.shape.time_in_force == candidate.shape.time_in_force
}

fn combined_obligation_shape(state: &StrategyState, obligation_ids: &[u64]) -> Option<OrderShape> {
    let first = state.replacement_obligations.get(obligation_ids.first()?)?;
    let mut shape = first.shape.clone();
    shape.quantity = obligation_ids.iter().try_fold(Decimal::ZERO, |total, id| {
        let obligation = state.replacement_obligations.get(id)?;
        obligations_are_compatible(first, obligation).then_some(())?;
        total.checked_add(obligation.shape.quantity)
    })?;
    Some(shape)
}

fn replacement_quantity_is_valid(shape: &OrderShape, rules: &InstrumentRules) -> bool {
    let quantity = shape.quantity;
    let quantity_rules = &rules.limit_quantity;
    if !quantity_rules.is_aligned(quantity)
        || quantity < quantity_rules.min
        || quantity_rules.max.is_some_and(|maximum| quantity > maximum)
    {
        return false;
    }
    if shape.reduce_only {
        return true;
    }
    if quantity < quantity_rules.min {
        return false;
    }
    shape.price.is_some_and(|price| {
        price
            .checked_mul(quantity)
            .is_some_and(|notional| notional >= rules.min_notional)
    })
}

fn materialize_obligation_bucket(
    state: &mut StrategyState,
    obligation_ids: &[u64],
    rules: &InstrumentRules,
    created: &mut Vec<ClientOrderId>,
) -> Result<(), String> {
    let shape = combined_obligation_shape(state, obligation_ids)
        .ok_or_else(|| "replacement obligation bucket is inconsistent".to_owned())?;
    if !replacement_quantity_is_valid(&shape, rules) {
        return Err("replacement obligation quantity is not currently submit-safe".into());
    }
    let first_id = obligation_ids
        .first()
        .ok_or_else(|| "replacement obligation bucket is empty".to_owned())?;
    let level_index = state
        .replacement_obligations
        .get(first_id)
        .ok_or_else(|| "replacement obligation is missing".to_owned())?
        .level_index;
    let client_order_id = state
        .next_client_order_id("r", Some(level_index), shape.side)
        .map_err(|error| error.to_string())?;
    state
        .insert_order(StrategyOrderRecord {
            client_order_id: client_order_id.clone(),
            shape,
            purpose: StrategyOrderPurpose::Replacement {
                level_index,
                obligation_ids: obligation_ids.to_vec(),
            },
            tracking: StrategyOrderTracking::Ready,
            exchange_order_id: None,
            cumulative_quantity: Decimal::ZERO,
            cumulative_quote: Decimal::ZERO,
            cumulative_fee: Decimal::ZERO,
            execution_audit: None,
            terminal_status: None,
            terminal_processed: false,
            completed_pair_counted: false,
        })
        .map_err(|error| error.to_string())?;
    for id in obligation_ids {
        let obligation = state
            .replacement_obligations
            .get_mut(id)
            .ok_or_else(|| "replacement obligation disappeared during assignment".to_owned())?;
        obligation.assigned_client_order_id = Some(client_order_id.clone());
    }
    created.push(client_order_id);
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StrategyStateError {
    #[error("strategy run ID must be 8-12 ASCII letters or digits")]
    InvalidRunId,
    #[error("strategy configuration is invalid: {0}")]
    InvalidConfig(crate::domain::GridConfigError),
    #[error("strategy instrument rules are invalid: {0}")]
    InvalidInstrument(crate::domain::InstrumentRulesError),
    #[error("strategy configuration must identify an exchange")]
    MissingExchange,
    #[error("strategy configuration identity does not match its snapshot")]
    ConfigIdentityMismatch,
    #[error("strategy symbol is invalid")]
    InvalidSymbol,
    #[error("strategy baseline is invalid")]
    InvalidBaseline,
    #[error("existing one-way position cannot be isolated from this grid direction")]
    BaselineDirectionConflict,
    #[error("stop loss or take profit is on the unsafe side of the strategy reference")]
    InvalidRiskPriceDirection,
    #[error("risk exit state is incomplete or invalid")]
    InvalidRiskExitState,
    #[error("triggered strategy activation metadata is incomplete or invalid")]
    InvalidTriggerActivation,
    #[error("market price must be positive")]
    InvalidMarketPrice,
    #[error("grid-owned risk close quantity cannot be represented by market rules")]
    RiskCloseQuantityInvalid,
    #[error("strategy plan failed deterministic validation: {0}")]
    InvalidPlan(GridPlanError),
    #[error("initial grid order ledger does not exactly match the immutable grid plan")]
    InitialGridOrderMismatch,
    #[error("initial grid deployment flag does not match the strategy lifecycle")]
    InitialDeploymentStateMismatch,
    #[error("unsupported strategy state version {0}")]
    UnsupportedVersion(u8),
    #[error("strategy timestamp regressed")]
    TimestampRegression,
    #[error("grid-owned position has the wrong directional sign")]
    GridPositionDirectionMismatch,
    #[error("strategy order identity or symbol does not match its map key")]
    OrderIdentityMismatch,
    #[error("strategy order exchange identity is missing or inconsistent")]
    ExchangeOrderIdentityMismatch,
    #[error("strategy contains an invalid order: {0}")]
    InvalidOrderIntent(crate::domain::OrderIntentError),
    #[error("strategy order violates the persisted exchange instrument rules")]
    OrderViolatesInstrumentRules,
    #[error("strategy execution totals are invalid")]
    InvalidExecutionTotals,
    #[error("strategy aggregate accounting does not match its durable order ledger")]
    AggregateAccountingMismatch,
    #[error("strategy inventory events do not match the durable order ledger")]
    InventoryEventLedgerMismatch,
    #[error("opening execution totals do not match the durable opening ledger")]
    OpeningAccountingMismatch,
    #[error("opening order chain does not match the immutable opening plan")]
    OpeningOrderMismatch,
    #[error("strategy execution audit evidence is invalid")]
    InvalidExecutionAudit,
    #[error("terminal order processing state is inconsistent")]
    TerminalProcessingMismatch,
    #[error("strategy level lot is invalid")]
    InvalidLevelLot,
    #[error("strategy neutral inventory lot is invalid")]
    InvalidNeutralLot,
    #[error("strategy level lots do not cover the grid-owned position")]
    LevelLotCoverageMismatch,
    #[error("strategy level lots do not match the durable execution ledger")]
    LevelLotLedgerMismatch,
    #[error("strategy neutral inventory lots do not cover the grid-owned position")]
    NeutralLotCoverageMismatch,
    #[error("strategy neutral inventory lots do not match the durable execution ledger")]
    NeutralLotLedgerMismatch,
    #[error("replacement obligation identity is invalid")]
    ObligationIdentityMismatch,
    #[error("replacement obligation references a missing assigned order")]
    MissingAssignedReplacement,
    #[error("replacement order does not exactly match its assigned obligations")]
    ReplacementOrderMismatch,
    #[error("replacement obligations are not fully proven by their source order executions")]
    ReplacementObligationLedgerMismatch,
    #[error("strategy generated a duplicate order identity")]
    DuplicateOrderIdentity,
    #[error("strategy order sequence does not follow the append-only order ledger")]
    OrderSequenceMismatch,
    #[error("replacement obligation sequence does not follow the append-only obligation ledger")]
    ObligationSequenceMismatch,
    #[error("strategy lifecycle transition is not allowed")]
    InvalidLifecycleTransition,
    #[error("strategy failure reason does not match its lifecycle")]
    InvalidFailureState,
    #[error("strategy still has accepted or uncertain exchange orders")]
    OrdersNotTerminal,
    #[error("strategy cannot be closed while grid position, lots, or orders remain")]
    CannotCloseStrategy,
    #[error("numeric overflow while calculating {0}")]
    NumericOverflow(&'static str),
}

#[derive(Debug, Error)]
pub enum StrategyStoreError {
    #[error("strategy state failed validation: {0}")]
    InvalidState(StrategyStateError),
    #[error("armed strategy state failed validation or activation: {0}")]
    ArmedStrategy(#[from] super::ArmedStrategyError),
    #[error("injected strategy state write failure")]
    InjectedWriteFailure,
    #[error("strategy state write attempt counter overflowed")]
    WriteAttemptOverflow,
    #[error("strategy state revision does not advance exactly once")]
    RevisionMismatch,
    #[error("strategy state file already exists")]
    AlreadyExists,
    #[error("failed to exclusively create strategy state: {0}")]
    CreateNew(std::io::Error),
    #[error("expected an active strategy file but found an armed strategy")]
    UnexpectedArmedState,
    #[error("expected an armed strategy file but found an active strategy")]
    UnexpectedActiveState,
    #[error("failed to read strategy state: {0}")]
    Read(std::io::Error),
    #[error("strategy state contains invalid JSON: {0}")]
    InvalidJson(serde_json::Error),
    #[error("failed to create strategy state directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to open atomic strategy state writer: {0}")]
    OpenAtomic(std::io::Error),
    #[error("failed to serialize strategy state: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to write strategy state: {0}")]
    Write(std::io::Error),
    #[error("failed to sync strategy state file: {0}")]
    SyncFile(std::io::Error),
    #[error("failed to commit strategy state: {0}")]
    Commit(std::io::Error),
    #[error("failed to sync strategy state directory: {0}")]
    SyncDirectory(std::io::Error),
}

#[derive(Debug, Error)]
pub enum StrategyMachineError {
    #[error(transparent)]
    InvalidState(#[from] StrategyStateError),
    #[error(transparent)]
    Persistence(#[from] StrategyStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            GridConfig, GridMode, InitialOrderType, InstrumentRules, PositionSizingMode,
            QuantityRules,
        },
        engine::{FeeValuation, MarketSnapshot, build_grid_plan},
        exchange::{AuthoritativeOrder, OrderExecutionSnapshot, OrderLifecycle, TradeFill},
        persistence::FileStrategyStateStore,
    };
    use tempfile::tempdir;

    fn decimal(value: i64) -> Decimal {
        Decimal::from(value)
    }

    fn config(direction: Direction) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction,
            upper_price: decimal(1020),
            lower_price: decimal(1000),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(2, 1)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: Some(decimal(1014)),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn instrument() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(1, 1),
            limit_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: None,
            },
            market_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: None,
            },
            min_notional: Decimal::ZERO,
        }
    }

    fn state(direction: Direction, baseline: PositionBaseline) -> StrategyState {
        let config = config(direction);
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1012),
                mark_price: decimal(1012),
            },
            &instrument(),
        )
        .unwrap();
        StrategyState::from_plan(
            StrategyRunId::parse("RUN00001").unwrap(),
            config,
            instrument(),
            plan,
            baseline,
            100,
        )
        .unwrap()
    }

    #[test]
    fn grid_unrealized_profit_uses_owned_lots_and_excludes_the_old_baseline() {
        let mut long = state(
            Direction::Long,
            PositionBaseline::from_authoritative_position(decimal(3), Some(decimal(900))).unwrap(),
        );
        long.grid_position_net_quantity = decimal(2);
        long.lots_by_level.insert(
            1,
            LevelLot {
                quantity: decimal(2),
                entry_value: decimal(2000),
            },
        );
        assert_eq!(
            long.grid_unrealized_profit(decimal(1010)).unwrap(),
            decimal(20)
        );

        let mut short = state(
            Direction::Short,
            PositionBaseline::from_authoritative_position(decimal(-7), Some(decimal(1100)))
                .unwrap(),
        );
        short.grid_position_net_quantity = decimal(-3);
        short.lots_by_level.insert(
            2,
            LevelLot {
                quantity: decimal(3),
                entry_value: decimal(3060),
            },
        );
        assert_eq!(
            short.grid_unrealized_profit(decimal(1000)).unwrap(),
            decimal(60)
        );
    }

    #[test]
    fn neutral_grid_unrealized_profit_values_the_exact_remaining_side() {
        let mut neutral = state(Direction::Neutral, PositionBaseline::flat());
        neutral.grid_position_net_quantity = decimal(-2);
        neutral.neutral_lots.insert(
            1,
            NeutralLot {
                id: 1,
                signed_quantity: decimal(-2),
                entry_value: decimal(2040),
            },
        );
        neutral.next_neutral_lot_sequence = 2;

        assert_eq!(
            neutral.grid_unrealized_profit(decimal(1000)).unwrap(),
            decimal(40)
        );
        assert!(matches!(
            neutral.grid_unrealized_profit(Decimal::ZERO),
            Err(StrategyStateError::InvalidMarketPrice)
        ));
    }

    #[test]
    fn grid_unrealized_profit_fails_closed_on_inventory_coverage_damage() {
        let mut short = state(Direction::Short, PositionBaseline::flat());
        short.grid_position_net_quantity = decimal(-3);
        short.lots_by_level.insert(
            2,
            LevelLot {
                quantity: decimal(2),
                entry_value: decimal(2040),
            },
        );

        assert!(matches!(
            short.grid_unrealized_profit(decimal(1000)),
            Err(StrategyStateError::LevelLotCoverageMismatch)
        ));
    }

    fn opening_id(state: &StrategyState) -> ClientOrderId {
        state
            .orders
            .values()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap()
            .client_order_id
            .clone()
    }

    fn grid_id(
        state: &StrategyState,
        level_index: u16,
        side: OrderSide,
        reduce_only: bool,
    ) -> ClientOrderId {
        state
            .orders
            .values()
            .find(|order| {
                order.purpose.level_index() == Some(level_index)
                    && order.shape.side == side
                    && order.shape.reduce_only == reduce_only
            })
            .unwrap()
            .client_order_id
            .clone()
    }

    fn rekey_order(
        state: &mut StrategyState,
        previous: &ClientOrderId,
        replacement: ClientOrderId,
    ) {
        let mut order = state.orders.remove(previous).unwrap();
        order.client_order_id = replacement.clone();
        assert!(state.orders.insert(replacement, order).is_none());
    }

    fn report(
        client_order_id: ClientOrderId,
        exchange_order_id: &str,
        quantity: Decimal,
        quote: Decimal,
        terminal_status: Option<TerminalOrderStatus>,
    ) -> ExecutionReport {
        ExecutionReport {
            client_order_id,
            exchange_order_id: exchange_order_id.into(),
            cumulative_quantity: quantity,
            cumulative_quote: quote,
            cumulative_fee: Decimal::ZERO,
            terminal_status,
        }
    }

    fn complete_initial_deployment(
        machine: &mut StrategyMachine<MemoryStrategyStateStore>,
        now_ms: u64,
    ) {
        let opening = machine
            .store()
            .snapshot()
            .orders
            .values()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .map(|order| (order.client_order_id.clone(), order.shape.clone()));
        if let Some((client_order_id, shape)) = opening {
            let fill_price = shape
                .price
                .unwrap_or(machine.store().snapshot().plan.reference_price);
            machine
                .apply_execution(
                    &report(
                        client_order_id,
                        "opening-complete",
                        shape.quantity,
                        fill_price.checked_mul(shape.quantity).unwrap(),
                        Some(TerminalOrderStatus::Filled),
                    ),
                    now_ms,
                )
                .unwrap();
        }

        let intents = machine
            .store()
            .snapshot()
            .ready_intents(now_ms + 1)
            .unwrap();
        for (index, mut intent) in intents.into_iter().enumerate() {
            intent.state = IntentState::Accepted {
                exchange_order_id: format!("initial-boundary-{index}"),
            };
            machine
                .synchronize_intent(&intent, now_ms + 2 + index as u64)
                .unwrap();
        }
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::Running
        );
        assert!(machine.store().snapshot().initial_deployment_complete);
    }

    fn short_machine_with_opening() -> StrategyMachine<MemoryStrategyStateStore> {
        let baseline = PositionBaseline {
            signed_quantity: decimal(-3),
            entry_price: Some(decimal(1015)),
        };
        let initial = state(Direction::Short, baseline);
        let opening = opening_id(&initial);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-1",
                    Decimal::new(28, 1),
                    Decimal::new(28392, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
    }

    fn short_machine_with_risk() -> StrategyMachine<MemoryStrategyStateStore> {
        let mut config = config(Direction::Short);
        config.stop_loss_price = Some(decimal(1016));
        config.take_profit_price = Some(decimal(1005));
        let rules = instrument();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1012),
                mark_price: decimal(1012),
            },
            &rules,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("RISK0001").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        StrategyMachine::new(MemoryStrategyStateStore::new(state))
    }

    fn short_risk_machine_with_opening() -> StrategyMachine<MemoryStrategyStateStore> {
        let mut machine = short_machine_with_risk();
        let opening = opening_id(machine.store().snapshot());
        machine
            .apply_execution(
                &report(
                    opening,
                    "risk-opening",
                    Decimal::new(28, 1),
                    Decimal::new(28392, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
    }

    fn small_neutral_machine() -> StrategyMachine<MemoryStrategyStateStore> {
        small_neutral_machine_with_max(None)
    }

    fn small_neutral_machine_with_max(
        maximum_quantity: Option<Decimal>,
    ) -> StrategyMachine<MemoryStrategyStateStore> {
        let config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "ANSEMUSDT".into(),
            direction: Direction::Neutral,
            upper_price: Decimal::new(30, 2),
            lower_price: Decimal::new(26, 2),
            grid_count: 2,
            total_investment: Decimal::ZERO,
            leverage: 2,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(decimal(20)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Market,
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        };
        let instrument = InstrumentRules {
            tick_size: Decimal::new(1, 2),
            limit_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: maximum_quantity,
            },
            market_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: maximum_quantity,
            },
            min_notional: decimal(5),
        };
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(29, 2),
                mark_price: Decimal::new(29, 2),
            },
            &instrument,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("ANSEM001").unwrap(),
            config,
            instrument,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        StrategyMachine::new(MemoryStrategyStateStore::new(state))
    }

    fn cross_order_neutral_machine() -> StrategyMachine<MemoryStrategyStateStore> {
        let mut config = small_neutral_machine().store().snapshot().config.clone();
        config.lower_price = Decimal::new(20, 2);
        config.upper_price = Decimal::new(40, 2);
        config.grid_count = 4;
        let mut instrument = small_neutral_machine()
            .store()
            .snapshot()
            .instrument_rules
            .clone();
        instrument.min_notional = Decimal::ZERO;
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(33, 2),
                mark_price: Decimal::new(33, 2),
            },
            &instrument,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("ANSEM002").unwrap(),
            config,
            instrument,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        StrategyMachine::new(MemoryStrategyStateStore::new(state))
    }

    fn accept_strategy_order(
        machine: &mut StrategyMachine<MemoryStrategyStateStore>,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
        accepted_at_ms: u64,
    ) {
        let mut accepted = machine
            .store()
            .snapshot()
            .ready_intents(accepted_at_ms)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == *client_order_id)
            .unwrap();
        accepted.state = IntentState::Accepted {
            exchange_order_id: exchange_order_id.into(),
        };
        machine
            .synchronize_intent(&accepted, accepted_at_ms)
            .unwrap();
    }

    struct ValuedTradeFixture<'a> {
        exchange_order_id: &'a str,
        trade_id: &'a str,
        price: Decimal,
        quantity: Decimal,
        trade_time_ms: u64,
        applied_at_ms: u64,
    }

    fn apply_single_valued_trade(
        machine: &mut StrategyMachine<MemoryStrategyStateStore>,
        client_order_id: ClientOrderId,
        fixture: ValuedTradeFixture<'_>,
    ) {
        let shape = machine
            .store()
            .snapshot()
            .orders
            .get(&client_order_id)
            .unwrap()
            .shape
            .clone();
        let quote = fixture.price * fixture.quantity;
        let trade = TradeFill {
            trade_id: fixture.trade_id.into(),
            exchange_order_id: fixture.exchange_order_id.into(),
            symbol: shape.symbol.clone(),
            side: shape.side,
            price: fixture.price,
            quantity: fixture.quantity,
            quote_quantity: quote,
            raw_commission: Decimal::ZERO,
            commission_cost: Decimal::ZERO,
            commission_asset: "USDT".into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: fixture.trade_time_ms,
        };
        let snapshot = OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: client_order_id.clone(),
                exchange_order_id: fixture.exchange_order_id.into(),
                exchange: Exchange::Aster,
                shape,
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
                executed_quantity: None,
            },
            cumulative_quantity: fixture.quantity,
            cumulative_quote: quote,
            fees_by_asset: [("USDT".into(), Decimal::ZERO)].into_iter().collect(),
            trades: vec![trade],
            order_time_ms: fixture.trade_time_ms - 1,
            update_time_ms: fixture.trade_time_ms,
        };
        let valued = ValuedExecutionReport {
            report: report(
                client_order_id,
                fixture.exchange_order_id,
                fixture.quantity,
                quote,
                Some(TerminalOrderStatus::Cancelled),
            ),
            fee_valuations: vec![FeeValuation {
                trade_id: fixture.trade_id.into(),
                fee_asset: "USDT".into(),
                fee_amount: Decimal::ZERO,
                quote_asset: "USDT".into(),
                quote_value: Decimal::ZERO,
                source: FeeValuationSource::ExchangeZero,
                valuation_symbol: None,
                valuation_minute_start_ms: None,
                valuation_price: None,
            }],
        };
        machine
            .apply_valued_execution(&snapshot, &valued, fixture.applied_at_ms)
            .unwrap();
    }

    fn small_neutral_risk_machine() -> StrategyMachine<MemoryStrategyStateStore> {
        let mut config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "ANSEMUSDT".into(),
            direction: Direction::Neutral,
            upper_price: Decimal::new(30, 2),
            lower_price: Decimal::new(26, 2),
            grid_count: 2,
            total_investment: Decimal::ZERO,
            leverage: 2,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(decimal(20)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Market,
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        };
        config.stop_loss_price = Some(Decimal::new(25, 2));
        config.take_profit_price = Some(Decimal::new(31, 2));
        let instrument = InstrumentRules {
            tick_size: Decimal::new(1, 2),
            limit_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: None,
            },
            market_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: None,
            },
            min_notional: decimal(5),
        };
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(29, 2),
                mark_price: Decimal::new(29, 2),
            },
            &instrument,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("ANSEMR01").unwrap(),
            config,
            instrument,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        StrategyMachine::new(MemoryStrategyStateStore::new(state))
    }

    fn replacement_orders(state: &StrategyState) -> Vec<&StrategyOrderRecord> {
        state
            .orders
            .values()
            .filter(|order| matches!(order.purpose, StrategyOrderPurpose::Replacement { .. }))
            .collect()
    }

    #[test]
    fn directional_state_keeps_existing_position_as_an_immutable_baseline() {
        let baseline = PositionBaseline {
            signed_quantity: decimal(-3),
            entry_price: Some(decimal(1015)),
        };
        let state = state(Direction::Short, baseline.clone());

        assert_eq!(state.lifecycle, StrategyLifecycle::AwaitingOpening);
        assert_eq!(state.baseline, baseline);
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert_eq!(state.expected_exchange_position().unwrap(), decimal(-3));
        assert_eq!(state.ready_intents(100).unwrap().len(), 1);
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| order.tracking == StrategyOrderTracking::Dormant)
                .count(),
            20
        );
    }

    #[test]
    fn opposite_or_neutral_baseline_is_rejected_when_it_cannot_be_isolated() {
        for (direction, signed_quantity) in [
            (Direction::Short, decimal(3)),
            (Direction::Long, decimal(-3)),
            (Direction::Neutral, decimal(3)),
        ] {
            let config = config(direction);
            let rules = instrument();
            let plan = build_grid_plan(
                &config,
                &MarketSnapshot {
                    last_price: decimal(1012),
                    mark_price: decimal(1012),
                },
                &rules,
            )
            .unwrap();

            assert_eq!(
                StrategyState::from_plan(
                    StrategyRunId::parse("BASEBAD1").unwrap(),
                    config,
                    rules,
                    plan,
                    PositionBaseline {
                        signed_quantity,
                        entry_price: Some(decimal(1010)),
                    },
                    100,
                ),
                Err(StrategyStateError::BaselineDirectionConflict)
            );
        }
    }

    #[test]
    fn position_reconciliation_never_rewrites_the_owned_ledger() {
        let mut machine = short_machine_with_opening();
        let expected = machine
            .store()
            .snapshot()
            .expected_exchange_position()
            .unwrap();
        let revision = machine.store().snapshot().revision;

        assert_eq!(
            machine.reconcile_position(expected, 110).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(machine.store().snapshot().revision, revision);

        let transition = machine
            .reconcile_position(expected + Decimal::new(1, 1), 111)
            .unwrap();
        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.expected_exchange_position().unwrap(), expected);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert_eq!(state.baseline.signed_quantity, decimal(-3));
    }

    #[test]
    fn risk_prices_must_be_on_the_correct_side_of_the_strategy() {
        let mut config = config(Direction::Short);
        config.stop_loss_price = Some(decimal(1005));
        let rules = instrument();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1012),
                mark_price: decimal(1012),
            },
            &rules,
        )
        .unwrap();

        assert_eq!(
            StrategyState::from_plan(
                StrategyRunId::parse("BADRISK1").unwrap(),
                config,
                rules,
                plan,
                PositionBaseline::flat(),
                100,
            ),
            Err(StrategyStateError::InvalidRiskPriceDirection)
        );
    }

    #[test]
    fn risk_price_hit_blocks_all_new_orders_without_using_grid_boundaries() {
        let mut machine = short_machine_with_risk();
        let revision = machine.store().snapshot().revision;

        assert_eq!(
            machine.evaluate_risk_price(decimal(1015), 101).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(machine.store().snapshot().revision, revision);
        assert_eq!(
            machine.evaluate_risk_price(decimal(1016), 102).unwrap(),
            StrategyTransition::RiskExitRequested {
                reason: RiskExitReason::StopLoss,
                mark_price: decimal(1016)
            }
        );

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::RiskExitRequested);
        assert_eq!(state.risk_exit_reason, Some(RiskExitReason::StopLoss));
        assert_eq!(state.risk_trigger_mark_price, Some(decimal(1016)));
        assert!(state.ready_intents(103).unwrap().is_empty());
        let rules = state.instrument_rules.clone();
        assert_eq!(
            machine.materialize_replacements(&rules, 103).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(
            machine.request_stop(104).unwrap(),
            StrategyTransition::NoChange
        );
    }

    #[test]
    fn crossing_a_grid_boundary_without_configured_risk_price_does_nothing() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        let revision = machine.store().snapshot().revision;

        assert_eq!(
            machine.evaluate_risk_price(decimal(2000), 101).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(machine.store().snapshot().revision, revision);
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::AwaitingOpening
        );
    }

    #[test]
    fn running_strategies_with_baselines_ignore_both_grid_boundaries_without_risk_prices() {
        for direction in [Direction::Long, Direction::Short, Direction::Neutral] {
            let baseline = match direction {
                Direction::Long => PositionBaseline {
                    signed_quantity: decimal(3),
                    entry_price: Some(decimal(1005)),
                },
                Direction::Short => PositionBaseline {
                    signed_quantity: decimal(-3),
                    entry_price: Some(decimal(1015)),
                },
                Direction::Neutral => PositionBaseline::flat(),
            };
            let mut machine =
                StrategyMachine::new(MemoryStrategyStateStore::new(state(direction, baseline)));
            complete_initial_deployment(&mut machine, 200);

            let before = machine.store().snapshot().clone();
            assert!(before.config.stop_loss_price.is_none());
            assert!(before.config.take_profit_price.is_none());
            for mark_price in [decimal(900), decimal(1100)] {
                assert_eq!(
                    machine.evaluate_risk_price(mark_price, 300).unwrap(),
                    StrategyTransition::NoChange
                );
                assert_eq!(machine.store().snapshot(), &before);
            }
        }
    }

    #[test]
    fn favorable_boundary_round_trip_preserves_the_baseline_and_exact_grid_quantity() {
        for (case_index, direction, baseline, outside_price) in [
            (
                0_u64,
                Direction::Short,
                PositionBaseline {
                    signed_quantity: decimal(-3),
                    entry_price: Some(decimal(1015)),
                },
                decimal(900),
            ),
            (
                1_u64,
                Direction::Long,
                PositionBaseline {
                    signed_quantity: decimal(3),
                    entry_price: Some(decimal(1005)),
                },
                decimal(1100),
            ),
        ] {
            let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state(
                direction,
                baseline.clone(),
            )));
            complete_initial_deployment(&mut machine, 200 + case_index * 1_000);

            let before = machine.store().snapshot().clone();
            let initial_grid_quantity = before.grid_position_net_quantity;
            let initial_expected_position = before.expected_exchange_position().unwrap();
            let mut profit_orders = before
                .orders
                .values()
                .filter(|order| order.purpose.is_initial_grid() && order.shape.reduce_only)
                .map(|order| {
                    (
                        order.purpose.level_index().unwrap(),
                        order.client_order_id.clone(),
                        order.shape.clone(),
                    )
                })
                .collect::<Vec<_>>();
            profit_orders.sort_by_key(|(level_index, _, _)| *level_index);
            assert_eq!(
                profit_orders
                    .iter()
                    .map(|(_, _, shape)| shape.quantity)
                    .sum::<Decimal>(),
                initial_grid_quantity.abs()
            );

            assert_eq!(
                machine
                    .evaluate_risk_price(outside_price, 300 + case_index * 1_000)
                    .unwrap(),
                StrategyTransition::NoChange
            );
            for (offset, (_, client_order_id, shape)) in profit_orders.iter().enumerate() {
                let price = shape.price.unwrap();
                let exchange_order_id = machine.store().snapshot().orders[client_order_id]
                    .exchange_order_id
                    .clone()
                    .unwrap();
                machine
                    .apply_execution(
                        &report(
                            client_order_id.clone(),
                            &exchange_order_id,
                            shape.quantity,
                            price.checked_mul(shape.quantity).unwrap(),
                            Some(TerminalOrderStatus::Filled),
                        ),
                        310 + case_index * 1_000 + offset as u64,
                    )
                    .unwrap();
            }

            let flattened = machine.store().snapshot();
            assert_eq!(flattened.baseline, baseline);
            assert_eq!(flattened.grid_position_net_quantity, Decimal::ZERO);
            assert_eq!(
                flattened.expected_exchange_position().unwrap(),
                baseline.signed_quantity
            );
            assert!(flattened.lots_by_level.is_empty());
            assert_eq!(
                flattened
                    .replacement_obligations
                    .values()
                    .filter(|obligation| {
                        obligation.kind == ReplacementObligationKind::Counter
                            && obligation.assigned_client_order_id.is_none()
                    })
                    .count(),
                profit_orders.len()
            );
            assert!(flattened.orders.values().all(|order| {
                order.purpose != StrategyOrderPurpose::RiskClose
                    && order.shape.kind == OrderKind::Limit
            }));

            let rules = flattened.instrument_rules.clone();
            let counter_ids = match machine
                .materialize_replacements(&rules, 400 + case_index * 1_000)
                .unwrap()
            {
                StrategyTransition::ReplacementOrdersReady { client_order_ids } => client_order_ids,
                other => panic!("expected exact counter orders, got {other:?}"),
            };
            assert_eq!(counter_ids.len(), profit_orders.len());
            for (offset, client_order_id) in counter_ids.iter().enumerate() {
                let counter = machine.store().snapshot().orders[client_order_id].clone();
                let source = profit_orders
                    .iter()
                    .find(|(level_index, _, _)| Some(*level_index) == counter.purpose.level_index())
                    .unwrap();
                assert_eq!(counter.shape.quantity, source.2.quantity);
                assert!(!counter.shape.reduce_only);
                assert_ne!(counter.shape.side, source.2.side);
                assert_eq!(counter.shape.kind, OrderKind::Limit);

                accept_strategy_order(
                    &mut machine,
                    client_order_id,
                    &format!("boundary-counter-{case_index}-{offset}"),
                    410 + case_index * 1_000 + offset as u64,
                );
            }
            for (offset, client_order_id) in counter_ids.into_iter().enumerate() {
                let shape = machine.store().snapshot().orders[&client_order_id]
                    .shape
                    .clone();
                let price = shape.price.unwrap();
                machine
                    .apply_execution(
                        &report(
                            client_order_id,
                            &format!("boundary-counter-{case_index}-{offset}"),
                            shape.quantity,
                            price.checked_mul(shape.quantity).unwrap(),
                            Some(TerminalOrderStatus::Filled),
                        ),
                        450 + case_index * 1_000 + offset as u64,
                    )
                    .unwrap();
            }

            let restored = machine.store().snapshot();
            assert_eq!(restored.baseline, baseline);
            assert_eq!(restored.grid_position_net_quantity, initial_grid_quantity);
            assert_eq!(
                restored.expected_exchange_position().unwrap(),
                initial_expected_position
            );
            assert_eq!(
                restored
                    .lots_by_level
                    .values()
                    .map(|lot| lot.quantity)
                    .sum::<Decimal>(),
                initial_grid_quantity.abs()
            );
            assert!(restored.orders.values().all(|order| {
                order.purpose != StrategyOrderPurpose::RiskClose
                    && order.shape.kind == OrderKind::Limit
            }));
            assert_eq!(restored.validate(), Ok(()));
        }
    }

    #[test]
    fn partial_opening_is_owned_before_risk_exit_and_terminal_cancel() {
        let mut machine = short_machine_with_risk();
        let opening = opening_id(machine.store().snapshot());
        machine
            .apply_execution(
                &report(
                    opening.clone(),
                    "partial-opening",
                    Decimal::new(1, 1),
                    Decimal::new(1014, 1),
                    None,
                ),
                101,
            )
            .unwrap();
        assert_eq!(
            machine
                .store()
                .snapshot()
                .lots_by_level
                .values()
                .map(|lot| lot.quantity)
                .sum::<Decimal>(),
            Decimal::new(1, 1)
        );

        machine.evaluate_risk_price(decimal(1016), 102).unwrap();
        machine
            .apply_execution(
                &report(
                    opening,
                    "partial-opening",
                    Decimal::new(1, 1),
                    Decimal::new(1014, 1),
                    Some(TerminalOrderStatus::Cancelled),
                ),
                103,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::RiskExitRequested);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-1, 1));
        assert_eq!(
            state
                .lots_by_level
                .values()
                .map(|lot| lot.quantity)
                .sum::<Decimal>(),
            Decimal::new(1, 1)
        );
        assert!(state.ready_intents(104).unwrap().is_empty());
    }

    #[test]
    fn risk_close_is_exact_reduce_only_and_preserves_the_baseline() {
        let mut machine = short_risk_machine_with_opening();
        machine.evaluate_risk_price(decimal(1016), 110).unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();

        let transition = machine
            .prepare_risk_close(Decimal::new(-28, 1), &rules, 111)
            .unwrap();
        let (close_id, close_quantity) = match transition {
            StrategyTransition::RiskCloseOrderReady {
                client_order_id,
                quantity,
            } => (client_order_id, quantity),
            other => panic!("unexpected transition: {other:?}"),
        };
        assert_eq!(close_quantity, Decimal::new(28, 1));
        assert_eq!(
            machine
                .prepare_risk_close(Decimal::new(-28, 1), &rules, 112)
                .unwrap(),
            StrategyTransition::NoChange
        );
        let ready = machine.store().snapshot().ready_intents(113).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].client_order_id, close_id);
        assert_eq!(ready[0].shape.side, OrderSide::Buy);
        assert_eq!(ready[0].shape.quantity, Decimal::new(28, 1));
        assert!(ready[0].shape.reduce_only);
        assert_eq!(ready[0].shape.kind, OrderKind::Market);
        assert_eq!(ready[0].shape.price, None);

        machine
            .apply_execution(
                &report(
                    close_id,
                    "risk-close",
                    Decimal::new(28, 1),
                    decimal(2800),
                    Some(TerminalOrderStatus::Filled),
                ),
                114,
            )
            .unwrap();
        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert!(state.lots_by_level.is_empty());
        assert_eq!(state.gross_realized_profit, Decimal::new(392, 1));

        assert_eq!(
            machine
                .prepare_risk_close(Decimal::ZERO, &rules, 115)
                .unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Closed
            }
        );
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::Closed
        );
        assert!(
            machine
                .store()
                .snapshot()
                .ready_intents(116)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn partial_cancelled_risk_close_prepares_only_the_exact_remainder() {
        let mut machine = short_risk_machine_with_opening();
        machine.evaluate_risk_price(decimal(1016), 110).unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();
        let first = match machine
            .prepare_risk_close(Decimal::new(-28, 1), &rules, 111)
            .unwrap()
        {
            StrategyTransition::RiskCloseOrderReady {
                client_order_id, ..
            } => client_order_id,
            other => panic!("unexpected transition: {other:?}"),
        };
        machine
            .apply_execution(
                &report(
                    first,
                    "risk-close-partial",
                    decimal(1),
                    decimal(1000),
                    Some(TerminalOrderStatus::Cancelled),
                ),
                112,
            )
            .unwrap();

        let transition = machine
            .prepare_risk_close(Decimal::new(-18, 1), &rules, 113)
            .unwrap();
        assert!(matches!(
            transition,
            StrategyTransition::RiskCloseOrderReady { quantity, .. }
                if quantity == Decimal::new(18, 1)
        ));
        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-18, 1));
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| order.purpose.is_risk_close())
                .count(),
            2
        );
        assert!(state.replacement_obligations.is_empty());
    }

    #[test]
    fn risk_close_waits_for_authoritative_terminal_grid_orders() {
        let mut machine = short_risk_machine_with_opening();
        let mut live = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        live.state = IntentState::Accepted {
            exchange_order_id: "still-live".into(),
        };
        machine.synchronize_intent(&live, 111).unwrap();
        machine.evaluate_risk_price(decimal(1016), 112).unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();

        assert!(matches!(
            machine.prepare_risk_close(Decimal::new(-28, 1), &rules, 113),
            Err(StrategyMachineError::InvalidState(
                StrategyStateError::OrdersNotTerminal
            ))
        ));
        assert!(
            machine
                .store()
                .snapshot()
                .orders
                .values()
                .all(|order| !order.purpose.is_risk_close())
        );
    }

    #[test]
    fn full_opening_atomically_activates_exact_protection_without_touching_baseline() {
        let machine = short_machine_with_opening();
        let state = machine.store().snapshot();

        assert_eq!(state.lifecycle, StrategyLifecycle::DeployingGrid);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert_eq!(
            state.expected_exchange_position().unwrap(),
            Decimal::new(-58, 1)
        );
        assert_eq!(state.opening_filled_quantity, Decimal::new(28, 1));
        assert_eq!(state.lots_by_level.len(), 14);
        assert_eq!(
            state
                .lots_by_level
                .values()
                .map(|lot| lot.quantity)
                .sum::<Decimal>(),
            Decimal::new(28, 1)
        );
        assert_eq!(state.ready_intents(102).unwrap().len(), 20);
        assert_eq!(state.baseline.signed_quantity, decimal(-3));
    }

    #[test]
    fn duplicate_opening_snapshot_is_idempotent() {
        let mut machine = short_machine_with_opening();
        let opening = opening_id(machine.store().snapshot());
        let revision = machine.store().snapshot().revision;

        let transition = machine
            .apply_execution(
                &report(
                    opening,
                    "opening-1",
                    Decimal::new(28, 1),
                    Decimal::new(28392, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();

        assert_eq!(transition, StrategyTransition::NoChange);
        assert_eq!(machine.store().snapshot().revision, revision);
        assert_eq!(machine.store().snapshot().lots_by_level.len(), 14);
    }

    #[test]
    fn partial_terminal_opening_creates_one_exact_remainder_before_grid_deployment() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let opening_shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = Decimal::new(11, 1);
        let partial_quote = opening_shape
            .price
            .unwrap()
            .checked_mul(partial_quantity)
            .unwrap();
        let expected_remainder = opening_shape.quantity - partial_quantity;
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        let cancelled = report(
            opening.clone(),
            "opening-partial",
            partial_quantity,
            partial_quote,
            Some(TerminalOrderStatus::Cancelled),
        );

        machine.apply_execution(&cancelled, 101).unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::AwaitingOpening);
        assert_eq!(state.opening_filled_quantity, partial_quantity);
        assert_eq!(state.grid_position_net_quantity, -partial_quantity);
        let remainder_orders = state
            .orders
            .values()
            .filter(|order| {
                order.purpose == StrategyOrderPurpose::Opening
                    && order.tracking == StrategyOrderTracking::Ready
            })
            .collect::<Vec<_>>();
        assert_eq!(remainder_orders.len(), 1);
        assert_ne!(remainder_orders[0].client_order_id, opening);
        assert_eq!(remainder_orders[0].shape.quantity, expected_remainder);
        assert_eq!(remainder_orders[0].shape.price, opening_shape.price);
        let remainder = remainder_orders[0].client_order_id.clone();
        assert_eq!(state.ready_intents(102).unwrap().len(), 1);
        assert!(
            state
                .orders
                .values()
                .filter(|order| order.purpose.is_initial_grid())
                .all(|order| order.tracking == StrategyOrderTracking::Dormant)
        );

        let revision = state.revision;
        assert_eq!(
            machine.apply_execution(&cancelled, 102).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(machine.store().snapshot().revision, revision);
        assert_eq!(
            machine
                .store()
                .snapshot()
                .orders
                .values()
                .filter(|order| {
                    order.purpose == StrategyOrderPurpose::Opening
                        && order.tracking == StrategyOrderTracking::Ready
                })
                .count(),
            1
        );

        let remainder_quote = opening_shape
            .price
            .unwrap()
            .checked_mul(expected_remainder)
            .unwrap();
        machine
            .apply_execution(
                &report(
                    remainder,
                    "opening-remainder",
                    expected_remainder,
                    remainder_quote,
                    Some(TerminalOrderStatus::Filled),
                ),
                103,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::DeployingGrid);
        assert_eq!(state.opening_filled_quantity, opening_shape.quantity);
        assert_eq!(state.grid_position_net_quantity, -opening_shape.quantity);
        assert_eq!(state.ready_intents(104).unwrap().len(), 20);
        assert!(
            state
                .orders
                .values()
                .filter(|order| order.purpose == StrategyOrderPurpose::Opening)
                .all(|order| !order_may_still_be_live(order))
        );
    }

    #[test]
    fn repeated_partial_terminal_opening_chain_recomputes_remainder_from_confirmed_total() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let first = opening_id(&initial);
        let shape = initial.orders.get(&first).unwrap().shape.clone();
        let price = shape.price.unwrap();
        let first_fill = Decimal::new(10, 1);
        let second_fill = Decimal::new(7, 1);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        machine
            .apply_execution(
                &report(
                    first,
                    "opening-partial-1",
                    first_fill,
                    price * first_fill,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();
        let second = machine
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
        assert_eq!(second.shape.quantity, shape.quantity - first_fill);

        machine
            .apply_execution(
                &report(
                    second.client_order_id,
                    "opening-partial-2",
                    second_fill,
                    price * second_fill,
                    Some(TerminalOrderStatus::Expired),
                ),
                102,
            )
            .unwrap();

        let state = machine.store().snapshot();
        let remaining = shape.quantity - first_fill - second_fill;
        assert_eq!(
            state.lifecycle,
            StrategyLifecycle::AwaitingOpening,
            "{:?}",
            state.failure
        );
        assert_eq!(state.opening_filled_quantity, first_fill + second_fill);
        assert_eq!(
            state.grid_position_net_quantity,
            -(first_fill + second_fill)
        );
        let ready = state.ready_intents(103).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].shape.quantity, remaining);
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| {
                    order.purpose == StrategyOrderPurpose::Opening
                        && order.terminal_status.is_none()
                })
                .count(),
            1
        );
    }

    #[test]
    fn partial_opening_remainder_is_atomic_across_state_write_failure() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = Decimal::new(11, 1);
        let cancelled = report(
            opening,
            "opening-partial",
            partial_quantity,
            shape.price.unwrap() * partial_quantity,
            Some(TerminalOrderStatus::Cancelled),
        );
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial.clone()));
        machine.store_mut().fail_next_write();

        assert!(matches!(
            machine.apply_execution(&cancelled, 101),
            Err(StrategyMachineError::Persistence(
                StrategyStoreError::InjectedWriteFailure
            ))
        ));
        assert_eq!(machine.store().snapshot(), &initial);

        machine.apply_execution(&cancelled, 102).unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.opening_filled_quantity, partial_quantity);
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| {
                    order.purpose == StrategyOrderPurpose::Opening
                        && order.terminal_status.is_none()
                })
                .count(),
            1
        );
        assert_eq!(state.ready_intents(103).unwrap().len(), 1);
    }

    #[test]
    fn partial_opening_remainder_round_trips_with_exact_quantity() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = Decimal::new(11, 1);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-partial",
                    partial_quantity,
                    shape.price.unwrap() * partial_quantity,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();
        let expected = machine.store().snapshot().clone();
        let directory = tempdir().unwrap();
        let path = directory.path().join("opening-remainder.json");

        FileStrategyStateStore::create(&path, expected.clone()).unwrap();
        let restored = FileStrategyStateStore::load(&path).unwrap();

        assert_eq!(restored.snapshot(), &expected);
        let ready = restored.snapshot().ready_intents(102).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].shape.quantity, shape.quantity - partial_quantity);
    }

    #[test]
    fn rejected_opening_remainder_preserves_confirmed_owned_position() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = Decimal::new(11, 1);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-partial",
                    partial_quantity,
                    shape.price.unwrap() * partial_quantity,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();
        let mut remainder = machine
            .store()
            .snapshot()
            .ready_intents(102)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        remainder.state = IntentState::Rejected {
            code: Some("MIN_NOTIONAL".into()),
            message: "opening remainder rejected".into(),
        };

        let transition = machine.synchronize_intent(&remainder, 102).unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.opening_filled_quantity, partial_quantity);
        assert_eq!(state.grid_position_net_quantity, -partial_quantity);
        assert_eq!(
            state
                .lots_by_level
                .values()
                .map(|lot| lot.quantity)
                .sum::<Decimal>(),
            partial_quantity
        );
        assert!(
            state
                .orders
                .values()
                .filter(|order| order.purpose.is_initial_grid())
                .all(|order| order.tracking == StrategyOrderTracking::Dormant)
        );
    }

    #[test]
    fn sub_minimum_opening_remainder_fails_closed_without_losing_the_fill() {
        let mut initial = state(Direction::Short, PositionBaseline::flat());
        initial.instrument_rules.limit_quantity.min = Decimal::new(2, 1);
        initial.validate().unwrap();
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = shape.quantity - Decimal::new(1, 1);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        let transition = machine
            .apply_execution(
                &report(
                    opening,
                    "opening-dust",
                    partial_quantity,
                    shape.price.unwrap() * partial_quantity,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.opening_filled_quantity, partial_quantity);
        assert_eq!(state.grid_position_net_quantity, -partial_quantity);
        assert!(
            state
                .failure
                .as_deref()
                .unwrap()
                .contains("cannot be submitted safely")
        );
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| {
                    order.purpose == StrategyOrderPurpose::Opening
                        && order.terminal_status.is_none()
                })
                .count(),
            0
        );
    }

    #[test]
    fn valued_sub_minimum_opening_remainder_keeps_exact_audit_when_failing_closed() {
        let mut initial = state(Direction::Short, PositionBaseline::flat());
        initial.instrument_rules.limit_quantity.min = Decimal::new(2, 1);
        initial.validate().unwrap();
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = shape.quantity - Decimal::new(1, 1);
        let partial_quote = shape.price.unwrap() * partial_quantity;
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        let mut accepted = machine
            .store()
            .snapshot()
            .ready_intents(101)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == opening)
            .unwrap();
        accepted.state = IntentState::Accepted {
            exchange_order_id: "opening-valued-dust".into(),
        };
        machine.synchronize_intent(&accepted, 101).unwrap();

        let trade = TradeFill {
            trade_id: "opening-valued-dust-fill".into(),
            exchange_order_id: "opening-valued-dust".into(),
            symbol: shape.symbol.clone(),
            side: shape.side,
            price: shape.price.unwrap(),
            quantity: partial_quantity,
            quote_quantity: partial_quote,
            raw_commission: Decimal::ZERO,
            commission_cost: Decimal::ZERO,
            commission_asset: "USDT".into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: 102,
        };
        let snapshot = OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: opening.clone(),
                exchange_order_id: "opening-valued-dust".into(),
                exchange: Exchange::Binance,
                shape,
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
                executed_quantity: None,
            },
            cumulative_quantity: partial_quantity,
            cumulative_quote: partial_quote,
            fees_by_asset: [("USDT".into(), Decimal::ZERO)].into_iter().collect(),
            trades: vec![trade.clone()],
            order_time_ms: 101,
            update_time_ms: 102,
        };
        let valued = ValuedExecutionReport {
            report: report(
                opening.clone(),
                "opening-valued-dust",
                partial_quantity,
                partial_quote,
                Some(TerminalOrderStatus::Cancelled),
            ),
            fee_valuations: vec![FeeValuation {
                trade_id: trade.trade_id.clone(),
                fee_asset: "USDT".into(),
                fee_amount: Decimal::ZERO,
                quote_asset: "USDT".into(),
                quote_value: Decimal::ZERO,
                source: FeeValuationSource::ExchangeZero,
                valuation_symbol: None,
                valuation_minute_start_ms: None,
                valuation_price: None,
            }],
        };

        let transition = machine
            .apply_valued_execution(&snapshot, &valued, 103)
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.opening_filled_quantity, partial_quantity);
        assert_eq!(state.grid_position_net_quantity, -partial_quantity);
        let order = state.orders.get(&opening).unwrap();
        assert_eq!(
            order.execution_audit.as_ref().unwrap().snapshot.trades[0].trade_id,
            trade.trade_id
        );
        assert_eq!(
            state
                .inventory_events
                .values()
                .next()
                .unwrap()
                .exchange_trade_id,
            Some("opening-valued-dust-fill".into())
        );
    }

    #[test]
    fn late_exact_audit_never_relabels_a_preexisting_aggregate_inventory_event() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let quantity = shape.quantity;
        let quote = shape.price.unwrap() * quantity;
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        let mut accepted = machine
            .store()
            .snapshot()
            .ready_intents(101)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == opening)
            .unwrap();
        accepted.state = IntentState::Accepted {
            exchange_order_id: "opening-aggregate-first".into(),
        };
        machine.synchronize_intent(&accepted, 101).unwrap();
        machine
            .apply_execution(
                &report(
                    opening.clone(),
                    "opening-aggregate-first",
                    quantity,
                    quote,
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();

        let trade = TradeFill {
            trade_id: "late-exact-opening-fill".into(),
            exchange_order_id: "opening-aggregate-first".into(),
            symbol: shape.symbol.clone(),
            side: shape.side,
            price: shape.price.unwrap(),
            quantity,
            quote_quantity: quote,
            raw_commission: Decimal::ZERO,
            commission_cost: Decimal::ZERO,
            commission_asset: "USDT".into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: 102,
        };
        let snapshot = OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: opening.clone(),
                exchange_order_id: "opening-aggregate-first".into(),
                exchange: Exchange::Binance,
                shape,
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
                executed_quantity: None,
            },
            cumulative_quantity: quantity,
            cumulative_quote: quote,
            fees_by_asset: [("USDT".into(), Decimal::ZERO)].into_iter().collect(),
            trades: vec![trade.clone()],
            order_time_ms: 101,
            update_time_ms: 102,
        };
        let valued = ValuedExecutionReport {
            report: report(
                opening.clone(),
                "opening-aggregate-first",
                quantity,
                quote,
                Some(TerminalOrderStatus::Filled),
            ),
            fee_valuations: vec![FeeValuation {
                trade_id: trade.trade_id,
                fee_asset: "USDT".into(),
                fee_amount: Decimal::ZERO,
                quote_asset: "USDT".into(),
                quote_value: Decimal::ZERO,
                source: FeeValuationSource::ExchangeZero,
                valuation_symbol: None,
                valuation_minute_start_ms: None,
                valuation_price: None,
            }],
        };

        let transition = machine
            .apply_valued_execution(&snapshot, &valued, 103)
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert!(
            state
                .orders
                .get(&opening)
                .unwrap()
                .execution_audit
                .is_none()
        );
        assert_eq!(state.inventory_events.len(), 1);
        assert!(
            state
                .inventory_events
                .values()
                .next()
                .unwrap()
                .exchange_trade_id
                .is_none()
        );
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn every_aligned_partial_opening_split_converges_exactly_for_long_and_short() {
        for direction in [Direction::Long, Direction::Short] {
            let template = state(direction, PositionBaseline::flat());
            let opening = opening_id(&template);
            let shape = template.orders.get(&opening).unwrap().shape.clone();
            let price = shape.price.unwrap();
            let step = template.instrument_rules.limit_quantity.step;
            let mut partial_quantity = step;
            while partial_quantity < shape.quantity {
                let mut machine =
                    StrategyMachine::new(MemoryStrategyStateStore::new(template.clone()));
                machine
                    .apply_execution(
                        &report(
                            opening.clone(),
                            "opening-partial",
                            partial_quantity,
                            price * partial_quantity,
                            Some(TerminalOrderStatus::Cancelled),
                        ),
                        101,
                    )
                    .unwrap();
                let remainder = machine
                    .store()
                    .snapshot()
                    .ready_intents(102)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap();
                assert_eq!(remainder.shape.quantity, shape.quantity - partial_quantity);
                let remainder_quantity = remainder.shape.quantity;
                machine
                    .apply_execution(
                        &report(
                            remainder.client_order_id,
                            "opening-remainder",
                            remainder_quantity,
                            price * remainder_quantity,
                            Some(TerminalOrderStatus::Filled),
                        ),
                        103,
                    )
                    .unwrap();

                let completed = machine.store().snapshot();
                let expected_signed = match shape.side {
                    OrderSide::Buy => shape.quantity,
                    OrderSide::Sell => -shape.quantity,
                };
                assert_eq!(completed.lifecycle, StrategyLifecycle::DeployingGrid);
                assert_eq!(completed.opening_filled_quantity, shape.quantity);
                assert_eq!(completed.grid_position_net_quantity, expected_signed);
                assert_eq!(completed.ready_intents(104).unwrap().len(), 20);
                partial_quantity += step;
            }
        }
    }

    #[test]
    fn partial_terminal_market_opening_retries_only_the_exact_market_remainder() {
        let mut config = config(Direction::Short);
        config.initial_order_type = InitialOrderType::Market;
        config.initial_order_price = None;
        let rules = instrument();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1012),
                mark_price: decimal(1012),
            },
            &rules,
        )
        .unwrap();
        let initial = StrategyState::from_plan(
            StrategyRunId::parse("MARKET01").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = shape.quantity / Decimal::new(2, 0);
        let remainder_quantity = shape.quantity - partial_quantity;
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        machine
            .apply_execution(
                &report(
                    opening,
                    "market-partial",
                    partial_quantity,
                    decimal(1012) * partial_quantity,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();

        let remainder = machine
            .store()
            .snapshot()
            .ready_intents(102)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(remainder.shape.kind, OrderKind::Market);
        assert_eq!(remainder.shape.price, None);
        assert_eq!(remainder.shape.quantity, remainder_quantity);
        machine
            .apply_execution(
                &report(
                    remainder.client_order_id,
                    "market-remainder",
                    remainder_quantity,
                    decimal(1013) * remainder_quantity,
                    Some(TerminalOrderStatus::Filled),
                ),
                103,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::DeployingGrid);
        assert_eq!(state.opening_filled_quantity, shape.quantity);
        assert_eq!(state.grid_position_net_quantity, -shape.quantity);
        assert_eq!(state.ready_intents(104).unwrap().len(), 20);
    }

    #[test]
    fn partial_opening_remainder_never_changes_the_existing_position_baseline() {
        let baseline = PositionBaseline {
            signed_quantity: Decimal::new(-3, 0),
            entry_price: Some(Decimal::new(1015, 0)),
        };
        let initial = state(Direction::Short, baseline.clone());
        let opening = opening_id(&initial);
        let shape = initial.orders.get(&opening).unwrap().shape.clone();
        let partial_quantity = Decimal::new(11, 1);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-partial",
                    partial_quantity,
                    shape.price.unwrap() * partial_quantity,
                    Some(TerminalOrderStatus::Expired),
                ),
                101,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.baseline, baseline);
        assert_eq!(state.grid_position_net_quantity, -partial_quantity);
        assert_eq!(
            state.expected_exchange_position().unwrap(),
            Decimal::new(-41, 1)
        );
        assert_eq!(state.ready_intents(102).unwrap().len(), 1);
    }

    #[test]
    fn opening_ledger_validation_rejects_mismatched_totals_and_two_pending_orders() {
        let mut mismatched = state(Direction::Short, PositionBaseline::flat());
        mismatched.opening_filled_quantity = Decimal::new(1, 1);
        assert_eq!(
            mismatched.validate(),
            Err(StrategyStateError::OpeningAccountingMismatch)
        );

        let mut duplicated = state(Direction::Short, PositionBaseline::flat());
        let original = opening_id(&duplicated);
        let mut duplicate = duplicated.orders.get(&original).unwrap().clone();
        duplicate.client_order_id = ClientOrderId::parse(format!(
            "o_{}_S_{}",
            duplicated.run_id.as_str(),
            duplicated.next_order_sequence
        ))
        .unwrap();
        duplicated
            .orders
            .insert(duplicate.client_order_id.clone(), duplicate);
        duplicated.next_order_sequence += 1;
        assert_eq!(
            duplicated.validate(),
            Err(StrategyStateError::OpeningAccountingMismatch)
        );
    }

    #[test]
    fn every_initial_grid_intent_must_be_represented_before_running() {
        let mut machine = short_machine_with_opening();
        let intents = machine.store().snapshot().ready_intents(102).unwrap();

        for (index, mut intent) in intents.into_iter().enumerate() {
            intent.state = IntentState::Accepted {
                exchange_order_id: format!("grid-{index}"),
            };
            machine
                .synchronize_intent(&intent, 103 + index as u64)
                .unwrap();
        }

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Running);
        assert!(state.initial_deployment_complete);
        assert_eq!(state.orders.len(), 21);
    }

    #[test]
    fn fragmented_open_fill_creates_exact_counter_obligations_without_rounding() {
        let mut machine = short_machine_with_opening();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);

        let first = machine
            .apply_execution(
                &report(
                    add.clone(),
                    "add-14",
                    Decimal::new(1, 1),
                    Decimal::new(1015, 1),
                    None,
                ),
                110,
            )
            .unwrap();
        let second = machine
            .apply_execution(
                &report(
                    add,
                    "add-14",
                    Decimal::new(2, 1),
                    decimal(203),
                    Some(TerminalOrderStatus::Filled),
                ),
                111,
            )
            .unwrap();

        assert_eq!(
            first,
            StrategyTransition::Updated {
                new_obligation_ids: vec![1]
            }
        );
        assert_eq!(
            second,
            StrategyTransition::Updated {
                new_obligation_ids: vec![2]
            }
        );
        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, decimal(-3));
        assert_eq!(
            state.lots_by_level.get(&14).unwrap().quantity,
            Decimal::new(2, 1)
        );
        assert_eq!(state.replacement_obligations.len(), 2);
        assert!(state.replacement_obligations.values().all(|obligation| {
            obligation.shape.side == OrderSide::Buy
                && obligation.shape.price == Some(decimal(1014))
                && obligation.shape.quantity == Decimal::new(1, 1)
                && obligation.shape.reduce_only
        }));
    }

    #[test]
    fn reduce_fill_consumes_exact_level_lot_and_records_realized_profit() {
        let mut machine = short_machine_with_opening();
        let reduce = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);

        machine
            .apply_execution(
                &report(
                    reduce,
                    "reduce-13",
                    Decimal::new(2, 1),
                    Decimal::new(2026, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                110,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-26, 1));
        assert!(!state.lots_by_level.contains_key(&13));
        assert_eq!(state.gross_realized_profit, Decimal::new(2, 1));
        assert_eq!(state.completed_pairs, 1);
        let obligation = state.replacement_obligations.values().next().unwrap();
        assert_eq!(obligation.shape.side, OrderSide::Sell);
        assert_eq!(obligation.shape.price, Some(decimal(1014)));
        assert_eq!(obligation.shape.quantity, Decimal::new(2, 1));
        assert!(!obligation.shape.reduce_only);
    }

    #[test]
    fn persisted_aggregate_accounting_drift_is_rejected() {
        let mut machine = short_machine_with_opening();
        let reduce = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);
        machine
            .apply_execution(
                &report(
                    reduce,
                    "reduce-13",
                    Decimal::new(2, 1),
                    Decimal::new(2026, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                110,
            )
            .unwrap();
        let valid = machine.store().snapshot().clone();

        type StateMutation = Box<dyn Fn(&mut StrategyState)>;
        let mut mutations: Vec<StateMutation> = vec![
            Box::new(|state| state.total_volume += Decimal::ONE),
            Box::new(|state| state.total_fee += Decimal::ONE),
            Box::new(|state| state.gross_realized_profit += Decimal::ONE),
            Box::new(|state| state.completed_pairs += 1),
            Box::new(|state| {
                let lot = state.lots_by_level.values_mut().next().unwrap();
                let extra_quantity = Decimal::new(1, 1);
                let extra_entry_value = lot
                    .entry_value
                    .checked_div(lot.quantity)
                    .unwrap()
                    .checked_mul(extra_quantity)
                    .unwrap();
                lot.quantity += extra_quantity;
                lot.entry_value += extra_entry_value;
                state.grid_position_net_quantity -= extra_quantity;
                state.gross_realized_profit -= extra_entry_value;
            }),
            Box::new(|state| {
                state
                    .orders
                    .values_mut()
                    .find(|order| order.completed_pair_counted)
                    .unwrap()
                    .completed_pair_counted = false;
                state.completed_pairs = 0;
            }),
        ];

        for mutate in mutations.drain(..) {
            let mut drifted = valid.clone();
            mutate(&mut drifted);
            assert_eq!(
                drifted.validate(),
                Err(StrategyStateError::AggregateAccountingMismatch)
            );
        }
    }

    #[test]
    fn persisted_level_lot_redistribution_is_rejected() {
        let machine = short_machine_with_opening();
        let mut drifted = machine.store().snapshot().clone();
        let moved_quantity = Decimal::new(1, 1);
        let moved_entry_value = {
            let source = drifted.lots_by_level.get(&0).unwrap();
            source
                .entry_value
                .checked_mul(moved_quantity)
                .and_then(|value| value.checked_div(source.quantity))
                .unwrap()
        };
        {
            let source = drifted.lots_by_level.get_mut(&0).unwrap();
            source.quantity -= moved_quantity;
            source.entry_value -= moved_entry_value;
        }
        {
            let destination = drifted.lots_by_level.get_mut(&1).unwrap();
            destination.quantity += moved_quantity;
            destination.entry_value += moved_entry_value;
        }

        assert_eq!(
            drifted.validate(),
            Err(StrategyStateError::LevelLotLedgerMismatch)
        );
    }

    #[test]
    fn missing_or_reordered_inventory_events_are_rejected() {
        let mut machine = short_machine_with_opening();
        let reduce = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, true);
        machine
            .apply_execution(
                &report(
                    reduce,
                    "reduce-level-zero",
                    Decimal::new(2, 1),
                    decimal(200),
                    Some(TerminalOrderStatus::Filled),
                ),
                110,
            )
            .unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();
        machine.materialize_replacements(&rules, 111).unwrap();
        let add = replacement_orders(machine.store().snapshot())
            .into_iter()
            .find(|order| {
                order.purpose.level_index() == Some(0)
                    && order.shape.side == OrderSide::Sell
                    && !order.shape.reduce_only
            })
            .unwrap()
            .client_order_id
            .clone();
        machine
            .apply_execution(
                &report(
                    add,
                    "add-level-zero",
                    Decimal::new(2, 1),
                    Decimal::new(2002, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                112,
            )
            .unwrap();
        let valid = machine.store().snapshot().clone();
        assert_eq!(valid.inventory_events.len(), 3);

        let mut missing = valid.clone();
        missing.inventory_events.remove(&2);
        assert_eq!(
            missing.validate(),
            Err(StrategyStateError::InventoryEventLedgerMismatch)
        );

        let mut reordered = valid;
        let mut second = reordered.inventory_events.remove(&2).unwrap();
        let mut third = reordered.inventory_events.remove(&3).unwrap();
        second.sequence = 3;
        third.sequence = 2;
        reordered.inventory_events.insert(2, third);
        reordered.inventory_events.insert(3, second);
        assert_eq!(
            reordered.validate(),
            Err(StrategyStateError::InventoryEventLedgerMismatch)
        );
    }

    #[test]
    fn persisted_neutral_lot_cost_redistribution_is_rejected() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    buy.clone(),
                    "neutral-two-lots",
                    decimal(10),
                    Decimal::new(27, 1),
                    None,
                ),
                101,
            )
            .unwrap();
        machine
            .apply_execution(
                &report(
                    buy,
                    "neutral-two-lots",
                    decimal(20),
                    Decimal::new(55, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();
        let mut drifted = machine.store().snapshot().clone();
        assert_eq!(drifted.neutral_lots.len(), 2);
        drifted.neutral_lots.get_mut(&1).unwrap().entry_value += Decimal::new(1, 1);
        drifted.neutral_lots.get_mut(&2).unwrap().entry_value -= Decimal::new(1, 1);

        assert_eq!(
            drifted.validate(),
            Err(StrategyStateError::NeutralLotLedgerMismatch)
        );
    }

    #[test]
    fn quote_only_execution_delta_fails_without_creating_phantom_volume() {
        let mut machine = short_machine_with_opening();
        let before = machine.store().snapshot().clone();
        let add = grid_id(&before, 14, OrderSide::Sell, false);

        let transition = machine
            .apply_execution(
                &report(add, "quote-only", Decimal::ZERO, Decimal::ONE, None),
                110,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.total_volume, before.total_volume);
        assert_eq!(
            state.grid_position_net_quantity,
            before.grid_position_net_quantity
        );
        assert_eq!(state.inventory_events, before.inventory_events);
    }

    #[test]
    fn directional_inventory_replay_survives_two_hundred_counter_cycles() {
        let mut machine = short_machine_with_opening();
        let rules = machine.store().snapshot().instrument_rules.clone();
        let mut next_order = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, true);
        let mut now_ms = 110_u64;

        for index in 0..200 {
            let shape = machine
                .store()
                .snapshot()
                .orders
                .get(&next_order)
                .unwrap()
                .shape
                .clone();
            let quote = shape.price.unwrap().checked_mul(shape.quantity).unwrap();
            machine
                .apply_execution(
                    &report(
                        next_order,
                        &format!("directional-cycle-{index}"),
                        shape.quantity,
                        quote,
                        Some(TerminalOrderStatus::Filled),
                    ),
                    now_ms,
                )
                .unwrap();
            now_ms += 1;
            let transition = machine.materialize_replacements(&rules, now_ms).unwrap();
            next_order = match transition {
                StrategyTransition::ReplacementOrdersReady { client_order_ids } => {
                    assert_eq!(client_order_ids.len(), 1);
                    client_order_ids.into_iter().next().unwrap()
                }
                other => panic!("unexpected replacement transition: {other:?}"),
            };
            now_ms += 1;
        }

        let state = machine.store().snapshot();
        assert_eq!(state.inventory_events.len(), 201);
        assert_eq!(state.next_inventory_event_sequence, 202);
        assert_eq!(state.completed_pairs, 100);
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn neutral_inventory_replay_survives_two_hundred_counter_cycles() {
        let mut machine = small_neutral_machine();
        let rules = machine.store().snapshot().instrument_rules.clone();
        let mut next_order = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        let mut now_ms = 110_u64;

        for index in 0..200 {
            let shape = machine
                .store()
                .snapshot()
                .orders
                .get(&next_order)
                .unwrap()
                .shape
                .clone();
            let quote = shape.price.unwrap().checked_mul(shape.quantity).unwrap();
            machine
                .apply_execution(
                    &report(
                        next_order,
                        &format!("neutral-cycle-{index}"),
                        shape.quantity,
                        quote,
                        Some(TerminalOrderStatus::Filled),
                    ),
                    now_ms,
                )
                .unwrap();
            now_ms += 1;
            let transition = machine.materialize_replacements(&rules, now_ms).unwrap();
            next_order = match transition {
                StrategyTransition::ReplacementOrdersReady { client_order_ids } => {
                    assert_eq!(client_order_ids.len(), 1);
                    client_order_ids.into_iter().next().unwrap()
                }
                other => panic!("unexpected replacement transition: {other:?}"),
            };
            now_ms += 1;
        }

        let state = machine.store().snapshot();
        assert_eq!(state.inventory_events.len(), 200);
        assert_eq!(state.next_inventory_event_sequence, 201);
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert!(state.neutral_lots.is_empty());
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn partial_cancel_records_fill_counter_and_exact_unfilled_restoration() {
        let mut machine = short_machine_with_opening();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);

        let transition = machine
            .apply_execution(
                &report(
                    add,
                    "add-cancel-14",
                    Decimal::new(1, 1),
                    Decimal::new(1015, 1),
                    Some(TerminalOrderStatus::Cancelled),
                ),
                110,
            )
            .unwrap();

        assert_eq!(
            transition,
            StrategyTransition::Updated {
                new_obligation_ids: vec![1, 2]
            }
        );
        let state = machine.store().snapshot();
        let obligations = state.replacement_obligations.values().collect::<Vec<_>>();
        assert_eq!(obligations.len(), 2);
        assert_eq!(obligations[0].kind, ReplacementObligationKind::Counter);
        assert_eq!(obligations[0].shape.side, OrderSide::Buy);
        assert_eq!(obligations[0].shape.quantity, Decimal::new(1, 1));
        assert_eq!(
            obligations[1].kind,
            ReplacementObligationKind::RestoreCancelledRemainder
        );
        assert_eq!(obligations[1].shape.side, OrderSide::Sell);
        assert_eq!(obligations[1].shape.quantity, Decimal::new(1, 1));
    }

    #[test]
    fn late_terminal_fill_cannot_rewrite_a_processed_cancelled_remainder() {
        let mut machine = short_machine_with_opening();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);
        let late_quantity = Decimal::new(1, 1);

        machine
            .apply_execution(
                &report(
                    add.clone(),
                    "late-cancel-14",
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                110,
            )
            .unwrap();
        let before = machine.store().snapshot().clone();
        let result = machine.apply_execution(
            &report(
                add,
                "late-cancel-14",
                late_quantity,
                Decimal::new(1015, 1),
                Some(TerminalOrderStatus::Cancelled),
            ),
            111,
        );

        assert!(matches!(
            result,
            Err(StrategyMachineError::InvalidState(
                StrategyStateError::ReplacementObligationLedgerMismatch
            ))
        ));
        assert_eq!(machine.store().snapshot(), &before);
    }

    #[test]
    fn duplicate_terminal_snapshot_never_duplicates_obligations_or_pairs() {
        let mut machine = short_machine_with_opening();
        let reduce = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);
        let execution = report(
            reduce,
            "reduce-13",
            Decimal::new(2, 1),
            Decimal::new(2026, 1),
            Some(TerminalOrderStatus::Filled),
        );
        machine.apply_execution(&execution, 110).unwrap();
        let revision = machine.store().snapshot().revision;

        assert_eq!(
            machine.apply_execution(&execution, 111).unwrap(),
            StrategyTransition::NoChange
        );
        let state = machine.store().snapshot();
        assert_eq!(state.revision, revision);
        assert_eq!(state.replacement_obligations.len(), 1);
        assert_eq!(state.completed_pairs, 1);
    }

    #[test]
    fn failed_atomic_write_returns_no_transition_and_preserves_original_state() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine.store_mut().fail_next_write();

        let result = machine.apply_execution(
            &report(
                opening,
                "opening-1",
                Decimal::new(28, 1),
                Decimal::new(28392, 1),
                Some(TerminalOrderStatus::Filled),
            ),
            101,
        );

        assert!(matches!(
            result,
            Err(StrategyMachineError::Persistence(
                StrategyStoreError::InjectedWriteFailure
            ))
        ));
        let state = machine.store().snapshot();
        assert_eq!(state.revision, 0);
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert_eq!(state.lifecycle, StrategyLifecycle::AwaitingOpening);
        assert!(state.lots_by_level.is_empty());
    }

    #[test]
    fn cumulative_execution_regression_is_durably_failed_without_reversing_position() {
        let mut machine = short_machine_with_opening();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);
        machine
            .apply_execution(
                &report(
                    add.clone(),
                    "add-14",
                    Decimal::new(1, 1),
                    Decimal::new(1015, 1),
                    None,
                ),
                110,
            )
            .unwrap();

        let transition = machine
            .apply_execution(
                &report(
                    add,
                    "add-14",
                    Decimal::new(5, 2),
                    Decimal::new(5075, 2),
                    None,
                ),
                111,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-29, 1));
        assert_eq!(
            state
                .orders
                .get(&grid_id(state, 14, OrderSide::Sell, false))
                .unwrap()
                .cumulative_quantity,
            Decimal::new(1, 1)
        );
    }

    #[test]
    fn neutral_fills_use_signed_exchange_quantity_and_non_reduce_counters() {
        let mut config = config(Direction::Neutral);
        config.initial_order_price = None;
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1010),
                mark_price: decimal(1010),
            },
            &instrument(),
        )
        .unwrap();
        let initial = StrategyState::from_plan(
            StrategyRunId::parse("RUN00002").unwrap(),
            config,
            instrument(),
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        let buy = grid_id(&initial, 9, OrderSide::Buy, false);
        let sell = grid_id(&initial, 10, OrderSide::Sell, false);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        machine
            .apply_execution(
                &report(
                    buy,
                    "neutral-buy",
                    Decimal::new(2, 1),
                    Decimal::new(2018, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
            .apply_execution(
                &report(
                    sell,
                    "neutral-sell",
                    Decimal::new(2, 1),
                    Decimal::new(2022, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert!(state.lots_by_level.is_empty());
        assert!(state.neutral_lots.is_empty());
        assert_eq!(state.gross_realized_profit, Decimal::new(4, 1));
        assert_eq!(state.replacement_obligations.len(), 2);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| !obligation.shape.reduce_only)
        );
    }

    #[test]
    fn neutral_fill_that_crosses_zero_closes_then_opens_only_the_remainder() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        let sell = grid_id(machine.store().snapshot(), 1, OrderSide::Sell, false);
        machine
            .apply_execution(
                &report(
                    buy,
                    "neutral-long",
                    decimal(10),
                    Decimal::new(28, 1),
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();
        machine
            .apply_execution(
                &report(
                    sell,
                    "neutral-flip",
                    decimal(20),
                    decimal(6),
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, decimal(-10));
        assert_eq!(state.gross_realized_profit, Decimal::new(2, 1));
        assert_eq!(state.neutral_lots.len(), 1);
        let lot = state.neutral_lots.values().next().unwrap();
        assert_eq!(lot.signed_quantity, decimal(-10));
        assert_eq!(lot.entry_value, decimal(3));
    }

    #[test]
    fn neutral_multi_trade_snapshot_uses_each_execution_price_across_zero() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        accept_strategy_order(&mut machine, &buy, "neutral-existing-long", 101);
        apply_single_valued_trade(
            &mut machine,
            buy,
            ValuedTradeFixture {
                exchange_order_id: "neutral-existing-long",
                trade_id: "neutral-existing-long-trade",
                price: Decimal::new(20, 2),
                quantity: decimal(10),
                trade_time_ms: 102,
                applied_at_ms: 103,
            },
        );

        let sell = grid_id(machine.store().snapshot(), 1, OrderSide::Sell, false);
        let shape = machine
            .store()
            .snapshot()
            .orders
            .get(&sell)
            .unwrap()
            .shape
            .clone();
        let mut accepted = machine
            .store()
            .snapshot()
            .ready_intents(103)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == sell)
            .unwrap();
        accepted.state = IntentState::Accepted {
            exchange_order_id: "neutral-multi-sell".into(),
        };
        machine.synchronize_intent(&accepted, 103).unwrap();

        let trades = vec![
            TradeFill {
                trade_id: "neutral-close".into(),
                exchange_order_id: "neutral-multi-sell".into(),
                symbol: "ANSEMUSDT".into(),
                side: OrderSide::Sell,
                price: Decimal::new(30, 2),
                quantity: decimal(10),
                quote_quantity: decimal(3),
                raw_commission: Decimal::ZERO,
                commission_cost: Decimal::ZERO,
                commission_asset: "USDT".into(),
                realized_profit: Decimal::ONE,
                is_maker: true,
                trade_time_ms: 104,
            },
            TradeFill {
                trade_id: "neutral-open".into(),
                exchange_order_id: "neutral-multi-sell".into(),
                symbol: "ANSEMUSDT".into(),
                side: OrderSide::Sell,
                price: Decimal::new(40, 2),
                quantity: decimal(10),
                quote_quantity: decimal(4),
                raw_commission: Decimal::ZERO,
                commission_cost: Decimal::ZERO,
                commission_asset: "USDT".into(),
                realized_profit: Decimal::ZERO,
                is_maker: true,
                trade_time_ms: 105,
            },
        ];
        let snapshot = OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: sell.clone(),
                exchange_order_id: "neutral-multi-sell".into(),
                exchange: Exchange::Aster,
                shape,
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
                executed_quantity: None,
            },
            cumulative_quantity: decimal(20),
            cumulative_quote: decimal(7),
            fees_by_asset: [("USDT".into(), Decimal::ZERO)].into_iter().collect(),
            trades: trades.clone(),
            order_time_ms: 103,
            update_time_ms: 105,
        };
        let valued = ValuedExecutionReport {
            report: report(
                sell.clone(),
                "neutral-multi-sell",
                decimal(20),
                decimal(7),
                Some(TerminalOrderStatus::Filled),
            ),
            fee_valuations: trades
                .into_iter()
                .map(|trade| FeeValuation {
                    trade_id: trade.trade_id,
                    fee_asset: "USDT".into(),
                    fee_amount: Decimal::ZERO,
                    quote_asset: "USDT".into(),
                    quote_value: Decimal::ZERO,
                    source: FeeValuationSource::ExchangeZero,
                    valuation_symbol: None,
                    valuation_minute_start_ms: None,
                    valuation_price: None,
                })
                .collect(),
        };

        machine
            .apply_valued_execution(&snapshot, &valued, 106)
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, decimal(-10));
        assert_eq!(state.gross_realized_profit, Decimal::ONE);
        assert_eq!(state.neutral_lots.len(), 1);
        let lot = state.neutral_lots.values().next().unwrap();
        assert_eq!(lot.signed_quantity, decimal(-10));
        assert_eq!(lot.entry_value, decimal(4));
        assert_eq!(
            state
                .inventory_events
                .values()
                .filter_map(|event| event.exchange_trade_id.as_deref())
                .collect::<Vec<_>>(),
            vec![
                "neutral-existing-long-trade",
                "neutral-close",
                "neutral-open"
            ]
        );
        let sell_obligations = state
            .replacement_obligations
            .values()
            .filter(|obligation| obligation.source_client_order_id == sell)
            .collect::<Vec<_>>();
        assert_eq!(sell_obligations.len(), 1);
        assert_eq!(sell_obligations[0].shape.quantity, decimal(20));

        let mut redistributed = state.clone();
        redistributed.inventory_events.get_mut(&2).unwrap().quote += Decimal::new(1, 1);
        redistributed.inventory_events.get_mut(&3).unwrap().quote -= Decimal::new(1, 1);
        assert_eq!(
            redistributed.validate(),
            Err(StrategyStateError::InventoryEventLedgerMismatch)
        );

        let mut reordered_audit = state.clone();
        let audit = reordered_audit
            .orders
            .get_mut(&sell)
            .unwrap()
            .execution_audit
            .as_mut()
            .unwrap();
        audit.snapshot.trades.swap(0, 1);
        audit.fee_valuations.swap(0, 1);
        assert_eq!(
            reordered_audit.validate(),
            Err(StrategyStateError::InvalidExecutionAudit)
        );

        let mut conflicting_quantity_audit = state.clone();
        conflicting_quantity_audit
            .orders
            .get_mut(&sell)
            .unwrap()
            .execution_audit
            .as_mut()
            .unwrap()
            .snapshot
            .order
            .executed_quantity = Some(decimal(19));
        assert_eq!(
            conflicting_quantity_audit.validate(),
            Err(StrategyStateError::InvalidExecutionAudit)
        );
    }

    #[test]
    fn neutral_inventory_uses_exchange_time_across_different_orders() {
        let chronological = cross_order_neutral_machine();
        let low_buy = grid_id(chronological.store().snapshot(), 0, OrderSide::Buy, false);
        let high_buy = grid_id(chronological.store().snapshot(), 2, OrderSide::Buy, false);
        let sell = grid_id(chronological.store().snapshot(), 2, OrderSide::Sell, false);

        let exercise = |mut machine: StrategyMachine<MemoryStrategyStateStore>, reversed: bool| {
            accept_strategy_order(&mut machine, &low_buy, "low-buy-order", 101);
            accept_strategy_order(&mut machine, &high_buy, "high-buy-order", 101);
            accept_strategy_order(&mut machine, &sell, "sell-order", 101);
            if reversed {
                apply_single_valued_trade(
                    &mut machine,
                    high_buy.clone(),
                    ValuedTradeFixture {
                        exchange_order_id: "high-buy-order",
                        trade_id: "high-buy-trade",
                        price: Decimal::new(30, 2),
                        quantity: decimal(10),
                        trade_time_ms: 104,
                        applied_at_ms: 106,
                    },
                );
                apply_single_valued_trade(
                    &mut machine,
                    low_buy.clone(),
                    ValuedTradeFixture {
                        exchange_order_id: "low-buy-order",
                        trade_id: "low-buy-trade",
                        price: Decimal::new(20, 2),
                        quantity: decimal(10),
                        trade_time_ms: 103,
                        applied_at_ms: 107,
                    },
                );
            } else {
                apply_single_valued_trade(
                    &mut machine,
                    low_buy.clone(),
                    ValuedTradeFixture {
                        exchange_order_id: "low-buy-order",
                        trade_id: "low-buy-trade",
                        price: Decimal::new(20, 2),
                        quantity: decimal(10),
                        trade_time_ms: 103,
                        applied_at_ms: 106,
                    },
                );
                apply_single_valued_trade(
                    &mut machine,
                    high_buy.clone(),
                    ValuedTradeFixture {
                        exchange_order_id: "high-buy-order",
                        trade_id: "high-buy-trade",
                        price: Decimal::new(30, 2),
                        quantity: decimal(10),
                        trade_time_ms: 104,
                        applied_at_ms: 107,
                    },
                );
            }
            apply_single_valued_trade(
                &mut machine,
                sell.clone(),
                ValuedTradeFixture {
                    exchange_order_id: "sell-order",
                    trade_id: "sell-trade",
                    price: Decimal::new(35, 2),
                    quantity: decimal(10),
                    trade_time_ms: 105,
                    applied_at_ms: 108,
                },
            );
            machine
        };

        let chronological = exercise(chronological, false);
        let reversed = exercise(cross_order_neutral_machine(), true);
        for state in [
            chronological.store().snapshot(),
            reversed.store().snapshot(),
        ] {
            assert_eq!(state.grid_position_net_quantity, decimal(10));
            assert_eq!(state.gross_realized_profit, Decimal::new(15, 1));
            assert_eq!(state.neutral_lots.len(), 1);
            assert_eq!(
                state.neutral_lots.values().next().unwrap().entry_value,
                decimal(3)
            );
        }
        assert_eq!(
            reversed
                .store()
                .snapshot()
                .inventory_events
                .values()
                .filter_map(|event| event.exchange_trade_id.as_deref())
                .collect::<Vec<_>>(),
            vec!["high-buy-trade", "low-buy-trade", "sell-trade"]
        );
        let restored: StrategyState =
            serde_json::from_slice(&serde_json::to_vec(reversed.store().snapshot()).unwrap())
                .unwrap();
        restored.validate().unwrap();
        assert_eq!(restored.gross_realized_profit, Decimal::new(15, 1));
        assert_eq!(
            restored.neutral_lots.values().next().unwrap().entry_value,
            decimal(3)
        );
    }

    #[test]
    fn same_millisecond_numeric_trade_ids_follow_exchange_sequence() {
        let mut machine = cross_order_neutral_machine();
        let low_buy = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, false);
        let high_buy = grid_id(machine.store().snapshot(), 2, OrderSide::Buy, false);
        let sell = grid_id(machine.store().snapshot(), 2, OrderSide::Sell, false);
        accept_strategy_order(&mut machine, &low_buy, "same-ms-low-order", 101);
        accept_strategy_order(&mut machine, &high_buy, "same-ms-high-order", 101);
        accept_strategy_order(&mut machine, &sell, "same-ms-sell-order", 101);

        apply_single_valued_trade(
            &mut machine,
            high_buy,
            ValuedTradeFixture {
                exchange_order_id: "same-ms-high-order",
                trade_id: "10",
                price: Decimal::new(30, 2),
                quantity: decimal(10),
                trade_time_ms: 103,
                applied_at_ms: 106,
            },
        );
        apply_single_valued_trade(
            &mut machine,
            low_buy,
            ValuedTradeFixture {
                exchange_order_id: "same-ms-low-order",
                trade_id: "9",
                price: Decimal::new(20, 2),
                quantity: decimal(10),
                trade_time_ms: 103,
                applied_at_ms: 107,
            },
        );
        apply_single_valued_trade(
            &mut machine,
            sell,
            ValuedTradeFixture {
                exchange_order_id: "same-ms-sell-order",
                trade_id: "11",
                price: Decimal::new(35, 2),
                quantity: decimal(10),
                trade_time_ms: 104,
                applied_at_ms: 108,
            },
        );

        let state = machine.store().snapshot();
        assert_eq!(state.gross_realized_profit, Decimal::new(15, 1));
        assert_eq!(
            state.neutral_lots.values().next().unwrap().entry_value,
            decimal(3)
        );
    }

    #[test]
    fn duplicate_exchange_trade_id_across_opposite_orders_fails_closed() {
        let mut machine = cross_order_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, false);
        let sell = grid_id(machine.store().snapshot(), 2, OrderSide::Sell, false);
        accept_strategy_order(&mut machine, &buy, "duplicate-buy-order", 101);
        accept_strategy_order(&mut machine, &sell, "duplicate-sell-order", 101);

        apply_single_valued_trade(
            &mut machine,
            buy,
            ValuedTradeFixture {
                exchange_order_id: "duplicate-buy-order",
                trade_id: "duplicate-trade-7",
                price: Decimal::new(20, 2),
                quantity: decimal(10),
                trade_time_ms: 103,
                applied_at_ms: 106,
            },
        );
        apply_single_valued_trade(
            &mut machine,
            sell,
            ValuedTradeFixture {
                exchange_order_id: "duplicate-sell-order",
                trade_id: "duplicate-trade-7",
                price: Decimal::new(30, 2),
                quantity: decimal(10),
                trade_time_ms: 104,
                applied_at_ms: 107,
            },
        );

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.grid_position_net_quantity, decimal(10));
        assert!(
            state
                .failure
                .as_deref()
                .is_some_and(|reason| reason.contains("execution audit is invalid"))
        );
    }

    #[test]
    fn persisted_state_rejects_trade_id_owned_by_two_orders() {
        let mut machine = cross_order_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, false);
        let sell = grid_id(machine.store().snapshot(), 2, OrderSide::Sell, false);
        accept_strategy_order(&mut machine, &buy, "persisted-buy-order", 101);
        accept_strategy_order(&mut machine, &sell, "persisted-sell-order", 101);

        apply_single_valued_trade(
            &mut machine,
            buy.clone(),
            ValuedTradeFixture {
                exchange_order_id: "persisted-buy-order",
                trade_id: "persisted-buy-trade",
                price: Decimal::new(20, 2),
                quantity: decimal(10),
                trade_time_ms: 103,
                applied_at_ms: 106,
            },
        );
        apply_single_valued_trade(
            &mut machine,
            sell.clone(),
            ValuedTradeFixture {
                exchange_order_id: "persisted-sell-order",
                trade_id: "persisted-sell-trade",
                price: Decimal::new(30, 2),
                quantity: decimal(10),
                trade_time_ms: 104,
                applied_at_ms: 107,
            },
        );

        let mut corrupted = machine.store().snapshot().clone();
        let duplicate_trade_id = corrupted.orders[&buy]
            .execution_audit
            .as_ref()
            .unwrap()
            .snapshot
            .trades[0]
            .trade_id
            .clone();
        let sell_audit = corrupted
            .orders
            .get_mut(&sell)
            .unwrap()
            .execution_audit
            .as_mut()
            .unwrap();
        sell_audit.snapshot.trades[0].trade_id = duplicate_trade_id.clone();
        sell_audit.fee_valuations[0].trade_id = duplicate_trade_id;

        assert_eq!(
            corrupted.validate(),
            Err(StrategyStateError::InvalidExecutionAudit)
        );
    }

    #[test]
    fn exact_trade_after_legacy_aggregate_inventory_fails_closed() {
        let mut machine = cross_order_neutral_machine();
        let low_buy = grid_id(machine.store().snapshot(), 0, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    low_buy,
                    "legacy-low-order",
                    decimal(10),
                    decimal(2),
                    Some(TerminalOrderStatus::Cancelled),
                ),
                101,
            )
            .unwrap();

        let high_buy = grid_id(machine.store().snapshot(), 2, OrderSide::Buy, false);
        accept_strategy_order(&mut machine, &high_buy, "exact-high-order", 102);
        apply_single_valued_trade(
            &mut machine,
            high_buy.clone(),
            ValuedTradeFixture {
                exchange_order_id: "exact-high-order",
                trade_id: "exact-high-trade",
                price: Decimal::new(30, 2),
                quantity: decimal(10),
                trade_time_ms: 103,
                applied_at_ms: 104,
            },
        );

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert!(
            state
                .failure
                .as_deref()
                .unwrap()
                .contains("cannot be mixed")
        );
        assert_eq!(state.grid_position_net_quantity, decimal(20));
        assert_eq!(state.inventory_events.len(), 2);
        assert!(
            state
                .inventory_events
                .values()
                .any(|event| event.exchange_trade_id.is_none())
        );
        assert!(
            state
                .inventory_events
                .values()
                .any(|event| { event.exchange_trade_id.as_deref() == Some("exact-high-trade") })
        );
        assert!(
            state
                .orders
                .get(&high_buy)
                .and_then(|order| order.execution_audit.as_ref())
                .is_some()
        );
        state.validate().unwrap();
    }

    #[test]
    fn neutral_risk_close_uses_its_cost_basis_and_closes_exact_net_quantity() {
        let mut machine = small_neutral_risk_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    buy,
                    "neutral-risk-long",
                    decimal(20),
                    Decimal::new(56, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
            .evaluate_risk_price(Decimal::new(31, 2), 102)
            .unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();
        let close_id = match machine
            .prepare_risk_close(decimal(20), &rules, 103)
            .unwrap()
        {
            StrategyTransition::RiskCloseOrderReady {
                client_order_id,
                quantity,
            } => {
                assert_eq!(quantity, decimal(20));
                client_order_id
            }
            other => panic!("unexpected transition: {other:?}"),
        };
        let intent = machine
            .store()
            .snapshot()
            .ready_intents(104)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == close_id)
            .unwrap();
        assert_eq!(intent.shape.side, OrderSide::Sell);
        assert!(intent.shape.reduce_only);
        machine
            .apply_execution(
                &report(
                    close_id,
                    "neutral-risk-close",
                    decimal(20),
                    Decimal::new(62, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                105,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert!(state.neutral_lots.is_empty());
        assert_eq!(state.gross_realized_profit, Decimal::new(6, 1));
        assert_eq!(
            machine
                .prepare_risk_close(Decimal::ZERO, &rules, 106)
                .unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Closed
            }
        );
    }

    #[test]
    fn neutral_inventory_invariants_hold_across_one_thousand_deterministic_fills() {
        let machine = small_neutral_machine();
        let mut state = machine.store().snapshot().clone();
        let mut seed = 0x5eed_u64;
        let mut realized = Decimal::ZERO;

        for _ in 0..1_000 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let side = if seed & 1 == 0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let quantity = Decimal::from((seed % 20) + 1);
            let price = Decimal::new(i64::try_from((seed % 7) + 26).unwrap(), 2);
            let value = price.checked_mul(quantity).unwrap();
            let signed = match side {
                OrderSide::Buy => quantity,
                OrderSide::Sell => -quantity,
            };
            state.grid_position_net_quantity = state
                .grid_position_net_quantity
                .checked_add(signed)
                .unwrap();
            realized = realized
                .checked_add(apply_neutral_fill(&mut state, side, quantity, value).unwrap())
                .unwrap();

            assert_eq!(
                state
                    .neutral_lots
                    .values()
                    .map(|lot| lot.signed_quantity)
                    .sum::<Decimal>(),
                state.grid_position_net_quantity
            );
            assert!(state.neutral_lots.values().all(|lot| {
                (state.grid_position_net_quantity > Decimal::ZERO
                    && lot.signed_quantity > Decimal::ZERO)
                    || (state.grid_position_net_quantity < Decimal::ZERO
                        && lot.signed_quantity < Decimal::ZERO)
            }));
        }
        assert!(!realized.is_zero());
    }

    #[test]
    fn corrupted_lot_coverage_is_rejected_on_load_or_write() {
        let machine = short_machine_with_opening();
        let mut corrupted = machine.store().snapshot().clone();
        corrupted.lots_by_level.remove(&0);
        assert_eq!(
            corrupted.validate(),
            Err(StrategyStateError::LevelLotCoverageMismatch)
        );
    }

    #[test]
    fn corrupted_initial_grid_ledger_is_rejected_before_restart() {
        let original = state(Direction::Short, PositionBaseline::flat());
        let initial_id = original
            .orders
            .values()
            .find(|order| order.purpose.is_initial_grid())
            .unwrap()
            .client_order_id
            .clone();
        let step = original.instrument_rules.limit_quantity.step;
        let tick = original.instrument_rules.tick_size;
        let mut corruptions = Vec::new();

        let mut quantity = original.clone();
        quantity.orders.get_mut(&initial_id).unwrap().shape.quantity += step;
        corruptions.push(("quantity", quantity));

        let mut price = original.clone();
        *price
            .orders
            .get_mut(&initial_id)
            .unwrap()
            .shape
            .price
            .as_mut()
            .unwrap() += tick;
        corruptions.push(("price", price));

        let mut side = original.clone();
        let shape = &mut side.orders.get_mut(&initial_id).unwrap().shape;
        shape.side = match shape.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        corruptions.push(("side", side));

        let mut reduce_only = original.clone();
        let shape = &mut reduce_only.orders.get_mut(&initial_id).unwrap().shape;
        shape.reduce_only = !shape.reduce_only;
        corruptions.push(("reduce-only", reduce_only));

        let mut level = original.clone();
        if let StrategyOrderPurpose::InitialGrid { level_index, .. } =
            &mut level.orders.get_mut(&initial_id).unwrap().purpose
        {
            *level_index += 1;
        }
        corruptions.push(("level", level));

        let mut role = original.clone();
        if let StrategyOrderPurpose::InitialGrid { role, .. } =
            &mut role.orders.get_mut(&initial_id).unwrap().purpose
        {
            *role = match *role {
                GridOrderRole::Profit => GridOrderRole::Add,
                GridOrderRole::Add => GridOrderRole::Profit,
            };
        }
        corruptions.push(("role", role));

        let mut missing = original.clone();
        missing.orders.remove(&initial_id);
        corruptions.push(("missing", missing));

        let mut duplicate = original.clone();
        let mut duplicate_order = duplicate.orders.get(&initial_id).unwrap().clone();
        duplicate_order.client_order_id = ClientOrderId::parse("g_RUN00001_99_S_999").unwrap();
        duplicate
            .orders
            .insert(duplicate_order.client_order_id.clone(), duplicate_order);
        corruptions.push(("duplicate", duplicate));

        for (label, corrupted) in corruptions {
            assert_eq!(
                corrupted.validate(),
                Err(StrategyStateError::InitialGridOrderMismatch),
                "{label} drift must fail closed"
            );
        }
    }

    #[test]
    fn coordinated_plan_and_initial_ledger_drift_is_rejected_before_restart() {
        let original = state(Direction::Short, PositionBaseline::flat());
        let quantity_step = original.instrument_rules.limit_quantity.step;
        let mut coordinated_quantity = original.clone();
        let planned = coordinated_quantity
            .plan
            .grid_orders
            .iter_mut()
            .find(|order| order.role == GridOrderRole::Add)
            .unwrap();
        planned.quantity += quantity_step;
        let changed_level = planned.level_index;
        coordinated_quantity
            .orders
            .values_mut()
            .find(|order| {
                matches!(
                    order.purpose,
                    StrategyOrderPurpose::InitialGrid {
                        level_index,
                        role: GridOrderRole::Add,
                    } if level_index == changed_level
                )
            })
            .unwrap()
            .shape
            .quantity += quantity_step;

        let mut raw_levels = original.clone();
        raw_levels.plan.raw_levels[1] += Decimal::new(1, 1);

        let mut normalized_levels = original.clone();
        normalized_levels.plan.levels[1] += normalized_levels.instrument_rules.tick_size;

        let mut active_count = original.clone();
        active_count.plan.active_grid_count += 1;

        let mut participating_count = original.clone();
        participating_count.plan.participating_level_count -= 1;

        let mut total_quantity = original.clone();
        total_quantity.plan.total_quantity += quantity_step;

        let mut reference_price = original;
        reference_price.plan.reference_price += reference_price.instrument_rules.tick_size;

        for (label, corrupted) in [
            ("coordinated quantity", coordinated_quantity),
            ("raw levels", raw_levels),
            ("normalized levels", normalized_levels),
            ("active count", active_count),
            ("participating count", participating_count),
            ("total quantity", total_quantity),
            ("reference price", reference_price),
        ] {
            assert_eq!(
                corrupted.validate(),
                Err(StrategyStateError::InvalidPlan(
                    GridPlanError::PlanSnapshotMismatch
                )),
                "{label} drift must fail closed"
            );
        }
    }

    #[test]
    fn corrupted_initial_deployment_flag_is_rejected_before_restart() {
        let mut running_machine = StrategyMachine::new(MemoryStrategyStateStore::new(state(
            Direction::Short,
            PositionBaseline::flat(),
        )));
        complete_initial_deployment(&mut running_machine, 200);
        let mut running = running_machine.store().snapshot().clone();
        running.initial_deployment_complete = false;
        assert_eq!(
            running.validate(),
            Err(StrategyStateError::InitialDeploymentStateMismatch)
        );

        let mut deploying = short_machine_with_opening().store().snapshot().clone();
        assert_eq!(deploying.lifecycle, StrategyLifecycle::DeployingGrid);
        deploying.initial_deployment_complete = true;
        assert_eq!(
            deploying.validate(),
            Err(StrategyStateError::InitialDeploymentStateMismatch)
        );
    }

    #[test]
    fn corrupted_append_only_sequences_are_rejected_before_restart() {
        let original = state(Direction::Short, PositionBaseline::flat());

        let mut order_sequence = original.clone();
        order_sequence.next_order_sequence = 1;
        assert_eq!(
            order_sequence.validate(),
            Err(StrategyStateError::OrderSequenceMismatch)
        );

        let mut obligation_sequence = original;
        obligation_sequence.next_obligation_sequence = 2;
        assert_eq!(
            obligation_sequence.validate(),
            Err(StrategyStateError::ObligationSequenceMismatch)
        );
    }

    #[test]
    fn every_generated_order_identity_component_and_sequence_is_validated() {
        let original = state(Direction::Short, PositionBaseline::flat());
        let victim = original
            .orders
            .values()
            .find(|order| order.purpose.is_initial_grid())
            .unwrap();
        let victim_id = victim.client_order_id.clone();
        let level_index = victim.purpose.level_index().unwrap();
        let side = match victim.shape.side {
            OrderSide::Buy => "B",
            OrderSide::Sell => "S",
        };
        let opposite_side = if side == "B" { "S" } else { "B" };
        let sequence = victim_id.as_str().rsplit_once('_').unwrap().1;
        let run_id = original.run_id.as_str().to_owned();
        let malformed_identities = [
            (
                "purpose prefix",
                format!("r_{run_id}_{level_index}_{side}_{sequence}"),
            ),
            (
                "run identity",
                format!("g_OTHER001_{level_index}_{side}_{sequence}"),
            ),
            (
                "level identity",
                format!("g_{run_id}_999_{side}_{sequence}"),
            ),
            (
                "side identity",
                format!("g_{run_id}_{level_index}_{opposite_side}_{sequence}"),
            ),
            (
                "canonical sequence",
                format!("g_{run_id}_{level_index}_{side}_0{sequence}"),
            ),
            (
                "numeric sequence",
                format!("g_{run_id}_{level_index}_{side}_notanumber"),
            ),
        ];

        for (label, identity) in malformed_identities {
            let mut corrupted = original.clone();
            rekey_order(
                &mut corrupted,
                &victim_id,
                ClientOrderId::parse(identity).unwrap(),
            );
            assert_eq!(
                corrupted.validate(),
                Err(StrategyStateError::OrderSequenceMismatch),
                "{label} drift must fail closed",
            );
        }

        let second = original
            .orders
            .values()
            .find(|order| {
                order.purpose.is_initial_grid()
                    && order.client_order_id != victim.client_order_id
                    && order.purpose.level_index() != Some(level_index)
            })
            .unwrap();
        let second_id = second.client_order_id.clone();
        let second_level = second.purpose.level_index().unwrap();
        let second_side = match second.shape.side {
            OrderSide::Buy => "B",
            OrderSide::Sell => "S",
        };
        let mut duplicated_sequence = original;
        rekey_order(
            &mut duplicated_sequence,
            &second_id,
            ClientOrderId::parse(format!(
                "g_{run_id}_{second_level}_{second_side}_{sequence}"
            ))
            .unwrap(),
        );
        assert_eq!(
            duplicated_sequence.validate(),
            Err(StrategyStateError::OrderSequenceMismatch),
            "two distinct orders must never share one append-only sequence",
        );
    }

    #[test]
    fn coordinated_order_identity_drift_that_can_block_a_future_replacement_is_rejected() {
        let mut corrupted = state(Direction::Short, PositionBaseline::flat());
        let source_client_order_id = grid_id(&corrupted, 13, OrderSide::Buy, true);
        let source = corrupted
            .orders
            .get(&source_client_order_id)
            .unwrap()
            .clone();
        let counter = counter_shape(&corrupted, 13, &source.shape, source.shape.quantity).unwrap();
        let side = match counter.side {
            OrderSide::Buy => "B",
            OrderSide::Sell => "S",
        };
        let future_replacement_id = ClientOrderId::parse(format!(
            "r_{}_13_{side}_{}",
            corrupted.run_id.as_str(),
            corrupted.next_order_sequence
        ))
        .unwrap();
        let victim_client_order_id = corrupted
            .orders
            .values()
            .find(|order| {
                order.purpose.is_initial_grid() && order.client_order_id != source_client_order_id
            })
            .unwrap()
            .client_order_id
            .clone();
        let mut victim = corrupted.orders.remove(&victim_client_order_id).unwrap();
        victim.client_order_id = future_replacement_id.clone();
        corrupted.orders.insert(future_replacement_id, victim);

        assert_eq!(
            corrupted.validate(),
            Err(StrategyStateError::OrderSequenceMismatch),
            "persisted IDs must not be able to reserve the next replacement identity",
        );
    }

    #[test]
    fn fabricated_replacement_obligation_is_rejected_before_restart() {
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state(
            Direction::Short,
            PositionBaseline::flat(),
        )));
        complete_initial_deployment(&mut machine, 200);
        let mut corrupted = machine.store().snapshot().clone();
        let source_client_order_id = grid_id(&corrupted, 13, OrderSide::Buy, true);
        let source = corrupted
            .orders
            .get(&source_client_order_id)
            .unwrap()
            .clone();
        assert_eq!(source.cumulative_quantity, Decimal::ZERO);

        let id = corrupted.next_obligation_sequence;
        corrupted.next_obligation_sequence += 1;
        corrupted.replacement_obligations.insert(
            id,
            ReplacementObligation {
                id,
                kind: ReplacementObligationKind::Counter,
                source_client_order_id,
                level_index: 13,
                shape: counter_shape(&corrupted, 13, &source.shape, source.shape.quantity).unwrap(),
                created_at_ms: corrupted.updated_at_ms,
                assigned_client_order_id: None,
            },
        );

        assert_eq!(
            corrupted.validate(),
            Err(StrategyStateError::ReplacementObligationLedgerMismatch),
            "a zero-fill source must not authorize an extra replacement order",
        );
    }

    #[test]
    fn missing_or_drifted_replacement_evidence_is_rejected_before_restart() {
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state(
            Direction::Short,
            PositionBaseline::flat(),
        )));
        complete_initial_deployment(&mut machine, 200);
        let source_client_order_id = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);
        let source = machine
            .store()
            .snapshot()
            .orders
            .get(&source_client_order_id)
            .unwrap()
            .clone();
        let filled = Decimal::new(1, 1);
        machine
            .apply_execution(
                &report(
                    source_client_order_id.clone(),
                    source.exchange_order_id.as_deref().unwrap(),
                    filled,
                    source.shape.price.unwrap() * filled,
                    None,
                ),
                300,
            )
            .unwrap();
        let valid = machine.store().snapshot().clone();
        assert_eq!(valid.validate(), Ok(()));
        assert_eq!(valid.replacement_obligations.len(), 1);

        let mut missing = valid.clone();
        missing.replacement_obligations.clear();
        missing.next_obligation_sequence = 1;

        let mut unknown_source = valid.clone();
        unknown_source
            .replacement_obligations
            .get_mut(&1)
            .unwrap()
            .source_client_order_id = ClientOrderId::parse("g_RUN00001_99_B_999").unwrap();

        let mut wrong_level = valid.clone();
        wrong_level
            .replacement_obligations
            .get_mut(&1)
            .unwrap()
            .level_index = 12;

        let mut over_allocated = valid.clone();
        let oversized = Decimal::new(2, 1);
        over_allocated
            .replacement_obligations
            .get_mut(&1)
            .unwrap()
            .shape = counter_shape(&over_allocated, 13, &source.shape, oversized).unwrap();

        let mut future_timestamp = valid.clone();
        future_timestamp
            .replacement_obligations
            .get_mut(&1)
            .unwrap()
            .created_at_ms = future_timestamp.updated_at_ms + 1;

        for (label, corrupted) in [
            ("missing", missing),
            ("unknown source", unknown_source),
            ("wrong level", wrong_level),
            ("over-allocated", over_allocated),
            ("future timestamp", future_timestamp),
        ] {
            assert_eq!(
                corrupted.validate(),
                Err(StrategyStateError::ReplacementObligationLedgerMismatch),
                "{label} replacement evidence must fail closed"
            );
        }

        let mut non_contiguous = valid;
        let mut obligation = non_contiguous.replacement_obligations.remove(&1).unwrap();
        obligation.id = 2;
        non_contiguous.replacement_obligations.insert(2, obligation);
        assert_eq!(
            non_contiguous.validate(),
            Err(StrategyStateError::ObligationSequenceMismatch)
        );
    }

    #[test]
    fn sub_minimum_non_reduce_obligations_coalesce_to_exact_submit_safe_quantity() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);

        machine
            .apply_execution(
                &report(
                    buy.clone(),
                    "ansem-buy",
                    decimal(8),
                    Decimal::new(224, 2),
                    None,
                ),
                101,
            )
            .unwrap();
        assert_eq!(
            machine
                .materialize_replacements(
                    &machine.store().snapshot().instrument_rules.clone(),
                    102,
                )
                .unwrap(),
            StrategyTransition::NoChange
        );

        machine
            .apply_execution(
                &report(buy, "ansem-buy", decimal(17), Decimal::new(476, 2), None),
                103,
            )
            .unwrap();
        let transition = machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 104)
            .unwrap();

        assert!(matches!(
            transition,
            StrategyTransition::ReplacementOrdersReady { ref client_order_ids }
                if client_order_ids.len() == 1
        ));
        let state = machine.store().snapshot();
        let replacements = replacement_orders(state);
        assert_eq!(replacements.len(), 1);
        assert_eq!(replacements[0].shape.side, OrderSide::Sell);
        assert_eq!(replacements[0].shape.price, Some(Decimal::new(30, 2)));
        assert_eq!(replacements[0].shape.quantity, decimal(17));
        assert!(!replacements[0].shape.reduce_only);
        assert_eq!(
            state
                .replacement_obligations
                .values()
                .map(|obligation| obligation.shape.quantity)
                .sum::<Decimal>(),
            decimal(17)
        );
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_some())
        );
    }

    fn machine_with_small_and_submit_safe_sell_obligations(
        maximum_quantity: Decimal,
    ) -> StrategyMachine<MemoryStrategyStateStore> {
        let mut machine = small_neutral_machine_with_max(Some(maximum_quantity));
        let initial_sell = grid_id(machine.store().snapshot(), 1, OrderSide::Sell, false);
        machine
            .apply_execution(
                &report(
                    initial_sell,
                    "initial-sell",
                    decimal(20),
                    decimal(6),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 102)
            .unwrap();
        let replacement_buy = replacement_orders(machine.store().snapshot())
            .into_iter()
            .find(|order| order.shape.side == OrderSide::Buy)
            .unwrap()
            .client_order_id
            .clone();
        accept_strategy_order(&mut machine, &replacement_buy, "replacement-buy", 103);

        let initial_buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    initial_buy,
                    "initial-buy",
                    decimal(8),
                    Decimal::new(224, 2),
                    None,
                ),
                104,
            )
            .unwrap();
        assert_eq!(
            machine
                .materialize_replacements(
                    &machine.store().snapshot().instrument_rules.clone(),
                    105,
                )
                .unwrap(),
            StrategyTransition::NoChange
        );

        machine
            .apply_execution(
                &report(
                    replacement_buy,
                    "replacement-buy",
                    decimal(17),
                    Decimal::new(476, 2),
                    None,
                ),
                106,
            )
            .unwrap();
        machine
    }

    #[test]
    fn sub_minimum_residual_cannot_block_a_later_submit_safe_obligation() {
        let mut machine = machine_with_small_and_submit_safe_sell_obligations(decimal(20));
        let transition = machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 107)
            .unwrap();

        assert!(matches!(
            transition,
            StrategyTransition::ReplacementOrdersReady { ref client_order_ids }
                if client_order_ids.len() == 1
        ));
        let state = machine.store().snapshot();
        let new_sell = replacement_orders(state)
            .into_iter()
            .filter(|order| {
                order.shape.side == OrderSide::Sell && order.shape.quantity == decimal(17)
            })
            .collect::<Vec<_>>();
        assert_eq!(new_sell.len(), 1);
        let unassigned = state
            .replacement_obligations
            .values()
            .filter(|obligation| obligation.assigned_client_order_id.is_none())
            .collect::<Vec<_>>();
        assert_eq!(unassigned.len(), 1);
        assert_eq!(unassigned[0].shape.quantity, decimal(8));
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn sub_minimum_residual_combines_when_the_result_stays_within_maximum() {
        let mut machine = machine_with_small_and_submit_safe_sell_obligations(decimal(30));

        let transition = machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 107)
            .unwrap();

        assert!(matches!(
            transition,
            StrategyTransition::ReplacementOrdersReady { ref client_order_ids }
                if client_order_ids.len() == 1
        ));
        let state = machine.store().snapshot();
        let combined = replacement_orders(state)
            .into_iter()
            .filter(|order| {
                order.shape.side == OrderSide::Sell && order.shape.quantity == decimal(25)
            })
            .collect::<Vec<_>>();
        assert_eq!(combined.len(), 1);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_some())
        );
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn later_sub_minimum_residual_combines_with_an_earlier_submit_safe_obligation() {
        let mut machine = small_neutral_machine_with_max(Some(decimal(30)));
        let initial_sell = grid_id(machine.store().snapshot(), 1, OrderSide::Sell, false);
        machine
            .apply_execution(
                &report(
                    initial_sell,
                    "initial-sell",
                    decimal(20),
                    decimal(6),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 102)
            .unwrap();
        let replacement_buy = replacement_orders(machine.store().snapshot())
            .into_iter()
            .find(|order| order.shape.side == OrderSide::Buy)
            .unwrap()
            .client_order_id
            .clone();
        accept_strategy_order(&mut machine, &replacement_buy, "replacement-buy", 103);

        machine
            .apply_execution(
                &report(
                    replacement_buy,
                    "replacement-buy",
                    decimal(17),
                    Decimal::new(476, 2),
                    None,
                ),
                104,
            )
            .unwrap();
        let initial_buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    initial_buy,
                    "initial-buy",
                    decimal(8),
                    Decimal::new(224, 2),
                    None,
                ),
                105,
            )
            .unwrap();

        let transition = machine
            .materialize_replacements(&machine.store().snapshot().instrument_rules.clone(), 106)
            .unwrap();

        assert!(matches!(
            transition,
            StrategyTransition::ReplacementOrdersReady { ref client_order_ids }
                if client_order_ids.len() == 1
        ));
        let state = machine.store().snapshot();
        let combined = replacement_orders(state)
            .into_iter()
            .filter(|order| {
                order.shape.side == OrderSide::Sell && order.shape.quantity == decimal(25)
            })
            .collect::<Vec<_>>();
        assert_eq!(combined.len(), 1);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_some())
        );
        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn exact_partition_avoids_an_order_dependent_residual_above_exchange_maximum() {
        let machine = small_neutral_machine_with_max(Some(decimal(25)));
        let mut state = machine.store().snapshot().clone();
        let source_client_order_id = grid_id(&state, 1, OrderSide::Buy, false);
        let source_shape = state
            .orders
            .get(&source_client_order_id)
            .unwrap()
            .shape
            .clone();
        let mut obligation_ids = Vec::new();
        for quantity in [decimal(8), decimal(17), decimal(9)] {
            let shape = counter_shape(&state, 1, &source_shape, quantity).unwrap();
            obligation_ids.push(
                add_obligation(
                    &mut state,
                    ReplacementObligationKind::Counter,
                    source_client_order_id.clone(),
                    1,
                    shape,
                    100,
                )
                .unwrap(),
            );
        }

        let buckets = plan_non_reduce_obligation_buckets(
            &state,
            obligation_ids.clone(),
            &state.instrument_rules,
        )
        .unwrap();

        let mut quantities = buckets
            .iter()
            .map(|bucket| combined_obligation_shape(&state, bucket).unwrap().quantity)
            .collect::<Vec<_>>();
        quantities.sort();
        assert_eq!(quantities, vec![decimal(17), decimal(17)]);
        let mut assigned_ids = buckets.into_iter().flatten().collect::<Vec<_>>();
        assigned_ids.sort_unstable();
        assert_eq!(assigned_ids, obligation_ids);
    }

    #[test]
    fn exact_partition_matches_brute_force_for_small_quantity_sequences() {
        fn brute_force_best(
            quantities: &[Decimal],
            index: usize,
            buckets: &mut Vec<Decimal>,
            best: &mut Decimal,
        ) {
            if index == quantities.len() {
                let assigned = buckets
                    .iter()
                    .filter(|quantity| **quantity >= decimal(2) && **quantity <= decimal(3))
                    .copied()
                    .sum::<Decimal>();
                *best = (*best).max(assigned);
                return;
            }
            let quantity = quantities[index];
            let mut attempted = BTreeSet::new();
            for bucket_index in 0..buckets.len() {
                let original = buckets[bucket_index];
                if !attempted.insert(original) || original + quantity > decimal(3) {
                    continue;
                }
                buckets[bucket_index] += quantity;
                brute_force_best(quantities, index + 1, buckets, best);
                buckets[bucket_index] = original;
            }
            buckets.push(quantity);
            brute_force_best(quantities, index + 1, buckets, best);
            buckets.pop();
        }

        let machine = small_neutral_machine();
        let mut state = machine.store().snapshot().clone();
        let source_client_order_id = grid_id(&state, 1, OrderSide::Buy, false);
        let symbol = state.symbol.clone();
        let rules = InstrumentRules {
            tick_size: Decimal::new(1, 2),
            limit_quantity: QuantityRules {
                step: Decimal::ONE,
                min: decimal(2),
                max: Some(decimal(3)),
            },
            market_quantity: QuantityRules {
                step: Decimal::ONE,
                min: decimal(2),
                max: Some(decimal(3)),
            },
            min_notional: Decimal::ZERO,
        };

        for length in 1..=5 {
            for encoded in 0..3_usize.pow(length) {
                let mut value = encoded;
                let mut quantities = Vec::with_capacity(length as usize);
                for _ in 0..length {
                    quantities.push(decimal((value % 3 + 1) as i64));
                    value /= 3;
                }
                state.replacement_obligations.clear();
                state.next_obligation_sequence = 1;
                let mut obligation_ids = Vec::new();
                for quantity in &quantities {
                    obligation_ids.push(
                        add_obligation(
                            &mut state,
                            ReplacementObligationKind::Counter,
                            source_client_order_id.clone(),
                            1,
                            OrderShape {
                                symbol: symbol.clone(),
                                side: OrderSide::Sell,
                                price: Some(Decimal::new(30, 2)),
                                quantity: *quantity,
                                reduce_only: false,
                                kind: OrderKind::Limit,
                                time_in_force: crate::domain::TimeInForce::Gtc,
                            },
                            100,
                        )
                        .unwrap(),
                    );
                }

                let planned =
                    plan_non_reduce_obligation_buckets(&state, obligation_ids, &rules).unwrap();
                let assigned = planned
                    .iter()
                    .map(|bucket| combined_obligation_shape(&state, bucket).unwrap().quantity)
                    .sum::<Decimal>();
                let mut expected = Decimal::ZERO;
                brute_force_best(&quantities, 0, &mut Vec::new(), &mut expected);
                assert_eq!(assigned, expected, "quantities={quantities:?}");
            }
        }
    }

    #[test]
    fn changed_exchange_rules_fail_closed_before_replacement_materialization() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    buy,
                    "ansem-buy",
                    decimal(20),
                    Decimal::new(560, 2),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        let mut changed = machine.store().snapshot().instrument_rules.clone();
        changed.tick_size = Decimal::new(2, 2);

        let transition = machine.materialize_replacements(&changed, 102).unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert!(replacement_orders(state).is_empty());
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );
    }

    #[test]
    fn materialization_write_failure_leaves_every_obligation_unassigned() {
        let mut machine = small_neutral_machine();
        let buy = grid_id(machine.store().snapshot(), 1, OrderSide::Buy, false);
        machine
            .apply_execution(
                &report(
                    buy,
                    "ansem-buy",
                    decimal(20),
                    Decimal::new(560, 2),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        let order_count = machine.store().snapshot().orders.len();
        let rules = machine.store().snapshot().instrument_rules.clone();
        machine.store_mut().fail_next_write();

        let result = machine.materialize_replacements(&rules, 102);

        assert!(matches!(
            result,
            Err(StrategyMachineError::Persistence(
                StrategyStoreError::InjectedWriteFailure
            ))
        ));
        let state = machine.store().snapshot();
        assert_eq!(state.orders.len(), order_count);
        assert!(replacement_orders(state).is_empty());
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );
    }

    #[test]
    fn reduce_only_partial_quantity_below_minimum_fails_closed_without_an_order() {
        let mut config = config(Direction::Short);
        config.grid_order_qty = Some(decimal(2));
        let mut instrument = instrument();
        instrument.limit_quantity.min = decimal(1);
        instrument.market_quantity.min = decimal(1);
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: decimal(1012),
                mark_price: decimal(1012),
            },
            &instrument,
        )
        .unwrap();
        let state = StrategyState::from_plan(
            StrategyRunId::parse("REDUCE01").unwrap(),
            config,
            instrument.clone(),
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        let opening = opening_id(&state);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state));
        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-large",
                    decimal(28),
                    decimal(28392),
                    Some(TerminalOrderStatus::Filled),
                ),
                101,
            )
            .unwrap();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);
        machine
            .apply_execution(
                &report(
                    add,
                    "add-small",
                    Decimal::new(1, 1),
                    Decimal::new(1015, 1),
                    None,
                ),
                102,
            )
            .unwrap();

        let transition = machine.materialize_replacements(&instrument, 103).unwrap();

        let state = machine.store().snapshot();
        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert!(replacement_orders(state).is_empty());
        assert_eq!(state.replacement_obligations.len(), 1);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );
    }

    #[test]
    fn cancelled_initial_order_requires_accepted_replacement_before_running() {
        let mut machine = short_machine_with_opening();
        let cancelled = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);
        machine
            .apply_execution(
                &report(
                    cancelled.clone(),
                    "cancelled-initial",
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Some(TerminalOrderStatus::Cancelled),
                ),
                110,
            )
            .unwrap();
        let rules = machine.store().snapshot().instrument_rules.clone();
        machine.materialize_replacements(&rules, 111).unwrap();
        let replacement_id = replacement_orders(machine.store().snapshot())[0]
            .client_order_id
            .clone();

        let intents = machine.store().snapshot().ready_intents(112).unwrap();
        for (index, mut intent) in intents.into_iter().enumerate() {
            if intent.client_order_id == replacement_id {
                continue;
            }
            intent.state = IntentState::Accepted {
                exchange_order_id: format!("initial-{index}"),
            };
            machine
                .synchronize_intent(&intent, 113 + index as u64)
                .unwrap();
        }
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::DeployingGrid
        );

        let replacement = machine
            .store()
            .snapshot()
            .ready_intents(140)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == replacement_id)
            .unwrap();
        let mut accepted_replacement = replacement;
        accepted_replacement.state = IntentState::Accepted {
            exchange_order_id: "replacement-13".into(),
        };
        machine
            .synchronize_intent(&accepted_replacement, 141)
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Running);
        assert!(state.initial_deployment_complete);
        assert_eq!(
            state
                .replacement_obligations
                .values()
                .filter(|obligation| {
                    obligation.source_client_order_id == cancelled
                        && obligation.kind == ReplacementObligationKind::RestoreCancelledRemainder
                })
                .count(),
            1
        );
    }

    #[test]
    fn terminal_intent_without_execution_accounting_blocks_stopped_state() {
        let mut machine = short_machine_with_opening();
        let mut intent = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        intent.state = IntentState::Terminal {
            status: TerminalOrderStatus::Filled,
            exchange_order_id: Some("terminal-order".into()),
        };
        machine.synchronize_intent(&intent, 111).unwrap();
        machine.request_stop(112).unwrap();

        assert!(matches!(
            machine.mark_stopped(113),
            Err(StrategyMachineError::InvalidState(
                StrategyStateError::OrdersNotTerminal
            ))
        ));
        let order = machine
            .store()
            .snapshot()
            .orders
            .get(&intent.client_order_id)
            .unwrap();
        assert!(!order.terminal_processed);
        assert_eq!(order.terminal_status, None);
    }

    #[test]
    fn accepted_intent_cannot_regress_to_prepared() {
        let mut machine = short_machine_with_opening();
        let prepared = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        machine.synchronize_intent(&prepared, 111).unwrap();
        let mut accepted = prepared.clone();
        accepted.state = IntentState::Accepted {
            exchange_order_id: "accepted-once".into(),
        };
        machine.synchronize_intent(&accepted, 112).unwrap();

        let transition = machine.synchronize_intent(&prepared, 113).unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(
            state
                .orders
                .get(&prepared.client_order_id)
                .unwrap()
                .tracking,
            StrategyOrderTracking::Intent {
                state: IntentState::Accepted {
                    exchange_order_id: "accepted-once".into()
                }
            }
        );
    }

    #[test]
    fn maximum_revision_never_saturates_into_an_accepted_write() {
        let mut current = state(Direction::Short, PositionBaseline::flat());
        current.revision = u64::MAX;
        let mut store = MemoryStrategyStateStore::new(current.clone());

        assert!(matches!(
            store.replace(current),
            Err(StrategyStoreError::RevisionMismatch)
        ));
    }

    #[test]
    fn exchange_execution_quantity_must_match_the_persisted_step() {
        let mut machine = short_machine_with_opening();
        let add = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);

        let transition = machine
            .apply_execution(
                &report(
                    add,
                    "misaligned-fill",
                    Decimal::new(15, 2),
                    Decimal::new(15225, 2),
                    None,
                ),
                110,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert!(state.replacement_obligations.is_empty());
    }

    #[test]
    fn execution_report_promotes_prepared_tracking_to_authoritative_accepted() {
        let mut machine = short_machine_with_opening();
        let reduce = grid_id(machine.store().snapshot(), 13, OrderSide::Buy, true);
        let prepared = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == reduce)
            .unwrap();
        machine.synchronize_intent(&prepared, 111).unwrap();

        machine
            .apply_execution(
                &report(
                    reduce.clone(),
                    "authoritative-fill",
                    Decimal::new(1, 1),
                    Decimal::new(1013, 1),
                    None,
                ),
                112,
            )
            .unwrap();

        assert_eq!(
            machine
                .store()
                .snapshot()
                .orders
                .get(&reduce)
                .unwrap()
                .tracking,
            StrategyOrderTracking::Intent {
                state: IntentState::Accepted {
                    exchange_order_id: "authoritative-fill".into()
                }
            }
        );
    }

    #[test]
    fn terminal_intent_cannot_be_regressed_by_a_non_terminal_execution_snapshot() {
        let mut machine = short_machine_with_opening();
        let mut intent = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        intent.state = IntentState::Terminal {
            status: TerminalOrderStatus::Filled,
            exchange_order_id: Some("terminal-regression".into()),
        };
        machine.synchronize_intent(&intent, 111).unwrap();

        let transition = machine
            .apply_execution(
                &report(
                    intent.client_order_id,
                    "terminal-regression",
                    Decimal::new(1, 1),
                    Decimal::new(1013, 1),
                    None,
                ),
                112,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::Failed
        );
    }

    #[test]
    fn failed_cleanup_releases_the_market_only_after_uncertain_orders_are_final() {
        let mut failed = state(Direction::Short, PositionBaseline::flat());
        let opening_id = failed
            .orders
            .values()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap()
            .client_order_id
            .clone();
        failed.orders.get_mut(&opening_id).unwrap().tracking = StrategyOrderTracking::Intent {
            state: IntentState::SubmitUnknown {
                message: "placement outcome is unknown".into(),
            },
        };
        failed.fail("replacement planning failed");
        failed.validate().unwrap();
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(failed));

        assert_eq!(
            machine.mark_failed_stopped(101).unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::Failed
        );

        let mut resolved = machine.store().snapshot().clone();
        resolved.orders.get_mut(&opening_id).unwrap().tracking = StrategyOrderTracking::Intent {
            state: IntentState::Rejected {
                code: Some("REJECTED".into()),
                message: "exchange definitively rejected the order".into(),
            },
        };
        resolved.validate().unwrap();
        let failure = resolved.failure.clone();
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(resolved));

        assert_eq!(
            machine.mark_failed_stopped(102).unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Stopped
            }
        );
        assert_eq!(machine.store().snapshot().failure, failure);
    }

    #[test]
    fn persisted_failure_reason_must_match_the_failure_lifecycle() {
        let mut missing = state(Direction::Short, PositionBaseline::flat());
        missing.lifecycle = StrategyLifecycle::Failed;
        assert_eq!(
            missing.validate(),
            Err(StrategyStateError::InvalidFailureState)
        );

        let mut blank = state(Direction::Short, PositionBaseline::flat());
        blank.fail("   ");
        assert_eq!(
            blank.validate(),
            Err(StrategyStateError::InvalidFailureState)
        );

        let mut misplaced = state(Direction::Short, PositionBaseline::flat());
        misplaced.failure = Some("stale failure".into());
        assert_eq!(
            misplaced.validate(),
            Err(StrategyStateError::InvalidFailureState)
        );

        let mut failed = state(Direction::Short, PositionBaseline::flat());
        failed.fail("replacement planning failed");
        failed.validate().unwrap();
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(failed));
        machine.mark_failed_stopped(101).unwrap();
        machine.mark_closed(102).unwrap();

        let archived = machine.store().snapshot();
        assert_eq!(archived.lifecycle, StrategyLifecycle::Closed);
        assert_eq!(
            archived.failure.as_deref(),
            Some("replacement planning failed")
        );
        archived.validate().unwrap();
    }

    #[test]
    fn ordinary_stop_retains_grid_and_baseline_positions_without_new_orders() {
        let mut machine = short_machine_with_opening();
        let order_count = machine.store().snapshot().orders.len();
        let expected = machine
            .store()
            .snapshot()
            .expected_exchange_position()
            .unwrap();

        assert_eq!(
            machine.request_stop(120).unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::StopRequested
            }
        );
        assert!(
            machine
                .store()
                .snapshot()
                .ready_intents(121)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            machine
                .materialize_replacements(
                    &machine.store().snapshot().instrument_rules.clone(),
                    121,
                )
                .unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(
            machine.mark_stopped(122).unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Stopped
            }
        );

        let state = machine.store().snapshot();
        assert_eq!(state.orders.len(), order_count);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert_eq!(state.baseline.signed_quantity, decimal(-3));
        assert_eq!(state.expected_exchange_position().unwrap(), expected);
        assert_eq!(state.lots_by_level.len(), 14);
        assert!(matches!(
            machine.mark_closed(123),
            Err(StrategyMachineError::InvalidState(
                StrategyStateError::CannotCloseStrategy
            ))
        ));
    }

    #[test]
    fn opening_fill_after_stop_request_is_recorded_but_never_activates_grid() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine.request_stop(101).unwrap();

        machine
            .apply_execution(
                &report(
                    opening,
                    "opening-after-stop",
                    Decimal::new(28, 1),
                    Decimal::new(28392, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                102,
            )
            .unwrap();

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::StopRequested);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert_eq!(state.lots_by_level.len(), 14);
        assert!(state.ready_intents(103).unwrap().is_empty());
        assert_eq!(
            state
                .orders
                .values()
                .filter(|order| order.purpose.is_initial_grid())
                .filter(|order| order.tracking == StrategyOrderTracking::Dormant)
                .count(),
            20
        );
    }

    #[test]
    fn accepted_exchange_order_blocks_stopped_state_until_reconciled_terminal() {
        let mut machine = short_machine_with_opening();
        let mut intent = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        intent.state = IntentState::Accepted {
            exchange_order_id: "live-grid-order".into(),
        };
        machine.synchronize_intent(&intent, 111).unwrap();
        machine.request_stop(112).unwrap();

        assert!(matches!(
            machine.mark_stopped(113),
            Err(StrategyMachineError::InvalidState(
                StrategyStateError::OrdersNotTerminal
            ))
        ));
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::StopRequested
        );
    }

    #[test]
    fn stop_race_fill_is_owned_without_creating_counter_or_remainder_orders() {
        let mut machine = short_machine_with_opening();
        let client_order_id = grid_id(machine.store().snapshot(), 14, OrderSide::Sell, false);
        let mut intent = machine
            .store()
            .snapshot()
            .ready_intents(110)
            .unwrap()
            .into_iter()
            .find(|intent| intent.client_order_id == client_order_id)
            .unwrap();
        intent.state = IntentState::Accepted {
            exchange_order_id: "stop-race-order".into(),
        };
        machine.synchronize_intent(&intent, 111).unwrap();
        machine.request_stop(112).unwrap();

        assert_eq!(
            machine
                .apply_execution(
                    &report(
                        client_order_id,
                        "stop-race-order",
                        Decimal::new(1, 1),
                        Decimal::new(1015, 1),
                        Some(TerminalOrderStatus::Cancelled),
                    ),
                    113,
                )
                .unwrap(),
            StrategyTransition::Updated {
                new_obligation_ids: vec![]
            }
        );

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::StopRequested);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-29, 1));
        assert!(state.replacement_obligations.is_empty());
        assert!(state.ready_intents(114).unwrap().is_empty());
        assert_eq!(
            machine.mark_stopped(115).unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Stopped
            }
        );
    }

    #[test]
    fn unsubmitted_flat_strategy_can_stop_and_close_without_market_action() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let order_count = initial.orders.len();
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));

        machine.request_stop(101).unwrap();
        machine.mark_stopped(102).unwrap();
        assert_eq!(
            machine.mark_closed(103).unwrap(),
            StrategyTransition::LifecycleChanged {
                lifecycle: StrategyLifecycle::Closed
            }
        );

        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Closed);
        assert_eq!(state.grid_position_net_quantity, Decimal::ZERO);
        assert_eq!(state.orders.len(), order_count);
        assert!(state.replacement_obligations.is_empty());
        assert!(state.ready_intents(104).unwrap().is_empty());
    }

    #[test]
    fn late_fill_after_stopped_is_owned_and_durably_escalated() {
        let initial = state(Direction::Short, PositionBaseline::flat());
        let opening = opening_id(&initial);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(initial));
        machine.request_stop(101).unwrap();
        machine.mark_stopped(102).unwrap();

        let transition = machine
            .apply_execution(
                &report(
                    opening,
                    "late-opening",
                    Decimal::new(28, 1),
                    Decimal::new(28392, 1),
                    Some(TerminalOrderStatus::Filled),
                ),
                103,
            )
            .unwrap();

        assert!(matches!(transition, StrategyTransition::Failed { .. }));
        let state = machine.store().snapshot();
        assert_eq!(state.lifecycle, StrategyLifecycle::Failed);
        assert_eq!(state.grid_position_net_quantity, Decimal::new(-28, 1));
        assert_eq!(state.lots_by_level.len(), 14);
        assert!(state.ready_intents(104).unwrap().is_empty());
    }
}
