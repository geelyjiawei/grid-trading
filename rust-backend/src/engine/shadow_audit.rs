use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{ClientOrderId, Exchange, IntentState, OrderShape, TerminalOrderStatus},
    exchange::{ActiveOrderStatus, AuthoritativeOrder, OrderLifecycle, PositionSnapshot},
};

use super::{
    StrategyLifecycle, StrategyOrderPurpose, StrategyOrderRecord, StrategyOrderTracking,
    StrategyState,
};

/// A deterministic, read-only comparison between one durable strategy ledger and
/// authoritative exchange snapshots. This module intentionally has no gateway or
/// persistence dependency, so an audit can never place, cancel, or repair an order.
pub fn audit_strategy_shadow(
    strategy: &StrategyState,
    position: &PositionSnapshot,
    observed_orders: &[AuthoritativeOrder],
) -> ShadowAuditReport {
    let mut issues = Vec::new();

    if let Err(error) = strategy.validate() {
        issues.push(ShadowAuditIssue::InvalidStrategyState {
            message: error.to_string(),
        });
    }
    if strategy.lifecycle == StrategyLifecycle::Failed {
        issues.push(ShadowAuditIssue::StrategyFailed {
            message: strategy
                .failure
                .clone()
                .unwrap_or_else(|| "strategy failed without a reason".into()),
        });
    }

    let expected_position = match strategy.expected_exchange_position() {
        Ok(quantity) => Some(quantity),
        Err(error) => {
            issues.push(ShadowAuditIssue::NumericOverflow {
                context: error.to_string(),
            });
            None
        }
    };
    let mut position_audit = ShadowPositionAudit {
        baseline_quantity: strategy.baseline.signed_quantity,
        grid_owned_quantity: strategy.grid_position_net_quantity,
        expected_quantity: expected_position,
        actual_quantity: None,
        quantity_delta: None,
    };

    if position.exchange != strategy.exchange || position.symbol != strategy.symbol {
        issues.push(ShadowAuditIssue::PositionIdentityMismatch {
            expected_exchange: strategy.exchange,
            actual_exchange: position.exchange,
            expected_symbol: strategy.symbol.clone(),
            actual_symbol: position.symbol.clone(),
        });
    }
    match position.one_way_position() {
        Ok((actual_quantity, entry_price)) => {
            position_audit.actual_quantity = Some(actual_quantity);
            if position.legs[0].mark_price <= Decimal::ZERO
                || position.legs[0]
                    .leverage
                    .is_some_and(|leverage| leverage == 0)
                || (actual_quantity.is_zero()
                    && entry_price.is_some_and(|price| price <= Decimal::ZERO))
                || (!actual_quantity.is_zero()
                    && entry_price.is_none_or(|price| price <= Decimal::ZERO))
            {
                issues.push(ShadowAuditIssue::MalformedPositionSnapshot {
                    message:
                        "one-way position contains an invalid mark price, entry price, or leverage"
                            .into(),
                });
            }
            if let Some(expected_quantity) = expected_position {
                match actual_quantity.checked_sub(expected_quantity) {
                    Some(delta) => {
                        position_audit.quantity_delta = Some(delta);
                        if !delta.is_zero() {
                            issues.push(ShadowAuditIssue::PositionQuantityMismatch {
                                baseline_quantity: strategy.baseline.signed_quantity,
                                grid_owned_quantity: strategy.grid_position_net_quantity,
                                expected_quantity,
                                actual_quantity,
                                quantity_delta: delta,
                            });
                        }
                    }
                    None => issues.push(ShadowAuditIssue::NumericOverflow {
                        context: "actual minus expected position".into(),
                    }),
                }
            }
        }
        Err(error) => issues.push(ShadowAuditIssue::UnsupportedPositionMode {
            message: error.to_string(),
        }),
    }

    let mut observed_by_client = BTreeMap::<ClientOrderId, Vec<&AuthoritativeOrder>>::new();
    let mut exchange_identity_owners = BTreeMap::<String, Vec<ClientOrderId>>::new();
    for order in observed_orders.iter().filter(|order| {
        strategy.orders.contains_key(&order.client_order_id)
            || belongs_to_run(&order.client_order_id, strategy.run_id.as_str())
    }) {
        observed_by_client
            .entry(order.client_order_id.clone())
            .or_default()
            .push(order);
        exchange_identity_owners
            .entry(order.exchange_order_id.clone())
            .or_default()
            .push(order.client_order_id.clone());
    }
    for orders in observed_by_client.values_mut() {
        orders.sort_by_key(|order| observed_order_sort_key(order));
    }

    let duplicate_client_ids = observed_by_client
        .iter()
        .filter_map(|(client_order_id, orders)| {
            (orders.len() > 1).then_some(client_order_id.clone())
        })
        .collect::<BTreeSet<_>>();
    for client_order_id in &duplicate_client_ids {
        let exchange_order_ids = observed_by_client[client_order_id]
            .iter()
            .map(|order| order.exchange_order_id.clone())
            .collect();
        issues.push(ShadowAuditIssue::DuplicateObservedClientOrderId {
            client_order_id: client_order_id.clone(),
            exchange_order_ids,
        });
    }

    let mut duplicate_exchange_order_id_count = 0;
    for (exchange_order_id, client_order_ids) in &mut exchange_identity_owners {
        client_order_ids.sort();
        if client_order_ids.len() > 1 {
            duplicate_exchange_order_id_count += 1;
            issues.push(ShadowAuditIssue::DuplicateObservedExchangeOrderId {
                exchange_order_id: exchange_order_id.clone(),
                client_order_ids: client_order_ids.clone(),
            });
        }
    }

    let pending_replacement_obligation_ids = strategy
        .replacement_obligations
        .iter()
        .filter_map(|(id, obligation)| obligation.assigned_client_order_id.is_none().then_some(*id))
        .collect::<Vec<_>>();
    if !pending_replacement_obligation_ids.is_empty()
        && matches!(
            strategy.lifecycle,
            StrategyLifecycle::DeployingGrid | StrategyLifecycle::Running
        )
    {
        issues.push(ShadowAuditIssue::UnassignedReplacementObligations {
            obligation_ids: pending_replacement_obligation_ids.clone(),
        });
    }

    let mut summary = ShadowOrderAuditSummary {
        strategy_order_count: strategy.orders.len(),
        expected_authoritative_order_count: 0,
        observed_owned_order_count: observed_by_client.values().map(Vec::len).sum(),
        exact_authoritative_match_count: 0,
        pending_submission_count: 0,
        unresolved_intent_count: 0,
        terminal_accounting_pending_count: 0,
        partial_execution_pending_count: 0,
        missing_order_count: 0,
        unexpected_order_count: 0,
        mismatched_order_count: duplicate_client_ids.len(),
        duplicate_client_order_id_count: duplicate_client_ids.len(),
        duplicate_exchange_order_id_count,
    };
    let mut expected_active_levels = BTreeSet::new();
    let mut observed_exact_active_levels = BTreeSet::new();

    for (client_order_id, order) in &strategy.orders {
        let observations = observed_by_client
            .get(client_order_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let duplicate = duplicate_client_ids.contains(client_order_id);
        let level_index = order_level(order);

        match &order.tracking {
            StrategyOrderTracking::Dormant => {
                if matches!(
                    strategy.lifecycle,
                    StrategyLifecycle::DeployingGrid | StrategyLifecycle::Running
                ) {
                    issues.push(ShadowAuditIssue::DormantOrderInActiveStrategy {
                        client_order_id: client_order_id.clone(),
                        level_index,
                    });
                }
                record_unexpected_observations(
                    observations,
                    &mut issues,
                    &mut summary.unexpected_order_count,
                );
            }
            StrategyOrderTracking::Ready => {
                summary.pending_submission_count += 1;
                issues.push(ShadowAuditIssue::OrderAwaitingSubmission {
                    client_order_id: client_order_id.clone(),
                    level_index,
                });
                record_unexpected_observations(
                    observations,
                    &mut issues,
                    &mut summary.unexpected_order_count,
                );
            }
            StrategyOrderTracking::Intent { state } => match state {
                IntentState::Accepted { exchange_order_id } => {
                    summary.expected_authoritative_order_count += 1;
                    if let Some(level_index) = level_index {
                        expected_active_levels.insert(level_index);
                    }
                    if duplicate {
                        continue;
                    }
                    let Some(observed) = observations.first().copied() else {
                        summary.missing_order_count += 1;
                        issues.push(ShadowAuditIssue::MissingAuthoritativeOrder {
                            client_order_id: client_order_id.clone(),
                            expected_exchange_order_id: Some(exchange_order_id.clone()),
                            level_index,
                        });
                        continue;
                    };
                    let comparison = compare_order(
                        strategy,
                        order,
                        Some(exchange_order_id),
                        LifecycleExpectation::Active,
                        observed,
                        &mut issues,
                    );
                    if comparison.exact {
                        summary.exact_authoritative_match_count += 1;
                    } else if comparison.mismatch {
                        summary.mismatched_order_count += 1;
                    }
                    if comparison.partial_execution_pending {
                        summary.partial_execution_pending_count += 1;
                    }
                    if comparison.exact_active
                        && let Some(level_index) = level_index
                    {
                        observed_exact_active_levels.insert(level_index);
                    }
                }
                IntentState::Prepared | IntentState::SubmitUnknown { .. } => {
                    summary.unresolved_intent_count += 1;
                    issues.push(ShadowAuditIssue::UnresolvedOrderIntent {
                        client_order_id: client_order_id.clone(),
                        level_index,
                        state: state.clone(),
                    });
                    if !duplicate && let Some(observed) = observations.first().copied() {
                        let comparison = compare_order(
                            strategy,
                            order,
                            order.exchange_order_id.as_deref(),
                            LifecycleExpectation::Unresolved,
                            observed,
                            &mut issues,
                        );
                        if comparison.mismatch {
                            summary.mismatched_order_count += 1;
                        }
                        if comparison.partial_execution_pending {
                            summary.partial_execution_pending_count += 1;
                        }
                    }
                }
                IntentState::Rejected { .. } | IntentState::OwnershipConflict { .. } => {
                    summary.unresolved_intent_count += 1;
                    issues.push(ShadowAuditIssue::IrrecoverableOrderIntent {
                        client_order_id: client_order_id.clone(),
                        level_index,
                        state: state.clone(),
                    });
                    record_unexpected_observations(
                        observations,
                        &mut issues,
                        &mut summary.unexpected_order_count,
                    );
                }
                IntentState::Terminal { status, .. } => {
                    if !order.terminal_processed {
                        summary.expected_authoritative_order_count += 1;
                        summary.terminal_accounting_pending_count += 1;
                        issues.push(ShadowAuditIssue::TerminalAccountingPending {
                            client_order_id: client_order_id.clone(),
                            status: *status,
                        });
                    }
                    if duplicate {
                        continue;
                    }
                    let Some(observed) = observations.first().copied() else {
                        if !order.terminal_processed {
                            summary.missing_order_count += 1;
                            issues.push(ShadowAuditIssue::MissingAuthoritativeOrder {
                                client_order_id: client_order_id.clone(),
                                expected_exchange_order_id: order.exchange_order_id.clone(),
                                level_index,
                            });
                        }
                        continue;
                    };
                    let comparison = compare_order(
                        strategy,
                        order,
                        order.exchange_order_id.as_deref(),
                        LifecycleExpectation::Terminal(*status),
                        observed,
                        &mut issues,
                    );
                    if comparison.exact && !order.terminal_processed {
                        summary.exact_authoritative_match_count += 1;
                    } else if comparison.mismatch {
                        summary.mismatched_order_count += 1;
                    }
                }
            },
        }
    }

    for (client_order_id, observations) in &observed_by_client {
        if strategy.orders.contains_key(client_order_id) {
            continue;
        }
        record_unexpected_observations(
            observations,
            &mut issues,
            &mut summary.unexpected_order_count,
        );
    }

    let coverage_required = strategy.lifecycle == StrategyLifecycle::Running;
    let missing_levels = if coverage_required {
        (0..strategy.config.grid_count)
            .filter(|level| !observed_exact_active_levels.contains(level))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if !missing_levels.is_empty() {
        issues.push(ShadowAuditIssue::GridCoverageMismatch {
            missing_levels: missing_levels.clone(),
        });
    }
    let level_coverage = ShadowLevelCoverage {
        required: coverage_required,
        configured_level_count: strategy.config.grid_count,
        expected_active_levels: expected_active_levels.into_iter().collect(),
        observed_exact_active_levels: observed_exact_active_levels.into_iter().collect(),
        missing_levels,
    };

    ShadowAuditReport {
        version: 1,
        run_id: strategy.run_id.as_str().to_owned(),
        exchange: strategy.exchange,
        symbol: strategy.symbol.clone(),
        strategy_revision: strategy.revision,
        clean: issues.is_empty(),
        position: position_audit,
        orders: summary,
        level_coverage,
        pending_replacement_obligation_ids,
        issues,
    }
}

fn belongs_to_run(client_order_id: &ClientOrderId, run_id: &str) -> bool {
    ["o", "g", "c", "r"].iter().any(|prefix| {
        client_order_id
            .as_str()
            .starts_with(&format!("{prefix}_{run_id}_"))
    })
}

fn observed_order_sort_key(order: &AuthoritativeOrder) -> String {
    format!(
        "{}|{:?}|{:?}|{}|{}|{}|{:?}|{:?}|{:?}",
        order.exchange_order_id,
        order.exchange,
        order.shape.side,
        order
            .shape
            .price
            .map(|price| price.normalize().to_string())
            .unwrap_or_default(),
        order.shape.quantity.normalize(),
        order.shape.reduce_only,
        order.shape.kind,
        order.shape.time_in_force,
        order.lifecycle,
    )
}

fn order_level(order: &StrategyOrderRecord) -> Option<u16> {
    match &order.purpose {
        StrategyOrderPurpose::Opening | StrategyOrderPurpose::RiskClose => None,
        StrategyOrderPurpose::InitialGrid { level_index, .. }
        | StrategyOrderPurpose::Replacement { level_index, .. } => Some(*level_index),
    }
}

fn record_unexpected_observations(
    observations: &[&AuthoritativeOrder],
    issues: &mut Vec<ShadowAuditIssue>,
    unexpected_order_count: &mut usize,
) {
    for order in observations {
        *unexpected_order_count += 1;
        issues.push(ShadowAuditIssue::UnexpectedOwnedOrder {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
            lifecycle: order.lifecycle,
            shape: order.shape.clone(),
        });
    }
}

#[derive(Debug, Clone, Copy)]
enum LifecycleExpectation {
    Active,
    Terminal(TerminalOrderStatus),
    Unresolved,
}

#[derive(Debug, Clone, Copy)]
struct OrderComparison {
    exact: bool,
    exact_active: bool,
    mismatch: bool,
    partial_execution_pending: bool,
}

fn compare_order(
    strategy: &StrategyState,
    expected: &StrategyOrderRecord,
    expected_exchange_order_id: Option<&str>,
    expected_lifecycle: LifecycleExpectation,
    actual: &AuthoritativeOrder,
    issues: &mut Vec<ShadowAuditIssue>,
) -> OrderComparison {
    let mut exact = true;
    let mut mismatch = false;
    if actual.exchange != strategy.exchange {
        exact = false;
        mismatch = true;
        issues.push(ShadowAuditIssue::OrderExchangeMismatch {
            client_order_id: expected.client_order_id.clone(),
            expected_exchange: strategy.exchange,
            actual_exchange: actual.exchange,
        });
    }
    if let Some(expected_exchange_order_id) = expected_exchange_order_id
        && actual.exchange_order_id != expected_exchange_order_id
    {
        exact = false;
        mismatch = true;
        issues.push(ShadowAuditIssue::ExchangeOrderIdMismatch {
            client_order_id: expected.client_order_id.clone(),
            expected_exchange_order_id: expected_exchange_order_id.to_owned(),
            actual_exchange_order_id: actual.exchange_order_id.clone(),
        });
    }
    if actual.exchange_order_id.trim().is_empty() {
        exact = false;
        mismatch = true;
        issues.push(ShadowAuditIssue::EmptyExchangeOrderId {
            client_order_id: expected.client_order_id.clone(),
        });
    }
    if actual.shape != expected.shape {
        exact = false;
        mismatch = true;
        issues.push(ShadowAuditIssue::OrderShapeMismatch {
            client_order_id: expected.client_order_id.clone(),
            expected_shape: expected.shape.clone(),
            actual_shape: actual.shape.clone(),
        });
    }
    let lifecycle_matches = match expected_lifecycle {
        LifecycleExpectation::Active => matches!(actual.lifecycle, OrderLifecycle::Active(_)),
        LifecycleExpectation::Terminal(status) => {
            actual.lifecycle == OrderLifecycle::Terminal(status)
        }
        LifecycleExpectation::Unresolved => true,
    };
    if !lifecycle_matches {
        exact = false;
        mismatch = true;
        issues.push(ShadowAuditIssue::OrderLifecycleMismatch {
            client_order_id: expected.client_order_id.clone(),
            expected: match expected_lifecycle {
                LifecycleExpectation::Active => ShadowExpectedLifecycle::Active,
                LifecycleExpectation::Terminal(status) => {
                    ShadowExpectedLifecycle::Terminal { status }
                }
                LifecycleExpectation::Unresolved => ShadowExpectedLifecycle::Unresolved,
            },
            actual: actual.lifecycle,
        });
    }
    let exact_active = exact && matches!(actual.lifecycle, OrderLifecycle::Active(_));
    let partial_execution_pending = exact_active
        && actual.lifecycle == OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
    if partial_execution_pending {
        exact = false;
        issues.push(ShadowAuditIssue::PartialExecutionRequiresAccounting {
            client_order_id: expected.client_order_id.clone(),
        });
    }
    OrderComparison {
        exact,
        exact_active,
        mismatch,
        partial_execution_pending,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowAuditReport {
    pub version: u8,
    pub run_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub strategy_revision: u64,
    pub clean: bool,
    pub position: ShadowPositionAudit,
    pub orders: ShadowOrderAuditSummary,
    pub level_coverage: ShadowLevelCoverage,
    pub pending_replacement_obligation_ids: Vec<u64>,
    pub issues: Vec<ShadowAuditIssue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowPositionAudit {
    pub baseline_quantity: Decimal,
    pub grid_owned_quantity: Decimal,
    pub expected_quantity: Option<Decimal>,
    pub actual_quantity: Option<Decimal>,
    pub quantity_delta: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowOrderAuditSummary {
    pub strategy_order_count: usize,
    pub expected_authoritative_order_count: usize,
    pub observed_owned_order_count: usize,
    pub exact_authoritative_match_count: usize,
    pub pending_submission_count: usize,
    pub unresolved_intent_count: usize,
    pub terminal_accounting_pending_count: usize,
    pub partial_execution_pending_count: usize,
    pub missing_order_count: usize,
    pub unexpected_order_count: usize,
    pub mismatched_order_count: usize,
    pub duplicate_client_order_id_count: usize,
    pub duplicate_exchange_order_id_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowLevelCoverage {
    pub required: bool,
    pub configured_level_count: u16,
    pub expected_active_levels: Vec<u16>,
    pub observed_exact_active_levels: Vec<u16>,
    pub missing_levels: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShadowExpectedLifecycle {
    Active,
    Terminal { status: TerminalOrderStatus },
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShadowAuditIssue {
    InvalidStrategyState {
        message: String,
    },
    StrategyFailed {
        message: String,
    },
    NumericOverflow {
        context: String,
    },
    PositionIdentityMismatch {
        expected_exchange: Exchange,
        actual_exchange: Exchange,
        expected_symbol: String,
        actual_symbol: String,
    },
    UnsupportedPositionMode {
        message: String,
    },
    MalformedPositionSnapshot {
        message: String,
    },
    PositionQuantityMismatch {
        baseline_quantity: Decimal,
        grid_owned_quantity: Decimal,
        expected_quantity: Decimal,
        actual_quantity: Decimal,
        quantity_delta: Decimal,
    },
    DuplicateObservedClientOrderId {
        client_order_id: ClientOrderId,
        exchange_order_ids: Vec<String>,
    },
    DuplicateObservedExchangeOrderId {
        exchange_order_id: String,
        client_order_ids: Vec<ClientOrderId>,
    },
    UnassignedReplacementObligations {
        obligation_ids: Vec<u64>,
    },
    DormantOrderInActiveStrategy {
        client_order_id: ClientOrderId,
        level_index: Option<u16>,
    },
    OrderAwaitingSubmission {
        client_order_id: ClientOrderId,
        level_index: Option<u16>,
    },
    UnresolvedOrderIntent {
        client_order_id: ClientOrderId,
        level_index: Option<u16>,
        state: IntentState,
    },
    IrrecoverableOrderIntent {
        client_order_id: ClientOrderId,
        level_index: Option<u16>,
        state: IntentState,
    },
    TerminalAccountingPending {
        client_order_id: ClientOrderId,
        status: TerminalOrderStatus,
    },
    PartialExecutionRequiresAccounting {
        client_order_id: ClientOrderId,
    },
    MissingAuthoritativeOrder {
        client_order_id: ClientOrderId,
        expected_exchange_order_id: Option<String>,
        level_index: Option<u16>,
    },
    UnexpectedOwnedOrder {
        client_order_id: ClientOrderId,
        exchange_order_id: String,
        lifecycle: OrderLifecycle,
        shape: OrderShape,
    },
    OrderExchangeMismatch {
        client_order_id: ClientOrderId,
        expected_exchange: Exchange,
        actual_exchange: Exchange,
    },
    ExchangeOrderIdMismatch {
        client_order_id: ClientOrderId,
        expected_exchange_order_id: String,
        actual_exchange_order_id: String,
    },
    EmptyExchangeOrderId {
        client_order_id: ClientOrderId,
    },
    OrderShapeMismatch {
        client_order_id: ClientOrderId,
        expected_shape: OrderShape,
        actual_shape: OrderShape,
    },
    OrderLifecycleMismatch {
        client_order_id: ClientOrderId,
        expected: ShadowExpectedLifecycle,
        actual: OrderLifecycle,
    },
    GridCoverageMismatch {
        missing_levels: Vec<u16>,
    },
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{
            Direction, GridConfig, GridMode, InitialOrderType, InstrumentRules, OrderKind,
            OrderSide, PositionSizingMode, QuantityRules, TimeInForce,
        },
        engine::{
            ExecutionReport, MemoryStrategyStateStore, NeutralLot, StrategyMachine,
            StrategyStateStore,
        },
        engine::{GridOrderRole, MarketSnapshot, PositionBaseline, StrategyRunId, build_grid_plan},
        exchange::{ActiveOrderStatus, PositionLeg, PositionSide},
    };

    fn config(direction: Direction, quantity: Decimal) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "MUUSDT".into(),
            direction,
            upper_price: Decimal::new(1020, 0),
            lower_price: Decimal::new(1000, 0),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(quantity),
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
        }
    }

    fn rules() -> InstrumentRules {
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

    fn running_neutral_state(quantity: Decimal) -> StrategyState {
        let config = config(Direction::Neutral, quantity);
        let rules = rules();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(10145, 1),
                mark_price: Decimal::new(10145, 1),
            },
            &rules,
        )
        .unwrap();
        let mut state = StrategyState::from_plan(
            StrategyRunId::parse("SHADOW01").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        for (index, order) in state.orders.values_mut().enumerate() {
            let exchange_order_id = format!("exchange-{index:02}");
            order.tracking = StrategyOrderTracking::Intent {
                state: IntentState::Accepted {
                    exchange_order_id: exchange_order_id.clone(),
                },
            };
            order.exchange_order_id = Some(exchange_order_id);
        }
        state.lifecycle = StrategyLifecycle::Running;
        state.initial_deployment_complete = true;
        state.validate().unwrap();
        state
    }

    fn awaiting_short_state() -> StrategyState {
        let mut config = config(Direction::Short, Decimal::new(2, 1));
        config.initial_order_type = InitialOrderType::Limit;
        config.initial_order_price = Some(Decimal::new(1014, 0));
        let rules = rules();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(10145, 1),
                mark_price: Decimal::new(10145, 1),
            },
            &rules,
        )
        .unwrap();
        let mut state = StrategyState::from_plan(
            StrategyRunId::parse("BASELINE1").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::from_authoritative_position(
                Decimal::new(-3, 0),
                Some(Decimal::new(1030, 0)),
            )
            .unwrap(),
            100,
        )
        .unwrap();
        let opening = state
            .orders
            .values_mut()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap();
        opening.tracking = StrategyOrderTracking::Intent {
            state: IntentState::Accepted {
                exchange_order_id: "opening-1".into(),
            },
        };
        opening.exchange_order_id = Some("opening-1".into());
        state.validate().unwrap();
        state
    }

    fn observations(state: &StrategyState) -> Vec<AuthoritativeOrder> {
        state
            .orders
            .values()
            .filter_map(|order| match &order.tracking {
                StrategyOrderTracking::Intent {
                    state: IntentState::Accepted { exchange_order_id },
                } => Some(AuthoritativeOrder {
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id: exchange_order_id.clone(),
                    exchange: state.exchange,
                    shape: order.shape.clone(),
                    lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
                }),
                _ => None,
            })
            .collect()
    }

    fn position(state: &StrategyState, quantity: Decimal) -> PositionSnapshot {
        PositionSnapshot {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            legs: vec![PositionLeg {
                side: PositionSide::Both,
                signed_quantity: quantity,
                entry_price: (!quantity.is_zero()).then_some(Decimal::new(10145, 1)),
                mark_price: Decimal::new(10145, 1),
                unrealized_profit: Decimal::ZERO,
                leverage: Some(5),
            }],
        }
    }

    #[test]
    fn exact_running_snapshot_is_clean_and_accepts_two_orders_at_one_level() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let orders = observations(&state);
        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(report.clean);
        assert_eq!(report.orders.strategy_order_count, 21);
        assert_eq!(report.orders.exact_authoritative_match_count, 21);
        assert_eq!(report.level_coverage.configured_level_count, 20);
        assert_eq!(report.level_coverage.observed_exact_active_levels.len(), 20);
        assert!(report.level_coverage.missing_levels.is_empty());
        let duplicate_level_count = state
            .orders
            .values()
            .filter(|order| {
                matches!(
                    order.purpose,
                    StrategyOrderPurpose::InitialGrid {
                        level_index: 14,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(duplicate_level_count, 2);
    }

    #[test]
    fn old_position_remains_an_immutable_baseline() {
        let state = awaiting_short_state();
        let orders = observations(&state);
        let report = audit_strategy_shadow(
            &state,
            &PositionSnapshot {
                exchange: state.exchange,
                symbol: state.symbol.clone(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: Decimal::new(-3, 0),
                    entry_price: Some(Decimal::new(1030, 0)),
                    mark_price: Decimal::new(10145, 1),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(5),
                }],
            },
            &orders,
        );

        assert!(report.clean);
        assert_eq!(report.position.baseline_quantity, Decimal::new(-3, 0));
        assert_eq!(report.position.grid_owned_quantity, Decimal::ZERO);
        assert_eq!(report.position.expected_quantity, Some(Decimal::new(-3, 0)));
        assert_eq!(report.position.quantity_delta, Some(Decimal::ZERO));
        assert!(!report.level_coverage.required);
    }

    #[test]
    fn unexplained_position_delta_is_reported_and_never_absorbed() {
        let state = awaiting_short_state();
        let report = audit_strategy_shadow(
            &state,
            &PositionSnapshot {
                exchange: state.exchange,
                symbol: state.symbol.clone(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: Decimal::new(-28, 1),
                    entry_price: Some(Decimal::new(1030, 0)),
                    mark_price: Decimal::new(10145, 1),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(5),
                }],
            },
            &observations(&state),
        );

        assert!(!report.clean);
        assert_eq!(report.position.baseline_quantity, Decimal::new(-3, 0));
        assert_eq!(report.position.expected_quantity, Some(Decimal::new(-3, 0)));
        assert_eq!(report.position.actual_quantity, Some(Decimal::new(-28, 1)));
        assert_eq!(report.position.quantity_delta, Some(Decimal::new(2, 1)));
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::PositionQuantityMismatch { quantity_delta, .. }
                if *quantity_delta == Decimal::new(2, 1)
        )));
    }

    #[test]
    fn quantity_100_observed_as_70_is_an_exact_shape_mismatch() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        orders[0].shape.quantity = Decimal::new(70, 0);

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!report.clean);
        assert_eq!(report.orders.mismatched_order_count, 1);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::OrderShapeMismatch {
                expected_shape,
                actual_shape,
                ..
            } if expected_shape.quantity == Decimal::new(100, 0)
                && actual_shape.quantity == Decimal::new(70, 0)
        )));
    }

    #[test]
    fn partially_filled_order_keeps_level_coverage_but_requires_execution_accounting() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        orders[0].lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!report.clean);
        assert_eq!(report.orders.partial_execution_pending_count, 1);
        assert_eq!(report.orders.mismatched_order_count, 0);
        assert!(report.level_coverage.missing_levels.is_empty());
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::PartialExecutionRequiresAccounting { .. }
        )));
    }

    #[test]
    fn baseline_plus_grid_owned_quantity_is_compared_without_rewriting_either_component() {
        let mut state = running_neutral_state(Decimal::new(100, 0));
        state.grid_position_net_quantity = Decimal::new(100, 0);
        state.neutral_lots.insert(
            1,
            NeutralLot {
                id: 1,
                signed_quantity: Decimal::new(100, 0),
                entry_value: Decimal::new(101450, 0),
            },
        );
        state.next_neutral_lot_sequence = 2;
        state.validate().unwrap();

        let report = audit_strategy_shadow(
            &state,
            &position(&state, Decimal::new(100, 0)),
            &observations(&state),
        );

        assert!(report.clean);
        assert_eq!(report.position.baseline_quantity, Decimal::ZERO);
        assert_eq!(report.position.grid_owned_quantity, Decimal::new(100, 0));
        assert_eq!(
            report.position.expected_quantity,
            Some(Decimal::new(100, 0))
        );
        assert_eq!(report.position.actual_quantity, Some(Decimal::new(100, 0)));
    }

    #[test]
    fn every_immutable_exchange_order_field_is_checked_independently() {
        let state = running_neutral_state(Decimal::new(100, 0));
        for mutation in 0..9 {
            let mut orders = observations(&state);
            match mutation {
                0 => orders[0].exchange = Exchange::Binance,
                1 => orders[0].exchange_order_id = "different-order-id".into(),
                2 => orders[0].shape.symbol = "ANSEMUSDT".into(),
                3 => {
                    orders[0].shape.side = match orders[0].shape.side {
                        OrderSide::Buy => OrderSide::Sell,
                        OrderSide::Sell => OrderSide::Buy,
                    }
                }
                4 => {
                    orders[0].shape.price = orders[0]
                        .shape
                        .price
                        .and_then(|price| price.checked_add(Decimal::new(1, 1)))
                }
                5 => orders[0].shape.quantity = Decimal::new(70, 0),
                6 => orders[0].shape.reduce_only = !orders[0].shape.reduce_only,
                7 => orders[0].shape.time_in_force = TimeInForce::PostOnly,
                8 => orders[0].lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Expired),
                _ => unreachable!(),
            }

            let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);
            assert!(!report.clean, "mutation {mutation} escaped the audit");
            assert_eq!(
                report.orders.mismatched_order_count, 1,
                "mutation {mutation} was not counted exactly once"
            );
        }
    }

    #[test]
    fn active_unassigned_replacement_obligation_is_never_hidden() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let source = state.orders.values().next().unwrap().clone();
        let quantity = Decimal::new(10, 0);
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state));
        machine
            .apply_execution(
                &ExecutionReport {
                    client_order_id: source.client_order_id,
                    exchange_order_id: source.exchange_order_id.unwrap(),
                    cumulative_quantity: quantity,
                    cumulative_quote: source.shape.price.unwrap() * quantity,
                    cumulative_fee: Decimal::ZERO,
                    terminal_status: None,
                },
                101,
            )
            .unwrap();
        let state = machine.store().snapshot().clone();
        assert_eq!(state.replacement_obligations.len(), 1);
        assert!(
            state
                .replacement_obligations
                .values()
                .all(|obligation| obligation.assigned_client_order_id.is_none())
        );

        let report = audit_strategy_shadow(
            &state,
            &position(&state, state.expected_exchange_position().unwrap()),
            &observations(&state),
        );

        assert!(!report.clean);
        assert_eq!(report.pending_replacement_obligation_ids, vec![1]);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::UnassignedReplacementObligations { obligation_ids }
                if obligation_ids == &vec![1]
        )));
    }

    #[test]
    fn foreign_and_malformed_position_snapshots_fail_closed() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let orders = observations(&state);

        let mut foreign_exchange = position(&state, Decimal::ZERO);
        foreign_exchange.exchange = Exchange::Binance;
        let mut foreign_symbol = position(&state, Decimal::ZERO);
        foreign_symbol.symbol = "ANSEMUSDT".into();
        let mut invalid_mark = position(&state, Decimal::ZERO);
        invalid_mark.legs[0].mark_price = Decimal::ZERO;
        let mut invalid_leverage = position(&state, Decimal::ZERO);
        invalid_leverage.legs[0].leverage = Some(0);
        let mut missing_entry = position(&state, Decimal::ONE);
        missing_entry.legs[0].entry_price = None;

        for (name, snapshot) in [
            ("foreign_exchange", foreign_exchange),
            ("foreign_symbol", foreign_symbol),
            ("invalid_mark", invalid_mark),
            ("invalid_leverage", invalid_leverage),
            ("missing_entry", missing_entry),
        ] {
            let report = audit_strategy_shadow(&state, &snapshot, &orders);
            assert!(!report.clean, "{name} position escaped the audit");
        }
    }

    #[test]
    fn missing_and_orphan_run_orders_are_both_visible() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        orders.remove(0);
        let mut orphan = orders[0].clone();
        orphan.client_order_id = ClientOrderId::parse("g_SHADOW01_99_B_999").unwrap();
        orphan.exchange_order_id = "orphan-999".into();
        orders.push(orphan);
        let mut manual = orders[0].clone();
        manual.client_order_id = ClientOrderId::parse("manual_1").unwrap();
        manual.exchange_order_id = "manual-order".into();
        orders.push(manual);

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!report.clean);
        assert_eq!(report.orders.missing_order_count, 1);
        assert_eq!(report.orders.unexpected_order_count, 1);
        assert_eq!(report.orders.observed_owned_order_count, 21);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::UnexpectedOwnedOrder { client_order_id, .. }
                if client_order_id.as_str() == "g_SHADOW01_99_B_999"
        )));
    }

    #[test]
    fn duplicate_client_and_exchange_identities_fail_closed_deterministically() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        let duplicate_client = orders[0].clone();
        orders.push(duplicate_client);
        orders[2].exchange_order_id = orders[1].exchange_order_id.clone();

        let first = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);
        orders.reverse();
        let second = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!first.clean);
        assert_eq!(first, second);
        assert_eq!(first.orders.duplicate_client_order_id_count, 1);
        assert_eq!(first.orders.duplicate_exchange_order_id_count, 2);
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
    }

    #[test]
    fn cancellation_is_not_mistaken_for_an_active_or_filled_order() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        orders[0].lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled);

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!report.clean);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::OrderLifecycleMismatch {
                expected: ShadowExpectedLifecycle::Active,
                actual: OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
                ..
            }
        )));
    }

    #[test]
    fn exact_terminal_order_still_blocks_until_execution_accounting_commits() {
        let mut state = running_neutral_state(Decimal::new(100, 0));
        let client_order_id = state.orders.keys().next().unwrap().clone();
        let (exchange_order_id, shape) = {
            let order = state.orders.get_mut(&client_order_id).unwrap();
            let exchange_order_id = order.exchange_order_id.clone().unwrap();
            order.tracking = StrategyOrderTracking::Intent {
                state: IntentState::Terminal {
                    status: TerminalOrderStatus::Cancelled,
                    exchange_order_id: Some(exchange_order_id.clone()),
                },
            };
            (exchange_order_id, order.shape.clone())
        };
        let mut orders = observations(&state);
        orders.push(AuthoritativeOrder {
            client_order_id: client_order_id.clone(),
            exchange_order_id,
            exchange: state.exchange,
            shape,
            lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
        });
        state.validate().unwrap();

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        assert!(!report.clean);
        assert_eq!(report.orders.terminal_accounting_pending_count, 1);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::TerminalAccountingPending { client_order_id: id, .. }
                if id == &client_order_id
        )));
    }

    #[test]
    fn empty_and_hedge_mode_positions_are_never_treated_as_flat() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let orders = observations(&state);
        let empty = PositionSnapshot {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            legs: Vec::new(),
        };
        let hedge = PositionSnapshot {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            legs: vec![
                PositionLeg {
                    side: PositionSide::Long,
                    signed_quantity: Decimal::ONE,
                    entry_price: Some(Decimal::new(1014, 0)),
                    mark_price: Decimal::new(10145, 1),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(5),
                },
                PositionLeg {
                    side: PositionSide::Short,
                    signed_quantity: -Decimal::ONE,
                    entry_price: Some(Decimal::new(1014, 0)),
                    mark_price: Decimal::new(10145, 1),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(5),
                },
            ],
        };

        for snapshot in [empty, hedge] {
            let report = audit_strategy_shadow(&state, &snapshot, &orders);
            assert!(!report.clean);
            assert_eq!(report.position.actual_quantity, None);
            assert!(
                report
                    .issues
                    .iter()
                    .any(|issue| matches!(issue, ShadowAuditIssue::UnsupportedPositionMode { .. }))
            );
        }
    }

    #[test]
    fn unresolved_submission_is_reported_without_guessing_order_ownership() {
        let mut state = running_neutral_state(Decimal::new(100, 0));
        let client_order_id = state.orders.keys().next().unwrap().clone();
        state.orders.get_mut(&client_order_id).unwrap().tracking = StrategyOrderTracking::Intent {
            state: IntentState::SubmitUnknown {
                message: "timeout".into(),
            },
        };
        state.validate().unwrap();

        let report = audit_strategy_shadow(
            &state,
            &position(&state, Decimal::ZERO),
            &observations(&state),
        );

        assert!(!report.clean);
        assert_eq!(report.orders.unresolved_intent_count, 1);
        assert!(report.issues.iter().any(|issue| matches!(
            issue,
            ShadowAuditIssue::UnresolvedOrderIntent { client_order_id: id, .. }
                if id == &client_order_id
        )));
    }

    #[test]
    fn report_preserves_full_order_shape_including_side_type_and_time_in_force() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let mut orders = observations(&state);
        orders[0].shape.side = OrderSide::Sell;
        orders[0].shape.kind = OrderKind::Market;
        orders[0].shape.price = None;
        orders[0].shape.time_in_force = TimeInForce::Gtc;

        let report = audit_strategy_shadow(&state, &position(&state, Decimal::ZERO), &orders);

        let mismatch = report.issues.iter().find_map(|issue| match issue {
            ShadowAuditIssue::OrderShapeMismatch {
                expected_shape,
                actual_shape,
                ..
            } => Some((expected_shape, actual_shape)),
            _ => None,
        });
        let (expected, actual) = mismatch.unwrap();
        assert_ne!(expected.side, actual.side);
        assert_eq!(expected.kind, OrderKind::Limit);
        assert_eq!(actual.kind, OrderKind::Market);
        assert_eq!(expected.quantity, actual.quantity);
    }

    #[test]
    fn fixture_really_contains_two_opposite_orders_on_the_same_level() {
        let state = running_neutral_state(Decimal::new(100, 0));
        let level_fourteen = state
            .orders
            .values()
            .filter_map(|order| match order.purpose {
                StrategyOrderPurpose::InitialGrid {
                    level_index: 14,
                    role,
                } => Some((role, order.shape.side)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(level_fourteen.len(), 2);
        assert!(level_fourteen.contains(&(GridOrderRole::Add, OrderSide::Buy)));
        assert!(level_fourteen.contains(&(GridOrderRole::Add, OrderSide::Sell)));
    }
}
