use std::collections::BTreeSet;

use thiserror::Error;

use crate::{
    domain::{ClientOrderId, IntentState},
    exchange::{
        AuthoritativeOrder, OpenOrderSnapshotGateway, OrderLifecycle, PositionSnapshot,
        PositionSnapshotGateway,
    },
};

use super::{ShadowAuditReport, StrategyOrderTracking, StrategyState, audit_strategy_shadow};

#[derive(Debug, Clone, PartialEq)]
pub struct StableExchangeView {
    pub open_orders: Vec<AuthoritativeOrder>,
    pub position: PositionSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectedStrategyShadow {
    pub report: ShadowAuditReport,
    pub open_orders: Vec<AuthoritativeOrder>,
    pub position: PositionSnapshot,
}

/// Collects an exchange-only view without assuming that a strategy state exists.
/// Open orders must remain identical around the position read, otherwise the
/// result is inconclusive rather than a stale combination of two moments.
pub async fn collect_stable_exchange_view<G>(
    gateway: &G,
    exchange: crate::domain::Exchange,
    symbol: &str,
) -> Result<StableExchangeView, ShadowCollectionError>
where
    G: OpenOrderSnapshotGateway + PositionSnapshotGateway + ?Sized,
{
    let first_open_orders = gateway
        .open_orders_snapshot(exchange, symbol)
        .await
        .map_err(|error| ShadowCollectionError::OpenOrderSnapshot {
            pass: 1,
            message: error.to_string(),
        })?;
    let first_open_orders = normalize_open_orders(exchange, symbol, first_open_orders)?;
    let position = gateway
        .position_snapshot(exchange, symbol)
        .await
        .map_err(|error| ShadowCollectionError::PositionSnapshot {
            message: error.to_string(),
        })?;
    validate_position_identity(exchange, symbol, &position)?;
    let second_open_orders = gateway
        .open_orders_snapshot(exchange, symbol)
        .await
        .map_err(|error| ShadowCollectionError::OpenOrderSnapshot {
            pass: 2,
            message: error.to_string(),
        })?;
    let second_open_orders = normalize_open_orders(exchange, symbol, second_open_orders)?;
    if first_open_orders != second_open_orders {
        return Err(ShadowCollectionError::OpenOrdersChangedDuringCollection);
    }
    Ok(StableExchangeView {
        open_orders: first_open_orders,
        position,
    })
}

/// Collects a stable read-only exchange view and runs the pure shadow audit.
/// The trait bounds deliberately contain no placement, cancellation, or leverage
/// gateway, preventing this path from performing a trading write.
pub async fn collect_strategy_shadow<G>(
    gateway: &G,
    strategy: &StrategyState,
) -> Result<ShadowAuditReport, ShadowCollectionError>
where
    G: OpenOrderSnapshotGateway + PositionSnapshotGateway + ?Sized,
{
    Ok(collect_strategy_shadow_view(gateway, strategy)
        .await?
        .report)
}

pub async fn collect_strategy_shadow_view<G>(
    gateway: &G,
    strategy: &StrategyState,
) -> Result<CollectedStrategyShadow, ShadowCollectionError>
where
    G: OpenOrderSnapshotGateway + PositionSnapshotGateway + ?Sized,
{
    strategy
        .validate()
        .map_err(|error| ShadowCollectionError::InvalidStrategyState {
            message: error.to_string(),
        })?;

    let first_open_orders = gateway
        .open_orders_snapshot(strategy.exchange, &strategy.symbol)
        .await
        .map_err(|error| ShadowCollectionError::OpenOrderSnapshot {
            pass: 1,
            message: error.to_string(),
        })?;
    let first_open_orders =
        normalize_open_orders(strategy.exchange, &strategy.symbol, first_open_orders)?;
    let owned_open_orders = owned_open_orders(strategy, &first_open_orders);
    let open_client_order_ids = owned_open_orders
        .iter()
        .map(|order| order.client_order_id.clone())
        .collect::<BTreeSet<_>>();

    let pending_accounting_count = strategy
        .orders
        .values()
        .filter(|order| {
            !open_client_order_ids.contains(&order.client_order_id)
                && should_lookup_missing_order(order)
        })
        .count();
    if pending_accounting_count > 0 {
        return Err(ShadowCollectionError::StrategyOrderAccountingPending {
            count: pending_accounting_count,
        });
    }

    let position = gateway
        .position_snapshot(strategy.exchange, &strategy.symbol)
        .await
        .map_err(|error| ShadowCollectionError::PositionSnapshot {
            message: error.to_string(),
        })?;
    validate_position_identity(strategy.exchange, &strategy.symbol, &position)?;
    let second_open_orders = gateway
        .open_orders_snapshot(strategy.exchange, &strategy.symbol)
        .await
        .map_err(|error| ShadowCollectionError::OpenOrderSnapshot {
            pass: 2,
            message: error.to_string(),
        })?;
    let second_open_orders =
        normalize_open_orders(strategy.exchange, &strategy.symbol, second_open_orders)?;
    if first_open_orders != second_open_orders {
        return Err(ShadowCollectionError::OpenOrdersChangedDuringCollection);
    }

    let mut observed_orders = owned_open_orders;
    observed_orders.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
    let report = audit_strategy_shadow(strategy, &position, &observed_orders);
    Ok(CollectedStrategyShadow {
        report,
        open_orders: first_open_orders,
        position,
    })
}

fn should_lookup_missing_order(order: &super::StrategyOrderRecord) -> bool {
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

fn normalize_open_orders(
    exchange: crate::domain::Exchange,
    symbol: &str,
    orders: Vec<AuthoritativeOrder>,
) -> Result<Vec<AuthoritativeOrder>, ShadowCollectionError> {
    let mut client_order_ids = BTreeSet::new();
    let mut exchange_order_ids = BTreeSet::new();
    let mut normalized = Vec::with_capacity(orders.len());
    for order in orders {
        if order.exchange != exchange
            || order.shape.symbol != symbol
            || !matches!(order.lifecycle, OrderLifecycle::Active(_))
            || order.shape.validate().is_err()
            || order.exchange_order_id.trim().is_empty()
        {
            return Err(ShadowCollectionError::InvalidOpenOrder {
                client_order_id: order.client_order_id,
            });
        }
        if !client_order_ids.insert(order.client_order_id.clone())
            || !exchange_order_ids.insert(order.exchange_order_id.clone())
        {
            return Err(ShadowCollectionError::DuplicateOpenOrderIdentity);
        }
        normalized.push(order);
    }
    normalized.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
    Ok(normalized)
}

fn owned_open_orders(
    strategy: &StrategyState,
    orders: &[AuthoritativeOrder],
) -> Vec<AuthoritativeOrder> {
    orders
        .iter()
        .filter(|order| {
            strategy.orders.contains_key(&order.client_order_id)
                || belongs_to_run(&order.client_order_id, strategy.run_id.as_str())
        })
        .cloned()
        .collect()
}

fn validate_position_identity(
    exchange: crate::domain::Exchange,
    symbol: &str,
    position: &PositionSnapshot,
) -> Result<(), ShadowCollectionError> {
    if position.exchange != exchange || position.symbol != symbol {
        return Err(ShadowCollectionError::InvalidPositionIdentity);
    }
    Ok(())
}

fn belongs_to_run(client_order_id: &ClientOrderId, run_id: &str) -> bool {
    ["o", "g", "c", "r"].iter().any(|prefix| {
        client_order_id
            .as_str()
            .starts_with(&format!("{prefix}_{run_id}_"))
    })
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShadowCollectionError {
    #[error("strategy state is invalid before shadow collection: {message}")]
    InvalidStrategyState { message: String },
    #[error("open-order snapshot pass {pass} failed: {message}")]
    OpenOrderSnapshot { pass: u8, message: String },
    #[error("position snapshot failed: {message}")]
    PositionSnapshot { message: String },
    #[error("position snapshot belongs to another exchange or symbol")]
    InvalidPositionIdentity,
    #[error("{count} strategy orders are waiting for runtime execution accounting")]
    StrategyOrderAccountingPending { count: usize },
    #[error("open-order snapshot contains an invalid owned order")]
    InvalidOpenOrder { client_order_id: ClientOrderId },
    #[error("open-order snapshot contains duplicate client or exchange identities")]
    DuplicateOpenOrderIdentity,
    #[error("open orders changed between the two read-only collection passes")]
    OpenOrdersChangedDuringCollection,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{
            Direction, Exchange, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            OrderShape, PositionSizingMode, QuantityRules,
        },
        engine::{
            MarketSnapshot, PositionBaseline, StrategyLifecycle, StrategyRunId, StrategyState,
            build_grid_plan,
        },
        exchange::{ActiveOrderStatus, PositionLeg, PositionSide, PositionSnapshot, SnapshotError},
    };

    type OpenOrderResponses = Arc<Mutex<VecDeque<Result<Vec<AuthoritativeOrder>, SnapshotError>>>>;

    #[derive(Clone)]
    struct ReadOnlyGateway {
        open_orders: OpenOrderResponses,
        position: Result<PositionSnapshot, SnapshotError>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl ReadOnlyGateway {
        fn stable(open_orders: Vec<AuthoritativeOrder>, position: PositionSnapshot) -> Self {
            Self {
                open_orders: Arc::new(Mutex::new(VecDeque::from([
                    Ok(open_orders.clone()),
                    Ok(open_orders),
                ]))),
                position: Ok(position),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OpenOrderSnapshotGateway for ReadOnlyGateway {
        async fn open_orders_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<Vec<AuthoritativeOrder>, SnapshotError> {
            self.calls.lock().unwrap().push("open".into());
            self.open_orders
                .lock()
                .unwrap()
                .pop_front()
                .expect("open-order pass is configured")
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for ReadOnlyGateway {
        async fn position_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            self.calls.lock().unwrap().push("position".into());
            self.position.clone()
        }
    }

    fn strategy() -> StrategyState {
        let config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "MUUSDT".into(),
            direction: Direction::Neutral,
            upper_price: Decimal::new(1020, 0),
            lower_price: Decimal::new(1000, 0),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(100, 0)),
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
        let rules = InstrumentRules {
            tick_size: Decimal::new(1, 1),
            max_price_significant_digits: None,
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
            min_notional: Decimal::ZERO,
        };
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
            StrategyRunId::parse("COLLECT01").unwrap(),
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

    fn open_orders(state: &StrategyState) -> Vec<AuthoritativeOrder> {
        state
            .orders
            .values()
            .map(|order| AuthoritativeOrder {
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone().unwrap(),
                exchange: state.exchange,
                shape: order.shape.clone(),
                lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
                executed_quantity: Some(Decimal::ZERO),
            })
            .collect()
    }

    fn flat_position(state: &StrategyState) -> PositionSnapshot {
        PositionSnapshot {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            legs: vec![PositionLeg {
                side: PositionSide::Both,
                signed_quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: Decimal::new(10145, 1),
                unrealized_profit: Decimal::ZERO,
                leverage: Some(5),
            }],
        }
    }

    #[tokio::test]
    async fn stable_complete_snapshot_produces_a_clean_audit_using_reads_only() {
        let state = strategy();
        let gateway = ReadOnlyGateway::stable(open_orders(&state), flat_position(&state));

        let report = collect_strategy_shadow(&gateway, &state).await.unwrap();

        assert!(report.clean);
        assert_eq!(gateway.calls(), vec!["open", "position", "open"]);
    }

    #[tokio::test]
    async fn exchange_only_collection_keeps_the_complete_stable_strategy_order_set() {
        let state = strategy();
        let mut opens = open_orders(&state);
        let mut other_run = opens[0].clone();
        other_run.client_order_id = ClientOrderId::parse("g_OTHER001_1_B_1").unwrap();
        other_run.exchange_order_id = "other-run".into();
        opens.push(other_run.clone());
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));

        let view = collect_stable_exchange_view(&gateway, state.exchange, &state.symbol)
            .await
            .unwrap();

        assert!(
            view.open_orders
                .iter()
                .any(|order| order.client_order_id == other_run.client_order_id)
        );
        assert_eq!(gateway.calls(), vec!["open", "position", "open"]);
    }

    #[tokio::test]
    async fn exchange_only_collection_rejects_orders_that_change_around_position_read() {
        let state = strategy();
        let first = open_orders(&state);
        let mut second = first.clone();
        second.pop();
        let gateway = ReadOnlyGateway {
            open_orders: Arc::new(Mutex::new(VecDeque::from([Ok(first), Ok(second)]))),
            position: Ok(flat_position(&state)),
            calls: Arc::new(Mutex::new(Vec::new())),
        };

        assert_eq!(
            collect_stable_exchange_view(&gateway, state.exchange, &state.symbol).await,
            Err(ShadowCollectionError::OpenOrdersChangedDuringCollection)
        );
    }

    #[tokio::test]
    async fn changed_second_open_order_pass_never_produces_a_stale_report() {
        let state = strategy();
        let first = open_orders(&state);
        let mut second = first.clone();
        second.pop();
        let gateway = ReadOnlyGateway {
            open_orders: Arc::new(Mutex::new(VecDeque::from([Ok(first), Ok(second)]))),
            position: Ok(flat_position(&state)),
            calls: Arc::new(Mutex::new(Vec::new())),
        };

        assert_eq!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::OpenOrdersChangedDuringCollection)
        );
    }

    #[tokio::test]
    async fn missing_order_defers_to_runtime_accounting_without_per_order_reads() {
        let state = strategy();
        let mut opens = open_orders(&state);
        opens.remove(0);
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));

        assert_eq!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::StrategyOrderAccountingPending { count: 1 })
        );
        assert_eq!(gateway.calls(), vec!["open"]);
    }

    #[tokio::test]
    async fn terminal_unprocessed_order_defers_to_runtime_accounting() {
        let mut state = strategy();
        let mut opens = open_orders(&state);
        let missing_order = opens.remove(0);
        let record = state
            .orders
            .get_mut(&missing_order.client_order_id)
            .unwrap();
        record.tracking = StrategyOrderTracking::Intent {
            state: IntentState::Terminal {
                status: crate::domain::TerminalOrderStatus::Filled,
                exchange_order_id: Some(missing_order.exchange_order_id),
            },
        };
        state.validate().unwrap();
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));

        assert_eq!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::StrategyOrderAccountingPending { count: 1 })
        );
        assert_eq!(gateway.calls(), vec!["open"]);
    }

    #[tokio::test]
    async fn orphan_run_order_is_reported_while_other_runs_are_ignored() {
        let state = strategy();
        let mut opens = open_orders(&state);
        let mut orphan = opens[0].clone();
        orphan.client_order_id = ClientOrderId::parse("g_COLLECT01_99_B_999").unwrap();
        orphan.exchange_order_id = "orphan".into();
        opens.push(orphan);
        let mut other_run = opens[0].clone();
        other_run.client_order_id = ClientOrderId::parse("g_OTHER001_1_B_1").unwrap();
        other_run.exchange_order_id = "other-run".into();
        opens.push(other_run);
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));

        let collected = collect_strategy_shadow_view(&gateway, &state)
            .await
            .unwrap();
        let report = collected.report;

        assert!(!report.clean);
        assert_eq!(report.orders.unexpected_order_count, 1);
        assert_eq!(report.orders.observed_owned_order_count, 22);
        assert!(
            collected
                .open_orders
                .iter()
                .any(|order| order.exchange_order_id == "other-run")
        );
    }

    #[tokio::test]
    async fn invalid_strategy_blocks_before_any_exchange_read() {
        let mut state = strategy();
        state.version = 2;
        let gateway = ReadOnlyGateway::stable(Vec::new(), flat_position(&state));

        assert!(matches!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::InvalidStrategyState { .. })
        ));
        assert!(gateway.calls().is_empty());
    }

    #[test]
    fn collector_signature_requires_only_read_gateways() {
        fn accepts_collector_gateway<G>()
        where
            G: OpenOrderSnapshotGateway + PositionSnapshotGateway,
        {
        }

        accepts_collector_gateway::<ReadOnlyGateway>();
    }

    #[test]
    fn malformed_owned_shape_is_rejected_even_from_a_trait_implementation() {
        let state = strategy();
        let mut orders = open_orders(&state);
        orders[0].shape = OrderShape {
            quantity: Decimal::ZERO,
            ..orders[0].shape.clone()
        };
        assert!(matches!(
            normalize_open_orders(state.exchange, &state.symbol, orders),
            Err(ShadowCollectionError::InvalidOpenOrder { .. })
        ));
    }
}
