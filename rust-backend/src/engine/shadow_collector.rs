use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::{
    domain::{ClientOrderId, IntentState},
    exchange::{
        AuthoritativeOrder, OpenOrderSnapshotGateway, OrderLifecycle, OrderLookup,
        OrderLookupGateway, PositionSnapshotGateway,
    },
};

use super::{ShadowAuditReport, StrategyOrderTracking, StrategyState, audit_strategy_shadow};

/// Collects a stable read-only exchange view and runs the pure shadow audit.
/// The trait bounds deliberately contain no placement, cancellation, or leverage
/// gateway, preventing this path from performing a trading write.
pub async fn collect_strategy_shadow<G>(
    gateway: &G,
    strategy: &StrategyState,
) -> Result<ShadowAuditReport, ShadowCollectionError>
where
    G: OpenOrderSnapshotGateway + OrderLookupGateway + PositionSnapshotGateway,
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
    let first_open_orders = normalize_owned_open_orders(strategy, first_open_orders)?;
    let open_client_order_ids = first_open_orders
        .iter()
        .map(|order| order.client_order_id.clone())
        .collect::<BTreeSet<_>>();

    let mut terminal_observations = Vec::new();
    for order in strategy.orders.values().filter(|order| {
        !open_client_order_ids.contains(&order.client_order_id)
            && should_lookup_missing_order(order)
    }) {
        let lookup = gateway
            .lookup_order_by_client_id(strategy.exchange, &strategy.symbol, &order.client_order_id)
            .await
            .map_err(|error| ShadowCollectionError::OrderLookup {
                client_order_id: order.client_order_id.clone(),
                message: error.to_string(),
            })?;
        let OrderLookup::Found(observed) = lookup else {
            continue;
        };
        validate_lookup_identity(strategy, &order.client_order_id, &observed)?;
        if matches!(observed.lifecycle, OrderLifecycle::Active(_)) {
            return Err(ShadowCollectionError::ActiveLookupMissingFromOpenOrders {
                client_order_id: order.client_order_id.clone(),
            });
        }
        terminal_observations.push(observed);
    }

    let position = gateway
        .position_snapshot(strategy.exchange, &strategy.symbol)
        .await
        .map_err(|error| ShadowCollectionError::PositionSnapshot {
            message: error.to_string(),
        })?;
    let second_open_orders = gateway
        .open_orders_snapshot(strategy.exchange, &strategy.symbol)
        .await
        .map_err(|error| ShadowCollectionError::OpenOrderSnapshot {
            pass: 2,
            message: error.to_string(),
        })?;
    let second_open_orders = normalize_owned_open_orders(strategy, second_open_orders)?;
    if first_open_orders != second_open_orders {
        return Err(ShadowCollectionError::OpenOrdersChangedDuringCollection);
    }

    let mut observed_orders = first_open_orders;
    observed_orders.extend(terminal_observations);
    validate_combined_observations(&observed_orders)?;
    observed_orders.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
    Ok(audit_strategy_shadow(strategy, &position, &observed_orders))
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

fn normalize_owned_open_orders(
    strategy: &StrategyState,
    orders: Vec<AuthoritativeOrder>,
) -> Result<Vec<AuthoritativeOrder>, ShadowCollectionError> {
    let mut client_order_ids = BTreeSet::new();
    let mut exchange_order_ids = BTreeSet::new();
    let mut owned = Vec::new();
    for order in orders.into_iter().filter(|order| {
        strategy.orders.contains_key(&order.client_order_id)
            || belongs_to_run(&order.client_order_id, strategy.run_id.as_str())
    }) {
        if order.exchange != strategy.exchange
            || order.shape.symbol != strategy.symbol
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
        owned.push(order);
    }
    owned.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
    Ok(owned)
}

fn validate_lookup_identity(
    strategy: &StrategyState,
    expected_client_order_id: &ClientOrderId,
    observed: &AuthoritativeOrder,
) -> Result<(), ShadowCollectionError> {
    if observed.client_order_id != *expected_client_order_id
        || observed.exchange != strategy.exchange
        || observed.shape.symbol != strategy.symbol
        || observed.exchange_order_id.trim().is_empty()
        || observed.shape.validate().is_err()
    {
        return Err(ShadowCollectionError::InvalidLookupIdentity {
            client_order_id: expected_client_order_id.clone(),
        });
    }
    Ok(())
}

fn validate_combined_observations(
    observations: &[AuthoritativeOrder],
) -> Result<(), ShadowCollectionError> {
    let mut client_order_ids = BTreeSet::new();
    let mut exchange_order_ids = BTreeMap::<String, ClientOrderId>::new();
    for order in observations {
        if !client_order_ids.insert(order.client_order_id.clone())
            || exchange_order_ids
                .insert(
                    order.exchange_order_id.clone(),
                    order.client_order_id.clone(),
                )
                .is_some()
        {
            return Err(ShadowCollectionError::DuplicateObservationIdentity);
        }
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
    #[error("order lookup failed for {client_order_id:?}: {message}")]
    OrderLookup {
        client_order_id: ClientOrderId,
        message: String,
    },
    #[error("open-order snapshot contains an invalid owned order")]
    InvalidOpenOrder { client_order_id: ClientOrderId },
    #[error("open-order snapshot contains duplicate client or exchange identities")]
    DuplicateOpenOrderIdentity,
    #[error("order lookup returned a foreign or malformed identity")]
    InvalidLookupIdentity { client_order_id: ClientOrderId },
    #[error("an active per-order lookup was absent from the complete open-order snapshot")]
    ActiveLookupMissingFromOpenOrders { client_order_id: ClientOrderId },
    #[error("open orders changed between the two read-only collection passes")]
    OpenOrdersChangedDuringCollection,
    #[error("combined open and terminal observations contain duplicate identities")]
    DuplicateObservationIdentity,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
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
        exchange::{
            ActiveOrderStatus, LookupError, PositionLeg, PositionSide, PositionSnapshot,
            SnapshotError,
        },
    };

    type OpenOrderResponses = Arc<Mutex<VecDeque<Result<Vec<AuthoritativeOrder>, SnapshotError>>>>;

    #[derive(Clone)]
    struct ReadOnlyGateway {
        open_orders: OpenOrderResponses,
        position: Result<PositionSnapshot, SnapshotError>,
        lookups: Arc<Mutex<BTreeMap<ClientOrderId, Result<OrderLookup, LookupError>>>>,
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
                lookups: Arc::new(Mutex::new(BTreeMap::new())),
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

    #[async_trait]
    impl OrderLookupGateway for ReadOnlyGateway {
        async fn lookup_order_by_client_id(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
        ) -> Result<OrderLookup, LookupError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("lookup:{}", client_order_id.as_str()));
            self.lookups
                .lock()
                .unwrap()
                .get(client_order_id)
                .cloned()
                .unwrap_or(Ok(OrderLookup::NotFound))
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
    async fn changed_second_open_order_pass_never_produces_a_stale_report() {
        let state = strategy();
        let first = open_orders(&state);
        let mut second = first.clone();
        second.pop();
        let gateway = ReadOnlyGateway {
            open_orders: Arc::new(Mutex::new(VecDeque::from([Ok(first), Ok(second)]))),
            position: Ok(flat_position(&state)),
            lookups: Arc::new(Mutex::new(BTreeMap::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
        };

        assert_eq!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::OpenOrdersChangedDuringCollection)
        );
    }

    #[tokio::test]
    async fn missing_order_is_looked_up_and_not_found_remains_an_explicit_gap() {
        let state = strategy();
        let mut opens = open_orders(&state);
        let missing = opens.remove(0).client_order_id;
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));

        let report = collect_strategy_shadow(&gateway, &state).await.unwrap();

        assert!(!report.clean);
        assert_eq!(report.orders.missing_order_count, 1);
        assert!(
            gateway
                .calls()
                .contains(&format!("lookup:{}", missing.as_str()))
        );
    }

    #[tokio::test]
    async fn active_lookup_missing_from_complete_open_orders_is_inconclusive() {
        let state = strategy();
        let mut opens = open_orders(&state);
        let missing_order = opens.remove(0);
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));
        gateway.lookups.lock().unwrap().insert(
            missing_order.client_order_id.clone(),
            Ok(OrderLookup::Found(missing_order.clone())),
        );

        assert_eq!(
            collect_strategy_shadow(&gateway, &state).await,
            Err(ShadowCollectionError::ActiveLookupMissingFromOpenOrders {
                client_order_id: missing_order.client_order_id,
            })
        );
    }

    #[tokio::test]
    async fn terminal_lookup_is_compared_but_never_treated_as_an_active_order() {
        let state = strategy();
        let mut opens = open_orders(&state);
        let mut terminal = opens.remove(0);
        terminal.lifecycle = OrderLifecycle::Terminal(crate::domain::TerminalOrderStatus::Filled);
        let gateway = ReadOnlyGateway::stable(opens, flat_position(&state));
        gateway.lookups.lock().unwrap().insert(
            terminal.client_order_id.clone(),
            Ok(OrderLookup::Found(terminal)),
        );

        let report = collect_strategy_shadow(&gateway, &state).await.unwrap();

        assert!(!report.clean);
        assert_eq!(report.orders.missing_order_count, 0);
        assert_eq!(report.orders.mismatched_order_count, 1);
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

        let report = collect_strategy_shadow(&gateway, &state).await.unwrap();

        assert!(!report.clean);
        assert_eq!(report.orders.unexpected_order_count, 1);
        assert_eq!(report.orders.observed_owned_order_count, 22);
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
            G: OpenOrderSnapshotGateway + OrderLookupGateway + PositionSnapshotGateway,
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
            normalize_owned_open_orders(&state, orders),
            Err(ShadowCollectionError::InvalidOpenOrder { .. })
        ));
    }
}
