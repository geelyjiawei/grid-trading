use std::sync::Arc;

use thiserror::Error;

use crate::{
    domain::{ClientOrderId, Exchange, IntentState, OrderShape},
    engine::{
        ExecutionAccountingError, ExecutionAccountingService, FeeValuation, StrategyMachine,
        StrategyMachineError, StrategyOrderTracking, StrategyStateStore, StrategyTransition,
        ValuedExecutionReport,
    },
    exchange::{
        ExecutionSnapshotGateway, HistoricalPriceGateway, OrderExecutionSnapshot, OrderLifecycle,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionSyncResult {
    pub snapshot: OrderExecutionSnapshot,
    pub valued_report: ValuedExecutionReport,
    pub transition: StrategyTransition,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedExecutionSync {
    pub(crate) snapshot: OrderExecutionSnapshot,
    pub(crate) valued_report: ValuedExecutionReport,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSyncRequest {
    exchange: Exchange,
    symbol: String,
    shape: OrderShape,
    client_order_id: ClientOrderId,
    exchange_order_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoadedExecutionSnapshot {
    request: ExecutionSyncRequest,
    snapshot: OrderExecutionSnapshot,
}

#[derive(Debug, Error)]
pub enum ExecutionSyncError {
    #[error("strategy does not own the requested client order ID")]
    UnknownStrategyOrder,
    #[error("strategy order does not yet have an authoritative exchange order ID")]
    MissingExchangeOrderId,
    #[error("execution snapshot does not match the immutable strategy order")]
    OrderIdentityMismatch,
    #[error("execution snapshot lookup is inconclusive: {0}")]
    SnapshotInconclusive(String),
    #[error(transparent)]
    Accounting(#[from] ExecutionAccountingError),
    #[error(transparent)]
    StateMachine(#[from] StrategyMachineError),
}

#[derive(Debug, Clone)]
pub struct ExecutionSyncService {
    accounting: Arc<ExecutionAccountingService>,
}

pub(crate) struct PendingFeeValuation<G> {
    pub(crate) gateway: G,
    pub(crate) service: ExecutionSyncService,
    pub(crate) snapshot: OrderExecutionSnapshot,
    pub(crate) fee_valuations: Vec<FeeValuation>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedFeeValuation {
    pub(crate) client_order_id: ClientOrderId,
    pub(crate) fee_valuations: Vec<FeeValuation>,
}

impl<G: HistoricalPriceGateway> PendingFeeValuation<G> {
    pub(crate) async fn resolve(self) -> Result<ResolvedFeeValuation, ExecutionSyncError> {
        let valued = self
            .service
            .accounting
            .resolve_pending_valuations(&self.gateway, &self.snapshot, &self.fee_valuations)
            .await?;
        Ok(ResolvedFeeValuation {
            client_order_id: self.snapshot.order.client_order_id,
            fee_valuations: valued.fee_valuations,
        })
    }
}

impl ExecutionSyncService {
    pub fn new(quote_asset: &str) -> Result<Self, ExecutionAccountingError> {
        Ok(Self {
            accounting: Arc::new(ExecutionAccountingService::new(quote_asset)?),
        })
    }

    pub async fn synchronize<G, S>(
        &self,
        gateway: &G,
        machine: &mut StrategyMachine<S>,
        client_order_id: &ClientOrderId,
        now_ms: u64,
    ) -> Result<ExecutionSyncResult, ExecutionSyncError>
    where
        G: ExecutionSnapshotGateway + HistoricalPriceGateway,
        S: StrategyStateStore,
    {
        let request = self.request_for(machine, client_order_id)?;
        let loaded = self.load_snapshot(gateway, request).await?;
        self.apply_loaded_snapshot(gateway, machine, loaded, now_ms)
            .await
    }

    pub(crate) fn request_for<S>(
        &self,
        machine: &StrategyMachine<S>,
        client_order_id: &ClientOrderId,
    ) -> Result<ExecutionSyncRequest, ExecutionSyncError>
    where
        S: StrategyStateStore,
    {
        let state = machine.store().snapshot();
        let order = state
            .orders
            .get(client_order_id)
            .ok_or(ExecutionSyncError::UnknownStrategyOrder)?;
        let exchange_order_id = order
            .exchange_order_id
            .clone()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or(ExecutionSyncError::MissingExchangeOrderId)?;
        Ok(ExecutionSyncRequest {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            shape: order.shape.clone(),
            client_order_id: client_order_id.clone(),
            exchange_order_id,
        })
    }

    pub(crate) async fn load_snapshot<G>(
        &self,
        gateway: &G,
        request: ExecutionSyncRequest,
    ) -> Result<LoadedExecutionSnapshot, ExecutionSyncError>
    where
        G: ExecutionSnapshotGateway,
    {
        let snapshot = gateway
            .execution_snapshot(
                request.exchange,
                &request.symbol,
                &request.client_order_id,
                &request.exchange_order_id,
            )
            .await
            .map_err(|error| ExecutionSyncError::SnapshotInconclusive(error.to_string()))?;
        if !request.matches(&snapshot) {
            return Err(ExecutionSyncError::OrderIdentityMismatch);
        }
        Ok(LoadedExecutionSnapshot { request, snapshot })
    }

    pub(crate) async fn apply_loaded_snapshot<G, S>(
        &self,
        gateway: &G,
        machine: &mut StrategyMachine<S>,
        loaded: LoadedExecutionSnapshot,
        now_ms: u64,
    ) -> Result<ExecutionSyncResult, ExecutionSyncError>
    where
        G: HistoricalPriceGateway,
        S: StrategyStateStore,
    {
        let prepared = self
            .prepare_loaded_snapshot(gateway, machine, loaded)
            .await?;
        let transition =
            machine.apply_valued_execution(&prepared.snapshot, &prepared.valued_report, now_ms)?;
        Ok(ExecutionSyncResult {
            snapshot: prepared.snapshot,
            valued_report: prepared.valued_report,
            transition,
        })
    }

    pub(crate) async fn prepare_loaded_snapshot<G, S>(
        &self,
        _gateway: &G,
        machine: &StrategyMachine<S>,
        loaded: LoadedExecutionSnapshot,
    ) -> Result<PreparedExecutionSync, ExecutionSyncError>
    where
        G: HistoricalPriceGateway,
        S: StrategyStateStore,
    {
        let current_request = self.request_for(machine, &loaded.request.client_order_id)?;
        if current_request != loaded.request || !current_request.matches(&loaded.snapshot) {
            return Err(ExecutionSyncError::OrderIdentityMismatch);
        }
        let state = machine.store().snapshot();
        let order = state
            .orders
            .get(&loaded.request.client_order_id)
            .ok_or(ExecutionSyncError::UnknownStrategyOrder)?;
        if matches!(
            (&order.tracking, &loaded.snapshot.order.lifecycle),
            (
                StrategyOrderTracking::Intent {
                    state: IntentState::Terminal { .. }
                },
                OrderLifecycle::Active(_)
            )
        ) {
            return Err(ExecutionSyncError::SnapshotInconclusive(
                "execution snapshot has not caught up to the authoritative terminal order state"
                    .into(),
            ));
        }
        let snapshot = loaded.snapshot;
        let previous = order
            .execution_audit
            .as_ref()
            .map_or(&[][..], |audit| audit.fee_valuations.as_slice());
        let valued_report = self
            .accounting
            .value_snapshot_deferred(&snapshot, previous)?;
        Ok(PreparedExecutionSync {
            snapshot,
            valued_report,
        })
    }
}

impl ExecutionSyncRequest {
    fn matches(&self, snapshot: &OrderExecutionSnapshot) -> bool {
        snapshot.order.exchange == self.exchange
            && snapshot.order.shape.symbol == self.symbol
            && snapshot.order.client_order_id == self.client_order_id
            && snapshot.order.exchange_order_id == self.exchange_order_id
            && snapshot.order.shape == self.shape
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            Direction, Exchange, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            IntentState, OrderIntent, OrderSide, PositionSizingMode, QuantityRules,
            TerminalOrderStatus,
        },
        engine::{
            MarketSnapshot, MemoryStrategyStateStore, PositionBaseline, ReplacementObligationKind,
            StrategyLifecycle, StrategyOrderPurpose, StrategyState, StrategyStateStore,
            build_grid_plan,
        },
        exchange::{
            ActiveOrderStatus, AuthoritativeOrder, ExecutionSnapshotError, HistoricalMinutePrice,
            OrderExecutionSnapshot, OrderLifecycle, SnapshotError, TradeFill,
        },
        persistence::FileStrategyStateStore,
    };

    #[derive(Clone)]
    struct MockGateway {
        execution_calls: Arc<Mutex<u64>>,
        price_calls: Arc<Mutex<u64>>,
        execution: Result<OrderExecutionSnapshot, ExecutionSnapshotError>,
        historical_price: Result<HistoricalMinutePrice, SnapshotError>,
    }

    #[async_trait]
    impl ExecutionSnapshotGateway for MockGateway {
        async fn execution_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            _client_order_id: &ClientOrderId,
            _exchange_order_id: &str,
        ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
            *self.execution_calls.lock().unwrap() += 1;
            self.execution.clone()
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
            *self.price_calls.lock().unwrap() += 1;
            self.historical_price.clone()
        }
    }

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::ONE,
            max_price_significant_digits: None,
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

    fn config() -> GridConfig {
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
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn accepted_machine() -> (StrategyMachine<MemoryStrategyStateStore>, ClientOrderId) {
        let rules = rules();
        let config = config();
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
            crate::engine::StrategyRunId::parse("syncrun01").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            1_000,
        )
        .unwrap();
        let opening = state
            .orders
            .values()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap()
            .clone();
        let client_order_id = opening.client_order_id.clone();
        let intent = OrderIntent {
            client_order_id: client_order_id.clone(),
            exchange: Exchange::Binance,
            shape: opening.shape,
            state: IntentState::Accepted {
                exchange_order_id: "opening-42".into(),
            },
            created_at_ms: 1_000,
            updated_at_ms: 1_100,
        };
        let mut machine = StrategyMachine::new(MemoryStrategyStateStore::new(state));
        machine.synchronize_intent(&intent, 1_100).unwrap();
        (machine, client_order_id)
    }

    fn execution_for(
        machine: &StrategyMachine<MemoryStrategyStateStore>,
        id: &ClientOrderId,
    ) -> OrderExecutionSnapshot {
        let state = machine.store().snapshot();
        let record = state.orders.get(id).unwrap();
        let quantity = record.shape.quantity;
        let quote = quantity * Decimal::new(1014, 0);
        let fee = Decimal::new(5, 2);
        let trade = TradeFill {
            trade_id: "7".into(),
            exchange_order_id: "opening-42".into(),
            symbol: "MUUSDT".into(),
            side: OrderSide::Sell,
            price: Decimal::new(1014, 0),
            quantity,
            quote_quantity: quote,
            raw_commission: fee,
            commission_cost: fee,
            commission_asset: "USDT".into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: 1_020_001,
        };
        OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: id.clone(),
                exchange_order_id: "opening-42".into(),
                exchange: Exchange::Binance,
                shape: record.shape.clone(),
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
                executed_quantity: None,
            },
            cumulative_quantity: quantity,
            cumulative_quote: quote,
            fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
            trades: vec![trade],
            order_time_ms: 1_020_000,
            update_time_ms: 1_080_000,
        }
    }

    async fn accepted_grid_machine() -> (StrategyMachine<MemoryStrategyStateStore>, ClientOrderId) {
        let (mut machine, opening_id) = accepted_machine();
        let opening_snapshot = execution_for(&machine, &opening_id);
        ExecutionSyncService::new("USDT")
            .unwrap()
            .synchronize(
                &gateway(Ok(opening_snapshot)),
                &mut machine,
                &opening_id,
                1_150,
            )
            .await
            .unwrap();

        let grid_order = machine
            .store()
            .snapshot()
            .orders
            .values()
            .find(|order| {
                matches!(order.purpose, StrategyOrderPurpose::InitialGrid { .. })
                    && !order.shape.reduce_only
            })
            .unwrap()
            .clone();
        let client_order_id = grid_order.client_order_id.clone();
        machine
            .synchronize_intent(
                &OrderIntent {
                    client_order_id: client_order_id.clone(),
                    exchange: Exchange::Binance,
                    shape: grid_order.shape,
                    state: IntentState::Accepted {
                        exchange_order_id: "grid-partial-42".into(),
                    },
                    created_at_ms: 1_150,
                    updated_at_ms: 1_160,
                },
                1_160,
            )
            .unwrap();
        (machine, client_order_id)
    }

    fn partial_cancel_execution_for(
        machine: &StrategyMachine<MemoryStrategyStateStore>,
        id: &ClientOrderId,
    ) -> OrderExecutionSnapshot {
        let record = machine.store().snapshot().orders.get(id).unwrap();
        let quantity = Decimal::new(1, 1);
        let price = record.shape.price.unwrap();
        let quote = quantity * price;
        let fee = Decimal::new(1, 2);
        let trade = TradeFill {
            trade_id: "partial-cancel-7".into(),
            exchange_order_id: "grid-partial-42".into(),
            symbol: record.shape.symbol.clone(),
            side: record.shape.side,
            price,
            quantity,
            quote_quantity: quote,
            raw_commission: fee,
            commission_cost: fee,
            commission_asset: "USDT".into(),
            realized_profit: Decimal::ZERO,
            is_maker: true,
            trade_time_ms: 1_170,
        };
        OrderExecutionSnapshot {
            order: AuthoritativeOrder {
                client_order_id: id.clone(),
                exchange_order_id: "grid-partial-42".into(),
                exchange: Exchange::Binance,
                shape: record.shape.clone(),
                lifecycle: OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
                executed_quantity: None,
            },
            cumulative_quantity: quantity,
            cumulative_quote: quote,
            fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
            trades: vec![trade],
            order_time_ms: 1_165,
            update_time_ms: 1_180,
        }
    }

    fn gateway(execution: Result<OrderExecutionSnapshot, ExecutionSnapshotError>) -> MockGateway {
        MockGateway {
            execution_calls: Arc::new(Mutex::new(0)),
            price_calls: Arc::new(Mutex::new(0)),
            execution,
            historical_price: Err(SnapshotError::new("price lookup should not be needed")),
        }
    }

    #[tokio::test]
    async fn pending_fee_price_does_not_delay_exact_counter_obligation() {
        let (mut machine, id) = accepted_grid_machine().await;
        let mut snapshot = partial_cancel_execution_for(&machine, &id);
        snapshot.trades[0].commission_asset = "BNB".into();
        snapshot.fees_by_asset = [("BNB".into(), Decimal::new(1, 2))].into_iter().collect();
        snapshot.order_time_ms = 1_165_000;
        snapshot.update_time_ms = 1_180_000;
        snapshot.trades[0].trade_time_ms = 1_170_000;
        let gateway = gateway(Ok(snapshot.clone()));
        let service = ExecutionSyncService::new("USDT").unwrap();

        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();

        let state = machine.store().snapshot();
        assert_ne!(state.lifecycle, StrategyLifecycle::Failed);
        let audit = state.orders[&id].execution_audit.as_ref().unwrap();
        assert_eq!(audit.snapshot, snapshot);
        assert_eq!(
            serde_json::to_value(audit.fee_valuations[0].source).unwrap(),
            "pending_historical_price"
        );
        assert_eq!(*gateway.price_calls.lock().unwrap(), 0);
        let counter = state
            .replacement_obligations
            .values()
            .find(|obligation| {
                obligation.source_client_order_id == id
                    && obligation.kind == ReplacementObligationKind::Counter
            })
            .unwrap();
        assert_eq!(counter.shape.quantity, Decimal::new(1, 1));
        state.validate().unwrap();
    }

    #[tokio::test]
    async fn pending_fee_price_never_bypasses_trade_quantity_validation() {
        let (mut machine, id) = accepted_grid_machine().await;
        let mut snapshot = partial_cancel_execution_for(&machine, &id);
        snapshot.trades[0].commission_asset = "BNB".into();
        snapshot.fees_by_asset = [("BNB".into(), Decimal::new(1, 2))].into_iter().collect();
        snapshot.order_time_ms = 1_165_000;
        snapshot.update_time_ms = 1_180_000;
        snapshot.trades[0].trade_time_ms = 1_170_000;
        snapshot.cumulative_quantity += Decimal::new(1, 1);
        let gateway = gateway(Ok(snapshot));
        let service = ExecutionSyncService::new("USDT").unwrap();
        let before = machine.store().snapshot().clone();
        assert!(
            service
                .synchronize(&gateway, &mut machine, &id, 1_200)
                .await
                .is_err()
        );
        assert_eq!(machine.store().snapshot(), &before);
        assert_eq!(*gateway.price_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn exact_snapshot_and_fee_are_atomically_applied_once() {
        let (mut machine, id) = accepted_machine();
        let snapshot = execution_for(&machine, &id);
        let gateway = gateway(Ok(snapshot.clone()));
        let service = ExecutionSyncService::new("USDT").unwrap();

        let first = service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let after_first = machine.store().snapshot().clone();
        let second = service
            .synchronize(&gateway, &mut machine, &id, 1_300)
            .await
            .unwrap();
        let after_second = machine.store().snapshot();

        assert_eq!(
            first.valued_report.report.cumulative_fee,
            Decimal::new(5, 2)
        );
        assert_eq!(after_first.total_fee, Decimal::new(5, 2));
        assert_eq!(
            after_first.grid_position_net_quantity,
            -snapshot.cumulative_quantity
        );
        let audit = after_first
            .orders
            .get(&id)
            .and_then(|order| order.execution_audit.as_ref())
            .expect("execution evidence must be embedded in strategy state");
        assert_eq!(audit.snapshot, snapshot);
        assert_eq!(audit.fee_valuations, first.valued_report.fee_valuations);
        assert_eq!(audit.synced_at_ms, 1_200);
        assert_eq!(after_second.total_fee, after_first.total_fee);
        assert_eq!(
            after_second.grid_position_net_quantity,
            after_first.grid_position_net_quantity
        );
        assert_eq!(second.transition, StrategyTransition::NoChange);
        assert_eq!(*gateway.execution_calls.lock().unwrap(), 2);
        assert_eq!(*gateway.price_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn partial_terminal_sync_creates_exact_counter_and_remainder_only_once() {
        let (mut machine, id) = accepted_grid_machine().await;
        let snapshot = partial_cancel_execution_for(&machine, &id);
        let original_quantity = snapshot.order.shape.quantity;
        let filled_quantity = snapshot.cumulative_quantity;
        let gateway = gateway(Ok(snapshot));
        let service = ExecutionSyncService::new("USDT").unwrap();

        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let after_first = machine.store().snapshot().clone();
        let duplicate = service
            .synchronize(&gateway, &mut machine, &id, 1_300)
            .await
            .unwrap();
        let obligations = machine
            .store()
            .snapshot()
            .replacement_obligations
            .values()
            .filter(|obligation| obligation.source_client_order_id == id)
            .collect::<Vec<_>>();

        assert_eq!(obligations.len(), 2);
        assert!(obligations.iter().any(|obligation| {
            obligation.kind == ReplacementObligationKind::Counter
                && obligation.shape.quantity == filled_quantity
        }));
        assert!(obligations.iter().any(|obligation| {
            obligation.kind == ReplacementObligationKind::RestoreCancelledRemainder
                && obligation.shape.quantity == original_quantity - filled_quantity
        }));
        assert_eq!(
            obligations
                .iter()
                .map(|obligation| obligation.shape.quantity)
                .sum::<Decimal>(),
            original_quantity
        );
        assert_eq!(duplicate.transition, StrategyTransition::NoChange);
        assert_eq!(machine.store().snapshot().total_fee, Decimal::new(6, 2));
        assert_eq!(
            machine.store().snapshot().replacement_obligations,
            after_first.replacement_obligations
        );
    }

    #[tokio::test]
    async fn state_write_failure_leaves_old_state_and_retry_is_exact() {
        let (mut machine, id) = accepted_machine();
        let snapshot = execution_for(&machine, &id);
        let gateway = gateway(Ok(snapshot.clone()));
        let service = ExecutionSyncService::new("USDT").unwrap();
        let before = machine.store().snapshot().clone();
        machine.store_mut().fail_next_write();

        assert!(matches!(
            service
                .synchronize(&gateway, &mut machine, &id, 1_200)
                .await,
            Err(ExecutionSyncError::StateMachine(_))
        ));
        assert_eq!(machine.store().snapshot(), &before);

        service
            .synchronize(&gateway, &mut machine, &id, 1_300)
            .await
            .unwrap();
        assert_eq!(machine.store().snapshot().total_fee, Decimal::new(5, 2));
        assert_eq!(
            machine.store().snapshot().grid_position_net_quantity,
            -snapshot.cumulative_quantity
        );
    }

    #[tokio::test]
    async fn inconclusive_or_foreign_snapshot_never_writes_strategy_state() {
        let (mut machine, id) = accepted_machine();
        let before = machine.store().snapshot().clone();
        let unavailable = gateway(Err(ExecutionSnapshotError::new("network timeout")));
        let service = ExecutionSyncService::new("USDT").unwrap();

        assert!(matches!(
            service
                .synchronize(&unavailable, &mut machine, &id, 1_200)
                .await,
            Err(ExecutionSyncError::SnapshotInconclusive(_))
        ));
        assert_eq!(machine.store().snapshot(), &before);

        let mut foreign = execution_for(&machine, &id);
        foreign.order.shape.quantity += Decimal::new(1, 1);
        let foreign_gateway = gateway(Ok(foreign));
        assert!(matches!(
            service
                .synchronize(&foreign_gateway, &mut machine, &id, 1_300)
                .await,
            Err(ExecutionSyncError::OrderIdentityMismatch)
        ));
        assert_eq!(machine.store().snapshot(), &before);
    }

    #[tokio::test]
    async fn unavailable_non_quote_fee_price_is_retried_without_rolling_back_execution() {
        let (mut machine, id) = accepted_machine();
        let mut snapshot = execution_for(&machine, &id);
        let fee = snapshot.trades[0].commission_cost;
        snapshot.trades[0].commission_asset = "BNB".into();
        snapshot.fees_by_asset = [("BNB".into(), fee)].into_iter().collect();
        let gateway = gateway(Ok(snapshot.clone()));
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let before_retry = machine.store().snapshot().clone();
        assert_eq!(before_retry.pending_fee_valuation_count(), 1);
        assert_eq!(
            before_retry.orders[&id].cumulative_quantity,
            snapshot.cumulative_quantity
        );
        let pending = PendingFeeValuation {
            gateway: gateway.clone(),
            service,
            snapshot,
            fee_valuations: machine.store().snapshot().orders[&id]
                .execution_audit
                .as_ref()
                .unwrap()
                .fee_valuations
                .clone(),
        };
        assert!(matches!(
            pending.resolve().await,
            Err(ExecutionSyncError::Accounting(
                ExecutionAccountingError::HistoricalPriceUnavailable(_)
            ))
        ));
        assert_eq!(machine.store().snapshot(), &before_retry);
        assert_eq!(*gateway.execution_calls.lock().unwrap(), 1);
        assert_eq!(*gateway.price_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn pending_fee_settlement_is_durable_idempotent_and_does_not_replay_inventory() {
        let (mut machine, id) = accepted_grid_machine().await;
        let mut snapshot = partial_cancel_execution_for(&machine, &id);
        snapshot.trades[0].commission_asset = "BNB".into();
        snapshot.fees_by_asset = [("BNB".into(), Decimal::new(1, 2))].into_iter().collect();
        snapshot.order_time_ms = 1_165_000;
        snapshot.update_time_ms = 1_180_000;
        snapshot.trades[0].trade_time_ms = 1_170_000;
        let mut gateway = gateway(Ok(snapshot.clone()));
        gateway.historical_price = Ok(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_140_000,
            open_price: Decimal::from(600),
        });
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let directory = tempdir().unwrap();
        let path = directory.path().join("pending.json");
        FileStrategyStateStore::create(&path, machine.store().snapshot().clone()).unwrap();
        let mut reloaded = StrategyMachine::new(FileStrategyStateStore::load(&path).unwrap());
        let before = reloaded.store().snapshot().clone();
        let resolved = PendingFeeValuation {
            gateway: gateway.clone(),
            service: service.clone(),
            snapshot,
            fee_valuations: machine.store().snapshot().orders[&id]
                .execution_audit
                .as_ref()
                .unwrap()
                .fee_valuations
                .clone(),
        }
        .resolve()
        .await
        .unwrap();
        let mut forged = resolved.fee_valuations.clone();
        forged[0].fee_amount += Decimal::ONE;
        assert!(reloaded.settle_fee_valuations(&id, &forged, 1_300).is_err());
        assert_eq!(reloaded.store().snapshot(), &before);
        reloaded
            .settle_fee_valuations(&id, &resolved.fee_valuations, 1_300)
            .unwrap();
        let after = reloaded.store().snapshot().clone();
        assert_eq!(after.pending_fee_valuation_count(), 0);
        assert_eq!(after.total_fee, before.total_fee + Decimal::from(6));
        assert_eq!(after.inventory_events, before.inventory_events);
        assert_eq!(
            after.replacement_obligations,
            before.replacement_obligations
        );
        assert_eq!(
            after.grid_position_net_quantity,
            before.grid_position_net_quantity
        );
        assert_eq!(after.completed_pairs, before.completed_pairs);
        assert_eq!(
            reloaded
                .settle_fee_valuations(&id, &resolved.fee_valuations, 1_400)
                .unwrap(),
            StrategyTransition::NoChange
        );
        assert_eq!(reloaded.store().snapshot(), &after);
        let duplicate = service
            .synchronize(&gateway, &mut reloaded, &id, 1_500)
            .await
            .unwrap();
        assert_eq!(duplicate.transition, StrategyTransition::NoChange);
        assert_eq!(reloaded.store().snapshot(), &after);
        assert_eq!(*gateway.price_calls.lock().unwrap(), 1);
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            &after
        );
    }

    #[tokio::test]
    async fn pending_fee_resolution_does_not_reprice_a_settled_prefix_after_restart() {
        let (mut machine, id) = accepted_machine();
        let mut full = execution_for(&machine, &id);
        let mut first_trade = full.trades[0].clone();
        first_trade.quantity /= Decimal::from(2);
        first_trade.quote_quantity /= Decimal::from(2);
        first_trade.commission_asset = "BNB".into();
        first_trade.commission_cost = Decimal::new(2, 2);
        first_trade.raw_commission = first_trade.commission_cost;
        let mut second_trade = first_trade.clone();
        second_trade.trade_id = "8".into();
        second_trade.trade_time_ms = 1_080_001;
        second_trade.commission_cost = Decimal::new(3, 2);
        second_trade.raw_commission = second_trade.commission_cost;
        full.trades = vec![first_trade.clone(), second_trade];
        full.update_time_ms = 1_140_000;
        full.fees_by_asset = [("BNB".into(), Decimal::new(5, 2))].into_iter().collect();
        let mut partial = full.clone();
        partial.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
        partial.cumulative_quantity = first_trade.quantity;
        partial.cumulative_quote = first_trade.quote_quantity;
        partial.trades = vec![first_trade];
        partial.fees_by_asset = [("BNB".into(), Decimal::new(2, 2))].into_iter().collect();
        partial.update_time_ms = 1_050_000;
        let mut gateway = gateway(Ok(full.clone()));
        gateway.historical_price = Ok(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::from(600),
        });
        let old_service = ExecutionSyncService::new("USDT").unwrap();
        let valued = old_service
            .accounting
            .value_snapshot(&gateway, &partial)
            .await
            .unwrap();
        machine
            .apply_valued_execution(&partial, &valued, 1_200)
            .unwrap();
        let service = ExecutionSyncService::new("USDT").unwrap();
        gateway.historical_price = Ok(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_080_000,
            open_price: Decimal::from(700),
        });
        service
            .synchronize(&gateway, &mut machine, &id, 1_300)
            .await
            .unwrap();
        let work = PendingFeeValuation {
            gateway: gateway.clone(),
            service,
            snapshot: full,
            fee_valuations: machine.store().snapshot().orders[&id]
                .execution_audit
                .as_ref()
                .unwrap()
                .fee_valuations
                .clone(),
        };
        let resolved = work.resolve().await.unwrap();
        assert_eq!(resolved.fee_valuations[0], valued.fee_valuations[0]);
        assert_eq!(resolved.fee_valuations[1].quote_value, Decimal::from(21));
        machine
            .settle_fee_valuations(&id, &resolved.fee_valuations, 1_400)
            .unwrap();
        assert_eq!(machine.store().snapshot().total_fee, Decimal::from(33));
        assert_eq!(*gateway.price_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn pending_fee_settlement_preserves_terminal_reconciliation_in_flight() {
        let (mut machine, id) = accepted_grid_machine().await;
        let mut snapshot = partial_cancel_execution_for(&machine, &id);
        snapshot.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
        snapshot.trades[0].commission_asset = "BNB".into();
        snapshot.fees_by_asset = [("BNB".into(), Decimal::new(1, 2))].into_iter().collect();
        snapshot.order_time_ms = 1_165_000;
        snapshot.update_time_ms = 1_180_000;
        snapshot.trades[0].trade_time_ms = 1_170_000;
        let mut gateway = gateway(Ok(snapshot.clone()));
        gateway.historical_price = Ok(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_140_000,
            open_price: Decimal::from(600),
        });
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let pending = PendingFeeValuation {
            gateway,
            service,
            snapshot,
            fee_valuations: machine.store().snapshot().orders[&id]
                .execution_audit
                .as_ref()
                .unwrap()
                .fee_valuations
                .clone(),
        };
        let record = machine.store().snapshot().orders[&id].clone();
        machine
            .synchronize_intent(
                &OrderIntent {
                    client_order_id: id.clone(),
                    exchange: Exchange::Binance,
                    shape: record.shape,
                    state: IntentState::Terminal {
                        status: TerminalOrderStatus::Cancelled,
                        exchange_order_id: record.exchange_order_id,
                    },
                    created_at_ms: 1_150,
                    updated_at_ms: 1_250,
                },
                1_250,
            )
            .unwrap();
        let before = machine.store().snapshot().clone();
        let resolved = pending.resolve().await.unwrap();
        let transition = machine
            .settle_fee_valuations(&id, &resolved.fee_valuations, 1_300)
            .unwrap();
        assert!(!matches!(transition, StrategyTransition::Failed { .. }));
        let after = machine.store().snapshot();
        assert_eq!(after.lifecycle, before.lifecycle);
        assert_eq!(after.orders[&id].tracking, before.orders[&id].tracking);
        assert_eq!(
            after.orders[&id].terminal_processed,
            before.orders[&id].terminal_processed
        );
        assert_eq!(after.inventory_events, before.inventory_events);
        assert_eq!(
            after.replacement_obligations,
            before.replacement_obligations
        );
        assert_eq!(after.total_fee, before.total_fee + Decimal::from(6));
    }

    #[tokio::test]
    async fn pending_fee_settlement_merges_old_work_without_regressing_new_fills() {
        let (mut machine, id) = accepted_machine();
        let mut full = execution_for(&machine, &id);
        let mut first_trade = full.trades[0].clone();
        first_trade.quantity /= Decimal::from(2);
        first_trade.quote_quantity /= Decimal::from(2);
        first_trade.commission_asset = "BNB".into();
        first_trade.commission_cost = Decimal::new(2, 2);
        first_trade.raw_commission = first_trade.commission_cost;
        let mut second_trade = first_trade.clone();
        second_trade.trade_id = "8".into();
        second_trade.trade_time_ms += 1;
        second_trade.commission_cost = Decimal::new(3, 2);
        second_trade.raw_commission = second_trade.commission_cost;
        full.trades = vec![first_trade.clone(), second_trade];
        full.fees_by_asset = [("BNB".into(), Decimal::new(5, 2))].into_iter().collect();
        let mut partial = full.clone();
        partial.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
        partial.cumulative_quantity = first_trade.quantity;
        partial.cumulative_quote = first_trade.quote_quantity;
        partial.trades = vec![first_trade];
        partial.fees_by_asset = [("BNB".into(), Decimal::new(2, 2))].into_iter().collect();
        let mut gateway = gateway(Ok(partial.clone()));
        gateway.historical_price = Ok(HistoricalMinutePrice {
            exchange: Exchange::Binance,
            symbol: "BNBUSDT".into(),
            minute_start_ms: 1_020_000,
            open_price: Decimal::from(600),
        });
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let work = PendingFeeValuation {
            gateway: gateway.clone(),
            service: service.clone(),
            snapshot: partial,
            fee_valuations: machine.store().snapshot().orders[&id]
                .execution_audit
                .as_ref()
                .unwrap()
                .fee_valuations
                .clone(),
        };
        gateway.execution = Ok(full);
        service
            .synchronize(&gateway, &mut machine, &id, 1_300)
            .await
            .unwrap();
        let before = machine.store().snapshot().clone();
        let resolved = work.resolve().await.unwrap();
        machine
            .settle_fee_valuations(&id, &resolved.fee_valuations, 1_400)
            .unwrap();
        let after = machine.store().snapshot();
        assert_eq!(after.pending_fee_valuation_count(), 1);
        assert_eq!(after.total_fee, Decimal::from(12));
        assert_eq!(after.inventory_events, before.inventory_events);
        assert_eq!(
            after.replacement_obligations,
            before.replacement_obligations
        );
        assert_eq!(
            after.orders[&id].cumulative_quantity,
            before.orders[&id].cumulative_quantity
        );
        after.validate().unwrap();
    }

    #[tokio::test]
    async fn execution_audit_round_trips_with_the_atomic_strategy_file() {
        let (mut machine, id) = accepted_machine();
        let snapshot = execution_for(&machine, &id);
        let gateway = gateway(Ok(snapshot));
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway, &mut machine, &id, 1_200)
            .await
            .unwrap();
        let expected = machine.store().snapshot().clone();
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy-with-audit.json");

        FileStrategyStateStore::create(&path, expected.clone()).unwrap();
        let restored = FileStrategyStateStore::load(&path).unwrap();

        assert_eq!(restored.snapshot(), &expected);
        assert!(
            restored
                .snapshot()
                .orders
                .get(&id)
                .and_then(|order| order.execution_audit.as_ref())
                .is_some()
        );
    }

    #[tokio::test]
    async fn previously_audited_trade_evidence_can_never_be_rewritten() {
        let (mut machine, id) = accepted_machine();
        let snapshot = execution_for(&machine, &id);
        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway(Ok(snapshot.clone())), &mut machine, &id, 1_200)
            .await
            .unwrap();
        let owned_quantity = machine.store().snapshot().grid_position_net_quantity;
        let mut rewritten = snapshot;
        rewritten.trades[0].price += Decimal::ONE;

        let result = service
            .synchronize(&gateway(Ok(rewritten)), &mut machine, &id, 1_300)
            .await
            .unwrap();

        assert!(matches!(
            result.transition,
            StrategyTransition::Failed { .. }
        ));
        assert_eq!(
            machine.store().snapshot().lifecycle,
            StrategyLifecycle::Failed
        );
        assert_eq!(
            machine.store().snapshot().grid_position_net_quantity,
            owned_quantity
        );
        assert_eq!(
            machine
                .store()
                .snapshot()
                .orders
                .get(&id)
                .unwrap()
                .execution_audit
                .as_ref()
                .unwrap()
                .snapshot
                .trades[0]
                .price,
            Decimal::new(1014, 0)
        );
    }

    #[tokio::test]
    async fn partial_execution_audit_extends_by_new_trade_id_without_recounting() {
        let (mut machine, id) = accepted_machine();
        let mut complete = execution_for(&machine, &id);
        let total_quantity = complete.cumulative_quantity;
        let total_quote = complete.cumulative_quote;
        let first_quantity = total_quantity / Decimal::new(2, 0);
        let first_quote = total_quote / Decimal::new(2, 0);
        let first_fee = Decimal::new(2, 2);
        let second_fee = Decimal::new(3, 2);
        let mut first_trade = complete.trades[0].clone();
        first_trade.quantity = first_quantity;
        first_trade.quote_quantity = first_quote;
        first_trade.raw_commission = first_fee;
        first_trade.commission_cost = first_fee;
        let mut partial = complete.clone();
        partial.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
        partial.cumulative_quantity = first_quantity;
        partial.cumulative_quote = first_quote;
        partial.fees_by_asset = [("USDT".into(), first_fee)].into_iter().collect();
        partial.trades = vec![first_trade.clone()];
        partial.update_time_ms = 1_050_000;

        let mut second_trade = complete.trades[0].clone();
        second_trade.trade_id = "8".into();
        second_trade.quantity = total_quantity - first_quantity;
        second_trade.quote_quantity = total_quote - first_quote;
        second_trade.raw_commission = second_fee;
        second_trade.commission_cost = second_fee;
        second_trade.trade_time_ms = 1_060_000;
        complete.trades = vec![first_trade, second_trade];
        complete.fees_by_asset = [("USDT".into(), first_fee + second_fee)]
            .into_iter()
            .collect();

        let service = ExecutionSyncService::new("USDT").unwrap();
        service
            .synchronize(&gateway(Ok(partial)), &mut machine, &id, 1_200)
            .await
            .unwrap();
        let after_partial = machine.store().snapshot().clone();
        service
            .synchronize(&gateway(Ok(complete)), &mut machine, &id, 1_300)
            .await
            .unwrap();
        let after_complete = machine.store().snapshot().clone();
        let audit = after_complete
            .orders
            .get(&id)
            .unwrap()
            .execution_audit
            .as_ref()
            .unwrap();

        assert_eq!(after_partial.grid_position_net_quantity, -first_quantity);
        assert_eq!(after_complete.grid_position_net_quantity, -total_quantity);
        assert_eq!(after_complete.total_fee, first_fee + second_fee);
        assert_eq!(audit.snapshot.trades.len(), 2);
        assert_eq!(audit.snapshot.trades[0].trade_id, "7");
        assert_eq!(audit.snapshot.trades[1].trade_id, "8");
        assert_eq!(
            after_partial
                .inventory_events
                .values()
                .filter_map(|event| event.exchange_trade_id.as_deref())
                .collect::<Vec<_>>(),
            vec!["7"]
        );
        assert_eq!(
            after_complete
                .inventory_events
                .values()
                .filter_map(|event| event.exchange_trade_id.as_deref())
                .collect::<Vec<_>>(),
            vec!["7", "8"]
        );

        let repeated = service
            .synchronize(
                &gateway(Ok(after_complete
                    .orders
                    .get(&id)
                    .unwrap()
                    .execution_audit
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .clone())),
                &mut machine,
                &id,
                1_400,
            )
            .await
            .unwrap();
        assert_eq!(repeated.transition, StrategyTransition::NoChange);
        assert_eq!(machine.store().snapshot(), &after_complete);
    }
}
