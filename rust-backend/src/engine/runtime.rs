use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::ClientOrderId,
    engine::{
        ExecutionAccountingError, ExecutionSyncService, ReconciliationError, ReconciliationResult,
        StrategyMachine, StrategyMachineError, StrategyOrderTracking, StrategyStateError,
        StrategyStateStore, StrategyTransition, SubmissionError, SubmissionResult,
        load_strategy_inputs, reconcile_with, submit_with,
    },
    exchange::{
        ExecutionSnapshotGateway, HistoricalPriceGateway, InstrumentRulesGateway,
        MarketSnapshotGateway, OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway,
    },
    persistence::IntentStore,
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
pub struct RuntimeTickReport {
    pub ledger_reconciliations: usize,
    pub execution_syncs: usize,
    pub submissions: Vec<RuntimeSubmission>,
    pub blockers: Vec<RuntimeBlocker>,
}

impl RuntimeTickReport {
    fn new() -> Self {
        Self {
            ledger_reconciliations: 0,
            execution_syncs: 0,
            submissions: Vec::new(),
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
        if maximum_market_age_ms == 0 {
            return Err(RuntimeBuildError::InvalidFreshnessWindow);
        }
        if maximum_submissions_per_tick == 0 {
            return Err(RuntimeBuildError::InvalidSubmissionLimit);
        }
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
        Ok(())
    }
}

impl<G, I, S> StrategyRuntime<G, I, S>
where
    G: OrderPlacementGateway
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
        if lifecycle == crate::engine::StrategyLifecycle::Failed {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::StrategyFailed,
                client_order_id: None,
                message: "strategy is failed".into(),
            });
            return Ok(report);
        }
        if lifecycle == crate::engine::StrategyLifecycle::StopRequested {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::Stop,
                client_order_id: None,
                message: "stop cancellation workflow is not yet complete".into(),
            });
            return Ok(report);
        }
        if lifecycle == crate::engine::StrategyLifecycle::RiskExitRequested {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::RiskExit,
                client_order_id: None,
                message: "risk-exit cancellation workflow is not yet complete".into(),
            });
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
                message: "exchange instrument rules changed".into(),
            });
            return Ok(report);
        }
        let position_transition = self
            .machine
            .reconcile_position(inputs.baseline.signed_quantity, now_ms)?;
        if matches!(position_transition, StrategyTransition::Failed { .. }) {
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: "exchange position differs from the owned strategy ledger".into(),
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
            report.blockers.push(RuntimeBlocker {
                stage: RuntimeStage::RiskExit,
                client_order_id: None,
                message: "configured risk price triggered; cancellation is required".into(),
            });
            return Ok(report);
        }
        self.machine
            .materialize_replacements(&inputs.instrument_rules, now_ms)?;

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
            ActiveOrderStatus, AuthoritativeOrder, ExchangeMarketSnapshot, ExecutionSnapshotError,
            HistoricalMinutePrice, LookupError, OrderExecutionSnapshot, OrderLifecycle,
            OrderLookup, PlacementAcknowledgement, PlacementError, PositionLeg, PositionSide,
            PositionSnapshot, SnapshotError, TradeFill,
        },
        persistence::{IntentStore, MemoryOrderIntentStore},
    };

    #[derive(Clone)]
    struct MockGateway {
        state: Arc<Mutex<MockGatewayState>>,
    }

    struct MockGatewayState {
        placement_calls: Vec<OrderIntent>,
        next_placement_error: Option<PlacementError>,
        orders: BTreeMap<ClientOrderId, AuthoritativeOrder>,
        executions: BTreeMap<ClientOrderId, OrderExecutionSnapshot>,
        market: ExchangeMarketSnapshot,
        rules: InstrumentRules,
        position_quantity: Decimal,
        position_entry_price: Option<Decimal>,
    }

    impl MockGateway {
        fn new(rules: InstrumentRules, observed_at_ms: u64) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockGatewayState {
                    placement_calls: Vec::new(),
                    next_placement_error: None,
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
            let order = state
                .orders
                .get_mut(client_order_id)
                .expect("order must have been placed");
            order.lifecycle = OrderLifecycle::Terminal(TerminalOrderStatus::Filled);
            let order = order.clone();
            let quantity = order.shape.quantity;
            let quote_quantity = quantity * price;
            let exchange_order_id = order.exchange_order_id.clone();
            let trade = TradeFill {
                trade_id: 1,
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
                trade_time_ms: 1_150,
            };
            state.executions.insert(
                client_order_id.clone(),
                OrderExecutionSnapshot {
                    order,
                    cumulative_quantity: quantity,
                    cumulative_quote: quote_quantity,
                    fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
                    trades: vec![trade],
                    order_time_ms: 1_100,
                    update_time_ms: 1_150,
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
            let order = state
                .orders
                .get_mut(client_order_id)
                .expect("order must have been placed");
            assert!(quantity > Decimal::ZERO && quantity < order.shape.quantity);
            order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
            let order = order.clone();
            let quote_quantity = quantity * price;
            let trade = TradeFill {
                trade_id: 1,
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
                trade_time_ms: 1_150,
            };
            state.executions.insert(
                client_order_id.clone(),
                OrderExecutionSnapshot {
                    order,
                    cumulative_quantity: quantity,
                    cumulative_quote: quote_quantity,
                    fees_by_asset: [("USDT".into(), fee)].into_iter().collect(),
                    trades: vec![trade],
                    order_time_ms: 1_100,
                    update_time_ms: 1_150,
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
            Ok(self.state.lock().unwrap().market.clone())
        }
    }

    #[async_trait]
    impl InstrumentRulesGateway for MockGateway {
        async fn instrument_rules(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<InstrumentRules, SnapshotError> {
            Ok(self.state.lock().unwrap().rules.clone())
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for MockGateway {
        async fn position_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            let state = self.state.lock().unwrap();
            Ok(PositionSnapshot {
                exchange: Exchange::Binance,
                symbol: "MUUSDT".into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: state.position_quantity,
                    entry_price: state.position_entry_price,
                    mark_price: state.market.mark_price,
                    unrealized_profit: Decimal::ZERO,
                }],
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
    async fn authoritative_position_mismatch_fails_before_any_exchange_write() {
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
            StrategyLifecycle::Failed
        );
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
    async fn stop_request_blocks_all_new_orders() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        let mut machine = machine(config(None), &rules);
        machine.request_stop(1_050).unwrap();
        let mut runtime = runtime(gateway.clone(), MemoryOrderIntentStore::default(), machine);

        let report = runtime.tick(1_100).await.unwrap();

        assert_eq!(report.blockers[0].stage, RuntimeStage::Stop);
        assert_eq!(gateway.placement_call_count(), 0);
    }

    #[tokio::test]
    async fn configured_risk_trigger_blocks_all_new_orders() {
        let rules = rules();
        let gateway = MockGateway::new(rules.clone(), 1_100);
        gateway.set_market_price(Decimal::new(1022, 0), 1_100);
        let mut runtime = runtime(
            gateway.clone(),
            MemoryOrderIntentStore::default(),
            machine(config(Some(Decimal::new(1021, 0))), &rules),
        );

        let report = runtime.tick(1_100).await.unwrap();

        assert_eq!(report.blockers[0].stage, RuntimeStage::RiskExit);
        assert_eq!(gateway.placement_call_count(), 0);
        assert_eq!(
            runtime.machine().store().snapshot().lifecycle,
            StrategyLifecycle::RiskExitRequested
        );
    }
}
