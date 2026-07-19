use std::time::Instant;

use thiserror::Error;

use crate::{
    domain::{Exchange, GridConfig},
    exchange::{
        InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway, PositionSnapshotGateway,
        SnapshotError, TradingFeeRateGateway,
    },
};

use super::exchange_inputs::advance_reference_time_ms;
use super::{
    ArmedStrategyError, ArmedStrategyState, FeeRateConfigError, GridPlanError, MarketSnapshot,
    StrategyInputError, StrategyRunId, StrategyState, StrategyStateError, build_grid_plan,
    ensure_symbol_leverage, load_authoritative_fee_config, load_strategy_inputs,
};

#[derive(Debug, Clone, PartialEq)]
pub enum PreparedStrategy {
    Armed(Box<ArmedStrategyState>),
    Active(Box<StrategyState>),
}

pub async fn prepare_new_strategy<G>(
    gateway: &G,
    run_id: StrategyRunId,
    config: GridConfig,
    now_ms: u64,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
) -> Result<PreparedStrategy, StrategyBootstrapError>
where
    G: TradingFeeRateGateway
        + LeverageGateway
        + PositionSnapshotGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway,
{
    config
        .validate()
        .map_err(|error| StrategyBootstrapError::InvalidConfig(error.to_string()))?;
    let exchange = config
        .exchange
        .ok_or(StrategyBootstrapError::MissingExchange)?;
    if config.trigger_price.is_some() {
        let market = load_fresh_market(
            gateway,
            exchange,
            &config.symbol,
            now_ms,
            maximum_market_age_ms,
            maximum_future_skew_ms,
        )
        .await?;
        return Ok(PreparedStrategy::Armed(Box::new(ArmedStrategyState::new(
            run_id, config, &market, now_ms,
        )?)));
    }

    prepare_active(
        gateway,
        run_id,
        &config,
        now_ms,
        maximum_market_age_ms,
        maximum_future_skew_ms,
        None,
    )
    .await
    .map(|state| PreparedStrategy::Active(Box::new(state)))
}

pub async fn activate_armed_strategy<G>(
    gateway: &G,
    armed: &ArmedStrategyState,
    now_ms: u64,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
) -> Result<StrategyState, StrategyBootstrapError>
where
    G: TradingFeeRateGateway
        + LeverageGateway
        + PositionSnapshotGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway,
{
    armed.validate()?;
    let market = load_fresh_market(
        gateway,
        armed.exchange,
        &armed.symbol,
        now_ms,
        maximum_market_age_ms,
        maximum_future_skew_ms,
    )
    .await?;
    if !armed.is_triggered(&market)? {
        return Err(StrategyBootstrapError::TriggerNotReached);
    }
    prepare_active(
        gateway,
        armed.run_id.clone(),
        &armed.config,
        now_ms,
        maximum_market_age_ms,
        maximum_future_skew_ms,
        Some(armed),
    )
    .await
}

async fn prepare_active<G>(
    gateway: &G,
    run_id: StrategyRunId,
    requested_config: &GridConfig,
    now_ms: u64,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
    armed: Option<&ArmedStrategyState>,
) -> Result<StrategyState, StrategyBootstrapError>
where
    G: TradingFeeRateGateway
        + LeverageGateway
        + PositionSnapshotGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway,
{
    let bootstrap_started = Instant::now();
    let exchange = requested_config
        .exchange
        .ok_or(StrategyBootstrapError::MissingExchange)?;
    let authoritative = load_authoritative_fee_config(gateway, requested_config).await?;
    ensure_symbol_leverage(
        gateway,
        exchange,
        &requested_config.symbol,
        requested_config.leverage,
    )
    .await?;
    let inputs = load_strategy_inputs(
        gateway,
        exchange,
        &requested_config.symbol,
        advance_reference_time_ms(now_ms, bootstrap_started.elapsed()),
        maximum_market_age_ms,
        maximum_future_skew_ms,
    )
    .await?;

    if let Some(armed) = armed {
        if !armed.is_triggered(&inputs.market)? {
            return Err(StrategyBootstrapError::TriggerNoLongerReached);
        }
        return armed
            .activate_with_config(
                authoritative.config,
                &inputs.market,
                inputs.instrument_rules,
                inputs.position,
                now_ms,
            )
            .map_err(StrategyBootstrapError::from);
    }

    let plan = build_grid_plan(
        &authoritative.config,
        &inputs.market,
        &inputs.instrument_rules,
    )?;
    StrategyState::from_plan(
        run_id,
        authoritative.config,
        inputs.instrument_rules,
        plan,
        inputs.position,
        now_ms,
    )
    .map_err(StrategyBootstrapError::from)
}

async fn load_fresh_market<G>(
    gateway: &G,
    exchange: Exchange,
    symbol: &str,
    now_ms: u64,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
) -> Result<MarketSnapshot, StrategyBootstrapError>
where
    G: MarketSnapshotGateway,
{
    if maximum_market_age_ms == 0 {
        return Err(StrategyBootstrapError::InvalidFreshnessWindow);
    }
    let symbol = symbol.to_ascii_uppercase();
    let market = gateway.market_snapshot(exchange, &symbol).await?;
    if market.exchange != exchange || market.symbol != symbol {
        return Err(StrategyBootstrapError::MarketIdentityMismatch);
    }
    market.ensure_fresh(now_ms, maximum_market_age_ms, maximum_future_skew_ms)?;
    Ok(MarketSnapshot {
        last_price: market.last_price,
        mark_price: market.mark_price,
    })
}

#[derive(Debug, Error)]
pub enum StrategyBootstrapError {
    #[error("grid configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("grid configuration must identify an exchange")]
    MissingExchange,
    #[error("market freshness window must be positive")]
    InvalidFreshnessWindow,
    #[error("market snapshot identity does not match the requested strategy")]
    MarketIdentityMismatch,
    #[error("trigger price has not been reached")]
    TriggerNotReached,
    #[error("trigger condition was no longer true after account preflight")]
    TriggerNoLongerReached,
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    FeeRates(#[from] FeeRateConfigError),
    #[error(transparent)]
    Leverage(#[from] super::LeveragePreflightError),
    #[error(transparent)]
    Inputs(#[from] StrategyInputError),
    #[error(transparent)]
    Armed(#[from] ArmedStrategyError),
    #[error(transparent)]
    Plan(#[from] GridPlanError),
    #[error(transparent)]
    State(#[from] StrategyStateError),
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
            Direction, GridMode, InitialOrderType, InstrumentRules, PositionSizingMode,
            QuantityRules,
        },
        engine::StrategyLifecycle,
        exchange::{
            ExchangeMarketSnapshot, LeverageAcknowledgement, LeverageError, PositionLeg,
            PositionSide, PositionSnapshot, TradingFeeRates,
        },
    };

    #[derive(Default)]
    struct Calls {
        market: usize,
        rules: usize,
        position: usize,
        fees: usize,
        leverage_writes: usize,
    }

    #[derive(Clone)]
    struct FakeGateway {
        state: Arc<Mutex<FakeState>>,
    }

    struct FakeState {
        market_price: Decimal,
        market_sequence: VecDeque<Decimal>,
        leverage: u16,
        baseline_quantity: Decimal,
        fee_error: Option<SnapshotError>,
        calls: Calls,
    }

    impl FakeGateway {
        fn new(market_price: Decimal, leverage: u16, baseline_quantity: Decimal) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    market_price,
                    market_sequence: VecDeque::new(),
                    leverage,
                    baseline_quantity,
                    fee_error: None,
                    calls: Calls::default(),
                })),
            }
        }

        fn queue_market_prices(&self, prices: impl IntoIterator<Item = Decimal>) {
            self.state.lock().unwrap().market_sequence.extend(prices);
        }

        fn fail_fee_rates(&self, message: &str) {
            self.state.lock().unwrap().fee_error = Some(SnapshotError::new(message));
        }
    }

    #[async_trait]
    impl MarketSnapshotGateway for FakeGateway {
        async fn market_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.calls.market += 1;
            if let Some(price) = state.market_sequence.pop_front() {
                state.market_price = price;
            }
            Ok(ExchangeMarketSnapshot {
                exchange,
                symbol: symbol.into(),
                last_price: state.market_price,
                mark_price: state.market_price,
                observed_at_ms: 10_000,
            })
        }
    }

    #[async_trait]
    impl InstrumentRulesGateway for FakeGateway {
        async fn instrument_rules(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<InstrumentRules, SnapshotError> {
            self.state.lock().unwrap().calls.rules += 1;
            Ok(rules())
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for FakeGateway {
        async fn position_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.calls.position += 1;
            Ok(PositionSnapshot {
                exchange,
                symbol: symbol.into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: state.baseline_quantity,
                    entry_price: (!state.baseline_quantity.is_zero())
                        .then_some(Decimal::new(1016, 0)),
                    mark_price: state.market_price,
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(state.leverage),
                }],
            })
        }
    }

    #[async_trait]
    impl TradingFeeRateGateway for FakeGateway {
        async fn trading_fee_rates(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<TradingFeeRates, SnapshotError> {
            let mut state = self.state.lock().unwrap();
            state.calls.fees += 1;
            if let Some(error) = &state.fee_error {
                return Err(error.clone());
            }
            Ok(TradingFeeRates {
                exchange,
                symbol: symbol.into(),
                maker_rate: Decimal::new(1, 4),
                taker_rate: Decimal::new(4, 4),
            })
        }
    }

    #[async_trait]
    impl LeverageGateway for FakeGateway {
        async fn set_leverage(
            &self,
            exchange: Exchange,
            symbol: &str,
            leverage: u16,
        ) -> Result<LeverageAcknowledgement, LeverageError> {
            let mut state = self.state.lock().unwrap();
            state.calls.leverage_writes += 1;
            state.leverage = leverage;
            Ok(LeverageAcknowledgement {
                exchange,
                symbol: symbol.into(),
                leverage,
            })
        }
    }

    fn config(trigger_price: Option<Decimal>) -> GridConfig {
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
            trigger_price,
            stop_loss_price: None,
            take_profit_price: None,
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
            min_notional: Decimal::ZERO,
        }
    }

    #[tokio::test]
    async fn waiting_trigger_creates_no_private_account_read_or_write() {
        let gateway = FakeGateway::new(Decimal::new(1010, 0), 3, Decimal::new(-3, 0));
        let prepared = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0001").unwrap(),
            config(Some(Decimal::new(1014, 0))),
            10_500,
            1_000,
            100,
        )
        .await
        .unwrap();
        assert!(matches!(prepared, PreparedStrategy::Armed(_)));
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.calls.market, 1);
        assert_eq!(state.calls.rules, 0);
        assert_eq!(state.calls.position, 0);
        assert_eq!(state.calls.fees, 0);
        assert_eq!(state.calls.leverage_writes, 0);
        assert_eq!(state.leverage, 3);
    }

    #[tokio::test]
    async fn immediate_strategy_preserves_matching_existing_leverage_and_baseline() {
        let gateway = FakeGateway::new(Decimal::new(1014, 0), 5, Decimal::new(-3, 0));
        let prepared = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0002").unwrap(),
            config(None),
            10_500,
            1_000,
            100,
        )
        .await
        .unwrap();
        let PreparedStrategy::Active(active) = prepared else {
            panic!("strategy should be active");
        };
        assert_eq!(active.config.maker_fee_rate, Some(Decimal::new(1, 4)));
        assert_eq!(active.config.taker_fee_rate, Some(Decimal::new(4, 4)));
        assert_eq!(active.baseline.signed_quantity, Decimal::new(-3, 0));
        assert_eq!(active.lifecycle, StrategyLifecycle::AwaitingOpening);
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.leverage, 5);
        assert_eq!(state.calls.leverage_writes, 0);
        assert!(state.calls.position >= 2);
        assert_eq!(state.calls.fees, 1);
        assert_eq!(state.calls.rules, 1);
    }

    #[tokio::test]
    async fn unhit_trigger_activation_still_performs_no_private_operation() {
        let gateway = FakeGateway::new(Decimal::new(1010, 0), 3, Decimal::ZERO);
        let PreparedStrategy::Armed(armed) = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0003").unwrap(),
            config(Some(Decimal::new(1014, 0))),
            10_500,
            1_000,
            100,
        )
        .await
        .unwrap() else {
            panic!("strategy should be armed");
        };
        assert!(matches!(
            activate_armed_strategy(&gateway, &armed, 10_500, 1_000, 100).await,
            Err(StrategyBootstrapError::TriggerNotReached)
        ));
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.calls.fees, 0);
        assert_eq!(state.calls.position, 0);
        assert_eq!(state.calls.leverage_writes, 0);
    }

    #[tokio::test]
    async fn reached_trigger_reloads_authoritative_account_facts_at_activation() {
        let gateway = FakeGateway::new(Decimal::new(1010, 0), 3, Decimal::ZERO);
        let PreparedStrategy::Armed(armed) = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0004").unwrap(),
            config(Some(Decimal::new(1014, 0))),
            10_500,
            1_000,
            100,
        )
        .await
        .unwrap() else {
            panic!("strategy should be armed");
        };
        gateway.state.lock().unwrap().market_price = Decimal::new(1014, 0);
        let active = activate_armed_strategy(&gateway, &armed, 10_500, 1_000, 100)
            .await
            .unwrap();

        assert_eq!(active.config.maker_fee_rate, Some(Decimal::new(1, 4)));
        assert_eq!(active.config.taker_fee_rate, Some(Decimal::new(4, 4)));
        assert_eq!(active.triggered_at_ms, Some(10_500));
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.leverage, 5);
        assert_eq!(state.calls.leverage_writes, 1);
        assert_eq!(state.calls.fees, 1);
        assert_eq!(state.calls.rules, 1);
    }

    #[tokio::test]
    async fn fee_snapshot_failure_prevents_leverage_write_and_strategy_creation() {
        let gateway = FakeGateway::new(Decimal::new(1014, 0), 3, Decimal::new(-3, 0));
        gateway.fail_fee_rates("fee endpoint unavailable");

        let result = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0005").unwrap(),
            config(None),
            10_500,
            1_000,
            100,
        )
        .await;

        assert!(matches!(
            result,
            Err(StrategyBootstrapError::FeeRates(
                FeeRateConfigError::Snapshot(_)
            ))
        ));
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.calls.fees, 1);
        assert_eq!(state.calls.leverage_writes, 0);
        assert_eq!(state.calls.position, 0);
        assert_eq!(state.calls.rules, 0);
        assert_eq!(state.calls.market, 0);
        assert_eq!(state.leverage, 3);
    }

    #[tokio::test]
    async fn trigger_reversal_during_preflight_never_creates_an_active_strategy() {
        let gateway = FakeGateway::new(Decimal::new(1010, 0), 3, Decimal::ZERO);
        let PreparedStrategy::Armed(armed) = prepare_new_strategy(
            &gateway,
            StrategyRunId::parse("BOOT0006").unwrap(),
            config(Some(Decimal::new(1014, 0))),
            10_500,
            1_000,
            100,
        )
        .await
        .unwrap() else {
            panic!("strategy should be armed");
        };
        gateway.queue_market_prices([Decimal::new(1014, 0), Decimal::new(1013, 0)]);

        let result = activate_armed_strategy(&gateway, &armed, 10_500, 1_000, 100).await;

        assert!(matches!(
            result,
            Err(StrategyBootstrapError::TriggerNoLongerReached)
        ));
        let state = gateway.state.lock().unwrap();
        assert_eq!(state.calls.market, 3);
        assert_eq!(state.calls.fees, 1);
        assert_eq!(state.calls.leverage_writes, 1);
        assert_eq!(state.calls.rules, 1);
        assert!(state.calls.position >= 3);
        assert_eq!(state.market_price, Decimal::new(1013, 0));
    }
}
