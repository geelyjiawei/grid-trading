use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{Exchange, GridConfig, InstrumentRules};

use super::{
    GridPlanError, MarketSnapshot, PositionBaseline, StrategyLifecycle, StrategyRunId,
    StrategyState, StrategyStateError, TriggerActivation, build_grid_plan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerCondition {
    AtOrAbove,
    AtOrBelow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArmedStrategyLifecycle {
    WaitingTrigger,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmedStrategyState {
    pub version: u8,
    pub revision: u64,
    pub run_id: StrategyRunId,
    pub config: GridConfig,
    pub exchange: Exchange,
    pub symbol: String,
    pub trigger_price: Decimal,
    pub armed_market_price: Decimal,
    pub condition: TriggerCondition,
    pub lifecycle: ArmedStrategyLifecycle,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl ArmedStrategyState {
    pub fn new(
        run_id: StrategyRunId,
        config: GridConfig,
        market: &MarketSnapshot,
        now_ms: u64,
    ) -> Result<Self, ArmedStrategyError> {
        config
            .validate()
            .map_err(ArmedStrategyError::InvalidConfig)?;
        validate_market(market)?;
        let exchange = config.exchange.ok_or(ArmedStrategyError::MissingExchange)?;
        let trigger_price = config
            .trigger_price
            .ok_or(ArmedStrategyError::MissingTriggerPrice)?;
        let condition = if trigger_price >= market.last_price {
            TriggerCondition::AtOrAbove
        } else {
            TriggerCondition::AtOrBelow
        };
        let state = Self {
            version: 1,
            revision: 0,
            run_id,
            symbol: config.symbol.clone(),
            config,
            exchange,
            trigger_price,
            armed_market_price: market.last_price,
            condition,
            lifecycle: ArmedStrategyLifecycle::WaitingTrigger,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), ArmedStrategyError> {
        if self.version != 1 {
            return Err(ArmedStrategyError::UnsupportedVersion(self.version));
        }
        StrategyRunId::parse(self.run_id.as_str())?;
        self.config
            .validate()
            .map_err(ArmedStrategyError::InvalidConfig)?;
        if self.config.exchange != Some(self.exchange)
            || self.config.symbol != self.symbol
            || self.config.trigger_price != Some(self.trigger_price)
        {
            return Err(ArmedStrategyError::IdentityMismatch);
        }
        if self.trigger_price <= Decimal::ZERO || self.armed_market_price <= Decimal::ZERO {
            return Err(ArmedStrategyError::InvalidMarketPrice);
        }
        let expected_condition = if self.trigger_price >= self.armed_market_price {
            TriggerCondition::AtOrAbove
        } else {
            TriggerCondition::AtOrBelow
        };
        if self.condition != expected_condition {
            return Err(ArmedStrategyError::ConditionMismatch);
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err(ArmedStrategyError::TimestampRegression);
        }
        Ok(())
    }

    pub fn is_triggered(&self, market: &MarketSnapshot) -> Result<bool, ArmedStrategyError> {
        validate_market(market)?;
        Ok(match self.condition {
            TriggerCondition::AtOrAbove => market.last_price >= self.trigger_price,
            TriggerCondition::AtOrBelow => market.last_price <= self.trigger_price,
        })
    }

    pub fn activate(
        &self,
        market: &MarketSnapshot,
        fresh_rules: InstrumentRules,
        baseline: PositionBaseline,
        now_ms: u64,
    ) -> Result<StrategyState, ArmedStrategyError> {
        self.activate_with_config(self.config.clone(), market, fresh_rules, baseline, now_ms)
    }

    pub fn activate_with_config(
        &self,
        effective_config: GridConfig,
        market: &MarketSnapshot,
        fresh_rules: InstrumentRules,
        baseline: PositionBaseline,
        now_ms: u64,
    ) -> Result<StrategyState, ArmedStrategyError> {
        self.validate()?;
        effective_config
            .validate()
            .map_err(ArmedStrategyError::InvalidConfig)?;
        if !same_strategy_except_fee_rates(&self.config, &effective_config) {
            return Err(ArmedStrategyError::ActivationConfigMismatch);
        }
        if self.lifecycle != ArmedStrategyLifecycle::WaitingTrigger {
            return Err(ArmedStrategyError::NotWaiting);
        }
        if now_ms < self.updated_at_ms {
            return Err(ArmedStrategyError::TimestampRegression);
        }
        if !self.is_triggered(market)? {
            return Err(ArmedStrategyError::NotTriggered);
        }
        let plan = build_grid_plan(&effective_config, market, &fresh_rules)?;
        let mut active = StrategyState::from_triggered_plan(
            self.run_id.clone(),
            effective_config,
            fresh_rules,
            plan,
            baseline,
            TriggerActivation {
                armed_price: self.armed_market_price,
                observed_price: market.last_price,
                triggered_at_ms: now_ms,
            },
        )?;
        active.revision = self
            .revision
            .checked_add(1)
            .ok_or(ArmedStrategyError::RevisionOverflow)?;
        active.created_at_ms = self.created_at_ms;
        active.validate()?;
        Ok(active)
    }

    pub fn validate_active_successor(
        &self,
        active: &StrategyState,
    ) -> Result<(), ArmedStrategyError> {
        self.validate()?;
        active.validate()?;
        if self.lifecycle != ArmedStrategyLifecycle::WaitingTrigger {
            return Err(ArmedStrategyError::NotWaiting);
        }
        let expected_revision = self
            .revision
            .checked_add(1)
            .ok_or(ArmedStrategyError::RevisionOverflow)?;
        let Some(observed_price) = active.trigger_observed_price else {
            return Err(ArmedStrategyError::ActiveSuccessorMismatch);
        };
        let trigger_still_matches = match self.condition {
            TriggerCondition::AtOrAbove => observed_price >= self.trigger_price,
            TriggerCondition::AtOrBelow => observed_price <= self.trigger_price,
        };
        if active.run_id != self.run_id
            || active.revision != expected_revision
            || active.exchange != self.exchange
            || active.symbol != self.symbol
            || active.direction != self.config.direction
            || !same_strategy_except_fee_rates(&self.config, &active.config)
            || active.created_at_ms != self.created_at_ms
            || active.updated_at_ms < self.updated_at_ms
            || active.triggered_at_ms != Some(active.updated_at_ms)
            || active.trigger_armed_price != Some(self.armed_market_price)
            || !trigger_still_matches
            || !matches!(
                active.lifecycle,
                StrategyLifecycle::AwaitingOpening | StrategyLifecycle::DeployingGrid
            )
        {
            return Err(ArmedStrategyError::ActiveSuccessorMismatch);
        }
        Ok(())
    }

    pub fn cancelled(&self, now_ms: u64) -> Result<Self, ArmedStrategyError> {
        self.validate()?;
        if now_ms < self.updated_at_ms {
            return Err(ArmedStrategyError::TimestampRegression);
        }
        if self.lifecycle == ArmedStrategyLifecycle::Cancelled {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(ArmedStrategyError::RevisionOverflow)?;
        next.lifecycle = ArmedStrategyLifecycle::Cancelled;
        next.updated_at_ms = now_ms;
        next.validate()?;
        Ok(next)
    }
}

fn same_strategy_except_fee_rates(left: &GridConfig, right: &GridConfig) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.fee_rate = None;
    left.maker_fee_rate = None;
    left.taker_fee_rate = None;
    right.fee_rate = None;
    right.maker_fee_rate = None;
    right.taker_fee_rate = None;
    left == right
}

fn validate_market(market: &MarketSnapshot) -> Result<(), ArmedStrategyError> {
    if market.last_price <= Decimal::ZERO || market.mark_price <= Decimal::ZERO {
        return Err(ArmedStrategyError::InvalidMarketPrice);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum ArmedStrategyError {
    #[error("armed strategy configuration is invalid: {0}")]
    InvalidConfig(crate::domain::GridConfigError),
    #[error("armed strategy configuration must identify an exchange")]
    MissingExchange,
    #[error("armed strategy requires a trigger price")]
    MissingTriggerPrice,
    #[error("market prices must be positive")]
    InvalidMarketPrice,
    #[error("unsupported armed strategy state version {0}")]
    UnsupportedVersion(u8),
    #[error("armed strategy identity does not match its configuration")]
    IdentityMismatch,
    #[error("armed trigger condition does not match its reference price")]
    ConditionMismatch,
    #[error("armed strategy timestamp regressed")]
    TimestampRegression,
    #[error("armed strategy is not waiting for a trigger")]
    NotWaiting,
    #[error("trigger activation changed a strategy field other than authoritative fee rates")]
    ActivationConfigMismatch,
    #[error("trigger price has not been reached")]
    NotTriggered,
    #[error("armed strategy revision overflowed")]
    RevisionOverflow,
    #[error("active strategy is not the direct successor of this armed strategy")]
    ActiveSuccessorMismatch,
    #[error("grid planning failed at trigger time: {0}")]
    GridPlan(#[from] GridPlanError),
    #[error("active strategy state is invalid: {0}")]
    StrategyState(#[from] StrategyStateError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Direction, GridMode, InitialOrderType, PositionSizingMode, QuantityRules};

    fn config(direction: Direction, trigger_price: Option<Decimal>) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction,
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
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn market(price: i64) -> MarketSnapshot {
        MarketSnapshot {
            last_price: Decimal::from(price),
            mark_price: Decimal::from(price),
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

    #[test]
    fn trigger_direction_is_derived_from_the_arming_market_not_grid_direction() {
        let above = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED001").unwrap(),
            config(Direction::Long, Some(Decimal::new(1015, 0))),
            &market(1010),
            100,
        )
        .unwrap();
        let below = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED002").unwrap(),
            config(Direction::Short, Some(Decimal::new(1005, 0))),
            &market(1010),
            100,
        )
        .unwrap();

        assert_eq!(above.condition, TriggerCondition::AtOrAbove);
        assert_eq!(below.condition, TriggerCondition::AtOrBelow);
        assert!(!above.is_triggered(&market(1014)).unwrap());
        assert!(above.is_triggered(&market(1015)).unwrap());
        assert!(!below.is_triggered(&market(1006)).unwrap());
        assert!(below.is_triggered(&market(1005)).unwrap());
    }

    #[test]
    fn activation_replans_with_trigger_time_market_and_captures_fresh_baseline() {
        let armed = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED003").unwrap(),
            config(Direction::Short, Some(Decimal::new(1014, 0))),
            &market(1010),
            100,
        )
        .unwrap();
        let baseline = PositionBaseline {
            signed_quantity: Decimal::new(-3, 0),
            entry_price: Some(Decimal::new(1016, 0)),
        };

        assert!(matches!(
            armed.activate(&market(1013), rules(), baseline.clone(), 101),
            Err(ArmedStrategyError::NotTriggered)
        ));
        let active = armed
            .activate(&market(1014), rules(), baseline.clone(), 102)
            .unwrap();

        assert_eq!(active.revision, 1);
        assert_eq!(active.created_at_ms, 100);
        assert_eq!(active.updated_at_ms, 102);
        assert_eq!(active.plan.reference_price, Decimal::new(10141, 1));
        assert_eq!(active.baseline, baseline);
        assert_eq!(active.trigger_armed_price, Some(Decimal::new(1010, 0)));
        assert_eq!(active.trigger_observed_price, Some(Decimal::new(1014, 0)));
        assert_eq!(active.triggered_at_ms, Some(102));
        assert_eq!(active.ready_intents(103).unwrap().len(), 1);
    }

    #[test]
    fn ordinary_active_constructor_rejects_an_unprocessed_trigger() {
        let config = config(Direction::Short, Some(Decimal::new(1014, 0)));
        let plan = build_grid_plan(&config, &market(1014), &rules()).unwrap();

        assert!(matches!(
            StrategyState::from_plan(
                StrategyRunId::parse("ARMED004").unwrap(),
                config,
                rules(),
                plan,
                PositionBaseline::flat(),
                100,
            ),
            Err(StrategyStateError::InvalidTriggerActivation)
        ));
    }

    #[test]
    fn cancelled_arm_never_activates() {
        let armed = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED005").unwrap(),
            config(Direction::Short, Some(Decimal::new(1014, 0))),
            &market(1010),
            100,
        )
        .unwrap()
        .cancelled(101)
        .unwrap();

        assert_eq!(armed.lifecycle, ArmedStrategyLifecycle::Cancelled);
        assert_eq!(armed.revision, 1);
        assert!(matches!(
            armed.activate(&market(1014), rules(), PositionBaseline::flat(), 102),
            Err(ArmedStrategyError::NotWaiting)
        ));
    }

    #[test]
    fn trigger_activation_allows_only_authoritative_fee_refresh() {
        let armed = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED006").unwrap(),
            config(Direction::Short, Some(Decimal::new(1014, 0))),
            &market(1010),
            100,
        )
        .unwrap();
        let mut effective = armed.config.clone();
        effective.fee_rate = Some(Decimal::new(4, 4));
        effective.maker_fee_rate = Some(Decimal::new(1, 4));
        effective.taker_fee_rate = Some(Decimal::new(4, 4));

        let active = armed
            .activate_with_config(
                effective,
                &market(1014),
                rules(),
                PositionBaseline::flat(),
                101,
            )
            .unwrap();
        assert_eq!(active.config.maker_fee_rate, Some(Decimal::new(1, 4)));
        assert_eq!(active.config.taker_fee_rate, Some(Decimal::new(4, 4)));
        armed.validate_active_successor(&active).unwrap();

        let mut wrong_observation = active.clone();
        wrong_observation.trigger_observed_price = Some(Decimal::new(1013, 0));
        assert!(matches!(
            armed.validate_active_successor(&wrong_observation),
            Err(ArmedStrategyError::ActiveSuccessorMismatch)
        ));

        let mut drifted = armed.config.clone();
        drifted.grid_order_qty = Some(Decimal::new(3, 1));
        assert!(matches!(
            armed.activate_with_config(
                drifted,
                &market(1014),
                rules(),
                PositionBaseline::flat(),
                101,
            ),
            Err(ArmedStrategyError::ActivationConfigMismatch)
        ));
    }
}
