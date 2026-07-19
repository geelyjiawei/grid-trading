use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::{Decimal, MathematicalOps, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    Direction, GridConfig, GridConfigError, GridMode, InitialOrderType, InstrumentRules,
    InstrumentRulesError, OrderKind, OrderSide, PositionSizingMode, TimeInForce,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub last_price: Decimal,
    pub mark_price: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridOrderRole {
    Profit,
    Add,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedOpeningOrder {
    pub side: OrderSide,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedGridOrder {
    pub level_index: u16,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub reduce_only: bool,
    pub time_in_force: TimeInForce,
    pub role: GridOrderRole,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridPlan {
    pub reference_price: Decimal,
    pub raw_levels: Vec<Decimal>,
    pub levels: Vec<Decimal>,
    pub active_grid_count: u16,
    pub participating_level_count: u16,
    pub total_quantity: Decimal,
    pub opening_order: Option<PlannedOpeningOrder>,
    pub grid_orders: Vec<PlannedGridOrder>,
}

impl GridPlan {
    pub(super) fn validate_snapshot(
        &self,
        config: &GridConfig,
        rules: &InstrumentRules,
    ) -> Result<(), GridPlanError> {
        config.validate()?;
        rules.validate()?;
        if config.direction != Direction::Neutral
            && config.initial_order_type != InitialOrderType::Market
            && rules.floor_price(self.reference_price) != Some(self.reference_price)
        {
            return Err(GridPlanError::PlanSnapshotMismatch);
        }
        if config.direction != Direction::Neutral
            && config.initial_order_type == InitialOrderType::Limit
            && config
                .initial_order_price
                .is_some_and(|price| rules.floor_price(price) != Some(self.reference_price))
        {
            return Err(GridPlanError::PlanSnapshotMismatch);
        }

        // The original market mark is intentionally not persisted. Rebuilding
        // with zero minimum notional reproduces every deterministic plan field;
        // the actual limit legs are checked against the persisted rules below.
        let mut structural_rules = rules.clone();
        structural_rules.min_notional = Decimal::ZERO;
        let replay_market = MarketSnapshot {
            last_price: self.reference_price,
            mark_price: self.reference_price,
        };
        let expected = build_grid_plan_at_reference(
            config,
            &replay_market,
            &structural_rules,
            self.reference_price,
        )?;
        if expected != *self {
            return Err(GridPlanError::PlanSnapshotMismatch);
        }
        validate_grid_orders(config, rules, &self.levels, &self.grid_orders)?;
        if config.direction != Direction::Neutral
            && config.initial_order_type != InitialOrderType::Market
        {
            validate_notional(
                None,
                self.reference_price,
                self.total_quantity,
                rules.min_notional,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct Target {
    level_index: u16,
    price: Decimal,
    side: OrderSide,
    role: GridOrderRole,
}

pub fn build_grid_plan(
    config: &GridConfig,
    market: &MarketSnapshot,
    rules: &InstrumentRules,
) -> Result<GridPlan, GridPlanError> {
    config.validate()?;
    rules.validate()?;
    validate_market(market)?;

    let reference_price = reference_price(config, market, rules)?;
    build_grid_plan_at_reference(config, market, rules, reference_price)
}

fn build_grid_plan_at_reference(
    config: &GridConfig,
    market: &MarketSnapshot,
    rules: &InstrumentRules,
    reference_price: Decimal,
) -> Result<GridPlan, GridPlanError> {
    let raw_levels = calculate_levels(config)?;
    let levels = normalize_levels(&raw_levels, rules)?;
    if reference_price <= levels[0] || reference_price >= levels[levels.len() - 1] {
        return Err(GridPlanError::ReferenceOutsideRange);
    }

    let (profit_targets, add_targets) = targets(config.direction, &levels, reference_price);
    let active_targets = if config.direction == Direction::Neutral {
        &add_targets
    } else {
        &profit_targets
    };
    if active_targets.is_empty() {
        return Err(GridPlanError::NoActiveTargets);
    }

    let (total_quantity, active_quantities) =
        allocate_active_quantities(config, market, rules, reference_price, active_targets.len())?;
    let active_grid_count = u16::try_from(active_targets.len())
        .map_err(|_| GridPlanError::NumericOverflow("active grid count"))?;

    let mut quantity_by_level = BTreeMap::new();
    for (target, quantity) in active_targets.iter().zip(&active_quantities) {
        quantity_by_level.insert(target.level_index, *quantity);
    }
    let fallback_quantity = rules
        .limit_quantity
        .floor(
            total_quantity
                .checked_div(Decimal::from(active_grid_count))
                .ok_or(GridPlanError::NumericOverflow("fallback quantity"))?,
        )
        .ok_or(GridPlanError::NumericOverflow("fallback quantity"))?;
    for target in &add_targets {
        quantity_by_level
            .entry(target.level_index)
            .or_insert(fallback_quantity);
    }

    let time_in_force = if config.grid_order_post_only {
        TimeInForce::PostOnly
    } else {
        TimeInForce::Gtc
    };
    let mut grid_orders = Vec::with_capacity(profit_targets.len() + add_targets.len());
    if config.direction != Direction::Neutral {
        for (target, quantity) in profit_targets.iter().zip(&active_quantities) {
            grid_orders.push(planned_grid_order(*target, *quantity, true, time_in_force));
        }
        for target in &add_targets {
            let quantity = *quantity_by_level
                .get(&target.level_index)
                .ok_or(GridPlanError::MissingLevelQuantity(target.level_index))?;
            grid_orders.push(planned_grid_order(*target, quantity, false, time_in_force));
        }
    } else {
        for (target, quantity) in add_targets.iter().zip(&active_quantities) {
            grid_orders.push(planned_grid_order(*target, *quantity, false, time_in_force));
        }
    }

    validate_grid_orders(config, rules, &levels, &grid_orders)?;
    let opening_order = opening_order(config, market, rules, reference_price, total_quantity)?;
    let participating_level_count = u16::try_from(
        grid_orders
            .iter()
            .map(|order| order.level_index)
            .collect::<BTreeSet<_>>()
            .len(),
    )
    .map_err(|_| GridPlanError::NumericOverflow("participating level count"))?;
    if participating_level_count != config.grid_count {
        return Err(GridPlanError::IncompleteGridCoverage);
    }
    let owned_quantity = grid_orders
        .iter()
        .filter(|order| config.direction == Direction::Neutral || order.reduce_only)
        .map(|order| order.quantity)
        .try_fold(Decimal::ZERO, |total, quantity| total.checked_add(quantity))
        .ok_or(GridPlanError::NumericOverflow("planned owned quantity"))?;
    if owned_quantity != total_quantity {
        return Err(GridPlanError::PlanQuantityMismatch);
    }

    Ok(GridPlan {
        reference_price,
        raw_levels,
        levels,
        active_grid_count,
        participating_level_count,
        total_quantity,
        opening_order,
        grid_orders,
    })
}

fn validate_market(market: &MarketSnapshot) -> Result<(), GridPlanError> {
    if market.last_price <= Decimal::ZERO || market.mark_price <= Decimal::ZERO {
        return Err(GridPlanError::InvalidMarketPrice);
    }
    Ok(())
}

fn calculate_levels(config: &GridConfig) -> Result<Vec<Decimal>, GridPlanError> {
    let count = usize::from(config.grid_count);
    let count_decimal = Decimal::from(config.grid_count);
    let mut levels = Vec::with_capacity(count + 1);
    match config.grid_mode {
        GridMode::Arithmetic => {
            let step = config
                .upper_price
                .checked_sub(config.lower_price)
                .and_then(|range| range.checked_div(count_decimal))
                .ok_or(GridPlanError::NumericOverflow("arithmetic grid step"))?;
            for index in 0..=count {
                let level = if index == count {
                    config.upper_price
                } else {
                    step.checked_mul(Decimal::from(index))
                        .and_then(|offset| config.lower_price.checked_add(offset))
                        .ok_or(GridPlanError::NumericOverflow("arithmetic grid level"))?
                };
                levels.push(level);
            }
        }
        GridMode::Geometric => {
            let range_ratio = config
                .upper_price
                .checked_div(config.lower_price)
                .ok_or(GridPlanError::NumericOverflow("geometric grid ratio"))?;
            for index in 0..=count {
                let level = if index == 0 {
                    config.lower_price
                } else if index == count {
                    config.upper_price
                } else {
                    let exponent = Decimal::from(index)
                        .checked_div(count_decimal)
                        .ok_or(GridPlanError::NumericOverflow("geometric exponent"))?;
                    range_ratio
                        .checked_ln()
                        .and_then(|logarithm| logarithm.checked_mul(exponent))
                        .and_then(|power| power.checked_exp_with_tolerance(Decimal::new(1, 24)))
                        .and_then(|factor| config.lower_price.checked_mul(factor))
                        .ok_or(GridPlanError::GeometricCalculation)?
                };
                levels.push(level);
            }
        }
    }
    Ok(levels)
}

fn normalize_levels(
    raw_levels: &[Decimal],
    rules: &InstrumentRules,
) -> Result<Vec<Decimal>, GridPlanError> {
    let levels = raw_levels
        .iter()
        .map(|price| {
            rules
                .floor_price(*price)
                .ok_or(GridPlanError::NumericOverflow("exchange grid price"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if levels.windows(2).any(|pair| pair[1] <= pair[0]) {
        return Err(GridPlanError::CollapsedExchangePrices);
    }
    Ok(levels)
}

fn reference_price(
    config: &GridConfig,
    market: &MarketSnapshot,
    rules: &InstrumentRules,
) -> Result<Decimal, GridPlanError> {
    if config.direction == Direction::Neutral
        || config.initial_order_type == InitialOrderType::Market
    {
        return Ok(market.last_price);
    }
    let side = opening_side(config.direction).ok_or(GridPlanError::NoOpeningSide)?;
    let maker_safe = match side {
        OrderSide::Buy => market.last_price.checked_sub(rules.tick_size),
        OrderSide::Sell => market.last_price.checked_add(rules.tick_size),
    }
    .ok_or(GridPlanError::NumericOverflow("maker-safe opening price"))?;
    let configured = config.initial_order_price;
    let requested = match config.initial_order_type {
        InitialOrderType::Limit => configured.unwrap_or(maker_safe),
        InitialOrderType::PostOnly => match (side, configured) {
            (OrderSide::Buy, Some(price)) if price < market.last_price => price,
            (OrderSide::Sell, Some(price)) if price > market.last_price => price,
            _ => maker_safe,
        },
        InitialOrderType::Market => market.last_price,
    };
    let normalized = rules
        .floor_price(requested)
        .ok_or(GridPlanError::NumericOverflow("opening price"))?;
    if normalized <= Decimal::ZERO {
        return Err(GridPlanError::InvalidOpeningPrice);
    }
    if config.initial_order_type == InitialOrderType::PostOnly {
        let crosses = match side {
            OrderSide::Buy => normalized >= market.last_price,
            OrderSide::Sell => normalized <= market.last_price,
        };
        if crosses {
            return Err(GridPlanError::PostOnlyWouldCross);
        }
    }
    Ok(normalized)
}

fn targets(
    direction: Direction,
    levels: &[Decimal],
    reference: Decimal,
) -> (Vec<Target>, Vec<Target>) {
    let mut profit = Vec::new();
    let mut add = Vec::new();
    for (index, pair) in levels.windows(2).enumerate() {
        let level_index = index as u16;
        let lower = pair[0];
        let upper = pair[1];
        match direction {
            Direction::Long => {
                if upper > reference {
                    profit.push(Target {
                        level_index,
                        price: upper,
                        side: OrderSide::Sell,
                        role: GridOrderRole::Profit,
                    });
                }
                if lower < reference {
                    add.push(Target {
                        level_index,
                        price: lower,
                        side: OrderSide::Buy,
                        role: GridOrderRole::Add,
                    });
                }
            }
            Direction::Short => {
                if lower < reference {
                    profit.push(Target {
                        level_index,
                        price: lower,
                        side: OrderSide::Buy,
                        role: GridOrderRole::Profit,
                    });
                }
                if upper > reference {
                    add.push(Target {
                        level_index,
                        price: upper,
                        side: OrderSide::Sell,
                        role: GridOrderRole::Add,
                    });
                }
            }
            Direction::Neutral => {
                if lower < reference {
                    add.push(Target {
                        level_index,
                        price: lower,
                        side: OrderSide::Buy,
                        role: GridOrderRole::Add,
                    });
                }
                if upper > reference {
                    add.push(Target {
                        level_index,
                        price: upper,
                        side: OrderSide::Sell,
                        role: GridOrderRole::Add,
                    });
                }
            }
        }
    }
    (profit, add)
}

fn allocate_active_quantities(
    config: &GridConfig,
    market: &MarketSnapshot,
    rules: &InstrumentRules,
    reference_price: Decimal,
    active_count: usize,
) -> Result<(Decimal, Vec<Decimal>), GridPlanError> {
    let (mut total, mut quantities) = match config.position_sizing_mode {
        PositionSizingMode::FixedGridQty => {
            let quantity = config
                .grid_order_qty
                .ok_or(GridPlanError::MissingFixedQuantity)?;
            if !rules.limit_quantity.is_aligned(quantity) {
                return Err(GridPlanError::FixedQuantityStepMismatch);
            }
            validate_quantity("fixed per-grid", quantity, &rules.limit_quantity)?;
            let total = quantity
                .checked_mul(Decimal::from(active_count))
                .ok_or(GridPlanError::NumericOverflow("fixed opening quantity"))?;
            (total, vec![quantity; active_count])
        }
        PositionSizingMode::Investment => {
            let leverage = Decimal::from(config.leverage);
            let raw_total = config
                .total_investment
                .checked_mul(leverage)
                .and_then(|notional| notional.checked_div(reference_price))
                .ok_or(GridPlanError::NumericOverflow(
                    "investment opening quantity",
                ))?;
            let total =
                rules
                    .limit_quantity
                    .floor(raw_total)
                    .ok_or(GridPlanError::NumericOverflow(
                        "investment opening quantity",
                    ))?;
            let quantities = allocate_steps(total, active_count, rules.limit_quantity.step)?;
            (total, quantities)
        }
    };

    if config.direction != Direction::Neutral
        && config.initial_order_type == InitialOrderType::Market
    {
        let market_total = rules
            .market_quantity
            .floor(total)
            .ok_or(GridPlanError::NumericOverflow("market opening quantity"))?;
        if config.position_sizing_mode == PositionSizingMode::FixedGridQty && market_total != total
        {
            return Err(GridPlanError::FixedMarketStepMismatch);
        }
        validate_quantity("initial market", market_total, &rules.market_quantity)?;
        if !rules.limit_quantity.is_aligned(market_total) {
            return Err(GridPlanError::MarketLimitStepMismatch);
        }
        total = market_total;
        quantities = allocate_steps(total, active_count, rules.limit_quantity.step)?;
        if config.position_sizing_mode == PositionSizingMode::FixedGridQty {
            let fixed = config
                .grid_order_qty
                .ok_or(GridPlanError::MissingFixedQuantity)?;
            if quantities.iter().any(|quantity| *quantity != fixed) {
                return Err(GridPlanError::FixedMarketAllocationMismatch);
            }
        }
    }

    if config.direction != Direction::Neutral {
        let notional_price = if config.initial_order_type == InitialOrderType::Market {
            market.mark_price
        } else {
            reference_price
        };
        validate_notional(None, notional_price, total, rules.min_notional)?;
    }
    Ok((total, quantities))
}

fn allocate_steps(
    total: Decimal,
    target_count: usize,
    step: Decimal,
) -> Result<Vec<Decimal>, GridPlanError> {
    let step_count = total
        .checked_div(step)
        .filter(|steps| steps.fract().is_zero())
        .and_then(|steps| steps.to_u64())
        .ok_or(GridPlanError::NumericOverflow("quantity step count"))?;
    let target_count_u64 =
        u64::try_from(target_count).map_err(|_| GridPlanError::NumericOverflow("target count"))?;
    if step_count < target_count_u64 {
        return Err(GridPlanError::InsufficientQuantityForTargets);
    }
    let base = step_count / target_count_u64;
    let remainder = step_count % target_count_u64;
    (0..target_count_u64)
        .map(|index| {
            step.checked_mul(Decimal::from(base + u64::from(index < remainder)))
                .ok_or(GridPlanError::NumericOverflow("allocated grid quantity"))
        })
        .collect()
}

fn opening_order(
    config: &GridConfig,
    _market: &MarketSnapshot,
    rules: &InstrumentRules,
    reference_price: Decimal,
    total_quantity: Decimal,
) -> Result<Option<PlannedOpeningOrder>, GridPlanError> {
    let Some(side) = opening_side(config.direction) else {
        return Ok(None);
    };
    let (price, kind, time_in_force, quantity_rules) = match config.initial_order_type {
        InitialOrderType::Market => (
            None,
            OrderKind::Market,
            TimeInForce::Gtc,
            &rules.market_quantity,
        ),
        InitialOrderType::Limit => (
            Some(reference_price),
            OrderKind::Limit,
            TimeInForce::Gtc,
            &rules.limit_quantity,
        ),
        InitialOrderType::PostOnly => (
            Some(reference_price),
            OrderKind::Limit,
            TimeInForce::PostOnly,
            &rules.limit_quantity,
        ),
    };
    validate_quantity("initial opening", total_quantity, quantity_rules)?;
    Ok(Some(PlannedOpeningOrder {
        side,
        price,
        quantity: total_quantity,
        kind,
        time_in_force,
    }))
}

fn opening_side(direction: Direction) -> Option<OrderSide> {
    match direction {
        Direction::Long => Some(OrderSide::Buy),
        Direction::Short => Some(OrderSide::Sell),
        Direction::Neutral => None,
    }
}

fn planned_grid_order(
    target: Target,
    quantity: Decimal,
    reduce_only: bool,
    time_in_force: TimeInForce,
) -> PlannedGridOrder {
    PlannedGridOrder {
        level_index: target.level_index,
        side: target.side,
        price: target.price,
        quantity,
        reduce_only,
        time_in_force,
        role: target.role,
    }
}

fn validate_grid_orders(
    config: &GridConfig,
    rules: &InstrumentRules,
    levels: &[Decimal],
    orders: &[PlannedGridOrder],
) -> Result<(), GridPlanError> {
    let mut identities = BTreeSet::new();
    for order in orders {
        if !identities.insert((order.level_index, order.side, order.reduce_only)) {
            return Err(GridPlanError::DuplicateGridTarget);
        }
        validate_quantity("grid", order.quantity, &rules.limit_quantity)?;
        let open_price = if config.direction == Direction::Short {
            levels[usize::from(order.level_index) + 1]
        } else {
            levels[usize::from(order.level_index)]
        };
        validate_notional(
            Some(order.level_index),
            open_price,
            order.quantity,
            rules.min_notional,
        )?;
    }
    Ok(())
}

fn validate_quantity(
    context: &'static str,
    quantity: Decimal,
    rules: &crate::domain::QuantityRules,
) -> Result<(), GridPlanError> {
    if !rules.is_aligned(quantity) {
        return Err(GridPlanError::QuantityStepMismatch { context });
    }
    if quantity < rules.min {
        return Err(GridPlanError::QuantityBelowMinimum { context });
    }
    if rules.max.is_some_and(|maximum| quantity > maximum) {
        return Err(GridPlanError::QuantityAboveMaximum { context });
    }
    Ok(())
}

fn validate_notional(
    level_index: Option<u16>,
    price: Decimal,
    quantity: Decimal,
    minimum: Decimal,
) -> Result<(), GridPlanError> {
    if minimum.is_zero() {
        return Ok(());
    }
    let notional = price
        .checked_mul(quantity)
        .ok_or(GridPlanError::NumericOverflow("order notional"))?;
    if notional < minimum {
        return Err(GridPlanError::NotionalBelowMinimum { level_index });
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GridPlanError {
    #[error(transparent)]
    InvalidConfig(#[from] GridConfigError),
    #[error(transparent)]
    InvalidInstrument(#[from] InstrumentRulesError),
    #[error("market prices must be positive")]
    InvalidMarketPrice,
    #[error("geometric grid calculation failed")]
    GeometricCalculation,
    #[error("configured levels collapse to duplicate exchange prices")]
    CollapsedExchangePrices,
    #[error("the actual opening reference price must stay strictly inside the grid range")]
    ReferenceOutsideRange,
    #[error("opening price must be positive after exchange quantization")]
    InvalidOpeningPrice,
    #[error("post-only opening price would cross the current market")]
    PostOnlyWouldCross,
    #[error("direction has no opening side")]
    NoOpeningSide,
    #[error("no active grid targets were found")]
    NoActiveTargets,
    #[error("fixed quantity is missing")]
    MissingFixedQuantity,
    #[error("fixed per-grid quantity is not an exact limit-order quantity step")]
    FixedQuantityStepMismatch,
    #[error("fixed opening quantity is not an exact market-order quantity step")]
    FixedMarketStepMismatch,
    #[error("market opening quantity cannot be represented by the limit-order quantity step")]
    MarketLimitStepMismatch,
    #[error("market opening allocation would change the fixed per-grid quantity")]
    FixedMarketAllocationMismatch,
    #[error("total quantity cannot allocate at least one exchange step to every active target")]
    InsufficientQuantityForTargets,
    #[error("{context} quantity is not aligned to its exchange step")]
    QuantityStepMismatch { context: &'static str },
    #[error("{context} quantity is below the exchange minimum")]
    QuantityBelowMinimum { context: &'static str },
    #[error("{context} quantity exceeds the exchange maximum")]
    QuantityAboveMaximum { context: &'static str },
    #[error("order notional is below the exchange minimum at level {level_index:?}")]
    NotionalBelowMinimum { level_index: Option<u16> },
    #[error("grid plan contains a duplicate order target")]
    DuplicateGridTarget,
    #[error("grid plan is missing quantity for level {0}")]
    MissingLevelQuantity(u16),
    #[error("grid plan does not represent every configured interval")]
    IncompleteGridCoverage,
    #[error("planned owned order quantity does not equal the opening or neutral allocation")]
    PlanQuantityMismatch,
    #[error("persisted grid plan does not match its deterministic configuration snapshot")]
    PlanSnapshotMismatch,
    #[error("numeric overflow while calculating {0}")]
    NumericOverflow(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Exchange, QuantityRules};

    fn decimal(value: i64) -> Decimal {
        Decimal::from(value)
    }

    fn fixed_config(direction: Direction) -> GridConfig {
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

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(1, 1),
            max_price_significant_digits: None,
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

    fn market(last: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            last_price: last,
            mark_price: last,
        }
    }

    #[test]
    fn every_supported_plan_shape_passes_deterministic_snapshot_validation() {
        let mut post_only = fixed_config(Direction::Short);
        post_only.initial_order_type = InitialOrderType::PostOnly;

        let mut market_open = fixed_config(Direction::Long);
        market_open.initial_order_type = InitialOrderType::Market;
        market_open.initial_order_price = None;

        let mut neutral = fixed_config(Direction::Neutral);
        neutral.initial_order_price = None;

        let mut investment = fixed_config(Direction::Short);
        investment.position_sizing_mode = PositionSizingMode::Investment;
        investment.total_investment = decimal(500);
        investment.grid_order_qty = None;
        investment.initial_order_type = InitialOrderType::Market;
        investment.initial_order_price = None;

        let mut geometric = fixed_config(Direction::Long);
        geometric.grid_mode = GridMode::Geometric;

        for (label, config, snapshot) in [
            (
                "regular limit",
                fixed_config(Direction::Long),
                market(decimal(1012)),
            ),
            ("post only", post_only, market(decimal(1014))),
            (
                "unaligned market reference",
                market_open,
                market(Decimal::new(101337, 2)),
            ),
            ("neutral", neutral, market(Decimal::new(101237, 2))),
            (
                "investment market",
                investment,
                market(Decimal::new(10123, 1)),
            ),
            ("geometric", geometric, market(decimal(1012))),
        ] {
            let plan = build_grid_plan(&config, &snapshot, &rules()).unwrap();
            assert_eq!(
                plan.validate_snapshot(&config, &rules()),
                Ok(()),
                "{label} plan must round-trip"
            );
        }
    }

    #[test]
    fn short_1014_opening_covers_exactly_fourteen_profit_levels() {
        let plan = build_grid_plan(
            &fixed_config(Direction::Short),
            &market(decimal(1012)),
            &rules(),
        )
        .unwrap();

        assert_eq!(plan.reference_price, decimal(1014));
        assert_eq!(plan.active_grid_count, 14);
        assert_eq!(plan.participating_level_count, 20);
        assert_eq!(plan.total_quantity, Decimal::new(28, 1));
        assert_eq!(
            plan.opening_order.as_ref().unwrap().quantity,
            Decimal::new(28, 1)
        );
        assert_eq!(plan.grid_orders.len(), 20);
        assert_eq!(
            plan.grid_orders
                .iter()
                .filter(|order| order.reduce_only)
                .count(),
            14
        );
        assert!(
            plan.grid_orders
                .iter()
                .all(|order| order.quantity == Decimal::new(2, 1))
        );
    }

    #[test]
    fn post_only_crossing_price_uses_quantized_maker_safe_reference() {
        let mut config = fixed_config(Direction::Short);
        config.initial_order_type = InitialOrderType::PostOnly;
        config.initial_order_price = Some(decimal(1012));
        let plan = build_grid_plan(&config, &market(decimal(1014)), &rules()).unwrap();

        assert_eq!(plan.reference_price, Decimal::new(10141, 1));
        assert_eq!(plan.active_grid_count, 15);
        assert_eq!(plan.total_quantity, decimal(3));
        assert_eq!(plan.participating_level_count, 20);
        assert_eq!(plan.grid_orders.len(), 21);
        assert_eq!(
            plan.opening_order.as_ref().unwrap().time_in_force,
            TimeInForce::PostOnly
        );
    }

    #[test]
    fn investment_allocation_preserves_every_exchange_step() {
        let mut config = fixed_config(Direction::Short);
        config.position_sizing_mode = PositionSizingMode::Investment;
        config.grid_order_qty = None;
        config.total_investment = decimal(500);
        config.initial_order_type = InitialOrderType::PostOnly;
        config.initial_order_price = Some(decimal(1011));
        let mut instrument = rules();
        instrument.limit_quantity.step = Decimal::new(1, 2);
        instrument.limit_quantity.min = Decimal::new(1, 2);
        let plan = build_grid_plan(&config, &market(decimal(1010)), &instrument).unwrap();

        assert_eq!(plan.active_grid_count, 11);
        assert_eq!(plan.total_quantity, Decimal::new(247, 2));
        let quantities = plan
            .grid_orders
            .iter()
            .filter(|order| order.reduce_only)
            .map(|order| order.quantity)
            .collect::<Vec<_>>();
        assert_eq!(
            quantities
                .iter()
                .filter(|quantity| **quantity == Decimal::new(23, 2))
                .count(),
            5
        );
        assert_eq!(
            quantities
                .iter()
                .filter(|quantity| **quantity == Decimal::new(22, 2))
                .count(),
            6
        );
    }

    #[test]
    fn fixed_quantity_that_is_not_representable_fails_closed() {
        let mut config = fixed_config(Direction::Short);
        config.grid_order_qty = Some(Decimal::new(25, 2));
        assert_eq!(
            build_grid_plan(&config, &market(decimal(1012)), &rules()),
            Err(GridPlanError::FixedQuantityStepMismatch)
        );
    }

    #[test]
    fn collapsed_exchange_prices_fail_before_any_order_plan_exists() {
        let mut config = fixed_config(Direction::Short);
        config.grid_count = 40;
        let mut rules = rules();
        rules.tick_size = decimal(1);
        assert_eq!(
            build_grid_plan(&config, &market(decimal(1012)), &rules),
            Err(GridPlanError::CollapsedExchangePrices)
        );
    }

    #[test]
    fn significant_digit_price_collisions_fail_before_any_order_plan_exists() {
        let mut config = fixed_config(Direction::Short);
        config.lower_price = Decimal::new(9_995, 1);
        config.upper_price = Decimal::new(10_005, 1);
        config.initial_order_price = Some(decimal(1000));
        let mut rules = rules();
        rules.tick_size = Decimal::new(1, 3);
        rules.max_price_significant_digits = Some(5);

        assert_eq!(
            build_grid_plan(&config, &market(decimal(1000)), &rules),
            Err(GridPlanError::CollapsedExchangePrices)
        );
    }

    #[test]
    fn fixed_market_step_drift_is_rejected() {
        let mut config = fixed_config(Direction::Short);
        config.initial_order_type = InitialOrderType::Market;
        config.initial_order_price = None;
        let mut rules = rules();
        rules.market_quantity.step = Decimal::new(3, 1);
        rules.market_quantity.min = Decimal::new(3, 1);
        assert_eq!(
            build_grid_plan(&config, &market(decimal(1011)), &rules),
            Err(GridPlanError::FixedMarketStepMismatch)
        );
    }

    #[test]
    fn every_direction_preserves_fixed_quantity_at_boundaries_and_between_levels() {
        for direction in [Direction::Long, Direction::Short, Direction::Neutral] {
            for reference in [
                Decimal::new(10001, 1),
                decimal(1010),
                Decimal::new(10199, 1),
            ] {
                let mut config = fixed_config(direction);
                config.initial_order_type = InitialOrderType::Limit;
                config.initial_order_price = Some(reference);
                let snapshot = market(reference);
                let plan = build_grid_plan(&config, &snapshot, &rules()).unwrap();
                assert!(
                    plan.grid_orders
                        .iter()
                        .all(|order| order.quantity == Decimal::new(2, 1))
                );
                assert_eq!(plan.participating_level_count, 20);
            }
        }
    }

    #[test]
    fn per_level_minimum_notional_uses_short_open_price() {
        let mut config = fixed_config(Direction::Short);
        config.lower_price = Decimal::new(38, 2);
        config.upper_price = Decimal::new(42, 2);
        config.initial_order_price = Some(Decimal::new(40, 2));
        config.grid_order_qty = Some(decimal(100));
        let mut rules = rules();
        rules.tick_size = Decimal::new(2, 3);
        rules.limit_quantity.step = decimal(1);
        rules.limit_quantity.min = decimal(1);
        rules.market_quantity = rules.limit_quantity.clone();
        rules.min_notional = Decimal::new(39, 0);

        assert_eq!(
            build_grid_plan(&config, &market(Decimal::new(40, 2)), &rules),
            Err(GridPlanError::NotionalBelowMinimum {
                level_index: Some(0)
            })
        );
    }

    #[test]
    fn geometric_levels_are_decimal_monotonic_and_keep_exact_endpoints() {
        let mut config = fixed_config(Direction::Neutral);
        config.grid_mode = GridMode::Geometric;
        config.initial_order_price = None;
        let plan = build_grid_plan(&config, &market(decimal(1010)), &rules()).unwrap();
        assert_eq!(plan.raw_levels.first(), Some(&decimal(1000)));
        assert_eq!(plan.raw_levels.last(), Some(&decimal(1020)));
        assert!(plan.levels.windows(2).all(|pair| pair[1] > pair[0]));
    }

    #[test]
    fn regular_limit_reference_is_the_actual_quantized_exchange_price() {
        let mut config = fixed_config(Direction::Short);
        config.initial_order_price = Some(Decimal::new(101409, 2));
        let plan = build_grid_plan(&config, &market(decimal(1012)), &rules()).unwrap();

        assert_eq!(plan.reference_price, decimal(1014));
        assert_eq!(plan.active_grid_count, 14);
        assert_eq!(plan.total_quantity, Decimal::new(28, 1));
        assert_eq!(plan.opening_order.unwrap().price, Some(decimal(1014)));
    }

    #[test]
    fn neutral_plan_has_no_opening_order_and_keeps_both_sides_in_the_current_interval() {
        let mut config = fixed_config(Direction::Neutral);
        config.initial_order_price = None;
        let plan = build_grid_plan(&config, &market(Decimal::new(10145, 1)), &rules()).unwrap();

        assert!(plan.opening_order.is_none());
        assert_eq!(plan.active_grid_count, 21);
        assert_eq!(plan.grid_orders.len(), 21);
        assert_eq!(plan.participating_level_count, 20);
        let interval_orders = plan
            .grid_orders
            .iter()
            .filter(|order| order.level_index == 14)
            .collect::<Vec<_>>();
        assert_eq!(interval_orders.len(), 2);
        assert_ne!(interval_orders[0].side, interval_orders[1].side);
    }

    #[test]
    fn neutral_investment_allocation_keeps_distinct_same_interval_quantities() {
        let reference = Decimal::new(10145, 1);
        let mut config = fixed_config(Direction::Neutral);
        config.position_sizing_mode = PositionSizingMode::Investment;
        config.grid_order_qty = None;
        config.leverage = 1;
        config.total_investment = reference.checked_mul(Decimal::new(246, 2)).unwrap();
        config.initial_order_price = None;
        let mut instrument = rules();
        instrument.limit_quantity.step = Decimal::new(1, 2);
        instrument.limit_quantity.min = Decimal::new(1, 2);

        let plan = build_grid_plan(&config, &market(reference), &instrument).unwrap();
        let interval_orders = plan
            .grid_orders
            .iter()
            .filter(|order| order.level_index == 14)
            .collect::<Vec<_>>();

        assert_eq!(plan.total_quantity, Decimal::new(246, 2));
        assert_eq!(interval_orders.len(), 2);
        assert_eq!(interval_orders[0].quantity, Decimal::new(12, 2));
        assert_eq!(interval_orders[1].quantity, Decimal::new(11, 2));
        assert_eq!(
            plan.grid_orders
                .iter()
                .map(|order| order.quantity)
                .sum::<Decimal>(),
            plan.total_quantity
        );
    }

    #[test]
    fn investment_market_fails_when_market_quantity_cannot_feed_limit_legs() {
        let mut config = fixed_config(Direction::Short);
        config.position_sizing_mode = PositionSizingMode::Investment;
        config.grid_order_qty = None;
        config.total_investment = decimal(500);
        config.initial_order_type = InitialOrderType::Market;
        config.initial_order_price = None;
        let mut instrument = rules();
        instrument.market_quantity.step = Decimal::new(7, 2);
        instrument.market_quantity.min = Decimal::new(7, 2);

        assert_eq!(
            build_grid_plan(&config, &market(decimal(1011)), &instrument),
            Err(GridPlanError::MarketLimitStepMismatch)
        );
    }

    #[test]
    fn fixed_quantity_below_exchange_minimum_is_never_inflated() {
        let config = fixed_config(Direction::Short);
        let mut instrument = rules();
        instrument.limit_quantity.min = Decimal::new(3, 1);

        assert_eq!(
            build_grid_plan(&config, &market(decimal(1012)), &instrument),
            Err(GridPlanError::QuantityBelowMinimum {
                context: "fixed per-grid"
            })
        );
    }

    #[test]
    fn fixed_grid_table_covers_all_directions_and_two_hundred_reference_prices() {
        for direction in [Direction::Long, Direction::Short, Direction::Neutral] {
            for tenth in 10_001..10_200 {
                let reference = Decimal::new(tenth, 1);
                let mut config = fixed_config(direction);
                config.initial_order_type = InitialOrderType::Limit;
                config.initial_order_price = Some(reference);
                let plan = build_grid_plan(&config, &market(reference), &rules()).unwrap();

                assert!(plan.levels.windows(2).all(|pair| pair[1] > pair[0]));
                assert_eq!(plan.participating_level_count, 20);
                assert!(
                    plan.grid_orders
                        .iter()
                        .all(|order| order.quantity == Decimal::new(2, 1))
                );
                let identities = plan
                    .grid_orders
                    .iter()
                    .map(|order| (order.level_index, order.side, order.reduce_only))
                    .collect::<BTreeSet<_>>();
                assert_eq!(identities.len(), plan.grid_orders.len());

                if direction == Direction::Neutral {
                    assert!(plan.opening_order.is_none());
                } else {
                    let protected = plan
                        .grid_orders
                        .iter()
                        .filter(|order| order.reduce_only)
                        .map(|order| order.quantity)
                        .sum::<Decimal>();
                    assert_eq!(plan.opening_order.as_ref().unwrap().quantity, protected);
                    assert_eq!(plan.total_quantity, protected);
                }
            }
        }
    }

    #[test]
    fn market_maximum_is_authoritative() {
        let mut config = fixed_config(Direction::Short);
        config.initial_order_type = InitialOrderType::Market;
        config.initial_order_price = None;
        let mut rules = rules();
        rules.market_quantity.max = Some(decimal(2));
        assert_eq!(
            build_grid_plan(&config, &market(decimal(1011)), &rules),
            Err(GridPlanError::QuantityAboveMaximum {
                context: "initial market"
            })
        );
    }
}
