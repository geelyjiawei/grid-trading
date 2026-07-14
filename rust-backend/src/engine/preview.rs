use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{Direction, GridConfig, InstrumentRules, OrderKind, OrderSide, TimeInForce};

use super::{GridPlan, GridPlanError, MarketSnapshot, PlannedGridOrder, build_grid_plan};

#[derive(Debug, Clone, PartialEq)]
pub struct GridCycleEstimate {
    pub level_index: u16,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub grid_step: Decimal,
    pub gross_profit: Decimal,
    pub open_fee: Decimal,
    pub close_fee: Decimal,
    pub net_profit: Decimal,
    pub gross_profit_percent: Decimal,
    pub fee_rate: Decimal,
    pub maker_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GridPreviewMetrics {
    pub plan: GridPlan,
    pub cycles: Vec<GridCycleEstimate>,
    pub representative_cycle: GridCycleEstimate,
    pub grid_step_min: Decimal,
    pub grid_step_max: Decimal,
    pub gross_profit_percent_min: Decimal,
    pub gross_profit_percent_max: Decimal,
    pub net_profit_min: Decimal,
    pub net_profit_max: Decimal,
    pub quantity_min: Decimal,
    pub quantity_max: Decimal,
    pub quantity_average: Decimal,
    pub initial_open_fee_rate: Option<Decimal>,
    pub initial_open_fee: Option<Decimal>,
}

pub fn build_grid_preview(
    config: &GridConfig,
    market: &MarketSnapshot,
    rules: &InstrumentRules,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
) -> Result<GridPreviewMetrics, GridPreviewError> {
    validate_fee_rate(maker_fee_rate)?;
    validate_fee_rate(taker_fee_rate)?;
    let plan = build_grid_plan(config, market, rules)?;
    let mut cycles = plan
        .grid_orders
        .iter()
        .filter(|order| config.direction == Direction::Neutral || order.reduce_only)
        .map(|order| {
            cycle_estimate(
                config.direction,
                order,
                &plan,
                maker_fee_rate,
                taker_fee_rate,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if cycles.is_empty() {
        return Err(GridPreviewError::NoCycles);
    }
    cycles.sort_by_key(|cycle| (cycle.level_index, cycle.entry_price, cycle.exit_price));
    let representative_cycle = cycles
        .iter()
        .min_by_key(|cycle| cycle.net_profit)
        .cloned()
        .ok_or(GridPreviewError::NoCycles)?;
    let grid_step_min = minimum(cycles.iter().map(|cycle| cycle.grid_step))?;
    let grid_step_max = maximum(cycles.iter().map(|cycle| cycle.grid_step))?;
    let gross_profit_percent_min = minimum(cycles.iter().map(|cycle| cycle.gross_profit_percent))?;
    let gross_profit_percent_max = maximum(cycles.iter().map(|cycle| cycle.gross_profit_percent))?;
    let net_profit_min = minimum(cycles.iter().map(|cycle| cycle.net_profit))?;
    let net_profit_max = maximum(cycles.iter().map(|cycle| cycle.net_profit))?;
    let quantity_min = minimum(cycles.iter().map(|cycle| cycle.quantity))?;
    let quantity_max = maximum(cycles.iter().map(|cycle| cycle.quantity))?;
    let quantity_average = cycles
        .iter()
        .try_fold(Decimal::ZERO, |sum, cycle| sum.checked_add(cycle.quantity))
        .and_then(|sum| sum.checked_div(Decimal::from(cycles.len())))
        .ok_or(GridPreviewError::NumericOverflow("average cycle quantity"))?;
    let (initial_open_fee_rate, initial_open_fee) = match &plan.opening_order {
        Some(opening) => {
            let rate = if opening.kind == OrderKind::Market {
                taker_fee_rate
            } else {
                fee_rate(opening.time_in_force, maker_fee_rate, taker_fee_rate)
            };
            let price = opening.price.unwrap_or(market.mark_price);
            let fee = price
                .checked_mul(opening.quantity)
                .and_then(|notional| notional.checked_mul(rate))
                .ok_or(GridPreviewError::NumericOverflow("initial opening fee"))?;
            (Some(rate), Some(fee))
        }
        None => (None, None),
    };

    Ok(GridPreviewMetrics {
        plan,
        cycles,
        representative_cycle,
        grid_step_min,
        grid_step_max,
        gross_profit_percent_min,
        gross_profit_percent_max,
        net_profit_min,
        net_profit_max,
        quantity_min,
        quantity_max,
        quantity_average,
        initial_open_fee_rate,
        initial_open_fee,
    })
}

fn cycle_estimate(
    direction: Direction,
    order: &PlannedGridOrder,
    plan: &GridPlan,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
) -> Result<GridCycleEstimate, GridPreviewError> {
    let lower = *plan
        .levels
        .get(usize::from(order.level_index))
        .ok_or(GridPreviewError::MissingLevel(order.level_index))?;
    let upper = *plan
        .levels
        .get(usize::from(order.level_index) + 1)
        .ok_or(GridPreviewError::MissingLevel(order.level_index))?;
    let (entry_price, exit_price) = match direction {
        Direction::Long => (lower, upper),
        Direction::Short => (upper, lower),
        Direction::Neutral => match order.side {
            OrderSide::Buy => (lower, upper),
            OrderSide::Sell => (upper, lower),
        },
    };
    let grid_step = upper
        .checked_sub(lower)
        .ok_or(GridPreviewError::NumericOverflow("grid step"))?;
    let gross_profit = grid_step
        .checked_mul(order.quantity)
        .ok_or(GridPreviewError::NumericOverflow("cycle gross profit"))?;
    let rate = fee_rate(order.time_in_force, maker_fee_rate, taker_fee_rate);
    let open_fee = entry_price
        .checked_mul(order.quantity)
        .and_then(|notional| notional.checked_mul(rate))
        .ok_or(GridPreviewError::NumericOverflow("cycle open fee"))?;
    let close_fee = exit_price
        .checked_mul(order.quantity)
        .and_then(|notional| notional.checked_mul(rate))
        .ok_or(GridPreviewError::NumericOverflow("cycle close fee"))?;
    let total_fee = open_fee
        .checked_add(close_fee)
        .ok_or(GridPreviewError::NumericOverflow("cycle total fee"))?;
    let net_profit = gross_profit
        .checked_sub(total_fee)
        .ok_or(GridPreviewError::NumericOverflow("cycle net profit"))?;
    let gross_profit_percent = grid_step
        .checked_div(entry_price)
        .and_then(|ratio| ratio.checked_mul(Decimal::from(100)))
        .ok_or(GridPreviewError::NumericOverflow(
            "cycle gross profit percent",
        ))?;
    Ok(GridCycleEstimate {
        level_index: order.level_index,
        quantity: order.quantity,
        entry_price,
        exit_price,
        grid_step,
        gross_profit,
        open_fee,
        close_fee,
        net_profit,
        gross_profit_percent,
        fee_rate: rate,
        maker_only: order.time_in_force == TimeInForce::PostOnly,
    })
}

fn fee_rate(time_in_force: TimeInForce, maker: Decimal, taker: Decimal) -> Decimal {
    if time_in_force == TimeInForce::PostOnly {
        maker
    } else {
        taker
    }
}

fn validate_fee_rate(rate: Decimal) -> Result<(), GridPreviewError> {
    if rate < Decimal::ZERO || rate >= Decimal::ONE {
        Err(GridPreviewError::InvalidFeeRate)
    } else {
        Ok(())
    }
}

fn minimum(values: impl IntoIterator<Item = Decimal>) -> Result<Decimal, GridPreviewError> {
    values.into_iter().min().ok_or(GridPreviewError::NoCycles)
}

fn maximum(values: impl IntoIterator<Item = Decimal>) -> Result<Decimal, GridPreviewError> {
    values.into_iter().max().ok_or(GridPreviewError::NoCycles)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GridPreviewError {
    #[error(transparent)]
    Plan(#[from] GridPlanError),
    #[error("fee rates must be non-negative and below one")]
    InvalidFeeRate,
    #[error("the plan has no complete grid cycles")]
    NoCycles,
    #[error("the plan is missing grid level {0}")]
    MissingLevel(u16),
    #[error("numeric overflow while calculating {0}")]
    NumericOverflow(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Exchange, GridMode, InitialOrderType, PositionSizingMode, QuantityRules};

    fn config(post_only: bool) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction: Direction::Short,
            upper_price: Decimal::from(1020),
            lower_price: Decimal::from(1000),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(2, 1)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: Some(Decimal::from(1014)),
            grid_order_post_only: post_only,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn market() -> MarketSnapshot {
        MarketSnapshot {
            last_price: Decimal::from(1012),
            mark_price: Decimal::from(1012),
        }
    }

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::ONE,
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
    fn fixed_quantity_preview_uses_exact_plan_quantity_without_redistribution() {
        let preview = build_grid_preview(
            &config(false),
            &market(),
            &rules(),
            Decimal::new(2, 4),
            Decimal::new(5, 4),
        )
        .unwrap();

        assert_eq!(preview.plan.active_grid_count, 14);
        assert_eq!(preview.plan.total_quantity, Decimal::new(28, 1));
        assert_eq!(preview.quantity_min, Decimal::new(2, 1));
        assert_eq!(preview.quantity_max, Decimal::new(2, 1));
        assert_eq!(preview.quantity_average, Decimal::new(2, 1));
        assert!(preview.cycles.iter().all(|cycle| {
            cycle.quantity == Decimal::new(2, 1)
                && cycle.fee_rate == Decimal::new(5, 4)
                && !cycle.maker_only
        }));
    }

    #[test]
    fn post_only_preview_uses_maker_fee_while_gtc_is_conservative_taker() {
        let maker = build_grid_preview(
            &config(true),
            &market(),
            &rules(),
            Decimal::new(2, 4),
            Decimal::new(5, 4),
        )
        .unwrap();
        let taker = build_grid_preview(
            &config(false),
            &market(),
            &rules(),
            Decimal::new(2, 4),
            Decimal::new(5, 4),
        )
        .unwrap();

        assert!(maker.cycles.iter().all(|cycle| cycle.maker_only));
        assert!(maker.net_profit_min > taker.net_profit_min);
        assert_eq!(maker.representative_cycle.fee_rate, Decimal::new(2, 4));
        assert_eq!(taker.representative_cycle.fee_rate, Decimal::new(5, 4));
    }

    #[test]
    fn invalid_fee_rate_never_generates_a_preview() {
        assert_eq!(
            build_grid_preview(
                &config(false),
                &market(),
                &rules(),
                Decimal::new(-1, 4),
                Decimal::new(5, 4),
            ),
            Err(GridPreviewError::InvalidFeeRate)
        );
    }
}
