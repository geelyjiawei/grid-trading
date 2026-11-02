use std::collections::BTreeSet;

use rust_decimal::Decimal;

use crate::{
    domain::{Exchange, OrderSide},
    exchange::{
        AccountBalanceSnapshotGateway, PositionSnapshot, SnapshotError,
        funding::{
            FundingAccountFill, FundingAttribution, FundingHistorySnapshot, attribute_funding,
        },
    },
};

use super::StrategyState;

#[derive(Clone, Copy)]
pub(crate) struct StrategyProfitMetrics {
    pub(crate) realized_net_profit: Decimal,
    pub(crate) grid_unrealized_profit: Decimal,
    pub(crate) total_equity_profit: Decimal,
    pub(crate) mark_price: Decimal,
}

pub(crate) fn validated_one_way_quantity(
    position: &PositionSnapshot,
    exchange: Exchange,
    symbol: &str,
) -> Result<Decimal, SnapshotError> {
    if position.exchange != exchange || position.symbol != symbol {
        return Err(SnapshotError::new(
            "position snapshot belongs to another exchange or symbol",
        ));
    }
    let (quantity, entry_price) = position.one_way_position()?;
    let leg = &position.legs[0];
    if leg.mark_price <= Decimal::ZERO
        || leg
            .leverage
            .is_some_and(|leverage| leverage <= Decimal::ZERO)
        || (quantity.is_zero() && entry_price.is_some_and(|price| price <= Decimal::ZERO))
        || (!quantity.is_zero() && entry_price.is_none_or(|price| price <= Decimal::ZERO))
    {
        return Err(SnapshotError::new(
            "one-way position contains an invalid mark price, entry price, or leverage",
        ));
    }
    Ok(quantity)
}

pub(crate) fn strategy_profit_metrics(
    strategy: &StrategyState,
    position: &PositionSnapshot,
) -> Result<StrategyProfitMetrics, SnapshotError> {
    let actual_position =
        validated_one_way_quantity(position, strategy.exchange, &strategy.symbol)?;
    let expected_position = strategy
        .expected_exchange_position()
        .map_err(|_| SnapshotError::new("strategy expected-position arithmetic is invalid"))?;
    if actual_position != expected_position {
        return Err(SnapshotError::new(
            "exchange position does not match the strategy-owned inventory ledger",
        ));
    }
    let mark_price = position
        .legs
        .first()
        .map(|leg| leg.mark_price)
        .ok_or_else(|| SnapshotError::new("position snapshot has no one-way mark price"))?;
    let realized_net_profit = strategy
        .gross_realized_profit
        .checked_sub(strategy.total_fee)
        .ok_or_else(|| SnapshotError::new("strategy realized profit arithmetic overflowed"))?;
    let grid_unrealized_profit = strategy
        .grid_unrealized_profit(mark_price)
        .map_err(|_| SnapshotError::new("strategy-owned inventory cannot be valued exactly"))?;
    let total_equity_profit = realized_net_profit
        .checked_add(grid_unrealized_profit)
        .ok_or_else(|| SnapshotError::new("strategy equity profit arithmetic overflowed"))?;
    Ok(StrategyProfitMetrics {
        realized_net_profit,
        grid_unrealized_profit,
        total_equity_profit,
        mark_price,
    })
}

pub(crate) async fn collect_strategy_funding_at<G: AccountBalanceSnapshotGateway + ?Sized>(
    gateway: &G,
    strategy: &StrategyState,
    through_ms: u64,
) -> Result<FundingAttribution, SnapshotError> {
    let history = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        gateway.funding_history(
            strategy.exchange,
            &strategy.symbol,
            strategy.created_at_ms,
            through_ms,
        ),
    )
    .await
    .map_err(|_| SnapshotError::new("funding history query timed out"))??;
    super::attribute_strategy_funding(strategy, &history)
}

pub(crate) fn attribute_strategy_funding(
    strategy: &StrategyState,
    history: &FundingHistorySnapshot,
) -> Result<FundingAttribution, SnapshotError> {
    let last_settlement = history.payments.iter().map(|payment| payment.at_ms).max();
    let mut owned = Vec::new();
    if let Some(last_settlement) = last_settlement {
        for event in strategy.inventory_events.values() {
            let at_ms = event.execution_time_ms.ok_or_else(|| {
                SnapshotError::new("funding attribution requires exact execution times")
            })?;
            if at_ms > last_settlement {
                continue;
            }
            let order = strategy
                .orders
                .get(&event.source_client_order_id)
                .ok_or_else(|| {
                    SnapshotError::new("funding execution has no strategy-owned order")
                })?;
            if event.quantity <= Decimal::ZERO {
                return Err(SnapshotError::new("funding execution quantity is invalid"));
            }
            owned.push(FundingAccountFill {
                trade_id: event.exchange_trade_id.clone().ok_or_else(|| {
                    SnapshotError::new("funding execution has no exact exchange trade id")
                })?,
                exchange_order_id: order.exchange_order_id.clone().ok_or_else(|| {
                    SnapshotError::new("funding execution has no exact exchange order id")
                })?,
                at_ms,
                signed_quantity: match order.shape.side {
                    OrderSide::Buy => event.quantity,
                    OrderSide::Sell => -event.quantity,
                },
            });
        }
        if let Some(account_fills) = &history.account_fills {
            let owned_orders = strategy
                .orders
                .values()
                .filter_map(|order| order.exchange_order_id.as_deref())
                .collect::<BTreeSet<_>>();
            let owned_trades = owned
                .iter()
                .map(|fill| fill.trade_id.as_str())
                .collect::<BTreeSet<_>>();
            // Legacy baselines have no exchange observation timestamp. External fills
            // after creation make that replay boundary unknowable; never double-count them.
            if account_fills.iter().any(|fill| {
                fill.at_ms <= last_settlement
                    && !owned_orders.contains(fill.exchange_order_id.as_str())
            }) {
                return Err(SnapshotError::new(
                    "funding baseline boundary is ambiguous after external executions",
                ));
            }
            if account_fills.iter().any(|fill| {
                fill.at_ms <= last_settlement
                    && owned_orders.contains(fill.exchange_order_id.as_str())
                    && !owned_trades.contains(fill.trade_id.as_str())
            }) {
                return Err(SnapshotError::new(
                    "funding settlement includes pending strategy executions",
                ));
            }
        }
    }
    attribute_funding(
        history,
        strategy.exchange,
        &strategy.symbol,
        strategy.created_at_ms,
        strategy.baseline.signed_quantity,
        &owned,
    )
}
