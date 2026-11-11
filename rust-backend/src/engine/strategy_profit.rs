use std::collections::BTreeSet;

use rust_decimal::Decimal;

use crate::{
    domain::{Exchange, OrderSide},
    exchange::{
        AccountBalanceSnapshotGateway, PositionSnapshot, PositionSnapshotGateway, SnapshotError,
        funding::{
            FundingAccountFill, FundingAttribution, FundingHistorySnapshot, attribute_funding,
        },
    },
};

use super::StrategyState;

#[cfg(test)]
mod manual_funding_tests {
    use super::*;
    use crate::exchange::{
        AccountPositionHistory, PositionSnapshotGateway, funding::FundingPayment,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct AccountHistoryGateway(AtomicUsize);

    #[async_trait::async_trait]
    impl PositionSnapshotGateway for AccountHistoryGateway {
        async fn position_snapshot(
            &self,
            _: Exchange,
            _: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            panic!("funding history enrichment must not read or mutate the position");
        }
        async fn account_position_history(
            &self,
            exchange: Exchange,
            symbol: &str,
            from_ms: u64,
            through_ms: u64,
        ) -> Result<AccountPositionHistory, SnapshotError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                (exchange, symbol, from_ms, through_ms),
                (Exchange::Bybit, "QUSDT", 100, 180)
            );
            Ok(AccountPositionHistory {
                exchange,
                symbol: symbol.into(),
                from_ms,
                through_ms,
                fills: vec![FundingAccountFill {
                    trade_id: "manual-opaque-001".into(),
                    exchange_order_id: "manual-order-001".into(),
                    at_ms: 160,
                    signed_quantity: Decimal::new(2, 1),
                }],
            })
        }
    }

    #[tokio::test]
    async fn manual_funding_enriches_missing_account_history_only_through_last_receipt() {
        let gateway = AccountHistoryGateway(AtomicUsize::new(0));
        let history = FundingHistorySnapshot {
            exchange: Exchange::Bybit,
            symbol: "QUSDT".into(),
            from_ms: 100,
            through_ms: 200,
            payments: vec![FundingPayment {
                id: "receipt".into(),
                at_ms: 180,
                amount: Decimal::new(-31, 1),
                asset: "USDT".into(),
                position_quantity: Some(Decimal::new(-31, 1)),
            }],
            account_fills: None,
        };
        let untouched = complete_funding_account_history(&gateway, history.clone(), false)
            .await
            .unwrap();
        assert_eq!(untouched, history);
        assert_eq!(gateway.0.load(Ordering::SeqCst), 0);
        let enriched = complete_funding_account_history(&gateway, history.clone(), true)
            .await
            .unwrap();
        assert_eq!(enriched.through_ms, 200);
        assert_eq!(enriched.payments, history.payments);
        assert_eq!(
            enriched.account_fills.unwrap()[0].trade_id,
            "manual-opaque-001"
        );
        assert_eq!(gateway.0.load(Ordering::SeqCst), 1);
    }
}

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
    if strategy.external_position_blocker.is_some() {
        return Err(SnapshotError::new(
            "external inventory ownership is awaiting reconciliation",
        ));
    }
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

pub(crate) async fn collect_strategy_funding_at<
    G: AccountBalanceSnapshotGateway + PositionSnapshotGateway + ?Sized,
>(
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
    let history =
        complete_funding_account_history(gateway, history, !strategy.external_fills.is_empty())
            .await?;
    super::attribute_strategy_funding(strategy, &history)
}

pub(crate) async fn complete_funding_account_history<G: PositionSnapshotGateway + ?Sized>(
    gateway: &G,
    mut history: FundingHistorySnapshot,
    external_activity: bool,
) -> Result<FundingHistorySnapshot, SnapshotError> {
    let Some(last_settlement) = history.payments.iter().map(|payment| payment.at_ms).max() else {
        return Ok(history);
    };
    if !external_activity || history.account_fills.is_some() {
        return Ok(history);
    }
    let account = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.account_position_history(
            history.exchange,
            &history.symbol,
            history.from_ms,
            last_settlement,
        ),
    )
    .await
    .map_err(|_| SnapshotError::new("funding account execution history timed out"))??;
    if account.exchange != history.exchange
        || account.symbol != history.symbol
        || account.from_ms != history.from_ms
        || account.through_ms != last_settlement
    {
        return Err(SnapshotError::new(
            "funding account history identity or coverage is invalid",
        ));
    }
    history.account_fills = Some(account.fills);
    Ok(history)
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
            // Only position-reconciled external identities can extend the immutable
            // baseline. Never absorb a funding-only fill into the manual budget.
            if account_fills.iter().any(|fill| {
                fill.at_ms <= last_settlement
                    && !owned_orders.contains(fill.exchange_order_id.as_str())
                    && strategy.external_fills.get(&fill.trade_id) != Some(fill)
            }) {
                return Err(SnapshotError::new(
                    "funding baseline boundary is ambiguous after external executions",
                ));
            }
            if strategy
                .external_fills
                .values()
                .any(|fill| fill.at_ms <= last_settlement && !account_fills.contains(fill))
            {
                return Err(SnapshotError::new(
                    "funding history omits confirmed external executions",
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
