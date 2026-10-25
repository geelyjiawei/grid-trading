use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::{domain::Exchange, exchange::SnapshotError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingPayment {
    pub id: String,
    pub at_ms: u64,
    pub amount: Decimal,
    pub asset: String,
    pub position_quantity: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingAccountFill {
    pub trade_id: String,
    pub exchange_order_id: String,
    pub at_ms: u64,
    pub signed_quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingHistorySnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub from_ms: u64,
    pub through_ms: u64,
    pub payments: Vec<FundingPayment>,
    pub account_fills: Option<Vec<FundingAccountFill>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FundingAttribution {
    pub net_income: Decimal,
    pub payment_count: usize,
    pub through_ms: u64,
}

pub fn attribute_funding(
    history: &FundingHistorySnapshot,
    exchange: Exchange,
    symbol: &str,
    start_ms: u64,
    baseline_quantity: Decimal,
    owned_fills: &[FundingAccountFill],
) -> Result<FundingAttribution, SnapshotError> {
    if history.exchange != exchange
        || history.symbol != symbol
        || history.from_ms != start_ms
        || history.through_ms < start_ms
    {
        return Err(SnapshotError::new(
            "funding history identity or window is invalid",
        ));
    }
    let asset = if exchange == Exchange::TradeXyz {
        "USDC"
    } else {
        "USDT"
    };
    let mut receipts = BTreeMap::new();
    for payment in &history.payments {
        if payment.id.trim().is_empty()
            || payment.asset != asset
            || payment.at_ms < start_ms
            || payment.at_ms > history.through_ms
        {
            return Err(SnapshotError::new(
                "funding receipt identity, currency or time is invalid",
            ));
        }
        if receipts
            .insert(&payment.id, payment)
            .is_some_and(|previous| previous != payment)
        {
            return Err(SnapshotError::new(
                "funding receipt has conflicting duplicate evidence",
            ));
        }
    }
    let mut payments = receipts.into_values().collect::<Vec<_>>();
    payments.sort_by_key(|payment| payment.at_ms);
    let mut result = FundingAttribution {
        net_income: Decimal::ZERO,
        payment_count: 0,
        through_ms: history.through_ms,
    };
    let Some(last_payment_ms) = payments.last().map(|payment| payment.at_ms) else {
        return Ok(result);
    };
    let owned = unique_fills(owned_fills, start_ms, last_payment_ms)?;
    let account = history
        .account_fills
        .as_ref()
        .map(|fills| unique_fills(fills, start_ms, last_payment_ms))
        .transpose()?;
    if let Some(account) = &account {
        let by_id = account
            .iter()
            .map(|fill| (&fill.trade_id, *fill))
            .collect::<BTreeMap<_, _>>();
        for fill in &owned {
            if by_id
                .get(&fill.trade_id)
                .is_none_or(|observed| *observed != *fill)
            {
                return Err(SnapshotError::new(
                    "strategy execution is absent from funding account history",
                ));
            }
        }
        let owned_orders = owned
            .iter()
            .map(|fill| &fill.exchange_order_id)
            .collect::<std::collections::BTreeSet<_>>();
        let owned_ids = owned
            .iter()
            .map(|fill| &fill.trade_id)
            .collect::<std::collections::BTreeSet<_>>();
        if account.iter().any(|fill| {
            owned_orders.contains(&fill.exchange_order_id) && !owned_ids.contains(&fill.trade_id)
        }) {
            return Err(SnapshotError::new(
                "funding account history contains unaccounted strategy executions",
            ));
        }
    }
    let mut owned_cursor = 0;
    let mut account_cursor = 0;
    let mut owned_quantity = Decimal::ZERO;
    let mut replayed_account = baseline_quantity;
    for payment in payments {
        advance_quantity(
            &owned,
            &mut owned_cursor,
            &mut owned_quantity,
            payment.at_ms,
        )?;
        if let Some(account) = &account {
            advance_quantity(
                account,
                &mut account_cursor,
                &mut replayed_account,
                payment.at_ms,
            )?;
        }
        let account_quantity = match (payment.position_quantity, &account) {
            (Some(quantity), Some(_)) if quantity != replayed_account => {
                return Err(SnapshotError::new(
                    "funding receipt position disagrees with account executions",
                ));
            }
            (Some(quantity), _) => quantity,
            (None, Some(_)) => replayed_account,
            (None, None) => {
                return Err(SnapshotError::new(
                    "funding account position evidence is missing",
                ));
            }
        };
        if account.is_none()
            && baseline_quantity.checked_add(owned_quantity) != Some(account_quantity)
        {
            return Err(SnapshotError::new(
                "funding receipt position does not match the complete owned ledger and baseline",
            ));
        }
        if owned_quantity.is_zero() {
            continue;
        }
        if account_quantity.is_zero()
            || owned_quantity.is_sign_negative() != account_quantity.is_sign_negative()
            || owned_quantity.abs() > account_quantity.abs()
        {
            return Err(SnapshotError::new(
                "funding cannot be attributed across netted manual exposure",
            ));
        }
        // Share actual settled cash flow, never a forecast rate or the whole account charge.
        let share = payment
            .amount
            .checked_mul(owned_quantity)
            .and_then(|value| value.checked_div(account_quantity))
            .ok_or_else(|| SnapshotError::new("funding attribution arithmetic overflowed"))?;
        result.net_income = result
            .net_income
            .checked_add(share)
            .ok_or_else(|| SnapshotError::new("funding total arithmetic overflowed"))?;
        result.payment_count += 1;
    }
    Ok(result)
}

fn unique_fills(
    fills: &[FundingAccountFill],
    from_ms: u64,
    through_ms: u64,
) -> Result<Vec<&FundingAccountFill>, SnapshotError> {
    let mut unique = BTreeMap::new();
    for fill in fills.iter().filter(|fill| fill.at_ms <= through_ms) {
        if fill.at_ms < from_ms
            || fill.trade_id.trim().is_empty()
            || fill.exchange_order_id.trim().is_empty()
            || fill.signed_quantity.is_zero()
        {
            return Err(SnapshotError::new("funding execution evidence is invalid"));
        }
        if unique
            .insert(&fill.trade_id, fill)
            .is_some_and(|previous| previous != fill)
        {
            return Err(SnapshotError::new(
                "funding execution has conflicting duplicate evidence",
            ));
        }
    }
    let mut ordered = unique.into_values().collect::<Vec<_>>();
    ordered.sort_by_key(|fill| fill.at_ms);
    Ok(ordered)
}

fn advance_quantity(
    fills: &[&FundingAccountFill],
    cursor: &mut usize,
    quantity: &mut Decimal,
    settlement_ms: u64,
) -> Result<(), SnapshotError> {
    while let Some(fill) = fills
        .get(*cursor)
        .filter(|fill| fill.at_ms <= settlement_ms)
    {
        if fill.at_ms == settlement_ms {
            return Err(SnapshotError::new(
                "funding settlement and execution ordering is ambiguous",
            ));
        }
        *quantity = quantity
            .checked_add(fill.signed_quantity)
            .ok_or_else(|| SnapshotError::new("funding position arithmetic overflowed"))?;
        *cursor += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(id: &str, time: u64, quantity: i64) -> FundingAccountFill {
        FundingAccountFill {
            trade_id: id.into(),
            exchange_order_id: format!("order-{id}"),
            at_ms: time,
            signed_quantity: Decimal::from(quantity),
        }
    }

    fn fixture() -> (FundingHistorySnapshot, Vec<FundingAccountFill>) {
        let owned = vec![fill("owned", 110, -20)];
        (
            FundingHistorySnapshot {
                exchange: Exchange::Binance,
                symbol: "QUSDT".into(),
                from_ms: 100,
                through_ms: 300,
                payments: vec![FundingPayment {
                    id: "funding".into(),
                    at_ms: 200,
                    amount: Decimal::from(-3),
                    asset: "USDT".into(),
                    position_quantity: None,
                }],
                account_fills: Some(owned.clone()),
            },
            owned,
        )
    }

    fn calculate(
        history: &FundingHistorySnapshot,
        fills: &[FundingAccountFill],
    ) -> Result<FundingAttribution, SnapshotError> {
        attribute_funding(
            history,
            Exchange::Binance,
            "QUSDT",
            100,
            Decimal::from(-10),
            fills,
        )
    }

    #[test]
    fn funding_replays_manual_account_fills_and_deduplicates_receipts() {
        let (mut history, owned) = fixture();
        history
            .account_fills
            .as_mut()
            .unwrap()
            .push(fill("manual", 120, -30));
        history.payments.push(history.payments[0].clone());
        let result = calculate(&history, &owned).unwrap();
        assert_eq!(result.net_income, Decimal::from(-1));
        assert_eq!(result.payment_count, 1);
    }

    #[test]
    fn funding_income_is_added_and_closed_inventory_gets_no_later_payment() {
        let (mut history, mut owned) = fixture();
        history.payments[0].amount = Decimal::from(3);
        owned.push(fill("close", 210, 20));
        history.account_fills = Some(owned.clone());
        history.payments.push(FundingPayment {
            id: "later".into(),
            at_ms: 250,
            amount: Decimal::from(1),
            asset: "USDT".into(),
            position_quantity: Some(Decimal::from(-10)),
        });
        assert_eq!(
            calculate(&history, &owned).unwrap().net_income,
            Decimal::from(2)
        );
    }

    #[test]
    fn funding_exact_receipt_position_does_not_require_account_trade_history() {
        let (mut history, owned) = fixture();
        history.account_fills = None;
        history.payments[0].position_quantity = Some(Decimal::from(-30));
        assert_eq!(
            calculate(&history, &owned).unwrap().net_income,
            Decimal::from(-2)
        );
    }

    #[test]
    fn funding_exact_receipt_rejects_missing_execution_or_changed_external_position() {
        let (mut history, owned) = fixture();
        history.account_fills = None;
        history.payments[0].position_quantity = Some(Decimal::from(-40));
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_empty_verified_history_means_zero_not_an_estimated_charge() {
        let (mut history, owned) = fixture();
        history.payments.clear();
        history.account_fills = None;
        assert_eq!(
            calculate(&history, &owned).unwrap().net_income,
            Decimal::ZERO
        );
    }

    #[test]
    fn funding_rejects_missing_or_conflicting_account_execution_evidence() {
        let (mut history, owned) = fixture();
        history.account_fills = Some(vec![]);
        assert!(calculate(&history, &owned).is_err());
        history.account_fills = Some(vec![
            owned[0].clone(),
            FundingAccountFill {
                signed_quantity: Decimal::from(-21),
                ..owned[0].clone()
            },
        ]);
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_rejects_conflicting_receipts_or_wrong_currency() {
        let (mut history, owned) = fixture();
        history.payments.push(FundingPayment {
            amount: Decimal::from(-4),
            ..history.payments[0].clone()
        });
        assert!(calculate(&history, &owned).is_err());
        history.payments.pop();
        history.payments[0].asset = "BNB".into();
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_rejects_netting_opposite_manual_exposure_or_overallocation() {
        let (mut history, owned) = fixture();
        history.account_fills = None;
        history.payments[0].position_quantity = Some(Decimal::from(30));
        assert!(calculate(&history, &owned).is_err());
        history.payments[0].position_quantity = Some(Decimal::from(-10));
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_rejects_exact_settlement_timestamp_execution_ambiguity() {
        let (mut history, owned) = fixture();
        history
            .account_fills
            .as_mut()
            .unwrap()
            .push(fill("manual", 200, -1));
        assert!(calculate(&history, &owned).is_err());
        history.account_fills = None;
        history.payments[0].position_quantity = Some(Decimal::from(-30));
        let mut ambiguous = owned;
        ambiguous[0].at_ms = 200;
        assert!(calculate(&history, &ambiguous).is_err());
    }

    #[test]
    fn funding_rejects_receipt_position_disagreeing_with_replayed_account() {
        let (mut history, owned) = fixture();
        history.payments[0].position_quantity = Some(Decimal::from(-40));
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_rejects_wrong_market_window_and_future_payment() {
        let (mut history, owned) = fixture();
        history.symbol = "BTCUSDT".into();
        assert!(calculate(&history, &owned).is_err());
        history.symbol = "QUSDT".into();
        history.from_ms = 101;
        assert!(calculate(&history, &owned).is_err());
        history.from_ms = 100;
        history.payments[0].at_ms = 301;
        assert!(calculate(&history, &owned).is_err());
    }

    #[test]
    fn funding_attributes_actual_payment_without_charging_the_baseline() {
        let owned = FundingAccountFill {
            trade_id: "owned-1".into(),
            exchange_order_id: "order-1".into(),
            at_ms: 110,
            signed_quantity: Decimal::from(-20),
        };
        let history = FundingHistorySnapshot {
            exchange: Exchange::Binance,
            symbol: "QUSDT".into(),
            from_ms: 100,
            through_ms: 300,
            payments: vec![FundingPayment {
                id: "funding-1".into(),
                at_ms: 200,
                amount: Decimal::from(-3),
                asset: "USDT".into(),
                position_quantity: Some(Decimal::from(-30)),
            }],
            account_fills: Some(vec![owned.clone()]),
        };
        let result = attribute_funding(
            &history,
            Exchange::Binance,
            "QUSDT",
            100,
            Decimal::from(-10),
            &[owned],
        )
        .unwrap();
        assert_eq!(result.net_income, Decimal::from(-2));
        assert_eq!(result.payment_count, 1);
    }
}
