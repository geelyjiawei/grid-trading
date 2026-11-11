use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::{
    domain::{Direction, IntentState, OrderSide},
    exchange::{
        AccountPositionHistory, compare_trade_chronology, funding::FundingAccountFill,
        is_valid_trade_id,
    },
};

use super::{StrategyOrderTracking, StrategyState, StrategyStateError};

fn invalid(message: &str) -> StrategyStateError {
    StrategyStateError::InvalidExternalInventory(message.into())
}

pub(super) fn replay_manual_quantity(
    state: &StrategyState,
    fills: &BTreeMap<String, FundingAccountFill>,
    through_ms: u64,
) -> Result<Decimal, StrategyStateError> {
    let mut ordered = fills.iter().collect::<Vec<_>>();
    ordered.sort_by(|(_, left), (_, right)| {
        compare_trade_chronology(left.at_ms, &left.trade_id, right.at_ms, &right.trade_id)
    });
    let mut quantity = state.baseline.signed_quantity;
    let mut previous: Option<&FundingAccountFill> = None;
    for (key, fill) in ordered {
        if key != &fill.trade_id
            || !is_valid_trade_id(key)
            || !is_valid_trade_id(&fill.exchange_order_id)
            || fill.signed_quantity.is_zero()
            || fill.at_ms <= state.created_at_ms
            || fill.at_ms > through_ms
        {
            return Err(invalid(
                "external fill identity, boundary or quantity is invalid",
            ));
        }
        if previous.is_some_and(|previous| {
            previous.at_ms == fill.at_ms
                && previous.signed_quantity.is_sign_negative()
                    != fill.signed_quantity.is_sign_negative()
        }) {
            return Err(invalid(
                "opposite external fills share an ambiguous execution timestamp",
            ));
        }
        quantity = quantity
            .checked_add(fill.signed_quantity)
            .ok_or_else(|| invalid("manual inventory quantity overflowed"))?;
        let compatible = match state.direction {
            Direction::Long => quantity >= Decimal::ZERO,
            Direction::Short => quantity <= Decimal::ZERO,
            Direction::Neutral => quantity.is_zero(),
        };
        if !compatible {
            return Err(invalid(
                "external execution exceeds the manual inventory budget or opposes the grid",
            ));
        }
        previous = Some(fill);
    }
    Ok(quantity)
}

pub(super) fn validate_persisted(state: &StrategyState) -> Result<(), StrategyStateError> {
    if state
        .external_position_blocker
        .as_ref()
        .is_some_and(|message| message.trim().is_empty() || message.len() > 4096)
    {
        return Err(invalid("external inventory blocker is invalid"));
    }
    let Some(through_ms) = state.external_history_through_ms else {
        return if state.external_fills.is_empty() && state.external_position_delta.is_zero() {
            Ok(())
        } else {
            Err(invalid(
                "external inventory has no verified history boundary",
            ))
        };
    };
    if through_ms < state.created_at_ms || through_ms > state.updated_at_ms {
        return Err(invalid("external history boundary is invalid"));
    }
    let manual = replay_manual_quantity(state, &state.external_fills, through_ms)?;
    if manual.checked_sub(state.baseline.signed_quantity) != Some(state.external_position_delta) {
        return Err(invalid(
            "external inventory total disagrees with execution evidence",
        ));
    }
    let owned_orders = state
        .orders
        .values()
        .filter_map(|order| order.exchange_order_id.as_ref())
        .collect::<BTreeSet<_>>();
    let owned_trades = state
        .orders
        .values()
        .filter_map(|order| order.execution_audit.as_ref())
        .flat_map(|audit| &audit.snapshot.trades)
        .map(|trade| &trade.trade_id)
        .collect::<BTreeSet<_>>();
    if state.external_fills.values().any(|fill| {
        owned_orders.contains(&fill.exchange_order_id) || owned_trades.contains(&fill.trade_id)
    }) {
        return Err(invalid(
            "an external fill conflicts with a strategy-owned identity",
        ));
    }
    Ok(())
}

pub(super) fn validate_extension(
    previous: &StrategyState,
    next: &StrategyState,
) -> Result<(), StrategyStateError> {
    if previous
        .external_fills
        .iter()
        .any(|(key, fill)| next.external_fills.get(key) != Some(fill))
        || previous.external_history_through_ms.is_some_and(|through| {
            next.external_history_through_ms
                .is_none_or(|next| next < through)
        })
    {
        return Err(invalid(
            "confirmed external inventory evidence cannot be removed or rewritten",
        ));
    }
    Ok(())
}

pub(super) fn reconcile_history(
    state: &StrategyState,
    history: &AccountPositionHistory,
    actual_quantity: Decimal,
    now_ms: u64,
) -> Result<(BTreeMap<String, FundingAccountFill>, Decimal), StrategyStateError> {
    if history.exchange != state.exchange
        || history.symbol != state.symbol
        || (history.from_ms != state.created_at_ms
            && state.external_history_through_ms != Some(history.from_ms))
        || history.through_ms < history.from_ms
        || history.through_ms > now_ms
        || state
            .external_history_through_ms
            .is_some_and(|previous| history.through_ms < previous)
    {
        return Err(invalid("account history identity or coverage is invalid"));
    }
    if state.orders.values().any(|order| {
        matches!(
            order.tracking,
            StrategyOrderTracking::Intent {
                state: IntentState::Prepared
                    | IntentState::SubmitUnknown { .. }
                    | IntentState::OwnershipConflict { .. }
            }
        )
    }) {
        return Err(invalid(
            "owned submissions are unresolved; external ownership cannot be proven",
        ));
    }
    let mut owned_orders = BTreeSet::new();
    let mut owned_trades = BTreeMap::new();
    for order in state.orders.values() {
        if let Some(id) = &order.exchange_order_id {
            owned_orders.insert(id.as_str());
        }
        if let Some(audit) = &order.execution_audit {
            for trade in &audit.snapshot.trades {
                let signed_quantity = match trade.side {
                    OrderSide::Buy => trade.quantity,
                    OrderSide::Sell => -trade.quantity,
                };
                let fill = FundingAccountFill {
                    trade_id: trade.trade_id.clone(),
                    exchange_order_id: trade.exchange_order_id.clone(),
                    at_ms: trade.trade_time_ms,
                    signed_quantity,
                };
                if fill.at_ms >= history.from_ms && fill.at_ms <= history.through_ms {
                    owned_trades.insert(fill.trade_id.clone(), fill);
                } else if fill.at_ms > history.through_ms {
                    return Err(invalid(
                        "account history predates already-accounted owned executions",
                    ));
                }
            }
        } else if order.cumulative_quantity > Decimal::ZERO {
            return Err(invalid("legacy owned executions lack exact trade evidence"));
        }
    }
    let mut account = BTreeMap::new();
    for fill in &history.fills {
        if !is_valid_trade_id(&fill.trade_id)
            || !is_valid_trade_id(&fill.exchange_order_id)
            || fill.signed_quantity.is_zero()
            || fill.at_ms < history.from_ms
            || fill.at_ms > history.through_ms
        {
            return Err(invalid(
                "account execution identity, quantity or time is invalid",
            ));
        }
        if account
            .insert(fill.trade_id.clone(), fill.clone())
            .is_some_and(|previous| previous != *fill)
        {
            return Err(invalid(
                "account execution has conflicting duplicate evidence",
            ));
        }
    }
    if owned_trades
        .iter()
        .any(|(key, fill)| account.get(key) != Some(fill))
    {
        return Err(invalid("complete account history omits an owned execution"));
    }
    let mut external = state.external_fills.clone();
    if state
        .external_fills
        .values()
        .any(|fill| fill.at_ms >= history.from_ms && account.get(&fill.trade_id) != Some(fill))
    {
        return Err(invalid(
            "account history omits confirmed external executions in its coverage interval",
        ));
    }
    for (key, fill) in account {
        if let Some(owned) = owned_trades.get(&key) {
            if owned != &fill {
                return Err(invalid(
                    "account execution conflicts with an owned trade identity",
                ));
            }
        } else if owned_orders.contains(fill.exchange_order_id.as_str()) {
            return Err(invalid(
                "account history contains a pending owned execution",
            ));
        } else if external
            .insert(key, fill.clone())
            .is_some_and(|previous| previous != fill)
        {
            return Err(invalid(
                "account history rewrites confirmed external executions",
            ));
        }
    }
    if state
        .external_fills
        .iter()
        .any(|(key, fill)| external.get(key) != Some(fill))
    {
        return Err(invalid(
            "account history omits or rewrites confirmed external executions",
        ));
    }
    let unbound_retryable = state.orders.values().any(|order| {
        order.exchange_order_id.is_none()
            && matches!(
                order.tracking,
                StrategyOrderTracking::Intent {
                    state: IntentState::RetryableNotSubmitted { .. }
                }
            )
    });
    if unbound_retryable
        && external
            .iter()
            .any(|(key, fill)| state.external_fills.get(key) != Some(fill))
    {
        return Err(invalid(
            "retryable owned submissions have unresolved identities; new external fills cannot be adopted",
        ));
    }
    let manual = replay_manual_quantity(state, &external, history.through_ms)?;
    if manual.checked_add(state.grid_position_net_quantity) != Some(actual_quantity) {
        return Err(invalid(
            "account executions do not explain the exact observed position",
        ));
    }
    let delta = manual
        .checked_sub(state.baseline.signed_quantity)
        .ok_or_else(|| invalid("external position delta overflowed"))?;
    Ok((external, delta))
}
