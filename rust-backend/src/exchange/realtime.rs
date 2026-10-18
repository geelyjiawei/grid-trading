use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};

#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use futures::{SinkExt, StreamExt};
#[cfg(not(test))]
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde_json::{Value, json};
#[cfg(not(test))]
use sha2::Sha256;
use tokio::sync::broadcast;
#[cfg(not(test))]
use tokio_tungstenite::{connect_async, connect_async_tls_with_config, tungstenite::Message};
use zeroize::Zeroizing;

use crate::{
    domain::{
        ClientOrderId, Exchange, OrderKind, OrderShape, OrderSide, TerminalOrderStatus,
        TimeInForce, is_valid_symbol_for_exchange,
    },
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, ExecutionSnapshotError, OrderExecutionSnapshot,
        OrderLifecycle, TradeFill,
        bybit_codec::{parse_execution_page, parse_order_row},
        execution::{OrderExecutionHeader, assemble_execution_snapshot, numeric_trade_id},
    },
};

const EXECUTION_WAKEUP_CAPACITY: usize = 1_024;
const RECENT_BINANCE_EXECUTION_CAPACITY: usize = 4_096;
const FUTURES_EXECUTION_CACHE_CAPACITY: usize = 4_096;
#[cfg(not(test))]
const RECONNECT_MIN: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const RECONNECT_MAX: Duration = Duration::from_secs(15);
#[cfg(not(test))]
const LIFETIME_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const BINANCE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30 * 60);
const BINANCE_STREAM_DISABLE_NAGLE: bool = true;
const BINANCE_STREAM_ESTABLISHED_RECONNECT_DELAY: Duration = Duration::ZERO;
const BINANCE_LISTEN_KEY_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(40 * 60);
const BINANCE_LISTEN_KEY_TCP_KEEPALIVE: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const BYBIT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinanceExecutionEvent {
    symbol: String,
    order_id: String,
    trade_id: Option<String>,
    event_time_ms: Option<u64>,
    full_order_update: bool,
    update_time_ms: Option<u64>,
    cumulative_quantity: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BinanceStreamEvent {
    Execution(BinanceExecutionEvent),
    ListenKeyExpired,
    Ignored,
}

#[derive(Debug, Default)]
struct RecentBinanceExecutions {
    order: VecDeque<(String, String, String)>,
    keys: BTreeMap<(String, String, String), BinanceExecutionEvent>,
}

impl RecentBinanceExecutions {
    fn is_new(&mut self, event: &BinanceExecutionEvent) -> bool {
        let Some(trade_id) = event.trade_id.as_ref() else {
            return true;
        };
        let key = (
            event.symbol.clone(),
            event.order_id.clone(),
            trade_id.clone(),
        );
        if let Some(previous) = self.keys.get_mut(&key) {
            // A full update can advance the order even for a previously seen trade.
            if event.full_order_update
                && (!previous.full_order_update
                    || event.update_time_ms > previous.update_time_ms
                    || event.cumulative_quantity > previous.cumulative_quantity
                    || event.event_time_ms > previous.event_time_ms)
            {
                previous.full_order_update = true;
                previous.update_time_ms = previous.update_time_ms.max(event.update_time_ms);
                previous.cumulative_quantity =
                    previous.cumulative_quantity.max(event.cumulative_quantity);
                previous.event_time_ms = previous.event_time_ms.max(event.event_time_ms);
                return true;
            }
            return false;
        }
        self.keys.insert(key.clone(), event.clone());
        self.order.push_back(key);
        while self.order.len() > RECENT_BINANCE_EXECUTION_CAPACITY {
            if let Some(expired) = self.order.pop_front() {
                self.keys.remove(&expired);
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FuturesOrderUpdate {
    pub(crate) order: AuthoritativeOrder,
    pub(crate) cumulative_quantity: Decimal,
    pub(crate) update_time_ms: u64,
    pub(crate) execution_type: String,
    pub(crate) trade: Option<TradeFill>,
}

#[derive(Debug, Clone)]
struct FuturesObservedOrder {
    initial_shape: OrderShape,
    client_order_id: ClientOrderId,
    order_time_ms: Option<u64>,
    pending_header: Option<OrderExecutionHeader>,
    conflicted: bool,
    quarantine_update_time_ms: u64,
    quarantine_quantity: Decimal,
    // REST binds preserve anchors, but only new stream progress grants a fast read.
    stream_generation: u64,
    consumed_stream_generation: u64,
    execution_update_observed: bool,
    latest_order: AuthoritativeOrder,
    cumulative_quantity: Decimal,
    trades: BTreeMap<String, TradeFill>,
    stream_cumulative_quantities: BTreeMap<String, Decimal>,
    snapshot: Option<OrderExecutionSnapshot>,
    last_update_time_ms: u64,
}

#[derive(Debug, Default)]
struct FuturesExecutionCacheState {
    session: u64,
    entries: BTreeMap<(String, String), FuturesObservedOrder>,
    order: VecDeque<(String, String)>,
}

/// Per-account cache populated only by one uninterrupted Binance-compatible
/// futures user-stream session. A signed REST creation-time anchor and exact
/// trade coverage are required; observing NEW is not an authority prerequisite.
#[derive(Debug, Clone, Default)]
pub(crate) struct FuturesExecutionCache {
    state: Arc<Mutex<FuturesExecutionCacheState>>,
    changed: Arc<tokio::sync::Notify>,
}

impl FuturesExecutionCache {
    pub(crate) fn begin_session(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = FuturesExecutionCacheState {
            session: state.session.wrapping_add(1),
            ..FuturesExecutionCacheState::default()
        };
        self.changed.notify_waiters();
    }

    pub(super) fn current_session(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session
    }

    pub(crate) fn apply(&self, update: FuturesOrderUpdate) {
        if update.order.exchange == Exchange::Binance
            && update
                .trade
                .as_ref()
                .is_some_and(|trade| numeric_trade_id(trade).is_err())
        {
            return;
        }
        let key = (
            update.order.shape.symbol.clone(),
            update.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let is_new = update.execution_type == "NEW"
            && matches!(
                update.order.lifecycle,
                OrderLifecycle::Active(ActiveOrderStatus::New)
            );
        if !state.entries.contains_key(&key) {
            state.order.push_back(key.clone());
            state.entries.insert(
                key.clone(),
                FuturesObservedOrder {
                    initial_shape: update.order.shape.clone(),
                    client_order_id: update.order.client_order_id.clone(),
                    order_time_ms: None,
                    pending_header: None,
                    conflicted: false,
                    quarantine_update_time_ms: 0,
                    quarantine_quantity: Decimal::ZERO,
                    stream_generation: 0,
                    consumed_stream_generation: 0,
                    execution_update_observed: !is_new,
                    latest_order: update.order.clone(),
                    cumulative_quantity: update.cumulative_quantity,
                    trades: BTreeMap::new(),
                    stream_cumulative_quantities: BTreeMap::new(),
                    snapshot: None,
                    last_update_time_ms: update.update_time_ms,
                },
            );
            prune_futures_execution_cache(&mut state);
        }

        let Some(observed) = state.entries.get(&key) else {
            return;
        };
        if observed.conflicted {
            return;
        }
        if observed.client_order_id != update.order.client_order_id
            || observed.initial_shape != update.order.shape
            || observed.latest_order.exchange != update.order.exchange
        {
            state.entries.remove(&key);
            state.order.retain(|tracked| tracked != &key);
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        let observed = state
            .entries
            .get_mut(&key)
            .expect("the validated futures execution cache entry must remain present");
        let mut new_evidence = false;
        // Header freshness must not discard a late, previously missing trade.
        if let Some(trade) = update.trade {
            if observed
                .trades
                .get(&trade.trade_id)
                .is_some_and(|cached| cached != &trade)
            {
                quarantine_futures_order(
                    observed,
                    update.update_time_ms,
                    update.cumulative_quantity,
                );
                drop(state);
                self.changed.notify_waiters();
                return;
            }
            new_evidence = !observed.trades.contains_key(&trade.trade_id);
            observed
                .stream_cumulative_quantities
                .entry(trade.trade_id.clone())
                .and_modify(|cumulative| {
                    *cumulative = (*cumulative).max(update.cumulative_quantity)
                })
                .or_insert(update.cumulative_quantity);
            observed
                .trades
                .entry(trade.trade_id.clone())
                .or_insert(trade);
        }
        if update.update_time_ms >= observed.last_update_time_ms
            && update.cumulative_quantity >= observed.cumulative_quantity
        {
            new_evidence |= update.cumulative_quantity > observed.cumulative_quantity
                || (!is_new && update.order.lifecycle != observed.latest_order.lifecycle);
            observed.latest_order = update.order;
            observed.cumulative_quantity = update.cumulative_quantity;
            observed.last_update_time_ms = update.update_time_ms;
            observed.execution_update_observed |= !is_new;
        }
        if new_evidence {
            observed.stream_generation = observed.stream_generation.wrapping_add(1);
        }
        refresh_futures_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
    }

    /// Binds immutable REST order identity to session-local stream updates.
    /// Binance-compatible streams expose transaction time rather than a stable
    /// creation time, so an unbound stream event is never auditable by itself.
    pub(crate) fn bind_authoritative_snapshot(&self, snapshot: &OrderExecutionSnapshot) -> bool {
        self.bind_authoritative_snapshot_for_session(snapshot, self.current_session())
            .is_some()
    }

    pub(super) fn bind_authoritative_snapshot_for_session(
        &self,
        snapshot: &OrderExecutionSnapshot,
        session: u64,
    ) -> Option<OrderExecutionSnapshot> {
        let key = (
            snapshot.order.shape.symbol.clone(),
            snapshot.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.session != session {
            return None;
        }
        let key_was_tracked = state.entries.contains_key(&key);
        let identity_matches = state.entries.get(&key).is_some_and(|observed| {
            observed.client_order_id == snapshot.order.client_order_id
                && observed.initial_shape == snapshot.order.shape
                && observed.latest_order.exchange == snapshot.order.exchange
        });
        if key_was_tracked && !identity_matches {
            return None;
        }
        if identity_matches
            && state.entries.get(&key).is_some_and(|observed| {
                !observed.conflicted && futures_trades_conflict(observed, &snapshot.trades)
            })
        {
            let observed = state.entries.get_mut(&key).unwrap();
            quarantine_futures_order(
                observed,
                snapshot.update_time_ms,
                snapshot.cumulative_quantity,
            );
            drop(state);
            self.changed.notify_waiters();
            return None;
        }
        if identity_matches {
            let observed = state
                .entries
                .get_mut(&key)
                .expect("the validated futures execution cache entry must remain present");
            if observed
                .order_time_ms
                .is_some_and(|time| time != snapshot.order_time_ms)
            {
                return None;
            }
            if observed.conflicted {
                let ids = snapshot
                    .trades
                    .iter()
                    .map(|trade| trade.trade_id.as_str())
                    .collect::<BTreeSet<_>>();
                if snapshot.update_time_ms < observed.quarantine_update_time_ms
                    || snapshot.cumulative_quantity < observed.quarantine_quantity
                    || observed.trades.keys().any(|id| !ids.contains(id.as_str()))
                {
                    return None;
                }
            }
            observed.order_time_ms = Some(snapshot.order_time_ms);
            observed.execution_update_observed |= !matches!(
                snapshot.order.lifecycle,
                OrderLifecycle::Active(ActiveOrderStatus::New)
            );
            if observed.conflicted {
                observed.trades.clear();
                observed.stream_cumulative_quantities.clear();
                observed.latest_order = snapshot.order.clone();
                observed.cumulative_quantity = snapshot.cumulative_quantity;
                observed.last_update_time_ms = snapshot.update_time_ms;
                observed.pending_header = None;
                observed.conflicted = false;
                observed.quarantine_update_time_ms = 0;
                observed.quarantine_quantity = Decimal::ZERO;
            }
            if observed.pending_header.as_ref().is_some_and(|pending| {
                snapshot.update_time_ms >= pending.update_time_ms
                    && snapshot.cumulative_quantity >= pending.cumulative_quantity
            }) {
                observed.pending_header = None;
            }
            if snapshot.update_time_ms >= observed.last_update_time_ms
                && snapshot.cumulative_quantity >= observed.cumulative_quantity
            {
                observed.latest_order = snapshot.order.clone();
                observed.cumulative_quantity = snapshot.cumulative_quantity;
                observed.last_update_time_ms = snapshot.update_time_ms;
            }
            for trade in &snapshot.trades {
                observed
                    .trades
                    .entry(trade.trade_id.clone())
                    .or_insert_with(|| trade.clone());
            }
            refresh_futures_snapshot(observed);
        } else {
            if !key_was_tracked {
                state.order.push_back(key.clone());
            }
            state.entries.insert(
                key.clone(),
                FuturesObservedOrder {
                    initial_shape: snapshot.order.shape.clone(),
                    client_order_id: snapshot.order.client_order_id.clone(),
                    order_time_ms: Some(snapshot.order_time_ms),
                    pending_header: None,
                    conflicted: false,
                    quarantine_update_time_ms: 0,
                    quarantine_quantity: Decimal::ZERO,
                    stream_generation: 0,
                    consumed_stream_generation: 0,
                    execution_update_observed: !matches!(
                        snapshot.order.lifecycle,
                        OrderLifecycle::Active(ActiveOrderStatus::New)
                    ),
                    latest_order: snapshot.order.clone(),
                    cumulative_quantity: snapshot.cumulative_quantity,
                    trades: snapshot
                        .trades
                        .iter()
                        .cloned()
                        .map(|trade| (trade.trade_id.clone(), trade))
                        .collect(),
                    stream_cumulative_quantities: BTreeMap::new(),
                    snapshot: None,
                    last_update_time_ms: snapshot.update_time_ms,
                },
            );
            refresh_futures_snapshot(state.entries.get_mut(&key).unwrap());
        }
        let observed = state.entries.get_mut(&key).unwrap();
        let result = observed
            .snapshot
            .as_ref()
            .filter(|cached| {
                cached.order.exchange == snapshot.order.exchange
                    && cached.order.shape == snapshot.order.shape
                    && cached.order_time_ms == snapshot.order_time_ms
                    && (cached.cumulative_quantity > snapshot.cumulative_quantity
                        || (cached.cumulative_quantity == snapshot.cumulative_quantity
                            && cached.cumulative_quote == snapshot.cumulative_quote
                            && cached.update_time_ms >= snapshot.update_time_ms))
            })
            .cloned()
            .unwrap_or_else(|| snapshot.clone());
        observed.consumed_stream_generation = observed.stream_generation;
        prune_futures_execution_cache(&mut state);
        drop(state);
        self.changed.notify_waiters();
        Some(result)
    }

    /// Supplements a lagging REST trade page with real session-local fills only.
    /// Accepts the union only against untouched, authoritative REST totals.
    pub(super) fn supplement_authoritative_trades(
        &self,
        header: &OrderExecutionHeader,
        trades: &[TradeFill],
        session: u64,
    ) -> Result<Option<OrderExecutionSnapshot>, ExecutionSnapshotError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.session != session {
            return Err(ExecutionSnapshotError::new(
                "Binance execution cache session changed during REST read",
            ));
        }
        let key = (
            header.order.shape.symbol.clone(),
            header.order.exchange_order_id.clone(),
        );
        if !state.entries.contains_key(&key) {
            state.order.push_back(key.clone());
            state.entries.insert(
                key.clone(),
                FuturesObservedOrder {
                    initial_shape: header.order.shape.clone(),
                    client_order_id: header.order.client_order_id.clone(),
                    order_time_ms: None,
                    pending_header: None,
                    conflicted: false,
                    quarantine_update_time_ms: 0,
                    quarantine_quantity: Decimal::ZERO,
                    stream_generation: 0,
                    consumed_stream_generation: 0,
                    execution_update_observed: false,
                    latest_order: header.order.clone(),
                    cumulative_quantity: header.cumulative_quantity,
                    trades: BTreeMap::new(),
                    stream_cumulative_quantities: BTreeMap::new(),
                    snapshot: None,
                    last_update_time_ms: header.update_time_ms,
                },
            );
            prune_futures_execution_cache(&mut state);
        }
        let observed = state.entries.get_mut(&key).unwrap();
        if header.order.exchange != Exchange::Binance
            || observed.latest_order.exchange != header.order.exchange
            || observed.client_order_id != header.order.client_order_id
            || observed.initial_shape != header.order.shape
            || observed
                .order_time_ms
                .is_some_and(|time| time != header.order_time_ms)
        {
            return Err(ExecutionSnapshotError::new(
                "conflicting Binance REST and realtime order identity",
            ));
        }
        if observed.conflicted {
            return Ok(None);
        }
        if futures_trades_conflict(observed, trades) {
            quarantine_futures_order(observed, header.update_time_ms, header.cumulative_quantity);
            drop(state);
            self.changed.notify_waiters();
            return Err(ExecutionSnapshotError::new(
                "conflicting Binance REST and realtime trade data",
            ));
        }
        let mut rest_ids = BTreeSet::new();
        for trade in trades {
            if numeric_trade_id(trade).is_err() || !rest_ids.insert(trade.trade_id.clone()) {
                return Ok(None);
            }
        }
        if trades.iter().any(|trade| {
            observed
                .stream_cumulative_quantities
                .get(&trade.trade_id)
                .is_some_and(|cumulative| *cumulative > header.cumulative_quantity)
        }) {
            return Err(ExecutionSnapshotError::new(
                "Binance trade page extends beyond the authoritative order prefix",
            ));
        }
        observed.order_time_ms = Some(header.order_time_ms);
        observed.execution_update_observed |= !matches!(
            header.order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        );
        if observed.pending_header.as_ref().is_none_or(|pending| {
            header.update_time_ms >= pending.update_time_ms
                && header.cumulative_quantity >= pending.cumulative_quantity
        }) {
            observed.pending_header = Some(header.clone());
        }
        if header.update_time_ms >= observed.last_update_time_ms
            && header.cumulative_quantity >= observed.cumulative_quantity
        {
            observed.latest_order = header.order.clone();
            observed.cumulative_quantity = header.cumulative_quantity;
            observed.last_update_time_ms = header.update_time_ms;
        }
        for trade in trades {
            observed
                .trades
                .entry(trade.trade_id.clone())
                .or_insert_with(|| trade.clone());
        }
        refresh_futures_snapshot(observed);
        let prefix = futures_prefix_trades(observed, header);
        let snapshot = (prefix.iter().all(|trade| numeric_trade_id(trade).is_ok()))
            .then(|| assemble_execution_snapshot(header.clone(), prefix).ok())
            .flatten();
        drop(state);
        self.changed.notify_waiters();
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn knows_order(&self, symbol: &str, exchange_order_id: &str) -> bool {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(&key)
            .is_some_and(|observed| !observed.conflicted && observed.order_time_ms.is_some())
    }

    pub(super) fn take_fresh_snapshot(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Option<OrderExecutionSnapshot> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observed = state
            .entries
            .get_mut(&(symbol.to_ascii_uppercase(), exchange_order_id.to_owned()))?;
        if observed.conflicted
            || observed.order_time_ms.is_none()
            || observed.client_order_id != *client_order_id
            || observed.stream_generation == observed.consumed_stream_generation
        {
            return None;
        }
        let snapshot = observed.snapshot.clone()?;
        observed.consumed_stream_generation = observed.stream_generation;
        Some(snapshot)
    }

    pub(super) fn has_pending_stream_evidence(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .entries
            .get(&(symbol.to_ascii_uppercase(), exchange_order_id.to_owned()))
            .is_some_and(|observed| {
                !observed.conflicted
                    && observed.order_time_ms.is_some()
                    && observed.client_order_id == *client_order_id
                    && observed.stream_generation != observed.consumed_stream_generation
                    && observed.snapshot.is_none()
            })
    }

    pub(crate) fn snapshot(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Option<OrderExecutionSnapshot> {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observed = state.entries.get(&key)?;
        (!observed.conflicted
            && observed.order_time_ms.is_some()
            && observed.client_order_id == *client_order_id)
            .then(|| observed.snapshot.clone())
            .flatten()
    }

    pub(crate) async fn wait_snapshot(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
        maximum_wait: Duration,
    ) -> Option<OrderExecutionSnapshot> {
        if let Some(snapshot) = self.snapshot(symbol, client_order_id, exchange_order_id) {
            return Some(snapshot);
        }
        let deadline = tokio::time::Instant::now() + maximum_wait;
        loop {
            let changed = self.changed.notified();
            if let Some(snapshot) = self.snapshot(symbol, client_order_id, exchange_order_id) {
                return Some(snapshot);
            }
            let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
            tokio::time::timeout(remaining, changed).await.ok()?;
        }
    }
}

fn quarantine_futures_order(
    observed: &mut FuturesObservedOrder,
    update_time_ms: u64,
    quantity: Decimal,
) {
    observed.conflicted = true;
    observed.snapshot = None;
    observed.quarantine_update_time_ms = observed
        .quarantine_update_time_ms
        .max(observed.last_update_time_ms)
        .max(update_time_ms);
    observed.quarantine_quantity = observed
        .quarantine_quantity
        .max(observed.cumulative_quantity)
        .max(quantity);
    if let Some(header) = &observed.pending_header {
        observed.quarantine_update_time_ms = observed
            .quarantine_update_time_ms
            .max(header.update_time_ms);
        observed.quarantine_quantity = observed.quarantine_quantity.max(header.cumulative_quantity);
    }
}

fn futures_trades_conflict(observed: &FuturesObservedOrder, trades: &[TradeFill]) -> bool {
    trades.iter().any(|trade| {
        observed
            .trades
            .get(&trade.trade_id)
            .is_some_and(|cached| cached != trade)
    })
}

fn futures_prefix_trades(
    observed: &FuturesObservedOrder,
    header: &OrderExecutionHeader,
) -> Vec<TradeFill> {
    observed
        .trades
        .values()
        .filter(|trade| {
            trade.trade_time_ms >= header.order_time_ms
                && trade.trade_time_ms <= header.update_time_ms
                && observed
                    .stream_cumulative_quantities
                    .get(&trade.trade_id)
                    .is_none_or(|cumulative| *cumulative <= header.cumulative_quantity)
        })
        .cloned()
        .collect()
}

fn refresh_futures_snapshot(observed: &mut FuturesObservedOrder) {
    let Some(order_time_ms) = observed.order_time_ms else {
        observed.snapshot = None;
        return;
    };
    if observed.conflicted || !observed.execution_update_observed {
        observed.snapshot = None;
        return;
    }
    if observed.trades.values().any(|trade| {
        trade.trade_time_ms < order_time_ms
            || trade.trade_time_ms > observed.last_update_time_ms
            || (observed.latest_order.exchange == Exchange::Binance
                && numeric_trade_id(trade).is_err())
    }) {
        observed.snapshot = None;
        return;
    }
    if let Some(header) = &observed.pending_header {
        let prefix = futures_prefix_trades(observed, header);
        if assemble_execution_snapshot(header.clone(), prefix).is_err() {
            observed.snapshot = None;
            return;
        }
        observed.pending_header = None;
    }
    let cumulative_quote = observed
        .trades
        .values()
        .try_fold(Decimal::ZERO, |total, trade| {
            total.checked_add(trade.quote_quantity)
        });
    let trades = observed.trades.values().cloned().collect::<Vec<_>>();
    observed.snapshot = cumulative_quote.and_then(|cumulative_quote| {
        assemble_execution_snapshot(
            OrderExecutionHeader {
                order: observed.latest_order.clone(),
                cumulative_quantity: observed.cumulative_quantity,
                cumulative_quote,
                order_time_ms,
                update_time_ms: observed.last_update_time_ms,
            },
            trades,
        )
        .ok()
    });
}

fn prune_futures_execution_cache(state: &mut FuturesExecutionCacheState) {
    while state.order.len() > FUTURES_EXECUTION_CACHE_CAPACITY {
        if let Some(expired) = state.order.pop_front() {
            state.entries.remove(&expired);
        }
    }
}

#[derive(Debug, Clone)]
struct BybitObservedOrder {
    initial_shape: Option<OrderShape>,
    client_order_id: ClientOrderId,
    // An ACK binds identity, not lifecycle, exchange timestamps, or execution totals.
    identity_bound: bool,
    header: Option<OrderExecutionHeader>,
    trades: BTreeMap<String, TradeFill>,
    pending_fast_trades: BTreeSet<String>,
    conflicted: bool,
    stream_generation: u64,
    consumed_stream_generation: u64,
    published_stream_generation: u64,
    snapshot: Option<OrderExecutionSnapshot>,
}

#[derive(Debug, Default)]
struct BybitExecutionCacheState {
    session: u64,
    entries: BTreeMap<(String, String), BybitObservedOrder>,
    order: VecDeque<(String, String)>,
    recent_fast: BTreeSet<(String, String, String)>,
    fast_order: VecDeque<(String, String, String)>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BybitExecutionCache {
    state: Arc<Mutex<BybitExecutionCacheState>>,
    changed: Arc<tokio::sync::Notify>,
}

impl BybitExecutionCache {
    pub(crate) fn session_epoch(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session
    }

    /// The caller must first exact-match signed REST evidence to its owned order request.
    pub(crate) fn seed_verified_snapshot(
        &self,
        epoch: u64,
        snapshot: &OrderExecutionSnapshot,
    ) -> bool {
        let Some(header) = verified_bybit_seed_header(snapshot) else {
            return false;
        };
        let key = (
            snapshot.order.shape.symbol.clone(),
            snapshot.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.session != epoch {
            return false;
        }
        let trades = snapshot
            .trades
            .iter()
            .map(|trade| (trade.trade_id.as_str(), trade))
            .collect::<BTreeMap<_, _>>();
        let mut replace_header = true;
        let mut covers_stream = true;
        if let Some(observed) = state.entries.get(&key) {
            if observed.conflicted
                || observed.client_order_id != snapshot.order.client_order_id
                || observed
                    .initial_shape
                    .as_ref()
                    .is_some_and(|shape| *shape != snapshot.order.shape)
                || observed
                    .header
                    .as_ref()
                    .is_some_and(|previous| previous.order_time_ms != snapshot.order_time_ms)
            {
                return false;
            }
            let conflicting = observed.trades.values().any(|trade| {
                trade.side != snapshot.order.shape.side
                    || trade.trade_time_ms < snapshot.order_time_ms
                    || trades
                        .get(trade.trade_id.as_str())
                        .is_some_and(|rest| *rest != trade)
            }) || observed.header.as_ref().is_some_and(|previous| {
                previous.cumulative_quantity == snapshot.cumulative_quantity
                    && previous.cumulative_quote != snapshot.cumulative_quote
            });
            let merged_quantity = observed
                .trades
                .values()
                .filter(|trade| !trades.contains_key(trade.trade_id.as_str()))
                .try_fold(snapshot.cumulative_quantity, |quantity, trade| {
                    quantity.checked_add(trade.quantity)
                });
            if conflicting
                || merged_quantity.is_none_or(|quantity| quantity > snapshot.order.shape.quantity)
            {
                let observed = state.entries.get_mut(&key).unwrap();
                observed.conflicted = true;
                observed.snapshot = None;
                drop(state);
                self.changed.notify_waiters();
                return false;
            }
            let additional_trades = snapshot
                .trades
                .iter()
                .filter(|trade| !observed.trades.contains_key(&trade.trade_id))
                .count();
            if observed.trades.len() + additional_trades > FUTURES_EXECUTION_CACHE_CAPACITY {
                return false;
            }
            if let Some(previous) = &observed.header {
                replace_header = snapshot.update_time_ms >= previous.update_time_ms
                    && snapshot.cumulative_quantity >= previous.cumulative_quantity
                    && !(matches!(previous.order.lifecycle, OrderLifecycle::Terminal(_))
                        && matches!(snapshot.order.lifecycle, OrderLifecycle::Active(_)));
            }
            covers_stream = replace_header
                && observed
                    .trades
                    .keys()
                    .all(|id| trades.contains_key(id.as_str()))
                && observed
                    .pending_fast_trades
                    .iter()
                    .all(|id| trades.contains_key(id.as_str()));
        }
        let observed = bybit_observed_order(&mut state, &key, &snapshot.order.client_order_id);
        observed.initial_shape = Some(snapshot.order.shape.clone());
        observed.identity_bound = true;
        if replace_header {
            observed.header = Some(header);
        }
        for trade in &snapshot.trades {
            observed.pending_fast_trades.remove(&trade.trade_id);
            observed
                .trades
                .entry(trade.trade_id.clone())
                .or_insert_with(|| trade.clone());
        }
        // REST is already returned to its caller. Do not consume newer, uncovered WS evidence.
        if covers_stream {
            observed.consumed_stream_generation = observed.stream_generation;
            observed.published_stream_generation = observed.stream_generation;
        }
        refresh_bybit_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
        true
    }

    pub(crate) fn register_placement(
        &self,
        intent: &crate::domain::OrderIntent,
        ack: &crate::exchange::PlacementAcknowledgement,
        observed_at_ms: u64,
    ) {
        if intent.exchange != Exchange::Bybit
            || intent.validate().is_err()
            || intent.client_order_id != ack.client_order_id
            || ack.exchange_order_id.trim().is_empty()
            || observed_at_ms < intent.created_at_ms
        {
            return;
        }
        let key = (intent.shape.symbol.clone(), ack.exchange_order_id.clone());
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observed = bybit_observed_order(&mut state, &key, &intent.client_order_id);
        if observed.client_order_id != intent.client_order_id
            || observed
                .initial_shape
                .as_ref()
                .is_some_and(|shape| *shape != intent.shape)
            || observed
                .trades
                .values()
                .any(|trade| trade.side != intent.shape.side)
        {
            observed.conflicted = true;
        } else {
            observed.initial_shape = Some(intent.shape.clone());
            observed.identity_bound = true;
        }
        refresh_bybit_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn begin_session(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = BybitExecutionCacheState {
            session: state.session.wrapping_add(1),
            ..BybitExecutionCacheState::default()
        };
        self.changed.notify_waiters();
    }

    pub(crate) fn knows_order(&self, symbol: &str, exchange_order_id: &str) -> bool {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(&key)
            .is_some_and(|observed| observed.identity_bound && !observed.conflicted)
    }

    fn apply_order(&self, header: OrderExecutionHeader) {
        if header.order.exchange != Exchange::Bybit {
            return;
        }
        let key = (
            header.order.shape.symbol.clone(),
            header.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let is_new = matches!(
            header.order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        ) && header.cumulative_quantity.is_zero();
        let observed = bybit_observed_order(&mut state, &key, &header.order.client_order_id);
        if observed.client_order_id != header.order.client_order_id
            || observed
                .initial_shape
                .as_ref()
                .is_some_and(|shape| *shape != header.order.shape)
            || observed
                .trades
                .values()
                .any(|trade| trade.side != header.order.shape.side)
            || observed
                .header
                .as_ref()
                .is_some_and(|previous| previous.order_time_ms != header.order_time_ms)
        {
            observed.conflicted = true;
            observed.snapshot = None;
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        observed.initial_shape = Some(header.order.shape.clone());
        // Preserve the legacy stream anchor without requiring it after an exact ACK.
        observed.identity_bound |= is_new;
        if let Some(previous) = &observed.header {
            if header.update_time_ms < previous.update_time_ms
                || header.cumulative_quantity < previous.cumulative_quantity
                || (matches!(previous.order.lifecycle, OrderLifecycle::Terminal(_))
                    && matches!(header.order.lifecycle, OrderLifecycle::Active(_)))
            {
                refresh_bybit_snapshot(observed);
                drop(state);
                self.changed.notify_waiters();
                return;
            }
            if header.cumulative_quantity == previous.cumulative_quantity
                && header.cumulative_quote != previous.cumulative_quote
            {
                observed.conflicted = true;
            }
            if previous == &header {
                refresh_bybit_snapshot(observed);
                drop(state);
                self.changed.notify_waiters();
                return;
            }
        }
        observed.header = Some(header);
        observed.stream_generation = observed.stream_generation.wrapping_add(1);
        refresh_bybit_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
    }

    fn apply_trade(&self, client_order_id: &ClientOrderId, trade: TradeFill) {
        let key = (trade.symbol.clone(), trade.exchange_order_id.clone());
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observed = bybit_observed_order(&mut state, &key, client_order_id);
        if observed.client_order_id != *client_order_id
            || observed
                .initial_shape
                .as_ref()
                .is_some_and(|shape| shape.symbol != trade.symbol || shape.side != trade.side)
            || observed
                .trades
                .get(&trade.trade_id)
                .is_some_and(|previous| previous != &trade)
        {
            observed.conflicted = true;
            observed.snapshot = None;
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        if observed.trades.contains_key(&trade.trade_id) {
            return;
        }
        // Bound pre-ACK evidence as well as very large orders; REST remains authoritative.
        let capacity = if observed.identity_bound {
            FUTURES_EXECUTION_CACHE_CAPACITY
        } else {
            64
        };
        if observed.trades.len() >= capacity {
            observed.conflicted = true;
            observed.snapshot = None;
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        observed.pending_fast_trades.remove(&trade.trade_id);
        observed.trades.insert(trade.trade_id.clone(), trade);
        observed.stream_generation = observed.stream_generation.wrapping_add(1);
        refresh_bybit_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
    }

    fn apply_fast(&self, symbol: &str, order_id: &str, trade_id: &str) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (symbol.to_ascii_uppercase(), order_id.to_owned());
        let fast_key = (key.0.clone(), key.1.clone(), trade_id.to_owned());
        if !state.recent_fast.insert(fast_key.clone()) {
            return false;
        }
        state.fast_order.push_back(fast_key);
        while state.fast_order.len() > FUTURES_EXECUTION_CACHE_CAPACITY {
            if let Some(expired) = state.fast_order.pop_front() {
                state.recent_fast.remove(&expired);
            }
        }
        if let Some(observed) = state.entries.get_mut(&key) {
            if observed.conflicted || observed.trades.contains_key(trade_id) {
                return false;
            }
            if observed.pending_fast_trades.len() >= FUTURES_EXECUTION_CACHE_CAPACITY {
                observed.conflicted = true;
            } else {
                observed.pending_fast_trades.insert(trade_id.to_owned());
            }
            observed.snapshot = None;
        }
        drop(state);
        self.changed.notify_waiters();
        true
    }

    pub(crate) async fn wait_snapshot(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
        maximum_wait: Duration,
    ) -> Option<OrderExecutionSnapshot> {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        let session = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session;
        let snapshot = || {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.session != session {
                return Err(());
            }
            let Some(observed) = state.entries.get_mut(&key) else {
                return Ok(None);
            };
            if observed.client_order_id != *client_order_id
                || observed.conflicted
                || observed.stream_generation == observed.consumed_stream_generation
            {
                return Ok(None);
            }
            let snapshot = observed.snapshot.clone();
            if snapshot.is_some() {
                observed.consumed_stream_generation = observed.stream_generation;
            }
            Ok(snapshot)
        };
        let deadline = tokio::time::Instant::now() + maximum_wait;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(snapshot) = snapshot().ok()? {
                return Some(snapshot);
            }
            let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
            tokio::time::timeout(remaining, changed).await.ok()?;
        }
    }
}

fn verified_bybit_seed_header(snapshot: &OrderExecutionSnapshot) -> Option<OrderExecutionHeader> {
    let order = &snapshot.order;
    let quantity = snapshot.cumulative_quantity;
    if order.exchange != Exchange::Bybit
        || order.shape.validate().is_err()
        || !is_valid_symbol_for_exchange(Exchange::Bybit, &order.shape.symbol)
        || order.exchange_order_id.is_empty()
        || order.exchange_order_id.len() > 128
        || !order
            .exchange_order_id
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || order.executed_quantity != Some(quantity)
        || quantity < Decimal::ZERO
        || quantity > order.shape.quantity
        || snapshot.cumulative_quote < Decimal::ZERO
        || (quantity.is_zero() && !snapshot.cumulative_quote.is_zero())
        || (quantity > Decimal::ZERO && snapshot.cumulative_quote <= Decimal::ZERO)
        || snapshot.order_time_ms == 0
        || snapshot.update_time_ms < snapshot.order_time_ms
        || snapshot.trades.len() > FUTURES_EXECUTION_CACHE_CAPACITY
    {
        return None;
    }
    match order.lifecycle {
        OrderLifecycle::Active(ActiveOrderStatus::New) if !quantity.is_zero() => return None,
        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
            if quantity <= Decimal::ZERO || quantity >= order.shape.quantity =>
        {
            return None;
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
            if quantity != order.shape.quantity =>
        {
            return None;
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Rejected) if !quantity.is_zero() => {
            return None;
        }
        _ => {}
    }
    if snapshot.trades.iter().any(|trade| {
        !crate::exchange::is_valid_trade_id(&trade.trade_id)
            || trade.price <= Decimal::ZERO
            || trade.quantity <= Decimal::ZERO
            || trade.quote_quantity <= Decimal::ZERO
            || trade.raw_commission != trade.commission_cost
            || trade.commission_asset.is_empty()
            || !trade
                .commission_asset
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            || trade.trade_time_ms < snapshot.order_time_ms
            || trade.trade_time_ms > snapshot.update_time_ms
    }) {
        return None;
    }
    let header = OrderExecutionHeader {
        order: order.clone(),
        cumulative_quantity: quantity,
        cumulative_quote: snapshot.cumulative_quote,
        order_time_ms: snapshot.order_time_ms,
        update_time_ms: snapshot.update_time_ms,
    };
    let assembled = assemble_execution_snapshot(header.clone(), snapshot.trades.clone()).ok()?;
    (assembled == *snapshot).then_some(header)
}

fn bybit_observed_order<'a>(
    state: &'a mut BybitExecutionCacheState,
    key: &(String, String),
    client_order_id: &ClientOrderId,
) -> &'a mut BybitObservedOrder {
    if !state.entries.contains_key(key) {
        state.order.push_back(key.clone());
        state.entries.insert(
            key.clone(),
            BybitObservedOrder {
                initial_shape: None,
                client_order_id: client_order_id.clone(),
                identity_bound: false,
                header: None,
                trades: BTreeMap::new(),
                pending_fast_trades: BTreeSet::new(),
                conflicted: false,
                stream_generation: 0,
                consumed_stream_generation: 0,
                published_stream_generation: 0,
                snapshot: None,
            },
        );
        prune_bybit_execution_cache(state);
    }
    state.entries.get_mut(key).expect("inserted Bybit order")
}

fn refresh_bybit_snapshot(observed: &mut BybitObservedOrder) {
    observed.snapshot = None;
    if observed.conflicted || !observed.identity_bound || !observed.pending_fast_trades.is_empty() {
        return;
    }
    let Some(header) = &observed.header else {
        return;
    };
    if observed.trades.values().any(|trade| {
        trade.trade_time_ms < header.order_time_ms || trade.trade_time_ms > header.update_time_ms
    }) {
        return;
    }
    observed.snapshot =
        assemble_execution_snapshot(header.clone(), observed.trades.values().cloned().collect())
            .ok();
    if let Some(snapshot) = &observed.snapshot
        && observed.stream_generation != observed.published_stream_generation
        && (snapshot.cumulative_quantity > Decimal::ZERO
            || matches!(snapshot.order.lifecycle, OrderLifecycle::Terminal(_)))
    {
        observed.published_stream_generation = observed.stream_generation;
        publish_execution_wakeup(
            Exchange::Bybit,
            &snapshot.order.shape.symbol,
            Some(snapshot.order.exchange_order_id.clone()),
            Some(snapshot.update_time_ms),
        );
    }
}

fn prune_bybit_execution_cache(state: &mut BybitExecutionCacheState) {
    while state.order.len() > FUTURES_EXECUTION_CACHE_CAPACITY {
        if let Some(expired) = state.order.pop_front() {
            state.entries.remove(&expired);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionWakeup {
    pub exchange: Exchange,
    pub symbol: String,
    pub exchange_order_id: Option<String>,
    pub exchange_event_time_ms: Option<u64>,
    pub observed_at_ms: u64,
}

fn execution_wakeup_sender() -> &'static broadcast::Sender<ExecutionWakeup> {
    static SENDER: OnceLock<broadcast::Sender<ExecutionWakeup>> = OnceLock::new();
    SENDER.get_or_init(|| broadcast::channel(EXECUTION_WAKEUP_CAPACITY).0)
}

pub fn subscribe_execution_wakeups() -> broadcast::Receiver<ExecutionWakeup> {
    execution_wakeup_sender().subscribe()
}

pub(crate) fn publish_execution_wakeup(
    exchange: Exchange,
    symbol: &str,
    exchange_order_id: Option<String>,
    exchange_event_time_ms: Option<u64>,
) {
    let symbol = symbol.trim().to_ascii_uppercase();
    if !is_valid_symbol_for_exchange(exchange, &symbol) {
        return;
    }
    let observed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default();
    tracing::info!(
        exchange = ?exchange,
        symbol = symbol.as_str(),
        exchange_order_id = exchange_order_id.as_deref().unwrap_or("-"),
        exchange_event_time_ms,
        observed_at_ms,
        "execution stream published wakeup"
    );
    let _ = execution_wakeup_sender().send(ExecutionWakeup {
        exchange,
        symbol,
        exchange_order_id,
        exchange_event_time_ms,
        observed_at_ms,
    });
}

#[cfg(not(test))]
pub(crate) fn spawn_binance_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    lifetime: Weak<()>,
    execution_cache: FuturesExecutionCache,
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!("Binance user execution stream was not started outside a Tokio runtime");
        return;
    };
    runtime.spawn(run_binance_execution_stream(
        testnet,
        api_key,
        lifetime,
        execution_cache,
    ));
}

#[cfg(test)]
pub(crate) fn spawn_binance_execution_stream(
    _testnet: bool,
    _api_key: Zeroizing<String>,
    _lifetime: Weak<()>,
    _execution_cache: FuturesExecutionCache,
) {
}

#[cfg(not(test))]
async fn run_binance_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    lifetime: Weak<()>,
    execution_cache: FuturesExecutionCache,
) {
    let rest_url = if testnet {
        "https://testnet.binancefuture.com"
    } else {
        "https://fapi.binance.com"
    };
    let client = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .pool_idle_timeout(BINANCE_LISTEN_KEY_POOL_IDLE_TIMEOUT)
        .tcp_keepalive(BINANCE_LISTEN_KEY_TCP_KEEPALIVE)
        .tcp_nodelay(true)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(error = %error, "Binance user execution HTTP client failed");
            return;
        }
    };
    let mut reconnect_delay = RECONNECT_MIN;
    while lifetime.upgrade().is_some() {
        let listen_key = match binance_listen_key(&client, rest_url, api_key.as_str()).await {
            Ok(listen_key) => listen_key,
            Err(error) => {
                tracing::warn!(error = %error, "Binance user execution listen key failed");
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        let stream_url = binance_user_stream_url(testnet, &listen_key);
        let (mut socket, _) = match connect_async_tls_with_config(
            &stream_url,
            None,
            BINANCE_STREAM_DISABLE_NAGLE,
            None,
        )
        .await
        {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %error, "Binance user execution stream connection failed");
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        tracing::info!(
            stream = if testnet { "testnet" } else { "private" },
            "Binance user execution stream connected"
        );
        execution_cache.begin_session();
        let mut recent_executions = RecentBinanceExecutions::default();
        let mut keepalive = tokio::time::interval(BINANCE_KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        keepalive.tick().await;
        let mut lifetime_check = tokio::time::interval(LIFETIME_CHECK_INTERVAL);
        lifetime_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        lifetime_check.tick().await;
        loop {
            tokio::select! {
                message = socket.next() => {
                    let Some(message) = message else { break };
                    match message {
                        Ok(Message::Text(text)) => {
                            if publish_binance_message(
                                text.as_ref(),
                                &mut recent_executions,
                                &execution_cache,
                            ) {
                                break;
                            }
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref())
                                && publish_binance_message(
                                    text,
                                    &mut recent_executions,
                                    &execution_cache,
                                )
                            {
                                break;
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Err(error) => {
                            tracing::warn!(error = %error, "Binance user execution stream read failed");
                            break;
                        }
                        Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                    }
                }
                _ = keepalive.tick() => {
                    if let Err(error) = binance_keepalive(&client, rest_url, api_key.as_str()).await {
                        tracing::warn!(error, "Binance user execution listen key keepalive failed");
                        break;
                    }
                }
                _ = lifetime_check.tick() => {
                    if lifetime.upgrade().is_none() {
                        return;
                    }
                }
            }
        }
        tracing::warn!("Binance user execution stream disconnected; REST fallback remains active");
        if !BINANCE_STREAM_ESTABLISHED_RECONNECT_DELAY.is_zero() {
            tokio::time::sleep(BINANCE_STREAM_ESTABLISHED_RECONNECT_DELAY).await;
        }
        reconnect_delay = RECONNECT_MIN;
    }
}

fn binance_user_stream_url(testnet: bool, listen_key: &str) -> String {
    if testnet {
        format!("wss://stream.binancefuture.com/ws/{listen_key}")
    } else {
        format!("wss://fstream.binance.com/private/ws/{listen_key}")
    }
}

#[cfg(not(test))]
async fn binance_listen_key(
    client: &reqwest::Client,
    rest_url: &str,
    api_key: &str,
) -> Result<String, String> {
    let response = client
        .post(format!("{rest_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| value.get("listenKey")?.as_str().map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "listen key response is invalid".to_owned())
}

#[cfg(not(test))]
async fn binance_keepalive(
    client: &reqwest::Client,
    rest_url: &str,
    api_key: &str,
) -> Result<(), String> {
    let response = client
        .put(format!("{rest_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("HTTP {}", response.status()))
    }
}

#[cfg(not(test))]
fn publish_binance_message(
    text: &str,
    recent_executions: &mut RecentBinanceExecutions,
    execution_cache: &FuturesExecutionCache,
) -> bool {
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    if let Some(update) = parse_futures_order_update(&message, Exchange::Binance) {
        execution_cache.apply(update);
    }
    match parse_binance_stream_event_value(&message) {
        BinanceStreamEvent::Execution(event) if recent_executions.is_new(&event) => {
            publish_execution_wakeup(
                Exchange::Binance,
                &event.symbol,
                Some(event.order_id),
                event.event_time_ms,
            );
            false
        }
        BinanceStreamEvent::Execution(_) | BinanceStreamEvent::Ignored => false,
        BinanceStreamEvent::ListenKeyExpired => {
            tracing::warn!("Binance user execution listen key expired; reconnecting immediately");
            true
        }
    }
}

#[cfg(test)]
fn parse_binance_stream_event(text: &str) -> BinanceStreamEvent {
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return BinanceStreamEvent::Ignored;
    };
    parse_binance_stream_event_value(&message)
}

fn parse_binance_stream_event_value(message: &Value) -> BinanceStreamEvent {
    match message.get("e").and_then(Value::as_str) {
        Some("TRADE_LITE") if positive_decimal_text(message.get("l")) => {
            let Some(symbol) = message.get("s").and_then(Value::as_str) else {
                return BinanceStreamEvent::Ignored;
            };
            let Some(order_id) = value_identifier(message.get("i")) else {
                return BinanceStreamEvent::Ignored;
            };
            BinanceStreamEvent::Execution(BinanceExecutionEvent {
                symbol: symbol.to_owned(),
                order_id,
                trade_id: value_identifier(message.get("t")),
                event_time_ms: message.get("E").and_then(Value::as_u64),
                full_order_update: false,
                update_time_ms: None,
                cumulative_quantity: None,
            })
        }
        Some("ORDER_TRADE_UPDATE") => {
            let Some(order) = message.get("o") else {
                return BinanceStreamEvent::Ignored;
            };
            if order.get("x").and_then(Value::as_str) != Some("TRADE")
                || !positive_decimal_text(order.get("l"))
            {
                return BinanceStreamEvent::Ignored;
            }
            let Some(symbol) = order.get("s").and_then(Value::as_str) else {
                return BinanceStreamEvent::Ignored;
            };
            let Some(order_id) = value_identifier(order.get("i")) else {
                return BinanceStreamEvent::Ignored;
            };
            BinanceStreamEvent::Execution(BinanceExecutionEvent {
                symbol: symbol.to_owned(),
                order_id,
                trade_id: value_identifier(order.get("t")),
                event_time_ms: message.get("E").and_then(Value::as_u64),
                full_order_update: true,
                update_time_ms: order
                    .get("T")
                    .and_then(Value::as_u64)
                    .or_else(|| message.get("T").and_then(Value::as_u64)),
                cumulative_quantity: decimal_text(order.get("z")),
            })
        }
        Some("listenKeyExpired") => BinanceStreamEvent::ListenKeyExpired,
        _ => BinanceStreamEvent::Ignored,
    }
}

pub(crate) fn parse_futures_order_update(
    message: &Value,
    exchange: Exchange,
) -> Option<FuturesOrderUpdate> {
    (message.get("e").and_then(Value::as_str) == Some("ORDER_TRADE_UPDATE")).then_some(())?;
    let row = message.get("o")?;
    let symbol = row.get("s")?.as_str()?.to_ascii_uppercase();
    if !is_valid_symbol_for_exchange(exchange, &symbol) {
        return None;
    }
    let client_order_id = ClientOrderId::parse(row.get("c")?.as_str()?.to_owned()).ok()?;
    let exchange_order_id = value_identifier(row.get("i"))?;
    let side = match row.get("S")?.as_str()? {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return None,
    };
    let kind = match row.get("o")?.as_str()? {
        "LIMIT" => OrderKind::Limit,
        "MARKET" => OrderKind::Market,
        _ => return None,
    };
    let quantity = decimal_text(row.get("q"))?;
    let (price, time_in_force) = match kind {
        OrderKind::Limit => {
            let price = decimal_text(row.get("p"))?;
            let time_in_force = match row.get("f")?.as_str()? {
                "GTC" => TimeInForce::Gtc,
                "GTX" => TimeInForce::PostOnly,
                _ => return None,
            };
            (Some(price), time_in_force)
        }
        OrderKind::Market => (None, TimeInForce::Gtc),
    };
    let shape = OrderShape {
        symbol: symbol.clone(),
        side,
        price,
        quantity,
        reduce_only: row.get("R")?.as_bool()?,
        kind,
        time_in_force,
    };
    shape.validate().ok()?;
    let cumulative_quantity = decimal_text(row.get("z"))?;
    let lifecycle = match row.get("X")?.as_str()? {
        "NEW" => OrderLifecycle::Active(ActiveOrderStatus::New),
        "PARTIALLY_FILLED" => OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled),
        "FILLED" => OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
        "CANCELED" | "CANCELLED" => OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
        "REJECTED" => OrderLifecycle::Terminal(TerminalOrderStatus::Rejected),
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderLifecycle::Terminal(TerminalOrderStatus::Expired),
        _ => return None,
    };
    if cumulative_quantity < Decimal::ZERO || cumulative_quantity > shape.quantity {
        return None;
    }
    match lifecycle {
        OrderLifecycle::Active(ActiveOrderStatus::New) if !cumulative_quantity.is_zero() => {
            return None;
        }
        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
            if cumulative_quantity <= Decimal::ZERO || cumulative_quantity >= shape.quantity =>
        {
            return None;
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
            if cumulative_quantity != shape.quantity =>
        {
            return None;
        }
        OrderLifecycle::Terminal(TerminalOrderStatus::Rejected)
            if !cumulative_quantity.is_zero() =>
        {
            return None;
        }
        _ => {}
    }
    let update_time_ms = row
        .get("T")
        .and_then(Value::as_u64)
        .or_else(|| message.get("T").and_then(Value::as_u64))?;
    if update_time_ms == 0 {
        return None;
    }
    let execution_type = row.get("x")?.as_str()?.to_owned();
    let trade = if execution_type == "TRADE" && positive_decimal_text(row.get("l")) {
        let trade_id = value_identifier(row.get("t"))?;
        let price = decimal_text(row.get("L"))?;
        let quantity = decimal_text(row.get("l"))?;
        let quote_quantity = price.checked_mul(quantity)?;
        let raw_commission = decimal_text(row.get("n"))?;
        let commission_asset = row.get("N")?.as_str()?.to_ascii_uppercase();
        let realized_profit = decimal_text(row.get("rp"))?;
        let is_maker = row.get("m")?.as_bool()?;
        if trade_id == "0"
            || price <= Decimal::ZERO
            || quantity <= Decimal::ZERO
            || quote_quantity <= Decimal::ZERO
            || (exchange == Exchange::Binance && raw_commission < Decimal::ZERO)
            || commission_asset.is_empty()
            || !commission_asset
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return None;
        }
        Some(TradeFill {
            trade_id,
            exchange_order_id: exchange_order_id.clone(),
            symbol: symbol.clone(),
            side,
            price,
            quantity,
            quote_quantity,
            raw_commission,
            commission_cost: if exchange == Exchange::Aster {
                raw_commission.abs()
            } else {
                raw_commission
            },
            commission_asset,
            realized_profit,
            is_maker,
            trade_time_ms: update_time_ms,
        })
    } else {
        None
    };
    Some(FuturesOrderUpdate {
        order: AuthoritativeOrder {
            client_order_id,
            exchange_order_id,
            exchange,
            shape,
            lifecycle,
            executed_quantity: Some(cumulative_quantity),
        },
        cumulative_quantity,
        update_time_ms,
        execution_type,
        trade,
    })
}

#[cfg(not(test))]
pub(crate) fn spawn_bybit_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
    execution_cache: BybitExecutionCache,
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!("Bybit execution stream was not started outside a Tokio runtime");
        return;
    };
    runtime.spawn(run_bybit_execution_stream(
        testnet,
        api_key,
        api_secret,
        lifetime,
        execution_cache,
    ));
}

#[cfg(test)]
pub(crate) fn spawn_bybit_execution_stream(
    _testnet: bool,
    _api_key: Zeroizing<String>,
    _api_secret: Zeroizing<String>,
    _lifetime: Weak<()>,
    _execution_cache: BybitExecutionCache,
) {
}

#[cfg(not(test))]
async fn run_bybit_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
    execution_cache: BybitExecutionCache,
) {
    let websocket_url = if testnet {
        "wss://stream-testnet.bybit.com/v5/private"
    } else {
        "wss://stream.bybit.com/v5/private"
    };
    let mut reconnect_delay = RECONNECT_MIN;
    while lifetime.upgrade().is_some() {
        let (mut socket, _) = match connect_async(websocket_url).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %error, "Bybit execution stream connection failed");
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        let expires = current_time_ms().saturating_add(5_000);
        let signature = match hmac_sha256(api_secret.as_bytes(), &format!("GET/realtime{expires}"))
        {
            Some(signature) => signature,
            None => return,
        };
        let auth = json!({"op":"auth","args":[api_key.as_str(), expires, signature]});
        if socket
            .send(Message::Text(auth.to_string().into()))
            .await
            .is_err()
            || !await_bybit_ack(&mut socket, "auth").await
        {
            tracing::warn!("Bybit execution stream authentication failed");
            tokio::time::sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
            continue;
        }
        let subscription = json!({
            "op":"subscribe",
            "args":["execution.fast.linear", "execution.linear", "order.linear"]
        });
        if socket
            .send(Message::Text(subscription.to_string().into()))
            .await
            .is_err()
            || !await_bybit_ack(&mut socket, "subscribe").await
        {
            tracing::warn!("Bybit private execution subscriptions failed");
            tokio::time::sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
            continue;
        }
        tracing::info!("Bybit fast execution, full execution, and order streams connected");
        execution_cache.begin_session();
        reconnect_delay = RECONNECT_MIN;
        let mut heartbeat = tokio::time::interval(BYBIT_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        let mut lifetime_check = tokio::time::interval(LIFETIME_CHECK_INTERVAL);
        lifetime_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        lifetime_check.tick().await;
        loop {
            tokio::select! {
                message = socket.next() => {
                    let Some(message) = message else { break };
                    match message {
                        Ok(Message::Text(text)) => {
                            publish_bybit_message(text.as_ref(), &execution_cache)
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                                publish_bybit_message(text, &execution_cache);
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Err(error) => {
                            tracing::warn!(error = %error, "Bybit execution stream read failed");
                            break;
                        }
                        Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                    }
                }
                _ = heartbeat.tick() => {
                    if socket
                        .send(Message::Text(json!({"op":"ping"}).to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ = lifetime_check.tick() => {
                    if lifetime.upgrade().is_none() {
                        return;
                    }
                }
            }
        }
        execution_cache.begin_session();
        tracing::warn!("Bybit execution stream disconnected; REST fallback remains active");
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
    }
}

#[cfg(not(test))]
async fn await_bybit_ack<S>(socket: &mut S, operation: &str) -> bool
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            let Ok(Message::Text(text)) = message else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                continue;
            };
            if value.get("op").and_then(Value::as_str) == Some(operation) {
                return value.get("success").and_then(Value::as_bool) == Some(true);
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

fn publish_bybit_message(text: &str, execution_cache: &BybitExecutionCache) {
    if let Ok(message) = serde_json::from_str::<Value>(text) {
        for header in parse_bybit_order_updates(&message) {
            execution_cache.apply_order(header);
        }
        for (client_order_id, trade) in parse_bybit_trade_updates(&message) {
            execution_cache.apply_trade(&client_order_id, trade);
        }
        for (symbol, order_id, event_time) in parse_bybit_execution_events(text) {
            let mut fresh = false;
            for row in message
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if row.get("category").and_then(Value::as_str) == Some("linear")
                    && row.get("symbol").and_then(Value::as_str) == Some(symbol.as_str())
                    && value_identifier(row.get("orderId")).as_deref() == Some(order_id.as_str())
                    && let Some(trade_id) = value_identifier(row.get("execId"))
                {
                    fresh |= execution_cache.apply_fast(&symbol, &order_id, &trade_id);
                }
            }
            if fresh {
                publish_execution_wakeup(Exchange::Bybit, &symbol, Some(order_id), event_time);
            }
        }
    }
}

fn parse_bybit_order_updates(message: &Value) -> Vec<OrderExecutionHeader> {
    let topic = message.get("topic").and_then(Value::as_str);
    if !topic.is_some_and(|topic| topic == "order.linear" || topic == "order") {
        return Vec::new();
    }
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("category").and_then(Value::as_str) == Some("linear"))
        .filter_map(|row| {
            let symbol = row.get("symbol")?.as_str()?.to_ascii_uppercase();
            let client_order_id = ClientOrderId::parse(row.get("orderLinkId")?.as_str()?).ok()?;
            let exchange_order_id = value_identifier(row.get("orderId"))?;
            parse_order_row(row, &symbol, &client_order_id, Some(&exchange_order_id)).ok()
        })
        .collect()
}

fn parse_bybit_trade_updates(message: &Value) -> Vec<(ClientOrderId, TradeFill)> {
    let topic = message.get("topic").and_then(Value::as_str);
    if !topic.is_some_and(|topic| topic == "execution.linear" || topic == "execution") {
        return Vec::new();
    }
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("category").and_then(Value::as_str) == Some("linear"))
        .filter_map(|row| {
            let symbol = row.get("symbol")?.as_str()?.to_ascii_uppercase();
            let client_order_id = ClientOrderId::parse(row.get("orderLinkId")?.as_str()?).ok()?;
            let exchange_order_id = value_identifier(row.get("orderId"))?;
            let body = json!({
                "retCode": 0,
                "result": {
                    "category": "linear",
                    "nextPageCursor": "",
                    "list": [row]
                }
            })
            .to_string();
            let mut page =
                parse_execution_page(&body, &symbol, &client_order_id, &exchange_order_id).ok()?;
            (page.trades.len() == 1).then(|| (client_order_id, page.trades.remove(0)))
        })
        .collect()
}

fn parse_bybit_execution_events(text: &str) -> Vec<(String, String, Option<u64>)> {
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let topic = message.get("topic").and_then(Value::as_str);
    if !topic.is_some_and(|topic| topic == "execution.fast" || topic.starts_with("execution.fast."))
    {
        return Vec::new();
    }
    let event_time = message.get("creationTime").and_then(Value::as_u64);
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("category").and_then(Value::as_str) == Some("linear"))
        .filter_map(|row| {
            Some((
                row.get("symbol")?.as_str()?.to_owned(),
                value_identifier(row.get("orderId"))?,
            ))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|(symbol, order_id)| (symbol, order_id, event_time))
        .collect()
}

fn value_identifier(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn positive_decimal_text(value: Option<&Value>) -> bool {
    decimal_text(value).is_some_and(|value| value > Decimal::ZERO)
}

fn decimal_text(value: Option<&Value>) -> Option<Decimal> {
    value?.as_str()?.parse::<Decimal>().ok()
}

#[cfg(not(test))]
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

#[cfg(not(test))]
fn hmac_sha256(secret: &[u8], message: &str) -> Option<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).ok()?;
    mac.update(message.as_bytes());
    Some(hex::encode(mac.finalize().into_bytes()))
}

pub(crate) fn new_realtime_lifetime() -> Arc<()> {
    Arc::new(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_stream_uses_low_latency_connection_and_reconnect_policy() {
        assert!(std::hint::black_box(BINANCE_STREAM_DISABLE_NAGLE));
        assert!(BINANCE_STREAM_ESTABLISHED_RECONNECT_DELAY.is_zero());
        assert!(BINANCE_LISTEN_KEY_POOL_IDLE_TIMEOUT > BINANCE_KEEPALIVE_INTERVAL);
        assert!(BINANCE_LISTEN_KEY_TCP_KEEPALIVE <= Duration::from_secs(60));
    }

    fn authoritative_new_snapshot(
        update: &FuturesOrderUpdate,
        order_time_ms: u64,
    ) -> OrderExecutionSnapshot {
        assemble_execution_snapshot(
            OrderExecutionHeader {
                order: update.order.clone(),
                cumulative_quantity: Decimal::ZERO,
                cumulative_quote: Decimal::ZERO,
                order_time_ms,
                update_time_ms: update.update_time_ms,
            },
            vec![],
        )
        .unwrap()
    }

    fn three_unit_order_update(
        trade_id: Option<u64>,
        quantity: &str,
        cumulative: &str,
        time: u64,
    ) -> FuturesOrderUpdate {
        let message = json!({
            "e": "ORDER_TRADE_UPDATE", "E": time, "T": time,
            "o": {
                "s": "MUUSDT", "c": "r_run_0_S_1", "S": "SELL", "o": "LIMIT",
                "f": "GTC", "q": "3", "p": "16", "R": false, "i": 43,
                "x": if trade_id.is_some() { "TRADE" } else { "NEW" },
                "X": match cumulative { "0" => "NEW", "3" => "FILLED", _ => "PARTIALLY_FILLED" },
                "l": quantity, "z": cumulative, "L": "16", "T": time,
                "t": trade_id, "N": "USDT", "n": "0.0032", "m": true, "rp": "0"
            }
        });
        parse_futures_order_update(&message, Exchange::Binance).unwrap()
    }

    #[test]
    fn release_blocker_quarantine_resolution_is_nonregressing_and_covers_known_ids() {
        for case in ["older_full", "missing_id", "lower_quantity"] {
            let cache = FuturesExecutionCache::default();
            let full = three_unit_order_update(Some(8), "3", "3", 1010);
            let client = full.order.client_order_id.clone();
            cache.apply(full.clone());
            let mut trade = full.trade.clone().unwrap();
            trade.raw_commission = Decimal::new(64, 4);
            trade.commission_cost = Decimal::new(64, 4);
            let resolved = assemble_execution_snapshot(
                OrderExecutionHeader {
                    order: full.order.clone(),
                    cumulative_quantity: Decimal::from(3),
                    cumulative_quote: Decimal::from(48),
                    order_time_ms: 990,
                    update_time_ms: 1020,
                },
                vec![trade],
            )
            .unwrap();
            assert!(!cache.bind_authoritative_snapshot(&resolved));
            let mut insufficient = resolved.clone();
            match case {
                "older_full" => insufficient.update_time_ms = 1015,
                "missing_id" => insufficient.trades[0].trade_id = "9".into(),
                _ => {
                    insufficient.update_time_ms = 1030;
                    insufficient.order.lifecycle =
                        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled);
                    insufficient.order.executed_quantity = Some(Decimal::ONE);
                    insufficient.cumulative_quantity = Decimal::ONE;
                    insufficient.cumulative_quote = Decimal::from(16);
                    insufficient.trades[0].quantity = Decimal::ONE;
                    insufficient.trades[0].quote_quantity = Decimal::from(16);
                }
            }
            assert!(
                !cache.bind_authoritative_snapshot(&insufficient),
                "{case} must not erase disputed history"
            );
            cache.apply(full);
            assert!(!cache.knows_order("MUUSDT", "43"));
            assert!(cache.snapshot("MUUSDT", &client, "43").is_none());
            assert!(cache.bind_authoritative_snapshot(&resolved));
            assert_eq!(
                cache
                    .snapshot("MUUSDT", &client, "43")
                    .unwrap()
                    .fees_by_asset["USDT"],
                Decimal::new(64, 4)
            );
        }
    }

    #[test]
    fn cold_rest_anchor_is_cleared_by_reconnect_and_stream_alone_stays_unbound() {
        let cache = FuturesExecutionCache::default();
        let partial = three_unit_order_update(Some(8), "1", "1", 1010);
        let client = partial.order.client_order_id.clone();
        let prefix = assemble_execution_snapshot(
            OrderExecutionHeader {
                order: partial.order,
                cumulative_quantity: Decimal::ONE,
                cumulative_quote: Decimal::from(16),
                order_time_ms: 990,
                update_time_ms: 1010,
            },
            vec![partial.trade.unwrap()],
        )
        .unwrap();
        assert!(cache.bind_authoritative_snapshot(&prefix));
        cache.apply(three_unit_order_update(Some(9), "2", "3", 1020));
        assert_eq!(
            cache
                .snapshot("MUUSDT", &client, "43")
                .expect("REST prefixes do not need NEW")
                .trades
                .len(),
            2
        );
        cache.begin_session();
        assert!(!cache.knows_order("MUUSDT", "43"));
        cache.apply(three_unit_order_update(Some(9), "2", "3", 1020));
        cache.apply(three_unit_order_update(Some(8), "1", "1", 1010));
        assert!(!cache.knows_order("MUUSDT", "43"));
        assert!(
            cache.snapshot("MUUSDT", &client, "43").is_none(),
            "complete stream trades cannot resurrect the old creation-time anchor"
        );
        assert!(cache.bind_authoritative_snapshot(&prefix));
        assert_eq!(
            cache
                .snapshot("MUUSDT", &client, "43")
                .unwrap()
                .cumulative_quote,
            Decimal::from(48)
        );
    }

    #[test]
    fn binance_late_trade_completes_latest_header_without_regressing_it() {
        let cache = FuturesExecutionCache::default();
        let new = three_unit_order_update(None, "0", "0", 1000);
        let client_order_id = new.order.client_order_id.clone();
        cache.apply(new.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new, 990));
        cache.apply(three_unit_order_update(Some(9), "2", "3", 1020));
        assert!(cache.snapshot("MUUSDT", &client_order_id, "43").is_none());

        cache.apply(three_unit_order_update(Some(8), "1", "1", 1010));
        let snapshot = cache
            .snapshot("MUUSDT", &client_order_id, "43")
            .expect("an older update can supply the missing canonical trade");
        assert_eq!(snapshot.update_time_ms, 1020);
        assert_eq!(snapshot.cumulative_quantity, Decimal::from(3));
        assert_eq!(snapshot.cumulative_quote, Decimal::from(48));
        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
        assert_eq!(
            snapshot
                .trades
                .iter()
                .map(|trade| trade.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["8", "9"]
        );
    }

    #[test]
    fn binance_stale_rest_binding_preserves_newer_stream_trades_and_totals() {
        for rest_update_time in [1020, 1030] {
            let cache = FuturesExecutionCache::default();
            let new = three_unit_order_update(None, "0", "0", 1000);
            let client_order_id = new.order.client_order_id.clone();
            cache.apply(new.clone());
            cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new, 990));
            cache.apply(three_unit_order_update(Some(8), "1", "1", 1010));
            let mut loaded_rest = cache.snapshot("MUUSDT", &client_order_id, "43").unwrap();
            loaded_rest.update_time_ms = rest_update_time;
            cache.apply(three_unit_order_update(Some(9), "2", "3", 1020));

            cache.bind_authoritative_snapshot(&loaded_rest);
            let snapshot = cache.snapshot("MUUSDT", &client_order_id, "43").unwrap();
            assert_eq!(
                snapshot.cumulative_quantity,
                Decimal::from(3),
                "REST must not roll cumulative execution backwards"
            );
            assert_eq!(snapshot.update_time_ms, 1020);
            assert_eq!(snapshot.trades.len(), 2);
        }
    }

    #[test]
    fn binance_conflicting_rest_trade_invalidates_cached_snapshot() {
        let cache = FuturesExecutionCache::default();
        let new = three_unit_order_update(None, "0", "0", 1000);
        let client = new.order.client_order_id.clone();
        cache.apply(new.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new, 990));
        cache.apply(three_unit_order_update(Some(8), "3", "3", 1010));
        let mut conflicting = cache.snapshot("MUUSDT", &client, "43").unwrap();
        conflicting.trades[0].raw_commission = Decimal::new(64, 4);
        conflicting.trades[0].commission_cost = Decimal::new(64, 4);
        conflicting
            .fees_by_asset
            .insert("USDT".into(), Decimal::new(64, 4));

        cache.bind_authoritative_snapshot(&conflicting);
        assert!(
            cache.snapshot("MUUSDT", &client, "43").is_none(),
            "a conflicting common trade ID must invalidate cache authority, not retain either fee"
        );
    }

    #[test]
    fn binance_rest_binding_cannot_change_immutable_creation_time() {
        let cache = FuturesExecutionCache::default();
        let new = three_unit_order_update(None, "0", "0", 1000);
        let client = new.order.client_order_id.clone();
        cache.apply(new.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new, 990));
        cache.apply(three_unit_order_update(Some(8), "3", "3", 1010));
        let mut conflicting = cache.snapshot("MUUSDT", &client, "43").unwrap();
        conflicting.order_time_ms = 991;
        cache.bind_authoritative_snapshot(&conflicting);
        assert!(
            cache
                .snapshot("MUUSDT", &client, "43")
                .is_none_or(|snapshot| snapshot.order_time_ms == 990)
        );
    }

    #[tokio::test]
    async fn binance_late_trade_wakes_all_snapshot_waiters_without_a_tick() {
        let cache = FuturesExecutionCache::default();
        let new = three_unit_order_update(None, "0", "0", 1000);
        let client = new.order.client_order_id.clone();
        cache.apply(new.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new, 990));
        let first = cache.wait_snapshot("MUUSDT", &client, "43", Duration::from_secs(1));
        let second = cache.wait_snapshot("MUUSDT", &client, "43", Duration::from_secs(1));
        tokio::pin!(first, second);
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        cache.apply(three_unit_order_update(Some(9), "2", "3", 1020));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        cache.apply(three_unit_order_update(Some(8), "1", "1", 1010));
        for ready in [futures::poll!(&mut first), futures::poll!(&mut second)] {
            let std::task::Poll::Ready(Some(snapshot)) = ready else {
                panic!("complete canonical trades must wake every waiter immediately");
            };
            assert_eq!(snapshot.cumulative_quantity, Decimal::from(3));
            assert_eq!(snapshot.trades.len(), 2);
        }
    }

    #[test]
    fn binance_same_trade_full_progress_wakes_but_exact_duplicates_do_not() {
        let mut recent = RecentBinanceExecutions::default();
        let mut message = json!({
            "e": "ORDER_TRADE_UPDATE", "E": 124,
            "o": {"x": "TRADE", "l": "1", "z": "1", "X": "PARTIALLY_FILLED",
                  "s": "MUUSDT", "t": 456, "i": 789, "T": 123}
        });
        let BinanceStreamEvent::Execution(first) = parse_binance_stream_event_value(&message)
        else {
            panic!("expected full fill");
        };
        assert!(recent.is_new(&first));
        assert!(!recent.is_new(&first));
        message["o"]["z"] = json!("3");
        message["o"]["X"] = json!("FILLED");
        let BinanceStreamEvent::Execution(progress) = parse_binance_stream_event_value(&message)
        else {
            panic!("expected cumulative progress");
        };
        assert!(
            recent.is_new(&progress),
            "same trade ID and event time do not imply duplicate order totals"
        );
        assert!(!recent.is_new(&progress));
        message["o"]["T"] = json!(125);
        let BinanceStreamEvent::Execution(newer) = parse_binance_stream_event_value(&message)
        else {
            panic!("expected newer order update");
        };
        assert!(recent.is_new(&newer));
        assert!(!recent.is_new(&newer));
        assert!(
            !recent.is_new(&first),
            "older full replays must remain deduplicated"
        );
    }

    #[test]
    fn binance_uses_the_official_private_user_stream_endpoint() {
        assert_eq!(
            binance_user_stream_url(false, "listen-key"),
            "wss://fstream.binance.com/private/ws/listen-key"
        );
        assert_eq!(
            binance_user_stream_url(true, "listen-key"),
            "wss://stream.binancefuture.com/ws/listen-key"
        );
    }

    #[test]
    fn binance_parses_low_latency_trade_lite_events() {
        let event = parse_binance_stream_event(
            r#"{"e":"TRADE_LITE","E":123,"T":122,"s":"MUUSDT","l":"0.25","t":456,"i":789}"#,
        );
        assert!(matches!(
            event,
            BinanceStreamEvent::Execution(BinanceExecutionEvent {
                symbol,
                order_id,
                trade_id: Some(trade_id),
                event_time_ms: Some(123),
                full_order_update: false,
                ..
            }) if symbol == "MUUSDT" && order_id == "789" && trade_id == "456"
        ));
    }

    #[test]
    fn binance_only_parses_positive_order_trade_updates() {
        let event = parse_binance_stream_event(
            r#"{"e":"ORDER_TRADE_UPDATE","E":123,"o":{"x":"TRADE","l":"0.25","s":"MUUSDT","t":456,"i":789}}"#,
        );
        assert!(matches!(event, BinanceStreamEvent::Execution(_)));
        assert_eq!(
            parse_binance_stream_event(
                r#"{"e":"ORDER_TRADE_UPDATE","E":124,"o":{"x":"NEW","l":"0","s":"MUUSDT","i":789}}"#,
            ),
            BinanceStreamEvent::Ignored
        );
    }

    #[test]
    fn binance_full_update_wakes_again_after_lite_but_duplicates_do_not() {
        let mut recent = RecentBinanceExecutions::default();
        let BinanceStreamEvent::Execution(lite) = parse_binance_stream_event(
            r#"{"e":"TRADE_LITE","E":123,"s":"MUUSDT","l":"0.25","t":456,"i":789}"#,
        ) else {
            panic!("expected lite fill");
        };
        let BinanceStreamEvent::Execution(full) = parse_binance_stream_event(
            r#"{"e":"ORDER_TRADE_UPDATE","E":124,"o":{"x":"TRADE","l":"0.25","s":"MUUSDT","t":456,"i":789}}"#,
        ) else {
            panic!("expected full fill");
        };
        assert!(recent.is_new(&lite));
        assert!(!recent.is_new(&lite));
        assert!(
            recent.is_new(&full),
            "the cache-ready full update must not be lost after an early lite wakeup"
        );
        assert!(!recent.is_new(&full));
        assert!(!recent.is_new(&lite));

        let mut reverse = RecentBinanceExecutions::default();
        assert!(reverse.is_new(&full));
        assert!(!reverse.is_new(&lite));
        assert!(!reverse.is_new(&full));
    }

    #[test]
    fn binance_reconnects_when_the_listen_key_expires() {
        assert_eq!(
            parse_binance_stream_event(r#"{"e":"listenKeyExpired","E":123}"#),
            BinanceStreamEvent::ListenKeyExpired
        );
    }

    #[test]
    fn binance_builds_an_exact_snapshot_only_after_observing_new_and_fill() {
        let cache = FuturesExecutionCache::default();
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":1000,"o":{"s":"MUUSDT","c":"r_run_0_B_1","S":"BUY","o":"LIMIT","f":"GTC","q":"3.14","p":"15.92","x":"NEW","X":"NEW","i":42,"l":"0","z":"0","L":"0","R":true,"T":1000}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.apply(new_update.clone());
        let client_order_id = ClientOrderId::parse("r_run_0_B_1").unwrap();
        assert!(!cache.knows_order("MUUSDT", "42"));
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 1000));
        assert!(cache.knows_order("MUUSDT", "42"));
        assert!(cache.snapshot("MUUSDT", &client_order_id, "42").is_none());

        let filled = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_B_1","S":"BUY","o":"LIMIT","f":"GTC","q":"3.14","p":"15.92","x":"TRADE","X":"FILLED","i":42,"l":"3.14","z":"3.14","L":"15.92","N":"USDT","n":"0.00999776","R":true,"T":1010,"t":7,"m":true,"rp":"0"}}"#,
        )
        .unwrap();
        cache.apply(parse_futures_order_update(&filled, Exchange::Binance).unwrap());
        let snapshot = cache.snapshot("MUUSDT", &client_order_id, "42").unwrap();
        assert_eq!(snapshot.cumulative_quantity, "3.14".parse().unwrap());
        assert_eq!(snapshot.cumulative_quote, "49.9888".parse().unwrap());
        assert_eq!(
            snapshot.fees_by_asset["USDT"],
            "0.00999776".parse().unwrap()
        );
        assert_eq!(snapshot.trades.len(), 1);
        assert_eq!(snapshot.order_time_ms, 1000);
        assert_eq!(snapshot.update_time_ms, 1010);
    }

    #[test]
    fn binance_preserves_the_rest_order_time_when_stream_transaction_time_differs() {
        let cache = FuturesExecutionCache::default();
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1786105786428,"T":1786105786428,"o":{"s":"SKHYNIXUSDT","c":"r_66ad5ac4ed3a_33_S_376","S":"SELL","o":"LIMIT","f":"GTC","q":"0.10","p":"1044","x":"NEW","X":"NEW","i":3112233650,"l":"0","z":"0","L":"0","R":false,"T":1786105786428}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.apply(new_update.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 1786105786410));

        let filled = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1786105801893,"T":1786105801893,"o":{"s":"SKHYNIXUSDT","c":"r_66ad5ac4ed3a_33_S_376","S":"SELL","o":"LIMIT","f":"GTC","q":"0.10","p":"1044","x":"TRADE","X":"FILLED","i":3112233650,"l":"0.10","z":"0.10","L":"1044","N":"USDT","n":"0","R":false,"T":1786105801893,"t":194852257,"m":true,"rp":"0"}}"#,
        )
        .unwrap();
        cache.apply(parse_futures_order_update(&filled, Exchange::Binance).unwrap());

        let snapshot = cache
            .snapshot(
                "SKHYNIXUSDT",
                &ClientOrderId::parse("r_66ad5ac4ed3a_33_S_376").unwrap(),
                "3112233650",
            )
            .unwrap();
        assert_eq!(snapshot.order_time_ms, 1786105786410);
    }

    #[test]
    fn cold_rest_binding_is_an_anchor_without_the_new_event() {
        let cache = FuturesExecutionCache::default();
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1001,"T":1001,"o":{"s":"MUUSDT","c":"r_run_0_B_early","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"15.92","x":"NEW","X":"NEW","i":47,"l":"0","z":"0","L":"0","R":true,"T":1001}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 990));
        assert!(cache.knows_order("MUUSDT", "47"));

        cache.apply(new_update);
        assert!(cache.knows_order("MUUSDT", "47"));
        assert!(
            cache
                .snapshot(
                    "MUUSDT",
                    &ClientOrderId::parse("r_run_0_B_early").unwrap(),
                    "47",
                )
                .is_none()
        );
    }

    #[test]
    fn futures_cache_reconstructs_a_fill_observed_before_rest_binding() {
        let cache = FuturesExecutionCache::default();
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1001,"T":1001,"o":{"s":"MUUSDT","c":"r_run_0_S_late","S":"SELL","o":"LIMIT","f":"GTC","q":"1","p":"16","x":"NEW","X":"NEW","i":48,"l":"0","z":"0","L":"0","R":false,"T":1001}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.apply(new_update.clone());
        let filled = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_S_late","S":"SELL","o":"LIMIT","f":"GTC","q":"1","p":"16","x":"TRADE","X":"FILLED","i":48,"l":"1","z":"1","L":"16","N":"USDT","n":"0.0032","R":false,"T":1010,"t":11,"m":true,"rp":"0"}}"#,
        )
        .unwrap();
        cache.apply(parse_futures_order_update(&filled, Exchange::Binance).unwrap());
        assert!(!cache.knows_order("MUUSDT", "48"));

        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 990));
        let snapshot = cache
            .snapshot(
                "MUUSDT",
                &ClientOrderId::parse("r_run_0_S_late").unwrap(),
                "48",
            )
            .unwrap();
        assert_eq!(snapshot.order_time_ms, 990);
        assert_eq!(snapshot.cumulative_quantity, Decimal::ONE);
        assert_eq!(snapshot.trades.len(), 1);
    }

    #[tokio::test]
    async fn futures_snapshot_waits_for_a_newly_arriving_cancellation_event() {
        let cache = FuturesExecutionCache::default();
        let waiting_cache = cache.clone();
        let client_order_id = ClientOrderId::parse("r_run_0_B_wait").unwrap();
        let waiting_client_order_id = client_order_id.clone();
        let waiter = tokio::spawn(async move {
            waiting_cache
                .wait_snapshot(
                    "MUUSDT",
                    &waiting_client_order_id,
                    "46",
                    Duration::from_millis(100),
                )
                .await
        });
        tokio::task::yield_now().await;
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":1000,"o":{"s":"MUUSDT","c":"r_run_0_B_wait","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"15.92","x":"NEW","X":"NEW","i":46,"l":"0","z":"0","L":"0","R":true,"T":1000}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.apply(new_update.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 999));
        let cancelled = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_B_wait","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"15.92","x":"CANCELED","X":"CANCELED","i":46,"l":"0","z":"0","L":"0","R":true,"T":1010}}"#,
        )
        .unwrap();
        cache.apply(parse_futures_order_update(&cancelled, Exchange::Binance).unwrap());

        let snapshot = waiter.await.unwrap().unwrap();
        assert_eq!(snapshot.order.client_order_id, client_order_id);
        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled)
        );
    }

    #[test]
    fn binance_accumulates_partial_fills_and_deduplicates_trade_ids() {
        let cache = FuturesExecutionCache::default();
        for text in [
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":1000,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"NEW","X":"NEW","i":43,"l":"0","z":"0","L":"0","R":false,"T":1000}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"PARTIALLY_FILLED","i":43,"l":"1","z":"1","L":"16","N":"USDT","n":"0.0032","R":false,"T":1010,"t":8,"m":true,"rp":"0"}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"PARTIALLY_FILLED","i":43,"l":"1","z":"1","L":"16","N":"USDT","n":"0.0032","R":false,"T":1010,"t":8,"m":true,"rp":"0"}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1020,"T":1020,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"FILLED","i":43,"l":"2","z":"3","L":"16","N":"USDT","n":"0.0064","R":false,"T":1020,"t":9,"m":true,"rp":"0"}}"#,
        ] {
            let message = serde_json::from_str::<Value>(text).unwrap();
            let update = parse_futures_order_update(&message, Exchange::Binance).unwrap();
            cache.apply(update.clone());
            if update.execution_type == "NEW" {
                cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&update, 999));
            }
        }
        let snapshot = cache
            .snapshot(
                "MUUSDT",
                &ClientOrderId::parse("r_run_0_S_1").unwrap(),
                "43",
            )
            .unwrap();
        assert_eq!(snapshot.cumulative_quantity, "3".parse().unwrap());
        assert_eq!(snapshot.cumulative_quote, "48".parse().unwrap());
        assert_eq!(snapshot.trades.len(), 2);
        assert_eq!(snapshot.fees_by_asset["USDT"], "0.0096".parse().unwrap());
    }

    #[test]
    fn binance_never_fast_paths_unbound_stream_updates() {
        let cache = FuturesExecutionCache::default();
        let filled = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_B_1","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"15.92","x":"TRADE","X":"FILLED","i":44,"l":"1","z":"1","L":"15.92","N":"USDT","n":"0.003184","R":true,"T":1010,"t":10,"m":true,"rp":"0"}}"#,
        )
        .unwrap();
        cache.apply(parse_futures_order_update(&filled, Exchange::Binance).unwrap());
        assert!(!cache.knows_order("MUUSDT", "44"));
        assert!(
            cache
                .snapshot(
                    "MUUSDT",
                    &ClientOrderId::parse("r_run_0_B_1").unwrap(),
                    "44",
                )
                .is_none()
        );
    }

    #[test]
    fn binance_reconnect_invalidates_all_session_local_snapshots() {
        let cache = FuturesExecutionCache::default();
        let new = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":1000,"o":{"s":"MUUSDT","c":"r_run_0_B_1","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"15.92","x":"NEW","X":"NEW","i":45,"l":"0","z":"0","L":"0","R":true,"T":1000}}"#,
        )
        .unwrap();
        let new_update = parse_futures_order_update(&new, Exchange::Binance).unwrap();
        cache.apply(new_update.clone());
        cache.bind_authoritative_snapshot(&authoritative_new_snapshot(&new_update, 999));
        assert!(cache.knows_order("MUUSDT", "45"));
        cache.begin_session();
        assert!(!cache.knows_order("MUUSDT", "45"));
    }

    #[test]
    fn bybit_parser_coalesces_one_message_by_symbol() {
        let events = parse_bybit_execution_events(
            r#"{"topic":"execution.fast","creationTime":456,"data":[{"category":"linear","symbol":"MUUSDT","orderId":"42"},{"category":"linear","symbol":"MUUSDT","orderId":"42"},{"category":"spot","symbol":"BTCUSDT","orderId":"43"}]}"#,
        );
        assert_eq!(
            events,
            vec![("MUUSDT".to_owned(), "42".to_owned(), Some(456))]
        );
    }

    #[test]
    fn aster_uses_signed_commission_cost_and_preserves_exchange_identity() {
        let message = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"ANSEMUSDT","c":"r_run_0_B_1","S":"BUY","o":"LIMIT","f":"GTC","q":"100","p":"0.38","x":"TRADE","X":"FILLED","i":42,"l":"100","z":"100","L":"0.38","N":"USDT","n":"-0.0076","R":true,"T":1010,"t":7,"m":true,"rp":"0"}}"#,
        )
        .unwrap();
        let update = parse_futures_order_update(&message, Exchange::Aster).unwrap();
        assert_eq!(update.order.exchange, Exchange::Aster);
        let trade = update.trade.unwrap();
        assert_eq!(trade.raw_commission, "-0.0076".parse().unwrap());
        assert_eq!(trade.commission_cost, "0.0076".parse().unwrap());
    }

    #[test]
    fn aster_unicode_order_update_is_not_dropped() {
        let message = serde_json::from_str::<Value>(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"牛来USDT","c":"g_unicode_aster","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"0.1","x":"NEW","X":"NEW","i":42,"l":"0","z":"0","L":"0","R":false,"T":1010}}"#,
        )
        .unwrap();

        let update = parse_futures_order_update(&message, Exchange::Aster).unwrap();
        assert_eq!(update.order.shape.symbol, "牛来USDT");
        assert!(parse_futures_order_update(&message, Exchange::Binance).is_none());
    }

    #[tokio::test]
    async fn bybit_builds_exact_snapshot_from_order_and_execution_streams() {
        let cache = BybitExecutionCache::default();
        cache.begin_session();
        let client_order_id = ClientOrderId::parse("r_run_0_B_1").unwrap();
        assert!(!cache.knows_order("MUUSDT", "42"));
        let new_message = json!({
            "topic": "order.linear",
            "data": [{
                "category": "linear", "symbol": "MUUSDT", "orderId": "42",
                "orderLinkId": client_order_id.as_str(), "positionIdx": 0,
                "side": "Buy", "orderType": "Limit", "qty": "3.14",
                "price": "15.92", "timeInForce": "GTC", "reduceOnly": true,
                "orderStatus": "New", "cumExecQty": "0", "cumExecValue": "0",
                "createdTime": "1000", "updatedTime": "1000"
            }]
        });
        for header in parse_bybit_order_updates(&new_message) {
            cache.apply_order(header);
        }
        assert!(cache.knows_order("MUUSDT", "42"));
        let execution_message = json!({
            "topic": "execution.linear",
            "data": [{
                "category": "linear", "symbol": "MUUSDT", "orderId": "42",
                "orderLinkId": client_order_id.as_str(), "execType": "Trade",
                "execId": "trade-7", "execPrice": "15.92", "execQty": "3.14",
                "execValue": "49.9888", "execFee": "0.00999776",
                "execTime": "1010", "feeCurrency": "USDT", "execPnl": "0",
                "side": "Buy", "isMaker": true
            }]
        });
        for (observed_client_order_id, trade) in parse_bybit_trade_updates(&execution_message) {
            cache.apply_trade(&observed_client_order_id, trade);
        }
        let filled_message = json!({
            "topic": "order.linear",
            "data": [{
                "category": "linear", "symbol": "MUUSDT", "orderId": "42",
                "orderLinkId": client_order_id.as_str(), "positionIdx": 0,
                "side": "Buy", "orderType": "Limit", "qty": "3.14",
                "price": "15.92", "timeInForce": "GTC", "reduceOnly": true,
                "orderStatus": "Filled", "cumExecQty": "3.14",
                "cumExecValue": "49.9888", "createdTime": "1000",
                "updatedTime": "1010"
            }]
        });
        for header in parse_bybit_order_updates(&filled_message) {
            cache.apply_order(header);
        }
        let snapshot = cache
            .wait_snapshot("MUUSDT", &client_order_id, "42", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(snapshot.cumulative_quantity, "3.14".parse().unwrap());
        assert_eq!(snapshot.cumulative_quote, "49.9888".parse().unwrap());
        assert_eq!(
            snapshot.fees_by_asset["USDT"],
            "0.00999776".parse().unwrap()
        );
        assert_eq!(snapshot.trades.len(), 1);
    }

    fn bybit_header(id: &str, status: &str, quantity: &str, time: u64) -> OrderExecutionHeader {
        let quantity: Decimal = quantity.parse().unwrap();
        parse_bybit_order_updates(&json!({
            "topic": "order.linear", "data": [{
                "category": "linear", "symbol": "MUUSDT", "orderId": id,
                "orderLinkId": "r_run_0_B_1", "positionIdx": 0,
                "side": "Buy", "orderType": "Limit", "qty": "500",
                "price": "2", "timeInForce": "GTC", "reduceOnly": true,
                "orderStatus": status, "cumExecQty": quantity.to_string(),
                "cumExecValue": (quantity * Decimal::TWO).to_string(),
                "createdTime": "1000", "updatedTime": time.to_string()
            }]
        }))
        .remove(0)
    }

    fn bybit_trade(id: &str, trade_id: &str, quantity: &str, time: u64) -> TradeFill {
        let quantity: Decimal = quantity.parse().unwrap();
        parse_bybit_trade_updates(&json!({
            "topic": "execution.linear", "data": [{
                "category": "linear", "symbol": "MUUSDT", "orderId": id,
                "orderLinkId": "r_run_0_B_1", "execType": "Trade",
                "execId": trade_id, "execPrice": "2", "execQty": quantity.to_string(),
                "execValue": (quantity * Decimal::TWO).to_string(),
                "execFee": (quantity / Decimal::from(500)).to_string(),
                "execTime": time.to_string(), "feeCurrency": "USDT", "execPnl": "0",
                "side": "Buy", "isMaker": false
            }]
        }))
        .remove(0)
        .1
    }

    async fn bybit_read(cache: &BybitExecutionCache, id: &str) -> Option<OrderExecutionSnapshot> {
        cache
            .wait_snapshot(
                "MUUSDT",
                &ClientOrderId::parse("r_run_0_B_1").unwrap(),
                id,
                Duration::ZERO,
            )
            .await
    }

    #[tokio::test]
    async fn bybit_late_new_does_not_erase_earlier_terminal_or_full_trades() {
        let cache = BybitExecutionCache::default();
        let client = ClientOrderId::parse("r_run_0_B_1").unwrap();
        cache.apply_trade(&client, bybit_trade("88101", "first", "460", 1010));
        cache.apply_order(bybit_header("88101", "Filled", "500", 1020));
        cache.apply_trade(&client, bybit_trade("88101", "second", "40", 1020));
        cache.apply_order(bybit_header("88101", "New", "0", 1000));
        let snapshot = bybit_read(&cache, "88101")
            .await
            .expect("late New must not erase fills");
        assert_eq!(snapshot.cumulative_quantity, Decimal::from(500));
        assert_eq!(snapshot.cumulative_quote, Decimal::from(1000));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::ONE);
        assert_eq!(snapshot.trades.len(), 2);
        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
    }

    #[tokio::test]
    async fn bybit_snapshot_is_consumed_and_duplicates_do_not_grant_another_read() {
        let cache = BybitExecutionCache::default();
        let client = ClientOrderId::parse("r_run_0_B_1").unwrap();
        cache.apply_order(bybit_header("88102", "New", "0", 1000));
        let trade = bybit_trade("88102", "first", "460", 1010);
        let partial = bybit_header("88102", "PartiallyFilled", "460", 1010);
        cache.apply_trade(&client, trade.clone());
        cache.apply_order(partial.clone());
        assert_eq!(
            bybit_read(&cache, "88102")
                .await
                .unwrap()
                .cumulative_quantity,
            Decimal::from(460)
        );
        cache.apply_trade(&client, trade);
        cache.apply_order(partial);
        assert!(
            bybit_read(&cache, "88102").await.is_none(),
            "duplicates must not refresh consumed evidence"
        );
        cache.apply_order(bybit_header("88102", "Filled", "500", 1020));
        assert!(
            bybit_read(&cache, "88102").await.is_none(),
            "missing final trade must not return stale partial"
        );
        cache.apply_trade(&client, bybit_trade("88102", "second", "40", 1020));
        assert_eq!(
            bybit_read(&cache, "88102")
                .await
                .unwrap()
                .cumulative_quantity,
            Decimal::from(500)
        );
    }

    #[tokio::test]
    async fn bybit_late_full_evidence_publishes_one_wakeup_without_fast_stream() {
        let cache = BybitExecutionCache::default();
        let mut wakeups = subscribe_execution_wakeups();
        let client = ClientOrderId::parse("r_run_0_B_1").unwrap();
        cache.apply_order(bybit_header("88103", "New", "0", 1000));
        cache.apply_order(bybit_header("88103", "Filled", "500", 1020));
        cache.apply_trade(&client, bybit_trade("88103", "first", "490", 1010));
        let final_trade = bybit_trade("88103", "second", "10", 1020);
        cache.apply_trade(&client, final_trade.clone());
        cache.apply_trade(&client, final_trade);
        let mut matching = Vec::new();
        while let Ok(wakeup) = wakeups.try_recv() {
            if wakeup.exchange_order_id.as_deref() == Some("88103") {
                matching.push(wakeup);
            }
        }
        assert_eq!(
            matching.len(),
            1,
            "complete full evidence must wake once, even after an earlier fast wake"
        );
        assert_eq!(matching[0].exchange_event_time_ms, Some(1020));
        assert_eq!(bybit_read(&cache, "88103").await.unwrap().trades.len(), 2);
    }

    #[tokio::test]
    async fn bybit_conflicting_duplicate_trade_cannot_be_silently_accepted() {
        let cache = BybitExecutionCache::default();
        let client = ClientOrderId::parse("r_run_0_B_1").unwrap();
        cache.apply_order(bybit_header("88104", "New", "0", 1000));
        cache.apply_order(bybit_header("88104", "Filled", "500", 1020));
        cache.apply_trade(&client, bybit_trade("88104", "first", "500", 1020));
        let mut conflict = bybit_trade("88104", "first", "500", 1020);
        conflict.commission_cost = Decimal::from(99);
        cache.apply_trade(&client, conflict);
        cache.apply_order(bybit_header("88104", "New", "0", 1000));
        cache.apply_order(bybit_header("88104", "Filled", "500", 1020));
        cache.apply_trade(&client, bybit_trade("88104", "first", "500", 1020));
        assert!(
            bybit_read(&cache, "88104").await.is_none(),
            "conflict must stay quarantined for this session"
        );
    }

    #[tokio::test]
    async fn bybit_reconnect_ends_old_waiter_instead_of_consuming_new_session() {
        let cache = BybitExecutionCache::default();
        let client = ClientOrderId::parse("r_run_0_B_1").unwrap();
        let old_wait = cache.wait_snapshot("MUUSDT", &client, "88105", Duration::from_secs(1));
        tokio::pin!(old_wait);
        assert!(matches!(
            futures::poll!(old_wait.as_mut()),
            std::task::Poll::Pending
        ));
        cache.begin_session();
        cache.apply_order(bybit_header("88105", "New", "0", 1000));
        cache.apply_order(bybit_header("88105", "Filled", "500", 1020));
        cache.apply_trade(&client, bybit_trade("88105", "first", "500", 1020));
        assert!(
            old_wait.await.is_none(),
            "old waiter must not cross reconnect boundary"
        );
        assert_eq!(
            bybit_read(&cache, "88105")
                .await
                .unwrap()
                .cumulative_quantity,
            Decimal::from(500)
        );
    }

    fn bybit_placement(
        id: &str,
    ) -> (
        crate::domain::OrderIntent,
        crate::exchange::PlacementAcknowledgement,
    ) {
        let header = bybit_header(id, "New", "0", 1000);
        let intent = crate::domain::OrderIntent::prepare(
            header.order.client_order_id.clone(),
            Exchange::Bybit,
            header.order.shape,
            990,
        )
        .unwrap();
        let ack = crate::exchange::PlacementAcknowledgement {
            client_order_id: intent.client_order_id.clone(),
            exchange_order_id: id.to_owned(),
        };
        (intent, ack)
    }

    #[tokio::test]
    async fn bybit_placement_ack_recovers_immediate_fill_before_ack_without_new() {
        for (id, first, second) in [("88106", "460", "40"), ("88107", "490", "10")] {
            let cache = BybitExecutionCache::default();
            let (intent, ack) = bybit_placement(id);
            cache.apply_trade(
                &intent.client_order_id,
                bybit_trade(id, "second", second, 1020),
            );
            cache.apply_order(bybit_header(id, "Filled", "500", 1020));
            cache.apply_trade(
                &intent.client_order_id,
                bybit_trade(id, "first", first, 1010),
            );
            assert!(
                bybit_read(&cache, id).await.is_none(),
                "unbound evidence is not ownership"
            );
            cache.register_placement(&intent, &ack, 1030);
            let snapshot = bybit_read(&cache, id)
                .await
                .expect("ACK must bind earlier exact evidence");
            assert_eq!(snapshot.cumulative_quantity, Decimal::from(500));
            assert_eq!(snapshot.cumulative_quote, Decimal::from(1000));
            assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::ONE);
            assert_eq!(snapshot.trades.len(), 2);
            cache.register_placement(&intent, &ack, 1040);
            assert!(
                bybit_read(&cache, id).await.is_none(),
                "duplicate ACK is not fresh evidence"
            );
        }
    }

    #[tokio::test]
    async fn bybit_placement_ack_alone_never_fabricates_new_zero_snapshot() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88108");
        cache.register_placement(&intent, &ack, 1005);
        assert!(cache.knows_order("MUUSDT", "88108"));
        assert!(bybit_read(&cache, "88108").await.is_none());
        cache.apply_order(bybit_header("88108", "Filled", "500", 1020));
        assert!(
            bybit_read(&cache, "88108").await.is_none(),
            "order totals alone cannot fabricate fees"
        );
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88108", "first", "500", 1020),
        );
        assert_eq!(
            bybit_read(&cache, "88108")
                .await
                .unwrap()
                .cumulative_quantity,
            Decimal::from(500)
        );
    }

    #[tokio::test]
    async fn bybit_foreign_ack_and_immutable_shape_conflicts_do_not_bind_fills() {
        let cache = BybitExecutionCache::default();
        let (mut intent, mut ack) = bybit_placement("88109");
        ack.client_order_id = ClientOrderId::parse("manual_order").unwrap();
        cache.register_placement(&intent, &ack, 1005);
        assert!(!cache.knows_order("MUUSDT", "88109"));
        ack.client_order_id = intent.client_order_id.clone();
        intent.shape.reduce_only = false;
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_order(bybit_header("88109", "Filled", "500", 1020));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88109", "first", "500", 1020),
        );
        assert!(bybit_read(&cache, "88109").await.is_none());
    }

    #[tokio::test]
    async fn bybit_fast_progress_invalidates_unconsumed_partial_until_full_evidence() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88110");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_order(bybit_header("88110", "New", "0", 1000));
        cache.apply_order(bybit_header("88110", "PartiallyFilled", "460", 1010));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88110", "first", "460", 1010),
        );
        publish_bybit_message(&json!({"topic":"execution.fast.linear","creationTime":1020,
            "data":[{"category":"linear","symbol":"MUUSDT","orderId":"88110",
                "orderLinkId":"", "execId":"second","execQty":"40","execPrice":"2","execTime":"1020"}]
        }).to_string(), &cache);
        assert!(
            bybit_read(&cache, "88110").await.is_none(),
            "new fast execution forbids cached partial reuse"
        );
        cache.apply_order(bybit_header("88110", "Filled", "500", 1020));
        assert!(
            bybit_read(&cache, "88110").await.is_none(),
            "fast has no fee evidence"
        );
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88110", "second", "40", 1020),
        );
        let snapshot = bybit_read(&cache, "88110").await.unwrap();
        assert_eq!(snapshot.cumulative_quantity, Decimal::from(500));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::ONE);
    }

    #[tokio::test]
    async fn bybit_all_ack_order_and_full_trade_arrival_orders_preserve_exact_totals() {
        for a in 0..4 {
            for b in 0..4 {
                for c in 0..4 {
                    for d in 0..4 {
                        let events = [a, b, c, d];
                        if events.into_iter().collect::<BTreeSet<_>>().len() != 4 {
                            continue;
                        }
                        let cache = BybitExecutionCache::default();
                        let (intent, ack) = bybit_placement("88111");
                        for event in events {
                            match event {
                                0 => cache.register_placement(&intent, &ack, 1030),
                                1 => {
                                    cache.apply_order(bybit_header("88111", "Filled", "500", 1020))
                                }
                                2 => cache.apply_trade(
                                    &intent.client_order_id,
                                    bybit_trade("88111", "first", "460", 1010),
                                ),
                                3 => cache.apply_trade(
                                    &intent.client_order_id,
                                    bybit_trade("88111", "second", "40", 1020),
                                ),
                                _ => unreachable!(),
                            }
                        }
                        let snapshot = bybit_read(&cache, "88111")
                            .await
                            .expect("all 24 permutations must complete");
                        assert_eq!(snapshot.cumulative_quantity, Decimal::from(500));
                        assert_eq!(snapshot.cumulative_quote, Decimal::from(1000));
                        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::ONE);
                        assert_eq!(snapshot.trades.len(), 2);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn bybit_full_completion_wakes_again_after_deduplicated_fast_hint() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88112");
        cache.register_placement(&intent, &ack, 1005);
        let mut wakeups = subscribe_execution_wakeups();
        let fast = json!({"topic":"execution.fast.linear","creationTime":1020,
            "data":[{"category":"linear","symbol":"MUUSDT","orderId":"88112",
                "orderLinkId":"", "execId":"first","execQty":"500","execPrice":"2","execTime":"1020"}]
        }).to_string();
        publish_bybit_message(&fast, &cache);
        publish_bybit_message(&fast, &cache);
        let count = |receiver: &mut broadcast::Receiver<ExecutionWakeup>| {
            let mut count = 0;
            while let Ok(wakeup) = receiver.try_recv() {
                if wakeup.exchange_order_id.as_deref() == Some("88112") {
                    count += 1;
                }
            }
            count
        };
        assert_eq!(
            count(&mut wakeups),
            1,
            "duplicate fast execution must not flood wakeups"
        );
        assert!(bybit_read(&cache, "88112").await.is_none());
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88112", "first", "500", 1020),
        );
        assert_eq!(
            count(&mut wakeups),
            0,
            "still missing authoritative order header"
        );
        let filled = bybit_header("88112", "Filled", "500", 1020);
        cache.apply_order(filled.clone());
        cache.apply_order(filled);
        publish_bybit_message(&fast, &cache);
        assert_eq!(
            count(&mut wakeups),
            1,
            "full evidence must wake after the earlier insufficient fast hint"
        );
        assert_eq!(
            bybit_read(&cache, "88112").await.unwrap().fees_by_asset["USDT"],
            Decimal::ONE
        );
    }

    #[tokio::test]
    async fn bybit_reconnect_discards_pending_and_complete_previous_session_evidence() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88113");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88113", "first", "460", 1010),
        );
        cache.begin_session();
        assert!(!cache.knows_order("MUUSDT", "88113"));
        cache.register_placement(&intent, &ack, 1030);
        cache.apply_order(bybit_header("88113", "Filled", "500", 1020));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88113", "second", "40", 1020),
        );
        assert!(
            bybit_read(&cache, "88113").await.is_none(),
            "must not mix fills across the disconnected gap"
        );
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88113", "first", "460", 1010),
        );
        assert!(bybit_read(&cache, "88113").await.is_some());
        cache.begin_session();
        assert!(
            bybit_read(&cache, "88113").await.is_none(),
            "old complete snapshot must also be discarded"
        );
    }

    #[tokio::test]
    async fn bybit_newer_timestamp_cannot_regress_cumulative_quantity_or_terminal_state() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88114");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_order(bybit_header("88114", "Filled", "500", 1020));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88114", "first", "500", 1020),
        );
        assert!(bybit_read(&cache, "88114").await.is_some());
        cache.apply_order(bybit_header("88114", "PartiallyFilled", "460", 1030));
        cache.apply_order(bybit_header("88114", "New", "0", 1040));
        assert!(
            bybit_read(&cache, "88114").await.is_none(),
            "regressive headers must not grant a fresh generation"
        );
    }

    fn bybit_rest_partial(id: &str) -> OrderExecutionSnapshot {
        OrderExecutionSnapshot {
            order: bybit_header(id, "PartiallyFilled", "460", 1010).order,
            cumulative_quantity: Decimal::from(460),
            cumulative_quote: Decimal::from(920),
            fees_by_asset: BTreeMap::from([("USDT".to_owned(), "0.92".parse().unwrap())]),
            trades: vec![bybit_trade(id, "first", "460", 1010)],
            order_time_ms: 1000,
            update_time_ms: 1010,
        }
    }

    #[tokio::test]
    async fn bybit_rest_seed_restores_reconnected_order_for_next_fresh_full_fill() {
        let cache = BybitExecutionCache::default();
        cache.begin_session();
        let epoch = cache.session_epoch();
        let snapshot = bybit_rest_partial("88201");
        assert!(cache.seed_verified_snapshot(epoch, &snapshot));
        assert!(cache.knows_order("MUUSDT", "88201"));
        assert!(
            bybit_read(&cache, "88201").await.is_none(),
            "REST seed is already consumed, not stream progress"
        );
        cache.apply_order(bybit_header("88201", "Filled", "500", 1020));
        assert!(bybit_read(&cache, "88201").await.is_none());
        cache.apply_trade(
            &snapshot.order.client_order_id,
            bybit_trade("88201", "second", "40", 1020),
        );
        let filled = bybit_read(&cache, "88201")
            .await
            .expect("REST prefix plus fresh full trade must complete");
        assert_eq!(filled.cumulative_quantity, Decimal::from(500));
        assert_eq!(filled.cumulative_quote, Decimal::from(1000));
        assert_eq!(filled.fees_by_asset["USDT"], Decimal::ONE);
        assert_eq!(filled.order_time_ms, 1000);
        assert_eq!(
            filled
                .trades
                .iter()
                .map(|trade| trade.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
    }

    #[tokio::test]
    async fn bybit_rest_seed_rejects_previous_epoch_without_mixing_new_session_evidence() {
        let cache = BybitExecutionCache::default();
        let old_epoch = cache.session_epoch();
        let mut old = bybit_rest_partial("88202");
        old.trades[0].trade_id = "old-session-only".to_owned();
        cache.begin_session();
        assert!(!cache.seed_verified_snapshot(old_epoch, &old));
        assert!(!cache.knows_order("MUUSDT", "88202"));
        let current = bybit_rest_partial("88202");
        assert!(cache.seed_verified_snapshot(cache.session_epoch(), &current));
        assert!(!cache.seed_verified_snapshot(old_epoch, &old));
        cache.apply_trade(
            &current.order.client_order_id,
            bybit_trade("88202", "second", "40", 1020),
        );
        cache.apply_order(bybit_header("88202", "Filled", "500", 1020));
        let filled = bybit_read(&cache, "88202").await.unwrap();
        assert_eq!(filled.cumulative_quantity, Decimal::from(500));
        assert_eq!(filled.trades.len(), 2);
        assert!(
            filled
                .trades
                .iter()
                .all(|trade| trade.trade_id != "old-session-only")
        );
    }

    #[tokio::test]
    async fn bybit_rest_seed_older_prefix_completes_but_never_consumes_newer_ws_progress() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88203");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_order(bybit_header("88203", "Filled", "500", 1020));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88203", "second", "40", 1020),
        );
        let prefix = bybit_rest_partial("88203");
        let epoch = cache.session_epoch();
        assert!(cache.seed_verified_snapshot(epoch, &prefix));
        assert!(cache.seed_verified_snapshot(epoch, &prefix));
        let filled = bybit_read(&cache, "88203")
            .await
            .expect("older REST must preserve the fresh WS generation");
        assert_eq!(filled.cumulative_quantity, Decimal::from(500));
        assert_eq!(filled.update_time_ms, 1020);
        assert_eq!(filled.fees_by_asset["USDT"], Decimal::ONE);
        assert_eq!(filled.trades.len(), 2);
        assert_eq!(
            filled.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
        assert!(cache.seed_verified_snapshot(epoch, &prefix));
        assert!(
            bybit_read(&cache, "88203").await.is_none(),
            "older REST cannot regrant consumed WS progress"
        );
    }

    #[tokio::test]
    async fn bybit_rest_seed_does_not_erase_already_complete_unconsumed_ws_snapshot() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88204");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_order(bybit_header("88204", "Filled", "500", 1020));
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88204", "first", "460", 1010),
        );
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88204", "second", "40", 1020),
        );
        assert!(cache.seed_verified_snapshot(cache.session_epoch(), &bybit_rest_partial("88204")));
        let filled = bybit_read(&cache, "88204").await.unwrap();
        assert_eq!(filled.cumulative_quantity, Decimal::from(500));
        assert_eq!(filled.trades.len(), 2);
    }

    #[tokio::test]
    async fn bybit_rest_seed_repeated_snapshot_never_republishes_wakeup_or_refreshes_read() {
        let cache = BybitExecutionCache::default();
        let mut wakeups = subscribe_execution_wakeups();
        let epoch = cache.session_epoch();
        let prefix = bybit_rest_partial("88205");
        assert!(cache.seed_verified_snapshot(epoch, &prefix));
        assert!(cache.seed_verified_snapshot(epoch, &prefix));
        assert!(bybit_read(&cache, "88205").await.is_none());
        let count = |receiver: &mut broadcast::Receiver<ExecutionWakeup>| {
            let mut count = 0;
            while let Ok(wakeup) = receiver.try_recv() {
                if wakeup.exchange_order_id.as_deref() == Some("88205") {
                    count += 1;
                }
            }
            count
        };
        assert_eq!(
            count(&mut wakeups),
            0,
            "REST does not publish execution stream wakeups"
        );
        cache.apply_trade(
            &prefix.order.client_order_id,
            bybit_trade("88205", "second", "40", 1020),
        );
        cache.apply_order(bybit_header("88205", "Filled", "500", 1020));
        assert_eq!(count(&mut wakeups), 1);
        let filled = bybit_read(&cache, "88205").await.unwrap();
        assert!(cache.seed_verified_snapshot(epoch, &filled));
        assert!(cache.seed_verified_snapshot(epoch, &filled));
        assert!(bybit_read(&cache, "88205").await.is_none());
        assert_eq!(
            count(&mut wakeups),
            0,
            "repeated REST cannot duplicate the full-fill wakeup"
        );
    }

    #[tokio::test]
    async fn bybit_rest_seed_rejects_quarantine_foreign_identity_and_inconsistent_evidence() {
        for mutation in 0..14 {
            let cache = BybitExecutionCache::default();
            let (intent, ack) = bybit_placement("88206");
            cache.register_placement(&intent, &ack, 1005);
            let mut snapshot = bybit_rest_partial("88206");
            match mutation {
                0 => snapshot.order.exchange = Exchange::Binance,
                1 => snapshot.order.client_order_id = ClientOrderId::parse("manual_order").unwrap(),
                2 => snapshot.order.shape.reduce_only = false,
                3 => snapshot.trades[0].exchange_order_id = "foreign".to_owned(),
                4 => snapshot.cumulative_quantity = Decimal::from(459),
                5 => snapshot.cumulative_quote = Decimal::from(919),
                6 => {
                    snapshot
                        .fees_by_asset
                        .insert("USDT".to_owned(), Decimal::ZERO);
                }
                7 => snapshot.order.executed_quantity = Some(Decimal::ZERO),
                8 => snapshot.order.lifecycle = OrderLifecycle::Active(ActiveOrderStatus::New),
                9 => snapshot.order_time_ms = 1011,
                10 => snapshot.trades.push(snapshot.trades[0].clone()),
                11 => {
                    cache.apply_trade(&intent.client_order_id, snapshot.trades[0].clone());
                    snapshot.trades[0].commission_cost = Decimal::from(2);
                    snapshot.trades[0].raw_commission = Decimal::from(2);
                    snapshot
                        .fees_by_asset
                        .insert("USDT".to_owned(), Decimal::from(2));
                }
                12 => {
                    cache.apply_trade(&intent.client_order_id, snapshot.trades[0].clone());
                    let mut conflicting = snapshot.trades[0].clone();
                    conflicting.quantity = Decimal::from(1);
                    cache.apply_trade(&intent.client_order_id, conflicting);
                }
                13 => {
                    cache.apply_order(bybit_header("88206", "PartiallyFilled", "460", 1010));
                    snapshot.order_time_ms = 999;
                }
                _ => unreachable!(),
            }
            assert!(
                !cache.seed_verified_snapshot(cache.session_epoch(), &snapshot),
                "must reject mutation {mutation}"
            );
            assert!(bybit_read(&cache, "88206").await.is_none());
        }
        let cache = BybitExecutionCache::default();
        assert!(
            cache.seed_verified_snapshot(cache.session_epoch(), &bybit_rest_partial("88207")),
            "rejecting bad evidence must not mean rejecting every seed"
        );
    }

    #[tokio::test]
    async fn bybit_rest_seed_keeps_uncovered_fast_evidence_pending() {
        let cache = BybitExecutionCache::default();
        let (intent, ack) = bybit_placement("88208");
        cache.register_placement(&intent, &ack, 1005);
        cache.apply_fast("MUUSDT", "88208", "second");
        let prefix = bybit_rest_partial("88208");
        assert!(cache.seed_verified_snapshot(cache.session_epoch(), &prefix));
        cache.apply_order(bybit_header("88208", "Filled", "500", 1020));
        assert!(
            bybit_read(&cache, "88208").await.is_none(),
            "REST prefix cannot clear an uncovered fast execution"
        );
        cache.apply_trade(
            &intent.client_order_id,
            bybit_trade("88208", "second", "40", 1020),
        );
        assert_eq!(
            bybit_read(&cache, "88208")
                .await
                .unwrap()
                .cumulative_quantity,
            Decimal::from(500)
        );
    }
}
