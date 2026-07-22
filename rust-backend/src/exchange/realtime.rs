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
use tokio_tungstenite::{connect_async, tungstenite::Message};
use zeroize::Zeroizing;

use crate::{
    domain::{
        ClientOrderId, Exchange, OrderKind, OrderShape, OrderSide, TerminalOrderStatus, TimeInForce,
    },
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, OrderExecutionSnapshot, OrderLifecycle, TradeFill,
        bybit_codec::{parse_execution_page, parse_order_row},
        execution::{OrderExecutionHeader, assemble_execution_snapshot},
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
#[cfg(not(test))]
const BINANCE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30 * 60);
#[cfg(not(test))]
const BYBIT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinanceExecutionEvent {
    symbol: String,
    order_id: String,
    trade_id: Option<String>,
    event_time_ms: Option<u64>,
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
    keys: BTreeSet<(String, String, String)>,
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
        if !self.keys.insert(key.clone()) {
            return false;
        }
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
    order_time_ms: u64,
    trades: BTreeMap<String, TradeFill>,
    snapshot: Option<OrderExecutionSnapshot>,
    last_update_time_ms: u64,
}

#[derive(Debug, Default)]
struct FuturesExecutionCacheState {
    entries: BTreeMap<(String, String), FuturesObservedOrder>,
    order: VecDeque<(String, String)>,
}

/// Per-account cache populated only by one uninterrupted Binance-compatible
/// futures user-stream session. Missing NEW events, stale updates, or incomplete
/// trade totals always fall back to REST.
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
        *state = FuturesExecutionCacheState::default();
        self.changed.notify_waiters();
    }

    pub(crate) fn apply(&self, update: FuturesOrderUpdate) {
        let key = (
            update.order.shape.symbol.clone(),
            update.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if update.execution_type == "NEW"
            && matches!(
                update.order.lifecycle,
                OrderLifecycle::Active(ActiveOrderStatus::New)
            )
        {
            if !state.entries.contains_key(&key) {
                state.order.push_back(key.clone());
            }
            state.entries.insert(
                key.clone(),
                FuturesObservedOrder {
                    initial_shape: update.order.shape.clone(),
                    client_order_id: update.order.client_order_id.clone(),
                    order_time_ms: update.update_time_ms,
                    trades: BTreeMap::new(),
                    snapshot: None,
                    last_update_time_ms: update.update_time_ms,
                },
            );
            prune_futures_execution_cache(&mut state);
            drop(state);
            self.changed.notify_waiters();
            return;
        }

        let Some(observed) = state.entries.get(&key) else {
            return;
        };
        if observed.client_order_id != update.order.client_order_id
            || observed.initial_shape != update.order.shape
        {
            state.entries.remove(&key);
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        let observed = state
            .entries
            .get_mut(&key)
            .expect("the validated futures execution cache entry must remain present");
        if update.update_time_ms < observed.last_update_time_ms
            || observed.snapshot.as_ref().is_some_and(|snapshot| {
                update.update_time_ms == observed.last_update_time_ms
                    && update.cumulative_quantity < snapshot.cumulative_quantity
            })
        {
            return;
        }
        if let Some(trade) = update.trade {
            observed
                .trades
                .entry(trade.trade_id.clone())
                .or_insert(trade);
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
                    order: update.order,
                    cumulative_quantity: update.cumulative_quantity,
                    cumulative_quote,
                    order_time_ms: observed.order_time_ms,
                    update_time_ms: update.update_time_ms,
                },
                trades,
            )
            .ok()
        });
        observed.last_update_time_ms = update.update_time_ms;
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn knows_order(&self, symbol: &str, exchange_order_id: &str) -> bool {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .contains_key(&key)
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
        (observed.client_order_id == *client_order_id)
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
        if !self.knows_order(symbol, exchange_order_id) {
            return None;
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

fn prune_futures_execution_cache(state: &mut FuturesExecutionCacheState) {
    while state.order.len() > FUTURES_EXECUTION_CACHE_CAPACITY {
        if let Some(expired) = state.order.pop_front() {
            state.entries.remove(&expired);
        }
    }
}

#[derive(Debug, Clone)]
struct BybitObservedOrder {
    initial_shape: OrderShape,
    client_order_id: ClientOrderId,
    header: OrderExecutionHeader,
    trades: BTreeMap<String, TradeFill>,
    snapshot: Option<OrderExecutionSnapshot>,
}

#[derive(Debug, Default)]
struct BybitExecutionCacheState {
    entries: BTreeMap<(String, String), BybitObservedOrder>,
    order: VecDeque<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BybitExecutionCache {
    state: Arc<Mutex<BybitExecutionCacheState>>,
    changed: Arc<tokio::sync::Notify>,
}

impl BybitExecutionCache {
    pub(crate) fn begin_session(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = BybitExecutionCacheState::default();
        self.changed.notify_waiters();
    }

    fn apply_order(&self, header: OrderExecutionHeader) {
        let key = (
            header.order.shape.symbol.clone(),
            header.order.exchange_order_id.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(
            header.order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        ) && header.cumulative_quantity.is_zero()
        {
            if !state.entries.contains_key(&key) {
                state.order.push_back(key.clone());
            }
            state.entries.insert(
                key,
                BybitObservedOrder {
                    initial_shape: header.order.shape.clone(),
                    client_order_id: header.order.client_order_id.clone(),
                    header,
                    trades: BTreeMap::new(),
                    snapshot: None,
                },
            );
            prune_bybit_execution_cache(&mut state);
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        let Some(observed) = state.entries.get_mut(&key) else {
            return;
        };
        if observed.client_order_id != header.order.client_order_id
            || observed.initial_shape != header.order.shape
        {
            state.entries.remove(&key);
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        if header.update_time_ms < observed.header.update_time_ms
            || (header.update_time_ms == observed.header.update_time_ms
                && header.cumulative_quantity < observed.header.cumulative_quantity)
        {
            return;
        }
        observed.header = header;
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
        let Some(observed) = state.entries.get_mut(&key) else {
            return;
        };
        if observed.client_order_id != *client_order_id
            || observed.initial_shape.symbol != trade.symbol
            || observed.initial_shape.side != trade.side
        {
            state.entries.remove(&key);
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        observed
            .trades
            .entry(trade.trade_id.clone())
            .or_insert(trade);
        refresh_bybit_snapshot(observed);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) async fn wait_snapshot(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
        maximum_wait: Duration,
    ) -> Option<OrderExecutionSnapshot> {
        let key = (symbol.to_ascii_uppercase(), exchange_order_id.to_owned());
        let known = || {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entries
                .contains_key(&key)
        };
        let snapshot = || {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let observed = state.entries.get(&key)?;
            (observed.client_order_id == *client_order_id)
                .then(|| observed.snapshot.clone())
                .flatten()
        };
        if let Some(snapshot) = snapshot() {
            return Some(snapshot);
        }
        if !known() {
            return None;
        }
        let deadline = tokio::time::Instant::now() + maximum_wait;
        loop {
            let changed = self.changed.notified();
            if let Some(snapshot) = snapshot() {
                return Some(snapshot);
            }
            let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
            tokio::time::timeout(remaining, changed).await.ok()?;
        }
    }
}

fn refresh_bybit_snapshot(observed: &mut BybitObservedOrder) {
    observed.snapshot = assemble_execution_snapshot(
        observed.header.clone(),
        observed.trades.values().cloned().collect(),
    )
    .ok();
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

#[cfg(not(test))]
pub(crate) fn publish_execution_wakeup(
    exchange: Exchange,
    symbol: &str,
    exchange_order_id: Option<String>,
    exchange_event_time_ms: Option<u64>,
) {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return;
    }
    let observed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default();
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
        let (mut socket, _) = match connect_async(&stream_url).await {
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
        reconnect_delay = RECONNECT_MIN;
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
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                                if publish_binance_message(
                                    text,
                                    &mut recent_executions,
                                    &execution_cache,
                                ) {
                                    break;
                                }
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
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
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
    if symbol.is_empty() || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
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

#[cfg(not(test))]
fn publish_bybit_message(text: &str, execution_cache: &BybitExecutionCache) {
    if let Ok(message) = serde_json::from_str::<Value>(text) {
        for header in parse_bybit_order_updates(&message) {
            execution_cache.apply_order(header);
        }
        for (client_order_id, trade) in parse_bybit_trade_updates(&message) {
            execution_cache.apply_trade(&client_order_id, trade);
        }
    }
    for (symbol, order_id, event_time) in parse_bybit_execution_events(text) {
        publish_execution_wakeup(Exchange::Bybit, &symbol, Some(order_id), event_time);
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
    fn binance_deduplicates_trade_lite_and_order_update_for_the_same_fill() {
        let mut recent = RecentBinanceExecutions::default();
        let lite = BinanceExecutionEvent {
            symbol: "MUUSDT".into(),
            order_id: "789".into(),
            trade_id: Some("456".into()),
            event_time_ms: Some(123),
        };
        let full = BinanceExecutionEvent {
            event_time_ms: Some(124),
            ..lite.clone()
        };
        assert!(recent.is_new(&lite));
        assert!(!recent.is_new(&full));
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
        cache.apply(parse_futures_order_update(&new, Exchange::Binance).unwrap());
        let client_order_id = ClientOrderId::parse("r_run_0_B_1").unwrap();
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
    fn binance_accumulates_partial_fills_and_deduplicates_trade_ids() {
        let cache = FuturesExecutionCache::default();
        for text in [
            r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":1000,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"NEW","X":"NEW","i":43,"l":"0","z":"0","L":"0","R":false,"T":1000}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"PARTIALLY_FILLED","i":43,"l":"1","z":"1","L":"16","N":"USDT","n":"0.0032","R":false,"T":1010,"t":8,"m":true,"rp":"0"}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1010,"T":1010,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"PARTIALLY_FILLED","i":43,"l":"1","z":"1","L":"16","N":"USDT","n":"0.0032","R":false,"T":1010,"t":8,"m":true,"rp":"0"}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","E":1020,"T":1020,"o":{"s":"MUUSDT","c":"r_run_0_S_1","S":"SELL","o":"LIMIT","f":"GTC","q":"3","p":"16","x":"TRADE","X":"FILLED","i":43,"l":"2","z":"3","L":"16","N":"USDT","n":"0.0064","R":false,"T":1020,"t":9,"m":true,"rp":"0"}}"#,
        ] {
            let message = serde_json::from_str::<Value>(text).unwrap();
            cache.apply(parse_futures_order_update(&message, Exchange::Binance).unwrap());
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
    fn binance_never_fast_paths_an_order_whose_new_event_was_not_observed() {
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
        cache.apply(parse_futures_order_update(&new, Exchange::Binance).unwrap());
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

    #[tokio::test]
    async fn bybit_builds_exact_snapshot_from_order_and_execution_streams() {
        let cache = BybitExecutionCache::default();
        cache.begin_session();
        let client_order_id = ClientOrderId::parse("r_run_0_B_1").unwrap();
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
}
