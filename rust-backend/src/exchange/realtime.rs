use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, OnceLock, Weak},
};

#[cfg(not(test))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use futures::{SinkExt, StreamExt};
#[cfg(not(test))]
use hmac::{Hmac, Mac};
use serde_json::Value;
#[cfg(not(test))]
use serde_json::json;
#[cfg(not(test))]
use sha2::Sha256;
use tokio::sync::broadcast;
#[cfg(not(test))]
use tokio_tungstenite::{connect_async, tungstenite::Message};
use zeroize::Zeroizing;

use crate::domain::Exchange;

const EXECUTION_WAKEUP_CAPACITY: usize = 1_024;
const RECENT_BINANCE_EXECUTION_CAPACITY: usize = 4_096;
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
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!("Binance user execution stream was not started outside a Tokio runtime");
        return;
    };
    runtime.spawn(run_binance_execution_stream(testnet, api_key, lifetime));
}

#[cfg(test)]
pub(crate) fn spawn_binance_execution_stream(
    _testnet: bool,
    _api_key: Zeroizing<String>,
    _lifetime: Weak<()>,
) {
}

#[cfg(not(test))]
async fn run_binance_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    lifetime: Weak<()>,
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
                            if publish_binance_message(text.as_ref(), &mut recent_executions) {
                                break;
                            }
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                                if publish_binance_message(text, &mut recent_executions) {
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
fn publish_binance_message(text: &str, recent_executions: &mut RecentBinanceExecutions) -> bool {
    match parse_binance_stream_event(text) {
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

fn parse_binance_stream_event(text: &str) -> BinanceStreamEvent {
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return BinanceStreamEvent::Ignored;
    };
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

#[cfg(not(test))]
pub(crate) fn spawn_bybit_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!("Bybit execution stream was not started outside a Tokio runtime");
        return;
    };
    runtime.spawn(run_bybit_execution_stream(
        testnet, api_key, api_secret, lifetime,
    ));
}

#[cfg(test)]
pub(crate) fn spawn_bybit_execution_stream(
    _testnet: bool,
    _api_key: Zeroizing<String>,
    _api_secret: Zeroizing<String>,
    _lifetime: Weak<()>,
) {
}

#[cfg(not(test))]
async fn run_bybit_execution_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
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
        let subscription = json!({"op":"subscribe","args":["execution.fast"]});
        if socket
            .send(Message::Text(subscription.to_string().into()))
            .await
            .is_err()
            || !await_bybit_ack(&mut socket, "subscribe").await
        {
            tracing::warn!("Bybit fast execution subscription failed");
            tokio::time::sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX);
            continue;
        }
        tracing::info!("Bybit fast execution stream connected");
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
                        Ok(Message::Text(text)) => publish_bybit_message(text.as_ref()),
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                                publish_bybit_message(text);
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
        tracing::warn!("Bybit fast execution stream disconnected; REST fallback remains active");
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
fn publish_bybit_message(text: &str) {
    for (symbol, order_id, event_time) in parse_bybit_execution_events(text) {
        publish_execution_wakeup(Exchange::Bybit, &symbol, Some(order_id), event_time);
    }
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
    value
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<rust_decimal::Decimal>().ok())
        .is_some_and(|value| value > rust_decimal::Decimal::ZERO)
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
    fn bybit_parser_coalesces_one_message_by_symbol() {
        let events = parse_bybit_execution_events(
            r#"{"topic":"execution.fast","creationTime":456,"data":[{"category":"linear","symbol":"MUUSDT","orderId":"42"},{"category":"linear","symbol":"MUUSDT","orderId":"42"},{"category":"spot","symbol":"BTCUSDT","orderId":"43"}]}"#,
        );
        assert_eq!(
            events,
            vec![("MUUSDT".to_owned(), "42".to_owned(), Some(456))]
        );
    }
}
