use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(not(test))]
use std::collections::HashMap;
#[cfg(test)]
use std::collections::VecDeque;

use async_trait::async_trait;
#[cfg(not(test))]
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{Map, Number, Value, json};
use sha2::Sha256;
use thiserror::Error;
#[cfg(not(test))]
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent, TimeInForce},
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, CancellationAcknowledgement,
        CancellationError, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalOrder, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderCancellationGateway,
        OrderCancellationTarget, OrderExecutionSnapshot, OrderHistorySnapshotGateway, OrderLookup,
        OrderLookupGateway, OrderPlacementGateway, PlacementAcknowledgement, PlacementError,
        PositionSnapshot, PositionSnapshotGateway, SnapshotError, TradingFeeRateGateway,
        TradingFeeRates,
        codec::{
            CodecError, build_order_parameters, execution_status_is_unknown,
            order_is_definitively_absent, parse_account_balance_snapshot,
            parse_authoritative_order, parse_cancellation_acknowledgement, parse_exchange_error,
            parse_instrument_rules, parse_leverage_acknowledgement, parse_mark_price_snapshot,
            parse_market_snapshot, parse_open_order_execution_progress, parse_open_orders,
            parse_order_history, parse_placement_acknowledgement, parse_position_snapshot,
            parse_trading_fee_rates, validate_snapshot_request,
        },
        execution::{
            CommissionConvention, assemble_execution_snapshot, numeric_trade_id,
            parse_historical_minute_open, parse_order_execution_header, parse_trade_page,
        },
        protocol::{
            BINANCE_LOCAL_COOLDOWN_CODE, BinanceWebsocketOrderBudget, HttpMethod, HttpResponse,
            HttpTransport, MillisecondClock, Parameters, PreparedHttpRequest, encode_parameters,
        },
        realtime::FuturesExecutionCache,
    },
};

const PRODUCTION_BASE_URL: &str = "https://fapi.binance.com";
const TESTNET_BASE_URL: &str = "https://testnet.binancefuture.com";
const TRADE_PAGE_LIMIT: usize = 1_000;
const MAX_TRADE_PAGES: usize = 64;
const MAX_BATCH_CANCELLATIONS: usize = 10;
const REALTIME_EXECUTION_SNAPSHOT_WAIT: Duration = Duration::from_millis(250);
const POST_ONLY_WOULD_TAKE_CODE: &str = "-5022";
const BINANCE_WEBSOCKET_ORDER_TIMEOUT: Duration = Duration::from_secs(10);
const BINANCE_WEBSOCKET_ORDER_QUEUE_CAPACITY: usize = 256;
const BINANCE_WEBSOCKET_DISABLE_NAGLE: bool = true;
#[cfg(not(test))]
const BINANCE_WEBSOCKET_RECONNECT_MIN: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const BINANCE_WEBSOCKET_RECONNECT_MAX: Duration = Duration::from_secs(15);
#[cfg(not(test))]
const BINANCE_WEBSOCKET_LIFETIME_CHECK: Duration = Duration::from_secs(5);

pub trait BinanceRequestSigner: Send + Sync {
    fn sign(&self, message: &str) -> Result<String, SignatureError>;
}

#[derive(Debug)]
pub(crate) enum BinanceWebsocketOrderError {
    NotSent(String),
    RateLimited(String),
    Unknown(String),
}

struct BinanceWebsocketOrderCommand {
    request: Value,
    #[cfg_attr(test, allow(dead_code))]
    queued_at: Instant,
    response: tokio::sync::oneshot::Sender<Result<Value, BinanceWebsocketOrderError>>,
}

#[derive(Clone)]
pub(crate) struct BinanceWebsocketOrderRelay {
    sender: tokio::sync::mpsc::Sender<BinanceWebsocketOrderCommand>,
    connected: Arc<AtomicBool>,
    order_budget: Option<BinanceWebsocketOrderBudget>,
}

impl BinanceWebsocketOrderRelay {
    async fn post(&self, request: Value) -> Result<Value, BinanceWebsocketOrderError> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(BinanceWebsocketOrderError::NotSent(
                "Binance order WebSocket is disconnected".into(),
            ));
        }
        let permit = self.sender.try_reserve().map_err(|_| {
            BinanceWebsocketOrderError::NotSent(
                "Binance order WebSocket writer queue is unavailable".into(),
            )
        })?;
        if let Some(order_budget) = &self.order_budget
            && let Err(retry_after) = order_budget.reserve().await
        {
            return Err(BinanceWebsocketOrderError::RateLimited(format!(
                "Binance order budget is reserved; retry after {} ms",
                retry_after.as_millis()
            )));
        }
        if !self.connected.load(Ordering::Acquire) {
            return Err(BinanceWebsocketOrderError::NotSent(
                "Binance order WebSocket disconnected before submission".into(),
            ));
        }

        let (response, receiver) = tokio::sync::oneshot::channel();
        permit.send(BinanceWebsocketOrderCommand {
            request,
            queued_at: Instant::now(),
            response,
        });
        match tokio::time::timeout(BINANCE_WEBSOCKET_ORDER_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BinanceWebsocketOrderError::Unknown(
                "Binance order WebSocket response channel closed after submission".into(),
            )),
            Err(_) => Err(BinanceWebsocketOrderError::Unknown(
                "Binance order WebSocket acknowledgement timed out after submission".into(),
            )),
        }
    }

    #[cfg(test)]
    fn scripted_for_test(
        connected: bool,
        responses: impl IntoIterator<Item = Result<Value, BinanceWebsocketOrderError>>,
    ) -> (Self, Arc<std::sync::Mutex<Vec<Value>>>) {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<BinanceWebsocketOrderCommand>(
            BINANCE_WEBSOCKET_ORDER_QUEUE_CAPACITY,
        );
        let connected_flag = Arc::new(AtomicBool::new(connected));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let mut responses = responses.into_iter().collect::<VecDeque<_>>();
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                captured.lock().unwrap().push(command.request);
                let result = responses
                    .pop_front()
                    .expect("a scripted Binance WebSocket response is configured");
                let _ = command.response.send(result);
            }
        });
        (
            Self {
                sender,
                connected: connected_flag,
                order_budget: None,
            },
            requests,
        )
    }
}

#[cfg(not(test))]
fn binance_websocket_order_relay(
    order_budget: BinanceWebsocketOrderBudget,
) -> (
    BinanceWebsocketOrderRelay,
    tokio::sync::mpsc::Receiver<BinanceWebsocketOrderCommand>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(BINANCE_WEBSOCKET_ORDER_QUEUE_CAPACITY);
    (
        BinanceWebsocketOrderRelay {
            sender,
            connected: Arc::new(AtomicBool::new(false)),
            order_budget: Some(order_budget),
        },
        receiver,
    )
}

#[cfg(not(test))]
pub(crate) fn spawn_binance_order_stream(
    testnet: bool,
    lifetime: Weak<()>,
    order_budget: BinanceWebsocketOrderBudget,
) -> Option<BinanceWebsocketOrderRelay> {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!("Binance order WebSocket was not started outside a Tokio runtime");
        return None;
    };
    let (relay, receiver) = binance_websocket_order_relay(order_budget);
    runtime.spawn(run_binance_order_stream(
        binance_order_stream_url(testnet),
        lifetime,
        relay.connected.clone(),
        relay
            .order_budget
            .clone()
            .expect("order budget is configured"),
        receiver,
    ));
    Some(relay)
}

#[cfg(test)]
pub(crate) fn spawn_binance_order_stream(
    _testnet: bool,
    _lifetime: Weak<()>,
    _order_budget: BinanceWebsocketOrderBudget,
) -> Option<BinanceWebsocketOrderRelay> {
    None
}

fn binance_order_stream_url(testnet: bool) -> &'static str {
    if testnet {
        "wss://testnet.binancefuture.com/ws-fapi/v1"
    } else {
        "wss://ws-fapi.binance.com/ws-fapi/v1"
    }
}

#[cfg(not(test))]
async fn run_binance_order_stream(
    websocket_url: &'static str,
    lifetime: Weak<()>,
    connected: Arc<AtomicBool>,
    order_budget: BinanceWebsocketOrderBudget,
    mut commands: tokio::sync::mpsc::Receiver<BinanceWebsocketOrderCommand>,
) {
    let mut reconnect_delay = BINANCE_WEBSOCKET_RECONNECT_MIN;
    while lifetime.upgrade().is_some() {
        let connection = connect_async_tls_with_config(
            websocket_url,
            None,
            BINANCE_WEBSOCKET_DISABLE_NAGLE,
            None,
        )
        .await;
        let (mut socket, _) = match connection {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %error, "Binance order WebSocket connection failed");
                connected.store(false, Ordering::Release);
                reject_binance_orders_while_disconnected(&mut commands, reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(BINANCE_WEBSOCKET_RECONNECT_MAX);
                continue;
            }
        };

        connected.store(true, Ordering::Release);
        reconnect_delay = BINANCE_WEBSOCKET_RECONNECT_MIN;
        tracing::info!("Binance order WebSocket connected");
        let mut pending = HashMap::new();
        let mut lifetime_check = tokio::time::interval(BINANCE_WEBSOCKET_LIFETIME_CHECK);
        lifetime_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        lifetime_check.tick().await;

        loop {
            tokio::select! {
                biased;
                command = commands.recv() => {
                    let Some(command) = command else { return };
                    if command.response.is_closed() {
                        continue;
                    }
                    let Some(id) = command.request.get("id").and_then(Value::as_str).map(str::to_owned) else {
                        let _ = command.response.send(Err(BinanceWebsocketOrderError::NotSent(
                            "Binance order WebSocket request id is invalid".into(),
                        )));
                        continue;
                    };
                    let queue_ms = u64::try_from(command.queued_at.elapsed().as_millis())
                        .unwrap_or(u64::MAX);
                    let write_started = Instant::now();
                    if let Err(error) = socket
                        .send(Message::Text(command.request.to_string().into()))
                        .await
                    {
                        let _ = command.response.send(Err(BinanceWebsocketOrderError::Unknown(
                            format!("Binance order WebSocket write failed after submission began: {error}"),
                        )));
                        break;
                    }
                    tracing::info!(
                        websocket_queue_ms = queue_ms,
                        websocket_write_ms = u64::try_from(write_started.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        "Binance order written to WebSocket"
                    );
                    pending.insert(id, command.response);
                }
                message = socket.next() => {
                    let Some(message) = message else { break };
                    match message {
                        Ok(Message::Text(text)) => {
                            resolve_binance_order_response(
                                text.as_ref(),
                                &mut pending,
                                &order_budget,
                            ).await;
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                                resolve_binance_order_response(text, &mut pending, &order_budget).await;
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Err(error) => {
                            tracing::warn!(error = %error, "Binance order WebSocket read failed");
                            break;
                        }
                        Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                    }
                }
                _ = lifetime_check.tick() => {
                    if lifetime.upgrade().is_none() {
                        return;
                    }
                }
            }
        }

        connected.store(false, Ordering::Release);
        for response in pending.into_values() {
            let _ = response.send(Err(BinanceWebsocketOrderError::Unknown(
                "Binance order WebSocket disconnected after submission".into(),
            )));
        }
        tracing::warn!("Binance order WebSocket disconnected; REST fallback remains active");
        reject_binance_orders_while_disconnected(&mut commands, Duration::ZERO).await;
    }
}

#[cfg(not(test))]
async fn resolve_binance_order_response(
    text: &str,
    pending: &mut HashMap<
        String,
        tokio::sync::oneshot::Sender<Result<Value, BinanceWebsocketOrderError>>,
    >,
    order_budget: &BinanceWebsocketOrderBudget,
) {
    let Ok(response) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let Some(id) = response.get("id").and_then(Value::as_str) else {
        return;
    };
    let Some(sender) = pending.remove(id) else {
        return;
    };
    order_budget.observe_response(&response).await;
    let _ = sender.send(Ok(response));
}

#[cfg(not(test))]
async fn reject_binance_orders_while_disconnected(
    commands: &mut tokio::sync::mpsc::Receiver<BinanceWebsocketOrderCommand>,
    delay: Duration,
) {
    if delay.is_zero() {
        while let Ok(command) = commands.try_recv() {
            let _ = command
                .response
                .send(Err(BinanceWebsocketOrderError::NotSent(
                    "Binance order WebSocket is reconnecting".into(),
                )));
        }
        return;
    }
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return,
            command = commands.recv() => {
                let Some(command) = command else { return };
                let _ = command.response.send(Err(BinanceWebsocketOrderError::NotSent(
                    "Binance order WebSocket is reconnecting".into(),
                )));
            }
        }
    }
}

#[async_trait]
impl<T, S, C> LeverageGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        if exchange != Exchange::Binance {
            return Err(invalid_leverage("request belongs to another exchange"));
        }
        if symbol.trim().is_empty()
            || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || !(1..=125).contains(&leverage)
        {
            return Err(invalid_leverage("symbol or leverage is invalid"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Post,
                "/fapi/v1/leverage",
                vec![
                    ("symbol".into(), symbol.clone()),
                    ("leverage".into(), leverage.to_string()),
                ],
            )
            .map_err(|error| invalid_leverage(error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| LeverageError::Unknown {
                    message: error.to_string(),
                })?;
        if (200..300).contains(&response.status) {
            return parse_leverage_acknowledgement(
                &response.body,
                Exchange::Binance,
                &symbol,
                leverage,
            )
            .map_err(|error| LeverageError::Unknown {
                message: format!("Binance leverage acknowledgement is invalid: {error}"),
            });
        }
        let error = parse_exchange_error(&response.body);
        if response.status < 400
            || response.status == 408
            || response.status == 429
            || response.status >= 500
            || execution_status_is_unknown(error.code.as_deref())
        {
            Err(LeverageError::Unknown {
                message: error.message,
            })
        } else {
            Err(LeverageError::Definitive {
                code: error.code,
                message: error.message,
            })
        }
    }
}

#[async_trait]
impl<T, S, C> TradingFeeRateGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v1/commissionRate",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance trading fee rates")
            .await?;
        parse_trading_fee_rates(&body, Exchange::Binance, &symbol)
            .map_err(|error| SnapshotError::new(format!("invalid Binance fee rates: {error}")))
    }
}

#[async_trait]
impl<T, S, C> AccountBalanceSnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        if exchange != Exchange::Binance {
            return Err(SnapshotError::new(
                "account balance belongs to another exchange",
            ));
        }
        let request = self
            .signed_request(HttpMethod::Get, "/fapi/v3/account", vec![])
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance account balance snapshot")
            .await?;
        parse_account_balance_snapshot(&body, Exchange::Binance).map_err(|error| {
            SnapshotError::new(format!("invalid Binance account balance snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> OrderCancellationGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        if exchange != Exchange::Binance {
            return Err(invalid_cancellation("order belongs to another exchange"));
        }
        if symbol.trim().is_empty() || exchange_order_id.trim().is_empty() {
            return Err(invalid_cancellation(
                "symbol and exchange order ID are required",
            ));
        }
        let params = vec![
            ("symbol".into(), symbol.to_ascii_uppercase()),
            ("orderId".into(), exchange_order_id.into()),
        ];
        let request = self
            .signed_request(HttpMethod::Delete, "/fapi/v1/order", params)
            .map_err(|error| invalid_cancellation(&error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| CancellationError::Unknown {
                    message: error.to_string(),
                })?;
        if (200..300).contains(&response.status) {
            return parse_cancellation_acknowledgement(
                &response.body,
                client_order_id,
                exchange_order_id,
            )
            .map_err(|error| CancellationError::Unknown {
                message: format!(
                    "Binance cancellation acknowledgement is not authoritative: {error}"
                ),
            });
        }
        let error = parse_exchange_error(&response.body);
        Err(CancellationError::Unknown {
            message: error.message,
        })
    }

    async fn cancel_orders(
        &self,
        exchange: Exchange,
        symbol: &str,
        targets: &[OrderCancellationTarget],
    ) -> Vec<Result<CancellationAcknowledgement, CancellationError>> {
        if targets.is_empty() {
            return Vec::new();
        }
        if exchange != Exchange::Binance {
            return vec![
                Err(invalid_cancellation("orders belong to another exchange"));
                targets.len()
            ];
        }
        if symbol.trim().is_empty()
            || targets.iter().any(|target| {
                target.exchange_order_id.is_empty()
                    || !target
                        .exchange_order_id
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
            })
        {
            return vec![
                Err(invalid_cancellation(
                    "symbol and numeric exchange order IDs are required",
                ));
                targets.len()
            ];
        }

        let symbol = symbol.to_ascii_uppercase();
        let mut results = Vec::with_capacity(targets.len());
        for chunk in targets.chunks(MAX_BATCH_CANCELLATIONS) {
            let order_ids = format!(
                "[{}]",
                chunk
                    .iter()
                    .map(|target| target.exchange_order_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let request = match self.signed_request(
                HttpMethod::Delete,
                "/fapi/v1/batchOrders",
                vec![
                    ("symbol".into(), symbol.clone()),
                    ("orderIdList".into(), order_ids),
                ],
            ) {
                Ok(request) => request,
                Err(error) => {
                    results.extend(
                        (0..chunk.len()).map(|_| Err(invalid_cancellation(&error.to_string()))),
                    );
                    continue;
                }
            };
            let response = match self.transport.execute(request).await {
                Ok(response) => response,
                Err(error) => {
                    results.extend((0..chunk.len()).map(|_| {
                        Err(CancellationError::Unknown {
                            message: error.to_string(),
                        })
                    }));
                    continue;
                }
            };
            if !(200..300).contains(&response.status) {
                let error = parse_exchange_error(&response.body);
                results.extend((0..chunk.len()).map(|_| {
                    Err(CancellationError::Unknown {
                        message: error.message.clone(),
                    })
                }));
                continue;
            }
            let rows = match serde_json::from_str::<serde_json::Value>(&response.body)
                .ok()
                .and_then(|value| value.as_array().cloned())
            {
                Some(rows) if rows.len() == chunk.len() => rows,
                _ => {
                    results.extend((0..chunk.len()).map(|_| {
                        Err(CancellationError::Unknown {
                            message: "Binance batch cancellation returned an invalid result set"
                                .into(),
                        })
                    }));
                    continue;
                }
            };
            results.extend(rows.into_iter().zip(chunk).map(|(row, target)| {
                if row.get("code").is_some() {
                    let error = parse_exchange_error(&row.to_string());
                    return Err(CancellationError::Unknown {
                        message: error.message,
                    });
                }
                parse_cancellation_acknowledgement(
                    &row.to_string(),
                    &target.client_order_id,
                    &target.exchange_order_id,
                )
                .map_err(|error| CancellationError::Unknown {
                    message: format!(
                        "Binance batch cancellation acknowledgement is not authoritative: {error}"
                    ),
                })
            }));
        }
        results
    }
}

#[derive(Clone)]
pub struct HmacSha256Signer {
    secret: Zeroizing<Vec<u8>>,
}

impl HmacSha256Signer {
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, SignatureError> {
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(SignatureError::MissingSecret);
        }
        Ok(Self {
            secret: Zeroizing::new(secret.to_vec()),
        })
    }
}

impl std::fmt::Debug for HmacSha256Signer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HmacSha256Signer")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl BinanceRequestSigner for HmacSha256Signer {
    fn sign(&self, message: &str) -> Result<String, SignatureError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_slice())
            .map_err(|_| SignatureError::InvalidSecret)?;
        mac.update(message.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SignatureError {
    #[error("Binance API key is required")]
    MissingApiKey,
    #[error("Binance API secret is required")]
    MissingSecret,
    #[error("Binance API secret cannot initialize HMAC-SHA256")]
    InvalidSecret,
    #[error("request signing failed: {0}")]
    Other(String),
    #[error("Binance receive window must be positive")]
    InvalidRecvWindow,
}

pub struct BinanceAdapter<T, S, C> {
    transport: T,
    signer: S,
    clock: C,
    api_key: Zeroizing<String>,
    base_url: String,
    recv_window_ms: u64,
    realtime_lifetime: Arc<()>,
    realtime_execution_cache: FuturesExecutionCache,
    websocket_order_relay: Option<BinanceWebsocketOrderRelay>,
}

impl<T, S, C> BinanceAdapter<T, S, C> {
    pub fn production(transport: T, signer: S, clock: C, api_key: impl Into<String>) -> Self {
        Self::with_base_url(transport, signer, clock, api_key, PRODUCTION_BASE_URL)
    }

    pub fn testnet(transport: T, signer: S, clock: C, api_key: impl Into<String>) -> Self {
        Self::with_base_url(transport, signer, clock, api_key, TESTNET_BASE_URL)
    }

    pub fn with_base_url(
        transport: T,
        signer: S,
        clock: C,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            transport,
            signer,
            clock,
            api_key: Zeroizing::new(api_key.into()),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            recv_window_ms: 5_000,
            realtime_lifetime: crate::exchange::realtime::new_realtime_lifetime(),
            realtime_execution_cache: FuturesExecutionCache::default(),
            websocket_order_relay: None,
        }
    }

    pub(crate) fn realtime_lifetime(&self) -> Weak<()> {
        Arc::downgrade(&self.realtime_lifetime)
    }

    pub(crate) fn realtime_execution_cache(&self) -> FuturesExecutionCache {
        self.realtime_execution_cache.clone()
    }

    pub fn set_recv_window_ms(&mut self, recv_window_ms: u64) {
        self.recv_window_ms = recv_window_ms;
    }

    pub(crate) fn set_websocket_order_relay(&mut self, relay: BinanceWebsocketOrderRelay) {
        self.websocket_order_relay = Some(relay);
    }

    fn public_request(&self, path: &str, parameters: Parameters) -> PreparedHttpRequest {
        PreparedHttpRequest {
            method: HttpMethod::Get,
            base_url: self.base_url.clone(),
            path: path.into(),
            query: parameters,
            body: vec![],
            raw_body: None,
            headers: vec![],
        }
    }
}

impl<T, S, C> BinanceAdapter<T, S, C>
where
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    #[cfg(test)]
    fn websocket_order_request(&self, intent: &OrderIntent) -> Result<Value, SignatureError> {
        let parameters = build_order_parameters(&intent.client_order_id, &intent.shape)
            .map_err(|error| SignatureError::Other(error.to_string()))?;
        self.websocket_order_request_from_parameters(&intent.client_order_id, parameters)
    }

    fn websocket_order_request_from_parameters(
        &self,
        client_order_id: &ClientOrderId,
        mut parameters: Parameters,
    ) -> Result<Value, SignatureError> {
        if self.api_key.trim().is_empty() {
            return Err(SignatureError::MissingApiKey);
        }
        if self.recv_window_ms == 0 {
            return Err(SignatureError::InvalidRecvWindow);
        }
        parameters.push(("apiKey".into(), self.api_key.to_string()));
        parameters.push(("recvWindow".into(), self.recv_window_ms.to_string()));
        parameters.push(("timestamp".into(), self.clock.now_millis().to_string()));
        parameters.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let signature_payload = parameters
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let signature = self.signer.sign(&signature_payload)?;
        let mut params = Map::new();
        for (key, value) in parameters {
            let value = match key.as_str() {
                "recvWindow" | "timestamp" => Value::Number(Number::from(
                    value
                        .parse::<u64>()
                        .map_err(|error| SignatureError::Other(error.to_string()))?,
                )),
                _ => Value::String(value),
            };
            params.insert(key, value);
        }
        params.insert("signature".into(), Value::String(signature));
        Ok(json!({
            "id": client_order_id.as_str(),
            "method": "order.place",
            "params": params,
        }))
    }

    fn signed_request(
        &self,
        method: HttpMethod,
        path: &str,
        mut parameters: Parameters,
    ) -> Result<PreparedHttpRequest, SignatureError> {
        if self.api_key.trim().is_empty() {
            return Err(SignatureError::MissingApiKey);
        }
        if self.recv_window_ms == 0 {
            return Err(SignatureError::InvalidRecvWindow);
        }
        parameters.push(("timestamp".into(), self.clock.now_millis().to_string()));
        parameters.push(("recvWindow".into(), self.recv_window_ms.to_string()));
        let signature = self.signer.sign(&encode_parameters(&parameters))?;
        parameters.push(("signature".into(), signature));
        Ok(PreparedHttpRequest {
            method,
            base_url: self.base_url.clone(),
            path: path.into(),
            query: parameters,
            body: vec![],
            raw_body: None,
            headers: vec![("X-MBX-APIKEY".into(), self.api_key.to_string())],
        })
    }
}

impl<T, S, C> BinanceAdapter<T, S, C>
where
    T: HttpTransport,
{
    async fn execute_snapshot(
        &self,
        request: PreparedHttpRequest,
        context: &str,
    ) -> Result<String, SnapshotError> {
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(|error| SnapshotError::new(format!("{context}: {error}")))?;
        if !(200..300).contains(&response.status) {
            let error = parse_exchange_error(&response.body);
            return Err(SnapshotError::new(format!(
                "{context}: HTTP {}: {}",
                response.status, error.message
            )));
        }
        Ok(response.body)
    }
}

#[async_trait]
impl<T, S, C> MarketSnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: Send + Sync,
    C: Send + Sync,
{
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let ticker = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v1/ticker/24hr",
                    vec![("symbol".into(), symbol.clone())],
                ),
                "Binance ticker snapshot",
            )
            .await?;
        let premium = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v1/premiumIndex",
                    vec![("symbol".into(), symbol.clone())],
                ),
                "Binance mark-price snapshot",
            )
            .await?;
        parse_market_snapshot(&ticker, &premium, Exchange::Binance, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Binance market snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> HistoricalPriceGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: Send + Sync,
    C: Send + Sync,
{
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        if minute_start_ms == 0 || !minute_start_ms.is_multiple_of(60_000) {
            return Err(SnapshotError::new(
                "historical price minute must be a positive UTC minute boundary",
            ));
        }
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v1/klines",
                    vec![
                        ("symbol".into(), symbol.clone()),
                        ("interval".into(), "1m".into()),
                        ("startTime".into(), minute_start_ms.to_string()),
                        ("limit".into(), "1".into()),
                    ],
                ),
                "Binance historical fee-price snapshot",
            )
            .await?;
        parse_historical_minute_open(&body, Exchange::Binance, &symbol, minute_start_ms)
            .map_err(|error| SnapshotError::new(format!("invalid Binance minute price: {error}")))
    }
}

#[async_trait]
impl<T, S, C> InstrumentRulesGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: Send + Sync,
    C: Send + Sync,
{
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<crate::domain::InstrumentRules, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request("/fapi/v1/exchangeInfo", vec![]),
                "Binance instrument snapshot",
            )
            .await?;
        parse_instrument_rules(&body, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Binance instrument snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> PositionSnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v2/positionRisk",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance position snapshot")
            .await?;
        match parse_position_snapshot(&body, Exchange::Binance, &symbol, None) {
            Ok(snapshot) => Ok(snapshot),
            Err(CodecError::InvalidField("markPrice")) => {
                let premium = self
                    .execute_snapshot(
                        self.public_request(
                            "/fapi/v1/premiumIndex",
                            vec![("symbol".into(), symbol.clone())],
                        ),
                        "Binance flat-position mark-price snapshot",
                    )
                    .await?;
                let fallback_mark_price =
                    parse_mark_price_snapshot(&premium, &symbol).map_err(|error| {
                        SnapshotError::new(format!(
                            "invalid Binance flat-position mark-price snapshot: {error}"
                        ))
                    })?;
                parse_position_snapshot(
                    &body,
                    Exchange::Binance,
                    &symbol,
                    Some(fallback_mark_price),
                )
                .map_err(|error| {
                    SnapshotError::new(format!("invalid Binance position snapshot: {error}"))
                })
            }
            Err(error) => Err(SnapshotError::new(format!(
                "invalid Binance position snapshot: {error}"
            ))),
        }
    }
}

#[async_trait]
impl<T, S, C> OrderPlacementGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        if intent.exchange != Exchange::Binance {
            return Err(definitive_local_error(
                "order intent belongs to another exchange",
            ));
        }
        intent
            .validate()
            .map_err(|error| definitive_local_error(&error.to_string()))?;
        let params = build_order_parameters(&intent.client_order_id, &intent.shape)
            .map_err(|error| definitive_local_error(&error.to_string()))?;
        if let Some(relay) = &self.websocket_order_relay {
            let request = self
                .websocket_order_request_from_parameters(&intent.client_order_id, params.clone())
                .map_err(|error| definitive_local_error(&error.to_string()))?;
            let websocket_started = Instant::now();
            match relay.post(request).await {
                Ok(response) => {
                    tracing::info!(
                        websocket_roundtrip_ms =
                            u64::try_from(websocket_started.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                        "Binance order action completed over WebSocket"
                    );
                    let response = websocket_order_http_response(response).map_err(|message| {
                        PlacementError::Unknown {
                            message: format!(
                                "Binance WebSocket acknowledgement is invalid: {message}"
                            ),
                        }
                    })?;
                    return interpret_placement_response(response, intent);
                }
                Err(BinanceWebsocketOrderError::NotSent(message)) => {
                    tracing::debug!(reason = %message, "Binance order is falling back to REST");
                }
                Err(BinanceWebsocketOrderError::RateLimited(message)) => {
                    return Err(PlacementError::NotSubmitted { message });
                }
                Err(BinanceWebsocketOrderError::Unknown(message)) => {
                    return Err(PlacementError::Unknown { message });
                }
            }
        }
        let request = self
            .signed_request(HttpMethod::Post, "/fapi/v1/order", params)
            .map_err(|error| definitive_local_error(&error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| PlacementError::Unknown {
                    message: error.to_string(),
                })?;
        interpret_placement_response(response, intent)
    }
}

fn websocket_order_http_response(response: Value) -> Result<HttpResponse, String> {
    let status = response
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .ok_or_else(|| "status is missing".to_owned())?;
    let body = if (200..300).contains(&status) {
        response.get("result")
    } else {
        response.get("error")
    }
    .ok_or_else(|| "result or error payload is missing".to_owned())?;
    serde_json::to_string(body).map_or_else(
        |error| Err(format!("payload serialization failed: {error}")),
        |body| Ok(HttpResponse { status, body }),
    )
}

fn interpret_placement_response(
    response: HttpResponse,
    intent: &OrderIntent,
) -> Result<PlacementAcknowledgement, PlacementError> {
    if (200..300).contains(&response.status) {
        return parse_placement_acknowledgement(&response.body, &intent.client_order_id).map_err(
            |error| PlacementError::Unknown {
                message: format!("Binance acknowledgement is not authoritative: {error}"),
            },
        );
    }

    let error = parse_exchange_error(&response.body);
    if response.status == 429 && error.code.as_deref() == Some(BINANCE_LOCAL_COOLDOWN_CODE) {
        return Err(PlacementError::NotSubmitted {
            message: error.message,
        });
    }
    if error.code.as_deref() == Some("-1008") {
        // Binance documents -1008 system-level throttling as a 100% failed
        // operation, unlike a timeout whose execution status is unknown.
        return Err(PlacementError::NotSubmitted {
            message: error.message,
        });
    }
    if intent.shape.time_in_force == TimeInForce::PostOnly
        && error.code.as_deref() == Some(POST_ONLY_WOULD_TAKE_CODE)
    {
        return Err(PlacementError::NotSubmitted {
            message: error.message,
        });
    }
    if response.status < 400
        || response.status == 408
        || response.status == 429
        || response.status >= 500
        || execution_status_is_unknown(error.code.as_deref())
    {
        Err(PlacementError::Unknown {
            message: error.message,
        })
    } else {
        Err(PlacementError::Definitive {
            code: error.code,
            message: error.message,
        })
    }
}

#[async_trait]
impl<T, S, C> OrderLookupGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        if exchange != Exchange::Binance {
            return Err(lookup_error("lookup belongs to another exchange"));
        }
        let params = vec![
            ("symbol".into(), symbol.to_ascii_uppercase()),
            ("origClientOrderId".into(), client_order_id.as_str().into()),
        ];
        let request = self
            .signed_request(HttpMethod::Get, "/fapi/v1/order", params)
            .map_err(|error| lookup_error(&error.to_string()))?;
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(|error| lookup_error(&error.to_string()))?;
        if (200..300).contains(&response.status) {
            return parse_authoritative_order(
                &response.body,
                Exchange::Binance,
                symbol,
                client_order_id,
            )
            .map(OrderLookup::Found)
            .map_err(|error| lookup_error(&format!("invalid Binance order snapshot: {error}")));
        }

        let error = parse_exchange_error(&response.body);
        if order_is_definitively_absent(error.code.as_deref()) {
            Ok(OrderLookup::NotFound)
        } else {
            Err(lookup_error(&error.message))
        }
    }
}

#[async_trait]
impl<T, S, C> OpenOrderSnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<crate::exchange::AuthoritativeOrder>, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v1/openOrders",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance open-order snapshot")
            .await?;
        parse_open_orders(&body, Exchange::Binance, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Binance open-order snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> OrderHistorySnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)?;
        if !(1..=1_000).contains(&limit) {
            return Err(SnapshotError::new("order-history limit must be 1..=1000"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v1/allOrders",
                vec![
                    ("symbol".into(), symbol.clone()),
                    ("limit".into(), limit.to_string()),
                ],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance order-history snapshot")
            .await?;
        parse_order_history(&body, Exchange::Binance, &symbol, limit).map_err(|error| {
            SnapshotError::new(format!("invalid Binance order-history snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> ExecutionSnapshotGateway for BinanceAdapter<T, S, C>
where
    T: HttpTransport,
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
    async fn open_order_execution_progress_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Option<Vec<crate::exchange::OpenOrderExecutionProgress>>, ExecutionSnapshotError>
    {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)
            .map_err(|error| execution_error(error.to_string()))?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v1/openOrders",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| execution_error(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Binance open-order execution progress")
            .await
            .map_err(|error| execution_error(error.to_string()))?;
        parse_open_order_execution_progress(&body, Exchange::Binance, &symbol)
            .map(Some)
            .map_err(|error| {
                execution_error(format!("invalid Binance open-order progress: {error}"))
            })
    }

    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        validate_snapshot_request(exchange, Exchange::Binance, symbol)
            .map_err(|error| execution_error(error.to_string()))?;
        if exchange_order_id.trim().is_empty() {
            return Err(execution_error("exchange order ID is required"));
        }
        let symbol = symbol.to_ascii_uppercase();
        if self
            .realtime_execution_cache
            .knows_order(&symbol, exchange_order_id)
            && let Some(snapshot) = self
                .realtime_execution_cache
                .wait_snapshot(
                    &symbol,
                    client_order_id,
                    exchange_order_id,
                    REALTIME_EXECUTION_SNAPSHOT_WAIT,
                )
                .await
        {
            tracing::info!(
                symbol = symbol.as_str(),
                exchange_order_id,
                "using Binance realtime execution snapshot"
            );
            return Ok(snapshot);
        }
        let detail_request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v1/order",
                vec![
                    ("symbol".into(), symbol.clone()),
                    ("origClientOrderId".into(), client_order_id.as_str().into()),
                ],
            )
            .map_err(|error| execution_error(error.to_string()))?;
        let detail_body = self
            .execute_snapshot(detail_request, "Binance execution order snapshot")
            .await
            .map_err(|error| execution_error(error.to_string()))?;
        let header = parse_order_execution_header(
            &detail_body,
            Exchange::Binance,
            &symbol,
            client_order_id,
            exchange_order_id,
        )
        .map_err(|error| execution_error(format!("invalid Binance order totals: {error}")))?;
        if header.cumulative_quantity.is_zero() {
            let snapshot = assemble_execution_snapshot(header, vec![])
                .map_err(|error| execution_error(error.to_string()))?;
            self.realtime_execution_cache
                .bind_authoritative_snapshot(&snapshot);
            return Ok(snapshot);
        }

        let mut trades = Vec::new();
        let mut next_from_id: Option<u64> = None;
        let mut completed = false;
        for _ in 0..MAX_TRADE_PAGES {
            let mut parameters = vec![
                ("symbol".into(), symbol.clone()),
                ("orderId".into(), exchange_order_id.into()),
                ("limit".into(), TRADE_PAGE_LIMIT.to_string()),
            ];
            if let Some(from_id) = next_from_id {
                parameters.push(("fromId".into(), from_id.to_string()));
            }
            let request = self
                .signed_request(HttpMethod::Get, "/fapi/v1/userTrades", parameters)
                .map_err(|error| execution_error(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Binance account trade snapshot")
                .await
                .map_err(|error| execution_error(error.to_string()))?;
            let page = parse_trade_page(&body, &symbol, CommissionConvention::PositiveCost)
                .map_err(|error| execution_error(format!("invalid Binance trade page: {error}")))?;
            if page.len() > TRADE_PAGE_LIMIT {
                return Err(execution_error(
                    "Binance trade page exceeds the requested limit",
                ));
            }
            if page
                .iter()
                .any(|trade| trade.exchange_order_id != exchange_order_id)
            {
                return Err(execution_error(
                    "Binance order-filtered trade page contains another order",
                ));
            }
            let page_trade_ids = page
                .iter()
                .map(numeric_trade_id)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    execution_error(format!("invalid Binance numeric trade ID: {error}"))
                })?;
            if let Some(from_id) = next_from_id
                && page_trade_ids.iter().any(|trade_id| *trade_id < from_id)
            {
                return Err(execution_error("Binance trade pagination moved backwards"));
            }
            if page.len() < TRADE_PAGE_LIMIT {
                trades.extend(page);
                completed = true;
                break;
            }
            let next = page_trade_ids
                .into_iter()
                .max()
                .and_then(|trade_id| trade_id.checked_add(1))
                .ok_or_else(|| execution_error("Binance trade pagination cannot advance"))?;
            if next_from_id.is_some_and(|current| next <= current) {
                return Err(execution_error("Binance trade pagination did not advance"));
            }
            trades.extend(page);
            next_from_id = Some(next);
        }
        if !completed {
            return Err(execution_error(
                "Binance trade history exceeded the bounded pagination limit",
            ));
        }
        let snapshot = assemble_execution_snapshot(header, trades)
            .map_err(|error| execution_error(format!("incomplete Binance execution: {error}")))?;
        self.realtime_execution_cache
            .bind_authoritative_snapshot(&snapshot);
        Ok(snapshot)
    }
}

fn definitive_local_error(message: &str) -> PlacementError {
    PlacementError::Definitive {
        code: None,
        message: message.into(),
    }
}

fn invalid_leverage(message: impl Into<String>) -> LeverageError {
    LeverageError::Invalid {
        message: message.into(),
    }
}

fn lookup_error(message: &str) -> LookupError {
    LookupError {
        message: message.into(),
    }
}

fn invalid_cancellation(message: &str) -> CancellationError {
    CancellationError::Invalid {
        message: message.into(),
    }
}

fn execution_error(message: impl Into<String>) -> ExecutionSnapshotError {
    ExecutionSnapshotError::new(message)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{IntentState, OrderKind, OrderShape, OrderSide, TerminalOrderStatus, TimeInForce},
        exchange::{
            ActiveOrderStatus, OrderLifecycle,
            protocol::{HttpResponse, TransportError},
        },
    };

    #[derive(Clone)]
    struct FixedClock(u64);

    impl MillisecondClock for FixedClock {
        fn now_millis(&self) -> u64 {
            self.0
        }
    }

    #[derive(Clone, Default)]
    struct MockTransport {
        requests: Arc<Mutex<Vec<PreparedHttpRequest>>>,
        responses: Arc<Mutex<VecDeque<Result<HttpResponse, TransportError>>>>,
    }

    impl MockTransport {
        fn with_response(response: Result<HttpResponse, TransportError>) -> Self {
            let transport = Self::default();
            transport.responses.lock().unwrap().push_back(response);
            transport
        }

        fn request(&self) -> PreparedHttpRequest {
            self.requests.lock().unwrap()[0].clone()
        }

        fn all_requests(&self) -> Vec<PreparedHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpTransport for MockTransport {
        async fn execute(
            &self,
            request: PreparedHttpRequest,
        ) -> Result<HttpResponse, TransportError> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("test response is configured")
        }
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            client_order_id: ClientOrderId::parse("g_7_S_fixed").unwrap(),
            exchange: Exchange::Binance,
            shape: OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(1011, 0)),
                quantity: Decimal::new(2, 1),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::PostOnly,
            },
            state: IntentState::Prepared,
            created_at_ms: 10,
            updated_at_ms: 10,
        }
    }

    fn adapter(
        transport: MockTransport,
    ) -> BinanceAdapter<MockTransport, HmacSha256Signer, FixedClock> {
        BinanceAdapter::with_base_url(
            transport,
            HmacSha256Signer::new("test-secret").unwrap(),
            FixedClock(1_700_000_000_123),
            "test-key",
            "https://example.test",
        )
    }

    fn execution_order_detail(original: &str, executed: &str, quote: &str, status: &str) -> String {
        format!(
            r#"{{"symbol":"MUUSDT","orderId":91,"clientOrderId":"g_7_S_fixed","side":"SELL","price":"1","origQty":"{original}","executedQty":"{executed}","cumQuote":"{quote}","status":"{status}","reduceOnly":false,"timeInForce":"GTC","type":"LIMIT","time":1000000,"updateTime":1100000}}"#
        )
    }

    fn binance_trade(trade_id: u64, quantity: &str, quote: &str) -> String {
        format!(
            r#"{{"symbol":"MUUSDT","id":{trade_id},"orderId":91,"side":"SELL","buyer":false,"price":"1","qty":"{quantity}","quoteQty":"{quote}","commission":"0","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1050000}}"#
        )
    }

    #[test]
    fn websocket_order_endpoint_uses_official_low_latency_transport_policy() {
        assert_eq!(
            binance_order_stream_url(false),
            "wss://ws-fapi.binance.com/ws-fapi/v1"
        );
        assert_eq!(
            binance_order_stream_url(true),
            "wss://testnet.binancefuture.com/ws-fapi/v1"
        );
        assert!(std::hint::black_box(BINANCE_WEBSOCKET_DISABLE_NAGLE));
    }

    #[test]
    fn hmac_signing_matches_fixed_sha256_vector() {
        let signer = HmacSha256Signer::new("key").unwrap();
        assert_eq!(
            signer
                .sign("The quick brown fox jumps over the lazy dog")
                .unwrap(),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[tokio::test]
    async fn open_order_snapshot_is_signed_and_preserves_exchange_original_quantity() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[
                {"symbol":"MUUSDT","orderId":91,"clientOrderId":"g_RUN00001_1_B_1","side":"BUY","price":"1010","origQty":"70","executedQty":"0","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"},
                {"symbol":"MUUSDT","orderId":92,"clientOrderId":"g_RUN00001_2_S_2","side":"SELL","price":"1012","origQty":"100","executedQty":"30","status":"PARTIALLY_FILLED","reduceOnly":false,"timeInForce":"GTX","type":"LIMIT"}
            ]"#
                .into(),
        }));

        let orders = adapter(transport.clone())
            .open_orders_snapshot(Exchange::Binance, "MUUSDT")
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(request.path, "/fapi/v1/openOrders");
        assert_eq!(request.method, HttpMethod::Get);
        assert!(
            request
                .query
                .iter()
                .any(|item| item == &("symbol".into(), "MUUSDT".into()))
        );
        assert!(request.query.iter().any(|(key, _)| key == "signature"));
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].shape.quantity, Decimal::new(70, 0));
        assert_eq!(orders[1].shape.time_in_force, TimeInForce::PostOnly);
    }

    #[tokio::test]
    async fn open_order_progress_preserves_authoritative_partial_quantity() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[
                {"symbol":"MUUSDT","orderId":92,"clientOrderId":"g_RUN00001_2_S_2","side":"SELL","price":"1012","origQty":"100","executedQty":"70","cumQuote":"70840","status":"PARTIALLY_FILLED","reduceOnly":false,"timeInForce":"GTC","type":"LIMIT"}
            ]"#
                .into(),
        }));

        let progress = adapter(transport.clone())
            .open_order_execution_progress_snapshot(Exchange::Binance, "MUUSDT")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(transport.request().path, "/fapi/v1/openOrders");
        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0].cumulative_quantity, Decimal::new(70, 0));
        assert_eq!(progress[0].order.shape.quantity, Decimal::new(100, 0));
    }

    #[tokio::test]
    async fn order_history_uses_signed_all_orders_and_preserves_exact_values() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[
                {"symbol":"MUUSDT","orderId":9007199254740993,"side":"SELL","price":"1011.00000","origQty":"0.240","status":"FILLED","time":1780000000001}
            ]"#
                .into(),
        }));

        let orders = adapter(transport.clone())
            .order_history_snapshot(Exchange::Binance, "MUUSDT", 25)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(request.path, "/fapi/v1/allOrders");
        assert_eq!(request.method, HttpMethod::Get);
        assert!(
            request
                .query
                .iter()
                .any(|item| item == &("limit".into(), "25".into()))
        );
        assert!(request.query.iter().any(|(key, _)| key == "signature"));
        assert_eq!(orders[0].exchange_order_id, "9007199254740993");
        assert_eq!(orders[0].price.to_string(), "1011.00000");
        assert_eq!(orders[0].quantity.to_string(), "0.240");
    }

    #[tokio::test]
    async fn market_snapshot_uses_separate_authoritative_ticker_and_mark_endpoints() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"MUUSDT","lastPrice":"1011.25"}"#.into(),
            }),
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"MUUSDT","markPrice":"1011.20","time":1700000000000}"#.into(),
            }),
        ]);
        let snapshot = adapter(transport.clone())
            .market_snapshot(Exchange::Binance, "muusdt")
            .await
            .unwrap();
        let requests = transport.all_requests();

        assert_eq!(snapshot.last_price, Decimal::new(101125, 2));
        assert_eq!(snapshot.mark_price, Decimal::new(101120, 2));
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/fapi/v1/ticker/24hr");
        assert_eq!(requests[1].path, "/fapi/v1/premiumIndex");
        assert!(requests.iter().all(|request| request.headers.is_empty()));
    }

    #[tokio::test]
    async fn missing_mark_price_never_falls_back_to_last_price() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"MUUSDT","lastPrice":"1011.25"}"#.into(),
            }),
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"MUUSDT"}"#.into(),
            }),
        ]);

        assert!(
            adapter(transport)
                .market_snapshot(Exchange::Binance, "MUUSDT")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn historical_fee_price_uses_one_exact_public_minute_candle() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[[1020000,"602.25","603","601","602","100"]]"#.into(),
        }));
        let price = adapter(transport.clone())
            .historical_minute_open(Exchange::Binance, "BNBUSDT", 1_020_000)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(price.open_price, Decimal::new(60225, 2));
        assert_eq!(request.path, "/fapi/v1/klines");
        assert_eq!(
            request.query_string(),
            "symbol=BNBUSDT&interval=1m&startTime=1020000&limit=1"
        );
        assert!(request.headers.is_empty());
    }

    #[tokio::test]
    async fn instrument_rules_are_loaded_from_the_exchange_without_local_defaults() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "symbols":[{"symbol":"MUUSDT","status":"TRADING","filters":[
                    {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                    {"filterType":"LOT_SIZE","stepSize":"0.01","minQty":"0.01","maxQty":"100"},
                    {"filterType":"MARKET_LOT_SIZE","stepSize":"0.1","minQty":"0.1","maxQty":"50"},
                    {"filterType":"MIN_NOTIONAL","notional":"5"}
                ]}]
            }"#
            .into(),
        }));
        let rules = adapter(transport.clone())
            .instrument_rules(Exchange::Binance, "MUUSDT")
            .await
            .unwrap();

        assert_eq!(rules.limit_quantity.step, Decimal::new(1, 2));
        assert_eq!(rules.market_quantity.step, Decimal::new(1, 1));
        assert_eq!(transport.request().path, "/fapi/v1/exchangeInfo");
        assert!(transport.request().query.is_empty());
    }

    #[tokio::test]
    async fn signed_position_snapshot_keeps_existing_short_as_a_separate_baseline() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[{
                "symbol":"MUUSDT","positionSide":"BOTH","positionAmt":"-3",
                "entryPrice":"1011.25","markPrice":"1008.10","unRealizedProfit":"9.45",
                "leverage":"5"
            }]"#
            .into(),
        }));
        let snapshot = adapter(transport.clone())
            .position_snapshot(Exchange::Binance, "MUUSDT")
            .await
            .unwrap();

        assert_eq!(
            snapshot.one_way_position().unwrap(),
            (Decimal::new(-3, 0), Some(Decimal::new(101125, 2)))
        );
        assert_eq!(snapshot.one_way_leverage().unwrap(), 5);
        let request = transport.request();
        assert_eq!(request.path, "/fapi/v2/positionRisk");
        assert!(request.query_string().contains("signature="));
    }

    #[tokio::test]
    async fn signed_position_snapshot_preserves_an_authoritative_flat_position() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[{
                "symbol":"MUUSDT","positionSide":"BOTH","positionAmt":"0.00",
                "entryPrice":"0.00000","markPrice":"956.42792409",
                "unRealizedProfit":"0.00000000","leverage":"10"
            }]"#
            .into(),
        }));
        let snapshot = adapter(transport.clone())
            .position_snapshot(Exchange::Binance, "MUUSDT")
            .await
            .unwrap();

        assert_eq!(snapshot.one_way_position().unwrap(), (Decimal::ZERO, None));
        assert_eq!(snapshot.one_way_leverage().unwrap(), 10);
        assert_eq!(snapshot.legs[0].mark_price, Decimal::new(95642792409, 8));
        let request = transport.request();
        assert_eq!(request.path, "/fapi/v2/positionRisk");
        assert!(request.query_string().contains("symbol=MUUSDT"));
        assert!(request.query_string().contains("signature="));
    }

    #[tokio::test]
    async fn flat_positions_without_valid_mark_prices_use_symbol_specific_public_marks() {
        for (symbol, private_mark_field, public_mark, expected_mark) in [
            ("RKLBUSDT", "", "75.52", Decimal::new(7552, 2)),
            (
                "NEWTRADIFIUSDT",
                r#","markPrice":"0""#,
                "12.3456",
                Decimal::new(123456, 4),
            ),
        ] {
            let transport = MockTransport::default();
            transport.responses.lock().unwrap().extend([
                Ok(HttpResponse {
                    status: 200,
                    body: format!(
                        r#"[{{
                            "symbol":"{symbol}","positionSide":"BOTH","positionAmt":"0",
                            "entryPrice":"0"{private_mark_field},"unRealizedProfit":"0","leverage":"20"
                        }}]"#
                    ),
                }),
                Ok(HttpResponse {
                    status: 200,
                    body: format!(
                        r#"{{"symbol":"{symbol}","markPrice":"{public_mark}","time":1786399426143}}"#
                    ),
                }),
            ]);

            let snapshot = adapter(transport.clone())
                .position_snapshot(Exchange::Binance, &symbol.to_ascii_lowercase())
                .await
                .unwrap();
            let requests = transport.all_requests();

            assert_eq!(snapshot.one_way_position().unwrap(), (Decimal::ZERO, None));
            assert_eq!(snapshot.one_way_leverage().unwrap(), 20);
            assert_eq!(snapshot.legs[0].mark_price, expected_mark);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].path, "/fapi/v2/positionRisk");
            assert_eq!(requests[1].path, "/fapi/v1/premiumIndex");
            assert!(
                requests[1]
                    .query_string()
                    .contains(&format!("symbol={symbol}"))
            );
        }
    }

    #[tokio::test]
    async fn leverage_change_is_signed_and_requires_exact_acknowledgement() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"symbol":"MUUSDT","leverage":5,"maxNotionalValue":"100000"}"#.into(),
        }));
        let acknowledgement = adapter(transport.clone())
            .set_leverage(Exchange::Binance, "muusdt", 5)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(acknowledgement.symbol, "MUUSDT");
        assert_eq!(acknowledgement.leverage, 5);
        assert_eq!(request.path, "/fapi/v1/leverage");
        assert!(request.query_string().starts_with(
            "symbol=MUUSDT&leverage=5&timestamp=1700000000123&recvWindow=5000&signature="
        ));

        let malformed = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"symbol":"MUUSDT","leverage":3}"#.into(),
        }));
        assert!(matches!(
            adapter(malformed)
                .set_leverage(Exchange::Binance, "MUUSDT", 5)
                .await,
            Err(LeverageError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn fee_rate_query_is_signed_and_preserves_account_rates() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"symbol":"MUUSDT","makerCommissionRate":"0.0002","takerCommissionRate":"0.0005"}"#.into(),
        }));
        let rates = adapter(transport.clone())
            .trading_fee_rates(Exchange::Binance, "muusdt")
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(rates.maker_rate, Decimal::new(2, 4));
        assert_eq!(rates.taker_rate, Decimal::new(5, 4));
        assert_eq!(request.path, "/fapi/v1/commissionRate");
        assert!(
            request
                .query_string()
                .starts_with("symbol=MUUSDT&timestamp=1700000000123&recvWindow=5000&signature=")
        );
    }

    #[tokio::test]
    async fn account_balance_query_uses_signed_v3_account_totals() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "totalWalletBalance":"126.724692060",
                "totalUnrealizedProfit":"-0.00400000",
                "totalMarginBalance":"126.720692060",
                "availableBalance":"120.10000000",
                "assets":[],"positions":[]
            }"#
            .into(),
        }));

        let snapshot = adapter(transport.clone())
            .account_balance_snapshot(Exchange::Binance)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(snapshot.wallet_balance.to_string(), "126.724692060");
        assert_eq!(snapshot.equity.to_string(), "126.720692060");
        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(request.path, "/fapi/v3/account");
        assert!(
            request
                .query_string()
                .starts_with("timestamp=1700000000123&recvWindow=5000&signature=")
        );
        assert_eq!(
            request.headers,
            vec![("X-MBX-APIKEY".into(), "test-key".into())]
        );
    }

    #[tokio::test]
    async fn placement_maps_exact_shape_and_identity_to_signed_query() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":91,"clientOrderId":"g_7_S_fixed"}"#.into(),
        }));
        let acknowledgement = adapter(transport.clone())
            .place_order(&intent())
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(acknowledgement.exchange_order_id, "91");
        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(request.path, "/fapi/v1/order");
        assert_eq!(
            request.headers,
            vec![("X-MBX-APIKEY".into(), "test-key".into())]
        );
        assert_eq!(
            request.query_string(),
            concat!(
                "symbol=MUUSDT&side=SELL&type=LIMIT&quantity=0.2&reduceOnly=false&",
                "price=1011&timeInForce=GTX&newClientOrderId=g_7_S_fixed&",
                "timestamp=1700000000123&recvWindow=5000&",
                "signature=426919c675812880a3ebf157138dbb77a5131eff743d1b2674908dea3c6c3b55"
            )
        );
    }

    #[test]
    fn websocket_order_request_matches_binance_sorted_hmac_contract() {
        let request = adapter(MockTransport::default())
            .websocket_order_request(&intent())
            .unwrap();
        let params = request.get("params").unwrap();

        assert_eq!(
            request.get("id").and_then(Value::as_str),
            Some("g_7_S_fixed")
        );
        assert_eq!(
            request.get("method").and_then(Value::as_str),
            Some("order.place")
        );
        assert_eq!(
            params.get("timestamp").and_then(Value::as_u64),
            Some(1_700_000_000_123)
        );
        assert_eq!(
            params.get("recvWindow").and_then(Value::as_u64),
            Some(5_000)
        );
        assert_eq!(params.get("price").and_then(Value::as_str), Some("1011"));
        assert_eq!(params.get("quantity").and_then(Value::as_str), Some("0.2"));
        assert_eq!(
            params.get("signature").and_then(Value::as_str),
            Some("666fba36e5f512b604244724c0c5897c2f4b2a1755326c4718bfda96de5c9572")
        );
    }

    #[tokio::test]
    async fn websocket_order_ack_skips_rest_and_preserves_authoritative_identity() {
        let transport = MockTransport::default();
        let (relay, requests) = BinanceWebsocketOrderRelay::scripted_for_test(
            true,
            [Ok(serde_json::json!({
                "id": "g_7_S_fixed",
                "status": 200,
                "result": {"orderId": 91, "clientOrderId": "g_7_S_fixed"},
                "rateLimits": []
            }))],
        );
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(relay);

        let acknowledgement = gateway.place_order(&intent()).await.unwrap();

        assert_eq!(acknowledgement.exchange_order_id, "91");
        assert!(transport.all_requests().is_empty());
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn websocket_unknown_after_submission_never_falls_back_to_rest() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":92,"clientOrderId":"g_7_S_fixed"}"#.into(),
        }));
        let (relay, _) = BinanceWebsocketOrderRelay::scripted_for_test(
            true,
            [Err(BinanceWebsocketOrderError::Unknown(
                "connection closed after write".into(),
            ))],
        );
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(relay);

        assert!(matches!(
            gateway.place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));
        assert!(transport.all_requests().is_empty());
    }

    #[tokio::test]
    async fn disconnected_websocket_falls_back_to_governed_rest_before_submission() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":91,"clientOrderId":"g_7_S_fixed"}"#.into(),
        }));
        let (relay, _) = BinanceWebsocketOrderRelay::scripted_for_test(false, []);
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(relay);

        let acknowledgement = gateway.place_order(&intent()).await.unwrap();

        assert_eq!(acknowledgement.exchange_order_id, "91");
        assert_eq!(transport.all_requests().len(), 1);
    }

    #[tokio::test]
    async fn transport_timeout_is_unknown_and_must_not_be_retried_blindly() {
        let transport = MockTransport::with_response(Err(TransportError::Timeout("late".into())));
        let error = adapter(transport.clone())
            .place_order(&intent())
            .await
            .unwrap_err();

        assert!(matches!(error, PlacementError::Unknown { .. }));
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn only_the_governors_private_cooldown_code_is_safe_to_retry() {
        let local = MockTransport::with_response(Ok(HttpResponse {
            status: 429,
            body: format!(
                "{{\"code\":\"{BINANCE_LOCAL_COOLDOWN_CODE}\",\"msg\":\"Binance request cooldown is active; retry after 5 ms\"}}"
            ),
        }));
        assert!(matches!(
            adapter(local).place_order(&intent()).await,
            Err(PlacementError::NotSubmitted { .. })
        ));

        let exchange = MockTransport::with_response(Ok(HttpResponse {
            status: 429,
            body: r#"{"code":-1003,"msg":"Too many requests"}"#.into(),
        }));
        assert!(matches!(
            adapter(exchange).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn system_level_throttle_is_known_not_to_have_submitted_an_order() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 503,
            body: r#"{"code":-1008,"msg":"Request throttled by system-level protection. Reduce-only/close-position orders are exempt. Please try again."}"#.into(),
        }));

        assert!(matches!(
            adapter(transport).place_order(&intent()).await,
            Err(PlacementError::NotSubmitted { .. })
        ));
    }

    #[tokio::test]
    async fn exchange_timeout_code_is_unknown_but_filter_rejection_is_definitive() {
        let unknown = MockTransport::with_response(Ok(HttpResponse {
            status: 400,
            body: r#"{"code":-1007,"msg":"Timeout waiting for response"}"#.into(),
        }));
        assert!(matches!(
            adapter(unknown).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));

        let rejected = MockTransport::with_response(Ok(HttpResponse {
            status: 400,
            body: r#"{"code":-1013,"msg":"Filter failure"}"#.into(),
        }));
        assert!(matches!(
            adapter(rejected).place_order(&intent()).await,
            Err(PlacementError::Definitive { code: Some(code), .. }) if code == "-1013"
        ));
    }

    #[tokio::test]
    async fn post_only_would_take_is_retryable_only_for_post_only_orders() {
        let response = || {
            Ok(HttpResponse {
                status: 400,
                body: r#"{"code":-5022,"msg":"Due to the order could not be executed as maker, the Post Only order will be rejected. The order will not be recorded in the order history"}"#.into(),
            })
        };
        let post_only = MockTransport::with_response(response());
        assert!(matches!(
            adapter(post_only).place_order(&intent()).await,
            Err(PlacementError::NotSubmitted { .. })
        ));

        let mut ordinary_limit = intent();
        ordinary_limit.shape.time_in_force = TimeInForce::Gtc;
        let gtc = MockTransport::with_response(response());
        assert!(matches!(
            adapter(gtc).place_order(&ordinary_limit).await,
            Err(PlacementError::Definitive { code: Some(code), .. })
                if code == POST_ONLY_WOULD_TAKE_CODE
        ));
    }

    #[tokio::test]
    async fn ambiguous_or_duplicate_identity_rejections_require_lookup_reconciliation() {
        for body in [
            r#"{"code":-1000,"msg":"Unknown error while processing"}"#,
            r#"{"code":-1001,"msg":"Internal error"}"#,
            r#"{"code":-4116,"msg":"clientOrderId is duplicated"}"#,
            "upstream returned an unreadable response",
        ] {
            let transport = MockTransport::with_response(Ok(HttpResponse {
                status: 400,
                body: body.into(),
            }));

            assert!(matches!(
                adapter(transport).place_order(&intent()).await,
                Err(PlacementError::Unknown { .. })
            ));
        }
    }

    #[tokio::test]
    async fn malformed_success_acknowledgement_is_unknown() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":91,"clientOrderId":"g_7_S_other"}"#.into(),
        }));
        assert!(matches!(
            adapter(transport).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn redirect_response_is_unknown_and_never_followed_by_the_adapter() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 307,
            body: "redirect".into(),
        }));
        let result = adapter(transport.clone()).place_order(&intent()).await;

        assert!(matches!(result, Err(PlacementError::Unknown { .. })));
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_verifies_both_order_identities_and_never_marks_execution_terminal() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":91,"clientOrderId":"g_7_S_fixed","status":"CANCELED"}"#.into(),
        }));
        let acknowledgement = adapter(transport.clone())
            .cancel_order(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "91",
            )
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(acknowledgement.exchange_order_id, "91");
        assert_eq!(request.method, HttpMethod::Delete);
        assert_eq!(request.path, "/fapi/v1/order");
        assert!(
            request
                .query_string()
                .starts_with("symbol=MUUSDT&orderId=91&timestamp=1700000000123")
        );
    }

    #[tokio::test]
    async fn mismatched_cancellation_acknowledgement_remains_unknown() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":92,"clientOrderId":"g_7_S_fixed","status":"CANCELED"}"#.into(),
        }));
        let result = adapter(transport)
            .cancel_order(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "91",
            )
            .await;

        assert!(matches!(result, Err(CancellationError::Unknown { .. })));
    }

    #[tokio::test]
    async fn batch_cancellation_uses_exact_ids_and_splits_at_the_official_limit() {
        let transport = MockTransport::default();
        let targets = (0..53)
            .map(|index| OrderCancellationTarget {
                client_order_id: ClientOrderId::parse(format!("g_{index}_S_batch")).unwrap(),
                exchange_order_id: (100 + index).to_string(),
            })
            .collect::<Vec<_>>();
        for chunk in targets.chunks(MAX_BATCH_CANCELLATIONS) {
            let body = chunk
                .iter()
                .map(|target| {
                    serde_json::json!({
                        "orderId": target.exchange_order_id.parse::<u64>().unwrap(),
                        "clientOrderId": target.client_order_id.as_str(),
                        "status": "CANCELED"
                    })
                })
                .collect::<Vec<_>>();
            transport
                .responses
                .lock()
                .unwrap()
                .push_back(Ok(HttpResponse {
                    status: 200,
                    body: serde_json::to_string(&body).unwrap(),
                }));
        }

        let results = adapter(transport.clone())
            .cancel_orders(Exchange::Binance, "MUUSDT", &targets)
            .await;
        let requests = transport.all_requests();

        assert_eq!(results.len(), targets.len());
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(requests.len(), 6);
        assert!(requests.iter().all(|request| {
            request.method == HttpMethod::Delete && request.path == "/fapi/v1/batchOrders"
        }));
        assert_eq!(
            requests[0]
                .query
                .iter()
                .find(|(key, _)| key == "orderIdList")
                .map(|(_, value)| value.as_str()),
            Some("[100,101,102,103,104,105,106,107,108,109]")
        );
        assert_eq!(
            requests[5]
                .query
                .iter()
                .find(|(key, _)| key == "orderIdList")
                .map(|(_, value)| value.as_str()),
            Some("[150,151,152]")
        );
    }

    #[tokio::test]
    async fn batch_cancellation_keeps_exact_results_when_one_middle_chunk_times_out() {
        let transport = MockTransport::default();
        let targets = (0..21)
            .map(|index| OrderCancellationTarget {
                client_order_id: ClientOrderId::parse(format!("g_{index}_S_timeout")).unwrap(),
                exchange_order_id: (200 + index).to_string(),
            })
            .collect::<Vec<_>>();
        for (chunk_index, chunk) in targets.chunks(MAX_BATCH_CANCELLATIONS).enumerate() {
            let response = if chunk_index == 1 {
                Err(TransportError::Timeout("middle batch timed out".into()))
            } else {
                let body = chunk
                    .iter()
                    .map(|target| {
                        serde_json::json!({
                            "orderId": target.exchange_order_id.parse::<u64>().unwrap(),
                            "clientOrderId": target.client_order_id.as_str(),
                            "status": "CANCELED"
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(HttpResponse {
                    status: 200,
                    body: serde_json::to_string(&body).unwrap(),
                })
            };
            transport.responses.lock().unwrap().push_back(response);
        }

        let results = adapter(transport.clone())
            .cancel_orders(Exchange::Binance, "MUUSDT", &targets)
            .await;

        assert_eq!(transport.all_requests().len(), 3);
        assert!(results[..10].iter().all(Result::is_ok));
        assert!(
            results[10..20]
                .iter()
                .all(|result| matches!(result, Err(CancellationError::Unknown { .. })))
        );
        assert!(results[20].is_ok());
    }

    #[tokio::test]
    async fn incomplete_batch_cancellation_response_never_claims_any_order_was_cancelled() {
        let targets = (0..10)
            .map(|index| OrderCancellationTarget {
                client_order_id: ClientOrderId::parse(format!("g_{index}_S_incomplete")).unwrap(),
                exchange_order_id: (300 + index).to_string(),
            })
            .collect::<Vec<_>>();
        let incomplete = targets[..9]
            .iter()
            .map(|target| {
                serde_json::json!({
                    "orderId": target.exchange_order_id.parse::<u64>().unwrap(),
                    "clientOrderId": target.client_order_id.as_str(),
                    "status": "CANCELED"
                })
            })
            .collect::<Vec<_>>();
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: serde_json::to_string(&incomplete).unwrap(),
        }));

        let results = adapter(transport)
            .cancel_orders(Exchange::Binance, "MUUSDT", &targets)
            .await;

        assert_eq!(results.len(), targets.len());
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Err(CancellationError::Unknown { .. })))
        );
    }

    #[tokio::test]
    async fn mixed_batch_cancellation_response_preserves_each_exact_outcome() {
        let targets = (0..3)
            .map(|index| OrderCancellationTarget {
                client_order_id: ClientOrderId::parse(format!("g_{index}_S_mixed")).unwrap(),
                exchange_order_id: (400 + index).to_string(),
            })
            .collect::<Vec<_>>();
        let body = serde_json::json!([
            {
                "orderId": 400,
                "clientOrderId": "g_0_S_mixed",
                "status": "CANCELED"
            },
            {
                "code": -2011,
                "msg": "Unknown order sent."
            },
            {
                "orderId": 402,
                "clientOrderId": "g_2_S_mixed",
                "status": "CANCELED"
            }
        ]);
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: serde_json::to_string(&body).unwrap(),
        }));

        let results = adapter(transport)
            .cancel_orders(Exchange::Binance, "MUUSDT", &targets)
            .await;

        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(CancellationError::Unknown { .. })));
        assert!(results[2].is_ok());
    }

    #[tokio::test]
    async fn lookup_uses_client_identity_and_preserves_authoritative_shape() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "symbol":"MUUSDT","orderId":91,"clientOrderId":"g_7_S_fixed",
                "side":"SELL","price":"1011","origQty":"0.2","executedQty":"0.1","status":"PARTIALLY_FILLED",
                "reduceOnly":false,"timeInForce":"GTX","type":"LIMIT"
            }"#
            .into(),
        }));
        let result = adapter(transport.clone())
            .lookup_order_by_client_id(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
            )
            .await
            .unwrap();
        let OrderLookup::Found(order) = result else {
            panic!("order should exist")
        };

        assert_eq!(order.shape, intent().shape);
        assert_eq!(
            order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled)
        );
        assert!(
            transport
                .request()
                .query_string()
                .contains("symbol=MUUSDT&origClientOrderId=g_7_S_fixed&timestamp=1700000000123")
        );
    }

    #[tokio::test]
    async fn lookup_returns_not_found_only_for_definitive_exchange_code() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 400,
            body: r#"{"code":-2013,"msg":"Order does not exist."}"#.into(),
        }));
        let result = adapter(transport)
            .lookup_order_by_client_id(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result, OrderLookup::NotFound);
    }

    #[tokio::test]
    async fn execution_snapshot_reconciles_order_totals_and_preserves_fee_assets() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("3.14", "3.14", "3.14", "FILLED"),
            }),
            Ok(HttpResponse {
                status: 200,
                body: r#"[
                    {"symbol":"MUUSDT","id":7,"orderId":91,"side":"SELL","buyer":false,"price":"1","qty":"2","quoteQty":"2","commission":"0.0001","commissionAsset":"BNB","realizedPnl":"0","maker":true,"time":1050000},
                    {"symbol":"MUUSDT","id":8,"orderId":91,"side":"SELL","buyer":false,"price":"1","qty":"1.14","quoteQty":"1.14","commission":"0.000628","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":1060000}
                ]"#
                .into(),
            }),
        ]);
        let snapshot = adapter(transport.clone())
            .execution_snapshot(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "91",
            )
            .await
            .unwrap();
        let requests = transport.all_requests();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(314, 2));
        assert_eq!(snapshot.fees_by_asset["BNB"], Decimal::new(1, 4));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::new(628, 6));
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/fapi/v1/order");
        assert_eq!(requests[1].path, "/fapi/v1/userTrades");
        assert!(requests[1].query_string().contains("orderId=91"));
    }

    #[tokio::test]
    async fn partial_terminal_orders_preserve_filled_and_unfilled_quantities() {
        for (status, expected_terminal) in [
            ("CANCELED", TerminalOrderStatus::Cancelled),
            ("EXPIRED_IN_MATCH", TerminalOrderStatus::Expired),
        ] {
            let transport = MockTransport::default();
            transport.responses.lock().unwrap().extend([
                Ok(HttpResponse {
                    status: 200,
                    body: execution_order_detail("3.14", "1.14", "1.14", status),
                }),
                Ok(HttpResponse {
                    status: 200,
                    body: format!("[{}]", binance_trade(7, "1.14", "1.14")),
                }),
            ]);

            let snapshot = adapter(transport)
                .execution_snapshot(
                    Exchange::Binance,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                    "91",
                )
                .await
                .unwrap();

            assert_eq!(
                snapshot.order.lifecycle,
                OrderLifecycle::Terminal(expected_terminal)
            );
            assert_eq!(snapshot.cumulative_quantity, Decimal::new(114, 2));
            assert_eq!(
                snapshot.order.shape.quantity - snapshot.cumulative_quantity,
                Decimal::new(2, 0)
            );
            assert_eq!(snapshot.trades.len(), 1);
        }
    }

    #[tokio::test]
    async fn full_binance_trade_page_must_advance_before_completion() {
        let full_page = format!(
            "[{}]",
            (1..=TRADE_PAGE_LIMIT as u64)
                .map(|id| binance_trade(id, "1", "1"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("1001", "1001", "1001", "FILLED"),
            }),
            Ok(HttpResponse {
                status: 200,
                body: full_page,
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", binance_trade(1001, "1", "1")),
            }),
        ]);
        let snapshot = adapter(transport.clone())
            .execution_snapshot(
                Exchange::Binance,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "91",
            )
            .await
            .unwrap();
        let requests = transport.all_requests();

        assert_eq!(snapshot.trades.len(), 1001);
        assert!(requests[2].query_string().contains("fromId=1001"));
    }

    #[tokio::test]
    async fn incomplete_binance_trade_history_never_becomes_an_execution() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("3.14", "3.14", "3.14", "FILLED"),
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", binance_trade(1, "3", "3")),
            }),
        ]);

        assert!(
            adapter(transport)
                .execution_snapshot(
                    Exchange::Binance,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                    "91",
                )
                .await
                .is_err()
        );
    }
}
