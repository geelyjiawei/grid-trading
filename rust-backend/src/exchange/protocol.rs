use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use thiserror::Error;
use url::form_urlencoded;

pub type Parameters = Vec<(String, String)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Delete,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PreparedHttpRequest {
    pub method: HttpMethod,
    pub base_url: String,
    pub path: String,
    pub query: Parameters,
    pub body: Parameters,
    /// Exact bytes for APIs that sign a JSON body. Exchange adapters must not
    /// combine this with form body parameters.
    pub raw_body: Option<String>,
    pub headers: Parameters,
}

impl PreparedHttpRequest {
    pub fn query_string(&self) -> String {
        encode_parameters(&self.query)
    }

    pub fn body_string(&self) -> String {
        self.raw_body
            .clone()
            .unwrap_or_else(|| encode_parameters(&self.body))
    }

    pub fn url(&self) -> String {
        let query = self.query_string();
        if query.is_empty() {
            format!("{}{}", self.base_url, self.path)
        } else {
            format!("{}{}?{query}", self.base_url, self.path)
        }
    }
}

impl fmt::Debug for PreparedHttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redact = |items: &Parameters| {
            items
                .iter()
                .map(|(key, value)| {
                    let hidden = matches!(
                        key.to_ascii_lowercase().as_str(),
                        "signature"
                            | "x-mbx-apikey"
                            | "x-bapi-api-key"
                            | "x-bapi-sign"
                            | "authorization"
                    );
                    (
                        key.clone(),
                        if hidden {
                            "[REDACTED]".into()
                        } else {
                            value.clone()
                        },
                    )
                })
                .collect::<Parameters>()
        };

        formatter
            .debug_struct("PreparedHttpRequest")
            .field("method", &self.method)
            .field("base_url", &self.base_url)
            .field("path", &self.path)
            .field("query", &redact(&self.query))
            .field("body", &redact(&self.body))
            .field(
                "raw_body",
                &self.raw_body.as_ref().map(|_| "[REDACTED JSON BODY]"),
            )
            .field("headers", &redact(&self.headers))
            .finish()
    }
}

pub fn encode_parameters(parameters: &Parameters) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in parameters {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpResponseMetadata {
    pub used_weight_1m: Option<u32>,
    pub order_count_10s: Option<u32>,
    pub order_count_1m: Option<u32>,
    pub retry_after: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTransportResponse {
    pub response: HttpResponse,
    pub metadata: HttpResponseMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("transport failed: {0}")]
    Other(String),
}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn execute(&self, request: PreparedHttpRequest) -> Result<HttpResponse, TransportError>;

    async fn execute_with_metadata(
        &self,
        request: PreparedHttpRequest,
    ) -> Result<HttpTransportResponse, TransportError> {
        let response = self.execute(request).await?;
        Ok(HttpTransportResponse {
            response,
            metadata: HttpResponseMetadata::default(),
        })
    }
}

const BINANCE_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
const BINANCE_ORDER_LIMIT_SHORT_WINDOW: Duration = Duration::from_secs(10);
const BINANCE_DEFAULT_REQUEST_WEIGHT_LIMIT: u32 = 2_400;
const BINANCE_DEFAULT_ORDER_LIMIT_10S: u32 = 300;
const BINANCE_DEFAULT_ORDER_LIMIT: u32 = 1_200;
const BINANCE_BUDGET_NUMERATOR: u32 = 3;
const BINANCE_BUDGET_DENOMINATOR: u32 = 4;
const BINANCE_DEFAULT_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);
const BINANCE_DEFAULT_IP_BAN_COOLDOWN: Duration = Duration::from_secs(2 * 60);
const BINANCE_WAF_COOLDOWN: Duration = Duration::from_secs(15 * 60);
const BINANCE_MAX_WAF_COOLDOWN: Duration = Duration::from_secs(60 * 60);
const BINANCE_MAX_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BinanceRequestCost {
    weight: u32,
    orders: u32,
}

#[derive(Debug, Clone, Copy)]
struct BinanceUsageEvent {
    at: Instant,
    cost: BinanceRequestCost,
}

#[derive(Debug)]
struct BinanceRequestState {
    next_request_at: Option<Instant>,
    cooldown_until: Option<Instant>,
    order_cooldown_until: Option<Instant>,
    consecutive_rate_limits: u32,
    request_weight_limit: u32,
    order_limit_10s: u32,
    order_limit: u32,
    usage: VecDeque<BinanceUsageEvent>,
}

impl Default for BinanceRequestState {
    fn default() -> Self {
        Self {
            next_request_at: None,
            cooldown_until: None,
            order_cooldown_until: None,
            consecutive_rate_limits: 0,
            request_weight_limit: BINANCE_DEFAULT_REQUEST_WEIGHT_LIMIT,
            order_limit_10s: BINANCE_DEFAULT_ORDER_LIMIT_10S,
            order_limit: BINANCE_DEFAULT_ORDER_LIMIT,
            usage: VecDeque::new(),
        }
    }
}

/// Serializes Binance REST traffic, enforces the published weight/order windows,
/// and opens a fail-fast circuit after 403/418/429 responses. The shared state is
/// deliberately below every adapter operation so reads and writes cannot bypass
/// the same IP-level protection.
#[derive(Clone)]
pub struct BinanceRequestGovernor<T> {
    inner: T,
    minimum_interval: Duration,
    state: Arc<tokio::sync::Mutex<BinanceRequestState>>,
}

impl<T> BinanceRequestGovernor<T> {
    pub fn new(inner: T, minimum_interval: Duration) -> Self {
        Self {
            inner,
            minimum_interval,
            state: Arc::new(tokio::sync::Mutex::new(BinanceRequestState::default())),
        }
    }
}

impl<T> fmt::Debug for BinanceRequestGovernor<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceRequestGovernor")
            .field("minimum_interval", &self.minimum_interval)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> HttpTransport for BinanceRequestGovernor<T>
where
    T: HttpTransport,
{
    async fn execute(&self, request: PreparedHttpRequest) -> Result<HttpResponse, TransportError> {
        // Keep the guard through the network request. This prevents already queued
        // requests from escaping after the first response opens the circuit.
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if let Some(cooldown_until) = state.cooldown_until {
            if cooldown_until > now {
                let remaining_ms = cooldown_until.saturating_duration_since(now).as_millis();
                return Ok(HttpResponse {
                    status: 429,
                    body: format!(
                        "{{\"code\":-1003,\"msg\":\"Binance request cooldown is active; retry after {remaining_ms} ms\"}}"
                    ),
                });
            }
            state.cooldown_until = None;
        }

        let request_cost = binance_request_cost(&request);
        if let Some(cooldown_until) = state.order_cooldown_until {
            if cooldown_until > now && request_cost.orders > 0 {
                let cooldown = cooldown_until.saturating_duration_since(now);
                return Ok(binance_cooldown_response(cooldown));
            }
            if cooldown_until <= now {
                state.order_cooldown_until = None;
            }
        }
        prune_binance_usage(&mut state, now);
        if let Some(limit) = binance_budget_cooldown(&state, request_cost, now) {
            let cooldown = match limit {
                BinanceBudgetCooldown::AllRequests(cooldown) => {
                    state.cooldown_until = Some(now + cooldown);
                    cooldown
                }
                BinanceBudgetCooldown::Orders(cooldown) => {
                    state.order_cooldown_until = Some(now + cooldown);
                    cooldown
                }
            };
            return Ok(binance_cooldown_response(cooldown));
        }

        if let Some(next_request_at) = state.next_request_at
            && next_request_at > now
        {
            tokio::time::sleep(next_request_at.saturating_duration_since(now)).await;
        }

        let is_exchange_info = request.path == "/fapi/v1/exchangeInfo";
        let requested_at = Instant::now();
        state.next_request_at = Some(requested_at + self.minimum_interval);
        state.usage.push_back(BinanceUsageEvent {
            at: requested_at,
            cost: request_cost,
        });
        let transport_response = self.inner.execute_with_metadata(request).await?;
        let response = transport_response.response;
        let metadata = transport_response.metadata;
        let responded_at = Instant::now();
        if is_exchange_info
            && let Some((request_weight_limit, order_limit_10s, order_limit)) =
                binance_exchange_limits(&response.body)
        {
            state.request_weight_limit = request_weight_limit;
            state.order_limit_10s = order_limit_10s;
            state.order_limit = order_limit;
        }
        match response.status {
            403 | 418 | 429 => {
                state.consecutive_rate_limits = state.consecutive_rate_limits.saturating_add(1);
                let cooldown = rate_limit_cooldown(
                    response.status,
                    &response.body,
                    metadata.retry_after,
                    state.consecutive_rate_limits,
                );
                state.cooldown_until = Some(responded_at + cooldown);
            }
            200..=399 => {
                state.consecutive_rate_limits = 0;
                if binance_weight_header_is_exhausted(&state, &metadata) {
                    state.cooldown_until = Some(responded_at + BINANCE_RATE_LIMIT_WINDOW);
                }
                if let Some(cooldown) = binance_order_header_cooldown(&state, &metadata) {
                    state.order_cooldown_until = Some(responded_at + cooldown);
                }
            }
            _ => {}
        }
        Ok(response)
    }
}

fn binance_cooldown_response(cooldown: Duration) -> HttpResponse {
    let remaining_ms = cooldown.as_millis();
    HttpResponse {
        status: 429,
        body: format!(
            "{{\"code\":-1003,\"msg\":\"Binance request cooldown is active; retry after {remaining_ms} ms\"}}"
        ),
    }
}

fn binance_request_cost(request: &PreparedHttpRequest) -> BinanceRequestCost {
    let has_symbol = request
        .query
        .iter()
        .chain(request.body.iter())
        .any(|(key, value)| key == "symbol" && !value.is_empty());
    let weight = match (request.method, request.path.as_str()) {
        (_, "/fapi/v1/commissionRate") => 20,
        (_, "/fapi/v3/account")
        | (_, "/fapi/v2/positionRisk")
        | (_, "/fapi/v1/allOrders")
        | (_, "/fapi/v1/userTrades") => 5,
        (_, "/fapi/v1/openOrders") if !has_symbol => 40,
        (_, "/fapi/v1/ticker/24hr") if !has_symbol => 40,
        (_, "/fapi/v1/premiumIndex") if !has_symbol => 10,
        (HttpMethod::Post, "/fapi/v1/order") => 0,
        (_, "/fapi/v1/order")
        | (_, "/fapi/v1/openOrders")
        | (_, "/fapi/v1/ticker/24hr")
        | (_, "/fapi/v1/premiumIndex")
        | (_, "/fapi/v1/exchangeInfo")
        | (_, "/fapi/v1/klines")
        | (_, "/fapi/v1/leverage") => 1,
        _ => 20,
    };
    let orders = u32::from(matches!(
        (request.method, request.path.as_str()),
        (HttpMethod::Post | HttpMethod::Delete, "/fapi/v1/order")
    ));
    BinanceRequestCost { weight, orders }
}

fn prune_binance_usage(state: &mut BinanceRequestState, now: Instant) {
    while state
        .usage
        .front()
        .is_some_and(|event| now.saturating_duration_since(event.at) >= BINANCE_RATE_LIMIT_WINDOW)
    {
        state.usage.pop_front();
    }
}

fn binance_budget(limit: u32) -> u32 {
    limit
        .saturating_mul(BINANCE_BUDGET_NUMERATOR)
        .checked_div(BINANCE_BUDGET_DENOMINATOR)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinanceBudgetCooldown {
    AllRequests(Duration),
    Orders(Duration),
}

fn binance_budget_cooldown(
    state: &BinanceRequestState,
    request_cost: BinanceRequestCost,
    now: Instant,
) -> Option<BinanceBudgetCooldown> {
    let (used_weight, used_orders) = state.usage.iter().fold((0_u32, 0_u32), |used, event| {
        (
            used.0.saturating_add(event.cost.weight),
            used.1.saturating_add(event.cost.orders),
        )
    });
    let used_orders_10s = state
        .usage
        .iter()
        .filter(|event| now.saturating_duration_since(event.at) < BINANCE_ORDER_LIMIT_SHORT_WINDOW)
        .fold(0_u32, |used, event| used.saturating_add(event.cost.orders));
    let exceeds_weight = used_weight.saturating_add(request_cost.weight)
        > binance_budget(state.request_weight_limit);
    let exceeds_orders = request_cost.orders > 0
        && used_orders.saturating_add(request_cost.orders) > binance_budget(state.order_limit);
    let exceeds_orders_10s = request_cost.orders > 0
        && used_orders_10s.saturating_add(request_cost.orders)
            > binance_budget(state.order_limit_10s);
    if !exceeds_weight && !exceeds_orders && !exceeds_orders_10s {
        return None;
    }

    let remaining_for = |window: Duration, includes: fn(&BinanceUsageEvent) -> bool| {
        state
            .usage
            .iter()
            .filter(|event| now.saturating_duration_since(event.at) < window)
            .find(|event| includes(event))
            .map_or(window, |oldest| {
                window.saturating_sub(now.saturating_duration_since(oldest.at))
            })
    };
    if exceeds_weight {
        let cooldown = remaining_for(BINANCE_RATE_LIMIT_WINDOW, |event| event.cost.weight > 0)
            .saturating_add(Duration::from_millis(1));
        return Some(BinanceBudgetCooldown::AllRequests(cooldown));
    }
    let mut cooldown = Duration::ZERO;
    if exceeds_orders {
        cooldown = cooldown.max(remaining_for(BINANCE_RATE_LIMIT_WINDOW, |event| {
            event.cost.orders > 0
        }));
    }
    if exceeds_orders_10s {
        cooldown = cooldown.max(remaining_for(BINANCE_ORDER_LIMIT_SHORT_WINDOW, |event| {
            event.cost.orders > 0
        }));
    }
    Some(BinanceBudgetCooldown::Orders(
        cooldown.saturating_add(Duration::from_millis(1)),
    ))
}

fn binance_weight_header_is_exhausted(
    state: &BinanceRequestState,
    metadata: &HttpResponseMetadata,
) -> bool {
    metadata
        .used_weight_1m
        .is_some_and(|used| used >= state.request_weight_limit)
}

fn binance_order_header_cooldown(
    state: &BinanceRequestState,
    metadata: &HttpResponseMetadata,
) -> Option<Duration> {
    let ten_second_exhausted = metadata
        .order_count_10s
        .is_some_and(|used| used >= state.order_limit_10s);
    let one_minute_exhausted = metadata
        .order_count_1m
        .is_some_and(|used| used >= state.order_limit);
    match (ten_second_exhausted, one_minute_exhausted) {
        (_, true) => Some(BINANCE_RATE_LIMIT_WINDOW),
        (true, false) => Some(BINANCE_ORDER_LIMIT_SHORT_WINDOW),
        (false, false) => None,
    }
}

fn binance_exchange_limits(body: &str) -> Option<(u32, u32, u32)> {
    let payload: serde_json::Value = serde_json::from_str(body).ok()?;
    let limits = payload.get("rateLimits")?.as_array()?;
    let mut request_weight = None;
    let mut orders_10s = None;
    let mut orders = None;
    for limit in limits {
        let Some(rate_type) = limit
            .get("rateLimitType")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(interval) = limit.get("interval").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(interval_num) = limit.get("intervalNum").and_then(serde_json::Value::as_u64)
        else {
            continue;
        };
        let Some(value) = limit
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
        else {
            continue;
        };
        match (rate_type, interval, interval_num) {
            ("REQUEST_WEIGHT", "MINUTE", 1) => request_weight = Some(value),
            ("ORDERS", "SECOND", 10) => orders_10s = Some(value),
            ("ORDERS", "MINUTE", 1) => orders = Some(value),
            _ => {}
        }
    }
    Some((
        request_weight?,
        orders_10s.unwrap_or(BINANCE_DEFAULT_ORDER_LIMIT_10S),
        orders?,
    ))
}

fn rate_limit_cooldown(
    status: u16,
    body: &str,
    retry_after: Option<Duration>,
    consecutive_rate_limits: u32,
) -> Duration {
    if let Some(retry_after) = retry_after.filter(|duration| !duration.is_zero()) {
        return retry_after.saturating_add(Duration::from_secs(1));
    }
    if status == 403 {
        let exponent = consecutive_rate_limits.saturating_sub(1).min(4);
        return BINANCE_WAF_COOLDOWN
            .saturating_mul(1_u32 << exponent)
            .min(BINANCE_MAX_WAF_COOLDOWN);
    }
    if status == 418 {
        if let Some(ban_until_ms) = binance_ban_until_ms(body) {
            let remaining_ms = ban_until_ms.saturating_sub(unix_duration_millis());
            if remaining_ms > 0 {
                return Duration::from_millis(remaining_ms).saturating_add(Duration::from_secs(1));
            }
        }
        return BINANCE_DEFAULT_IP_BAN_COOLDOWN;
    }

    let exponent = consecutive_rate_limits.saturating_sub(1).min(5);
    BINANCE_DEFAULT_RATE_LIMIT_COOLDOWN
        .saturating_mul(1_u32 << exponent)
        .min(BINANCE_MAX_RATE_LIMIT_COOLDOWN)
}

fn binance_ban_until_ms(body: &str) -> Option<u64> {
    let marker = "banned until ";
    let start = body.find(marker)?.saturating_add(marker.len());
    let digits = body[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub fn new(timeout: Duration) -> Result<Self, TransportBuildError> {
        if timeout.is_zero() {
            return Err(TransportBuildError::InvalidTimeout);
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("grid-trading-rust/0.1")
            .build()
            .map_err(|error| TransportBuildError::Build(error.to_string()))?;
        Ok(Self { client })
    }

    pub fn standard() -> Result<Self, TransportBuildError> {
        Self::new(Duration::from_secs(10))
    }

    async fn send(
        &self,
        request: PreparedHttpRequest,
    ) -> Result<HttpTransportResponse, TransportError> {
        let method = match request.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Delete => reqwest::Method::DELETE,
        };
        let mut builder = self.client.request(method, request.url());
        for (name, value) in &request.headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| TransportError::Other(format!("invalid header name: {error}")))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|error| TransportError::Other(format!("invalid header value: {error}")))?;
            builder = builder.header(name, value);
        }
        let body = request.body_string();
        if !body.is_empty() {
            builder = builder.body(body);
        }
        let response = builder.send().await.map_err(classify_reqwest_error)?;
        let status = response.status().as_u16();
        let metadata = response_metadata(response.headers());
        let body = response.text().await.map_err(classify_reqwest_error)?;
        Ok(HttpTransportResponse {
            response: HttpResponse { status, body },
            metadata,
        })
    }
}

impl fmt::Debug for ReqwestTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestTransport")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportBuildError {
    #[error("HTTP timeout must be positive")]
    InvalidTimeout,
    #[error("HTTP client initialization failed: {0}")]
    Build(String),
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn execute(&self, request: PreparedHttpRequest) -> Result<HttpResponse, TransportError> {
        Ok(self.send(request).await?.response)
    }

    async fn execute_with_metadata(
        &self,
        request: PreparedHttpRequest,
    ) -> Result<HttpTransportResponse, TransportError> {
        self.send(request).await
    }
}

fn response_metadata(headers: &reqwest::header::HeaderMap) -> HttpResponseMetadata {
    HttpResponseMetadata {
        used_weight_1m: decimal_header(headers, "x-mbx-used-weight-1m"),
        order_count_10s: decimal_header(headers, "x-mbx-order-count-10s"),
        order_count_1m: decimal_header(headers, "x-mbx-order-count-1m"),
        retry_after: decimal_header::<u64>(headers, "retry-after").map(Duration::from_secs),
    }
}

fn decimal_header<T>(headers: &reqwest::header::HeaderMap, name: &'static str) -> Option<T>
where
    T: std::str::FromStr,
{
    headers.get(name)?.to_str().ok()?.parse().ok()
}

fn classify_reqwest_error(error: reqwest::Error) -> TransportError {
    if error.is_timeout() {
        TransportError::Timeout("HTTP request timed out".into())
    } else if error.is_connect() {
        TransportError::Connection("HTTP connection failed".into())
    } else {
        let message = error.status().map_or_else(
            || "HTTP transport operation failed".to_owned(),
            |status| format!("HTTP transport operation failed with status {status}"),
        );
        TransportError::Other(message)
    }
}

pub trait MillisecondClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

pub trait NonceSource: Send + Sync {
    fn next_nonce(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl MillisecondClock for SystemClock {
    fn now_millis(&self) -> u64 {
        unix_duration_millis()
    }
}

#[derive(Debug, Default)]
pub struct MonotonicMicrosecondNonce {
    last: Mutex<u64>,
}

impl NonceSource for MonotonicMicrosecondNonce {
    fn next_nonce(&self) -> u64 {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observed = unix_duration_micros();
        let next = observed.max(last.saturating_add(1));
        *last = next;
        next
    }
}

fn unix_duration_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn unix_duration_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Clone)]
    struct ScriptedTransport {
        calls: Arc<AtomicUsize>,
        responses: Arc<Mutex<VecDeque<HttpResponse>>>,
        metadata: HttpResponseMetadata,
    }

    impl ScriptedTransport {
        fn new(responses: impl IntoIterator<Item = HttpResponse>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                metadata: HttpResponseMetadata::default(),
            }
        }

        fn with_metadata(response: HttpResponse, metadata: HttpResponseMetadata) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(VecDeque::from([response]))),
                metadata,
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl HttpTransport for ScriptedTransport {
        async fn execute(
            &self,
            _request: PreparedHttpRequest,
        ) -> Result<HttpResponse, TransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| TransportError::Other("script is exhausted".into()))
        }

        async fn execute_with_metadata(
            &self,
            request: PreparedHttpRequest,
        ) -> Result<HttpTransportResponse, TransportError> {
            let response = self.execute(request).await?;
            Ok(HttpTransportResponse {
                response,
                metadata: self.metadata.clone(),
            })
        }
    }

    fn test_request() -> PreparedHttpRequest {
        PreparedHttpRequest {
            method: HttpMethod::Get,
            base_url: "https://fapi.binance.test".into(),
            path: "/fapi/v1/openOrders".into(),
            query: vec![],
            body: vec![],
            raw_body: None,
            headers: vec![],
        }
    }

    fn symbol_request(method: HttpMethod, path: &str) -> PreparedHttpRequest {
        PreparedHttpRequest {
            method,
            base_url: "https://fapi.binance.test".into(),
            path: path.into(),
            query: vec![("symbol".into(), "ESPORTSUSDT".into())],
            body: vec![],
            raw_body: None,
            headers: vec![],
        }
    }

    #[test]
    fn form_encoding_preserves_insertion_order_and_escapes_values() {
        let params = vec![
            ("symbol".into(), "MUUSDT".into()),
            ("note".into(), "a b&c".into()),
        ];

        assert_eq!(encode_parameters(&params), "symbol=MUUSDT&note=a+b%26c");
    }

    #[test]
    fn request_debug_output_redacts_authentication_material() {
        let request = PreparedHttpRequest {
            method: HttpMethod::Post,
            base_url: "https://example.test".into(),
            path: "/order".into(),
            query: vec![("signature".into(), "secret-signature".into())],
            body: vec![],
            raw_body: None,
            headers: vec![("X-MBX-APIKEY".into(), "secret-key".into())],
        };

        let rendered = format!("{request:?}");
        assert!(!rendered.contains("secret-signature"));
        assert!(!rendered.contains("secret-key"));
        assert_eq!(rendered.matches("[REDACTED]").count(), 2);
    }

    #[test]
    fn exact_json_body_is_transported_without_reencoding_and_debug_is_redacted() {
        let exact_body = r#"{"category":"linear","qty":"0.20"}"#;
        let request = PreparedHttpRequest {
            method: HttpMethod::Post,
            base_url: "https://example.test".into(),
            path: "/v5/order/create".into(),
            query: vec![],
            body: vec![],
            raw_body: Some(exact_body.into()),
            headers: vec![
                ("X-BAPI-API-KEY".into(), "secret-key".into()),
                ("X-BAPI-SIGN".into(), "secret-signature".into()),
            ],
        };

        assert_eq!(request.body_string(), exact_body);
        let rendered = format!("{request:?}");
        assert!(!rendered.contains(exact_body));
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("secret-signature"));
        assert!(rendered.contains("[REDACTED JSON BODY]"));
    }

    #[tokio::test]
    async fn transport_errors_never_disclose_the_signed_request_url() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            drop(stream);
        });
        let transport = ReqwestTransport::new(Duration::from_secs(2)).unwrap();
        let request = PreparedHttpRequest {
            method: HttpMethod::Get,
            base_url: format!("http://{address}"),
            path: "/signed-order".into(),
            query: vec![("signature".into(), "secret-signature".into())],
            body: vec![],
            raw_body: None,
            headers: vec![],
        };

        let error = transport.execute(request).await.unwrap_err();
        server.join().unwrap();
        let rendered = error.to_string();

        assert!(!rendered.contains("secret-signature"));
        assert!(!rendered.contains("signature="));
        assert!(!rendered.contains("/signed-order"));
    }

    #[tokio::test]
    async fn binance_governor_stops_queued_network_calls_during_an_ip_ban() {
        let ban_until_ms = unix_duration_millis() + 60_000;
        let transport = ScriptedTransport::new([HttpResponse {
            status: 418,
            body: format!(
                "{{\"code\":-1003,\"msg\":\"Way too many requests; IP banned until {ban_until_ms}.\"}}"
            ),
        }]);
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first = governor.execute(test_request()).await.unwrap();
        let blocked = governor.execute(test_request()).await.unwrap();

        assert_eq!(first.status, 418);
        assert_eq!(blocked.status, 429);
        assert!(blocked.body.contains("cooldown is active"));
        assert_eq!(transport.call_count(), 1);
    }

    #[tokio::test]
    async fn binance_governor_opens_a_cooldown_after_the_first_429() {
        let transport = ScriptedTransport::new([HttpResponse {
            status: 429,
            body: r#"{"code":-1003,"msg":"Too many requests"}"#.into(),
        }]);
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first = governor.execute(test_request()).await.unwrap();
        let blocked = governor.execute(test_request()).await.unwrap();

        assert_eq!(first.status, 429);
        assert_eq!(blocked.status, 429);
        assert_eq!(transport.call_count(), 1);
    }

    #[tokio::test]
    async fn binance_governor_treats_a_waf_403_as_an_ip_level_cooldown() {
        let transport = ScriptedTransport::new([HttpResponse {
            status: 403,
            body: "request blocked by WAF".into(),
        }]);
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first = governor.execute(test_request()).await.unwrap();
        let blocked = governor.execute(test_request()).await.unwrap();

        assert_eq!(first.status, 403);
        assert_eq!(blocked.status, 429);
        assert_eq!(transport.call_count(), 1);
    }

    #[tokio::test]
    async fn binance_governor_honors_the_exchange_retry_after_header() {
        let transport = ScriptedTransport::with_metadata(
            HttpResponse {
                status: 429,
                body: r#"{"code":-1003,"msg":"Too many requests"}"#.into(),
            },
            HttpResponseMetadata {
                retry_after: Some(Duration::from_secs(37)),
                ..HttpResponseMetadata::default()
            },
        );
        let governor = BinanceRequestGovernor::new(transport, Duration::ZERO);
        let before = Instant::now();

        governor.execute(test_request()).await.unwrap();

        let state = governor.state.lock().await;
        let cooldown_until = state.cooldown_until.unwrap();
        assert!(cooldown_until >= before + Duration::from_secs(38));
        assert!(cooldown_until <= Instant::now() + Duration::from_secs(39));
    }

    #[tokio::test]
    async fn binance_governor_fails_fast_before_crossing_the_weight_budget() {
        let transport = ScriptedTransport::new((0..4).map(|_| HttpResponse {
            status: 200,
            body: "[]".into(),
        }));
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);
        {
            let mut state = governor.state.lock().await;
            state.request_weight_limit = 4;
        }

        for _ in 0..3 {
            let response = governor
                .execute(symbol_request(HttpMethod::Get, "/fapi/v1/openOrders"))
                .await
                .unwrap();
            assert_eq!(response.status, 200);
        }
        let blocked = governor
            .execute(symbol_request(HttpMethod::Get, "/fapi/v1/openOrders"))
            .await
            .unwrap();

        assert_eq!(blocked.status, 429);
        assert!(blocked.body.contains("cooldown is active"));
        assert_eq!(transport.call_count(), 3);
    }

    #[tokio::test]
    async fn binance_governor_enforces_the_ten_second_order_budget() {
        let transport = ScriptedTransport::new((0..4).map(|_| HttpResponse {
            status: 200,
            body: r#"{"orderId":1}"#.into(),
        }));
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);
        {
            let mut state = governor.state.lock().await;
            state.order_limit_10s = 4;
        }

        for _ in 0..3 {
            let response = governor
                .execute(symbol_request(HttpMethod::Post, "/fapi/v1/order"))
                .await
                .unwrap();
            assert_eq!(response.status, 200);
        }
        let read = governor
            .execute(symbol_request(HttpMethod::Get, "/fapi/v1/openOrders"))
            .await
            .unwrap();
        let blocked = governor
            .execute(symbol_request(HttpMethod::Post, "/fapi/v1/order"))
            .await
            .unwrap();

        assert_eq!(read.status, 200);
        assert_eq!(blocked.status, 429);
        assert_eq!(transport.call_count(), 4);
        let state = governor.state.lock().await;
        let remaining = state
            .order_cooldown_until
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(remaining <= BINANCE_ORDER_LIMIT_SHORT_WINDOW + Duration::from_millis(1));
    }

    #[test]
    fn ten_second_order_cooldown_ignores_older_order_events() {
        let now = Instant::now();
        let mut state = BinanceRequestState {
            order_limit_10s: 4,
            ..BinanceRequestState::default()
        };
        let order_cost = BinanceRequestCost {
            weight: 0,
            orders: 1,
        };
        state.usage.push_back(BinanceUsageEvent {
            at: now - Duration::from_secs(20),
            cost: order_cost,
        });
        for _ in 0..3 {
            state.usage.push_back(BinanceUsageEvent {
                at: now - Duration::from_secs(1),
                cost: order_cost,
            });
        }

        let BinanceBudgetCooldown::Orders(cooldown) =
            binance_budget_cooldown(&state, order_cost, now).unwrap()
        else {
            panic!("the order window must not pause read requests");
        };

        assert!(cooldown >= Duration::from_secs(9));
        assert!(cooldown <= Duration::from_secs(9) + Duration::from_millis(1));
    }

    #[test]
    fn binance_request_costs_cover_every_adapter_endpoint_shape() {
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Get, "/fapi/v1/openOrders")),
            BinanceRequestCost {
                weight: 1,
                orders: 0
            }
        );
        assert_eq!(
            binance_request_cost(&test_request()),
            BinanceRequestCost {
                weight: 40,
                orders: 0
            }
        );
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Get, "/fapi/v1/commissionRate")),
            BinanceRequestCost {
                weight: 20,
                orders: 0
            }
        );
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Get, "/fapi/v3/account")),
            BinanceRequestCost {
                weight: 5,
                orders: 0
            }
        );
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Post, "/fapi/v1/order")),
            BinanceRequestCost {
                weight: 0,
                orders: 1
            }
        );
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Delete, "/fapi/v1/order")),
            BinanceRequestCost {
                weight: 1,
                orders: 1
            }
        );
        assert_eq!(
            binance_request_cost(&symbol_request(HttpMethod::Get, "/unknown")),
            BinanceRequestCost {
                weight: 20,
                orders: 0
            }
        );
    }

    #[test]
    fn binance_exchange_info_updates_the_official_one_minute_limits() {
        let body = r#"{
            "rateLimits": [
                {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400},
                {"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1200},
                {"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":300}
            ]
        }"#;

        assert_eq!(binance_exchange_limits(body), Some((2_400, 300, 1_200)));
        assert_eq!(binance_exchange_limits("{}"), None);
    }

    #[test]
    fn binance_response_headers_preserve_usage_and_retry_deadline() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-mbx-used-weight-1m", "1789".parse().unwrap());
        headers.insert("x-mbx-order-count-10s", "45".parse().unwrap());
        headers.insert("x-mbx-order-count-1m", "321".parse().unwrap());
        headers.insert("retry-after", "42".parse().unwrap());

        assert_eq!(
            response_metadata(&headers),
            HttpResponseMetadata {
                used_weight_1m: Some(1_789),
                order_count_10s: Some(45),
                order_count_1m: Some(321),
                retry_after: Some(Duration::from_secs(42)),
            }
        );
    }

    #[tokio::test]
    async fn safety_budget_headers_do_not_starve_follow_up_requests() {
        let mut transport = ScriptedTransport::new((0..3).map(|_| HttpResponse {
            status: 200,
            body: "[]".into(),
        }));
        transport.metadata = HttpResponseMetadata {
            used_weight_1m: Some(binance_budget(BINANCE_DEFAULT_REQUEST_WEIGHT_LIMIT)),
            order_count_10s: Some(binance_budget(BINANCE_DEFAULT_ORDER_LIMIT_10S)),
            order_count_1m: Some(binance_budget(BINANCE_DEFAULT_ORDER_LIMIT)),
            ..HttpResponseMetadata::default()
        };
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first_read = governor.execute(test_request()).await.unwrap();
        let second_read = governor.execute(test_request()).await.unwrap();
        let order = governor
            .execute(symbol_request(HttpMethod::Post, "/fapi/v1/order"))
            .await
            .unwrap();

        assert_eq!(first_read.status, 200);
        assert_eq!(second_read.status, 200);
        assert_eq!(order.status, 200);
        assert_eq!(transport.call_count(), 3);
    }

    #[tokio::test]
    async fn exchange_order_headers_at_the_hard_limit_pause_writes_only() {
        let mut transport = ScriptedTransport::new((0..2).map(|_| HttpResponse {
            status: 200,
            body: "[]".into(),
        }));
        transport.metadata = HttpResponseMetadata {
            order_count_10s: Some(BINANCE_DEFAULT_ORDER_LIMIT_10S),
            ..HttpResponseMetadata::default()
        };
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first_read = governor.execute(test_request()).await.unwrap();
        let second_read = governor.execute(test_request()).await.unwrap();
        let blocked_order = governor
            .execute(symbol_request(HttpMethod::Post, "/fapi/v1/order"))
            .await
            .unwrap();

        assert_eq!(first_read.status, 200);
        assert_eq!(second_read.status, 200);
        assert_eq!(blocked_order.status, 429);
        assert_eq!(transport.call_count(), 2);
    }

    #[tokio::test]
    async fn exchange_weight_header_at_the_hard_limit_pauses_all_requests() {
        let mut transport = ScriptedTransport::new([HttpResponse {
            status: 200,
            body: "[]".into(),
        }]);
        transport.metadata = HttpResponseMetadata {
            used_weight_1m: Some(BINANCE_DEFAULT_REQUEST_WEIGHT_LIMIT),
            ..HttpResponseMetadata::default()
        };
        let governor = BinanceRequestGovernor::new(transport.clone(), Duration::ZERO);

        let first_read = governor.execute(test_request()).await.unwrap();
        let blocked_read = governor.execute(test_request()).await.unwrap();

        assert_eq!(first_read.status, 200);
        assert_eq!(blocked_read.status, 429);
        assert_eq!(transport.call_count(), 1);
    }

    #[test]
    fn repeated_waf_rejections_use_a_bounded_exponential_cooldown() {
        assert_eq!(
            rate_limit_cooldown(403, "", None, 1),
            Duration::from_secs(15 * 60)
        );
        assert_eq!(
            rate_limit_cooldown(403, "", None, 2),
            Duration::from_secs(30 * 60)
        );
        assert_eq!(
            rate_limit_cooldown(403, "", None, 3),
            Duration::from_secs(60 * 60)
        );
        assert_eq!(
            rate_limit_cooldown(403, "", None, 10),
            Duration::from_secs(60 * 60)
        );
    }

    #[test]
    fn binance_ip_ban_deadline_is_parsed_without_trusting_other_numbers() {
        assert_eq!(
            binance_ban_until_ms(
                r#"{"code":-1003,"msg":"IP(43.163.232.101) banned until 1784418117158."}"#
            ),
            Some(1_784_418_117_158)
        );
    }

    #[test]
    fn microsecond_nonce_is_strictly_monotonic() {
        let source = MonotonicMicrosecondNonce::default();
        let first = source.next_nonce();
        let second = source.next_nonce();
        assert!(second > first);
    }
}
