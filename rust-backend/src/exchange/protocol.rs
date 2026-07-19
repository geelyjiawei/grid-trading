use std::{
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
}

const BINANCE_DEFAULT_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);
const BINANCE_DEFAULT_IP_BAN_COOLDOWN: Duration = Duration::from_secs(2 * 60);
const BINANCE_WAF_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const BINANCE_MAX_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Default)]
struct BinanceRequestState {
    next_request_at: Option<Instant>,
    cooldown_until: Option<Instant>,
    consecutive_rate_limits: u32,
}

/// Serializes Binance REST traffic and opens a fail-fast circuit after 429/418.
/// The shared state is deliberately below every adapter operation so reads and
/// writes cannot bypass the same IP-level protection.
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

        if let Some(next_request_at) = state.next_request_at
            && next_request_at > now
        {
            tokio::time::sleep(next_request_at.saturating_duration_since(now)).await;
        }

        let response = self.inner.execute(request).await?;
        state.next_request_at = Some(Instant::now() + self.minimum_interval);
        match response.status {
            403 | 418 | 429 => {
                state.consecutive_rate_limits = state.consecutive_rate_limits.saturating_add(1);
                let cooldown = rate_limit_cooldown(
                    response.status,
                    &response.body,
                    state.consecutive_rate_limits,
                );
                state.cooldown_until = Some(Instant::now() + cooldown);
            }
            200..=399 => state.consecutive_rate_limits = 0,
            _ => {}
        }
        Ok(response)
    }
}

fn rate_limit_cooldown(status: u16, body: &str, consecutive_rate_limits: u32) -> Duration {
    if status == 403 {
        return BINANCE_WAF_COOLDOWN;
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
        let body = response.text().await.map_err(classify_reqwest_error)?;
        Ok(HttpResponse { status, body })
    }
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
    }

    impl ScriptedTransport {
        fn new(responses: impl IntoIterator<Item = HttpResponse>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
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
