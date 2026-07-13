use std::{
    fmt,
    sync::Mutex,
    time::Duration,
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
        TransportError::Timeout(error.to_string())
    } else if error.is_connect() {
        TransportError::Connection(error.to_string())
    } else {
        TransportError::Other(error.to_string())
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
    use super::*;

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

    #[test]
    fn microsecond_nonce_is_strictly_monotonic() {
        let source = MonotonicMicrosecondNonce::default();
        let first = source.next_nonce();
        let second = source.next_nonce();
        assert!(second > first);
    }
}
