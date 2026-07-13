use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent},
    exchange::{
        LookupError, OrderLookup, OrderLookupGateway, OrderPlacementGateway,
        PlacementAcknowledgement, PlacementError,
        codec::{
            build_order_parameters, execution_status_is_unknown, order_is_definitively_absent,
            parse_authoritative_order, parse_exchange_error, parse_placement_acknowledgement,
        },
        protocol::{
            HttpMethod, HttpTransport, MillisecondClock, Parameters, PreparedHttpRequest,
            encode_parameters,
        },
    },
};

const PRODUCTION_BASE_URL: &str = "https://fapi.binance.com";
const TESTNET_BASE_URL: &str = "https://testnet.binancefuture.com";

pub trait BinanceRequestSigner: Send + Sync {
    fn sign(&self, message: &str) -> Result<String, SignatureError>;
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
        }
    }

    pub fn set_recv_window_ms(&mut self, recv_window_ms: u64) {
        self.recv_window_ms = recv_window_ms;
    }
}

impl<T, S, C> BinanceAdapter<T, S, C>
where
    S: BinanceRequestSigner,
    C: MillisecondClock,
{
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
            headers: vec![("X-MBX-APIKEY".into(), self.api_key.to_string())],
        })
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

        if (200..300).contains(&response.status) {
            return parse_placement_acknowledgement(&response.body, &intent.client_order_id)
                .map_err(|error| PlacementError::Unknown {
                    message: format!("Binance acknowledgement is not authoritative: {error}"),
                });
        }

        let error = parse_exchange_error(&response.body);
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

fn definitive_local_error(message: &str) -> PlacementError {
    PlacementError::Definitive {
        code: None,
        message: message.into(),
    }
}

fn lookup_error(message: &str) -> LookupError {
    LookupError {
        message: message.into(),
    }
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
        domain::{IntentState, OrderKind, OrderShape, OrderSide, TimeInForce},
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
    async fn lookup_uses_client_identity_and_preserves_authoritative_shape() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "symbol":"MUUSDT","orderId":91,"clientOrderId":"g_7_S_fixed",
                "side":"SELL","price":"1011","origQty":"0.2","status":"PARTIALLY_FILLED",
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
}
