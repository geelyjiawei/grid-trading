use async_trait::async_trait;
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent},
    exchange::{
        CancellationAcknowledgement, CancellationError, LookupError, OrderCancellationGateway,
        OrderLookup, OrderLookupGateway, OrderPlacementGateway, PlacementAcknowledgement,
        PlacementError,
        codec::{
            build_order_parameters, execution_status_is_unknown, order_is_definitively_absent,
            parse_authoritative_order, parse_cancellation_acknowledgement, parse_exchange_error,
            parse_placement_acknowledgement,
        },
        protocol::{
            HttpMethod, HttpTransport, NonceSource, Parameters, PreparedHttpRequest,
            encode_parameters,
        },
    },
};

const PRODUCTION_BASE_URL: &str = "https://fapi.asterdex.com";
const TESTNET_BASE_URL: &str = "https://fapi.asterdex-testnet.com";
const ASTER_CHAIN_ID: u64 = 1666;
const EIP712_DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
const ASTER_MESSAGE_TYPE: &str = "Message(string msg)";

pub trait AsterMessageSigner: Send + Sync {
    fn signer_address(&self) -> &str;
    fn sign_eip712_message(&self, message: &str) -> Result<String, AsterSignatureError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AsterSignatureError {
    #[error("Aster production wallet address is required")]
    MissingUserAddress,
    #[error("Aster signer address is required")]
    MissingSignerAddress,
    #[error("Aster private key must be a valid 32-byte secp256k1 key")]
    InvalidPrivateKey,
    #[error("Aster EIP-712 signing failed: {0}")]
    SigningFailed(String),
}

pub struct LocalEip712Signer {
    signing_key: SigningKey,
    signer_address: String,
}

impl LocalEip712Signer {
    pub fn from_private_key(private_key: &str) -> Result<Self, AsterSignatureError> {
        let normalized = private_key
            .trim()
            .strip_prefix("0x")
            .unwrap_or(private_key.trim());
        let key_bytes = Zeroizing::new(
            hex::decode(normalized).map_err(|_| AsterSignatureError::InvalidPrivateKey)?,
        );
        if key_bytes.len() != 32 {
            return Err(AsterSignatureError::InvalidPrivateKey);
        }
        let signing_key = SigningKey::from_slice(key_bytes.as_slice())
            .map_err(|_| AsterSignatureError::InvalidPrivateKey)?;
        let signer_address = ethereum_address(signing_key.verifying_key());
        Ok(Self {
            signing_key,
            signer_address,
        })
    }
}

impl std::fmt::Debug for LocalEip712Signer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalEip712Signer")
            .field("signing_key", &"[REDACTED]")
            .field("signer_address", &self.signer_address)
            .finish()
    }
}

impl AsterMessageSigner for LocalEip712Signer {
    fn signer_address(&self) -> &str {
        &self.signer_address
    }

    fn sign_eip712_message(&self, message: &str) -> Result<String, AsterSignatureError> {
        let digest = aster_eip712_digest(message);
        let (signature, recovery_id) = self
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|error| AsterSignatureError::SigningFailed(error.to_string()))?;
        let mut bytes = signature.to_bytes().to_vec();
        bytes.push(recovery_id.to_byte().saturating_add(27));
        Ok(hex::encode(bytes))
    }
}

pub struct AsterAdapter<T, S, N> {
    transport: T,
    signer: S,
    nonce_source: N,
    user_address: String,
    base_url: String,
}

impl<T, S, N> AsterAdapter<T, S, N> {
    pub fn production(
        transport: T,
        signer: S,
        nonce_source: N,
        user_address: impl Into<String>,
    ) -> Self {
        Self::with_base_url(
            transport,
            signer,
            nonce_source,
            user_address,
            PRODUCTION_BASE_URL,
        )
    }

    pub fn testnet(
        transport: T,
        signer: S,
        nonce_source: N,
        user_address: impl Into<String>,
    ) -> Self {
        Self::with_base_url(
            transport,
            signer,
            nonce_source,
            user_address,
            TESTNET_BASE_URL,
        )
    }

    pub fn with_base_url(
        transport: T,
        signer: S,
        nonce_source: N,
        user_address: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            transport,
            signer,
            nonce_source,
            user_address: user_address.into(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        }
    }
}

impl<T, N> AsterAdapter<T, LocalEip712Signer, N> {
    pub fn production_wallet(
        transport: T,
        nonce_source: N,
        private_key: &str,
    ) -> Result<Self, AsterSignatureError> {
        let signer = LocalEip712Signer::from_private_key(private_key)?;
        let user_address = signer.signer_address().to_owned();
        Ok(Self::production(
            transport,
            signer,
            nonce_source,
            user_address,
        ))
    }

    pub fn testnet_wallet(
        transport: T,
        nonce_source: N,
        private_key: &str,
    ) -> Result<Self, AsterSignatureError> {
        let signer = LocalEip712Signer::from_private_key(private_key)?;
        let user_address = signer.signer_address().to_owned();
        Ok(Self::testnet(transport, signer, nonce_source, user_address))
    }
}

impl<T, S, N> AsterAdapter<T, S, N>
where
    S: AsterMessageSigner,
    N: NonceSource,
{
    fn signed_request(
        &self,
        method: HttpMethod,
        path: &str,
        mut parameters: Parameters,
    ) -> Result<PreparedHttpRequest, AsterSignatureError> {
        if self.user_address.trim().is_empty() {
            return Err(AsterSignatureError::MissingUserAddress);
        }
        let signer_address = self.signer.signer_address().trim();
        if signer_address.is_empty() {
            return Err(AsterSignatureError::MissingSignerAddress);
        }
        parameters.push(("nonce".into(), self.nonce_source.next_nonce().to_string()));
        parameters.push(("user".into(), self.user_address.clone()));
        parameters.push(("signer".into(), signer_address.into()));
        let signature = self
            .signer
            .sign_eip712_message(&encode_parameters(&parameters))?;
        parameters.push(("signature".into(), signature));

        let (query, body) = match method {
            HttpMethod::Get => (parameters, vec![]),
            HttpMethod::Post | HttpMethod::Delete => (vec![], parameters),
        };
        Ok(PreparedHttpRequest {
            method,
            base_url: self.base_url.clone(),
            path: path.into(),
            query,
            body,
            headers: vec![(
                "Content-Type".into(),
                "application/x-www-form-urlencoded".into(),
            )],
        })
    }
}

#[async_trait]
impl<T, S, N> OrderPlacementGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        if intent.exchange != Exchange::Aster {
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
            .signed_request(HttpMethod::Post, "/fapi/v3/order", params)
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
                    message: format!("Aster acknowledgement is not authoritative: {error}"),
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
impl<T, S, N> OrderLookupGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        if exchange != Exchange::Aster {
            return Err(lookup_error("lookup belongs to another exchange"));
        }
        let params = vec![
            ("symbol".into(), symbol.to_ascii_uppercase()),
            ("origClientOrderId".into(), client_order_id.as_str().into()),
        ];
        let request = self
            .signed_request(HttpMethod::Get, "/fapi/v3/order", params)
            .map_err(|error| lookup_error(&error.to_string()))?;
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(|error| lookup_error(&error.to_string()))?;
        if (200..300).contains(&response.status) {
            return parse_authoritative_order(
                &response.body,
                Exchange::Aster,
                symbol,
                client_order_id,
            )
            .map(OrderLookup::Found)
            .map_err(|error| lookup_error(&format!("invalid Aster order snapshot: {error}")));
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
impl<T, S, N> OrderCancellationGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        if exchange != Exchange::Aster {
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
            .signed_request(HttpMethod::Delete, "/fapi/v3/order", params)
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
                    "Aster cancellation acknowledgement is not authoritative: {error}"
                ),
            });
        }
        let error = parse_exchange_error(&response.body);
        Err(CancellationError::Unknown {
            message: error.message,
        })
    }
}

fn aster_eip712_digest(message: &str) -> [u8; 32] {
    let mut domain_words = Vec::with_capacity(32 * 5);
    domain_words.extend_from_slice(&keccak256(EIP712_DOMAIN_TYPE.as_bytes()));
    domain_words.extend_from_slice(&keccak256(b"AsterSignTransaction"));
    domain_words.extend_from_slice(&keccak256(b"1"));
    let mut chain_id_word = [0_u8; 32];
    chain_id_word[24..].copy_from_slice(&ASTER_CHAIN_ID.to_be_bytes());
    domain_words.extend_from_slice(&chain_id_word);
    domain_words.extend_from_slice(&[0_u8; 32]);
    let domain_separator = keccak256(&domain_words);

    let mut message_words = Vec::with_capacity(64);
    message_words.extend_from_slice(&keccak256(ASTER_MESSAGE_TYPE.as_bytes()));
    message_words.extend_from_slice(&keccak256(message.as_bytes()));
    let message_hash = keccak256(&message_words);

    let mut envelope = Vec::with_capacity(66);
    envelope.extend_from_slice(&[0x19, 0x01]);
    envelope.extend_from_slice(&domain_separator);
    envelope.extend_from_slice(&message_hash);
    keccak256(&envelope)
}

fn keccak256(bytes: &[u8]) -> [u8; 32] {
    let digest = Keccak256::digest(bytes);
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn ethereum_address(verifying_key: &k256::ecdsa::VerifyingKey) -> String {
    let encoded = verifying_key.to_encoded_point(false);
    let hash = keccak256(&encoded.as_bytes()[1..]);
    checksum_address(&hex::encode(&hash[12..]))
}

fn checksum_address(lowercase_hex: &str) -> String {
    let normalized = lowercase_hex.to_ascii_lowercase();
    let hash = keccak256(normalized.as_bytes());
    let mut output = String::with_capacity(42);
    output.push_str("0x");
    for (index, character) in normalized.chars().enumerate() {
        let byte = hash[index / 2];
        let nibble = if index % 2 == 0 {
            byte >> 4
        } else {
            byte & 0x0f
        };
        if character.is_ascii_alphabetic() && nibble >= 8 {
            output.push(character.to_ascii_uppercase());
        } else {
            output.push(character);
        }
    }
    output
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

fn invalid_cancellation(message: &str) -> CancellationError {
    CancellationError::Invalid {
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
    struct FixedNonce(u64);

    impl NonceSource for FixedNonce {
        fn next_nonce(&self) -> u64 {
            self.0
        }
    }

    #[derive(Clone)]
    struct RecordingSigner {
        address: String,
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingSigner {
        fn new(address: &str) -> Self {
            Self {
                address: address.into(),
                messages: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl AsterMessageSigner for RecordingSigner {
        fn signer_address(&self) -> &str {
            &self.address
        }

        fn sign_eip712_message(&self, message: &str) -> Result<String, AsterSignatureError> {
            self.messages.lock().unwrap().push(message.into());
            Ok("0xfixed-signature".into())
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
            client_order_id: ClientOrderId::parse("g_0_B_fixed").unwrap(),
            exchange: Exchange::Aster,
            shape: OrderShape {
                symbol: "ANSEMUSDT".into(),
                side: OrderSide::Buy,
                price: Some(Decimal::new(38, 2)),
                quantity: Decimal::new(100, 0),
                reduce_only: true,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            state: IntentState::Prepared,
            created_at_ms: 10,
            updated_at_ms: 10,
        }
    }

    fn adapter(
        transport: MockTransport,
        signer: RecordingSigner,
    ) -> AsterAdapter<MockTransport, RecordingSigner, FixedNonce> {
        AsterAdapter::with_base_url(
            transport,
            signer,
            FixedNonce(1_700_000_000_123_456),
            "0x1111111111111111111111111111111111111111",
            "https://example.test",
        )
    }

    #[test]
    fn local_eip712_signer_matches_python_eth_account_vector() {
        let signer = LocalEip712Signer::from_private_key(&format!("0x{}", "1".repeat(64))).unwrap();
        let message = concat!(
            "symbol=ANSEMUSDT&side=BUY&type=LIMIT&quantity=100&reduceOnly=true&",
            "price=0.38&timeInForce=GTC&newClientOrderId=g_0_B_fixed&",
            "nonce=1700000000123456&user=0x1111111111111111111111111111111111111111&",
            "signer=0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
        );

        assert_eq!(
            signer.signer_address(),
            "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
        );
        assert_eq!(
            signer.sign_eip712_message(message).unwrap(),
            concat!(
                "4acb1974f5e9ff174eca9fa40955d1fceae0185cf2bcef9914293cb5fd042018",
                "1c7689ea0382b6868ae5548ae1007cb05a7632b66fa885d12dba7275246231361c"
            )
        );
        assert!(!format!("{signer:?}").contains(&"1".repeat(64)));
    }

    #[test]
    fn one_wallet_constructor_uses_derived_address_for_user_and_signer() {
        let adapter = AsterAdapter::production_wallet(
            MockTransport::default(),
            FixedNonce(1),
            &format!("0x{}", "1".repeat(64)),
        )
        .unwrap();

        assert_eq!(adapter.user_address, adapter.signer.signer_address());
        assert_eq!(adapter.base_url, PRODUCTION_BASE_URL);
    }

    #[test]
    fn malformed_private_key_is_rejected_before_any_request_exists() {
        assert!(matches!(
            LocalEip712Signer::from_private_key("not-a-private-key"),
            Err(AsterSignatureError::InvalidPrivateKey)
        ));
    }

    #[tokio::test]
    async fn placement_signs_canonical_v3_message_and_posts_exact_body() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":4770039,"clientOrderId":"g_0_B_fixed"}"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let acknowledgement = adapter(transport.clone(), signer.clone())
            .place_order(&intent())
            .await
            .unwrap();
        let request = transport.request();

        let expected_message = concat!(
            "symbol=ANSEMUSDT&side=BUY&type=LIMIT&quantity=100&reduceOnly=true&",
            "price=0.38&timeInForce=GTC&newClientOrderId=g_0_B_fixed&",
            "nonce=1700000000123456&user=0x1111111111111111111111111111111111111111&",
            "signer=0x2222222222222222222222222222222222222222"
        );
        assert_eq!(
            signer.messages.lock().unwrap().as_slice(),
            [expected_message]
        );
        assert_eq!(acknowledgement.exchange_order_id, "4770039");
        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(request.path, "/fapi/v3/order");
        assert!(request.query.is_empty());
        assert_eq!(
            request.body_string(),
            format!("{expected_message}&signature=0xfixed-signature")
        );
    }

    #[tokio::test]
    async fn transport_failure_is_unknown_and_request_is_never_blindly_retried() {
        let transport =
            MockTransport::with_response(Err(TransportError::Connection("reset".into())));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let result = adapter(transport.clone(), signer)
            .place_order(&intent())
            .await;

        assert!(matches!(result, Err(PlacementError::Unknown { .. })));
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn non_authoritative_success_is_unknown() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"orderId":4770039,"clientOrderId":"g_0_B_other"}"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        assert!(matches!(
            adapter(transport, signer).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn cancellation_uses_signed_v3_delete_and_strict_identity_acknowledgement() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "orderId":4770039,"clientOrderId":"g_0_B_fixed","status":"CANCELED"
            }"#
            .into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let acknowledgement = adapter(transport.clone(), signer.clone())
            .cancel_order(
                Exchange::Aster,
                "ANSEMUSDT",
                &ClientOrderId::parse("g_0_B_fixed").unwrap(),
                "4770039",
            )
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(acknowledgement.exchange_order_id, "4770039");
        assert_eq!(request.method, HttpMethod::Delete);
        assert_eq!(request.path, "/fapi/v3/order");
        assert!(request.query.is_empty());
        assert!(
            request
                .body_string()
                .starts_with("symbol=ANSEMUSDT&orderId=4770039&nonce=1700000000123456&user=")
        );
        assert_eq!(signer.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lookup_is_signed_and_preserves_exchange_accepted_quantity() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "symbol":"ANSEMUSDT","orderId":4770039,"clientOrderId":"g_0_B_fixed",
                "side":"BUY","price":"0.3800000","origQty":"70","status":"NEW",
                "reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"
            }"#
            .into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let result = adapter(transport.clone(), signer)
            .lookup_order_by_client_id(
                Exchange::Aster,
                "ANSEMUSDT",
                &ClientOrderId::parse("g_0_B_fixed").unwrap(),
            )
            .await
            .unwrap();
        let OrderLookup::Found(order) = result else {
            panic!("order should exist")
        };

        assert_eq!(order.shape.quantity, Decimal::new(70, 0));
        assert_eq!(
            order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        );
        assert!(
            transport.request().query_string().starts_with(
                "symbol=ANSEMUSDT&origClientOrderId=g_0_B_fixed&nonce=1700000000123456"
            )
        );
    }

    #[tokio::test]
    async fn lookup_not_found_requires_definitive_exchange_code() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 400,
            body: r#"{"code":-2013,"msg":"Order does not exist."}"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let result = adapter(transport, signer)
            .lookup_order_by_client_id(
                Exchange::Aster,
                "ANSEMUSDT",
                &ClientOrderId::parse("g_0_B_fixed").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result, OrderLookup::NotFound);
    }
}
