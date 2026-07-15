use async_trait::async_trait;
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent},
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, CancellationAcknowledgement,
        CancellationError, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderCancellationGateway,
        OrderExecutionSnapshot, OrderLookup, OrderLookupGateway, OrderPlacementGateway,
        PlacementAcknowledgement, PlacementError, PositionSnapshot, PositionSnapshotGateway,
        SnapshotError, TradingFeeRateGateway, TradingFeeRates,
        codec::{
            build_order_parameters, execution_status_is_unknown, order_is_definitively_absent,
            parse_account_balance_snapshot, parse_authoritative_order,
            parse_cancellation_acknowledgement, parse_exchange_error, parse_instrument_rules,
            parse_leverage_acknowledgement, parse_market_snapshot, parse_open_orders,
            parse_placement_acknowledgement, parse_position_snapshot, parse_trading_fee_rates,
            validate_snapshot_request,
        },
        execution::{
            CommissionConvention, assemble_execution_snapshot, numeric_trade_id,
            parse_historical_minute_open, parse_order_execution_header, parse_trade_page,
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
const TRADE_PAGE_LIMIT: usize = 1_000;
const MAX_TRADE_HISTORY_QUERIES: usize = 64;
const TRADE_PROBE_PADDING_MS: u64 = 5 * 60 * 1_000;
const TRADE_WINDOW_LIMIT_MS: u64 = (7 * 24 * 60 * 60 * 1_000) - 1;

pub trait AsterMessageSigner: Send + Sync {
    fn signer_address(&self) -> &str;
    fn sign_eip712_message(&self, message: &str) -> Result<String, AsterSignatureError>;
}

#[async_trait]
impl<T, S, N> LeverageGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        if exchange != Exchange::Aster {
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
                "/fapi/v3/leverage",
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
                Exchange::Aster,
                &symbol,
                leverage,
            )
            .map_err(|error| LeverageError::Unknown {
                message: format!("Aster leverage acknowledgement is invalid: {error}"),
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
impl<T, S, N> TradingFeeRateGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v3/commissionRate",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Aster trading fee rates")
            .await?;
        parse_trading_fee_rates(&body, Exchange::Aster, &symbol)
            .map_err(|error| SnapshotError::new(format!("invalid Aster fee rates: {error}")))
    }
}

#[async_trait]
impl<T, S, N> AccountBalanceSnapshotGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        if exchange != Exchange::Aster {
            return Err(SnapshotError::new(
                "account balance belongs to another exchange",
            ));
        }
        let request = self
            .signed_request(HttpMethod::Get, "/fapi/v3/account", vec![])
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Aster account balance snapshot")
            .await?;
        parse_account_balance_snapshot(&body, Exchange::Aster).map_err(|error| {
            SnapshotError::new(format!("invalid Aster account balance snapshot: {error}"))
        })
    }
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
            raw_body: None,
            headers: vec![(
                "Content-Type".into(),
                "application/x-www-form-urlencoded".into(),
            )],
        })
    }
}

impl<T, S, N> AsterAdapter<T, S, N>
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
impl<T, S, N> MarketSnapshotGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: Send + Sync,
    N: Send + Sync,
{
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let ticker = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v3/ticker/24hr",
                    vec![("symbol".into(), symbol.clone())],
                ),
                "Aster ticker snapshot",
            )
            .await?;
        let premium = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v3/premiumIndex",
                    vec![("symbol".into(), symbol.clone())],
                ),
                "Aster mark-price snapshot",
            )
            .await?;
        parse_market_snapshot(&ticker, &premium, Exchange::Aster, &symbol)
            .map_err(|error| SnapshotError::new(format!("invalid Aster market snapshot: {error}")))
    }
}

#[async_trait]
impl<T, S, N> HistoricalPriceGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: Send + Sync,
    N: Send + Sync,
{
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        if minute_start_ms == 0 || !minute_start_ms.is_multiple_of(60_000) {
            return Err(SnapshotError::new(
                "historical price minute must be a positive UTC minute boundary",
            ));
        }
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request(
                    "/fapi/v3/klines",
                    vec![
                        ("symbol".into(), symbol.clone()),
                        ("interval".into(), "1m".into()),
                        ("startTime".into(), minute_start_ms.to_string()),
                        ("limit".into(), "1".into()),
                    ],
                ),
                "Aster historical fee-price snapshot",
            )
            .await?;
        parse_historical_minute_open(&body, Exchange::Aster, &symbol, minute_start_ms)
            .map_err(|error| SnapshotError::new(format!("invalid Aster minute price: {error}")))
    }
}

#[async_trait]
impl<T, S, N> InstrumentRulesGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: Send + Sync,
    N: Send + Sync,
{
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<crate::domain::InstrumentRules, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request("/fapi/v3/exchangeInfo", vec![]),
                "Aster instrument snapshot",
            )
            .await?;
        parse_instrument_rules(&body, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Aster instrument snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, N> PositionSnapshotGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v3/positionRisk",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Aster position snapshot")
            .await?;
        parse_position_snapshot(&body, Exchange::Aster, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Aster position snapshot: {error}"))
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
impl<T, S, N> OpenOrderSnapshotGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<crate::exchange::AuthoritativeOrder>, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v3/openOrders",
                vec![("symbol".into(), symbol.clone())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Aster open-order snapshot")
            .await?;
        parse_open_orders(&body, Exchange::Aster, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Aster open-order snapshot: {error}"))
        })
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

#[async_trait]
impl<T, S, N> ExecutionSnapshotGateway for AsterAdapter<T, S, N>
where
    T: HttpTransport,
    S: AsterMessageSigner,
    N: NonceSource,
{
    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        validate_snapshot_request(exchange, Exchange::Aster, symbol)
            .map_err(|error| execution_error(error.to_string()))?;
        if exchange_order_id.trim().is_empty() {
            return Err(execution_error("exchange order ID is required"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let detail_request = self
            .signed_request(
                HttpMethod::Get,
                "/fapi/v3/order",
                vec![
                    ("symbol".into(), symbol.clone()),
                    ("origClientOrderId".into(), client_order_id.as_str().into()),
                ],
            )
            .map_err(|error| execution_error(error.to_string()))?;
        let detail_body = self
            .execute_snapshot(detail_request, "Aster execution order snapshot")
            .await
            .map_err(|error| execution_error(error.to_string()))?;
        let header = parse_order_execution_header(
            &detail_body,
            Exchange::Aster,
            &symbol,
            client_order_id,
            exchange_order_id,
        )
        .map_err(|error| execution_error(format!("invalid Aster order totals: {error}")))?;
        if header.cumulative_quantity.is_zero() {
            return assemble_execution_snapshot(header, vec![])
                .map_err(|error| execution_error(error.to_string()));
        }

        let final_end = header
            .update_time_ms
            .checked_add(TRADE_PROBE_PADDING_MS)
            .ok_or_else(|| execution_error("Aster execution time range overflowed"))?;
        let mut window_start = header.order_time_ms.saturating_sub(TRADE_PROBE_PADDING_MS);
        let mut query_count = 0usize;
        let mut trades = Vec::new();
        while window_start <= final_end {
            let window_end = window_start
                .saturating_add(TRADE_WINDOW_LIMIT_MS)
                .min(final_end);
            query_count = query_count
                .checked_add(1)
                .ok_or_else(|| execution_error("Aster trade query count overflowed"))?;
            if query_count > MAX_TRADE_HISTORY_QUERIES {
                return Err(execution_error(
                    "Aster trade history requires too many bounded queries",
                ));
            }
            let request = self
                .signed_request(
                    HttpMethod::Get,
                    "/fapi/v3/userTrades",
                    vec![
                        ("symbol".into(), symbol.clone()),
                        ("startTime".into(), window_start.to_string()),
                        ("endTime".into(), window_end.to_string()),
                        ("limit".into(), TRADE_PAGE_LIMIT.to_string()),
                    ],
                )
                .map_err(|error| execution_error(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Aster account trade snapshot")
                .await
                .map_err(|error| execution_error(error.to_string()))?;
            let mut page = parse_trade_page(
                &body,
                &symbol,
                CommissionConvention::SignedBalanceDeltaOrPositiveCost,
            )
            .map_err(|error| execution_error(format!("invalid Aster trade page: {error}")))?;

            let mut minimum_trade_id = None;
            loop {
                if page.len() > TRADE_PAGE_LIMIT {
                    return Err(execution_error(
                        "Aster trade page exceeds the requested limit",
                    ));
                }
                let page_trade_ids = page
                    .iter()
                    .map(numeric_trade_id)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        execution_error(format!("invalid Aster numeric trade ID: {error}"))
                    })?;
                if minimum_trade_id.is_some_and(|minimum| {
                    page_trade_ids.iter().any(|trade_id| *trade_id < minimum)
                }) {
                    return Err(execution_error("Aster trade pagination moved backwards"));
                }
                if page.len() == TRADE_PAGE_LIMIT
                    && (page_trade_ids.windows(2).any(|pair| pair[0] >= pair[1])
                        || page
                            .windows(2)
                            .any(|pair| pair[0].trade_time_ms > pair[1].trade_time_ms))
                {
                    return Err(execution_error(
                        "Aster full trade page is not strictly ordered",
                    ));
                }
                let reached_window_end = page.iter().any(|trade| trade.trade_time_ms > window_end);
                trades.extend(
                    page.iter()
                        .filter(|trade| {
                            trade.trade_time_ms >= window_start
                                && trade.trade_time_ms <= window_end
                                && trade.exchange_order_id == exchange_order_id
                        })
                        .cloned(),
                );
                if page.len() < TRADE_PAGE_LIMIT || reached_window_end {
                    break;
                }
                let next_from_id = page_trade_ids
                    .last()
                    .and_then(|trade_id| trade_id.checked_add(1))
                    .ok_or_else(|| execution_error("Aster trade pagination cannot advance"))?;
                query_count = query_count
                    .checked_add(1)
                    .ok_or_else(|| execution_error("Aster trade query count overflowed"))?;
                if query_count > MAX_TRADE_HISTORY_QUERIES {
                    return Err(execution_error(
                        "Aster trade history requires too many bounded queries",
                    ));
                }
                let request = self
                    .signed_request(
                        HttpMethod::Get,
                        "/fapi/v3/userTrades",
                        vec![
                            ("symbol".into(), symbol.clone()),
                            ("fromId".into(), next_from_id.to_string()),
                            ("limit".into(), TRADE_PAGE_LIMIT.to_string()),
                        ],
                    )
                    .map_err(|error| execution_error(error.to_string()))?;
                let body = self
                    .execute_snapshot(request, "Aster paginated account trade snapshot")
                    .await
                    .map_err(|error| execution_error(error.to_string()))?;
                page = parse_trade_page(
                    &body,
                    &symbol,
                    CommissionConvention::SignedBalanceDeltaOrPositiveCost,
                )
                .map_err(|error| {
                    execution_error(format!("invalid Aster paginated trade page: {error}"))
                })?;
                minimum_trade_id = Some(next_from_id);
            }
            if window_end == u64::MAX {
                break;
            }
            window_start = window_end + 1;
        }
        assemble_execution_snapshot(header, trades)
            .map_err(|error| execution_error(format!("incomplete Aster execution: {error}")))
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

    fn execution_order_detail(
        executed: &str,
        quote: &str,
        status: &str,
        order_time: u64,
        update_time: u64,
    ) -> String {
        format!(
            r#"{{"symbol":"ANSEMUSDT","orderId":4770039,"clientOrderId":"g_0_B_fixed","side":"BUY","price":"0.38","origQty":"100","executedQty":"{executed}","cumQuote":"{quote}","status":"{status}","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT","time":{order_time},"updateTime":{update_time}}}"#
        )
    }

    fn aster_trade(trade_id: u64, quantity: &str, quote: &str, time: u64) -> String {
        format!(
            r#"{{"symbol":"ANSEMUSDT","id":{trade_id},"orderId":4770039,"side":"BUY","buyer":true,"price":"0.38","qty":"{quantity}","quoteQty":"{quote}","commission":"-0.001","commissionAsset":"USDT","realizedPnl":"0","maker":true,"time":{time}}}"#
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

    #[tokio::test]
    async fn open_order_snapshot_uses_signed_v3_query_and_preserves_exact_shape() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[
                {"symbol":"ANSEMUSDT","orderId":4770039,"clientOrderId":"g_RUN00001_1_B_1","side":"BUY","price":"0.38","origQty":"70","status":"NEW","reduceOnly":true,"timeInForce":"GTC","type":"LIMIT"}
            ]"#
                .into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");

        let orders = adapter(transport.clone(), signer.clone())
            .open_orders_snapshot(Exchange::Aster, "ANSEMUSDT")
            .await
            .unwrap();
        let request = transport.request();
        let signed_message = signer.messages.lock().unwrap()[0].clone();

        assert_eq!(request.path, "/fapi/v3/openOrders");
        assert_eq!(request.method, HttpMethod::Get);
        assert!(request.body.is_empty());
        assert!(signed_message.starts_with("symbol=ANSEMUSDT&nonce="));
        assert!(signed_message.contains("&user=0x1111111111111111111111111111111111111111"));
        assert!(signed_message.contains("&signer=0x2222222222222222222222222222222222222222"));
        assert_eq!(orders[0].shape.quantity, Decimal::new(70, 0));
        assert!(orders[0].shape.reduce_only);
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
    async fn market_and_instrument_snapshots_use_public_v3_endpoints() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"ANSEMUSDT","lastPrice":"0.381"}"#.into(),
            }),
            Ok(HttpResponse {
                status: 200,
                body: r#"{"symbol":"ANSEMUSDT","markPrice":"0.3809","time":1700000000000}"#.into(),
            }),
            Ok(HttpResponse {
                status: 200,
                body: r#"{
                    "symbols":[{"symbol":"ANSEMUSDT","status":"TRADING","filters":[
                        {"filterType":"PRICE_FILTER","tickSize":"0.0001"},
                        {"filterType":"LOT_SIZE","stepSize":"1","minQty":"1","maxQty":"100000"},
                        {"filterType":"MIN_NOTIONAL","notional":"5"}
                    ]}]
                }"#
                .into(),
            }),
        ]);
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let adapter = adapter(transport.clone(), signer);

        let market = adapter
            .market_snapshot(Exchange::Aster, "ANSEMUSDT")
            .await
            .unwrap();
        let rules = adapter
            .instrument_rules(Exchange::Aster, "ANSEMUSDT")
            .await
            .unwrap();
        let requests = transport.all_requests();

        assert_eq!(market.mark_price, Decimal::new(3809, 4));
        assert_eq!(rules.tick_size, Decimal::new(1, 4));
        assert_eq!(rules.market_quantity, rules.limit_quantity);
        assert_eq!(requests[0].path, "/fapi/v3/ticker/24hr");
        assert_eq!(requests[1].path, "/fapi/v3/premiumIndex");
        assert_eq!(requests[2].path, "/fapi/v3/exchangeInfo");
        assert!(requests.iter().all(|request| request.headers.is_empty()));
    }

    #[tokio::test]
    async fn signed_hedge_position_snapshot_is_rejected_as_one_way_baseline() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[
                {"symbol":"ANSEMUSDT","positionSide":"LONG","positionAmt":"200","entryPrice":"0.38","markPrice":"0.381","unRealizedProfit":"0.2"},
                {"symbol":"ANSEMUSDT","positionSide":"SHORT","positionAmt":"-100","entryPrice":"0.39","markPrice":"0.381","unRealizedProfit":"0.9"}
            ]"#
            .into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let snapshot = adapter(transport.clone(), signer)
            .position_snapshot(Exchange::Aster, "ANSEMUSDT")
            .await
            .unwrap();

        assert!(snapshot.one_way_position().is_err());
        let request = transport.request();
        assert_eq!(request.path, "/fapi/v3/positionRisk");
        assert!(request.query_string().contains("signature="));
    }

    #[tokio::test]
    async fn historical_fee_price_uses_one_exact_public_v3_minute_candle() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"[[1020000,"602.25","603","601","602","100"]]"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let price = adapter(transport.clone(), signer)
            .historical_minute_open(Exchange::Aster, "BNBUSDT", 1_020_000)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(price.open_price, Decimal::new(60225, 2));
        assert_eq!(request.path, "/fapi/v3/klines");
        assert_eq!(
            request.query_string(),
            "symbol=BNBUSDT&interval=1m&startTime=1020000&limit=1"
        );
        assert!(request.headers.is_empty());
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
    async fn leverage_change_uses_exact_signed_v3_body_and_acknowledgement() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"symbol":"ANSEMUSDT","leverage":5,"maxNotionalValue":"100000"}"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let acknowledgement = adapter(transport.clone(), signer.clone())
            .set_leverage(Exchange::Aster, "ansemusdt", 5)
            .await
            .unwrap();
        let request = transport.request();
        let expected_message = concat!(
            "symbol=ANSEMUSDT&leverage=5&nonce=1700000000123456&",
            "user=0x1111111111111111111111111111111111111111&",
            "signer=0x2222222222222222222222222222222222222222"
        );

        assert_eq!(acknowledgement.leverage, 5);
        assert_eq!(request.path, "/fapi/v3/leverage");
        assert_eq!(
            request.body_string(),
            format!("{expected_message}&signature=0xfixed-signature")
        );
        assert_eq!(
            signer.messages.lock().unwrap().as_slice(),
            [expected_message]
        );
    }

    #[tokio::test]
    async fn fee_rate_query_uses_signed_v3_identity_and_exact_account_rates() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{"symbol":"ANSEMUSDT","makerCommissionRate":"0.0002","takerCommissionRate":"0.0004"}"#.into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let rates = adapter(transport.clone(), signer.clone())
            .trading_fee_rates(Exchange::Aster, "ansemusdt")
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(rates.maker_rate, Decimal::new(2, 4));
        assert_eq!(rates.taker_rate, Decimal::new(4, 4));
        assert_eq!(request.path, "/fapi/v3/commissionRate");
        assert!(request.query_string().starts_with(
            "symbol=ANSEMUSDT&nonce=1700000000123456&user=0x1111111111111111111111111111111111111111&signer="
        ));
        assert_eq!(signer.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn account_balance_query_uses_signed_v3_account_totals() {
        let transport = MockTransport::with_response(Ok(HttpResponse {
            status: 200,
            body: r#"{
                "totalWalletBalance":"23.724692060",
                "totalUnrealizedProfit":"0.00400000",
                "totalMarginBalance":"23.728692060",
                "availableBalance":"20.10000000",
                "assets":[],"positions":[]
            }"#
            .into(),
        }));
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");

        let snapshot = adapter(transport.clone(), signer.clone())
            .account_balance_snapshot(Exchange::Aster)
            .await
            .unwrap();
        let request = transport.request();

        assert_eq!(snapshot.available_balance.to_string(), "20.10000000");
        assert_eq!(snapshot.equity.to_string(), "23.728692060");
        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(request.path, "/fapi/v3/account");
        assert!(request.query_string().starts_with(
            "nonce=1700000000123456&user=0x1111111111111111111111111111111111111111&signer="
        ));
        assert_eq!(request.body, Vec::<(String, String)>::new());
        assert_eq!(signer.messages.lock().unwrap().len(), 1);
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
            let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");

            assert!(matches!(
                adapter(transport, signer).place_order(&intent()).await,
                Err(PlacementError::Unknown { .. })
            ));
        }
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

    #[tokio::test]
    async fn aster_execution_snapshot_uses_order_bounded_trade_windows() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("100", "38", "FILLED", 1_000_000, 1_100_000),
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", aster_trade(7, "100", "38", 1_050_000)),
            }),
        ]);
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let snapshot = adapter(transport.clone(), signer)
            .execution_snapshot(
                Exchange::Aster,
                "ANSEMUSDT",
                &ClientOrderId::parse("g_0_B_fixed").unwrap(),
                "4770039",
            )
            .await
            .unwrap();
        let requests = transport.all_requests();

        assert_eq!(snapshot.cumulative_quantity, Decimal::new(100, 0));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::new(1, 3));
        assert_eq!(snapshot.trades[0].raw_commission, Decimal::new(-1, 3));
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/fapi/v3/order");
        assert_eq!(requests[1].path, "/fapi/v3/userTrades");
        assert!(requests[1].query_string().contains("startTime=700000"));
        assert!(requests[1].query_string().contains("endTime=1400000"));
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
                    body: execution_order_detail("70", "26.6", status, 1_000_000, 1_100_000),
                }),
                Ok(HttpResponse {
                    status: 200,
                    body: format!("[{}]", aster_trade(7, "70", "26.6", 1_050_000)),
                }),
            ]);
            let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");

            let snapshot = adapter(transport, signer)
                .execution_snapshot(
                    Exchange::Aster,
                    "ANSEMUSDT",
                    &ClientOrderId::parse("g_0_B_fixed").unwrap(),
                    "4770039",
                )
                .await
                .unwrap();

            assert_eq!(
                snapshot.order.lifecycle,
                OrderLifecycle::Terminal(expected_terminal)
            );
            assert_eq!(snapshot.cumulative_quantity, Decimal::new(70, 0));
            assert_eq!(
                snapshot.order.shape.quantity - snapshot.cumulative_quantity,
                Decimal::new(30, 0)
            );
            assert_eq!(snapshot.trades.len(), 1);
            assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::new(1, 3));
        }
    }

    #[tokio::test]
    async fn long_lived_aster_order_is_reconciled_across_seven_day_windows() {
        let order_time = 1_000_000;
        let update_time = order_time + TRADE_WINDOW_LIMIT_MS + 1_000;
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("100", "38", "FILLED", order_time, update_time),
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", aster_trade(7, "40", "15.2", order_time)),
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", aster_trade(8, "60", "22.8", update_time)),
            }),
        ]);
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");
        let snapshot = adapter(transport.clone(), signer)
            .execution_snapshot(
                Exchange::Aster,
                "ANSEMUSDT",
                &ClientOrderId::parse("g_0_B_fixed").unwrap(),
                "4770039",
            )
            .await
            .unwrap();
        let trade_requests = transport
            .all_requests()
            .into_iter()
            .filter(|request| request.path == "/fapi/v3/userTrades")
            .collect::<Vec<_>>();

        assert_eq!(snapshot.trades.len(), 2);
        assert_eq!(snapshot.cumulative_quote, Decimal::new(38, 0));
        assert_eq!(trade_requests.len(), 2);
        assert!(
            trade_requests
                .iter()
                .all(|request| !request.query_string().contains("fromId="))
        );
    }

    #[tokio::test]
    async fn incomplete_aster_trade_history_never_becomes_an_execution() {
        let transport = MockTransport::default();
        transport.responses.lock().unwrap().extend([
            Ok(HttpResponse {
                status: 200,
                body: execution_order_detail("100", "38", "FILLED", 1_000_000, 1_100_000),
            }),
            Ok(HttpResponse {
                status: 200,
                body: format!("[{}]", aster_trade(7, "70", "26.6", 1_050_000)),
            }),
        ]);
        let signer = RecordingSigner::new("0x2222222222222222222222222222222222222222");

        assert!(
            adapter(transport, signer)
                .execution_snapshot(
                    Exchange::Aster,
                    "ANSEMUSDT",
                    &ClientOrderId::parse("g_0_B_fixed").unwrap(),
                    "4770039",
                )
                .await
                .is_err()
        );
    }
}
