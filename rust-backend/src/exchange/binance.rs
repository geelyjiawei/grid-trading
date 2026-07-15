use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent},
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, CancellationAcknowledgement,
        CancellationError, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalOrder, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderCancellationGateway,
        OrderExecutionSnapshot, OrderHistorySnapshotGateway, OrderLookup, OrderLookupGateway,
        OrderPlacementGateway, PlacementAcknowledgement, PlacementError, PositionSnapshot,
        PositionSnapshotGateway, SnapshotError, TradingFeeRateGateway, TradingFeeRates,
        codec::{
            build_order_parameters, execution_status_is_unknown, order_is_definitively_absent,
            parse_account_balance_snapshot, parse_authoritative_order,
            parse_cancellation_acknowledgement, parse_exchange_error, parse_instrument_rules,
            parse_leverage_acknowledgement, parse_market_snapshot,
            parse_open_order_execution_progress, parse_open_orders, parse_order_history,
            parse_placement_acknowledgement, parse_position_snapshot, parse_trading_fee_rates,
            validate_snapshot_request,
        },
        execution::{
            CommissionConvention, assemble_execution_snapshot, numeric_trade_id,
            parse_historical_minute_open, parse_order_execution_header, parse_trade_page,
        },
        protocol::{
            HttpMethod, HttpTransport, MillisecondClock, Parameters, PreparedHttpRequest,
            encode_parameters,
        },
    },
};

const PRODUCTION_BASE_URL: &str = "https://fapi.binance.com";
const TESTNET_BASE_URL: &str = "https://testnet.binancefuture.com";
const TRADE_PAGE_LIMIT: usize = 1_000;
const MAX_TRADE_PAGES: usize = 64;

pub trait BinanceRequestSigner: Send + Sync {
    fn sign(&self, message: &str) -> Result<String, SignatureError>;
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
        parse_position_snapshot(&body, Exchange::Binance, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Binance position snapshot: {error}"))
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
            return assemble_execution_snapshot(header, vec![])
                .map_err(|error| execution_error(error.to_string()));
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
        assemble_execution_snapshot(header, trades)
            .map_err(|error| execution_error(format!("incomplete Binance execution: {error}")))
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
