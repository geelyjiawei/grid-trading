use std::collections::BTreeSet;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, OrderIntent, OrderKind, OrderSide, TimeInForce},
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, CancellationAcknowledgement,
        CancellationError, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalOrder, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderCancellationGateway,
        OrderExecutionSnapshot, OrderHistorySnapshotGateway, OrderLookup, OrderLookupGateway,
        OrderPlacementGateway, PlacementAcknowledgement, PlacementError, PositionSnapshot,
        PositionSnapshotGateway, SnapshotError, TradingFeeRateGateway, TradingFeeRates,
        bybit_codec::{
            parse_account_balance_snapshot, parse_cancellation_acknowledgement, parse_error,
            parse_exact_order_record, parse_execution_page, parse_historical_minute_open,
            parse_instrument_rules, parse_leverage_acknowledgement, parse_market_snapshot,
            parse_open_order_page, parse_order_history_page, parse_placement_acknowledgement,
            parse_position_snapshot, parse_trading_fee_rates,
        },
        codec::validate_snapshot_request,
        execution::assemble_execution_snapshot,
        protocol::{
            HttpMethod, HttpResponse, HttpTransport, MillisecondClock, Parameters,
            PreparedHttpRequest, encode_parameters,
        },
    },
};

const PRODUCTION_BASE_URL: &str = "https://api.bybit.com";
const TESTNET_BASE_URL: &str = "https://api-testnet.bybit.com";
const CATEGORY: &str = "linear";
const EXECUTION_PAGE_LIMIT: usize = 100;
const MAX_EXECUTION_PAGES: usize = 100;
const OPEN_ORDER_PAGE_LIMIT: usize = 50;
const MAX_OPEN_ORDER_PAGES: usize = 100;
const ORDER_HISTORY_PAGE_LIMIT: usize = 50;
const MAX_ORDER_HISTORY_PAGES: usize = 20;

pub trait BybitRequestSigner: Send + Sync {
    fn sign(&self, message: &str) -> Result<String, BybitSignatureError>;
}

#[derive(Clone)]
pub struct BybitHmacSha256Signer {
    secret: Zeroizing<Vec<u8>>,
}

impl BybitHmacSha256Signer {
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, BybitSignatureError> {
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(BybitSignatureError::MissingSecret);
        }
        Ok(Self {
            secret: Zeroizing::new(secret.to_vec()),
        })
    }
}

impl std::fmt::Debug for BybitHmacSha256Signer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BybitHmacSha256Signer")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl BybitRequestSigner for BybitHmacSha256Signer {
    fn sign(&self, message: &str) -> Result<String, BybitSignatureError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_slice())
            .map_err(|_| BybitSignatureError::InvalidSecret)?;
        mac.update(message.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BybitSignatureError {
    #[error("Bybit API key is required")]
    MissingApiKey,
    #[error("Bybit API secret is required")]
    MissingSecret,
    #[error("Bybit API secret cannot initialize HMAC-SHA256")]
    InvalidSecret,
    #[error("Bybit receive window must be positive")]
    InvalidRecvWindow,
    #[error("Bybit JSON request serialization failed: {0}")]
    InvalidJson(String),
}

pub struct BybitAdapter<T, S, C> {
    transport: T,
    signer: S,
    clock: C,
    api_key: Zeroizing<String>,
    base_url: String,
    recv_window_ms: u64,
}

impl<T, S, C> BybitAdapter<T, S, C> {
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

    fn public_request(&self, path: &str, query: Parameters) -> PreparedHttpRequest {
        PreparedHttpRequest {
            method: HttpMethod::Get,
            base_url: self.base_url.clone(),
            path: path.into(),
            query,
            body: vec![],
            raw_body: None,
            headers: vec![],
        }
    }
}

impl<T, S, C> BybitAdapter<T, S, C>
where
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    fn signed_get(
        &self,
        path: &str,
        query: Parameters,
    ) -> Result<PreparedHttpRequest, BybitSignatureError> {
        let payload = encode_parameters(&query);
        let headers = self.authentication_headers(&payload)?;
        Ok(PreparedHttpRequest {
            method: HttpMethod::Get,
            base_url: self.base_url.clone(),
            path: path.into(),
            query,
            body: vec![],
            raw_body: None,
            headers,
        })
    }

    fn signed_post<P: Serialize>(
        &self,
        path: &str,
        payload: &P,
    ) -> Result<PreparedHttpRequest, BybitSignatureError> {
        let body = serde_json::to_string(payload)
            .map_err(|error| BybitSignatureError::InvalidJson(error.to_string()))?;
        let headers = self.authentication_headers(&body)?;
        Ok(PreparedHttpRequest {
            method: HttpMethod::Post,
            base_url: self.base_url.clone(),
            path: path.into(),
            query: vec![],
            body: vec![],
            raw_body: Some(body),
            headers,
        })
    }

    fn authentication_headers(&self, payload: &str) -> Result<Parameters, BybitSignatureError> {
        if self.api_key.trim().is_empty() {
            return Err(BybitSignatureError::MissingApiKey);
        }
        if self.recv_window_ms == 0 {
            return Err(BybitSignatureError::InvalidRecvWindow);
        }
        let timestamp = self.clock.now_millis().to_string();
        let message = format!(
            "{}{}{}{}",
            timestamp,
            self.api_key.as_str(),
            self.recv_window_ms,
            payload
        );
        let signature = self.signer.sign(&message)?;
        Ok(vec![
            ("X-BAPI-API-KEY".into(), self.api_key.to_string()),
            ("X-BAPI-SIGN".into(), signature),
            ("X-BAPI-SIGN-TYPE".into(), "2".into()),
            ("X-BAPI-TIMESTAMP".into(), timestamp),
            ("X-BAPI-RECV-WINDOW".into(), self.recv_window_ms.to_string()),
            ("Content-Type".into(), "application/json".into()),
        ])
    }
}

impl<T, S, C> BybitAdapter<T, S, C>
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
        let exchange_error = parse_error(&response.body);
        if !(200..300).contains(&response.status) || exchange_error.code.as_deref() != Some("0") {
            return Err(SnapshotError::new(format!(
                "{context}: HTTP {}: {}",
                response.status, exchange_error.message
            )));
        }
        Ok(response.body)
    }

    async fn load_order_record(
        &self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: Option<&str>,
    ) -> Result<Option<crate::exchange::execution::OrderExecutionHeader>, LookupError>
    where
        S: BybitRequestSigner,
        C: MillisecondClock,
    {
        for (path, context) in [
            ("/v5/order/realtime", "Bybit realtime order lookup"),
            ("/v5/order/history", "Bybit historical order lookup"),
        ] {
            let request = self
                .signed_get(
                    path,
                    vec![
                        ("category".into(), CATEGORY.into()),
                        ("symbol".into(), symbol.into()),
                        ("orderLinkId".into(), client_order_id.as_str().into()),
                        ("limit".into(), "1".into()),
                    ],
                )
                .map_err(|error| lookup_error(error.to_string()))?;
            let response = self
                .transport
                .execute(request)
                .await
                .map_err(|error| lookup_error(format!("{context}: {error}")))?;
            let exchange_error = parse_error(&response.body);
            if exchange_error.code.as_deref() == Some("110001") {
                continue;
            }
            if !(200..300).contains(&response.status) || exchange_error.code.as_deref() != Some("0")
            {
                return Err(lookup_error(format!(
                    "{context}: HTTP {}: {}",
                    response.status, exchange_error.message
                )));
            }
            let record = parse_exact_order_record(
                &response.body,
                symbol,
                client_order_id,
                exchange_order_id,
            )
            .map_err(|error| lookup_error(format!("invalid Bybit order snapshot: {error}")))?;
            if record.is_some() {
                return Ok(record);
            }
        }
        Ok(None)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderPayload<'a> {
    category: &'static str,
    symbol: &'a str,
    side: &'static str,
    order_type: &'static str,
    qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<&'static str>,
    position_idx: u8,
    order_link_id: &'a str,
    reduce_only: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderPayload<'a> {
    category: &'static str,
    symbol: &'a str,
    order_id: &'a str,
    order_link_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SetLeveragePayload<'a> {
    category: &'static str,
    symbol: &'a str,
    buy_leverage: String,
    sell_leverage: String,
}

#[async_trait]
impl<T, S, C> LeverageGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        if exchange != Exchange::Bybit {
            return Err(invalid_leverage("request belongs to another exchange"));
        }
        if symbol.trim().is_empty()
            || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || !(1..=125).contains(&leverage)
        {
            return Err(invalid_leverage("symbol or leverage is invalid"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let leverage_text = leverage.to_string();
        let payload = SetLeveragePayload {
            category: CATEGORY,
            symbol: &symbol,
            buy_leverage: leverage_text.clone(),
            sell_leverage: leverage_text,
        };
        let request = self
            .signed_post("/v5/position/set-leverage", &payload)
            .map_err(|error| invalid_leverage(error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| LeverageError::Unknown {
                    message: error.to_string(),
                })?;
        let error = parse_error(&response.body);
        if (200..300).contains(&response.status)
            && matches!(error.code.as_deref(), Some("0" | "110043"))
        {
            return parse_leverage_acknowledgement(&response.body, &symbol, leverage).map_err(
                |codec_error| LeverageError::Unknown {
                    message: format!("Bybit leverage acknowledgement is invalid: {codec_error}"),
                },
            );
        }
        if response.status == 408
            || response.status == 429
            || response.status >= 500
            || error.code.as_deref() == Some("0")
            || error
                .code
                .as_deref()
                .is_none_or(bybit_write_outcome_is_unknown)
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
impl<T, S, C> TradingFeeRateGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let request = self
            .signed_get(
                "/v5/account/fee-rate",
                vec![
                    ("category".into(), CATEGORY.into()),
                    ("symbol".into(), symbol.clone()),
                ],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Bybit trading fee rates")
            .await?;
        parse_trading_fee_rates(&body, &symbol)
            .map_err(|error| SnapshotError::new(format!("invalid Bybit fee rates: {error}")))
    }
}

#[async_trait]
impl<T, S, C> AccountBalanceSnapshotGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        if exchange != Exchange::Bybit {
            return Err(SnapshotError::new(
                "account balance belongs to another exchange",
            ));
        }
        let request = self
            .signed_get(
                "/v5/account/wallet-balance",
                vec![("accountType".into(), "UNIFIED".into())],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Bybit account balance snapshot")
            .await?;
        parse_account_balance_snapshot(&body).map_err(|error| {
            SnapshotError::new(format!("invalid Bybit account balance snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> OrderPlacementGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        if intent.exchange != Exchange::Bybit {
            return Err(definitive_placement(
                "order intent belongs to another exchange",
            ));
        }
        intent
            .validate()
            .map_err(|error| definitive_placement(error.to_string()))?;
        let shape = &intent.shape;
        let payload = PlaceOrderPayload {
            category: CATEGORY,
            symbol: &shape.symbol,
            side: match shape.side {
                OrderSide::Buy => "Buy",
                OrderSide::Sell => "Sell",
            },
            order_type: match shape.kind {
                OrderKind::Limit => "Limit",
                OrderKind::Market => "Market",
            },
            qty: shape.quantity.to_string(),
            price: shape.price.map(|price| price.to_string()),
            time_in_force: (shape.kind == OrderKind::Limit).then_some(match shape.time_in_force {
                TimeInForce::Gtc => "GTC",
                TimeInForce::PostOnly => "PostOnly",
            }),
            position_idx: 0,
            order_link_id: intent.client_order_id.as_str(),
            reduce_only: shape.reduce_only,
        };
        let request = self
            .signed_post("/v5/order/create", &payload)
            .map_err(|error| definitive_placement(error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| PlacementError::Unknown {
                    message: error.to_string(),
                })?;
        classify_placement_response(response, &intent.client_order_id)
    }
}

#[async_trait]
impl<T, S, C> OrderCancellationGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        if exchange != Exchange::Bybit {
            return Err(invalid_cancellation(
                "cancellation belongs to another exchange",
            ));
        }
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)
            .map_err(|error| invalid_cancellation(error.to_string()))?;
        if exchange_order_id.trim().is_empty() {
            return Err(invalid_cancellation("exchange order ID is required"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let payload = CancelOrderPayload {
            category: CATEGORY,
            symbol: &symbol,
            order_id: exchange_order_id,
            order_link_id: client_order_id.as_str(),
        };
        let request = self
            .signed_post("/v5/order/cancel", &payload)
            .map_err(|error| invalid_cancellation(error.to_string()))?;
        let response =
            self.transport
                .execute(request)
                .await
                .map_err(|error| CancellationError::Unknown {
                    message: error.to_string(),
                })?;
        let error = parse_error(&response.body);
        if (200..300).contains(&response.status) && error.code.as_deref() == Some("0") {
            return parse_cancellation_acknowledgement(
                &response.body,
                client_order_id,
                exchange_order_id,
            )
            .map_err(|codec_error| CancellationError::Unknown {
                message: format!("Bybit cancellation acknowledgement is invalid: {codec_error}"),
            });
        }
        Err(CancellationError::Unknown {
            message: error.message,
        })
    }
}

#[async_trait]
impl<T, S, C> OrderLookupGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)
            .map_err(|error| lookup_error(error.to_string()))?;
        let symbol = symbol.to_ascii_uppercase();
        Ok(
            match self
                .load_order_record(&symbol, client_order_id, None)
                .await?
            {
                Some(record) => OrderLookup::Found(record.order),
                None => OrderLookup::NotFound,
            },
        )
    }
}

#[async_trait]
impl<T, S, C> OpenOrderSnapshotGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<crate::exchange::AuthoritativeOrder>, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut client_order_ids = BTreeSet::new();
        let mut exchange_order_ids = BTreeSet::new();
        let mut orders = Vec::new();
        for _ in 0..MAX_OPEN_ORDER_PAGES {
            let mut query = vec![
                ("category".into(), CATEGORY.into()),
                ("symbol".into(), symbol.clone()),
                ("openOnly".into(), "0".into()),
                ("limit".into(), OPEN_ORDER_PAGE_LIMIT.to_string()),
            ];
            if let Some(value) = &cursor {
                query.push(("cursor".into(), value.clone()));
            }
            let request = self
                .signed_get("/v5/order/realtime", query)
                .map_err(|error| SnapshotError::new(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Bybit open-order snapshot")
                .await?;
            let page = parse_open_order_page(&body, &symbol).map_err(|error| {
                SnapshotError::new(format!("invalid Bybit open-order snapshot: {error}"))
            })?;
            for order in page.orders {
                if !client_order_ids.insert(order.client_order_id.clone())
                    || !exchange_order_ids.insert(order.exchange_order_id.clone())
                {
                    return Err(SnapshotError::new(
                        "Bybit open-order pages contain duplicate identities",
                    ));
                }
                orders.push(order);
            }
            let Some(next_cursor) = page.next_cursor else {
                orders.sort_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
                return Ok(orders);
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(SnapshotError::new(
                    "Bybit open-order cursor did not advance",
                ));
            }
            cursor = Some(next_cursor);
        }
        Err(SnapshotError::new(
            "Bybit open orders exceeded bounded pagination",
        ))
    }
}

#[async_trait]
impl<T, S, C> OrderHistorySnapshotGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        if !(1..=1_000).contains(&limit) {
            return Err(SnapshotError::new("order-history limit must be 1..=1000"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut exchange_order_ids = BTreeSet::new();
        let mut orders = Vec::with_capacity(limit);

        for _ in 0..MAX_ORDER_HISTORY_PAGES {
            let page_limit = ORDER_HISTORY_PAGE_LIMIT.min(limit - orders.len());
            let mut query = vec![
                ("category".into(), CATEGORY.into()),
                ("symbol".into(), symbol.clone()),
                ("limit".into(), page_limit.to_string()),
            ];
            if let Some(value) = &cursor {
                query.push(("cursor".into(), value.clone()));
            }
            let request = self
                .signed_get("/v5/order/history", query)
                .map_err(|error| SnapshotError::new(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Bybit order-history snapshot")
                .await?;
            let page = parse_order_history_page(&body, &symbol).map_err(|error| {
                SnapshotError::new(format!("invalid Bybit order-history snapshot: {error}"))
            })?;
            if page.orders.len() > page_limit {
                return Err(SnapshotError::new(
                    "Bybit order-history page exceeded the requested limit",
                ));
            }
            for order in page.orders {
                if !exchange_order_ids.insert(order.exchange_order_id.clone()) {
                    return Err(SnapshotError::new(
                        "Bybit order-history pages contain duplicate order identities",
                    ));
                }
                orders.push(order);
            }
            if orders.len() == limit {
                break;
            }
            let Some(next_cursor) = page.next_cursor else {
                cursor = None;
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(SnapshotError::new(
                    "Bybit order-history cursor did not advance",
                ));
            }
            cursor = Some(next_cursor);
        }

        if orders.len() < limit && cursor.is_some() {
            return Err(SnapshotError::new(
                "Bybit order history exceeded bounded pagination",
            ));
        }
        orders.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| left.exchange_order_id.cmp(&right.exchange_order_id))
        });
        Ok(orders)
    }
}

#[async_trait]
impl<T, S, C> ExecutionSnapshotGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)
            .map_err(|error| execution_error(error.to_string()))?;
        if exchange_order_id.trim().is_empty() {
            return Err(execution_error("exchange order ID is required"));
        }
        let symbol = symbol.to_ascii_uppercase();
        let header = self
            .load_order_record(&symbol, client_order_id, Some(exchange_order_id))
            .await
            .map_err(|error| execution_error(error.to_string()))?
            .ok_or_else(|| execution_error("Bybit order snapshot is not available"))?;
        if header.cumulative_quantity.is_zero() {
            return assemble_execution_snapshot(header, vec![])
                .map_err(|error| execution_error(error.to_string()));
        }

        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut trades = Vec::new();
        let mut completed = false;
        for _ in 0..MAX_EXECUTION_PAGES {
            let mut query = vec![
                ("category".into(), CATEGORY.into()),
                ("symbol".into(), symbol.clone()),
                ("orderId".into(), exchange_order_id.into()),
                ("limit".into(), EXECUTION_PAGE_LIMIT.to_string()),
            ];
            if let Some(value) = &cursor {
                query.push(("cursor".into(), value.clone()));
            }
            let request = self
                .signed_get("/v5/execution/list", query)
                .map_err(|error| execution_error(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Bybit execution snapshot")
                .await
                .map_err(|error| execution_error(error.to_string()))?;
            let page = parse_execution_page(&body, &symbol, client_order_id, exchange_order_id)
                .map_err(|error| {
                    execution_error(format!("invalid Bybit execution page: {error}"))
                })?;
            if page.trades.len() > EXECUTION_PAGE_LIMIT {
                return Err(execution_error(
                    "Bybit execution page exceeds requested limit",
                ));
            }
            trades.extend(page.trades);
            let Some(next_cursor) = page.next_cursor else {
                completed = true;
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(execution_error("Bybit execution cursor did not advance"));
            }
            cursor = Some(next_cursor);
        }
        if !completed {
            return Err(execution_error(
                "Bybit execution history exceeded bounded pagination",
            ));
        }
        assemble_execution_snapshot(header, trades)
            .map_err(|error| execution_error(format!("incomplete Bybit execution: {error}")))
    }
}

#[async_trait]
impl<T, S, C> MarketSnapshotGateway for BybitAdapter<T, S, C>
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
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request(
                    "/v5/market/tickers",
                    vec![
                        ("category".into(), CATEGORY.into()),
                        ("symbol".into(), symbol.clone()),
                    ],
                ),
                "Bybit market snapshot",
            )
            .await?;
        parse_market_snapshot(&body, &symbol)
            .map_err(|error| SnapshotError::new(format!("invalid Bybit market snapshot: {error}")))
    }
}

#[async_trait]
impl<T, S, C> InstrumentRulesGateway for BybitAdapter<T, S, C>
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
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request(
                    "/v5/market/instruments-info",
                    vec![
                        ("category".into(), CATEGORY.into()),
                        ("symbol".into(), symbol.clone()),
                    ],
                ),
                "Bybit instrument snapshot",
            )
            .await?;
        parse_instrument_rules(&body, &symbol).map_err(|error| {
            SnapshotError::new(format!("invalid Bybit instrument snapshot: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> HistoricalPriceGateway for BybitAdapter<T, S, C>
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
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        if minute_start_ms == 0 || !minute_start_ms.is_multiple_of(60_000) {
            return Err(SnapshotError::new(
                "historical price minute must be a positive UTC minute boundary",
            ));
        }
        let end = minute_start_ms
            .checked_add(59_999)
            .ok_or_else(|| SnapshotError::new("historical minute range overflowed"))?;
        let symbol = symbol.to_ascii_uppercase();
        let body = self
            .execute_snapshot(
                self.public_request(
                    "/v5/market/kline",
                    vec![
                        ("category".into(), CATEGORY.into()),
                        ("symbol".into(), symbol.clone()),
                        ("interval".into(), "1".into()),
                        ("start".into(), minute_start_ms.to_string()),
                        ("end".into(), end.to_string()),
                        ("limit".into(), "1".into()),
                    ],
                ),
                "Bybit historical fee-price snapshot",
            )
            .await?;
        parse_historical_minute_open(&body, &symbol, minute_start_ms).map_err(|error| {
            SnapshotError::new(format!("invalid Bybit historical minute: {error}"))
        })
    }
}

#[async_trait]
impl<T, S, C> PositionSnapshotGateway for BybitAdapter<T, S, C>
where
    T: HttpTransport,
    S: BybitRequestSigner,
    C: MillisecondClock,
{
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let market =
            <Self as MarketSnapshotGateway>::market_snapshot(self, exchange, &symbol).await?;
        let request = self
            .signed_get(
                "/v5/position/list",
                vec![
                    ("category".into(), CATEGORY.into()),
                    ("symbol".into(), symbol.clone()),
                ],
            )
            .map_err(|error| SnapshotError::new(error.to_string()))?;
        let body = self
            .execute_snapshot(request, "Bybit position snapshot")
            .await?;
        parse_position_snapshot(&body, &symbol, market.mark_price).map_err(|error| {
            SnapshotError::new(format!("invalid Bybit position snapshot: {error}"))
        })
    }
}

fn classify_placement_response(
    response: HttpResponse,
    client_order_id: &ClientOrderId,
) -> Result<PlacementAcknowledgement, PlacementError> {
    let error = parse_error(&response.body);
    if (200..300).contains(&response.status) && error.code.as_deref() == Some("0") {
        return parse_placement_acknowledgement(&response.body, client_order_id).map_err(
            |codec_error| PlacementError::Unknown {
                message: format!("Bybit acknowledgement is invalid: {codec_error}"),
            },
        );
    }
    if response.status == 408
        || response.status == 429
        || response.status >= 500
        || error.code.as_deref() == Some("0")
        || error
            .code
            .as_deref()
            .is_none_or(bybit_write_outcome_is_unknown)
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

fn bybit_write_outcome_is_unknown(code: &str) -> bool {
    matches!(
        code,
        "429" | "10000" | "10006" | "10014" | "10016" | "110072" | "110079" | "3400214"
    )
}

fn definitive_placement(message: impl Into<String>) -> PlacementError {
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

fn invalid_cancellation(message: impl Into<String>) -> CancellationError {
    CancellationError::Invalid {
        message: message.into(),
    }
}

fn lookup_error(message: impl Into<String>) -> LookupError {
    LookupError {
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
        domain::{IntentState, OrderShape, TerminalOrderStatus},
        exchange::{ActiveOrderStatus, OrderLifecycle, protocol::TransportError},
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

        fn push(&self, response: Result<HttpResponse, TransportError>) {
            self.responses.lock().unwrap().push_back(response);
        }

        fn requests(&self) -> Vec<PreparedHttpRequest> {
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

    fn adapter(
        transport: MockTransport,
    ) -> BybitAdapter<MockTransport, BybitHmacSha256Signer, FixedClock> {
        BybitAdapter::with_base_url(
            transport,
            BybitHmacSha256Signer::new("test-secret").unwrap(),
            FixedClock(1_700_000_000_123),
            "test-key",
            "https://example.test",
        )
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            client_order_id: ClientOrderId::parse("g_7_S_fixed").unwrap(),
            exchange: Exchange::Bybit,
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

    fn empty_order_result() -> String {
        r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[],"nextPageCursor":""},"time":1700000000123}"#.into()
    }

    fn order_result(status: &str, executed: &str, quote: &str) -> String {
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"category":"linear","list":[{{"orderId":"order-91","orderLinkId":"g_7_S_fixed","symbol":"MUUSDT","price":"1011","qty":"0.2","side":"Sell","positionIdx":0,"orderStatus":"{status}","cumExecQty":"{executed}","cumExecValue":"{quote}","timeInForce":"PostOnly","orderType":"Limit","reduceOnly":false,"createdTime":"1700000000000","updatedTime":"1700000001000"}}],"nextPageCursor":""}},"time":1700000001001}}"#
        )
    }

    fn execution_result(id: &str, qty: &str, value: &str, time: u64, cursor: &str) -> String {
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"category":"linear","list":[{{"symbol":"MUUSDT","orderId":"order-91","orderLinkId":"g_7_S_fixed","side":"Sell","execType":"Trade","execFee":"0","execId":"{id}","execPrice":"1011","execQty":"{qty}","execValue":"{value}","execTime":"{time}","feeCurrency":"","isMaker":true}}],"nextPageCursor":"{cursor}"}},"time":1700000001001}}"#
        )
    }

    fn ok(body: impl Into<String>) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status: 200,
            body: body.into(),
        })
    }

    #[test]
    fn hmac_signer_matches_sha256_test_vector() {
        let signer = BybitHmacSha256Signer::new("key").unwrap();
        assert_eq!(
            signer
                .sign("The quick brown fox jumps over the lazy dog")
                .unwrap(),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        assert!(!format!("{signer:?}").contains("key"));
    }

    fn open_order_page(
        order_id: &str,
        client_order_id: &str,
        price: &str,
        quantity: &str,
        cursor: &str,
    ) -> String {
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"category":"linear","list":[{{"orderId":"{order_id}","orderLinkId":"{client_order_id}","symbol":"MUUSDT","price":"{price}","qty":"{quantity}","side":"Buy","positionIdx":0,"orderStatus":"New","cumExecQty":"0","cumExecValue":"0","timeInForce":"GTC","orderType":"Limit","reduceOnly":true,"createdTime":"1700000000000","updatedTime":"1700000000000"}}],"nextPageCursor":"{cursor}"}},"time":1700000001001}}"#
        )
    }

    fn order_history_page(order_id: &str, time: u64, cursor: &str) -> String {
        format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"category":"linear","list":[{{"orderId":"{order_id}","orderLinkId":"","symbol":"MUUSDT","price":"1011.00000","qty":"0.240","side":"Sell","orderStatus":"Filled","createdTime":"{time}"}}],"nextPageCursor":"{cursor}"}},"time":1700000001001}}"#
        )
    }

    #[tokio::test]
    async fn open_order_snapshot_exhausts_cursor_pages_before_returning() {
        let transport = MockTransport::default();
        transport.push(ok(open_order_page(
            "order-2",
            "g_RUN00001_2_B_2",
            "1012",
            "100",
            "cursor:2",
        )));
        transport.push(ok(open_order_page(
            "order-1",
            "g_RUN00001_1_B_1",
            "1010",
            "70",
            "",
        )));

        let orders = adapter(transport.clone())
            .open_orders_snapshot(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap();
        let requests = transport.requests();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/v5/order/realtime");
        assert!(
            requests[0]
                .query
                .iter()
                .any(|item| item == &("openOnly".into(), "0".into()))
        );
        assert!(
            requests[1]
                .query
                .iter()
                .any(|item| item == &("cursor".into(), "cursor:2".into()))
        );
        assert_eq!(orders[0].client_order_id.as_str(), "g_RUN00001_1_B_1");
        assert_eq!(orders[0].shape.quantity, Decimal::new(70, 0));
        assert_eq!(orders[1].shape.quantity, Decimal::new(100, 0));
    }

    #[tokio::test]
    async fn order_history_exhausts_cursor_pages_and_honors_exact_limit() {
        let transport = MockTransport::default();
        transport.push(ok(order_history_page(
            "order-older",
            1_780_000_000_001,
            "cursor:2",
        )));
        transport.push(ok(order_history_page("order-newer", 1_780_000_000_002, "")));

        let orders = adapter(transport.clone())
            .order_history_snapshot(Exchange::Bybit, "MUUSDT", 2)
            .await
            .unwrap();
        let requests = transport.requests();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/v5/order/history");
        assert!(
            requests[0]
                .query
                .iter()
                .any(|item| item == &("limit".into(), "2".into()))
        );
        assert!(
            requests[1]
                .query
                .iter()
                .any(|item| item == &("limit".into(), "1".into()))
        );
        assert!(
            requests[1]
                .query
                .iter()
                .any(|item| item == &("cursor".into(), "cursor:2".into()))
        );
        assert_eq!(orders[0].exchange_order_id, "order-newer");
        assert_eq!(orders[0].price.to_string(), "1011.00000");
        assert_eq!(orders[0].quantity.to_string(), "0.240");
    }

    #[tokio::test]
    async fn repeated_cursor_and_cross_page_duplicate_never_return_partial_open_orders() {
        let repeated = MockTransport::default();
        repeated.push(ok(open_order_page(
            "order-1",
            "g_RUN00001_1_B_1",
            "1010",
            "100",
            "same",
        )));
        repeated.push(ok(open_order_page(
            "order-2",
            "g_RUN00001_2_B_2",
            "1012",
            "100",
            "same",
        )));
        assert!(
            adapter(repeated)
                .open_orders_snapshot(Exchange::Bybit, "MUUSDT")
                .await
                .is_err()
        );

        let duplicate = MockTransport::default();
        duplicate.push(ok(open_order_page(
            "order-1",
            "g_RUN00001_1_B_1",
            "1010",
            "100",
            "next",
        )));
        duplicate.push(ok(open_order_page(
            "order-1",
            "g_RUN00001_1_B_1",
            "1010",
            "100",
            "",
        )));
        assert!(
            adapter(duplicate)
                .open_orders_snapshot(Exchange::Bybit, "MUUSDT")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn placement_signs_and_transports_the_same_exact_json_bytes() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"order-91","orderLinkId":"g_7_S_fixed"},"time":1700000000124}"#,
        ));
        let acknowledgement = adapter(transport.clone())
            .place_order(&intent())
            .await
            .unwrap();
        let request = &transport.requests()[0];
        let exact_body = r#"{"category":"linear","symbol":"MUUSDT","side":"Sell","orderType":"Limit","qty":"0.2","price":"1011","timeInForce":"PostOnly","positionIdx":0,"orderLinkId":"g_7_S_fixed","reduceOnly":false}"#;

        assert_eq!(acknowledgement.exchange_order_id, "order-91");
        assert_eq!(request.path, "/v5/order/create");
        assert_eq!(request.body_string(), exact_body);
        assert!(request.query.is_empty());
        assert!(request.body.is_empty());
        let expected_signature = BybitHmacSha256Signer::new("test-secret")
            .unwrap()
            .sign(&format!("1700000000123test-key5000{exact_body}"))
            .unwrap();
        assert_eq!(
            request
                .headers
                .iter()
                .find(|(name, _)| name == "X-BAPI-SIGN")
                .map(|(_, value)| value.as_str()),
            Some(expected_signature.as_str())
        );
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("test-key"));
        assert!(!rendered.contains(&expected_signature));
        assert!(!rendered.contains(exact_body));
    }

    #[tokio::test]
    async fn leverage_change_signs_exact_json_and_accepts_already_configured_code() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":110043,"retMsg":"Set leverage not modified","result":{},"time":1700000000124}"#,
        ));
        let acknowledgement = adapter(transport.clone())
            .set_leverage(Exchange::Bybit, "muusdt", 5)
            .await
            .unwrap();
        let request = &transport.requests()[0];
        let exact_body =
            r#"{"category":"linear","symbol":"MUUSDT","buyLeverage":"5","sellLeverage":"5"}"#;

        assert_eq!(acknowledgement.leverage, 5);
        assert_eq!(request.path, "/v5/position/set-leverage");
        assert_eq!(request.body_string(), exact_body);
        let expected_signature = BybitHmacSha256Signer::new("test-secret")
            .unwrap()
            .sign(&format!("1700000000123test-key5000{exact_body}"))
            .unwrap();
        assert!(
            request
                .headers
                .contains(&("X-BAPI-SIGN".into(), expected_signature))
        );
    }

    #[tokio::test]
    async fn fee_rate_query_is_signed_and_requires_one_exact_symbol() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"MUUSDT","takerFeeRate":"0.0005","makerFeeRate":"0.0002"}]},"time":1700000000124}"#,
        ));
        let rates = adapter(transport.clone())
            .trading_fee_rates(Exchange::Bybit, "muusdt")
            .await
            .unwrap();
        let request = &transport.requests()[0];

        assert_eq!(rates.maker_rate, Decimal::new(2, 4));
        assert_eq!(rates.taker_rate, Decimal::new(5, 4));
        assert_eq!(request.path, "/v5/account/fee-rate");
        assert_eq!(request.query_string(), "category=linear&symbol=MUUSDT");
        assert!(!request.headers.is_empty());
    }

    #[tokio::test]
    async fn account_balance_query_uses_signed_unified_usd_totals() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"list":[{"accountType":"UNIFIED","totalEquity":"3.312165910","totalWalletBalance":"3.003260560","totalAvailableBalance":"2.900000000","totalPerpUPL":"0.308905350","coin":[]}]},"time":1700000000124}"#,
        ));

        let snapshot = adapter(transport.clone())
            .account_balance_snapshot(Exchange::Bybit)
            .await
            .unwrap();
        let request = &transport.requests()[0];

        assert_eq!(snapshot.equity.to_string(), "3.312165910");
        assert_eq!(snapshot.available_balance.to_string(), "2.900000000");
        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(request.path, "/v5/account/wallet-balance");
        assert_eq!(request.query_string(), "accountType=UNIFIED");
        assert!(!request.headers.is_empty());
    }

    #[tokio::test]
    async fn reduce_only_market_payload_omits_limit_only_fields() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"close-91","orderLinkId":"g_7_S_fixed"}}"#,
        ));
        let mut close = intent();
        close.shape.side = OrderSide::Buy;
        close.shape.kind = OrderKind::Market;
        close.shape.price = None;
        close.shape.time_in_force = TimeInForce::Gtc;
        close.shape.reduce_only = true;

        adapter(transport.clone())
            .place_order(&close)
            .await
            .unwrap();

        assert_eq!(
            transport.requests()[0].body_string(),
            r#"{"category":"linear","symbol":"MUUSDT","side":"Buy","orderType":"Market","qty":"0.2","positionIdx":0,"orderLinkId":"g_7_S_fixed","reduceOnly":true}"#
        );
    }

    #[tokio::test]
    async fn uncertain_and_definitive_placement_failures_are_not_conflated() {
        let timeout = MockTransport::with_response(Err(TransportError::Timeout("late".into())));
        assert!(matches!(
            adapter(timeout).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));

        let server_timeout = MockTransport::with_response(ok(
            r#"{"retCode":10000,"retMsg":"Server Timeout","result":{},"time":1700000000124}"#,
        ));
        assert!(matches!(
            adapter(server_timeout).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));

        let invalid = MockTransport::with_response(ok(
            r#"{"retCode":10001,"retMsg":"Request parameter error","result":{},"time":1700000000124}"#,
        ));
        assert!(matches!(
            adapter(invalid).place_order(&intent()).await,
            Err(PlacementError::Definitive { .. })
        ));

        let duplicate_identity = MockTransport::with_response(ok(
            r#"{"retCode":110072,"retMsg":"OrderLinkedID is duplicate","result":{}}"#,
        ));
        assert!(matches!(
            adapter(duplicate_identity).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));

        let false_success = MockTransport::with_response(Ok(HttpResponse {
            status: 400,
            body: r#"{"retCode":0,"retMsg":"OK","result":{}}"#.into(),
        }));
        assert!(matches!(
            adapter(false_success).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));

        let malformed_success = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"order-91"}}"#,
        ));
        assert!(matches!(
            adapter(malformed_success).place_order(&intent()).await,
            Err(PlacementError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn cancellation_acknowledges_only_the_exact_immutable_target() {
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"order-91","orderLinkId":"g_7_S_fixed"},"time":1700000000124}"#,
        ));
        let acknowledgement = adapter(transport.clone())
            .cancel_order(
                Exchange::Bybit,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "order-91",
            )
            .await
            .unwrap();
        let request = &transport.requests()[0];

        assert_eq!(acknowledgement.exchange_order_id, "order-91");
        assert_eq!(request.path, "/v5/order/cancel");
        assert_eq!(
            request.body_string(),
            r#"{"category":"linear","symbol":"MUUSDT","orderId":"order-91","orderLinkId":"g_7_S_fixed"}"#
        );

        let mismatch = MockTransport::with_response(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"orderId":"other","orderLinkId":"g_7_S_fixed"}}"#,
        ));
        assert!(matches!(
            adapter(mismatch)
                .cancel_order(
                    Exchange::Bybit,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                    "order-91",
                )
                .await,
            Err(CancellationError::Unknown { .. })
        ));

        let timeout = MockTransport::with_response(Err(TransportError::Timeout("late".into())));
        assert!(matches!(
            adapter(timeout)
                .cancel_order(
                    Exchange::Bybit,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                    "order-91",
                )
                .await,
            Err(CancellationError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn lookup_falls_back_from_realtime_absence_to_exact_history() {
        let transport = MockTransport::default();
        transport.push(ok(
            r#"{"retCode":110001,"retMsg":"Order does not exist","result":{},"time":1700000000123}"#,
        ));
        transport.push(ok(order_result("New", "0", "0")));

        let lookup = adapter(transport.clone())
            .lookup_order_by_client_id(
                Exchange::Bybit,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
            )
            .await
            .unwrap();
        let OrderLookup::Found(order) = lookup else {
            panic!("historical order must be found");
        };
        assert_eq!(
            order.lifecycle,
            OrderLifecycle::Active(ActiveOrderStatus::New)
        );
        assert_eq!(order.shape.quantity, Decimal::new(2, 1));
        assert_eq!(transport.requests()[0].path, "/v5/order/realtime");
        assert_eq!(transport.requests()[1].path, "/v5/order/history");
    }

    #[tokio::test]
    async fn exact_empty_realtime_and_history_results_are_not_found() {
        let transport = MockTransport::default();
        transport.push(ok(empty_order_result()));
        transport.push(ok(empty_order_result()));
        assert_eq!(
            adapter(transport)
                .lookup_order_by_client_id(
                    Exchange::Bybit,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                )
                .await
                .unwrap(),
            OrderLookup::NotFound
        );
    }

    #[tokio::test]
    async fn execution_snapshot_paginates_opaque_cursor_and_reconciles_exact_totals() {
        let transport = MockTransport::default();
        transport.push(ok(order_result("Filled", "0.2", "202.2")));
        transport.push(ok(execution_result(
            "e0cbe81d-0f18-5866-9415-cf319b5dab3b",
            "0.1",
            "101.1",
            1_700_000_000_500,
            "cursor%3A2",
        )));
        transport.push(ok(execution_result(
            "95dcaa18-44b0-55f1-a3c4-99869bc498ce",
            "0.1",
            "101.1",
            1_700_000_000_600,
            "",
        )));

        let snapshot = adapter(transport.clone())
            .execution_snapshot(
                Exchange::Bybit,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "order-91",
            )
            .await
            .unwrap();
        let requests = transport.requests();

        assert_eq!(snapshot.trades.len(), 2);
        assert_eq!(snapshot.cumulative_quantity, Decimal::new(2, 1));
        assert_eq!(snapshot.fees_by_asset["USDT"], Decimal::ZERO);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].path, "/v5/execution/list");
        assert_eq!(
            requests[2]
                .query
                .iter()
                .find(|(name, _)| name == "cursor")
                .map(|(_, value)| value.as_str()),
            Some("cursor%3A2")
        );
        let expected_payload = requests[2].query_string();
        let expected_signature = BybitHmacSha256Signer::new("test-secret")
            .unwrap()
            .sign(&format!("1700000000123test-key5000{expected_payload}"))
            .unwrap();
        assert_eq!(
            requests[2]
                .headers
                .iter()
                .find(|(name, _)| name == "X-BAPI-SIGN")
                .map(|(_, value)| value.as_str()),
            Some(expected_signature.as_str())
        );
    }

    #[tokio::test]
    async fn partially_filled_cancellation_preserves_exact_remainder_end_to_end() {
        let transport = MockTransport::default();
        transport.push(ok(order_result("PartiallyFilledCanceled", "0.07", "70.77")));
        transport.push(ok(execution_result(
            "partial-cancel-fill",
            "0.07",
            "70.77",
            1_700_000_000_500,
            "",
        )));

        let snapshot = adapter(transport)
            .execution_snapshot(
                Exchange::Bybit,
                "MUUSDT",
                &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                "order-91",
            )
            .await
            .unwrap();

        assert_eq!(
            snapshot.order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled)
        );
        assert_eq!(snapshot.cumulative_quantity, Decimal::new(7, 2));
        assert_eq!(
            snapshot.order.shape.quantity - snapshot.cumulative_quantity,
            Decimal::new(13, 2)
        );
        assert_eq!(snapshot.trades.len(), 1);
    }

    #[tokio::test]
    async fn repeated_execution_cursor_never_becomes_a_partial_snapshot() {
        let transport = MockTransport::default();
        transport.push(ok(order_result("Filled", "0.2", "202.2")));
        transport.push(ok(execution_result(
            "id-1",
            "0.1",
            "101.1",
            1_700_000_000_500,
            "same-cursor",
        )));
        transport.push(ok(execution_result(
            "id-2",
            "0.1",
            "101.1",
            1_700_000_000_600,
            "same-cursor",
        )));

        assert!(
            adapter(transport)
                .execution_snapshot(
                    Exchange::Bybit,
                    "MUUSDT",
                    &ClientOrderId::parse("g_7_S_fixed").unwrap(),
                    "order-91",
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn public_market_rules_and_historical_price_use_v5_linear_contracts() {
        let transport = MockTransport::default();
        transport.push(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"MUUSDT","lastPrice":"1011.25","markPrice":"1011.20"}]},"time":1700000000123}"#,
        ));
        transport.push(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"MUUSDT","status":"Trading","priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"maxOrderQty":"100","minOrderQty":"0.01","qtyStep":"0.01","maxMktOrderQty":"50","minNotionalValue":"5"}}],"nextPageCursor":""},"time":1700000000123}"#,
        ));
        transport.push(ok(
            r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","symbol":"BNBUSDT","list":[["1020000","602.25","603","601","602","100","60000"]]},"time":1700000000123}"#,
        ));
        let adapter = adapter(transport.clone());

        let market = adapter
            .market_snapshot(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap();
        let rules = adapter
            .instrument_rules(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap();
        let price = adapter
            .historical_minute_open(Exchange::Bybit, "BNBUSDT", 1_020_000)
            .await
            .unwrap();

        assert_eq!(market.mark_price, Decimal::new(101120, 2));
        assert_eq!(rules.market_quantity.max, Some(Decimal::new(50, 0)));
        assert_eq!(price.open_price, Decimal::new(60225, 2));
        assert!(
            transport
                .requests()
                .iter()
                .all(|request| request.headers.is_empty())
        );
    }

    #[tokio::test]
    async fn signed_position_snapshot_preserves_old_short_and_detects_hedge_mode() {
        let short_transport = MockTransport::default();
        short_transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","lastPrice":"1001","markPrice":"1000"}]},"time":1700000000123}"#,
        ));
        short_transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","positionIdx":0,"side":"Sell","size":"3","avgPrice":"1011","markPrice":"1000","unrealisedPnl":"33","leverage":"5"}]},"time":1700000000123}"#,
        ));
        let snapshot = adapter(short_transport.clone())
            .position_snapshot(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap();
        assert_eq!(
            snapshot.one_way_position().unwrap(),
            (Decimal::new(-3, 0), Some(Decimal::new(1011, 0)))
        );
        assert_eq!(snapshot.one_way_leverage().unwrap(), 5);
        assert_eq!(short_transport.requests().len(), 2);
        assert!(short_transport.requests()[0].headers.is_empty());
        assert!(!short_transport.requests()[1].headers.is_empty());

        let hedge_transport = MockTransport::default();
        hedge_transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","lastPrice":"1001","markPrice":"1000"}]},"time":1700000000123}"#,
        ));
        hedge_transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"MUUSDT","positionIdx":1,"side":"Buy","size":"2","avgPrice":"990","markPrice":"1000","unrealisedPnl":"20"},{"symbol":"MUUSDT","positionIdx":2,"side":"Sell","size":"1","avgPrice":"1020","markPrice":"1000","unrealisedPnl":"20"}]},"time":1700000000123}"#,
        ));
        let hedge = adapter(hedge_transport)
            .position_snapshot(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap();
        assert!(hedge.one_way_position().is_err());
    }
}
