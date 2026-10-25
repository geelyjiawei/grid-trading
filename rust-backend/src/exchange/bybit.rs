use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
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
        funding::{FundingHistorySnapshot, FundingPayment},
        protocol::{
            HttpMethod, HttpResponse, HttpTransport, MillisecondClock, Parameters,
            PreparedHttpRequest, encode_parameters,
        },
        realtime::BybitExecutionCache,
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
const MAX_EXACT_ORDER_PAGES: usize = 20;
const FUNDING_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const FUNDING_RETENTION_MS: u64 = 730 * 24 * 60 * 60 * 1_000;
const MAX_FUNDING_REQUESTS: usize = 256;
const FUNDING_PAGE_LIMIT: usize = 50;
const REALTIME_EXECUTION_SNAPSHOT_WAIT: Duration = Duration::from_millis(250);

mod order_ws;
pub(crate) use order_ws::{BybitOrderRelay, spawn_bybit_order_stream};

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
    realtime_lifetime: Arc<()>,
    realtime_execution_cache: BybitExecutionCache,
    websocket_order_relay: Option<BybitOrderRelay>,
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
            realtime_lifetime: crate::exchange::realtime::new_realtime_lifetime(),
            realtime_execution_cache: BybitExecutionCache::default(),
            websocket_order_relay: None,
        }
    }

    pub(crate) fn realtime_lifetime(&self) -> Weak<()> {
        Arc::downgrade(&self.realtime_lifetime)
    }

    pub(crate) fn realtime_execution_cache(&self) -> BybitExecutionCache {
        self.realtime_execution_cache.clone()
    }

    pub fn set_recv_window_ms(&mut self, recv_window_ms: u64) {
        self.recv_window_ms = recv_window_ms;
    }

    pub(crate) fn set_websocket_order_relay(&mut self, relay: BybitOrderRelay) {
        self.websocket_order_relay = Some(relay);
    }

    fn seed_open_order_headers(
        &self,
        epoch: u64,
        headers: Vec<crate::exchange::execution::OrderExecutionHeader>,
    ) -> Result<(), SnapshotError> {
        let mut snapshots = Vec::new();
        for header in headers {
            if matches!(
                header.order.lifecycle,
                crate::exchange::OrderLifecycle::Active(crate::exchange::ActiveOrderStatus::New)
            ) && header.cumulative_quantity.is_zero()
            {
                let snapshot = assemble_execution_snapshot(header, vec![]).map_err(|error| {
                    SnapshotError::new(format!("invalid Bybit New order snapshot: {error}"))
                })?;
                snapshots.push(snapshot);
            }
        }
        // Bind only after the complete scan validates. These REST observations
        // are already returned to the caller, not fresh fill notifications.
        for snapshot in snapshots {
            self.realtime_execution_cache
                .seed_verified_snapshot(epoch, &snapshot);
        }
        Ok(())
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
            let mut cursor: Option<String> = None;
            let mut seen_cursors = BTreeSet::new();
            let mut found = None;
            for page_index in 0..MAX_EXACT_ORDER_PAGES {
                let mut query = vec![
                    ("category".into(), CATEGORY.into()),
                    ("symbol".into(), symbol.into()),
                    ("orderLinkId".into(), client_order_id.as_str().into()),
                    ("limit".into(), "1".into()),
                ];
                if let Some(cursor) = &cursor {
                    query.push(("cursor".into(), cursor.clone()));
                }
                let request = self
                    .signed_get(path, query)
                    .map_err(|error| lookup_error(error.to_string()))?;
                let response = self
                    .transport
                    .execute(request)
                    .await
                    .map_err(|error| lookup_error(format!("{context}: {error}")))?;
                let exchange_error = parse_error(&response.body);
                if exchange_error.code.as_deref() == Some("110001") {
                    if page_index == 0 && found.is_none() {
                        break;
                    }
                    return Err(lookup_error("Bybit exact lookup continuation disappeared"));
                }
                if !(200..300).contains(&response.status)
                    || exchange_error.code.as_deref() != Some("0")
                {
                    return Err(lookup_error(format!(
                        "{context}: HTTP {}: {}",
                        response.status, exchange_error.message
                    )));
                }
                let page = parse_exact_order_record(
                    &response.body,
                    symbol,
                    client_order_id,
                    exchange_order_id,
                )
                .map_err(|error| lookup_error(format!("invalid Bybit order snapshot: {error}")))?;
                if let Some(record) = page.record {
                    if found.is_some() {
                        return Err(lookup_error(
                            "Bybit exact lookup contains duplicate records",
                        ));
                    }
                    found = Some(record);
                }
                match page.next_cursor {
                    None => {
                        if found.is_some() {
                            return Ok(found);
                        }
                        break;
                    }
                    Some(next) => {
                        if !seen_cursors.insert(next.clone())
                            || page_index + 1 == MAX_EXACT_ORDER_PAGES
                        {
                            return Err(lookup_error(
                                "Bybit exact lookup pagination did not terminate",
                            ));
                        }
                        cursor = Some(next);
                    }
                }
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
    async fn funding_history(
        &self,
        exchange: Exchange,
        symbol: &str,
        from_ms: u64,
        through_ms: u64,
    ) -> Result<FundingHistorySnapshot, SnapshotError> {
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.trim().to_ascii_uppercase();
        let asset = if symbol.ends_with("USDT") {
            "USDT"
        } else if symbol.ends_with("USDC") || symbol.ends_with("PERP") {
            "USDC"
        } else {
            return Err(SnapshotError::new(
                "Bybit funding settlement currency is unsupported",
            ));
        };
        let now = self.clock.now_millis();
        if from_ms > through_ms || through_ms > now {
            return Err(SnapshotError::new(
                "Bybit funding interval is invalid or in the future",
            ));
        }
        // The endpoint only retains two years; never label an older empty scan complete.
        if from_ms < now.saturating_sub(FUNDING_RETENTION_MS) {
            return Err(SnapshotError::new(
                "Bybit funding history exceeds the retention limit",
            ));
        }
        let mut payments = BTreeMap::<String, FundingPayment>::new();
        let mut request_count = 0;
        let mut window_start = from_ms;
        loop {
            let window_end = through_ms.min(window_start.saturating_add(FUNDING_WINDOW_MS - 1));
            let mut cursor: Option<String> = None;
            let mut seen_cursors = BTreeSet::new();
            loop {
                if request_count == MAX_FUNDING_REQUESTS {
                    return Err(SnapshotError::new(
                        "Bybit funding history request limit exceeded",
                    ));
                }
                let mut query = vec![
                    ("accountType".into(), "UNIFIED".into()),
                    ("category".into(), CATEGORY.into()),
                    ("type".into(), "SETTLEMENT".into()),
                    ("currency".into(), asset.into()),
                    ("startTime".into(), window_start.to_string()),
                    ("endTime".into(), window_end.to_string()),
                    ("limit".into(), FUNDING_PAGE_LIMIT.to_string()),
                ];
                if let Some(cursor) = &cursor {
                    query.push(("cursor".into(), cursor.clone()));
                }
                let request = self
                    .signed_get("/v5/account/transaction-log", query)
                    .map_err(|error| SnapshotError::new(error.to_string()))?;
                request_count += 1;
                let body = self
                    .execute_snapshot(request, "Bybit settled funding history")
                    .await?;
                let value: Value = serde_json::from_str(&body)
                    .map_err(|_| SnapshotError::new("Bybit funding response is not valid JSON"))?;
                let result = value
                    .get("result")
                    .ok_or_else(|| SnapshotError::new("Bybit funding result is missing"))?;
                let rows = result
                    .get("list")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        SnapshotError::new("Bybit funding list is missing or invalid")
                    })?;
                if rows.len() > FUNDING_PAGE_LIMIT {
                    return Err(SnapshotError::new(
                        "Bybit funding page exceeds the history limit",
                    ));
                }
                for row in rows {
                    let row_symbol = funding_text(row, "symbol")?;
                    let row_type = funding_text(row, "type")?;
                    if row_type != "SETTLEMENT" {
                        continue;
                    }
                    if funding_text(row, "category")? != CATEGORY
                        || funding_text(row, "currency")? != asset
                    {
                        return Err(SnapshotError::new(
                            "Bybit funding settlement scope is invalid",
                        ));
                    }
                    let at_ms = funding_text(row, "transactionTime")?
                        .parse::<u64>()
                        .map_err(|_| SnapshotError::new("Bybit funding timestamp is invalid"))?;
                    if at_ms < window_start || at_ms > window_end {
                        return Err(SnapshotError::new(
                            "Bybit funding receipt is outside the requested interval",
                        ));
                    }
                    if row_symbol != symbol {
                        continue;
                    }
                    // funding is already receive-positive. cashFlow/change also contain session PnL/fees.
                    let payment = FundingPayment {
                        id: funding_text(row, "id")?.to_owned(),
                        at_ms,
                        amount: funding_decimal(row, "funding")?,
                        asset: asset.into(),
                        position_quantity: Some(funding_decimal(row, "size")?),
                    };
                    if payment.position_quantity == Some(Decimal::ZERO) && !payment.amount.is_zero()
                    {
                        return Err(SnapshotError::new(
                            "Bybit nonzero funding receipt has zero settlement position",
                        ));
                    }
                    if let Some(previous) = payments.get(&payment.id) {
                        if previous != &payment {
                            return Err(SnapshotError::new(
                                "Bybit funding receipt identity conflict",
                            ));
                        }
                    } else {
                        payments.insert(payment.id.clone(), payment);
                    }
                }
                let next = result
                    .get("nextPageCursor")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        SnapshotError::new("Bybit funding cursor is missing or invalid")
                    })?;
                if next.is_empty() {
                    break;
                }
                if next.trim().is_empty() || !seen_cursors.insert(next.to_owned()) {
                    return Err(SnapshotError::new("Bybit funding cursor did not advance"));
                }
                cursor = Some(next.to_owned());
            }
            if window_end == through_ms {
                break;
            }
            window_start = window_end + 1;
        }
        let mut payments = payments.into_values().collect::<Vec<_>>();
        payments.sort_by(|left, right| (left.at_ms, &left.id).cmp(&(right.at_ms, &right.id)));
        Ok(FundingHistorySnapshot {
            exchange,
            symbol,
            from_ms,
            through_ms,
            payments,
            account_fills: None,
        })
    }

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

fn funding_text<'a>(value: &'a Value, field: &str) -> Result<&'a str, SnapshotError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && *text == text.trim())
        .ok_or_else(|| {
            SnapshotError::new(format!("Bybit funding field {field} is missing or invalid"))
        })
}

fn funding_decimal(value: &Value, field: &str) -> Result<Decimal, SnapshotError> {
    let text = funding_text(value, field)?;
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let valid = match unsigned.split_once('.') {
        Some((whole, fraction)) => digits(whole) && digits(fraction),
        None => digits(unsigned),
    };
    if !valid {
        return Err(SnapshotError::new(format!(
            "Bybit funding field {field} is invalid"
        )));
    }
    Decimal::from_str_exact(text).map_err(|_| {
        SnapshotError::new(format!(
            "Bybit funding field {field} is invalid or loses precision"
        ))
    })
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
        if let Some(relay) = &self.websocket_order_relay {
            let payload = serde_json::to_value(&payload)
                .map_err(|_| definitive_placement("Bybit order serialization failed"))?;
            let started = std::time::Instant::now();
            match relay.post(payload, self.recv_window_ms).await {
                Ok(response) => {
                    let roundtrip_ms =
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                    if let Some(ret_code) = parse_error(&response.body)
                        .code
                        .and_then(|code| code.parse::<i64>().ok())
                    {
                        tracing::info!(
                            transport = "websocket",
                            roundtrip_ms,
                            ret_code,
                            client_order_id = intent.client_order_id.as_str(),
                            symbol = intent.shape.symbol.as_str(),
                            "Bybit order placement response"
                        );
                    }
                    let ack = match classify_placement_response(response, &intent.client_order_id) {
                        Ok(ack) => ack,
                        Err(error) => {
                            tracing::warn!(
                                transport = "websocket",
                                roundtrip_ms,
                                client_order_id = intent.client_order_id.as_str(),
                                symbol = intent.shape.symbol.as_str(),
                                "Bybit order acknowledgement is invalid, rejected, or uncertain"
                            );
                            return Err(error);
                        }
                    };
                    self.realtime_execution_cache.register_placement(
                        intent,
                        &ack,
                        self.clock.now_millis(),
                    );
                    return Ok(ack);
                }
                Err(order_ws::BybitOrderError::NotSent) => {
                    tracing::info!(
                        transport = "rest",
                        fallback_reason = "websocket_not_sent",
                        client_order_id = intent.client_order_id.as_str(),
                        symbol = intent.shape.symbol.as_str(),
                        "Bybit order placement REST fallback"
                    );
                }
                Err(order_ws::BybitOrderError::Unknown) => {
                    tracing::warn!(
                        transport = "websocket",
                        outcome = "unknown",
                        roundtrip_ms =
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        client_order_id = intent.client_order_id.as_str(),
                        symbol = intent.shape.symbol.as_str(),
                        "Bybit order placement needs exact reconciliation; no REST fallback"
                    );
                    return Err(PlacementError::Unknown {
                        message:
                            "Bybit order WebSocket outcome is unknown; reconcile exact identity"
                                .into(),
                    });
                }
            }
        }
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
        let ack = classify_placement_response(response, &intent.client_order_id)?;
        self.realtime_execution_cache
            .register_placement(intent, &ack, self.clock.now_millis());
        Ok(ack)
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
        let cache_epoch = self.realtime_execution_cache.session_epoch();
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)?;
        let symbol = symbol.to_ascii_uppercase();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut client_order_ids = BTreeSet::new();
        let mut exchange_order_ids = BTreeSet::new();
        let mut orders = Vec::new();
        let mut headers = Vec::new();
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
            headers.extend(page.headers);
            let Some(next_cursor) = page.next_cursor else {
                self.seed_open_order_headers(cache_epoch, headers)?;
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
    async fn open_order_execution_progress_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Option<Vec<crate::exchange::OpenOrderExecutionProgress>>, ExecutionSnapshotError>
    {
        let cache_epoch = self.realtime_execution_cache.session_epoch();
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)
            .map_err(|error| execution_error(error.to_string()))?;
        let symbol = symbol.to_ascii_uppercase();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut client_order_ids = BTreeSet::new();
        let mut exchange_order_ids = BTreeSet::new();
        let mut progress = Vec::new();
        let mut headers = Vec::new();
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
                .map_err(|error| execution_error(error.to_string()))?;
            let body = self
                .execute_snapshot(request, "Bybit open-order execution progress")
                .await
                .map_err(|error| execution_error(error.to_string()))?;
            let page = parse_open_order_page(&body, &symbol).map_err(|error| {
                execution_error(format!("invalid Bybit open-order progress: {error}"))
            })?;
            for item in page.progress {
                if !client_order_ids.insert(item.order.client_order_id.clone())
                    || !exchange_order_ids.insert(item.order.exchange_order_id.clone())
                {
                    return Err(execution_error(
                        "Bybit open-order progress contains duplicate identities",
                    ));
                }
                progress.push(item);
            }
            headers.extend(page.headers);
            let Some(next_cursor) = page.next_cursor else {
                self.seed_open_order_headers(cache_epoch, headers)
                    .map_err(|error| execution_error(error.to_string()))?;
                progress.sort_by(|left, right| {
                    left.order.client_order_id.cmp(&right.order.client_order_id)
                });
                return Ok(Some(progress));
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(execution_error(
                    "Bybit open-order progress cursor did not advance",
                ));
            }
            cursor = Some(next_cursor);
        }
        Err(execution_error(
            "Bybit open-order progress exceeded bounded pagination",
        ))
    }

    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        let cache_epoch = self.realtime_execution_cache.session_epoch();
        validate_snapshot_request(exchange, Exchange::Bybit, symbol)
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
                "using Bybit realtime execution snapshot"
            );
            return Ok(snapshot);
        }
        let header = self
            .load_order_record(&symbol, client_order_id, Some(exchange_order_id))
            .await
            .map_err(|error| execution_error(error.to_string()))?
            .ok_or_else(|| execution_error("Bybit order snapshot is not available"))?;
        if header.cumulative_quantity.is_zero() {
            let mut snapshot = assemble_execution_snapshot(header, vec![])
                .map_err(|error| execution_error(error.to_string()))?;
            self.realtime_execution_cache
                .merge_verified_snapshot(cache_epoch, &mut snapshot)?;
            return Ok(snapshot);
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
        let mut snapshot = assemble_execution_snapshot(header, trades)
            .map_err(|error| execution_error(format!("incomplete Bybit execution: {error}")))?;
        self.realtime_execution_cache
            .merge_verified_snapshot(cache_epoch, &mut snapshot)?;
        Ok(snapshot)
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
        "429"
            | "10000"
            | "10006"
            | "10014"
            | "10016"
            | "10403"
            | "10429"
            | "10019"
            | "20006"
            | "110072"
            | "110079"
            | "3400214"
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
        reset_cache: Option<BybitExecutionCache>,
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
            if let Some(cache) = &self.reset_cache {
                cache.begin_session();
            }
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

    const FUNDING_START: u64 = 1_699_000_000_000;

    fn funding_row(id: &str, at_ms: u64, amount: &str, size: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "symbol": "MUUSDT", "category": "linear", "type": "SETTLEMENT",
            "transactionTime": at_ms.to_string(), "funding": amount, "size": size,
            "currency": "USDT", "cashFlow": "999", "fee": "12", "change": "987"
        })
    }

    fn funding_page(rows: Vec<serde_json::Value>, cursor: &str) -> String {
        serde_json::json!({"retCode": 0, "retMsg": "OK", "result": {
            "list": rows, "nextPageCursor": cursor
        }})
        .to_string()
    }

    #[tokio::test]
    async fn funding_history_preserves_signs_positions_and_filters_response_symbol() {
        let mut other = funding_row("other", FUNDING_START, "90", "3");
        other["symbol"] = serde_json::json!("BTCUSDT");
        let mut trade = funding_row("trade", FUNDING_START, "100", "3");
        trade["type"] = serde_json::json!("TRADE");
        let transport = MockTransport::with_response(ok(funding_page(
            vec![
                funding_row("received", FUNDING_START, "1.25", "-10"),
                funding_row("paid", FUNDING_START + 1, "-2.5", "20"),
                funding_row("zero", FUNDING_START + 2, "0", "0"),
                other,
                trade,
            ],
            "",
        )));
        let history = adapter(transport.clone())
            .funding_history(Exchange::Bybit, "muusdt", FUNDING_START, FUNDING_START + 2)
            .await
            .unwrap();
        assert_eq!(history.symbol, "MUUSDT");
        assert_eq!(history.exchange, Exchange::Bybit);
        assert_eq!(
            (history.from_ms, history.through_ms),
            (FUNDING_START, FUNDING_START + 2)
        );
        assert!(history.account_fills.is_none());
        assert_eq!(history.payments.len(), 3);
        assert_eq!(history.payments[0].amount, Decimal::new(125, 2));
        assert_eq!(
            history.payments[0].position_quantity,
            Some(Decimal::from(-10))
        );
        assert_eq!(history.payments[1].amount, Decimal::new(-25, 1));
        assert_eq!(
            history.payments[1].position_quantity,
            Some(Decimal::from(20))
        );
        assert_eq!(history.payments[2].amount, Decimal::ZERO);
        assert_eq!(history.payments[2].position_quantity, Some(Decimal::ZERO));
        let request = &transport.requests()[0];
        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(request.path, "/v5/account/transaction-log");
        assert!(request.headers.iter().any(|(key, _)| key == "X-BAPI-SIGN"));
        for (key, value) in [
            ("accountType", "UNIFIED"),
            ("category", "linear"),
            ("limit", "50"),
        ] {
            assert!(request.query.contains(&(key.into(), value.into())));
        }
        assert!(!request.query.iter().any(|(key, _)| key == "symbol"));
    }

    #[tokio::test]
    async fn funding_history_usdc_uses_only_funding_not_session_pnl_or_fees() {
        let mut row = funding_row("usdc", FUNDING_START, "-0.75", "-2");
        row["symbol"] = serde_json::json!("MUPERP");
        row["currency"] = serde_json::json!("USDC");
        let transport = MockTransport::with_response(ok(funding_page(vec![row], "")));
        let result = adapter(transport)
            .funding_history(Exchange::Bybit, "MUPERP", FUNDING_START, FUNDING_START)
            .await
            .unwrap();
        assert_eq!(result.payments[0].amount, Decimal::new(-75, 2));
        assert_eq!(result.payments[0].asset, "USDC");
    }

    #[tokio::test]
    async fn funding_history_exhausts_cursor_then_advances_nonoverlapping_seven_day_window() {
        let end = FUNDING_START + 604_800_001;
        let transport = MockTransport::with_response(ok(funding_page(
            vec![funding_row("a", FUNDING_START, "1", "2")],
            "opaque%3Anext",
        )));
        transport.push(ok(funding_page(
            vec![funding_row("b", FUNDING_START + 1, "2", "2")],
            "",
        )));
        transport.push(ok(funding_page(vec![funding_row("c", end, "3", "2")], "")));
        let result = adapter(transport.clone())
            .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, end)
            .await
            .unwrap();
        assert_eq!(result.payments.len(), 3);
        let requests = transport.requests();
        assert_eq!(requests.len(), 3);
        assert!(
            requests[1]
                .query
                .contains(&("cursor".into(), "opaque%3Anext".into()))
        );
        assert!(!requests[2].query.iter().any(|(key, _)| key == "cursor"));
        let mut previous_end = None;
        for index in [0, 2] {
            let query = &requests[index].query;
            let time = |name: &str| {
                query
                    .iter()
                    .find(|(key, _)| key == name)
                    .unwrap()
                    .1
                    .parse::<u64>()
                    .unwrap()
            };
            let start = time("startTime");
            let through = time("endTime");
            assert!(through - start <= 604_800_000);
            if let Some(previous) = previous_end {
                assert_eq!(start, previous + 1);
            }
            previous_end = Some(through);
        }
        assert_eq!(previous_end, Some(end));
    }

    #[tokio::test]
    async fn funding_history_rejects_missing_malformed_out_of_range_and_wrong_currency() {
        for (field, value) in [
            ("funding", serde_json::Value::Null),
            ("funding", serde_json::json!("")),
            ("funding", serde_json::json!("NaN")),
            ("funding", serde_json::json!("1_000")),
            ("funding", serde_json::json!("1.")),
            (
                "funding",
                serde_json::json!("0.123456789012345678901234567890"),
            ),
            ("funding", serde_json::json!(1.5)),
            ("size", serde_json::Value::Null),
            ("size", serde_json::json!("")),
            ("size", serde_json::json!("0")),
            ("size", serde_json::json!("1e100")),
            ("id", serde_json::json!("")),
            (
                "transactionTime",
                serde_json::json!((FUNDING_START - 1).to_string()),
            ),
            (
                "transactionTime",
                serde_json::json!((FUNDING_START + 1).to_string()),
            ),
            ("currency", serde_json::json!("USDC")),
            ("category", serde_json::json!("inverse")),
        ] {
            let mut row = funding_row("bad", FUNDING_START, "1", "2");
            row[field] = value;
            let transport = MockTransport::with_response(ok(funding_page(vec![row], "")));
            assert!(
                adapter(transport)
                    .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
                    .await
                    .is_err(),
                "accepted {field}"
            );
        }
        for field in ["size", "funding", "symbol", "type", "transactionTime"] {
            let mut row = funding_row("bad", FUNDING_START, "1", "2");
            row.as_object_mut().unwrap().remove(field);
            let transport = MockTransport::with_response(ok(funding_page(vec![row], "")));
            assert!(
                adapter(transport)
                    .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
                    .await
                    .is_err(),
                "accepted missing {field}"
            );
        }
    }

    #[tokio::test]
    async fn funding_history_rejects_incomplete_envelopes_and_cursor_cycles() {
        for body in [
            "{}",
            r#"{"retCode":0,"result":{"list":[]}}"#,
            r#"{"retCode":0,"result":{"list":null,"nextPageCursor":""}}"#,
            r#"{"retCode":0,"result":{"list":[],"nextPageCursor":null}}"#,
        ] {
            let transport = MockTransport::with_response(ok(body));
            assert!(
                adapter(transport)
                    .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
                    .await
                    .is_err()
            );
        }
        let transport = MockTransport::with_response(ok(funding_page(vec![], "repeat")));
        transport.push(ok(funding_page(vec![], "repeat")));
        let error = adapter(transport.clone())
            .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
            .await
            .unwrap_err();
        assert!(error.message.contains("cursor"));
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn funding_history_rejects_out_of_window_account_rows_before_symbol_filtering() {
        let mut row = funding_row("other", FUNDING_START + 1, "1", "2");
        row["symbol"] = serde_json::json!("BTCUSDT");
        let transport = MockTransport::with_response(ok(funding_page(vec![row], "")));
        let error = adapter(transport)
            .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
            .await
            .unwrap_err();
        assert!(error.message.contains("interval"));
    }

    #[tokio::test]
    async fn funding_history_deduplicates_identical_receipts_and_rejects_conflicting_ids() {
        for amount in ["1", "2"] {
            let transport = MockTransport::with_response(ok(funding_page(
                vec![funding_row("same", FUNDING_START, "1", "2")],
                "next",
            )));
            transport.push(ok(funding_page(
                vec![funding_row("same", FUNDING_START, amount, "2")],
                "",
            )));
            let result = adapter(transport)
                .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
                .await;
            if amount == "1" {
                assert_eq!(result.unwrap().payments.len(), 1);
            } else {
                assert!(result.unwrap_err().message.contains("conflict"));
            }
        }
    }

    #[tokio::test]
    async fn funding_history_accepts_empty_history_but_rejects_http_errors_and_oversized_pages() {
        let transport = MockTransport::with_response(ok(funding_page(vec![], "")));
        let history = adapter(transport)
            .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
            .await
            .unwrap();
        assert!(history.payments.is_empty());
        let rows = (0..51)
            .map(|index| funding_row(&index.to_string(), FUNDING_START, "1", "2"))
            .collect();
        for response in [
            ok(funding_page(rows, "")),
            Ok(HttpResponse {
                status: 429,
                body: funding_page(vec![], ""),
            }),
            ok(
                r#"{"retCode":10001,"retMsg":"bad request","result":{"list":[],"nextPageCursor":""}}"#,
            ),
            Err(TransportError::Other("offline".into())),
        ] {
            let transport = MockTransport::with_response(response);
            assert!(
                adapter(transport)
                    .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn funding_history_fails_closed_at_request_cap_and_rejects_invalid_requests_locally() {
        let transport = MockTransport::default();
        for index in 0..256 {
            transport.push(ok(funding_page(vec![], &format!("cursor-{index}"))));
        }
        let error = adapter(transport.clone())
            .funding_history(Exchange::Bybit, "MUUSDT", FUNDING_START, FUNDING_START)
            .await
            .unwrap_err();
        assert!(error.message.contains("limit"));
        assert_eq!(transport.requests().len(), 256);
        let transport = MockTransport::default();
        let gateway = adapter(transport.clone());
        for (exchange, symbol, from, through) in [
            (Exchange::Binance, "MUUSDT", FUNDING_START, FUNDING_START),
            (Exchange::Bybit, "", FUNDING_START, FUNDING_START),
            (Exchange::Bybit, "MUUSDT", FUNDING_START + 1, FUNDING_START),
            (Exchange::Bybit, "MUUSDT", FUNDING_START, 1_700_000_000_124),
            (Exchange::Bybit, "MUUSDT", 0, FUNDING_START),
        ] {
            assert!(
                gateway
                    .funding_history(exchange, symbol, from, through)
                    .await
                    .is_err()
            );
        }
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn exact_lookup_exhausts_cursor_before_accepting_filled_500() {
        let transport = MockTransport::default();
        let mut page: serde_json::Value =
            serde_json::from_str(&order_result("Filled", "500", "505500")).unwrap();
        page["result"]["list"][0]["qty"] = serde_json::json!("500");
        page["result"]["nextPageCursor"] = serde_json::json!("opaque%3A500");
        transport.push(ok(page.to_string()));
        transport.push(ok(empty_order_result()));
        let result = adapter(transport.clone())
            .lookup_order_by_client_id(Exchange::Bybit, "MUUSDT", &intent().client_order_id)
            .await
            .unwrap();
        let OrderLookup::Found(order) = result else {
            panic!("exact filled order missing")
        };
        assert_eq!(order.shape.quantity, Decimal::from(500));
        assert_eq!(order.executed_quantity, Some(Decimal::from(500)));
        assert_eq!(
            order.lifecycle,
            OrderLifecycle::Terminal(TerminalOrderStatus::Filled)
        );
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].path, "/v5/order/realtime");
        assert!(
            requests[1]
                .query
                .contains(&("cursor".into(), "opaque%3A500".into()))
        );
        assert!(requests.iter().all(|r| r.method == HttpMethod::Get));
    }

    #[tokio::test]
    async fn exact_lookup_never_accepts_duplicate_foreign_malformed_or_unbounded_pages() {
        let row: serde_json::Value = serde_json::from_str(&order_result("New", "0", "0")).unwrap();
        let mut first = row.clone();
        first["result"]["nextPageCursor"] = serde_json::json!("page-2");
        let mut foreign = row.clone();
        foreign["result"]["list"][0]["orderLinkId"] = serde_json::json!("g_8_S_foreign");
        let mut malformed = row.clone();
        malformed["result"]["list"][0]["qty"] = serde_json::json!("broken");
        let mut bad_cursor =
            serde_json::from_str::<serde_json::Value>(&empty_order_result()).unwrap();
        bad_cursor["result"]["nextPageCursor"] = serde_json::json!({"cursor":"bad"});
        let mut repeated =
            serde_json::from_str::<serde_json::Value>(&empty_order_result()).unwrap();
        repeated["result"]["nextPageCursor"] = serde_json::json!("page-2");
        for second in [row, foreign, malformed, bad_cursor, repeated] {
            let transport = MockTransport::default();
            transport.push(ok(first.to_string()));
            transport.push(ok(second.to_string()));
            assert!(adapter(transport.clone()).lookup_order_by_client_id(
                Exchange::Bybit, "MUUSDT", &intent().client_order_id,
            ).await.is_err());
            assert_eq!(
                transport.requests().len(),
                2,
                "must validate the continuation, not reject every cursor"
            );
            assert!(
                transport
                    .requests()
                    .iter()
                    .all(|r| r.method == HttpMethod::Get)
            );
        }
    }

    #[tokio::test]
    async fn exact_lookup_cursor_bound_and_malformed_continuations_fail_closed() {
        let mut page = serde_json::from_str::<serde_json::Value>(&empty_order_result()).unwrap();
        let transport = MockTransport::default();
        for index in 0..20 {
            page["result"]["nextPageCursor"] = serde_json::json!(format!("page-{index}"));
            transport.push(ok(page.to_string()));
        }
        assert!(
            adapter(transport.clone())
                .lookup_order_by_client_id(Exchange::Bybit, "MUUSDT", &intent().client_order_id,)
                .await
                .is_err()
        );
        assert_eq!(transport.requests().len(), 20);
        for cursor in [
            serde_json::json!(17),
            serde_json::json!(["bad"]),
            serde_json::json!("\n"),
            serde_json::json!(" "),
            serde_json::json!("x".repeat(4097)),
        ] {
            let transport = MockTransport::default();
            page["result"]["nextPageCursor"] = cursor;
            transport.push(ok(page.to_string()));
            assert!(adapter(transport.clone()).lookup_order_by_client_id(
                Exchange::Bybit, "MUUSDT", &intent().client_order_id,
            ).await.is_err());
            assert_eq!(transport.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn bybit_rest_verified_snapshot_seeds_current_session_without_inventing_fills() {
        for (status, quantity, quote) in [("New", "0", "0"), ("Filled", "0.2", "202.2")] {
            let transport = MockTransport::default();
            transport.push(ok(order_result(status, quantity, quote)));
            if status == "Filled" {
                transport.push(ok(execution_result(
                    "seed-fill",
                    "0.2",
                    "202.2",
                    1_700_000_000_500,
                    "",
                )));
            }
            let gateway = adapter(transport.clone());
            let snapshot = gateway
                .execution_snapshot(
                    Exchange::Bybit,
                    "MUUSDT",
                    &intent().client_order_id,
                    "order-91",
                )
                .await
                .unwrap();
            let cached = gateway
                .realtime_execution_cache()
                .wait_snapshot(
                    "MUUSDT",
                    &intent().client_order_id,
                    "order-91",
                    Duration::ZERO,
                )
                .await;
            assert!(
                gateway
                    .realtime_execution_cache()
                    .knows_order("MUUSDT", "order-91"),
                "verified REST evidence must bind the same live session"
            );
            assert_eq!(
                cached, None,
                "REST evidence already returned to its caller must not become a second fresh WS read"
            );
            assert_eq!(
                snapshot.trades.len(),
                if status == "Filled" { 1 } else { 0 }
            );
            assert_eq!(
                transport.requests().len(),
                if status == "Filled" { 2 } else { 1 }
            );
        }
    }

    #[tokio::test]
    async fn bybit_rest_snapshot_started_before_reconnect_never_seeds_new_epoch() {
        let cache = BybitExecutionCache::default();
        let transport = MockTransport {
            reset_cache: Some(cache.clone()),
            ..MockTransport::default()
        };
        transport.push(ok(order_result("New", "0", "0")));
        let mut gateway = adapter(transport.clone());
        gateway.realtime_execution_cache = cache.clone();
        let snapshot = gateway
            .execution_snapshot(
                Exchange::Bybit,
                "MUUSDT",
                &intent().client_order_id,
                "order-91",
            )
            .await
            .unwrap();
        assert_eq!(snapshot.cumulative_quantity, Decimal::ZERO);
        assert!(!cache.knows_order("MUUSDT", "order-91"));
        assert!(
            cache
                .wait_snapshot(
                    "MUUSDT",
                    &intent().client_order_id,
                    "order-91",
                    Duration::ZERO
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_auth_frame_ack_preserves_qty_and_reduce_only() {
        use order_ws::test_support::{Reply, mock_relay};
        let fixture = mock_relay(Reply::Ack).await;
        fixture.ready().await;
        let transport = MockTransport::default();
        let mut gateway = adapter(transport.clone());
        gateway.set_recv_window_ms(8000);
        gateway.set_websocket_order_relay(fixture.relay.clone());
        let mut order = intent();
        order.shape.quantity = Decimal::from(500);
        order.shape.reduce_only = true;
        let ack = gateway.place_order(&order).await.unwrap();
        assert_eq!(ack.exchange_order_id, "ws-order-91");
        assert_eq!(ack.client_order_id, order.client_order_id);
        assert!(
            gateway
                .realtime_execution_cache()
                .knows_order("MUUSDT", "ws-order-91")
        );
        assert!(
            transport.requests().is_empty(),
            "accepted WS order must never reach REST"
        );
        // An acceptance acknowledgement must not fabricate a fill.
        if let Some(snapshot) = gateway
            .realtime_execution_cache()
            .wait_snapshot(
                "MUUSDT",
                &order.client_order_id,
                "ws-order-91",
                Duration::ZERO,
            )
            .await
        {
            assert_eq!(snapshot.cumulative_quantity, Decimal::ZERO);
            assert!(snapshot.trades.is_empty());
        }
        gateway.place_order(&order).await.unwrap();
        let frames = fixture.orders.lock().unwrap();
        assert_eq!(frames.len(), 2);
        let request = &frames[0];
        let id = request["reqId"].as_str().unwrap();
        assert!(!id.is_empty() && id.len() <= 36);
        assert_ne!(request["reqId"], frames[1]["reqId"]);
        assert!(
            request["header"]["X-BAPI-TIMESTAMP"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert_eq!(request["header"]["X-BAPI-RECV-WINDOW"], "8000");
        assert_eq!(
            request["args"],
            serde_json::json!([{
                "category":"linear", "symbol":"MUUSDT", "side":"Sell", "orderType":"Limit",
                "qty":"500", "price":"1011", "timeInForce":"PostOnly", "positionIdx":0,
                "orderLinkId":"g_7_S_fixed", "reduceOnly":true
            }])
        );
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_unknown_never_resends_or_falls_back() {
        use order_ws::test_support::{Reply, mock_relay};
        for reply in [
            Reply::Disconnect,
            Reply::Silence,
            Reply::ForeignClient,
            Reply::ForeignRequest,
            Reply::WrongOp,
            Reply::Malformed,
        ] {
            let fixture = mock_relay(reply).await;
            fixture.ready().await;
            let transport = MockTransport::default();
            let mut gateway = adapter(transport.clone());
            gateway.set_websocket_order_relay(fixture.relay.clone());
            assert!(matches!(
                gateway.place_order(&intent()).await,
                Err(PlacementError::Unknown { .. })
            ));
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert!(transport.requests().is_empty());
            assert_eq!(fixture.orders.lock().unwrap().len(), 1);
            if matches!(reply, Reply::Disconnect | Reply::Malformed) {
                assert!(
                    fixture
                        .connections
                        .load(std::sync::atomic::Ordering::Acquire)
                        >= 2,
                    "must verify no replay on a reauthenticated connection"
                );
            }
        }
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_auth_failure_is_not_sent_rest_fallback() {
        use order_ws::test_support::{Reply, mock_relay};
        let fixture = mock_relay(Reply::AuthRejected).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let transport = MockTransport::with_response(ok(
            r#"{"retCode":0,"result":{"orderId":"rest-91","orderLinkId":"g_7_S_fixed"}}"#,
        ));
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(fixture.relay.clone());
        assert_eq!(
            gateway
                .place_order(&intent())
                .await
                .unwrap()
                .exchange_order_id,
            "rest-91"
        );
        assert_eq!(transport.requests().len(), 1);
        assert!(fixture.orders.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_definitive_rejection_does_not_fallback() {
        use order_ws::test_support::{Reply, mock_relay};
        let fixture = mock_relay(Reply::Reject).await;
        fixture.ready().await;
        let transport = MockTransport::default();
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(fixture.relay.clone());
        assert!(matches!(
            gateway.place_order(&intent()).await,
            Err(PlacementError::Definitive { .. })
        ));
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_weak_lifetime_stops_writer_and_market_omits_price() {
        use order_ws::test_support::{Reply, mock_relay};
        let mut fixture = mock_relay(Reply::Ack).await;
        fixture.ready().await;
        let transport = MockTransport::default();
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(fixture.relay.clone());
        let mut order = intent();
        order.shape.kind = OrderKind::Market;
        order.shape.price = None;
        order.shape.time_in_force = TimeInForce::Gtc;
        order.shape.reduce_only = true;
        gateway.place_order(&order).await.unwrap();
        let payload = fixture.orders.lock().unwrap()[0]["args"][0].clone();
        assert_eq!(payload["orderType"], "Market");
        assert_eq!(payload["qty"], "0.2");
        assert_eq!(payload["reduceOnly"], true);
        assert!(payload.get("price").is_none());
        assert!(payload.get("timeInForce").is_none());
        fixture.stop().await;
        assert!(transport.requests().is_empty());
        assert_eq!(fixture.orders.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_heartbeat_keeps_authenticated_session_reusable() {
        use order_ws::test_support::{Reply, mock_relay};
        let mut fixture = mock_relay(Reply::Ack).await;
        fixture.ready().await;
        tokio::time::timeout(Duration::from_secs(25), async {
            while fixture.pings.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("persistent authenticated socket must emit an application heartbeat");
        let transport = MockTransport::default();
        let mut gateway = adapter(transport.clone());
        gateway.set_websocket_order_relay(fixture.relay.clone());
        gateway.place_order(&intent()).await.unwrap();
        assert_eq!(
            fixture
                .connections
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert!(transport.requests().is_empty());
        fixture.stop().await;
    }

    #[tokio::test]
    async fn bybit_ws_real_transport_logs_sanitized_transport_latency_and_failure_code() {
        use order_ws::test_support::{Reply, mock_relay};
        #[derive(Clone)]
        struct LogWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for LogWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = Arc::new(Mutex::new(Vec::new()));
        let output = logs.clone();
        // Avoid tracing's single-dispatch callsite fast path while parallel
        // tests register the same callsites without a thread-local subscriber.
        let _parallel_dispatch =
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || LogWriter(output.clone()))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        for reply in [Reply::Ack, Reply::Reject, Reply::AuthRejected] {
            let fixture = mock_relay(reply).await;
            if matches!(reply, Reply::AuthRejected) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            fixture.ready().await;
            let transport = MockTransport::default();
            let mut gateway = adapter(transport.clone());
            gateway.set_websocket_order_relay(fixture.relay.clone());
            let result = gateway.place_order(&intent()).await;
            assert!(result.is_ok() || matches!(result, Err(PlacementError::Definitive { .. })));
            assert!(transport.requests().is_empty());
        }
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("authenticated Bybit trading WebSocket connected"),
            "{logs}"
        );
        assert!(
            logs.contains("transport=\"websocket\"") && logs.contains("roundtrip_ms="),
            "{logs}"
        );
        assert!(
            logs.contains("g_7_S_fixed") && logs.contains("MUUSDT"),
            "{logs}"
        );
        assert!(
            logs.contains("ret_code=10001") && logs.contains("ret_code=10004"),
            "{logs}"
        );
        for secret in [
            "test-key",
            "test-secret",
            "GET/realtime",
            "\"args\"",
            "X-BAPI-SIGN",
        ] {
            assert!(
                !logs.contains(secret),
                "secret or raw frame appeared in logs"
            );
        }
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
    async fn open_order_progress_preserves_authoritative_partial_quantity() {
        let transport =
            MockTransport::with_response(ok(order_result("PartiallyFilled", "0.07", "70.77")));

        let progress = adapter(transport.clone())
            .open_order_execution_progress_snapshot(Exchange::Bybit, "MUUSDT")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(transport.requests()[0].path, "/v5/order/realtime");
        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0].cumulative_quantity, Decimal::new(7, 2));
        assert_eq!(progress[0].order.shape.quantity, Decimal::new(2, 1));
    }

    #[tokio::test]
    async fn bybit_open_pages_cold_seed_only_real_zero_new_orders() {
        for progress_only in [false, true] {
            let transport = MockTransport::default();
            let mut page: serde_json::Value =
                serde_json::from_str(&order_result("New", "0", "0")).unwrap();
            let mut partial: serde_json::Value =
                serde_json::from_str(&order_result("PartiallyFilled", "0.07", "70.77")).unwrap();
            partial["result"]["list"][0]["orderId"] = serde_json::json!("partial-92");
            partial["result"]["list"][0]["orderLinkId"] = serde_json::json!("g_8_S_partial");
            let mut manual = page["result"]["list"][0].clone();
            manual["orderId"] = serde_json::json!("manual-93");
            manual["orderLinkId"] = serde_json::json!("m_manual");
            page["result"]["list"]
                .as_array_mut()
                .unwrap()
                .extend([partial["result"]["list"][0].clone(), manual]);
            transport.push(ok(page.to_string()));
            let gateway = adapter(transport.clone());
            let cache = gateway.realtime_execution_cache();
            assert!(!cache.knows_order("MUUSDT", "order-91"));
            let orders = if progress_only {
                gateway
                    .open_order_execution_progress_snapshot(Exchange::Bybit, "MUUSDT")
                    .await
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .map(|progress| progress.order)
                    .collect::<Vec<_>>()
            } else {
                gateway
                    .open_orders_snapshot(Exchange::Bybit, "MUUSDT")
                    .await
                    .unwrap()
            };
            assert_eq!(orders.len(), 2, "manual namespace must remain excluded");
            assert!(
                cache.knows_order("MUUSDT", "order-91"),
                "complete zero New REST evidence must warm owned identity"
            );
            assert!(
                !cache.knows_order("MUUSDT", "partial-92"),
                "partial header without exact trades and fees must remain cold"
            );
            assert!(!cache.knows_order("MUUSDT", "manual-93"));
            assert!(
                cache
                    .wait_snapshot(
                        "MUUSDT",
                        &ClientOrderId::parse("g_8_S_partial").unwrap(),
                        "partial-92",
                        Duration::ZERO
                    )
                    .await
                    .is_none(),
                "partial REST headers cannot synthesize fills or fees"
            );
            assert!(
                cache
                    .wait_snapshot(
                        "MUUSDT",
                        &intent().client_order_id,
                        "order-91",
                        Duration::ZERO
                    )
                    .await
                    .is_none(),
                "returned REST evidence is already consumed"
            );
            assert_eq!(transport.requests().len(), 1);
            assert!(transport.requests()[0].raw_body.is_none());
            assert!(
                transport.requests()[0]
                    .headers
                    .iter()
                    .any(|(name, _)| name == "X-BAPI-SIGN")
            );
        }
    }

    #[tokio::test]
    async fn bybit_open_pages_never_seed_across_reconnect_or_incomplete_pagination() {
        for progress_only in [false, true] {
            for reset_session in [false, true] {
                let cache = BybitExecutionCache::default();
                let transport = MockTransport {
                    reset_cache: reset_session.then(|| cache.clone()),
                    ..MockTransport::default()
                };
                if reset_session {
                    transport.push(ok(order_result("New", "0", "0")));
                } else {
                    let mut page: serde_json::Value =
                        serde_json::from_str(&order_result("New", "0", "0")).unwrap();
                    page["result"]["nextPageCursor"] = serde_json::json!("page-2");
                    transport.push(ok(page.to_string()));
                    transport.push(ok(page.to_string()));
                }
                let mut gateway = adapter(transport.clone());
                gateway.realtime_execution_cache = cache.clone();
                let succeeded = if progress_only {
                    gateway
                        .open_order_execution_progress_snapshot(Exchange::Bybit, "MUUSDT")
                        .await
                        .is_ok()
                } else {
                    gateway
                        .open_orders_snapshot(Exchange::Bybit, "MUUSDT")
                        .await
                        .is_ok()
                };
                assert_eq!(succeeded, reset_session);
                assert!(
                    !cache.knows_order("MUUSDT", "order-91"),
                    "only a complete same-epoch scan may warm identity"
                );
                assert_eq!(
                    transport.requests().len(),
                    if reset_session { 1 } else { 2 }
                );
            }
        }
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
        let gateway = adapter(transport.clone());
        let acknowledgement = gateway.place_order(&intent()).await.unwrap();
        let request = &transport.requests()[0];
        let exact_body = r#"{"category":"linear","symbol":"MUUSDT","side":"Sell","orderType":"Limit","qty":"0.2","price":"1011","timeInForce":"PostOnly","positionIdx":0,"orderLinkId":"g_7_S_fixed","reduceOnly":false}"#;

        assert_eq!(acknowledgement.exchange_order_id, "order-91");
        assert!(
            gateway
                .realtime_execution_cache()
                .knows_order("MUUSDT", "order-91")
        );
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
    async fn bybit_start_signed_exact_symbol_preserves_cursor_and_fractional_leverage() {
        let transport = MockTransport::default();
        transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","list":[{"symbol":"QUSDT","lastPrice":"0.10413","markPrice":"0.104129"}]},"time":1700000000123}"#,
        ));
        transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","nextPageCursor":"QUSDT%2C0","list":[{"symbol":"QUSDT","positionIdx":0,"side":"","size":"0","avgPrice":"0","markPrice":"0.104129","unrealisedPnl":"","leverage":"12.5","positionStatus":"Normal","updatedTime":"1792296317559"}]},"time":1700000000123}"#,
        ));
        // Continuation placeholders must not overwrite the exact-symbol response.
        transport.push(ok(
            r#"{"retCode":0,"result":{"category":"linear","nextPageCursor":"","list":[{"symbol":"QUSDT","positionIdx":0,"side":"","size":"0","avgPrice":"","markPrice":"","unrealisedPnl":"","leverage":"10","positionStatus":"","updatedTime":""}]},"time":1700000000123}"#,
        ));
        let snapshot = adapter(transport.clone())
            .position_snapshot(Exchange::Bybit, "qusdt")
            .await
            .unwrap();
        assert_eq!(snapshot.one_way_position().unwrap(), (Decimal::ZERO, None));
        assert_eq!(
            snapshot.one_way_leverage_decimal().unwrap(),
            Decimal::new(125, 1)
        );
        assert_eq!(snapshot.legs[0].mark_price, Decimal::new(104129, 6));
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.method == HttpMethod::Get)
        );
        assert_eq!(requests[1].path, "/v5/position/list");
        assert_eq!(
            requests[1].query,
            vec![
                ("category".into(), "linear".into()),
                ("symbol".into(), "QUSDT".into())
            ]
        );
        assert!(
            requests[1]
                .headers
                .iter()
                .any(|(name, _)| name == "X-BAPI-SIGN")
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
