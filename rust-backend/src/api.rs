use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequestParts, OriginalUri, Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER, SET_COOKIE, WWW_AUTHENTICATE,
        },
        request::Parts,
    },
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::{
    domain::{Exchange, OrderSide},
    exchange::{
        ActiveOrderStatus, OrderLifecycle, PositionLeg, SnapshotError,
        registry::{ExchangeGatewayRegistry, ReadOnlyExchangeGateway, RegistryError},
    },
    persistence::{
        BeginIdempotency, FileIdempotencyStore, IdempotencyError, IdempotencyKey, IdempotencyStore,
        RequestFingerprint, StoredCommandResponse,
    },
    security::AdminTokenVerifier,
    web_auth::{
        SESSION_COOKIE_NAME, SESSION_TTL_SECONDS, WebAuthService, WebAuthUnavailable,
        WebAuthorizationError, WebLoginError, WebLoginOutcome,
    },
};

const MAX_CONTROL_BODY_BYTES: usize = 64 * 1_024;
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const IDEMPOTENCY_REPLAYED_HEADER: &str = "idempotency-replayed";

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    runtime: &'static str,
    trading_enabled: bool,
    contract_version: u8,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        runtime: "rust",
        trading_enabled: false,
        contract_version: 1,
    })
}

#[derive(Clone)]
enum AdminAuthentication {
    Unconfigured,
    Configured(AdminTokenVerifier),
}

#[derive(Clone)]
struct ApiState {
    authentication: AdminAuthentication,
    web_authentication: WebAuthService,
    trading_enabled: bool,
    idempotency: Arc<dyn IdempotencyStore>,
    start_command: Arc<dyn StartGridCommand>,
    exchange_gateways: ExchangeGatewayRegistry,
}

impl ApiState {
    fn disabled(
        admin_token: Option<AdminTokenVerifier>,
        web_authentication: WebAuthService,
        exchange_gateways: ExchangeGatewayRegistry,
        idempotency_root: PathBuf,
    ) -> Self {
        Self {
            authentication: admin_token.map_or(
                AdminAuthentication::Unconfigured,
                AdminAuthentication::Configured,
            ),
            web_authentication,
            trading_enabled: false,
            idempotency: Arc::new(FileIdempotencyStore::new(idempotency_root)),
            start_command: Arc::new(DisabledStartGridCommand),
            exchange_gateways,
        }
    }

    #[cfg(test)]
    fn for_test(
        admin_token: AdminTokenVerifier,
        trading_enabled: bool,
        idempotency: Arc<dyn IdempotencyStore>,
        start_command: Arc<dyn StartGridCommand>,
    ) -> Self {
        Self {
            authentication: AdminAuthentication::Configured(admin_token),
            web_authentication: WebAuthService::disabled(),
            trading_enabled,
            idempotency,
            start_command,
            exchange_gateways: ExchangeGatewayRegistry::default(),
        }
    }

    #[cfg(test)]
    fn with_web_authentication(mut self, web_authentication: WebAuthService) -> Self {
        self.web_authentication = web_authentication;
        self
    }

    #[cfg(test)]
    fn with_exchange_gateways(mut self, exchange_gateways: ExchangeGatewayRegistry) -> Self {
        self.exchange_gateways = exchange_gateways;
        self
    }
}

#[async_trait]
trait StartGridCommand: Send + Sync {
    async fn execute(&self, payload: Value)
    -> Result<StoredCommandResponse, CommandOutcomeUnknown>;
}

struct DisabledStartGridCommand;

#[async_trait]
impl StartGridCommand for DisabledStartGridCommand {
    async fn execute(
        &self,
        _payload: Value,
    ) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
        Err(CommandOutcomeUnknown)
    }
}

#[derive(Debug)]
struct CommandOutcomeUnknown;

struct AuthenticatedAdmin;

impl FromRequestParts<ApiState> for AuthenticatedAdmin {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        let AdminAuthentication::Configured(verifier) = &state.authentication else {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication_not_configured",
                "Rust write authentication is not configured",
            ));
        };

        let mut values = parts.headers.get_all(AUTHORIZATION).iter();
        let Some(value) = values.next() else {
            return Err(unauthorized());
        };
        if values.next().is_some() {
            return Err(unauthorized());
        }
        let Ok(value) = value.to_str() else {
            return Err(unauthorized());
        };
        let mut fields = value.split_ascii_whitespace();
        let Some(scheme) = fields.next() else {
            return Err(unauthorized());
        };
        let Some(token) = fields.next() else {
            return Err(unauthorized());
        };
        if fields.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") {
            return Err(unauthorized());
        }
        if !verifier.verify(token) {
            return Err(unauthorized());
        }
        Ok(Self)
    }
}

struct AuthenticatedWebSession;

impl FromRequestParts<ApiState> for AuthenticatedWebSession {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        state
            .web_authentication
            .authorize(&parts.headers)
            .map_err(web_authorization_error)?;
        Ok(Self)
    }
}

#[derive(Deserialize)]
struct WebLoginRequest {
    username: String,
    password: String,
    code: String,
}

async fn web_auth_status(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    match state.web_authentication.status(&headers) {
        Ok(status) => no_store_json(StatusCode::OK, status),
        Err(error) => web_auth_unavailable(error),
    }
}

async fn web_auth_login(
    State(state): State<ApiState>,
    Json(payload): Json<WebLoginRequest>,
) -> Response {
    let password = Zeroizing::new(payload.password);
    match state
        .web_authentication
        .login(&payload.username, &password, &payload.code)
    {
        Ok(WebLoginOutcome::AuthenticationDisabled) => no_store_json(
            StatusCode::OK,
            json!({"ok": true, "message": "Authentication is disabled"}),
        ),
        Ok(WebLoginOutcome::Authenticated { session_token }) => {
            let Some(cookie) = session_cookie(
                &session_token,
                state.web_authentication.cookie_secure(),
                SESSION_TTL_SECONDS,
            ) else {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "session_cookie_failed",
                    "The authenticated session could not be returned safely",
                );
            };
            let mut response =
                no_store_json(StatusCode::OK, json!({"ok": true, "message": "Logged in"}));
            response.headers_mut().insert(SET_COOKIE, cookie);
            response
        }
        Err(WebLoginError::NotConfigured) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_not_configured",
            "Authentication is required but not configured",
        ),
        Err(WebLoginError::InvalidCredentials) => api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "Invalid username, password, or code",
        ),
        Err(WebLoginError::RateLimited) => {
            let mut response = api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "login_rate_limited",
                "Too many login attempts; wait before trying again",
            );
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("60"));
            response
        }
        Err(WebLoginError::Unavailable(error)) => web_auth_unavailable(error),
    }
}

async fn web_auth_logout(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.web_authentication.logout(&headers) {
        return web_auth_unavailable(error);
    }
    let Some(cookie) = session_cookie("", state.web_authentication.cookie_secure(), 0) else {
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "session_cookie_failed",
            "The authenticated session could not be cleared safely",
        );
    };
    let mut response = no_store_json(StatusCode::OK, json!({"ok": true, "message": "Logged out"}));
    response.headers_mut().insert(SET_COOKIE, cookie);
    response
}

struct TradingEnabled;

impl FromRequestParts<ApiState> for TradingEnabled {
    type Rejection = Response;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        if !state.trading_enabled {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "rust_trading_disabled",
                "Rust trading writes remain disabled during migration",
            ));
        }
        Ok(Self)
    }
}

struct IdempotencyHeader(IdempotencyKey);

impl FromRequestParts<ApiState> for IdempotencyHeader {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        let mut values = parts.headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
        let Some(value) = values.next() else {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "idempotency_key_required",
                "A single Idempotency-Key header is required",
            ));
        };
        if values.next().is_some() {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "idempotency_key_invalid",
                "A single valid Idempotency-Key header is required",
            ));
        }
        let value = value.to_str().map_err(|_| {
            api_error(
                StatusCode::BAD_REQUEST,
                "idempotency_key_invalid",
                "A single valid Idempotency-Key header is required",
            )
        })?;
        let key = IdempotencyKey::parse(value).map_err(|_| {
            api_error(
                StatusCode::BAD_REQUEST,
                "idempotency_key_invalid",
                "A single valid Idempotency-Key header is required",
            )
        })?;
        Ok(Self(key))
    }
}

struct JsonContentType;

impl FromRequestParts<ApiState> for JsonContentType {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        let Some(value) = parts.headers.get(CONTENT_TYPE) else {
            return Err(unsupported_content_type());
        };
        let Ok(value) = value.to_str() else {
            return Err(unsupported_content_type());
        };
        let media_type = value.split(';').next().map(str::trim).unwrap_or_default();
        if !media_type.eq_ignore_ascii_case("application/json") {
            return Err(unsupported_content_type());
        }
        Ok(Self)
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_grid(
    _admin: AuthenticatedAdmin,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(Value::Object(object)) => Value::Object(object),
        Ok(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "The request body must be a JSON object",
            );
        }
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "The request body is not valid JSON",
            );
        }
    };

    let target = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or_else(|| uri.path());
    let fingerprint =
        match RequestFingerprint::new(method.as_str(), target, "application/json", &body) {
            Ok(fingerprint) => fingerprint,
            Err(_) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_target",
                    "The request target cannot be fingerprinted safely",
                );
            }
        };
    let started_at_ms = match unix_time_ms() {
        Ok(now) => now,
        Err(ClockUnavailable) => return clock_unavailable(),
    };

    let begin_store = Arc::clone(&state.idempotency);
    let begin_key = key.clone();
    let begin_fingerprint = fingerprint.clone();
    let begin = match tokio::task::spawn_blocking(move || {
        begin_store.begin(&begin_key, &begin_fingerprint, started_at_ms)
    })
    .await
    {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => return idempotency_error(error),
        Err(_) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "idempotency_unavailable",
                "The durable idempotency service is unavailable",
            );
        }
    };

    match begin {
        BeginIdempotency::InProgress => {
            return api_error(
                StatusCode::CONFLICT,
                "idempotency_outcome_unknown",
                "This request is already in progress or has an unknown outcome",
            );
        }
        BeginIdempotency::Completed(response) => {
            return stored_response(response, true);
        }
        BeginIdempotency::Started => {}
    }

    let response = match state.start_command.execute(payload).await {
        Ok(response) => response,
        Err(CommandOutcomeUnknown) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "command_outcome_unknown",
                "The command outcome is unknown and will not be retried automatically",
            );
        }
    };

    let completed_at_ms = unix_time_ms().unwrap_or(started_at_ms).max(started_at_ms);
    let complete_store = Arc::clone(&state.idempotency);
    let complete_key = key.clone();
    let complete_fingerprint = fingerprint.clone();
    let complete_response = response.clone();
    match tokio::task::spawn_blocking(move || {
        complete_store.complete(
            &complete_key,
            &complete_fingerprint,
            &complete_response,
            completed_at_ms,
        )
    })
    .await
    {
        Ok(Ok(_)) => stored_response(response, false),
        Ok(Err(_)) | Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "idempotency_completion_failed",
            "The command ran but its durable response could not be finalized",
        ),
    }
}

#[derive(Debug)]
struct ClockUnavailable;

fn unix_time_ms() -> Result<u64, ClockUnavailable> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ClockUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| ClockUnavailable)
}

fn clock_unavailable() -> Response {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "clock_unavailable",
        "The server clock is unavailable",
    )
}

fn unauthorized() -> Response {
    let mut response = api_error(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "A valid administrator bearer token is required",
    );
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"grid-trading-control\""),
    );
    response
}

fn unsupported_content_type() -> Response {
    api_error(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "json_content_type_required",
        "Content-Type must be application/json",
    )
}

fn idempotency_error(error: IdempotencyError) -> Response {
    match error {
        IdempotencyError::FingerprintConflict => api_error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "This idempotency key was already used for a different request",
        ),
        IdempotencyError::IncompleteReservation => api_error(
            StatusCode::CONFLICT,
            "idempotency_outcome_unknown",
            "This idempotency key has an incomplete request with an unknown outcome",
        ),
        _ => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "idempotency_unavailable",
            "The durable idempotency service is unavailable",
        ),
    }
}

fn stored_response(stored: StoredCommandResponse, replayed: bool) -> Response {
    let status = StatusCode::from_u16(stored.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Json(stored.body().clone())).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if replayed {
        response.headers_mut().insert(
            IDEMPOTENCY_REPLAYED_HEADER,
            HeaderValue::from_static("true"),
        );
    }
    response
}

fn no_store_json<T: Serialize>(status: StatusCode, body: T) -> Response {
    let mut response = (status, Json(body)).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn api_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let mut response = (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn web_auth_unavailable(error: WebAuthUnavailable) -> Response {
    tracing::warn!(error = %error, "web authentication service is unavailable");
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "authentication_unavailable",
        "The authentication service is unavailable",
    )
}

fn web_authorization_error(error: WebAuthorizationError) -> Response {
    match error {
        WebAuthorizationError::NotConfigured => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_not_configured",
            "Authentication is required but not configured",
        ),
        WebAuthorizationError::NotAuthenticated => api_error(
            StatusCode::UNAUTHORIZED,
            "authentication_required",
            "Authentication required",
        ),
        WebAuthorizationError::Unavailable(error) => web_auth_unavailable(error),
    }
}

fn session_cookie(token: &str, secure: bool, max_age: u64) -> Option<HeaderValue> {
    let mut value = Zeroizing::new(format!(
        "{SESSION_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}"
    ));
    if max_age == 0 {
        value.push_str("; Expires=Thu, 01 Jan 1970 00:00:00 GMT");
    }
    if secure {
        value.push_str("; Secure");
    }
    HeaderValue::from_str(&value).ok()
}

#[derive(Debug, Default, Deserialize)]
struct ExchangeSelection {
    exchange: Option<Exchange>,
}

async fn exchange_config(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
) -> Response {
    let preferred = state.exchange_gateways.preferred();
    let preferred_summary = state.exchange_gateways.summary(preferred);
    no_store_json(
        StatusCode::OK,
        json!({
            "configured": preferred_summary.configured,
            "exchange": preferred,
            "active_exchange": preferred,
            "testnet": preferred_summary.testnet,
            "configs": {
                "binance": state.exchange_gateways.summary(Exchange::Binance),
                "aster": state.exchange_gateways.summary(Exchange::Aster),
                "bybit": state.exchange_gateways.summary(Exchange::Bybit),
            }
        }),
    )
}

async fn exchange_balance(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Query(selection): Query<ExchangeSelection>,
) -> Response {
    let exchange = selected_exchange(&state, selection);
    let gateway = match read_gateway(&state, exchange) {
        Ok(gateway) => gateway,
        Err(error) => return error.response(),
    };
    match gateway.account_balance_snapshot(exchange).await {
        Ok(snapshot) => {
            if snapshot.exchange != exchange {
                return snapshot_failure(
                    "account balance",
                    exchange,
                    SnapshotError::new("account balance belongs to another exchange"),
                );
            }
            if let Err(error) = snapshot.validate() {
                return snapshot_failure("account balance", exchange, error);
            }
            no_store_json(
                StatusCode::OK,
                json!({
                    "exchange": snapshot.exchange,
                    "unit": snapshot.unit,
                    "available": snapshot.available_balance.to_string(),
                    "available_balance": snapshot.available_balance.to_string(),
                    "wallet_balance": snapshot.wallet_balance.to_string(),
                    "equity": snapshot.equity.to_string(),
                    "unrealised_pnl": snapshot.unrealized_profit.to_string(),
                    "source": "exchange",
                }),
            )
        }
        Err(error) => snapshot_failure("account balance", exchange, error),
    }
}

async fn exchange_price(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<ExchangeSelection>,
) -> Response {
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selected_exchange(&state, selection);
    let gateway = match read_gateway(&state, exchange) {
        Ok(gateway) => gateway,
        Err(error) => return error.response(),
    };
    match gateway.market_snapshot(exchange, &symbol).await {
        Ok(snapshot) => no_store_json(
            StatusCode::OK,
            json!({
                "exchange": snapshot.exchange,
                "symbol": snapshot.symbol,
                "last_price": snapshot.last_price.to_string(),
                "mark_price": snapshot.mark_price.to_string(),
                "observed_at_ms": snapshot.observed_at_ms,
            }),
        ),
        Err(error) => snapshot_failure("market", exchange, error),
    }
}

async fn exchange_fee_rates(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<ExchangeSelection>,
) -> Response {
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selected_exchange(&state, selection);
    let gateway = match read_gateway(&state, exchange) {
        Ok(gateway) => gateway,
        Err(error) => return error.response(),
    };
    match gateway.trading_fee_rates(exchange, &symbol).await {
        Ok(rates) => {
            let maker_fee_rate = match decimal_json_number(rates.maker_rate) {
                Ok(rate) => rate,
                Err(error) => return error.response(),
            };
            let taker_fee_rate = match decimal_json_number(rates.taker_rate) {
                Ok(rate) => rate,
                Err(error) => return error.response(),
            };
            no_store_json(
                StatusCode::OK,
                json!({
                    "exchange": rates.exchange,
                    "symbol": rates.symbol,
                    "maker_fee_rate": maker_fee_rate,
                    "taker_fee_rate": taker_fee_rate,
                    "source": "exchange",
                }),
            )
        }
        Err(error) => snapshot_failure("fee rates", exchange, error),
    }
}

async fn exchange_positions(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<ExchangeSelection>,
) -> Response {
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selected_exchange(&state, selection);
    let gateway = match read_gateway(&state, exchange) {
        Ok(gateway) => gateway,
        Err(error) => return error.response(),
    };
    match gateway.position_snapshot(exchange, &symbol).await {
        Ok(snapshot) => {
            let positions = snapshot
                .legs
                .iter()
                .filter_map(position_response)
                .collect::<Vec<_>>();
            no_store_json(StatusCode::OK, json!({"positions": positions}))
        }
        Err(error) => snapshot_failure("positions", exchange, error),
    }
}

async fn exchange_open_orders(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<ExchangeSelection>,
) -> Response {
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selected_exchange(&state, selection);
    let gateway = match read_gateway(&state, exchange) {
        Ok(gateway) => gateway,
        Err(error) => return error.response(),
    };
    match gateway.open_orders_snapshot(exchange, &symbol).await {
        Ok(snapshot) => {
            let mut orders = Vec::with_capacity(snapshot.len());
            for order in snapshot {
                let status = match order.lifecycle {
                    OrderLifecycle::Active(ActiveOrderStatus::New) => "NEW",
                    OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled) => {
                        "PARTIALLY_FILLED"
                    }
                    OrderLifecycle::Terminal(_) => {
                        return snapshot_failure(
                            "open orders",
                            exchange,
                            SnapshotError::new("open-order snapshot contained a terminal order"),
                        );
                    }
                };
                orders.push(json!({
                    "order_id": order.exchange_order_id,
                    "order_link_id": order.client_order_id.as_str(),
                    "side": order_side_name(order.shape.side),
                    "price": order.shape.price.map(|price| price.to_string()).unwrap_or_else(|| "0".into()),
                    "qty": order.shape.quantity.to_string(),
                    "status": status,
                    "reduce_only": order.shape.reduce_only,
                }));
            }
            no_store_json(
                StatusCode::OK,
                json!({"orders": orders, "scope": "strategy"}),
            )
        }
        Err(error) => snapshot_failure("open orders", exchange, error),
    }
}

fn selected_exchange(state: &ApiState, selection: ExchangeSelection) -> Exchange {
    selection
        .exchange
        .unwrap_or_else(|| state.exchange_gateways.preferred())
}

fn read_gateway(
    state: &ApiState,
    exchange: Exchange,
) -> Result<Arc<dyn ReadOnlyExchangeGateway>, ReadApiError> {
    state.exchange_gateways.gateway(exchange).map_err(|error| {
        if matches!(error, RegistryError::NotConfigured(_)) {
            ReadApiError::ExchangeNotConfigured
        } else {
            ReadApiError::RegistryUnavailable
        }
    })
}

fn normalize_symbol(value: &str) -> Result<String, ReadApiError> {
    if value.is_empty()
        || value.len() > 32
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(ReadApiError::InvalidSymbol);
    }
    Ok(value.to_ascii_uppercase())
}

fn decimal_json_number(value: rust_decimal::Decimal) -> Result<f64, ReadApiError> {
    value
        .to_string()
        .parse::<f64>()
        .map_err(|_| ReadApiError::InvalidExchangeDecimal)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadApiError {
    InvalidSymbol,
    ExchangeNotConfigured,
    RegistryUnavailable,
    InvalidExchangeDecimal,
}

impl ReadApiError {
    fn response(self) -> Response {
        match self {
            Self::InvalidSymbol => api_error(
                StatusCode::BAD_REQUEST,
                "invalid_symbol",
                "Symbol must contain only ASCII letters and digits",
            ),
            Self::ExchangeNotConfigured => api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "exchange_not_configured",
                "The selected exchange is not configured",
            ),
            Self::RegistryUnavailable => api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "exchange_registry_unavailable",
                "The exchange registry is unavailable",
            ),
            Self::InvalidExchangeDecimal => api_error(
                StatusCode::BAD_GATEWAY,
                "invalid_exchange_decimal",
                "The exchange returned an invalid decimal value",
            ),
        }
    }
}

fn position_response(leg: &PositionLeg) -> Option<Value> {
    if leg.signed_quantity.is_zero() {
        return None;
    }
    let side = if leg.signed_quantity.is_sign_positive() {
        "Buy"
    } else {
        "Sell"
    };
    Some(json!({
        "side": side,
        "size": leg.signed_quantity.abs().to_string(),
        "entry_price": leg.entry_price.map(|price| price.to_string()),
        "mark_price": leg.mark_price.to_string(),
        "unrealised_pnl": leg.unrealized_profit.to_string(),
        "leverage": leg.leverage,
    }))
}

fn order_side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn snapshot_failure(context: &'static str, exchange: Exchange, error: SnapshotError) -> Response {
    tracing::warn!(?exchange, context, error = %error, "exchange snapshot failed");
    api_error(
        StatusCode::BAD_GATEWAY,
        "exchange_snapshot_unavailable",
        "The exchange snapshot is unavailable or invalid",
    )
}

async fn api_not_found(_session: AuthenticatedWebSession) -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        "api_route_not_found",
        "API route not found",
    )
}

pub(crate) fn router(
    admin_token: Option<AdminTokenVerifier>,
    web_authentication: WebAuthService,
    exchange_gateways: ExchangeGatewayRegistry,
    idempotency_root: PathBuf,
) -> Router {
    router_with_state(ApiState::disabled(
        admin_token,
        web_authentication,
        exchange_gateways,
        idempotency_root,
    ))
}

fn router_with_state(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/api/auth/status", get(web_auth_status))
        .route("/api/auth/login", post(web_auth_login))
        .route("/api/auth/logout", post(web_auth_logout))
        .route("/api/config", get(exchange_config))
        .route("/api/balance", get(exchange_balance))
        .route("/api/price/{symbol}", get(exchange_price))
        .route("/api/fees/{symbol}", get(exchange_fee_rates))
        .route("/api/positions/{symbol}", get(exchange_positions))
        .route("/api/orders/open/{symbol}", get(exchange_open_orders))
        .route("/api/v1/grid/start", post(start_grid))
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .layer(DefaultBodyLimit::max(MAX_CONTROL_BODY_BYTES))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        body::{Body, to_bytes},
        http::{
            Request,
            header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE},
        },
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use zeroize::Zeroizing;

    use super::*;
    use crate::{
        domain::{ClientOrderId, OrderKind, OrderShape, TimeInForce},
        exchange::{
            AccountBalanceSnapshot, AccountBalanceSnapshotGateway, AccountBalanceUnit,
            AuthoritativeOrder, ExchangeIdentityGateway, ExchangeMarketSnapshot,
            MarketSnapshotGateway, OpenOrderSnapshotGateway, PositionSide, PositionSnapshot,
            PositionSnapshotGateway, TradingFeeRateGateway, TradingFeeRates,
            configured::ExchangeEnvironment,
        },
        web_auth::{
            WebAuthConfiguration,
            test_support::{PASSWORD, USERNAME, configured_service},
        },
    };

    const ADMIN_TOKEN: &str = "zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXY";
    const KEY: &str = "01J2X0W2F8E4Q8MNNNNNNNNNNN";

    fn verifier() -> AdminTokenVerifier {
        AdminTokenVerifier::from_secret(Zeroizing::new(ADMIN_TOKEN.to_owned())).unwrap()
    }

    fn request(body: &str, token: Option<&str>, key: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/grid/start")
            .header(CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(key) = key {
            builder = builder.header(IDEMPOTENCY_KEY_HEADER, key);
        }
        builder.body(Body::from(body.to_owned())).unwrap()
    }

    async fn response_json(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    struct CountingCommand {
        calls: AtomicUsize,
        response: StoredCommandResponse,
    }

    impl CountingCommand {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                response: StoredCommandResponse::new(
                    StatusCode::CREATED.as_u16(),
                    json!({"run_id": "run-safe-1"}),
                )
                .unwrap(),
            }
        }
    }

    #[async_trait]
    impl StartGridCommand for CountingCommand {
        async fn execute(
            &self,
            _payload: Value,
        ) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    struct UnknownCommand {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl StartGridCommand for UnknownCommand {
        async fn execute(
            &self,
            _payload: Value,
        ) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(CommandOutcomeUnknown)
        }
    }

    struct BlockingCommand {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
        response: StoredCommandResponse,
    }

    #[async_trait]
    impl StartGridCommand for BlockingCommand {
        async fn execute(
            &self,
            _payload: Value,
        ) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.response.clone())
        }
    }

    #[derive(Default)]
    struct CompletionFailureStore {
        fingerprint: Mutex<Option<RequestFingerprint>>,
    }

    struct BeginFailureStore;

    impl IdempotencyStore for BeginFailureStore {
        fn begin(
            &self,
            _key: &IdempotencyKey,
            _fingerprint: &RequestFingerprint,
            _started_at_ms: u64,
        ) -> Result<BeginIdempotency, IdempotencyError> {
            Err(IdempotencyError::CreateRoot(std::io::Error::other(
                "injected begin failure",
            )))
        }

        fn complete(
            &self,
            _key: &IdempotencyKey,
            _fingerprint: &RequestFingerprint,
            _response: &StoredCommandResponse,
            _completed_at_ms: u64,
        ) -> Result<crate::persistence::CompleteIdempotency, IdempotencyError> {
            unreachable!("a failed reservation must never reach completion")
        }
    }

    impl IdempotencyStore for CompletionFailureStore {
        fn begin(
            &self,
            _key: &IdempotencyKey,
            fingerprint: &RequestFingerprint,
            _started_at_ms: u64,
        ) -> Result<BeginIdempotency, IdempotencyError> {
            let mut stored = self.fingerprint.lock().unwrap();
            match &*stored {
                None => {
                    *stored = Some(fingerprint.clone());
                    Ok(BeginIdempotency::Started)
                }
                Some(existing) if existing == fingerprint => Ok(BeginIdempotency::InProgress),
                Some(_) => Err(IdempotencyError::FingerprintConflict),
            }
        }

        fn complete(
            &self,
            _key: &IdempotencyKey,
            _fingerprint: &RequestFingerprint,
            _response: &StoredCommandResponse,
            _completed_at_ms: u64,
        ) -> Result<crate::persistence::CompleteIdempotency, IdempotencyError> {
            Err(IdempotencyError::CommitRecord(std::io::Error::other(
                "injected completion failure",
            )))
        }
    }

    struct ExactReadGateway;

    impl ExchangeIdentityGateway for ExactReadGateway {
        fn exchange(&self) -> Exchange {
            Exchange::Aster
        }
    }

    #[async_trait]
    impl AccountBalanceSnapshotGateway for ExactReadGateway {
        async fn account_balance_snapshot(
            &self,
            exchange: Exchange,
        ) -> Result<AccountBalanceSnapshot, SnapshotError> {
            assert_eq!(exchange, Exchange::Aster);
            Ok(AccountBalanceSnapshot {
                exchange,
                unit: AccountBalanceUnit::Usdt,
                available_balance: Decimal::from_str_exact("120.10000000").unwrap(),
                wallet_balance: Decimal::from_str_exact("126.724692060").unwrap(),
                equity: Decimal::from_str_exact("126.720692060").unwrap(),
                unrealized_profit: Decimal::from_str_exact("-0.00400000").unwrap(),
            })
        }
    }

    #[async_trait]
    impl MarketSnapshotGateway for ExactReadGateway {
        async fn market_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
            assert_eq!(exchange, Exchange::Aster);
            assert_eq!(symbol, "ANSEMUSDT");
            Ok(ExchangeMarketSnapshot {
                exchange,
                symbol: symbol.into(),
                last_price: Decimal::from_str_exact("0.38000").unwrap(),
                mark_price: Decimal::from_str_exact("0.37990").unwrap(),
                observed_at_ms: 1_780_000_000_000,
            })
        }
    }

    #[async_trait]
    impl TradingFeeRateGateway for ExactReadGateway {
        async fn trading_fee_rates(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<TradingFeeRates, SnapshotError> {
            assert_eq!(exchange, Exchange::Aster);
            assert_eq!(symbol, "ANSEMUSDT");
            Ok(TradingFeeRates {
                exchange,
                symbol: symbol.into(),
                maker_rate: Decimal::from_str_exact("0.00020").unwrap(),
                taker_rate: Decimal::from_str_exact("0.00050").unwrap(),
            })
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for ExactReadGateway {
        async fn position_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            assert_eq!(exchange, Exchange::Aster);
            assert_eq!(symbol, "ANSEMUSDT");
            Ok(PositionSnapshot {
                exchange,
                symbol: symbol.into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: Decimal::from_str_exact("-1326.000").unwrap(),
                    entry_price: Some(Decimal::from_str_exact("0.40010").unwrap()),
                    mark_price: Decimal::from_str_exact("0.37990").unwrap(),
                    unrealized_profit: Decimal::from_str_exact("26.78660").unwrap(),
                    leverage: Some(5),
                }],
            })
        }
    }

    #[async_trait]
    impl OpenOrderSnapshotGateway for ExactReadGateway {
        async fn open_orders_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<Vec<AuthoritativeOrder>, SnapshotError> {
            assert_eq!(exchange, Exchange::Aster);
            assert_eq!(symbol, "ANSEMUSDT");
            Ok(vec![AuthoritativeOrder {
                client_order_id: ClientOrderId::parse("g_9_B_exact01").unwrap(),
                exchange_order_id: "90071992547409931234".into(),
                exchange,
                shape: OrderShape {
                    symbol: symbol.into(),
                    side: OrderSide::Buy,
                    price: Some(Decimal::from_str_exact("0.38000").unwrap()),
                    quantity: Decimal::from_str_exact("100.000").unwrap(),
                    reduce_only: true,
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::Gtc,
                },
                lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
            }])
        }
    }

    fn exact_read_app() -> Router {
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(ExactReadGateway),
                ExchangeEnvironment::Production,
                "env",
                Some("wallet configured".into()),
            )
            .unwrap();
        router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    std::env::temp_dir().join("grid-trading-api-read-tests"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways),
        )
    }

    #[tokio::test]
    async fn exchange_read_endpoints_preserve_exact_exchange_values() {
        let app = exact_read_app();

        let config = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(config.status(), StatusCode::OK);
        assert_eq!(config.headers()[CACHE_CONTROL], "no-store");
        let config = response_json(config).await;
        assert_eq!(config["active_exchange"], "aster");
        assert_eq!(config["configs"]["aster"]["configured"], true);
        assert_eq!(config["configs"]["aster"]["api_key"], "wallet configured");
        assert_eq!(config["configs"]["binance"]["configured"], false);

        let balance = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/balance?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(balance.status(), StatusCode::OK);
        assert_eq!(balance.headers()[CACHE_CONTROL], "no-store");
        let balance = response_json(balance).await;
        assert_eq!(balance["exchange"], "aster");
        assert_eq!(balance["unit"], "USDT");
        assert_eq!(balance["available"], "120.10000000");
        assert_eq!(balance["available_balance"], "120.10000000");
        assert_eq!(balance["wallet_balance"], "126.724692060");
        assert_eq!(balance["equity"], "126.720692060");
        assert_eq!(balance["unrealised_pnl"], "-0.00400000");
        assert_eq!(balance["source"], "exchange");

        let price = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/price/ansemusdt?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let price = response_json(price).await;
        assert_eq!(price["last_price"], "0.38000");
        assert_eq!(price["mark_price"], "0.37990");

        let fees = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/fees/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let fees = response_json(fees).await;
        assert_eq!(fees["maker_fee_rate"].as_f64(), Some(0.0002));
        assert_eq!(fees["taker_fee_rate"].as_f64(), Some(0.0005));

        let positions = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/positions/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let positions = response_json(positions).await;
        assert_eq!(positions["positions"][0]["side"], "Sell");
        assert_eq!(positions["positions"][0]["size"], "1326.000");
        assert_eq!(positions["positions"][0]["entry_price"], "0.40010");
        assert_eq!(positions["positions"][0]["unrealised_pnl"], "26.78660");

        let orders = app
            .oneshot(
                Request::builder()
                    .uri("/api/orders/open/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let orders = response_json(orders).await;
        assert_eq!(orders["scope"], "strategy");
        assert_eq!(orders["orders"][0]["order_id"], "90071992547409931234");
        assert_eq!(orders["orders"][0]["price"], "0.38000");
        assert_eq!(orders["orders"][0]["qty"], "100.000");
        assert_eq!(orders["orders"][0]["reduce_only"], true);
    }

    #[tokio::test]
    async fn exchange_read_endpoints_fail_closed_for_bad_symbols_and_missing_config() {
        let invalid = exact_read_app()
            .oneshot(
                Request::builder()
                    .uri("/api/price/ANSEM-USDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(invalid).await["error"]["code"],
            "invalid_symbol"
        );

        let directory = tempdir().unwrap();
        let unconfigured = router_with_state(ApiState::for_test(
            verifier(),
            false,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            Arc::new(DisabledStartGridCommand),
        ))
        .oneshot(
            Request::builder()
                .uri("/api/price/MUUSDT?exchange=binance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(unconfigured.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(unconfigured).await["error"]["code"],
            "exchange_not_configured"
        );
    }

    #[tokio::test]
    async fn migration_server_is_explicitly_non_trading() {
        let response = super::super::app()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let payload = response_json(response).await;
        assert_eq!(payload["runtime"], "rust");
        assert_eq!(payload["trading_enabled"], false);
        assert_eq!(payload["contract_version"], 1);
    }

    #[tokio::test]
    async fn disabled_web_authentication_is_explicit_and_cache_safe() {
        let response = super::super::app()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            response_json(response).await,
            json!({
                "required": false,
                "configured": false,
                "authenticated": true,
                "username": null,
            })
        );
    }

    #[tokio::test]
    async fn required_but_incomplete_web_authentication_fails_closed() {
        let directory = tempdir().unwrap();
        let web_auth = WebAuthService::from_configuration(WebAuthConfiguration {
            required: true,
            username: USERNAME.to_owned(),
            password_hash: Zeroizing::new(String::new()),
            totp_secret: Zeroizing::new(String::new()),
            cookie_secure: true,
        })
        .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(directory.path())),
                Arc::new(DisabledStartGridCommand),
            )
            .with_web_authentication(web_auth),
        );

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        assert_eq!(response_json(status).await["configured"], false);

        let protected = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(protected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(protected).await["error"]["code"],
            "authentication_not_configured"
        );
    }

    #[tokio::test]
    async fn web_login_cookie_authenticates_and_logout_revokes_the_session() {
        let directory = tempdir().unwrap();
        let (web_auth, code) = configured_service(59, true);
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(directory.path())),
                Arc::new(DisabledStartGridCommand),
            )
            .with_web_authentication(web_auth),
        );
        let login = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/auth/login")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"username": USERNAME, "password": PASSWORD, "code": code})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(login.status(), StatusCode::OK);
        assert_eq!(login.headers()[CACHE_CONTROL], "no-store");
        let set_cookie = login.headers()[SET_COOKIE].to_str().unwrap().to_owned();
        assert!(set_cookie.starts_with(&format!("{SESSION_COOKIE_NAME}=")));
        assert!(set_cookie.contains("; Path=/"));
        assert!(set_cookie.contains("; HttpOnly"));
        assert!(set_cookie.contains("; SameSite=Strict"));
        assert!(set_cookie.contains("; Max-Age=43200"));
        assert!(set_cookie.ends_with("; Secure"));
        let cookie = set_cookie.split(';').next().unwrap().to_owned();
        let login_body = response_json(login).await;
        assert_eq!(login_body["ok"], true);
        assert!(!login_body.to_string().contains(&code));

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .header(COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let status = response_json(status).await;
        assert_eq!(status["authenticated"], true);
        assert_eq!(status["username"], USERNAME);

        let protected = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/not-migrated-yet")
                    .header(COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(protected.status(), StatusCode::NOT_FOUND);

        let logout = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/auth/logout")
                    .header(COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logout.status(), StatusCode::OK);
        let cleared = logout.headers()[SET_COOKIE].to_str().unwrap();
        assert!(cleared.contains("grid_session=;"));
        assert!(cleared.contains("Max-Age=0"));
        assert!(cleared.ends_with("; Secure"));

        let revoked = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn failed_web_login_is_generic_and_never_sets_a_cookie() {
        let directory = tempdir().unwrap();
        let (web_auth, code) = configured_service(59, false);
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(directory.path())),
                Arc::new(DisabledStartGridCommand),
            )
            .with_web_authentication(web_auth),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/auth/login")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"username": USERNAME, "password": "wrong", "code": code})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!response.headers().contains_key(SET_COOKIE));
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], "invalid_credentials");
        assert_eq!(
            body["error"]["message"],
            "Invalid username, password, or code"
        );
    }

    #[tokio::test]
    async fn web_session_configuration_never_replaces_bearer_control_authentication() {
        let directory = tempdir().unwrap();
        let web_auth = WebAuthService::from_configuration(WebAuthConfiguration {
            required: true,
            username: USERNAME.to_owned(),
            password_hash: Zeroizing::new(String::new()),
            totp_secret: Zeroizing::new(String::new()),
            cookie_secure: true,
        })
        .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(directory.path())),
                Arc::new(DisabledStartGridCommand),
            )
            .with_web_authentication(web_auth),
        );

        let response = app
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "rust_trading_disabled"
        );
    }

    #[tokio::test]
    async fn legacy_mutating_route_remains_absent_until_compatible() {
        let response = super::super::app()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
    }

    #[tokio::test]
    async fn unconfigured_authentication_fails_closed() {
        let response = super::super::app()
            .oneshot(request("not-json", None, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "authentication_not_configured"
        );
    }

    #[tokio::test]
    async fn authentication_and_disabled_gate_run_before_body_processing() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let state = ApiState::for_test(
            verifier(),
            false,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        );
        let app = router_with_state(state);

        let missing = app
            .clone()
            .oneshot(request("not-json", None, None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert!(missing.headers().contains_key(WWW_AUTHENTICATE));

        let wrong = app
            .clone()
            .oneshot(request(
                "not-json",
                Some("zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXx"),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        let disabled = app
            .oneshot(request("not-json", Some(ADMIN_TOKEN), None))
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(disabled).await["error"]["code"],
            "rust_trading_disabled"
        );
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn enabled_control_plane_requires_valid_key_content_type_and_json() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));

        let missing_key = app
            .clone()
            .oneshot(request("{}", Some(ADMIN_TOKEN), None))
            .await
            .unwrap();
        assert_eq!(missing_key.status(), StatusCode::BAD_REQUEST);

        let bad_json = app
            .clone()
            .oneshot(request("not-json", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(bad_json.status(), StatusCode::BAD_REQUEST);

        let no_content_type = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/grid/start")
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(IDEMPOTENCY_KEY_HEADER, KEY)
            .body(Body::from("{}"))
            .unwrap();
        let no_content_type = app.oneshot(no_content_type).await.unwrap();
        assert_eq!(no_content_type.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn duplicate_auth_headers_and_oversized_bodies_never_execute() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));

        let duplicate_auth = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/grid/start")
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(IDEMPOTENCY_KEY_HEADER, KEY)
            .body(Body::from("{}"))
            .unwrap();
        let duplicate_auth = app.clone().oneshot(duplicate_auth).await.unwrap();
        assert_eq!(duplicate_auth.status(), StatusCode::UNAUTHORIZED);

        let oversized = format!(r#"{{"padding":"{}"}}"#, "x".repeat(MAX_CONTROL_BODY_BYTES));
        let oversized = app
            .oneshot(request(&oversized, Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn completed_request_is_replayed_without_running_command_twice() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));

        let first = app
            .clone()
            .oneshot(request(
                r#"{"symbol":"MUUSDT"}"#,
                Some(ADMIN_TOKEN),
                Some(KEY),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(!first.headers().contains_key(IDEMPOTENCY_REPLAYED_HEADER));

        let replay = app
            .oneshot(request(
                r#"{"symbol":"MUUSDT"}"#,
                Some(ADMIN_TOKEN),
                Some(KEY),
            ))
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(replay.headers()[IDEMPOTENCY_REPLAYED_HEADER], "true");
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn changed_request_with_same_key_is_rejected_without_execution() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));
        app.clone()
            .oneshot(request(
                r#"{"symbol":"MUUSDT"}"#,
                Some(ADMIN_TOKEN),
                Some(KEY),
            ))
            .await
            .unwrap();

        let conflict = app
            .oneshot(request(
                r#"{"symbol":"ANSEMUSDT"}"#,
                Some(ADMIN_TOKEN),
                Some(KEY),
            ))
            .await
            .unwrap();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(conflict).await["error"]["code"],
            "idempotency_conflict"
        );
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn overlapping_same_key_executes_exactly_once() {
        let directory = tempdir().unwrap();
        let command = Arc::new(BlockingCommand {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
            response: StoredCommandResponse::new(201, json!({"ok": true})).unwrap(),
        });
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));

        let first_app = app.clone();
        let first = tokio::spawn(async move {
            first_app
                .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
                .await
                .unwrap()
        });
        command.started.notified().await;

        let overlap = app
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(overlap.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(overlap).await["error"]["code"],
            "idempotency_outcome_unknown"
        );
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);

        command.release.notify_one();
        assert_eq!(first.await.unwrap().status(), StatusCode::CREATED);
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unknown_command_outcome_is_never_reexecuted() {
        let directory = tempdir().unwrap();
        let command = Arc::new(UnknownCommand {
            calls: AtomicUsize::new(0),
        });
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(FileIdempotencyStore::new(directory.path())),
            command.clone(),
        ));

        let first = app
            .clone()
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(first).await["error"]["code"],
            "command_outcome_unknown"
        );

        let retry = app
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::CONFLICT);
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completion_failure_leaves_request_blocked_and_never_reexecutes() {
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(CompletionFailureStore::default()),
            command.clone(),
        ));

        let first = app
            .clone()
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(first).await["error"]["code"],
            "idempotency_completion_failed"
        );

        let retry = app
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::CONFLICT);
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reservation_failure_prevents_command_execution() {
        let command = Arc::new(CountingCommand::new());
        let app = router_with_state(ApiState::for_test(
            verifier(),
            true,
            Arc::new(BeginFailureStore),
            command.clone(),
        ));

        let response = app
            .oneshot(request("{}", Some(ADMIN_TOKEN), Some(KEY)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "idempotency_unavailable"
        );
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }
}
