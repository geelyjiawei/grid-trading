use std::{
    collections::BTreeSet,
    future::Future,
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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::{
    domain::{Exchange, GridConfig, OrderSide},
    engine::{
        ArmedStrategyLifecycle, FeeValuationSource, MarketSnapshot, PreparedStrategyKind,
        PreparedStrategyLifecycle, PreparedStrategyStopOutcome, RuntimeCoordinator,
        RuntimeCoordinatorError, RuntimeRegistryEntry, ShadowCollectionError, StrategyLifecycle,
        StrategyOrderPurpose, StrategyState, StrategyTransition, build_grid_preview,
        collect_stable_exchange_view, collect_strategy_shadow_view, load_authoritative_fee_config,
    },
    exchange::{
        ActiveOrderStatus, AuthoritativeOrder, InstrumentRulesGateway, MarketSnapshotGateway,
        OrderLifecycle, PositionLeg, PositionSnapshot, SnapshotError,
        configured::SharedConfiguredExchangeGateway,
        registry::{ExchangeGatewayRegistry, ReadOnlyExchangeGateway, RegistryError},
    },
    persistence::{
        BeginIdempotency, FileIdempotencyStore, IdempotencyError, IdempotencyKey, IdempotencyStore,
        RequestFingerprint, StoredCommandResponse, StrategyCatalog, StrategyCatalogSnapshot,
        load_strategy_catalog,
    },
    security::AdminTokenVerifier,
    web_auth::{
        SESSION_COOKIE_NAME, SESSION_TTL_SECONDS, WebAuthService, WebAuthUnavailable,
        WebAuthorizationError, WebLoginError, WebLoginOutcome,
    },
};

const MAX_CONTROL_BODY_BYTES: usize = 64 * 1_024;
const PREVIEW_MARKET_MAX_AGE_MS: u64 = 15_000;
const PREVIEW_MARKET_FUTURE_SKEW_MS: u64 = 1_000;
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const IDEMPOTENCY_REPLAYED_HEADER: &str = "idempotency-replayed";

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    runtime: &'static str,
    trading_enabled: bool,
    contract_version: u8,
}

async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        runtime: "rust",
        trading_enabled: state.trading_enabled,
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
    runtime: Option<Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>>,
    exchange_gateways: ExchangeGatewayRegistry,
    strategy_root: PathBuf,
}

impl ApiState {
    fn disabled(
        admin_token: Option<AdminTokenVerifier>,
        web_authentication: WebAuthService,
        exchange_gateways: ExchangeGatewayRegistry,
        idempotency_root: PathBuf,
        strategy_root: PathBuf,
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
            runtime: None,
            exchange_gateways,
            strategy_root,
        }
    }

    #[cfg(test)]
    fn for_test(
        admin_token: AdminTokenVerifier,
        trading_enabled: bool,
        idempotency: Arc<dyn IdempotencyStore>,
        start_command: Arc<dyn StartGridCommand>,
    ) -> Self {
        let strategy_root = std::env::temp_dir().join("grid-trading-api-test-strategies");
        Self {
            authentication: AdminAuthentication::Configured(admin_token),
            web_authentication: WebAuthService::disabled(),
            trading_enabled,
            idempotency,
            start_command,
            runtime: None,
            exchange_gateways: ExchangeGatewayRegistry::default(),
            strategy_root,
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

    #[cfg(test)]
    fn with_strategy_root(mut self, strategy_root: PathBuf) -> Self {
        self.strategy_root = strategy_root;
        self
    }

    #[cfg(test)]
    fn with_runtime(
        mut self,
        runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    ) -> Self {
        self.trading_enabled = true;
        self.runtime = Some(runtime);
        self
    }

    fn enabled(
        admin_token: Option<AdminTokenVerifier>,
        web_authentication: WebAuthService,
        exchange_gateways: ExchangeGatewayRegistry,
        idempotency_root: PathBuf,
        strategy_root: PathBuf,
        runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    ) -> Self {
        let start_command = Arc::new(RuntimeStartGridCommand {
            runtime: Arc::clone(&runtime),
            exchange_gateways: exchange_gateways.clone(),
        });
        Self {
            authentication: admin_token.map_or(
                AdminAuthentication::Unconfigured,
                AdminAuthentication::Configured,
            ),
            web_authentication,
            trading_enabled: true,
            idempotency: Arc::new(FileIdempotencyStore::new(idempotency_root)),
            start_command,
            runtime: Some(runtime),
            exchange_gateways,
            strategy_root,
        }
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

struct RuntimeStartGridCommand {
    runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    exchange_gateways: ExchangeGatewayRegistry,
}

#[async_trait]
impl StartGridCommand for RuntimeStartGridCommand {
    async fn execute(
        &self,
        payload: Value,
    ) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
        let config = match serde_json::from_value::<GridConfig>(payload) {
            Ok(config) => config,
            Err(_) => {
                return command_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_grid_config",
                    "The grid configuration is invalid",
                );
            }
        };
        let Some(exchange) = config.exchange else {
            return command_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "missing_exchange",
                "The grid exchange is required",
            );
        };
        let gateway = match self.exchange_gateways.trading_gateway(exchange) {
            Ok(gateway) => gateway,
            Err(_) => {
                return command_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "exchange_trading_unavailable",
                    "The selected exchange trading gateway is not configured",
                );
            }
        };
        let now_ms = unix_time_ms().map_err(|_| CommandOutcomeUnknown)?;
        match self.runtime.start(gateway, config, now_ms).await {
            Ok(receipt) => StoredCommandResponse::new(
                StatusCode::ACCEPTED.as_u16(),
                json!({
                    "ok": true,
                    "message": "The strategy is durably persisted and scheduled",
                    "run_id": receipt.run_id.as_str(),
                    "exchange": receipt.exchange,
                    "symbol": receipt.symbol,
                    "lifecycle": receipt.lifecycle,
                }),
            )
            .map_err(|_| CommandOutcomeUnknown),
            Err(
                RuntimeCoordinatorError::InvalidConfig(_)
                | RuntimeCoordinatorError::MissingExchange
                | RuntimeCoordinatorError::GatewayMismatch { .. }
                | RuntimeCoordinatorError::UnsupportedQuoteAsset { .. },
            ) => command_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_grid_config",
                "The grid configuration is invalid or unsupported",
            ),
            Err(RuntimeCoordinatorError::MarketAlreadyOwned { .. }) => command_response(
                StatusCode::CONFLICT,
                "grid_already_running",
                "A strategy already owns this exchange and symbol",
            ),
            Err(
                RuntimeCoordinatorError::RegistrationInvariant
                | RuntimeCoordinatorError::ConcurrentMarketOwner { .. },
            ) => Err(CommandOutcomeUnknown),
            Err(_) => command_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "grid_start_unavailable",
                "The strategy could not be started safely",
            ),
        }
    }
}

fn command_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
    StoredCommandResponse::new(
        status.as_u16(),
        json!({"ok": false, "error": {"code": code, "message": message}}),
    )
    .map_err(|_| CommandOutcomeUnknown)
}

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
async fn start_grid_admin(
    _admin: AuthenticatedAdmin,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    execute_start_grid(state, key, method, uri, body).await
}

#[allow(clippy::too_many_arguments)]
async fn start_grid_web(
    _session: AuthenticatedWebSession,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    execute_start_grid(state, key, method, uri, body).await
}

async fn execute_start_grid(
    state: ApiState,
    key: IdempotencyKey,
    method: Method,
    uri: axum::http::Uri,
    body: Bytes,
) -> Response {
    let payload = match parse_json_object(&body) {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    let command = Arc::clone(&state.start_command);
    run_idempotent_command(state, key, method, uri, body, async move {
        command.execute(payload).await
    })
    .await
}

fn parse_json_object(body: &[u8]) -> Result<Value, Box<Response>> {
    let payload = match serde_json::from_slice(body) {
        Ok(Value::Object(object)) => Value::Object(object),
        Ok(_) => {
            return Err(Box::new(api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "The request body must be a JSON object",
            )));
        }
        Err(_) => {
            return Err(Box::new(api_error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "The request body is not valid JSON",
            )));
        }
    };
    Ok(payload)
}

async fn run_idempotent_command<F>(
    state: ApiState,
    key: IdempotencyKey,
    method: Method,
    uri: axum::http::Uri,
    body: Bytes,
    command: F,
) -> Response
where
    F: Future<Output = Result<StoredCommandResponse, CommandOutcomeUnknown>>,
{
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

    let response = match command.await {
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

#[allow(clippy::too_many_arguments)]
async fn stop_grid_web(
    _session: AuthenticatedWebSession,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<ExchangeSelection>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    if let Err(response) = parse_json_object(&body) {
        return *response;
    }
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selected_exchange(&state, selection);
    execute_stop_grid(state, key, method, uri, body, exchange, symbol).await
}

#[allow(clippy::too_many_arguments)]
async fn stop_only_grid_web(
    _session: AuthenticatedWebSession,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    if let Err(response) = parse_json_object(&body) {
        return *response;
    }
    let (catalog, _) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let live = catalog
        .entries()
        .iter()
        .filter(|entry| entry.is_live())
        .map(|entry| (entry.exchange(), entry.symbol().to_owned()))
        .collect::<Vec<_>>();
    let [(exchange, symbol)] = live.as_slice() else {
        return if live.is_empty() {
            api_error(
                StatusCode::BAD_REQUEST,
                "grid_not_running",
                "No running strategy exists",
            )
        } else {
            api_error(
                StatusCode::BAD_REQUEST,
                "multiple_grids_running",
                "Multiple strategies are running; stop one by exchange and symbol",
            )
        };
    };
    execute_stop_grid(state, key, method, uri, body, *exchange, symbol.clone()).await
}

#[allow(clippy::too_many_arguments)]
async fn stop_all_grids_web(
    _session: AuthenticatedWebSession,
    _trading: TradingEnabled,
    IdempotencyHeader(key): IdempotencyHeader,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    if let Err(response) = parse_json_object(&body) {
        return *response;
    }
    let (catalog, _) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let mut targets = catalog
        .entries()
        .iter()
        .filter(|entry| entry.is_live())
        .map(|entry| (entry.exchange(), entry.symbol().to_owned()))
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "grid_not_running",
            "No running strategy exists",
        );
    }
    targets.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| (left.0 as u8).cmp(&(right.0 as u8)))
    });
    let Some(runtime) = state.runtime.clone() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_unavailable",
            "The Rust trading runtime is unavailable",
        );
    };
    let now_ms = match unix_time_ms() {
        Ok(now_ms) => now_ms,
        Err(ClockUnavailable) => return clock_unavailable(),
    };
    run_idempotent_command(state, key, method, uri, body, async move {
        let mut stopped = Vec::with_capacity(targets.len());
        for (exchange, symbol) in targets {
            match runtime.request_stop(exchange, &symbol, now_ms).await {
                Ok(receipt) => {
                    let lifecycle = stop_lifecycle(&receipt.outcome)?;
                    stopped.push(json!({
                        "run_id": receipt.run_id.as_str(),
                        "exchange": receipt.exchange,
                        "symbol": receipt.symbol,
                        "lifecycle": lifecycle,
                    }));
                }
                Err(error) if stopped.is_empty() => return stop_error_response(error),
                Err(_) => return Err(CommandOutcomeUnknown),
            }
        }
        StoredCommandResponse::new(
            StatusCode::ACCEPTED.as_u16(),
            json!({
                "ok": true,
                "message": "All stop requests are durable; strategy orders will be cancelled without closing positions",
                "count": stopped.len(),
                "strategies": stopped,
            }),
        )
        .map_err(|_| CommandOutcomeUnknown)
    })
    .await
}

async fn execute_stop_grid(
    state: ApiState,
    key: IdempotencyKey,
    method: Method,
    uri: axum::http::Uri,
    body: Bytes,
    exchange: Exchange,
    symbol: String,
) -> Response {
    let Some(runtime) = state.runtime.clone() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime_unavailable",
            "The Rust trading runtime is unavailable",
        );
    };
    let now_ms = match unix_time_ms() {
        Ok(now_ms) => now_ms,
        Err(ClockUnavailable) => return clock_unavailable(),
    };
    run_idempotent_command(state, key, method, uri, body, async move {
        match runtime.request_stop(exchange, &symbol, now_ms).await {
            Ok(receipt) => stop_response(receipt),
            Err(error) => stop_error_response(error),
        }
    })
    .await
}

fn stop_error_response(
    error: RuntimeCoordinatorError,
) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
    match error {
        RuntimeCoordinatorError::MarketNotRunning { .. } => command_response(
            StatusCode::NOT_FOUND,
            "grid_not_running",
            "No running strategy owns the selected exchange and symbol",
        ),
        RuntimeCoordinatorError::CatalogTask
        | RuntimeCoordinatorError::CatalogLease(_)
        | RuntimeCoordinatorError::Catalog(_)
        | RuntimeCoordinatorError::CatalogAnomalies { .. }
        | RuntimeCoordinatorError::CatalogSelection(_)
        | RuntimeCoordinatorError::RegistryCatalogMismatch { .. } => command_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "strategy_state_inconsistent",
            "The strategy state could not be reconciled safely",
        ),
        _ => Err(CommandOutcomeUnknown),
    }
}

fn stop_lifecycle(
    outcome: &PreparedStrategyStopOutcome,
) -> Result<&'static str, CommandOutcomeUnknown> {
    match outcome {
        PreparedStrategyStopOutcome::ArmedCancelled => Ok("cancelled"),
        PreparedStrategyStopOutcome::Active(StrategyTransition::LifecycleChanged {
            lifecycle: StrategyLifecycle::StopRequested,
        }) => Ok("stop_requested"),
        PreparedStrategyStopOutcome::Active(StrategyTransition::NoChange) => Ok("unchanged"),
        PreparedStrategyStopOutcome::Active(_) => Err(CommandOutcomeUnknown),
    }
}

fn stop_response(
    receipt: crate::engine::RuntimeStopReceipt,
) -> Result<StoredCommandResponse, CommandOutcomeUnknown> {
    let lifecycle = stop_lifecycle(&receipt.outcome)?;
    StoredCommandResponse::new(
        StatusCode::ACCEPTED.as_u16(),
        json!({
            "ok": true,
            "message": "The stop request is durable; strategy orders will be cancelled without closing positions",
            "run_id": receipt.run_id.as_str(),
            "exchange": receipt.exchange,
            "symbol": receipt.symbol,
            "lifecycle": lifecycle,
        }),
    )
    .map_err(|_| CommandOutcomeUnknown)
}

async fn grid_status(_session: AuthenticatedWebSession, State(state): State<ApiState>) -> Response {
    let (catalog, runtime_entries) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let mut grids = Vec::new();
    for snapshot in catalog.entries().iter().filter(|entry| entry.is_live()) {
        let runtime_entry = runtime_entries
            .iter()
            .find(|entry| entry.run_id.as_str() == snapshot.run_id());
        match strategy_status_response(snapshot, runtime_entry) {
            Ok(response) => grids.push(response),
            Err(response) => return *response,
        }
    }
    no_store_json(
        StatusCode::OK,
        json!({
            "running": !grids.is_empty(),
            "count": grids.len(),
            "running_count": grids.len(),
            "trading_enabled": state.trading_enabled,
            "grids": grids,
        }),
    )
}

async fn grid_symbol_status(
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
    let (catalog, runtime_entries) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let selected = match catalog.select_live(exchange, &symbol) {
        Ok(selected) => selected,
        Err(error) => {
            tracing::error!(?exchange, symbol, error = %error, "strategy status selection is ambiguous");
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "strategy_selection_ambiguous",
                "Multiple live strategies exist for this exchange and symbol",
            );
        }
    };
    let Some(snapshot) = selected else {
        return no_store_json(
            StatusCode::OK,
            json!({
                "running": false,
                "engine_running": false,
                "exchange": exchange,
                "symbol": symbol,
                "trading_enabled": state.trading_enabled,
            }),
        );
    };
    let runtime_entry = runtime_entries
        .iter()
        .find(|entry| entry.run_id.as_str() == snapshot.run_id());
    match strategy_status_response(&snapshot, runtime_entry) {
        Ok(response) => no_store_json(StatusCode::OK, response),
        Err(response) => *response,
    }
}

async fn grid_preview(
    _session: AuthenticatedWebSession,
    _content_type: JsonContentType,
    State(state): State<ApiState>,
    body: Bytes,
) -> Response {
    let payload = match parse_json_object(&body) {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    let config = match serde_json::from_value::<GridConfig>(payload) {
        Ok(config) => config,
        Err(error) => {
            tracing::debug!(error = %error, "grid preview request could not be decoded");
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_grid_config",
                "The grid configuration is invalid",
            );
        }
    };
    if let Err(error) = config.validate() {
        tracing::debug!(error = %error, "grid preview configuration failed validation");
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_grid_config",
            "The grid configuration is invalid",
        );
    }
    let Some(exchange) = config.exchange else {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "missing_exchange",
            "The grid exchange is required",
        );
    };
    let gateway = match state.exchange_gateways.trading_gateway(exchange) {
        Ok(gateway) => gateway,
        Err(error) => {
            tracing::warn!(?exchange, error = %error, "grid preview gateway is unavailable");
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "exchange_preview_unavailable",
                "The selected exchange is not configured for an authoritative preview",
            );
        }
    };
    let now_ms = match unix_time_ms() {
        Ok(now_ms) => now_ms,
        Err(ClockUnavailable) => return clock_unavailable(),
    };
    let (authoritative, market, rules) = tokio::join!(
        load_authoritative_fee_config(&gateway, &config),
        gateway.market_snapshot(exchange, &config.symbol),
        gateway.instrument_rules(exchange, &config.symbol),
    );
    let authoritative = match authoritative {
        Ok(authoritative) => authoritative,
        Err(error) => {
            tracing::warn!(?exchange, symbol = config.symbol, error = %error, "authoritative preview fee rates failed");
            return api_error(
                StatusCode::BAD_GATEWAY,
                "fee_snapshot_unavailable",
                "The exchange fee-rate snapshot is unavailable or invalid",
            );
        }
    };
    let market = match market {
        Ok(market) => market,
        Err(error) => return snapshot_failure("grid preview market", exchange, error),
    };
    if market.exchange != exchange || market.symbol != config.symbol {
        return snapshot_failure(
            "grid preview market",
            exchange,
            SnapshotError::new("market snapshot identity mismatch"),
        );
    }
    if let Err(error) = market.ensure_fresh(
        now_ms,
        PREVIEW_MARKET_MAX_AGE_MS,
        PREVIEW_MARKET_FUTURE_SKEW_MS,
    ) {
        return snapshot_failure("grid preview market", exchange, error);
    }
    let rules = match rules {
        Ok(rules) => rules,
        Err(error) => return snapshot_failure("grid preview instrument", exchange, error),
    };
    let preview = match build_grid_preview(
        &authoritative.config,
        &MarketSnapshot {
            last_price: market.last_price,
            mark_price: market.mark_price,
        },
        &rules,
        authoritative.rates.maker_rate,
        authoritative.rates.taker_rate,
    ) {
        Ok(preview) => preview,
        Err(error) => {
            tracing::debug!(?exchange, symbol = config.symbol, error = %error, "grid preview could not produce an exchange-valid plan");
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "grid_plan_invalid",
                "The requested grid cannot produce an exchange-valid order plan",
            );
        }
    };
    let representative = &preview.representative_cycle;
    let per_grid_fee = match representative
        .open_fee
        .checked_add(representative.close_fee)
    {
        Some(fee) => fee,
        None => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "preview_numeric_overflow",
                "The grid preview arithmetic overflowed",
            );
        }
    };
    let opening_order = preview.plan.opening_order.as_ref().map(|order| {
        json!({
            "side": order_side_name(order.side),
            "price": order.price.map(|price| price.to_string()),
            "qty": order.quantity.to_string(),
            "kind": order.kind,
            "time_in_force": order.time_in_force,
        })
    });
    let grid_orders = preview
        .plan
        .grid_orders
        .iter()
        .map(|order| {
            json!({
                "level_index": order.level_index,
                "side": order_side_name(order.side),
                "price": order.price.to_string(),
                "qty": order.quantity.to_string(),
                "reduce_only": order.reduce_only,
                "time_in_force": order.time_in_force,
                "role": order.role,
            })
        })
        .collect::<Vec<_>>();
    let cycles = preview
        .cycles
        .iter()
        .map(|cycle| {
            json!({
                "level_index": cycle.level_index,
                "qty": cycle.quantity.to_string(),
                "entry_price": cycle.entry_price.to_string(),
                "exit_price": cycle.exit_price.to_string(),
                "gross_profit": cycle.gross_profit.to_string(),
                "open_fee": cycle.open_fee.to_string(),
                "close_fee": cycle.close_fee.to_string(),
                "net_profit": cycle.net_profit.to_string(),
                "gross_profit_pct": cycle.gross_profit_percent.to_string(),
                "fee_rate": cycle.fee_rate.to_string(),
                "liquidity_estimate": if cycle.maker_only { "maker" } else { "taker_conservative" },
            })
        })
        .collect::<Vec<_>>();
    no_store_json(
        StatusCode::OK,
        json!({
            "exchange": exchange,
            "symbol": config.symbol,
            "reference_price": preview.plan.reference_price.to_string(),
            "grid_step": representative.grid_step.to_string(),
            "grid_step_min": preview.grid_step_min.to_string(),
            "grid_step_max": preview.grid_step_max.to_string(),
            "grid_profit_pct": representative.gross_profit_percent.to_string(),
            "grid_profit_pct_min": preview.gross_profit_percent_min.to_string(),
            "grid_profit_pct_max": preview.gross_profit_percent_max.to_string(),
            "per_grid_gross_profit": representative.gross_profit.to_string(),
            "per_grid_open_fee": representative.open_fee.to_string(),
            "per_grid_close_fee": representative.close_fee.to_string(),
            "per_grid_fee": per_grid_fee.to_string(),
            "per_grid_net_profit": representative.net_profit.to_string(),
            "per_grid_net_profit_min": preview.net_profit_min.to_string(),
            "per_grid_net_profit_max": preview.net_profit_max.to_string(),
            "active_grid_count": preview.plan.active_grid_count,
            "participating_level_count": preview.plan.participating_level_count,
            "grid_count": config.grid_count,
            "qty_per_grid_min": preview.quantity_min.to_string(),
            "qty_per_grid_max": preview.quantity_max.to_string(),
            "qty_per_grid_avg": preview.quantity_average.to_string(),
            "total_qty": preview.plan.total_quantity.to_string(),
            "min_notional": rules.min_notional.to_string(),
            "maker_fee_rate": authoritative.rates.maker_rate.to_string(),
            "taker_fee_rate": authoritative.rates.taker_rate.to_string(),
            "fee_rate_source": "exchange",
            "fee_estimate_liquidity": if representative.maker_only { "maker" } else { "taker_conservative" },
            "initial_open_fee_rate": preview.initial_open_fee_rate.map(|rate| rate.to_string()),
            "initial_open_fee": preview.initial_open_fee.map(|fee| fee.to_string()),
            "opening_order": opening_order,
            "grid_orders": grid_orders,
            "cycles": cycles,
        }),
    )
}

async fn strategy_trades(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(selection): Query<TradeSelection>,
) -> Response {
    let symbol = match normalize_symbol(&symbol) {
        Ok(symbol) => symbol,
        Err(error) => return error.response(),
    };
    let exchange = selection
        .exchange
        .unwrap_or_else(|| state.exchange_gateways.preferred());
    let limit = match validated_limit(selection.limit, 100, 1_000) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let (catalog, _) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let selected = match catalog.select_live(exchange, &symbol) {
        Ok(Some(snapshot)) => Some(snapshot),
        Ok(None) => catalog
            .entries()
            .iter()
            .filter(|entry| entry.exchange() == exchange && entry.symbol() == symbol)
            .max_by_key(|entry| strategy_snapshot_updated_at(entry))
            .cloned(),
        Err(error) => {
            tracing::error!(?exchange, symbol, error = %error, "multiple live strategies prevent an authoritative trade view");
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "strategy_selection_ambiguous",
                "Multiple live strategies exist for this exchange and symbol",
            );
        }
    };
    let rows = match selected.as_ref() {
        Some(StrategyCatalogSnapshot::Active(strategy)) => {
            match strategy_trade_rows(strategy, limit) {
                Ok(rows) => rows,
                Err(response) => return *response,
            }
        }
        Some(StrategyCatalogSnapshot::Armed(_)) | None => Vec::new(),
    };
    no_store_json(
        StatusCode::OK,
        json!({
            "exchange": exchange,
            "symbol": symbol,
            "scope": "strategy",
            "source": "durable_exchange_execution_audit",
            "count": rows.len(),
            "trades": rows,
        }),
    )
}

async fn strategy_history(
    _session: AuthenticatedWebSession,
    State(state): State<ApiState>,
    Query(selection): Query<HistorySelection>,
) -> Response {
    let limit = match validated_limit(selection.limit, 100, 1_000) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let (catalog, _) = match stable_runtime_catalog(&state).await {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };
    let mut rows = Vec::with_capacity(catalog.entries().len());
    for snapshot in catalog.entries() {
        match strategy_history_row(snapshot) {
            Ok(row) => rows.push(row),
            Err(response) => return *response,
        }
    }
    rows.sort_by(|left, right| {
        right["started_at"]
            .as_u64()
            .unwrap_or_default()
            .cmp(&left["started_at"].as_u64().unwrap_or_default())
            .then_with(|| right["run_id"].as_str().cmp(&left["run_id"].as_str()))
    });
    rows.truncate(limit);
    no_store_json(
        StatusCode::OK,
        json!({
            "source": "durable_strategy_state",
            "count": rows.len(),
            "runs": rows,
        }),
    )
}

fn validated_limit(
    requested: Option<usize>,
    default: usize,
    maximum: usize,
) -> Result<usize, Box<Response>> {
    let limit = requested.unwrap_or(default);
    if limit == 0 || limit > maximum {
        return Err(Box::new(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            "The requested result limit is outside the supported range",
        )));
    }
    Ok(limit)
}

fn strategy_snapshot_updated_at(snapshot: &StrategyCatalogSnapshot) -> u64 {
    match snapshot {
        StrategyCatalogSnapshot::Armed(state) => state.updated_at_ms,
        StrategyCatalogSnapshot::Active(state) => state.updated_at_ms,
    }
}

fn strategy_trade_rows(
    strategy: &StrategyState,
    limit: usize,
) -> Result<Vec<StrategyTradeRow>, Box<Response>> {
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for order in strategy.orders.values() {
        let Some(audit) = &order.execution_audit else {
            continue;
        };
        for trade in &audit.snapshot.trades {
            if !seen.insert(trade.trade_id.clone()) {
                return Err(Box::new(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "duplicate_trade_audit",
                    "The durable execution audit contains a duplicate trade identity",
                )));
            }
            let valuations = audit
                .fee_valuations
                .iter()
                .filter(|valuation| valuation.trade_id == trade.trade_id)
                .collect::<Vec<_>>();
            let [valuation] = valuations.as_slice() else {
                return Err(Box::new(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "invalid_trade_audit",
                    "The durable execution audit does not contain one exact fee valuation",
                )));
            };
            let profit = trade
                .realized_profit
                .checked_sub(valuation.quote_value)
                .ok_or_else(|| {
                    Box::new(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "trade_numeric_overflow",
                        "The durable execution audit arithmetic overflowed",
                    ))
                })?;
            let level_idx = match &order.purpose {
                StrategyOrderPurpose::InitialGrid { level_index, .. }
                | StrategyOrderPurpose::Replacement { level_index, .. } => Some(*level_index),
                StrategyOrderPurpose::Opening | StrategyOrderPurpose::RiskClose => None,
            };
            rows.push(StrategyTradeRow {
                order_id: trade.exchange_order_id.clone(),
                order_link_id: order.client_order_id.as_str().to_owned(),
                trade_id: trade.trade_id.clone(),
                side: order_side_name(trade.side),
                price: trade.price.to_string(),
                qty: trade.quantity.to_string(),
                volume: trade.quote_quantity.to_string(),
                fee: trade.commission_cost.to_string(),
                fee_usdt: valuation.quote_value.to_string(),
                fee_asset: trade.commission_asset.clone(),
                fee_source: fee_valuation_source_name(valuation.source),
                liquidity: if trade.is_maker { "maker" } else { "taker" },
                is_maker: trade.is_maker,
                realized_pnl: trade.realized_profit.to_string(),
                profit: profit.to_string(),
                reduce_only: order.shape.reduce_only,
                level_idx,
                time: trade.trade_time_ms,
            });
        }
    }
    rows.sort_by(|left, right| {
        right
            .time
            .cmp(&left.time)
            .then_with(|| right.trade_id.cmp(&left.trade_id))
            .then_with(|| right.order_id.cmp(&left.order_id))
    });
    rows.truncate(limit);
    Ok(rows)
}

fn fee_valuation_source_name(source: FeeValuationSource) -> &'static str {
    match source {
        FeeValuationSource::ExchangeZero => "exchange_zero",
        FeeValuationSource::QuoteAsset => "quote_asset",
        FeeValuationSource::HistoricalMinuteOpen => "historical_minute_open",
    }
}

fn strategy_history_row(snapshot: &StrategyCatalogSnapshot) -> Result<Value, Box<Response>> {
    match snapshot {
        StrategyCatalogSnapshot::Armed(state) => Ok(json!({
            "run_id": state.run_id.as_str(),
            "started_at": state.created_at_ms,
            "updated_at": state.updated_at_ms,
            "symbol": state.symbol,
            "exchange": state.exchange,
            "direction": state.config.direction,
            "grid_mode": state.config.grid_mode,
            "grid_count": state.config.grid_count,
            "initial_order_type": state.config.initial_order_type,
            "initial_order_price": state.config.initial_order_price.map(|value| value.to_string()),
            "position_sizing_mode": state.config.position_sizing_mode,
            "grid_order_qty": state.config.grid_order_qty.map(|value| value.to_string()),
            "total_investment": state.config.total_investment.to_string(),
            "status": state.lifecycle,
            "realized_net_profit": "0",
            "net_profit": "0",
            "total_fee": "0",
            "total_volume": "0",
            "completed_pairs": 0,
        })),
        StrategyCatalogSnapshot::Active(state) => {
            let net_profit = state
                .gross_realized_profit
                .checked_sub(state.total_fee)
                .ok_or_else(|| {
                    Box::new(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "strategy_history_invalid",
                        "The durable strategy profit arithmetic overflowed",
                    ))
                })?;
            Ok(json!({
                "run_id": state.run_id.as_str(),
                "started_at": state.created_at_ms,
                "updated_at": state.updated_at_ms,
                "symbol": state.symbol,
                "exchange": state.exchange,
                "direction": state.direction,
                "grid_mode": state.config.grid_mode,
                "grid_count": state.config.grid_count,
                "initial_order_type": state.config.initial_order_type,
                "initial_order_price": state.config.initial_order_price.map(|value| value.to_string()),
                "position_sizing_mode": state.config.position_sizing_mode,
                "grid_order_qty": state.config.grid_order_qty.map(|value| value.to_string()),
                "total_investment": state.config.total_investment.to_string(),
                "status": state.lifecycle,
                "gross_profit": state.gross_realized_profit.to_string(),
                "realized_net_profit": net_profit.to_string(),
                "net_profit": net_profit.to_string(),
                "total_fee": state.total_fee.to_string(),
                "total_volume": state.total_volume.to_string(),
                "completed_pairs": state.completed_pairs,
                "baseline_position": state.baseline.signed_quantity.to_string(),
                "grid_position_net_qty": state.grid_position_net_quantity.to_string(),
            }))
        }
    }
}

async fn stable_runtime_catalog(
    state: &ApiState,
) -> Result<(StrategyCatalog, Vec<RuntimeRegistryEntry>), Response> {
    for _ in 0..3 {
        let before = load_catalog_snapshot(state.strategy_root.clone()).await?;
        let runtime_entries = match &state.runtime {
            Some(runtime) => runtime.entries().await,
            None => Vec::new(),
        };
        let after = load_catalog_snapshot(state.strategy_root.clone()).await?;
        if before != after {
            continue;
        }
        if !after.anomalies().is_empty() {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "strategy_catalog_anomaly",
                "The strategy catalog contains unresolved anomalies",
            ));
        }
        if state.runtime.is_some() && !runtime_catalog_matches(&after, &runtime_entries) {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_catalog_mismatch",
                "The runtime registry and durable strategy catalog do not agree",
            ));
        }
        return Ok((after, runtime_entries));
    }
    Err(api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "strategy_status_changed",
        "The strategy state changed while it was being read; retry the status request",
    ))
}

async fn load_catalog_snapshot(root: PathBuf) -> Result<StrategyCatalog, Response> {
    match tokio::task::spawn_blocking(move || load_strategy_catalog(root)).await {
        Ok(Ok(catalog)) => Ok(catalog),
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "strategy catalog could not be read");
            Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "strategy_catalog_unavailable",
                "The strategy catalog is unavailable",
            ))
        }
        Err(error) => {
            tracing::error!(error = %error, "strategy catalog task failed");
            Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "strategy_catalog_unavailable",
                "The strategy catalog is unavailable",
            ))
        }
    }
}

fn runtime_catalog_matches(
    catalog: &StrategyCatalog,
    runtime_entries: &[RuntimeRegistryEntry],
) -> bool {
    let live = catalog
        .entries()
        .iter()
        .filter(|entry| entry.is_live())
        .collect::<Vec<_>>();
    if live.len() != runtime_entries.len() {
        return false;
    }
    live.iter().all(|snapshot| {
        runtime_entries.iter().any(|entry| {
            runtime_entry_identity_matches(snapshot, entry)
                && runtime_entry_snapshot_matches(snapshot, entry)
        })
    })
}

fn runtime_entry_identity_matches(
    snapshot: &StrategyCatalogSnapshot,
    entry: &RuntimeRegistryEntry,
) -> bool {
    entry.run_id.as_str() == snapshot.run_id()
        && entry.exchange == snapshot.exchange()
        && entry.symbol == snapshot.symbol()
}

fn runtime_entry_snapshot_matches(
    snapshot: &StrategyCatalogSnapshot,
    entry: &RuntimeRegistryEntry,
) -> bool {
    entry.advancing
        || (entry.kind == Some(prepared_kind(snapshot))
            && entry.lifecycle == Some(prepared_lifecycle(snapshot)))
}

fn prepared_kind(snapshot: &StrategyCatalogSnapshot) -> PreparedStrategyKind {
    match snapshot {
        StrategyCatalogSnapshot::Armed(_) => PreparedStrategyKind::Armed,
        StrategyCatalogSnapshot::Active(_) => PreparedStrategyKind::Active,
    }
}

fn prepared_lifecycle(snapshot: &StrategyCatalogSnapshot) -> PreparedStrategyLifecycle {
    match snapshot {
        StrategyCatalogSnapshot::Armed(state) => match state.lifecycle {
            ArmedStrategyLifecycle::WaitingTrigger => PreparedStrategyLifecycle::WaitingTrigger,
            ArmedStrategyLifecycle::Cancelled => PreparedStrategyLifecycle::Cancelled,
        },
        StrategyCatalogSnapshot::Active(state) => match state.lifecycle {
            StrategyLifecycle::AwaitingOpening => PreparedStrategyLifecycle::AwaitingOpening,
            StrategyLifecycle::DeployingGrid => PreparedStrategyLifecycle::DeployingGrid,
            StrategyLifecycle::Running => PreparedStrategyLifecycle::Running,
            StrategyLifecycle::RiskExitRequested => PreparedStrategyLifecycle::RiskExitRequested,
            StrategyLifecycle::StopRequested => PreparedStrategyLifecycle::StopRequested,
            StrategyLifecycle::Stopped => PreparedStrategyLifecycle::Stopped,
            StrategyLifecycle::Failed => PreparedStrategyLifecycle::Failed,
            StrategyLifecycle::Closed => PreparedStrategyLifecycle::Closed,
        },
    }
}

fn strategy_status_response(
    snapshot: &StrategyCatalogSnapshot,
    runtime_entry: Option<&RuntimeRegistryEntry>,
) -> Result<Value, Box<Response>> {
    let engine_running = runtime_entry.is_some();
    let runtime_advancing = runtime_entry.is_some_and(|entry| entry.advancing);
    match snapshot {
        StrategyCatalogSnapshot::Armed(state) => Ok(json!({
            "run_id": state.run_id.as_str(),
            "exchange": state.exchange,
            "symbol": state.symbol,
            "running": state.lifecycle == ArmedStrategyLifecycle::WaitingTrigger,
            "engine_running": engine_running,
            "runtime_advancing": runtime_advancing,
            "lifecycle": state.lifecycle,
            "direction": state.config.direction,
            "grid_mode": state.config.grid_mode,
            "grid_count": state.config.grid_count,
            "lower_price": state.config.lower_price.to_string(),
            "upper_price": state.config.upper_price.to_string(),
            "waiting_trigger": true,
            "waiting_initial_order": false,
            "trigger_price": state.trigger_price.to_string(),
            "trigger_message": "Waiting for the configured trigger price",
            "grid_ready": false,
            "completed_pairs": 0,
            "gross_profit": "0",
            "total_fee": "0",
            "realized_net_profit": "0",
            "total_profit": "0",
            "total_volume": "0",
            "grid_position_net_qty": "0",
            "created_at_ms": state.created_at_ms,
            "updated_at_ms": state.updated_at_ms,
        })),
        StrategyCatalogSnapshot::Active(state) => {
            let expected_position = state.expected_exchange_position().map_err(|error| {
                tracing::error!(run_id = state.run_id.as_str(), error = %error, "persisted strategy position arithmetic failed");
                Box::new(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "strategy_state_invalid",
                    "The persisted strategy state is invalid",
                ))
            })?;
            let realized_net = state
                .gross_realized_profit
                .checked_sub(state.total_fee)
                .ok_or_else(|| {
                    Box::new(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "strategy_state_invalid",
                        "The persisted strategy profit arithmetic overflowed",
                    ))
                })?;
            let baseline_side = if state.baseline.signed_quantity.is_sign_positive() {
                Some("Buy")
            } else if state.baseline.signed_quantity.is_sign_negative() {
                Some("Sell")
            } else {
                None
            };
            Ok(json!({
                "run_id": state.run_id.as_str(),
                "exchange": state.exchange,
                "symbol": state.symbol,
                "running": !matches!(state.lifecycle, StrategyLifecycle::Stopped | StrategyLifecycle::Closed),
                "engine_running": engine_running,
                "runtime_advancing": runtime_advancing,
                "lifecycle": state.lifecycle,
                "direction": state.direction,
                "grid_mode": state.config.grid_mode,
                "grid_count": state.config.grid_count,
                "active_grid_count": state.plan.active_grid_count,
                "participating_level_count": state.plan.participating_level_count,
                "lower_price": state.config.lower_price.to_string(),
                "upper_price": state.config.upper_price.to_string(),
                "reference_price": state.plan.reference_price.to_string(),
                "waiting_trigger": false,
                "waiting_initial_order": state.lifecycle == StrategyLifecycle::AwaitingOpening,
                "grid_ready": state.initial_deployment_complete,
                "completed_pairs": state.completed_pairs,
                "gross_profit": state.gross_realized_profit.to_string(),
                "total_fee": state.total_fee.to_string(),
                "realized_net_profit": realized_net.to_string(),
                "total_profit": realized_net.to_string(),
                "total_volume": state.total_volume.to_string(),
                "baseline_position": {
                    "side": baseline_side,
                    "qty": state.baseline.signed_quantity.abs().to_string(),
                    "signed_qty": state.baseline.signed_quantity.to_string(),
                    "entry_price": state.baseline.entry_price.map(|price| price.to_string()),
                },
                "grid_position_net_qty": state.grid_position_net_quantity.to_string(),
                "expected_position_net_qty": expected_position.to_string(),
                "opening_filled_qty": state.opening_filled_quantity.to_string(),
                "planned_total_qty": state.plan.total_quantity.to_string(),
                "failure": state.failure,
                "created_at_ms": state.created_at_ms,
                "updated_at_ms": state.updated_at_ms,
            }))
        }
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

#[derive(Debug, Default, Deserialize)]
struct TradeSelection {
    exchange: Option<Exchange>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct HistorySelection {
    limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyTradeRow {
    order_id: String,
    order_link_id: String,
    trade_id: String,
    side: &'static str,
    price: String,
    qty: String,
    volume: String,
    fee: String,
    fee_usdt: String,
    fee_asset: String,
    fee_source: &'static str,
    liquidity: &'static str,
    is_maker: bool,
    realized_pnl: String,
    profit: String,
    reduce_only: bool,
    level_idx: Option<u16>,
    time: u64,
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

#[derive(Debug)]
struct RiskStrategyObservation {
    selected: Option<StrategyCatalogSnapshot>,
    catalog_anomaly_count: usize,
    catalog_problem: Option<&'static str>,
    runtime: RuntimeMarketObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeMarketObservation {
    configured: bool,
    engine_running: bool,
    advancing: bool,
    market_entry_count: usize,
    run_id: Option<String>,
    state_error: Option<&'static str>,
}

async fn observe_risk_strategy(
    state: &ApiState,
    exchange: Exchange,
    symbol: &str,
) -> RiskStrategyObservation {
    let Some(runtime) = state.runtime.as_ref() else {
        let catalog = load_risk_catalog(state.strategy_root.clone()).await.ok();
        return build_risk_strategy_observation(catalog, Vec::new(), false, exchange, symbol, None);
    };

    for _ in 0..3 {
        let before_runtime = runtime.entries().await;
        let catalog = match load_risk_catalog(state.strategy_root.clone()).await {
            Ok(catalog) => catalog,
            Err(()) => {
                return build_risk_strategy_observation(
                    None,
                    runtime.entries().await,
                    true,
                    exchange,
                    symbol,
                    Some("strategy_catalog_unavailable"),
                );
            }
        };
        let after_runtime = runtime.entries().await;
        if before_runtime == after_runtime {
            return build_risk_strategy_observation(
                Some(catalog),
                after_runtime,
                true,
                exchange,
                symbol,
                None,
            );
        }
    }

    let runtime_entries = runtime.entries().await;
    let catalog = load_risk_catalog(state.strategy_root.clone()).await.ok();
    build_risk_strategy_observation(
        catalog,
        runtime_entries,
        true,
        exchange,
        symbol,
        Some("strategy_status_changed"),
    )
}

async fn load_risk_catalog(root: PathBuf) -> Result<StrategyCatalog, ()> {
    match tokio::task::spawn_blocking(move || load_strategy_catalog(root)).await {
        Ok(Ok(catalog)) => Ok(catalog),
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "strategy catalog could not be read for risk observation");
            Err(())
        }
        Err(error) => {
            tracing::error!(error = %error, "strategy catalog risk observation task failed");
            Err(())
        }
    }
}

fn build_risk_strategy_observation(
    catalog: Option<StrategyCatalog>,
    runtime_entries: Vec<RuntimeRegistryEntry>,
    runtime_configured: bool,
    exchange: Exchange,
    symbol: &str,
    forced_catalog_problem: Option<&'static str>,
) -> RiskStrategyObservation {
    let (selected, catalog_anomaly_count, catalog_problem) = match catalog {
        Some(catalog) => {
            let anomaly_count = catalog.anomalies().len();
            let mut problem = forced_catalog_problem;
            if anomaly_count > 0 {
                problem = Some("strategy_catalog_anomaly");
            }
            let selected = match catalog.select_live(exchange, symbol) {
                Ok(selected) => selected,
                Err(error) => {
                    tracing::warn!(?exchange, symbol, error = %error, "multiple live strategy states prevent deterministic risk attribution");
                    problem = Some("multiple_live_strategies");
                    None
                }
            };
            (selected, anomaly_count, problem)
        }
        None => (
            None,
            0,
            forced_catalog_problem.or(Some("strategy_catalog_unavailable")),
        ),
    };
    let runtime = scoped_runtime_observation(
        runtime_configured,
        selected.as_ref(),
        &runtime_entries,
        exchange,
        symbol,
    );
    RiskStrategyObservation {
        selected,
        catalog_anomaly_count,
        catalog_problem,
        runtime,
    }
}

fn scoped_runtime_observation(
    runtime_configured: bool,
    selected: Option<&StrategyCatalogSnapshot>,
    runtime_entries: &[RuntimeRegistryEntry],
    exchange: Exchange,
    symbol: &str,
) -> RuntimeMarketObservation {
    if !runtime_configured {
        return RuntimeMarketObservation {
            configured: false,
            engine_running: false,
            advancing: false,
            market_entry_count: 0,
            run_id: None,
            state_error: None,
        };
    }

    let market_entries = runtime_entries
        .iter()
        .filter(|entry| entry.exchange == exchange && entry.symbol == symbol)
        .collect::<Vec<_>>();
    let selected_entry = selected.and_then(|snapshot| {
        market_entries
            .iter()
            .copied()
            .find(|entry| runtime_entry_identity_matches(snapshot, entry))
    });
    let engine_running =
        selected.map_or_else(|| !market_entries.is_empty(), |_| selected_entry.is_some());
    let advancing = selected.map_or_else(
        || market_entries.iter().any(|entry| entry.advancing),
        |_| selected_entry.is_some_and(|entry| entry.advancing),
    );
    let run_id = selected_entry
        .map(|entry| entry.run_id.as_str().to_owned())
        .or_else(|| {
            (market_entries.len() == 1).then(|| market_entries[0].run_id.as_str().to_owned())
        });
    let state_error = match selected {
        Some(snapshot)
            if market_entries.len() != 1
                || selected_entry
                    .is_none_or(|entry| !runtime_entry_snapshot_matches(snapshot, entry)) =>
        {
            Some("runtime_catalog_mismatch")
        }
        None if !market_entries.is_empty() => Some("runtime_catalog_mismatch"),
        Some(_) | None => None,
    };

    RuntimeMarketObservation {
        configured: true,
        engine_running,
        advancing,
        market_entry_count: market_entries.len(),
        run_id,
        state_error,
    }
}

fn runtime_risk_fields(runtime: &RuntimeMarketObservation) -> Value {
    json!({
        "engine_running": runtime.engine_running,
        "runtime_advancing": runtime.advancing,
        "runtime_configured": runtime.configured,
        "runtime_market_entry_count": runtime.market_entry_count,
        "runtime_run_id": runtime.run_id,
        "runtime_state_error": runtime.state_error,
    })
}

fn no_store_runtime_risk_json(response: Value, runtime: &RuntimeMarketObservation) -> Response {
    let (Value::Object(mut response), Value::Object(runtime_fields)) =
        (response, runtime_risk_fields(runtime))
    else {
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "risk_response_failed",
            "The strategy risk response could not be constructed safely",
        );
    };
    response.extend(runtime_fields);
    no_store_json(StatusCode::OK, Value::Object(response))
}

async fn exchange_risk(
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

    let RiskStrategyObservation {
        selected,
        catalog_anomaly_count,
        catalog_problem,
        runtime,
    } = observe_risk_strategy(&state, exchange, &symbol).await;
    let catalog_has_risk = catalog_problem.is_some();
    let runtime_has_risk = runtime.state_error.is_some();

    match selected {
        Some(StrategyCatalogSnapshot::Active(strategy)) => {
            let collected = match collect_strategy_shadow_view(gateway.as_ref(), &strategy).await {
                Ok(collected) => collected,
                Err(error) => return risk_collection_failure(exchange, &symbol, error),
            };
            let report = &collected.report;
            let orphan_orders = collected
                .open_orders
                .iter()
                .filter(|order| !strategy.orders.contains_key(&order.client_order_id))
                .map(risk_order_response)
                .collect::<Vec<_>>();
            let orphan_order_count = orphan_orders.len();
            let unmanaged_position = report
                .position
                .quantity_delta
                .is_none_or(|delta| !delta.is_zero());
            let grid_coverage_risk = !report.level_coverage.missing_levels.is_empty();
            let order_protection_risk = report.orders.missing_order_count > 0
                || report.orders.mismatched_order_count > 0
                || report.orders.partial_execution_pending_count > 0
                || report.orders.terminal_accounting_pending_count > 0;
            let has_risk =
                !report.clean || orphan_order_count > 0 || catalog_has_risk || runtime_has_risk;
            let positions = collected
                .position
                .legs
                .iter()
                .filter_map(position_response)
                .collect::<Vec<_>>();
            let initialization_in_progress = matches!(
                strategy.lifecycle,
                StrategyLifecycle::AwaitingOpening | StrategyLifecycle::DeployingGrid
            );
            let profit_metrics = match strategy_profit_metrics(&strategy, &collected.position) {
                Ok(metrics) => Some(metrics),
                Err(error) => {
                    tracing::warn!(
                        ?exchange,
                        symbol,
                        run_id = strategy.run_id.as_str(),
                        error = %error,
                        "strategy-owned profit metrics are unavailable"
                    );
                    None
                }
            };
            let profit_calculation_has_risk = profit_metrics.is_none();
            let profit_fields = json!({
                "gross_profit": strategy.gross_realized_profit.to_string(),
                "realized_net_profit": profit_metrics.map(|metrics| metrics.realized_net_profit.to_string()),
                "unrealised_pnl": profit_metrics.map(|metrics| metrics.grid_unrealized_profit.to_string()),
                "grid_unrealised_pnl": profit_metrics.map(|metrics| metrics.grid_unrealized_profit.to_string()),
                "total_equity_profit": profit_metrics.map(|metrics| metrics.total_equity_profit.to_string()),
                "total_profit": profit_metrics.map(|metrics| metrics.total_equity_profit.to_string()),
                "profit_mark_price": profit_metrics.map(|metrics| metrics.mark_price.to_string()),
                "profit_scope": "strategy_owned_inventory",
                "profit_calculation_error": profit_calculation_has_risk.then_some("strategy_profit_unavailable"),
                "total_fee": strategy.total_fee.to_string(),
                "total_volume": strategy.total_volume.to_string(),
                "completed_pairs": strategy.completed_pairs,
            });
            let response = json!({
                "version": 1,
                "symbol": symbol,
                "exchange": exchange,
                "strategy_present": true,
                "strategy_kind": "active",
                "run_id": strategy.run_id.as_str(),
                "strategy_revision": strategy.revision,
                "lifecycle": strategy.lifecycle,
                "observation_complete": true,
                "baseline_pending": false,
                "baseline_position": report.position.baseline_quantity.to_string(),
                "grid_position_net_qty": report.position.grid_owned_quantity.to_string(),
                "expected_position_net_qty": report.position.expected_quantity.map(|value| value.to_string()),
                "actual_position_net_qty": report.position.actual_quantity.map(|value| value.to_string()),
                "unmanaged_delta_qty": report.position.quantity_delta.map(|value| value.to_string()),
                "unmanaged_position": unmanaged_position,
                "orphan_order_count": orphan_order_count,
                "orphan_orders": orphan_orders,
                "pending_submission_count": report.orders.pending_submission_count,
                "queued_replacement_count": report.pending_replacement_obligation_ids.len(),
                "accepted_shape_mismatch_count": report.orders.mismatched_order_count,
                "reduce_protection": {
                    "has_risk": order_protection_risk,
                },
                "grid_coverage": {
                    "has_risk": grid_coverage_risk,
                    "required": report.level_coverage.required,
                    "configured_level_count": report.level_coverage.configured_level_count,
                    "expected_active_levels": report.level_coverage.expected_active_levels,
                    "observed_exact_active_levels": report.level_coverage.observed_exact_active_levels,
                    "missing_levels": report.level_coverage.missing_levels,
                },
                "waiting_trigger": false,
                "waiting_initial_order": strategy.lifecycle == StrategyLifecycle::AwaitingOpening,
                "risk_shutdown_pending": strategy.lifecycle == StrategyLifecycle::RiskExitRequested,
                "manual_stop_pending": strategy.lifecycle == StrategyLifecycle::StopRequested,
                "initialization_failed": strategy.lifecycle == StrategyLifecycle::Failed,
                "initialization_in_progress": initialization_in_progress,
                "initial_grid_deployment_pending": strategy.lifecycle == StrategyLifecycle::DeployingGrid && !strategy.initial_deployment_complete,
                "grid_ready": strategy.initial_deployment_complete,
                "catalog_anomaly_count": catalog_anomaly_count,
                "state_store_error": catalog_problem,
                "positions": positions,
                "shadow_audit": report,
                "has_risk": has_risk || profit_calculation_has_risk,
            });
            let (Value::Object(mut response), Value::Object(profit_fields)) =
                (response, profit_fields)
            else {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "risk_response_failed",
                    "The strategy risk response could not be constructed safely",
                );
            };
            response.extend(profit_fields);
            no_store_runtime_risk_json(Value::Object(response), &runtime)
        }
        Some(StrategyCatalogSnapshot::Armed(strategy)) => {
            let collected =
                match collect_stable_exchange_view(gateway.as_ref(), exchange, &symbol).await {
                    Ok(collected) => collected,
                    Err(error) => return risk_collection_failure(exchange, &symbol, error),
                };
            let actual_quantity =
                match validated_one_way_quantity(&collected.position, exchange, &symbol) {
                    Ok(quantity) => quantity,
                    Err(error) => return snapshot_failure("risk position", exchange, error),
                };
            let orphan_orders = collected
                .open_orders
                .iter()
                .map(risk_order_response)
                .collect::<Vec<_>>();
            let orphan_order_count = orphan_orders.len();
            let positions = collected
                .position
                .legs
                .iter()
                .filter_map(position_response)
                .collect::<Vec<_>>();
            no_store_runtime_risk_json(
                json!({
                    "version": 1,
                    "symbol": symbol,
                    "exchange": exchange,
                    "strategy_present": true,
                    "strategy_kind": "armed",
                    "run_id": strategy.run_id.as_str(),
                    "strategy_revision": strategy.revision,
                    "lifecycle": strategy.lifecycle,
                    "observation_complete": true,
                    "baseline_pending": true,
                    "baseline_position": null,
                    "grid_position_net_qty": "0",
                    "expected_position_net_qty": null,
                    "actual_position_net_qty": actual_quantity.to_string(),
                    "unmanaged_delta_qty": null,
                    "unmanaged_position": false,
                    "orphan_order_count": orphan_order_count,
                    "orphan_orders": orphan_orders,
                    "pending_submission_count": 0,
                    "queued_replacement_count": 0,
                    "accepted_shape_mismatch_count": 0,
                    "reduce_protection": { "has_risk": false },
                    "grid_coverage": {
                        "has_risk": false,
                        "required": false,
                        "configured_level_count": strategy.config.grid_count,
                        "expected_active_levels": [],
                        "observed_exact_active_levels": [],
                        "missing_levels": [],
                    },
                    "waiting_trigger": strategy.lifecycle == ArmedStrategyLifecycle::WaitingTrigger,
                    "waiting_initial_order": false,
                    "risk_shutdown_pending": false,
                    "manual_stop_pending": false,
                    "initialization_failed": false,
                    "initialization_in_progress": false,
                    "initial_grid_deployment_pending": false,
                    "grid_ready": false,
                    "catalog_anomaly_count": catalog_anomaly_count,
                    "state_store_error": catalog_problem,
                    "positions": positions,
                    "shadow_audit": null,
                    "has_risk": orphan_order_count > 0 || catalog_has_risk || runtime_has_risk,
                }),
                &runtime,
            )
        }
        None => {
            let collected =
                match collect_stable_exchange_view(gateway.as_ref(), exchange, &symbol).await {
                    Ok(collected) => collected,
                    Err(error) => return risk_collection_failure(exchange, &symbol, error),
                };
            let actual_quantity =
                match validated_one_way_quantity(&collected.position, exchange, &symbol) {
                    Ok(quantity) => quantity,
                    Err(error) => return snapshot_failure("risk position", exchange, error),
                };
            let orphan_orders = collected
                .open_orders
                .iter()
                .map(risk_order_response)
                .collect::<Vec<_>>();
            let orphan_order_count = orphan_orders.len();
            let unmanaged_position = !actual_quantity.is_zero();
            let positions = collected
                .position
                .legs
                .iter()
                .filter_map(position_response)
                .collect::<Vec<_>>();
            no_store_runtime_risk_json(
                json!({
                    "version": 1,
                    "symbol": symbol,
                    "exchange": exchange,
                    "strategy_present": false,
                    "strategy_kind": null,
                    "run_id": null,
                    "strategy_revision": null,
                    "lifecycle": null,
                    "observation_complete": true,
                    "baseline_pending": false,
                    "baseline_position": null,
                    "grid_position_net_qty": null,
                    "expected_position_net_qty": null,
                    "actual_position_net_qty": actual_quantity.to_string(),
                    "unmanaged_delta_qty": actual_quantity.to_string(),
                    "unmanaged_position": unmanaged_position,
                    "orphan_order_count": orphan_order_count,
                    "orphan_orders": orphan_orders,
                    "pending_submission_count": 0,
                    "queued_replacement_count": 0,
                    "accepted_shape_mismatch_count": 0,
                    "reduce_protection": { "has_risk": false },
                    "grid_coverage": {
                        "has_risk": false,
                        "required": false,
                        "configured_level_count": 0,
                        "expected_active_levels": [],
                        "observed_exact_active_levels": [],
                        "missing_levels": [],
                    },
                    "waiting_trigger": false,
                    "waiting_initial_order": false,
                    "risk_shutdown_pending": false,
                    "manual_stop_pending": false,
                    "initialization_failed": false,
                    "initialization_in_progress": false,
                    "initial_grid_deployment_pending": false,
                    "grid_ready": false,
                    "catalog_anomaly_count": catalog_anomaly_count,
                    "state_store_error": catalog_problem,
                    "positions": positions,
                    "shadow_audit": null,
                    "has_risk": unmanaged_position || orphan_order_count > 0 || catalog_has_risk || runtime_has_risk,
                }),
                &runtime,
            )
        }
    }
}

fn validated_one_way_quantity(
    position: &PositionSnapshot,
    exchange: Exchange,
    symbol: &str,
) -> Result<Decimal, SnapshotError> {
    if position.exchange != exchange || position.symbol != symbol {
        return Err(SnapshotError::new(
            "position snapshot belongs to another exchange or symbol",
        ));
    }
    let (quantity, entry_price) = position.one_way_position()?;
    let leg = &position.legs[0];
    if leg.mark_price <= Decimal::ZERO
        || leg.leverage.is_some_and(|leverage| leverage == 0)
        || (quantity.is_zero() && entry_price.is_some_and(|price| price <= Decimal::ZERO))
        || (!quantity.is_zero() && entry_price.is_none_or(|price| price <= Decimal::ZERO))
    {
        return Err(SnapshotError::new(
            "one-way position contains an invalid mark price, entry price, or leverage",
        ));
    }
    Ok(quantity)
}

#[derive(Clone, Copy)]
struct StrategyProfitMetrics {
    realized_net_profit: Decimal,
    grid_unrealized_profit: Decimal,
    total_equity_profit: Decimal,
    mark_price: Decimal,
}

fn strategy_profit_metrics(
    strategy: &StrategyState,
    position: &PositionSnapshot,
) -> Result<StrategyProfitMetrics, SnapshotError> {
    validated_one_way_quantity(position, strategy.exchange, &strategy.symbol)?;
    let mark_price = position
        .legs
        .first()
        .map(|leg| leg.mark_price)
        .ok_or_else(|| SnapshotError::new("position snapshot has no one-way mark price"))?;
    let realized_net_profit = strategy
        .gross_realized_profit
        .checked_sub(strategy.total_fee)
        .ok_or_else(|| SnapshotError::new("strategy realized profit arithmetic overflowed"))?;
    let grid_unrealized_profit = strategy
        .grid_unrealized_profit(mark_price)
        .map_err(|_| SnapshotError::new("strategy-owned inventory cannot be valued exactly"))?;
    let total_equity_profit = realized_net_profit
        .checked_add(grid_unrealized_profit)
        .ok_or_else(|| SnapshotError::new("strategy equity profit arithmetic overflowed"))?;
    Ok(StrategyProfitMetrics {
        realized_net_profit,
        grid_unrealized_profit,
        total_equity_profit,
        mark_price,
    })
}

fn risk_order_response(order: &AuthoritativeOrder) -> Value {
    let status = match order.lifecycle {
        OrderLifecycle::Active(ActiveOrderStatus::New) => "NEW",
        OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled) => "PARTIALLY_FILLED",
        OrderLifecycle::Terminal(_) => "TERMINAL",
    };
    json!({
        "order_id": order.exchange_order_id,
        "order_link_id": order.client_order_id.as_str(),
        "side": order_side_name(order.shape.side),
        "price": order.shape.price.map(|price| price.to_string()).unwrap_or_else(|| "0".into()),
        "qty": order.shape.quantity.to_string(),
        "status": status,
        "reduce_only": order.shape.reduce_only,
    })
}

fn risk_collection_failure(
    exchange: Exchange,
    symbol: &str,
    error: ShadowCollectionError,
) -> Response {
    tracing::warn!(?exchange, symbol, error = %error, "risk shadow collection was inconclusive");
    api_error(
        StatusCode::BAD_GATEWAY,
        "risk_snapshot_unavailable",
        "The risk snapshot changed during collection or is invalid",
    )
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
    strategy_root: PathBuf,
    runtime: Option<Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>>,
) -> Router {
    let state = match runtime {
        Some(runtime) => ApiState::enabled(
            admin_token,
            web_authentication,
            exchange_gateways,
            idempotency_root,
            strategy_root,
            runtime,
        ),
        None => ApiState::disabled(
            admin_token,
            web_authentication,
            exchange_gateways,
            idempotency_root,
            strategy_root,
        ),
    };
    router_with_state(state)
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
        .route("/api/trades/{symbol}", get(strategy_trades))
        .route("/api/risk/{symbol}", get(exchange_risk))
        .route("/api/grid/status", get(grid_status))
        .route("/api/grid/status/{symbol}", get(grid_symbol_status))
        .route("/api/grid/history", get(strategy_history))
        .route("/api/grid/preview", post(grid_preview))
        .route("/api/grid/start", post(start_grid_web))
        .route("/api/grid/stop", post(stop_only_grid_web))
        .route("/api/grid/stop-all", post(stop_all_grids_web))
        .route("/api/grid/stop/{symbol}", post(stop_grid_web))
        .route("/api/v1/grid/start", post(start_grid_admin))
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .layer(DefaultBodyLimit::max(MAX_CONTROL_BODY_BYTES))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
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
        domain::{
            ClientOrderId, Direction, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            IntentState, OrderIntent, OrderKind, OrderShape, PositionSizingMode, QuantityRules,
            TerminalOrderStatus, TimeInForce,
        },
        engine::{
            ArmedStrategyState, ExecutionAuditRecord, ExecutionReport, FeeValuation,
            MarketSnapshot, PositionBaseline, RuntimeRecoveryProvider, RuntimeSettings,
            StrategyLifecycle, StrategyMachine, StrategyOrderTracking, StrategyRunId,
            StrategyState, StrategyStateStore, build_grid_plan,
        },
        exchange::{
            AccountBalanceSnapshot, AccountBalanceSnapshotGateway, AccountBalanceUnit,
            AuthoritativeOrder, ExchangeIdentityGateway, ExchangeMarketSnapshot, LookupError,
            MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderExecutionSnapshot, OrderLookup,
            OrderLookupGateway, PositionSide, PositionSnapshot, PositionSnapshotGateway, TradeFill,
            TradingFeeRateGateway, TradingFeeRates,
            configured::{
                ExchangeCredentials, ExchangeEnvironment, ExchangeGatewayFactory,
                SharedConfiguredExchangeGateway,
            },
        },
        persistence::{
            FileArmedStrategyStateStore, FileOrderIntentStore, FileStrategyStateStore, IntentStore,
            StrategyFilePaths,
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

    fn valid_preview_config() -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction: Direction::Short,
            upper_price: Decimal::from_str_exact("1020").unwrap(),
            lower_price: Decimal::from_str_exact("1000").unwrap(),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::from_str_exact("0.2").unwrap()),
            fee_rate: Some(Decimal::from_str_exact("0.0005").unwrap()),
            maker_fee_rate: Some(Decimal::from_str_exact("0.0002").unwrap()),
            taker_fee_rate: Some(Decimal::from_str_exact("0.0005").unwrap()),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: Some(Decimal::from_str_exact("1014").unwrap()),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        }
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

    #[async_trait]
    impl OrderLookupGateway for ExactReadGateway {
        async fn lookup_order_by_client_id(
            &self,
            exchange: Exchange,
            symbol: &str,
            client_order_id: &ClientOrderId,
        ) -> Result<OrderLookup, LookupError> {
            assert_eq!(exchange, Exchange::Aster);
            assert_eq!(symbol, "ANSEMUSDT");
            if client_order_id.as_str() != "g_9_B_exact01" {
                return Ok(OrderLookup::NotFound);
            }
            Ok(OrderLookup::Found(AuthoritativeOrder {
                client_order_id: client_order_id.clone(),
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
            }))
        }
    }

    #[derive(Clone)]
    struct StrategyRiskGateway {
        open_orders: Vec<AuthoritativeOrder>,
        position: PositionSnapshot,
    }

    impl ExchangeIdentityGateway for StrategyRiskGateway {
        fn exchange(&self) -> Exchange {
            Exchange::Aster
        }
    }

    #[async_trait]
    impl AccountBalanceSnapshotGateway for StrategyRiskGateway {
        async fn account_balance_snapshot(
            &self,
            _exchange: Exchange,
        ) -> Result<AccountBalanceSnapshot, SnapshotError> {
            Err(SnapshotError::new("not used by strategy risk test"))
        }
    }

    #[async_trait]
    impl MarketSnapshotGateway for StrategyRiskGateway {
        async fn market_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
            Err(SnapshotError::new("not used by strategy risk test"))
        }
    }

    #[async_trait]
    impl TradingFeeRateGateway for StrategyRiskGateway {
        async fn trading_fee_rates(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<TradingFeeRates, SnapshotError> {
            Err(SnapshotError::new("not used by strategy risk test"))
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for StrategyRiskGateway {
        async fn position_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            assert_eq!(exchange, self.position.exchange);
            assert_eq!(symbol, self.position.symbol);
            Ok(self.position.clone())
        }
    }

    #[async_trait]
    impl OpenOrderSnapshotGateway for StrategyRiskGateway {
        async fn open_orders_snapshot(
            &self,
            exchange: Exchange,
            symbol: &str,
        ) -> Result<Vec<AuthoritativeOrder>, SnapshotError> {
            assert_eq!(exchange, self.position.exchange);
            assert_eq!(symbol, self.position.symbol);
            Ok(self.open_orders.clone())
        }
    }

    #[async_trait]
    impl OrderLookupGateway for StrategyRiskGateway {
        async fn lookup_order_by_client_id(
            &self,
            exchange: Exchange,
            symbol: &str,
            client_order_id: &ClientOrderId,
        ) -> Result<OrderLookup, LookupError> {
            assert_eq!(exchange, self.position.exchange);
            assert_eq!(symbol, self.position.symbol);
            Ok(self
                .open_orders
                .iter()
                .find(|order| &order.client_order_id == client_order_id)
                .cloned()
                .map(OrderLookup::Found)
                .unwrap_or(OrderLookup::NotFound))
        }
    }

    struct ConfiguredTestRuntimeProvider {
        gateway: SharedConfiguredExchangeGateway,
        settings: RuntimeSettings,
    }

    impl RuntimeRecoveryProvider for ConfiguredTestRuntimeProvider {
        type Gateway = SharedConfiguredExchangeGateway;
        type Error = std::convert::Infallible;

        fn runtime_for(
            &self,
            exchange: Exchange,
            _run_id: &StrategyRunId,
        ) -> Result<(Self::Gateway, RuntimeSettings), Self::Error> {
            assert_eq!(exchange, self.gateway.exchange());
            Ok((self.gateway.clone(), self.settings.clone()))
        }
    }

    fn persist_clean_running_strategy(
        root: &std::path::Path,
    ) -> (StrategyState, StrategyRiskGateway) {
        persist_clean_running_strategy_for(root, "RISKAPI1", "ANSEMUSDT")
    }

    fn persist_clean_running_strategy_for(
        root: &std::path::Path,
        run_id: &str,
        symbol: &str,
    ) -> (StrategyState, StrategyRiskGateway) {
        let config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: symbol.into(),
            direction: Direction::Neutral,
            upper_price: Decimal::from_str_exact("0.42000").unwrap(),
            lower_price: Decimal::from_str_exact("0.38000").unwrap(),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 3,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::from_str_exact("100.000").unwrap()),
            fee_rate: Some(Decimal::from_str_exact("0.00050").unwrap()),
            maker_fee_rate: Some(Decimal::from_str_exact("0.00020").unwrap()),
            taker_fee_rate: Some(Decimal::from_str_exact("0.00050").unwrap()),
            initial_order_type: InitialOrderType::Market,
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        };
        let rules = InstrumentRules {
            tick_size: Decimal::from_str_exact("0.00010").unwrap(),
            limit_quantity: QuantityRules {
                step: Decimal::from_str_exact("1.000").unwrap(),
                min: Decimal::from_str_exact("1.000").unwrap(),
                max: None,
            },
            market_quantity: QuantityRules {
                step: Decimal::from_str_exact("1.000").unwrap(),
                min: Decimal::from_str_exact("1.000").unwrap(),
                max: None,
            },
            min_notional: Decimal::ZERO,
        };
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::from_str_exact("0.40000").unwrap(),
                mark_price: Decimal::from_str_exact("0.40000").unwrap(),
            },
            &rules,
        )
        .unwrap();
        let mut state = StrategyState::from_plan(
            StrategyRunId::parse(run_id).unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap();
        let paths = StrategyFilePaths::new(root, state.run_id.clone()).unwrap();
        let mut ledger = FileOrderIntentStore::load(paths.intents()).unwrap();
        let mut open_orders = Vec::new();
        for (index, order) in state.orders.values_mut().enumerate() {
            let exchange_order_id = format!("risk-exchange-{index:02}");
            let intent = OrderIntent::prepare(
                order.client_order_id.clone(),
                state.exchange,
                order.shape.clone(),
                100,
            )
            .unwrap();
            ledger.insert_prepared(intent).unwrap();
            ledger
                .transition(
                    &order.client_order_id,
                    IntentState::Accepted {
                        exchange_order_id: exchange_order_id.clone(),
                    },
                    101,
                )
                .unwrap();
            order.tracking = StrategyOrderTracking::Intent {
                state: IntentState::Accepted {
                    exchange_order_id: exchange_order_id.clone(),
                },
            };
            order.exchange_order_id = Some(exchange_order_id.clone());
            open_orders.push(AuthoritativeOrder {
                client_order_id: order.client_order_id.clone(),
                exchange_order_id,
                exchange: state.exchange,
                shape: order.shape.clone(),
                lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
            });
        }
        state.lifecycle = StrategyLifecycle::Running;
        state.initial_deployment_complete = true;
        state.validate().unwrap();
        FileStrategyStateStore::create(paths.state(), state.clone()).unwrap();
        let position = PositionSnapshot {
            exchange: state.exchange,
            symbol: state.symbol.clone(),
            legs: vec![PositionLeg {
                side: PositionSide::Both,
                signed_quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: Decimal::from_str_exact("0.40000").unwrap(),
                unrealized_profit: Decimal::ZERO,
                leverage: Some(3),
            }],
        };
        (
            state,
            StrategyRiskGateway {
                open_orders,
                position,
            },
        )
    }

    async fn recovered_stop_app(directory: &std::path::Path) -> (Router, StrategyState, PathBuf) {
        let strategy_root = directory.join("strategies");
        let (strategy, _) = persist_clean_running_strategy(&strategy_root);
        let app = recovered_stop_app_from_root(directory, strategy_root.clone(), 1).await;
        (app, strategy, strategy_root)
    }

    async fn recovered_stop_app_from_root(
        directory: &std::path::Path,
        strategy_root: PathBuf,
        expected_count: usize,
    ) -> Router {
        let settings = RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap();
        let runtime = Arc::new(RuntimeCoordinator::new(
            strategy_root.clone(),
            settings.clone(),
        ));
        let configured_gateway = ExchangeGatewayFactory::standard(ExchangeEnvironment::Testnet)
            .unwrap()
            .build(ExchangeCredentials::aster("1".repeat(64)).unwrap())
            .unwrap()
            .shared();
        let report = runtime
            .recover(&ConfiguredTestRuntimeProvider {
                gateway: configured_gateway,
                settings,
            })
            .await
            .unwrap();
        assert_eq!(report.registered.len(), expected_count);
        assert!(report.discovery_anomalies.is_empty());
        assert!(report.failures.is_empty());
        router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(directory.join("idempotency"))),
                Arc::new(DisabledStartGridCommand),
            )
            .with_strategy_root(strategy_root.clone())
            .with_runtime(runtime),
        )
    }

    fn strategy_with_trade_audit(root: &std::path::Path) -> StrategyState {
        let (mut strategy, _) = persist_clean_running_strategy(root);
        let order = strategy.orders.values_mut().next().unwrap();
        let exchange_order_id = order.exchange_order_id.clone().unwrap();
        let trade = TradeFill {
            trade_id: "opaque-trade-1".into(),
            exchange_order_id: exchange_order_id.clone(),
            symbol: strategy.symbol.clone(),
            side: order.shape.side,
            price: Decimal::from_str_exact("15.95").unwrap(),
            quantity: Decimal::from_str_exact("3.14").unwrap(),
            quote_quantity: Decimal::from_str_exact("50.083").unwrap(),
            raw_commission: Decimal::from_str_exact("0.0277").unwrap(),
            commission_cost: Decimal::from_str_exact("0.0277").unwrap(),
            commission_asset: "BNB".into(),
            realized_profit: Decimal::from_str_exact("1.5").unwrap(),
            is_maker: false,
            trade_time_ms: 1_779_550_014_852,
        };
        order.execution_audit = Some(ExecutionAuditRecord {
            snapshot: OrderExecutionSnapshot {
                order: AuthoritativeOrder {
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id,
                    exchange: strategy.exchange,
                    shape: order.shape.clone(),
                    lifecycle: OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled),
                },
                cumulative_quantity: trade.quantity,
                cumulative_quote: trade.quote_quantity,
                fees_by_asset: BTreeMap::from([("BNB".into(), trade.commission_cost)]),
                trades: vec![trade.clone()],
                order_time_ms: 1_779_550_000_000,
                update_time_ms: trade.trade_time_ms,
            },
            fee_valuations: vec![FeeValuation {
                trade_id: trade.trade_id,
                fee_asset: "BNB".into(),
                fee_amount: trade.commission_cost,
                quote_asset: "USDT".into(),
                quote_value: Decimal::from_str_exact("0.12").unwrap(),
                source: FeeValuationSource::HistoricalMinuteOpen,
                valuation_symbol: Some("BNBUSDT".into()),
                valuation_minute_start_ms: Some(1_779_549_960_000),
                valuation_price: Some(Decimal::from_str_exact("4.3321299639").unwrap()),
            }],
            synced_at_ms: 1_779_550_015_000,
        });
        strategy
    }

    #[test]
    fn strategy_trade_rows_preserve_exchange_quantity_fee_and_liquidity_exactly() {
        let directory = tempdir().unwrap();
        let strategy = strategy_with_trade_audit(directory.path());

        let rows = strategy_trade_rows(&strategy, 100).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].trade_id, "opaque-trade-1");
        assert_eq!(rows[0].price, "15.95");
        assert_eq!(rows[0].qty, "3.14");
        assert_eq!(rows[0].volume, "50.083");
        assert_eq!(rows[0].fee, "0.0277");
        assert_eq!(rows[0].fee_usdt, "0.12");
        assert_eq!(rows[0].liquidity, "taker");
        assert_eq!(rows[0].realized_pnl, "1.5");
        assert_eq!(rows[0].profit, "1.38");
    }

    #[test]
    fn duplicate_trade_identity_fails_closed_instead_of_hiding_a_bad_audit() {
        let directory = tempdir().unwrap();
        let mut strategy = strategy_with_trade_audit(directory.path());
        let audit = strategy
            .orders
            .values_mut()
            .find_map(|order| order.execution_audit.as_mut())
            .unwrap();
        audit.snapshot.trades.push(audit.snapshot.trades[0].clone());

        let response = strategy_trade_rows(&strategy, 100).unwrap_err();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn cross_order_duplicate_trade_identity_fails_closed_in_trade_rows() {
        let directory = tempdir().unwrap();
        let mut strategy = strategy_with_trade_audit(directory.path());
        let source_audit = strategy
            .orders
            .values()
            .find_map(|order| order.execution_audit.clone())
            .unwrap();
        let target = strategy
            .orders
            .values_mut()
            .find(|order| order.execution_audit.is_none())
            .unwrap();
        let mut duplicate = source_audit;
        duplicate.snapshot.order.client_order_id = target.client_order_id.clone();
        duplicate.snapshot.order.exchange_order_id = target.exchange_order_id.clone().unwrap();
        duplicate.snapshot.order.shape = target.shape.clone();
        duplicate.snapshot.trades[0].exchange_order_id = target.exchange_order_id.clone().unwrap();
        duplicate.snapshot.trades[0].side = target.shape.side;
        duplicate.snapshot.trades[0].symbol = target.shape.symbol.clone();
        target.execution_audit = Some(duplicate);

        let response = strategy_trade_rows(&strategy, 100).unwrap_err();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    fn exact_read_app() -> Router {
        static STRATEGY_ROOT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let strategy_root = std::env::temp_dir().join(format!(
            "grid-trading-api-read-no-strategy-{}-{}",
            std::process::id(),
            STRATEGY_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
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
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
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

        let risk = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk.status(), StatusCode::OK);
        assert_eq!(risk.headers()[CACHE_CONTROL], "no-store");
        let risk = response_json(risk).await;
        assert_eq!(risk["strategy_present"], false);
        assert_eq!(risk["observation_complete"], true);
        assert_eq!(risk["actual_position_net_qty"], "-1326.000");
        assert_eq!(risk["expected_position_net_qty"], Value::Null);
        assert_eq!(risk["unmanaged_delta_qty"], "-1326.000");
        assert_eq!(risk["unmanaged_position"], true);
        assert_eq!(risk["orphan_order_count"], 1);
        assert_eq!(risk["orphan_orders"][0]["qty"], "100.000");
        assert_eq!(risk["has_risk"], true);

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

    #[test]
    fn scoped_runtime_observation_requires_exact_identity_kind_and_lifecycle() {
        let directory = tempdir().unwrap();
        let (strategy, _) = persist_clean_running_strategy(directory.path());
        let snapshot = StrategyCatalogSnapshot::Active(Box::new(strategy.clone()));
        let exact = RuntimeRegistryEntry {
            run_id: strategy.run_id.clone(),
            exchange: strategy.exchange,
            symbol: strategy.symbol.clone(),
            kind: Some(PreparedStrategyKind::Active),
            lifecycle: Some(PreparedStrategyLifecycle::Running),
            advancing: false,
        };

        let healthy = scoped_runtime_observation(
            true,
            Some(&snapshot),
            std::slice::from_ref(&exact),
            strategy.exchange,
            &strategy.symbol,
        );
        assert_eq!(
            healthy,
            RuntimeMarketObservation {
                configured: true,
                engine_running: true,
                advancing: false,
                market_entry_count: 1,
                run_id: Some(strategy.run_id.as_str().to_owned()),
                state_error: None,
            }
        );

        let wrong_kind = RuntimeRegistryEntry {
            kind: Some(PreparedStrategyKind::Armed),
            ..exact.clone()
        };
        let mismatched = scoped_runtime_observation(
            true,
            Some(&snapshot),
            &[wrong_kind],
            strategy.exchange,
            &strategy.symbol,
        );
        assert!(mismatched.engine_running);
        assert_eq!(mismatched.state_error, Some("runtime_catalog_mismatch"));

        let advancing = RuntimeRegistryEntry {
            kind: None,
            lifecycle: None,
            advancing: true,
            ..exact
        };
        let transient = scoped_runtime_observation(
            true,
            Some(&snapshot),
            &[advancing],
            strategy.exchange,
            &strategy.symbol,
        );
        assert!(transient.engine_running);
        assert!(transient.advancing);
        assert_eq!(transient.state_error, None);
    }

    #[test]
    fn scoped_runtime_observation_fails_closed_for_missing_or_unowned_runtime() {
        let directory = tempdir().unwrap();
        let (strategy, _) = persist_clean_running_strategy(directory.path());
        let snapshot = StrategyCatalogSnapshot::Active(Box::new(strategy.clone()));

        let missing = scoped_runtime_observation(
            true,
            Some(&snapshot),
            &[],
            strategy.exchange,
            &strategy.symbol,
        );
        assert!(!missing.engine_running);
        assert_eq!(missing.state_error, Some("runtime_catalog_mismatch"));

        let orphan = RuntimeRegistryEntry {
            run_id: StrategyRunId::parse("OTHER001").unwrap(),
            exchange: strategy.exchange,
            symbol: strategy.symbol.clone(),
            kind: Some(PreparedStrategyKind::Active),
            lifecycle: Some(PreparedStrategyLifecycle::Running),
            advancing: false,
        };
        let unowned =
            scoped_runtime_observation(true, None, &[orphan], strategy.exchange, &strategy.symbol);
        assert!(unowned.engine_running);
        assert_eq!(unowned.market_entry_count, 1);
        assert_eq!(unowned.state_error, Some("runtime_catalog_mismatch"));

        let disabled = scoped_runtime_observation(
            false,
            Some(&snapshot),
            &[],
            strategy.exchange,
            &strategy.symbol,
        );
        assert!(!disabled.configured);
        assert_eq!(disabled.state_error, None);
    }

    #[tokio::test]
    async fn active_strategy_risk_endpoint_requires_exact_state_ledger_and_exchange_agreement() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (strategy, gateway) = persist_clean_running_strategy(&strategy_root);
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let status = response_json(status).await;
        assert_eq!(status["running_count"], 1);
        assert_eq!(status["grids"][0]["run_id"], strategy.run_id.as_str());
        assert_eq!(status["grids"][0]["waiting_trigger"], false);
        assert_eq!(status["grids"][0]["engine_running"], false);
        assert_eq!(status["grids"][0]["grid_position_net_qty"], "0");
        assert_eq!(status["grids"][0]["expected_position_net_qty"], "0");

        let history = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/history?limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history.status(), StatusCode::OK);
        let history = response_json(history).await;
        assert_eq!(history["source"], "durable_strategy_state");
        assert_eq!(history["count"], 1);
        assert_eq!(history["runs"][0]["run_id"], strategy.run_id.as_str());
        assert_eq!(history["runs"][0]["status"], "running");
        assert_eq!(history["runs"][0]["net_profit"], "0");
        assert_eq!(history["runs"][0]["grid_order_qty"], "100.000");

        let trades = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/trades/ANSEMUSDT?exchange=aster&limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(trades.status(), StatusCode::OK);
        let trades = response_json(trades).await;
        assert_eq!(trades["source"], "durable_exchange_execution_audit");
        assert_eq!(trades["scope"], "strategy");
        assert_eq!(trades["count"], 0);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["strategy_present"], true);
        assert_eq!(risk["strategy_kind"], "active");
        assert_eq!(risk["run_id"], strategy.run_id.as_str());
        assert_eq!(risk["baseline_position"], "0");
        assert_eq!(risk["grid_position_net_qty"], "0");
        assert_eq!(risk["expected_position_net_qty"], "0");
        assert_eq!(risk["actual_position_net_qty"], "0");
        assert_eq!(risk["unmanaged_delta_qty"], "0");
        assert_eq!(risk["orphan_order_count"], 0);
        assert_eq!(risk["grid_coverage"]["missing_levels"], json!([]));
        assert_eq!(risk["shadow_audit"]["clean"], true);
        assert_eq!(risk["realized_net_profit"], "0");
        assert_eq!(risk["grid_unrealised_pnl"], "0");
        assert_eq!(risk["total_equity_profit"], "0");
        assert_eq!(risk["profit_scope"], "strategy_owned_inventory");
        assert_eq!(risk["profit_calculation_error"], Value::Null);
        assert_eq!(risk["has_risk"], false);
    }

    #[tokio::test]
    async fn symbol_status_route_selects_one_exact_market_and_reports_absence() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (strategy, _) = persist_clean_running_strategy(&strategy_root);
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_strategy_root(strategy_root),
        );

        let selected = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/status/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(selected.status(), StatusCode::OK);
        let selected = response_json(selected).await;
        assert_eq!(selected["run_id"], strategy.run_id.as_str());
        assert_eq!(selected["exchange"], "aster");
        assert_eq!(selected["symbol"], "ANSEMUSDT");
        assert_eq!(selected["running"], true);

        let absent = app
            .oneshot(
                Request::builder()
                    .uri("/api/grid/status/MISSINGUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(absent.status(), StatusCode::OK);
        assert_eq!(
            response_json(absent).await,
            json!({
                "running": false,
                "engine_running": false,
                "exchange": "aster",
                "symbol": "MISSINGUSDT",
                "trading_enabled": false,
            })
        );
    }

    #[tokio::test]
    async fn aggregate_stop_routes_are_guarded_before_body_processing() {
        for path in ["/api/grid/stop", "/api/grid/stop-all"] {
            let response = super::super::app()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri(path)
                        .body(Body::from("not-json"))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(
                response_json(response).await["error"]["code"],
                "rust_trading_disabled",
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn aggregate_stop_routes_durably_request_stop_and_replay_the_same_response() {
        for path in ["/api/grid/stop", "/api/grid/stop-all"] {
            let directory = tempdir().unwrap();
            let (app, strategy, strategy_root) = recovered_stop_app(directory.path()).await;
            let make_request = || {
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header(CONTENT_TYPE, "application/json")
                    .header(IDEMPOTENCY_KEY_HEADER, KEY)
                    .body(Body::from("{}"))
                    .unwrap()
            };

            let first = app.clone().oneshot(make_request()).await.unwrap();
            assert_eq!(first.status(), StatusCode::ACCEPTED, "{path}");
            let first_payload = response_json(first).await;
            assert_eq!(first_payload["ok"], true, "{path}");
            if path.ends_with("stop-all") {
                assert_eq!(first_payload["count"], 1);
                assert_eq!(
                    first_payload["strategies"][0]["run_id"],
                    strategy.run_id.as_str()
                );
                assert_eq!(
                    first_payload["strategies"][0]["lifecycle"],
                    "stop_requested"
                );
            } else {
                assert_eq!(first_payload["run_id"], strategy.run_id.as_str());
                assert_eq!(first_payload["lifecycle"], "stop_requested");
            }

            let replay = app.clone().oneshot(make_request()).await.unwrap();
            assert_eq!(replay.status(), StatusCode::ACCEPTED, "{path}");
            assert_eq!(response_json(replay).await, first_payload, "{path}");

            let paths = StrategyFilePaths::new(strategy_root, strategy.run_id.clone()).unwrap();
            let persisted = FileStrategyStateStore::load(paths.state()).unwrap();
            assert_eq!(
                persisted.snapshot().lifecycle,
                StrategyLifecycle::StopRequested,
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn stop_one_rejects_ambiguity_and_stop_all_durably_stops_every_strategy() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (ansem, _) =
            persist_clean_running_strategy_for(&strategy_root, "RISKAPI1", "ANSEMUSDT");
        let (mu, _) = persist_clean_running_strategy_for(&strategy_root, "RISKAPI2", "MUUSDT");
        let app = recovered_stop_app_from_root(directory.path(), strategy_root.clone(), 2).await;

        let ambiguous = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/stop")
                    .header(CONTENT_TYPE, "application/json")
                    .header(IDEMPOTENCY_KEY_HEADER, "01J2X0W2F8E4Q8MNNNNNNNNNNP")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ambiguous.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(ambiguous).await["error"]["code"],
            "multiple_grids_running"
        );
        for strategy in [&ansem, &mu] {
            let paths = StrategyFilePaths::new(&strategy_root, strategy.run_id.clone()).unwrap();
            let persisted = FileStrategyStateStore::load(paths.state()).unwrap();
            assert_eq!(persisted.snapshot().lifecycle, StrategyLifecycle::Running);
        }

        let stop_all_request = || {
            Request::builder()
                .method(Method::POST)
                .uri("/api/grid/stop-all")
                .header(CONTENT_TYPE, "application/json")
                .header(IDEMPOTENCY_KEY_HEADER, KEY)
                .body(Body::from("{}"))
                .unwrap()
        };
        let stopped = app.clone().oneshot(stop_all_request()).await.unwrap();
        assert_eq!(stopped.status(), StatusCode::ACCEPTED);
        let stopped_payload = response_json(stopped).await;
        assert_eq!(stopped_payload["count"], 2);
        assert_eq!(stopped_payload["strategies"][0]["symbol"], "ANSEMUSDT");
        assert_eq!(stopped_payload["strategies"][1]["symbol"], "MUUSDT");

        let replay = app.clone().oneshot(stop_all_request()).await.unwrap();
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        assert_eq!(response_json(replay).await, stopped_payload);
        for strategy in [&ansem, &mu] {
            let paths = StrategyFilePaths::new(&strategy_root, strategy.run_id.clone()).unwrap();
            let persisted = FileStrategyStateStore::load(paths.state()).unwrap();
            assert_eq!(
                persisted.snapshot().lifecycle,
                StrategyLifecycle::StopRequested
            );
        }
    }

    #[tokio::test]
    async fn active_strategy_risk_reports_a_missing_enabled_runtime_as_unsafe() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (strategy, gateway) = persist_clean_running_strategy(&strategy_root);
        let runtime = Arc::new(RuntimeCoordinator::new(
            strategy_root.clone(),
            RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap(),
        ));
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root)
            .with_runtime(runtime),
        );

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::SERVICE_UNAVAILABLE);
        let status = response_json(status).await;
        assert_eq!(status["error"]["code"], "runtime_catalog_mismatch");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["run_id"], strategy.run_id.as_str());
        assert_eq!(risk["engine_running"], false);
        assert_eq!(risk["runtime_configured"], true);
        assert_eq!(risk["runtime_market_entry_count"], 0);
        assert_eq!(risk["runtime_run_id"], Value::Null);
        assert_eq!(risk["runtime_state_error"], "runtime_catalog_mismatch");
        assert_eq!(risk["has_risk"], true);
    }

    #[tokio::test]
    async fn recovered_runtime_is_reported_consistently_by_status_and_risk() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (strategy, gateway) = persist_clean_running_strategy(&strategy_root);
        let settings = RuntimeSettings::new("USDT", 10_000, 100, 100).unwrap();
        let runtime = Arc::new(RuntimeCoordinator::new(
            strategy_root.clone(),
            settings.clone(),
        ));
        let configured_gateway = ExchangeGatewayFactory::standard(ExchangeEnvironment::Testnet)
            .unwrap()
            .build(ExchangeCredentials::aster("1".repeat(64)).unwrap())
            .unwrap()
            .shared();
        let report = runtime
            .recover(&ConfiguredTestRuntimeProvider {
                gateway: configured_gateway,
                settings,
            })
            .await
            .unwrap();
        assert_eq!(report.registered, vec![strategy.run_id.clone()]);
        assert!(report.discovery_anomalies.is_empty());
        assert!(report.failures.is_empty());

        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root)
            .with_runtime(runtime),
        );

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let status = response_json(status).await;
        assert_eq!(status["grids"][0]["run_id"], strategy.run_id.as_str());
        assert_eq!(status["grids"][0]["engine_running"], true);
        assert_eq!(status["grids"][0]["runtime_advancing"], false);

        let risk = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk.status(), StatusCode::OK);
        let risk = response_json(risk).await;
        assert_eq!(risk["run_id"], strategy.run_id.as_str());
        assert_eq!(risk["engine_running"], true);
        assert_eq!(risk["runtime_advancing"], false);
        assert_eq!(risk["runtime_run_id"], strategy.run_id.as_str());
        assert_eq!(risk["runtime_state_error"], Value::Null);
        assert_eq!(risk["has_risk"], false);
    }

    #[tokio::test]
    async fn active_strategy_risk_values_only_grid_owned_inventory_at_the_live_mark() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (initial, mut gateway) = persist_clean_running_strategy(&strategy_root);
        let paths = StrategyFilePaths::new(&strategy_root, initial.run_id.clone()).unwrap();
        let source = initial
            .orders
            .values()
            .find(|order| order.shape.side == OrderSide::Sell)
            .unwrap()
            .clone();
        let source_id = source.client_order_id.clone();
        let source_exchange_id = source.exchange_order_id.clone().unwrap();
        let entry_price = source.shape.price.unwrap();
        let rules = initial.instrument_rules.clone();
        let mut machine =
            StrategyMachine::new(FileStrategyStateStore::load(paths.state()).unwrap());
        machine
            .apply_execution(
                &ExecutionReport {
                    client_order_id: source_id.clone(),
                    exchange_order_id: source_exchange_id.clone(),
                    cumulative_quantity: source.shape.quantity,
                    cumulative_quote: entry_price * source.shape.quantity,
                    cumulative_fee: Decimal::ZERO,
                    terminal_status: Some(TerminalOrderStatus::Filled),
                },
                200,
            )
            .unwrap();
        machine.materialize_replacements(&rules, 201).unwrap();
        let replacement_prepared = machine
            .store()
            .snapshot()
            .ready_intents(202)
            .unwrap()
            .pop()
            .unwrap();
        let replacement_id = replacement_prepared.client_order_id.clone();
        let replacement_exchange_id = "risk-replacement".to_owned();
        let mut replacement_accepted = replacement_prepared.clone();
        replacement_accepted.state = IntentState::Accepted {
            exchange_order_id: replacement_exchange_id.clone(),
        };
        machine
            .synchronize_intent(&replacement_accepted, 202)
            .unwrap();
        let strategy = machine.store().snapshot().clone();
        strategy.validate().unwrap();

        let mut ledger = FileOrderIntentStore::load(paths.intents()).unwrap();
        ledger
            .transition(
                &source_id,
                IntentState::Terminal {
                    status: TerminalOrderStatus::Filled,
                    exchange_order_id: Some(source_exchange_id),
                },
                200,
            )
            .unwrap();
        ledger.insert_prepared(replacement_prepared).unwrap();
        ledger
            .transition(
                &replacement_id,
                IntentState::Accepted {
                    exchange_order_id: replacement_exchange_id.clone(),
                },
                202,
            )
            .unwrap();

        gateway
            .open_orders
            .retain(|order| order.client_order_id != source_id);
        let replacement = strategy.orders.get(&replacement_id).unwrap();
        gateway.open_orders.push(AuthoritativeOrder {
            client_order_id: replacement_id,
            exchange_order_id: replacement_exchange_id,
            exchange: strategy.exchange,
            shape: replacement.shape.clone(),
            lifecycle: OrderLifecycle::Active(ActiveOrderStatus::New),
        });
        let mark_price = entry_price - Decimal::from_str_exact("0.02000").unwrap();
        let expected_unrealized = ((entry_price - mark_price) * source.shape.quantity).to_string();
        gateway.position.legs[0].signed_quantity = Decimal::from(-100);
        gateway.position.legs[0].entry_price = Some(entry_price);
        gateway.position.legs[0].mark_price = mark_price;
        // The account-level field is deliberately inconsistent: grid PnL must
        // be recomputed from owned lots instead of copied from this merged value.
        gateway.position.legs[0].unrealized_profit = Decimal::from(999);
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["realized_net_profit"], "0");
        assert_eq!(risk["grid_unrealised_pnl"], expected_unrealized);
        assert_eq!(risk["unrealised_pnl"], expected_unrealized);
        assert_eq!(risk["total_equity_profit"], expected_unrealized);
        assert_eq!(risk["total_profit"], expected_unrealized);
        assert_eq!(risk["profit_mark_price"], mark_price.to_string());
        assert_eq!(risk["profit_scope"], "strategy_owned_inventory");
        assert_eq!(risk["profit_calculation_error"], Value::Null);
        assert_eq!(risk["has_risk"], false);
    }

    #[tokio::test]
    async fn active_strategy_risk_endpoint_reports_a_missing_grid_order_without_repairing_it() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (_strategy, mut gateway) = persist_clean_running_strategy(&strategy_root);
        gateway.open_orders.pop();
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["shadow_audit"]["orders"]["missing_order_count"], 1);
        assert_eq!(
            risk["grid_coverage"]["missing_levels"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(risk["has_risk"], true);
    }

    #[tokio::test]
    async fn active_strategy_risk_endpoint_reports_an_order_owned_by_another_run() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (_strategy, mut gateway) = persist_clean_running_strategy(&strategy_root);
        let mut other_run_order = gateway.open_orders[0].clone();
        other_run_order.client_order_id = ClientOrderId::parse("g_OTHER001_1_B_1").unwrap();
        other_run_order.exchange_order_id = "other-run-exchange-order".into();
        gateway.open_orders.push(other_run_order);
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["orphan_order_count"], 1);
        assert_eq!(
            risk["orphan_orders"][0]["order_id"],
            "other-run-exchange-order"
        );
        assert_eq!(risk["shadow_audit"]["clean"], true);
        assert_eq!(risk["has_risk"], true);
    }

    #[tokio::test]
    async fn strategy_catalog_damage_can_never_produce_a_safe_risk_snapshot() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let paths =
            StrategyFilePaths::new(&strategy_root, StrategyRunId::parse("BROKENAPI").unwrap())
                .unwrap();
        fs::create_dir_all(paths.directory()).unwrap();
        fs::write(paths.state(), b"{").unwrap();
        let gateway = StrategyRiskGateway {
            open_orders: Vec::new(),
            position: PositionSnapshot {
                exchange: Exchange::Aster,
                symbol: "ANSEMUSDT".into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: Decimal::from_str_exact("0.40000").unwrap(),
                    unrealized_profit: Decimal::ZERO,
                    leverage: Some(3),
                }],
            },
        };
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["strategy_present"], false);
        assert_eq!(risk["catalog_anomaly_count"], 1);
        assert_eq!(risk["state_store_error"], "strategy_catalog_anomaly");
        assert_eq!(risk["has_risk"], true);
    }

    #[tokio::test]
    async fn active_strategy_risk_endpoint_reports_exact_position_delta() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let (_strategy, mut gateway) = persist_clean_running_strategy(&strategy_root);
        gateway.position.legs[0].signed_quantity = Decimal::from(-100);
        gateway.position.legs[0].entry_price = Some(Decimal::from_str_exact("0.40100").unwrap());
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["expected_position_net_qty"], "0");
        assert_eq!(risk["actual_position_net_qty"], "-100");
        assert_eq!(risk["unmanaged_delta_qty"], "-100");
        assert_eq!(risk["unmanaged_position"], true);
        assert_eq!(risk["has_risk"], true);
    }

    #[tokio::test]
    async fn armed_strategy_keeps_existing_position_as_a_future_baseline() {
        let directory = tempdir().unwrap();
        let strategy_root = directory.path().join("strategies");
        let config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "ANSEMUSDT".into(),
            direction: Direction::Short,
            upper_price: Decimal::from_str_exact("0.42000").unwrap(),
            lower_price: Decimal::from_str_exact("0.38000").unwrap(),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 3,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::from(100)),
            fee_rate: Some(Decimal::from_str_exact("0.00050").unwrap()),
            maker_fee_rate: Some(Decimal::from_str_exact("0.00020").unwrap()),
            taker_fee_rate: Some(Decimal::from_str_exact("0.00050").unwrap()),
            initial_order_type: InitialOrderType::Market,
            initial_order_price: None,
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: Some(Decimal::from_str_exact("0.40500").unwrap()),
            stop_loss_price: None,
            take_profit_price: None,
        };
        let armed = ArmedStrategyState::new(
            StrategyRunId::parse("ARMEDAPI").unwrap(),
            config.clone(),
            &MarketSnapshot {
                last_price: Decimal::from_str_exact("0.40000").unwrap(),
                mark_price: Decimal::from_str_exact("0.40000").unwrap(),
            },
            100,
        )
        .unwrap();
        let paths = StrategyFilePaths::new(&strategy_root, armed.run_id.clone()).unwrap();
        FileArmedStrategyStateStore::create(paths.state(), armed).unwrap();
        let gateway = StrategyRiskGateway {
            open_orders: Vec::new(),
            position: PositionSnapshot {
                exchange: Exchange::Aster,
                symbol: "ANSEMUSDT".into(),
                legs: vec![PositionLeg {
                    side: PositionSide::Both,
                    signed_quantity: Decimal::from(-300),
                    entry_price: Some(Decimal::from_str_exact("0.41000").unwrap()),
                    mark_price: Decimal::from_str_exact("0.40000").unwrap(),
                    unrealized_profit: Decimal::from(3),
                    leverage: Some(3),
                }],
            },
        };
        let mut gateways = ExchangeGatewayRegistry::empty(Exchange::Aster);
        gateways
            .register_gateway(
                Arc::new(gateway),
                ExchangeEnvironment::Production,
                "test",
                None,
            )
            .unwrap();
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                false,
                Arc::new(FileIdempotencyStore::new(
                    directory.path().join("idempotency"),
                )),
                Arc::new(DisabledStartGridCommand),
            )
            .with_exchange_gateways(gateways)
            .with_strategy_root(strategy_root),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk/ANSEMUSDT?exchange=aster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let risk = response_json(response).await;
        assert_eq!(risk["strategy_kind"], "armed");
        assert_eq!(risk["baseline_pending"], true);
        assert_eq!(risk["actual_position_net_qty"], "-300");
        assert_eq!(risk["expected_position_net_qty"], Value::Null);
        assert_eq!(risk["unmanaged_delta_qty"], Value::Null);
        assert_eq!(risk["unmanaged_position"], false);
        assert_eq!(risk["has_risk"], false);
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
    async fn strategy_detail_limits_are_bounded_before_catalog_work() {
        let app = super::super::app();
        let zero = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/grid/history?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(zero.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_json(zero).await["error"]["code"], "invalid_limit");

        let excessive = app
            .oneshot(
                Request::builder()
                    .uri("/api/trades/MUUSDT?exchange=binance&limit=1001")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(excessive.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(excessive).await["error"]["code"],
            "invalid_limit"
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
    async fn authenticated_web_start_uses_the_same_durable_idempotency_gate() {
        let directory = tempdir().unwrap();
        let command = Arc::new(CountingCommand::new());
        let (web_auth, code) = configured_service(59, false);
        let app = router_with_state(
            ApiState::for_test(
                verifier(),
                true,
                Arc::new(FileIdempotencyStore::new(directory.path())),
                command.clone(),
            )
            .with_web_authentication(web_auth),
        );

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/start")
                    .header(CONTENT_TYPE, "application/json")
                    .header(IDEMPOTENCY_KEY_HEADER, KEY)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);

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
        let cookie = login.headers()[SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();

        let web_request = || {
            Request::builder()
                .method(Method::POST)
                .uri("/api/grid/start")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .header(IDEMPOTENCY_KEY_HEADER, KEY)
                .body(Body::from("{}"))
                .unwrap()
        };
        let first = app.clone().oneshot(web_request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(first.headers().get(IDEMPOTENCY_REPLAYED_HEADER).is_none());

        let replay = app.oneshot(web_request()).await.unwrap();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(replay.headers()[IDEMPOTENCY_REPLAYED_HEADER], "true");
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
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
    async fn web_start_route_exists_but_remains_fail_closed_when_trading_is_disabled() {
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
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "rust_trading_disabled"
        );
    }

    #[tokio::test]
    async fn preview_fails_closed_before_using_unconfigured_exchange_data() {
        let app = super::super::app();
        let no_content_type = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/preview")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_content_type.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            response_json(no_content_type).await["error"]["code"],
            "json_content_type_required"
        );

        let invalid_config = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/preview")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_config.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json(invalid_config).await["error"]["code"],
            "invalid_grid_config"
        );

        let valid_config = serde_json::to_vec(&valid_preview_config()).unwrap();
        let unavailable = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/preview")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(valid_config))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(unavailable).await["error"]["code"],
            "exchange_preview_unavailable"
        );
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
