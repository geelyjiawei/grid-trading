use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequestParts, OriginalUri, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE},
        request::Parts,
    },
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    persistence::{
        BeginIdempotency, FileIdempotencyStore, IdempotencyError, IdempotencyKey, IdempotencyStore,
        RequestFingerprint, StoredCommandResponse,
    },
    security::AdminTokenVerifier,
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
    trading_enabled: bool,
    idempotency: Arc<dyn IdempotencyStore>,
    start_command: Arc<dyn StartGridCommand>,
}

impl ApiState {
    fn disabled(admin_token: Option<AdminTokenVerifier>, idempotency_root: PathBuf) -> Self {
        Self {
            authentication: admin_token.map_or(
                AdminAuthentication::Unconfigured,
                AdminAuthentication::Configured,
            ),
            trading_enabled: false,
            idempotency: Arc::new(FileIdempotencyStore::new(idempotency_root)),
            start_command: Arc::new(DisabledStartGridCommand),
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
            trading_enabled,
            idempotency,
            start_command,
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

async fn api_not_found() -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        "api_route_not_found",
        "API route not found",
    )
}

pub(crate) fn router(admin_token: Option<AdminTokenVerifier>, idempotency_root: PathBuf) -> Router {
    router_with_state(ApiState::disabled(admin_token, idempotency_root))
}

fn router_with_state(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(health))
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
        http::{Request, header::CONTENT_TYPE},
    };
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use zeroize::Zeroizing;

    use super::*;

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
