pub mod api;
pub mod domain;
pub mod engine;
pub mod exchange;
pub mod persistence;
pub mod security;
pub mod web_auth;

use std::{env, path::PathBuf};

use axum::Router;
use thiserror::Error;
use tower_http::{services::ServeDir, trace::TraceLayer};
use zeroize::Zeroizing;

use crate::web_auth::{WebAuthConfiguration, WebAuthConfigurationError, WebAuthService};
use crate::{
    domain::Exchange,
    exchange::{
        configured::{
            ExchangeCredentials, ExchangeEnvironment, ExchangeGatewayBuildError,
            ExchangeGatewayFactory,
        },
        registry::{ExchangeGatewayRegistry, RegistryError},
    },
    security::{AdminTokenError, AdminTokenVerifier},
};

const DEFAULT_CONTROL_ROOT: &str = "/app/data/rust-control/idempotency";
const DEFAULT_WEB_ROOT: &str = "/app/web";

pub fn app() -> Router {
    build_app(
        None,
        WebAuthService::disabled(),
        ExchangeGatewayRegistry::default(),
        PathBuf::from(DEFAULT_CONTROL_ROOT),
        PathBuf::from(DEFAULT_WEB_ROOT),
    )
}

pub fn app_from_environment() -> Result<Router, AppConfigurationError> {
    if parse_env_flag("GRID_RUST_TRADING_ENABLED")? {
        return Err(AppConfigurationError::TradingWritesUnavailable);
    }

    let admin_token = match env::var("GRID_RUST_ADMIN_TOKEN") {
        Ok(secret) => Some(AdminTokenVerifier::from_secret(Zeroizing::new(secret))?),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeAdminToken);
        }
    };
    let web_authentication = WebAuthService::from_configuration(WebAuthConfiguration {
        required: parse_env_flag("AUTH_REQUIRED")?,
        username: read_env_text("ADMIN_USERNAME")?.unwrap_or_else(|| "admin".to_owned()),
        password_hash: Zeroizing::new(read_env_text("ADMIN_PASSWORD_HASH")?.unwrap_or_default()),
        totp_secret: Zeroizing::new(read_env_text("TOTP_SECRET")?.unwrap_or_default()),
        cookie_secure: parse_env_flag("AUTH_COOKIE_SECURE")?,
    })?;
    let control_root = match env::var("GRID_RUST_CONTROL_ROOT") {
        Ok(value) if value.trim().is_empty() => {
            return Err(AppConfigurationError::EmptyControlRoot);
        }
        Ok(value) => PathBuf::from(value),
        Err(env::VarError::NotPresent) => PathBuf::from(DEFAULT_CONTROL_ROOT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeControlRoot);
        }
    };
    if !control_root.is_absolute() {
        return Err(AppConfigurationError::RelativeControlRoot);
    }
    let web_root = match env::var("GRID_WEB_ROOT") {
        Ok(value) if value.trim().is_empty() => return Err(AppConfigurationError::EmptyWebRoot),
        Ok(value) => PathBuf::from(value),
        Err(env::VarError::NotPresent) => PathBuf::from(DEFAULT_WEB_ROOT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeWebRoot);
        }
    };
    if !web_root.is_absolute() {
        return Err(AppConfigurationError::RelativeWebRoot);
    }
    let exchange_gateways = exchange_gateways_from_environment()?;
    Ok(build_app(
        admin_token,
        web_authentication,
        exchange_gateways,
        control_root,
        web_root,
    ))
}

fn build_app(
    admin_token: Option<AdminTokenVerifier>,
    web_authentication: WebAuthService,
    exchange_gateways: ExchangeGatewayRegistry,
    control_root: PathBuf,
    web_root: PathBuf,
) -> Router {
    Router::new()
        .merge(api::router(
            admin_token,
            web_authentication,
            exchange_gateways,
            control_root,
        ))
        .fallback_service(ServeDir::new(web_root))
        .layer(TraceLayer::new_for_http())
}

fn exchange_gateways_from_environment() -> Result<ExchangeGatewayRegistry, AppConfigurationError> {
    let preferred = preferred_exchange_from_environment()?;
    let mut registry = ExchangeGatewayRegistry::empty(preferred);

    if let Some((api_key, api_secret)) =
        read_credential_pair("BINANCE_API_KEY", "BINANCE_API_SECRET", "Binance")?
    {
        let masked = mask_identifier(&api_key);
        let environment = exchange_environment("BINANCE_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::binance(api_key, api_secret)?)?;
        registry.register_configured(gateway, environment, "env", Some(masked))?;
    }

    if let Some(private_key) = read_aster_private_key()? {
        let environment = exchange_environment("ASTER_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::aster(private_key)?)?;
        registry.register_configured(
            gateway,
            environment,
            "env",
            Some("wallet configured".into()),
        )?;
    }

    if let Some((api_key, api_secret)) =
        read_credential_pair("BYBIT_API_KEY", "BYBIT_API_SECRET", "Bybit")?
    {
        let masked = mask_identifier(&api_key);
        let environment = exchange_environment("BYBIT_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::bybit(api_key, api_secret)?)?;
        registry.register_configured(gateway, environment, "env", Some(masked))?;
    }

    Ok(registry)
}

fn preferred_exchange_from_environment() -> Result<Exchange, AppConfigurationError> {
    let value = read_nonempty_env_text("GRID_EXCHANGE")?.or(read_nonempty_env_text("EXCHANGE")?);
    parse_preferred_exchange(value)
}

fn parse_preferred_exchange(value: Option<String>) -> Result<Exchange, AppConfigurationError> {
    let value = value.unwrap_or_else(|| "bybit".into());
    match value.to_ascii_lowercase().as_str() {
        "binance" => Ok(Exchange::Binance),
        "aster" => Ok(Exchange::Aster),
        "bybit" => Ok(Exchange::Bybit),
        _ => Err(AppConfigurationError::InvalidExchange),
    }
}

fn read_credential_pair(
    key_name: &'static str,
    secret_name: &'static str,
    exchange: &'static str,
) -> Result<Option<(String, String)>, AppConfigurationError> {
    complete_credential_pair(
        read_nonempty_env_text(key_name)?,
        read_nonempty_env_text(secret_name)?,
        exchange,
    )
}

fn complete_credential_pair(
    key: Option<String>,
    secret: Option<String>,
    exchange: &'static str,
) -> Result<Option<(String, String)>, AppConfigurationError> {
    match (key, secret) {
        (None, None) => Ok(None),
        (Some(key), Some(secret)) => Ok(Some((key, secret))),
        _ => Err(AppConfigurationError::IncompleteExchangeCredentials(
            exchange,
        )),
    }
}

fn read_aster_private_key() -> Result<Option<String>, AppConfigurationError> {
    let current = read_nonempty_env_text("ASTER_SIGNER_PRIVATE_KEY")?;
    let legacy = read_nonempty_env_text("ASTER_API_SECRET")?;
    select_aster_private_key(current, legacy)
}

fn select_aster_private_key(
    current: Option<String>,
    legacy: Option<String>,
) -> Result<Option<String>, AppConfigurationError> {
    match (current, legacy) {
        (Some(current), Some(legacy)) if current != legacy => {
            Err(AppConfigurationError::ConflictingAsterPrivateKeys)
        }
        (Some(current), _) => Ok(Some(current)),
        (None, legacy) => Ok(legacy),
    }
}

fn read_nonempty_env_text(name: &'static str) -> Result<Option<String>, AppConfigurationError> {
    normalize_nonempty_env_text(name, read_env_text(name)?)
}

fn normalize_nonempty_env_text(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<String>, AppConfigurationError> {
    match value {
        None => Ok(None),
        Some(value) if value.trim().is_empty() => Ok(None),
        Some(value) if value.trim() != value => {
            Err(AppConfigurationError::EnvironmentWhitespace(name))
        }
        Some(value) => Ok(Some(value)),
    }
}

fn exchange_environment(
    testnet_flag: &'static str,
) -> Result<ExchangeEnvironment, AppConfigurationError> {
    Ok(if parse_env_flag(testnet_flag)? {
        ExchangeEnvironment::Testnet
    } else {
        ExchangeEnvironment::Production
    })
}

fn mask_identifier(value: &str) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    if characters.len() <= 8 {
        return "****".into();
    }
    let prefix = characters[..4].iter().collect::<String>();
    let suffix = characters[characters.len() - 4..]
        .iter()
        .collect::<String>();
    format!("{prefix}****{suffix}")
}

fn read_env_text(name: &'static str) -> Result<Option<String>, AppConfigurationError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(AppConfigurationError::NonUnicodeEnvironment(name))
        }
    }
}

fn parse_env_flag(name: &'static str) -> Result<bool, AppConfigurationError> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(false),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::InvalidBoolean(name));
        }
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        _ => Err(AppConfigurationError::InvalidBoolean(name)),
    }
}

#[derive(Debug, Error)]
pub enum AppConfigurationError {
    #[error("GRID_RUST_ADMIN_TOKEN is not valid Unicode")]
    NonUnicodeAdminToken,
    #[error("GRID_RUST_ADMIN_TOKEN is invalid: {0}")]
    InvalidAdminToken(#[from] AdminTokenError),
    #[error("{0} is not valid Unicode")]
    NonUnicodeEnvironment(&'static str),
    #[error("web authentication configuration is invalid: {0}")]
    InvalidWebAuthentication(#[from] WebAuthConfigurationError),
    #[error("GRID_RUST_CONTROL_ROOT must not be empty")]
    EmptyControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT is not valid Unicode")]
    NonUnicodeControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT must be an absolute path")]
    RelativeControlRoot,
    #[error("GRID_WEB_ROOT must not be empty")]
    EmptyWebRoot,
    #[error("GRID_WEB_ROOT is not valid Unicode")]
    NonUnicodeWebRoot,
    #[error("GRID_WEB_ROOT must be an absolute path")]
    RelativeWebRoot,
    #[error("{0} must be a boolean value")]
    InvalidBoolean(&'static str),
    #[error("{0} must not contain leading or trailing whitespace")]
    EnvironmentWhitespace(&'static str),
    #[error("GRID_EXCHANGE or EXCHANGE must be binance, aster, or bybit")]
    InvalidExchange,
    #[error("{0} API credentials must provide both key and secret")]
    IncompleteExchangeCredentials(&'static str),
    #[error("ASTER_SIGNER_PRIVATE_KEY conflicts with legacy ASTER_API_SECRET")]
    ConflictingAsterPrivateKeys,
    #[error("exchange credential is invalid: {0}")]
    InvalidExchangeCredential(#[from] exchange::configured::CredentialError),
    #[error("exchange gateway initialization failed: {0}")]
    ExchangeGateway(#[from] ExchangeGatewayBuildError),
    #[error("exchange gateway registry is invalid: {0}")]
    ExchangeRegistry(#[from] RegistryError),
    #[error("Rust trading writes are not available in this migration build")]
    TradingWritesUnavailable,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn rust_app_serves_the_built_vue_assets_without_a_python_runtime() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            web.path().to_path_buf(),
        )
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/html");
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            "<main>vue shell</main>"
        );
    }

    #[tokio::test]
    async fn unknown_api_path_is_never_rewritten_to_the_vue_index() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            web.path().to_path_buf(),
        )
        .oneshot(
            Request::builder()
                .uri("/api/not-a-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_api_post_is_a_not_found_instead_of_a_static_method_error() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            web.path().to_path_buf(),
        )
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/not-a-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn exchange_identifier_masking_never_returns_the_original_value() {
        assert_eq!(mask_identifier("1234567890abcdef"), "1234****cdef");
        assert_eq!(mask_identifier("short"), "****");
    }

    #[test]
    fn exchange_environment_text_fails_early_on_surrounding_whitespace() {
        assert_eq!(normalize_nonempty_env_text("KEY", None).unwrap(), None);
        assert_eq!(
            normalize_nonempty_env_text("KEY", Some("   ".into())).unwrap(),
            None
        );
        assert_eq!(
            normalize_nonempty_env_text("KEY", Some("exact-value".into())).unwrap(),
            Some("exact-value".into())
        );
        assert!(matches!(
            normalize_nonempty_env_text("KEY", Some(" exact-value ".into())),
            Err(AppConfigurationError::EnvironmentWhitespace("KEY"))
        ));
    }

    #[test]
    fn preferred_exchange_parser_is_deterministic_and_fails_closed() {
        assert_eq!(parse_preferred_exchange(None).unwrap(), Exchange::Bybit);
        assert_eq!(
            parse_preferred_exchange(Some("BINANCE".into())).unwrap(),
            Exchange::Binance
        );
        assert_eq!(
            parse_preferred_exchange(Some("aster".into())).unwrap(),
            Exchange::Aster
        );
        assert!(matches!(
            parse_preferred_exchange(Some("unknown".into())),
            Err(AppConfigurationError::InvalidExchange)
        ));
    }

    #[test]
    fn exchange_credentials_must_be_complete_before_gateway_construction() {
        assert_eq!(
            complete_credential_pair(None, None, "Binance").unwrap(),
            None
        );
        assert_eq!(
            complete_credential_pair(Some("key".into()), Some("secret".into()), "Binance").unwrap(),
            Some(("key".into(), "secret".into()))
        );
        assert!(matches!(
            complete_credential_pair(Some("key".into()), None, "Binance"),
            Err(AppConfigurationError::IncompleteExchangeCredentials(
                "Binance"
            ))
        ));
        assert!(matches!(
            complete_credential_pair(None, Some("secret".into()), "Binance"),
            Err(AppConfigurationError::IncompleteExchangeCredentials(
                "Binance"
            ))
        ));
    }

    #[test]
    fn aster_current_and_legacy_private_keys_cannot_disagree() {
        assert_eq!(select_aster_private_key(None, None).unwrap(), None);
        assert_eq!(
            select_aster_private_key(Some("same".into()), Some("same".into())).unwrap(),
            Some("same".into())
        );
        assert!(matches!(
            select_aster_private_key(Some("current".into()), Some("legacy".into())),
            Err(AppConfigurationError::ConflictingAsterPrivateKeys)
        ));
    }
}
