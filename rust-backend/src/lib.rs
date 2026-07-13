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

use crate::security::{AdminTokenError, AdminTokenVerifier};
use crate::web_auth::{WebAuthConfiguration, WebAuthConfigurationError, WebAuthService};

const DEFAULT_CONTROL_ROOT: &str = "/app/data/rust-control/idempotency";
const DEFAULT_WEB_ROOT: &str = "/app/web";

pub fn app() -> Router {
    build_app(
        None,
        WebAuthService::disabled(),
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
    Ok(build_app(
        admin_token,
        web_authentication,
        control_root,
        web_root,
    ))
}

fn build_app(
    admin_token: Option<AdminTokenVerifier>,
    web_authentication: WebAuthService,
    control_root: PathBuf,
    web_root: PathBuf,
) -> Router {
    Router::new()
        .merge(api::router(admin_token, web_authentication, control_root))
        .fallback_service(ServeDir::new(web_root))
        .layer(TraceLayer::new_for_http())
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
}
