pub mod api;
pub mod domain;
pub mod engine;
pub mod exchange;
pub mod persistence;
pub mod security;

use std::{env, path::PathBuf};

use axum::Router;
use thiserror::Error;
use tower_http::trace::TraceLayer;
use zeroize::Zeroizing;

use crate::security::{AdminTokenError, AdminTokenVerifier};

const DEFAULT_CONTROL_ROOT: &str = "/app/data/rust-control/idempotency";

pub fn app() -> Router {
    build_app(None, PathBuf::from(DEFAULT_CONTROL_ROOT))
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
    Ok(build_app(admin_token, control_root))
}

fn build_app(admin_token: Option<AdminTokenVerifier>, control_root: PathBuf) -> Router {
    Router::new()
        .merge(api::router(admin_token, control_root))
        .layer(TraceLayer::new_for_http())
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
    #[error("GRID_RUST_CONTROL_ROOT must not be empty")]
    EmptyControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT is not valid Unicode")]
    NonUnicodeControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT must be an absolute path")]
    RelativeControlRoot,
    #[error("{0} must be a boolean value")]
    InvalidBoolean(&'static str),
    #[error("Rust trading writes are not available in this migration build")]
    TradingWritesUnavailable,
}
