use anyhow::Context;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "grid_trading_server=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let address = std::env::var("GRID_BIND").unwrap_or_else(|_| "127.0.0.1:8001".into());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind Rust migration server to {address}"))?;
    tracing::info!(%address, "Rust migration server listening in non-trading mode");
    axum::serve(listener, grid_trading_server::app())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Rust migration server stopped unexpectedly")?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
