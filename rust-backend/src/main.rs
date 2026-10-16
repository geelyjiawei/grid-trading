use anyhow::Context;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn validate_strategy_file(path: &std::path::Path) -> anyhow::Result<()> {
    use grid_trading_server::{
        engine::{StrategyOrderTracking, StrategyStateStore, validate_cross_ledger_ownership},
        persistence::{FileOrderIntentStore, FileStrategyStateStore, IntentStore},
    };
    let store = FileStrategyStateStore::load(path)?;
    let state = store.snapshot();
    let ledger_path = path
        .parent()
        .context("strategy parent is missing")?
        .join("intents.json");
    anyhow::ensure!(ledger_path.is_file(), "intent ledger is missing");
    let ledger = FileOrderIntentStore::load(ledger_path)?;
    validate_cross_ledger_ownership(state, ledger.snapshot())?;
    for (id, intent) in &ledger.snapshot().intents {
        let order = state
            .orders
            .get(id)
            .context("intent has no strategy owner")?;
        anyhow::ensure!(
            intent.exchange == state.exchange && intent.shape == order.shape,
            "cross-ledger order shape mismatch"
        );
    }
    for (id, order) in &state.orders {
        if let StrategyOrderTracking::Intent { state: tracked } = &order.tracking {
            let intent = ledger
                .snapshot()
                .intents
                .get(id)
                .context("tracked intent is missing")?;
            anyhow::ensure!(&intent.state == tracked, "cross-ledger tracking mismatch");
        }
    }
    println!(
        "{}",
        serde_json::json!({"valid": true, "run_id": state.run_id,
        "revision": state.revision, "lifecycle": state.lifecycle})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if !args.is_empty() {
        anyhow::ensure!(
            args.len() == 2 && args[0] == "--validate-strategy-file",
            "unsupported command"
        );
        return validate_strategy_file(std::path::Path::new(&args[1]));
    }
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "grid_trading_server=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = grid_trading_server::app_from_environment()
        .await
        .context("invalid Rust server configuration")?;
    let address = std::env::var("GRID_BIND").unwrap_or_else(|_| "127.0.0.1:8001".into());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind Rust server to {address}"))?;
    tracing::info!(%address, "Rust server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Rust server stopped unexpectedly")?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn offline_validator_rejects_missing_or_malformed_state_without_starting_runtime() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("strategy.json");
        assert!(super::validate_strategy_file(&path).is_err());
        std::fs::write(&path, b"{}").unwrap();
        assert!(super::validate_strategy_file(&path).is_err());
    }
}
