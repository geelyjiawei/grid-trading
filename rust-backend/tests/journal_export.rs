use std::{fs, path::Path, process::Command};

use grid_trading_server::{
    domain::{ClientOrderId, Exchange, OrderIntent},
    engine::{
        MarketSnapshot, PositionBaseline, StrategyRunId, StrategyState, StrategyStateStore,
        build_grid_plan,
    },
    persistence::{
        FileOrderIntentStore, FileStrategyStateStore, IntentStore, PersistedStrategyState,
    },
};
use serde_json::Value;

#[test]
fn native_offline_exports_replay_latest_durable_state_without_modifying_files() {
    let fixture: Value =
        serde_json::from_slice(include_bytes!("../../contracts/replay/grid-plan-v1.json")).unwrap();
    let case = &fixture["cases"][0];
    let config = serde_json::from_value(case["config"].clone()).unwrap();
    let rules = serde_json::from_value(case["rules"].clone()).unwrap();
    let market: MarketSnapshot = serde_json::from_value(case["market"].clone()).unwrap();
    let plan = build_grid_plan(&config, &market, &rules).unwrap();
    let state = StrategyState::from_plan(
        StrategyRunId::parse("EXPORT01").unwrap(),
        config,
        rules,
        plan,
        PositionBaseline::flat(),
        100,
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("strategy.json");
    let ledger_path = directory.path().join("intents.json");
    let mut state_store = FileStrategyStateStore::create(&path, state).unwrap();
    let mut next = state_store.snapshot().clone();
    next.revision += 1;
    next.updated_at_ms += 1;
    state_store.replace(next.clone()).unwrap();
    let mut intents = FileOrderIntentStore::load(&ledger_path).unwrap();
    let order = next.orders.values().next().unwrap();
    intents
        .insert_prepared(
            OrderIntent::prepare(
                ClientOrderId::parse("export_intent").unwrap(),
                Exchange::Binance,
                order.shape.clone(),
                100,
            )
            .unwrap(),
        )
        .unwrap();
    let before = files(directory.path());
    let output = Command::new(env!("CARGO_BIN_EXE_grid-trading-server"))
        .arg("--export-strategy-file")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let exported: PersistedStrategyState = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(exported, PersistedStrategyState::Active(Box::new(next)));
    let output = Command::new(env!("CARGO_BIN_EXE_grid-trading-server"))
        .arg("--export-intent-file")
        .arg(&ledger_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let exported: grid_trading_server::persistence::LedgerSnapshot =
        serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(&exported, intents.snapshot());
    assert_eq!(files(directory.path()), before);
}

fn files(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            // Windows mandatory leases intentionally make lock-file contents unreadable.
            let bytes = if path
                .extension()
                .is_some_and(|extension| extension == "lock")
            {
                Vec::new()
            } else {
                fs::read(&path).unwrap()
            };
            (path, bytes)
        })
        .collect()
}
