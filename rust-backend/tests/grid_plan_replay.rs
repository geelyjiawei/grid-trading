use std::{fs, path::Path, process::Command};

use serde_json::Value;

#[test]
fn rust_grid_plans_match_the_cross_language_golden_replay() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("Rust crate must remain inside the repository");
    let fixture = repository.join("contracts/replay/grid-plan-v1.json");
    let expected_path = repository.join("contracts/replay/grid-plan-v1.expected.json");
    let output = Command::new(env!("CARGO_BIN_EXE_grid_plan_replay"))
        .arg(&fixture)
        .output()
        .expect("grid plan replay binary must start");
    assert!(
        output.status.success(),
        "grid plan replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let actual: Value =
        serde_json::from_slice(&output.stdout).expect("Rust replay output must be JSON");
    let expected: Value =
        serde_json::from_slice(&fs::read(&expected_path).expect("golden replay output must exist"))
            .expect("golden replay output must be JSON");
    assert_eq!(actual, expected);
}

#[test]
fn version_two_replay_uses_unique_cell_capacity_instead_of_legacy_contract() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut fixture: Value = serde_json::from_slice(
        &fs::read(repository.join("contracts/replay/grid-plan-v1.json")).unwrap(),
    )
    .unwrap();
    fixture["version"] = Value::from(2);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("v2.json");
    fs::write(&path, serde_json::to_vec(&fixture).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_grid_plan_replay"))
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual["version"], 2);
    for (case, result) in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .zip(actual["results"].as_array().unwrap())
    {
        let orders = result["plan"]["grid_orders"].as_array().unwrap();
        assert_eq!(
            orders.len() as u64,
            case["config"]["grid_count"].as_u64().unwrap()
        );
        let mut levels = std::collections::BTreeSet::new();
        for order in orders {
            assert!(levels.insert(order["level_index"].as_u64().unwrap()));
            if case["config"]["position_sizing_mode"] == "fixed_grid_qty" {
                let expected: rust_decimal::Decimal = case["config"]["grid_order_qty"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let actual: rust_decimal::Decimal =
                    order["quantity"].as_str().unwrap().parse().unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
}
