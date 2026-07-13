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
