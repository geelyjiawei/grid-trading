use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};

use crate::{
    domain::InstrumentRules,
    engine::{
        ArmedStrategyState, MarketSnapshot, PositionBaseline, StrategyState, StrategyStateStore,
        StrategyStoreError,
    },
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "runtime_state", content = "state", rename_all = "snake_case")]
pub enum PersistedStrategyState {
    Armed(Box<ArmedStrategyState>),
    Active(Box<StrategyState>),
}

fn read_persisted(path: &Path) -> Result<PersistedStrategyState, StrategyStoreError> {
    let bytes = fs::read(path).map_err(StrategyStoreError::Read)?;
    serde_json::from_slice(&bytes).map_err(StrategyStoreError::InvalidJson)
}

fn commit_persisted(
    path: &Path,
    snapshot: &PersistedStrategyState,
) -> Result<(), StrategyStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(StrategyStoreError::CreateDirectory)?;
    }
    let mut file = AtomicWriteFile::options()
        .open(path)
        .map_err(StrategyStoreError::OpenAtomic)?;
    serde_json::to_writer_pretty(&mut file, snapshot).map_err(StrategyStoreError::Serialize)?;
    file.write_all(b"\n").map_err(StrategyStoreError::Write)?;
    file.commit().map_err(StrategyStoreError::Commit)?;
    sync_parent(path)?;
    Ok(())
}

#[derive(Debug)]
pub struct FileStrategyStateStore {
    path: PathBuf,
    snapshot: StrategyState,
}

impl FileStrategyStateStore {
    pub fn create(
        path: impl Into<PathBuf>,
        snapshot: StrategyState,
    ) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        if path.exists() {
            return Err(StrategyStoreError::AlreadyExists);
        }
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        let store = Self { path, snapshot };
        store.commit_snapshot(&store.snapshot)?;
        Ok(store)
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        let PersistedStrategyState::Active(snapshot) = read_persisted(&path)? else {
            return Err(StrategyStoreError::UnexpectedArmedState);
        };
        let snapshot = *snapshot;
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        Ok(Self { path, snapshot })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn commit_snapshot(&self, snapshot: &StrategyState) -> Result<(), StrategyStoreError> {
        commit_persisted(
            &self.path,
            &PersistedStrategyState::Active(Box::new(snapshot.clone())),
        )
    }
}

#[derive(Debug)]
pub struct FileArmedStrategyStateStore {
    path: PathBuf,
    snapshot: ArmedStrategyState,
}

impl FileArmedStrategyStateStore {
    pub fn create(
        path: impl Into<PathBuf>,
        snapshot: ArmedStrategyState,
    ) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        if path.exists() {
            return Err(StrategyStoreError::AlreadyExists);
        }
        snapshot.validate()?;
        commit_persisted(
            &path,
            &PersistedStrategyState::Armed(Box::new(snapshot.clone())),
        )?;
        Ok(Self { path, snapshot })
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        let PersistedStrategyState::Armed(snapshot) = read_persisted(&path)? else {
            return Err(StrategyStoreError::UnexpectedActiveState);
        };
        let snapshot = *snapshot;
        snapshot.validate()?;
        Ok(Self { path, snapshot })
    }

    pub fn snapshot(&self) -> &ArmedStrategyState {
        &self.snapshot
    }

    pub fn activate(
        self,
        market: &MarketSnapshot,
        fresh_rules: InstrumentRules,
        baseline: PositionBaseline,
        now_ms: u64,
    ) -> Result<FileStrategyStateStore, StrategyStoreError> {
        let active = self
            .snapshot
            .activate(market, fresh_rules, baseline, now_ms)?;
        commit_persisted(
            &self.path,
            &PersistedStrategyState::Active(Box::new(active.clone())),
        )?;
        Ok(FileStrategyStateStore {
            path: self.path,
            snapshot: active,
        })
    }

    pub fn cancel(&mut self, now_ms: u64) -> Result<(), StrategyStoreError> {
        let next = self.snapshot.cancelled(now_ms)?;
        if next == self.snapshot {
            return Ok(());
        }
        commit_persisted(
            &self.path,
            &PersistedStrategyState::Armed(Box::new(next.clone())),
        )?;
        self.snapshot = next;
        Ok(())
    }
}

impl StrategyStateStore for FileStrategyStateStore {
    fn snapshot(&self) -> &StrategyState {
        &self.snapshot
    }

    fn replace(&mut self, next: StrategyState) -> Result<(), StrategyStoreError> {
        if self.snapshot.revision.checked_add(1) != Some(next.revision) {
            return Err(StrategyStoreError::RevisionMismatch);
        }
        next.validate().map_err(StrategyStoreError::InvalidState)?;
        self.commit_snapshot(&next)?;
        self.snapshot = next;
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), StrategyStoreError> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(StrategyStoreError::SyncDirectory)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<(), StrategyStoreError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            Direction, Exchange, GridConfig, GridMode, InitialOrderType, InstrumentRules,
            PositionSizingMode, QuantityRules,
        },
        engine::{MarketSnapshot, PositionBaseline, StrategyRunId, build_grid_plan},
    };

    fn instrument_rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(2, 3),
            limit_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: None,
            },
            market_quantity: QuantityRules {
                step: Decimal::ONE,
                min: Decimal::ONE,
                max: None,
            },
            min_notional: Decimal::ZERO,
        }
    }

    fn state() -> StrategyState {
        let config = GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: "ANSEMUSDT".into(),
            direction: Direction::Short,
            upper_price: Decimal::new(42, 2),
            lower_price: Decimal::new(38, 2),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 3,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(100, 0)),
            fee_rate: Some(Decimal::new(5, 4)),
            maker_fee_rate: Some(Decimal::new(2, 4)),
            taker_fee_rate: Some(Decimal::new(5, 4)),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: Some(Decimal::new(40, 2)),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        };
        let rules = instrument_rules();
        let plan = build_grid_plan(
            &config,
            &MarketSnapshot {
                last_price: Decimal::new(40, 2),
                mark_price: Decimal::new(40, 2),
            },
            &rules,
        )
        .unwrap();
        StrategyState::from_plan(
            StrategyRunId::parse("ASTER001").unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap()
    }

    fn armed_state() -> ArmedStrategyState {
        let mut config = state().config;
        config.initial_order_price = None;
        config.trigger_price = Some(Decimal::new(405, 3));
        ArmedStrategyState::new(
            StrategyRunId::parse("ARMFILE1").unwrap(),
            config,
            &MarketSnapshot {
                last_price: Decimal::new(400, 3),
                mark_price: Decimal::new(400, 3),
            },
            100,
        )
        .unwrap()
    }

    #[test]
    fn atomic_file_store_round_trips_exact_strategy_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let original = state();

        let store = FileStrategyStateStore::create(&path, original.clone()).unwrap();
        let restored = FileStrategyStateStore::load(&path).unwrap();

        assert_eq!(store.path(), path);
        assert_eq!(restored.snapshot(), &original);
        assert!(fs::read_to_string(&path).unwrap().ends_with('\n'));
    }

    #[test]
    fn existing_state_file_is_never_overwritten_by_create() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        fs::write(&path, b"audit evidence").unwrap();

        assert!(matches!(
            FileStrategyStateStore::create(&path, state()),
            Err(StrategyStoreError::AlreadyExists)
        ));
        assert_eq!(fs::read(&path).unwrap(), b"audit evidence");
    }

    #[test]
    fn corrupt_state_is_retained_and_fails_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        fs::write(&path, b"{not-json").unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidJson(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), b"{not-json");
    }

    #[test]
    fn stale_revision_cannot_replace_newer_durable_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let original = state();
        let mut store = FileStrategyStateStore::create(&path, original.clone()).unwrap();
        let bytes_before = fs::read(&path).unwrap();

        assert!(matches!(
            store.replace(original),
            Err(StrategyStoreError::RevisionMismatch)
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
    }

    #[test]
    fn armed_state_round_trips_without_creating_any_order_plan() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let original = armed_state();

        FileArmedStrategyStateStore::create(&path, original.clone()).unwrap();
        let restored = FileArmedStrategyStateStore::load(&path).unwrap();
        let json = fs::read_to_string(&path).unwrap();

        assert_eq!(restored.snapshot(), &original);
        assert!(json.contains("\"runtime_state\": \"armed\""));
        assert!(!json.contains("\"orders\""));
        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::UnexpectedArmedState)
        ));
    }

    #[test]
    fn unhit_trigger_leaves_the_durable_armed_file_byte_for_byte_unchanged() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let store = FileArmedStrategyStateStore::create(&path, armed_state()).unwrap();
        let bytes_before = fs::read(&path).unwrap();

        let result = store.activate(
            &MarketSnapshot {
                last_price: Decimal::new(404, 3),
                mark_price: Decimal::new(404, 3),
            },
            instrument_rules(),
            PositionBaseline::flat(),
            101,
        );

        assert!(matches!(
            result,
            Err(StrategyStoreError::ArmedStrategy(
                crate::engine::ArmedStrategyError::NotTriggered
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
    }

    #[test]
    fn trigger_activation_atomically_replaces_armed_state_with_active_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let store = FileArmedStrategyStateStore::create(&path, armed_state()).unwrap();

        let active = store
            .activate(
                &MarketSnapshot {
                    last_price: Decimal::new(406, 3),
                    mark_price: Decimal::new(406, 3),
                },
                instrument_rules(),
                PositionBaseline::flat(),
                102,
            )
            .unwrap();
        let restored = FileStrategyStateStore::load(&path).unwrap();
        let json = fs::read_to_string(&path).unwrap();

        assert_eq!(active.snapshot(), restored.snapshot());
        assert_eq!(active.snapshot().revision, 1);
        assert_eq!(active.snapshot().triggered_at_ms, Some(102));
        assert!(json.contains("\"runtime_state\": \"active\""));
        assert!(json.contains("\"orders\""));
        assert!(matches!(
            FileArmedStrategyStateStore::load(&path),
            Err(StrategyStoreError::UnexpectedActiveState)
        ));
    }

    #[test]
    fn invalid_fresh_baseline_cannot_partially_activate_the_armed_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let store = FileArmedStrategyStateStore::create(&path, armed_state()).unwrap();
        let bytes_before = fs::read(&path).unwrap();

        let result = store.activate(
            &MarketSnapshot {
                last_price: Decimal::new(406, 3),
                mark_price: Decimal::new(406, 3),
            },
            instrument_rules(),
            PositionBaseline {
                signed_quantity: Decimal::new(10, 0),
                entry_price: Some(Decimal::new(40, 2)),
            },
            102,
        );

        assert!(matches!(
            result,
            Err(StrategyStoreError::ArmedStrategy(
                crate::engine::ArmedStrategyError::StrategyState(
                    crate::engine::StrategyStateError::BaselineDirectionConflict
                )
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
        assert!(FileArmedStrategyStateStore::load(&path).is_ok());
    }

    #[test]
    fn cancelling_an_armed_strategy_is_durable_and_idempotent() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let mut store = FileArmedStrategyStateStore::create(&path, armed_state()).unwrap();

        store.cancel(101).unwrap();
        let bytes = fs::read(&path).unwrap();
        store.cancel(102).unwrap();

        assert_eq!(
            store.snapshot().lifecycle,
            crate::engine::ArmedStrategyLifecycle::Cancelled
        );
        assert_eq!(store.snapshot().revision, 1);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}
