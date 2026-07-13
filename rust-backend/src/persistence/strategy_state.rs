use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use atomic_write_file::AtomicWriteFile;

use crate::engine::{StrategyState, StrategyStateStore, StrategyStoreError};

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
        let bytes = fs::read(&path).map_err(StrategyStoreError::Read)?;
        let snapshot: StrategyState =
            serde_json::from_slice(&bytes).map_err(StrategyStoreError::InvalidJson)?;
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        Ok(Self { path, snapshot })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn commit_snapshot(&self, snapshot: &StrategyState) -> Result<(), StrategyStoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(StrategyStoreError::CreateDirectory)?;
        }
        let mut file = AtomicWriteFile::options()
            .open(&self.path)
            .map_err(StrategyStoreError::OpenAtomic)?;
        serde_json::to_writer_pretty(&mut file, snapshot).map_err(StrategyStoreError::Serialize)?;
        file.write_all(b"\n").map_err(StrategyStoreError::Write)?;
        file.commit().map_err(StrategyStoreError::Commit)?;
        sync_parent(&self.path)?;
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
        let rules = InstrumentRules {
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
        };
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
}
