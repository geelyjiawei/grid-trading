use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};

use super::journal::{self, Journal};

use crate::{
    domain::InstrumentRules,
    engine::{
        ArmedStrategyState, MarketSnapshot, PositionBaseline, PreparedStrategy, StrategyState,
        StrategyStateStore, StrategyStoreError, ValidatedStrategyState,
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
    if journal::is_checkpoint(&bytes).map_err(StrategyStoreError::InvalidJson)? {
        let (state, _) =
            Journal::read::<StrategyState>(&bytes, path).map_err(StrategyStoreError::Journal)?;
        return Ok(PersistedStrategyState::Active(Box::new(state)));
    }
    serde_json::from_slice(&bytes).map_err(StrategyStoreError::InvalidJson)
}

fn commit_persisted(
    path: &Path,
    snapshot: &PersistedStrategyState,
) -> Result<(), StrategyStoreError> {
    let mut bytes = serde_json::to_vec(snapshot).map_err(StrategyStoreError::Serialize)?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(StrategyStoreError::CreateDirectory)?;
    }
    let mut file = AtomicWriteFile::options()
        .open(path)
        .map_err(StrategyStoreError::OpenAtomic)?;
    file.write_all(&bytes).map_err(StrategyStoreError::Write)?;
    file.commit().map_err(StrategyStoreError::Commit)?;
    sync_parent(path)?;
    Ok(())
}

fn create_persisted(
    path: &Path,
    snapshot: &PersistedStrategyState,
) -> Result<(), StrategyStoreError> {
    let mut bytes = serde_json::to_vec(snapshot).map_err(StrategyStoreError::Serialize)?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(StrategyStoreError::CreateDirectory)?;
    }

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(StrategyStoreError::AlreadyExists);
        }
        Err(error) => return Err(StrategyStoreError::CreateNew(error)),
    };
    file.write_all(&bytes).map_err(StrategyStoreError::Write)?;
    file.sync_all().map_err(StrategyStoreError::SyncFile)?;
    sync_parent(path)?;
    Ok(())
}

#[derive(Debug)]
pub enum FilePreparedStrategyStore {
    Armed(Box<FileArmedStrategyStateStore>),
    Active(Box<FileStrategyStateStore>),
}

impl FilePreparedStrategyStore {
    pub fn create(
        path: impl Into<PathBuf>,
        prepared: PreparedStrategy,
    ) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        match prepared {
            PreparedStrategy::Armed(state) => FileArmedStrategyStateStore::create(path, *state)
                .map(Box::new)
                .map(Self::Armed),
            PreparedStrategy::Active(state) => FileStrategyStateStore::create(path, *state)
                .map(Box::new)
                .map(Self::Active),
        }
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        let bytes = fs::read(&path).map_err(StrategyStoreError::Read)?;
        if journal::is_checkpoint(&bytes).map_err(StrategyStoreError::InvalidJson)? {
            return FileStrategyStateStore::from_bytes(path, &bytes)
                .map(Box::new)
                .map(Self::Active);
        }
        match serde_json::from_slice(&bytes).map_err(StrategyStoreError::InvalidJson)? {
            PersistedStrategyState::Armed(snapshot) => {
                let snapshot = *snapshot;
                snapshot.validate()?;
                Ok(Self::Armed(Box::new(FileArmedStrategyStateStore {
                    path,
                    snapshot,
                })))
            }
            PersistedStrategyState::Active(snapshot) => {
                FileStrategyStateStore::from_legacy(path, *snapshot, &bytes)
                    .map(Box::new)
                    .map(Self::Active)
            }
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            Self::Armed(store) => store.path(),
            Self::Active(store) => store.path(),
        }
    }
}

#[derive(Debug)]
pub struct FileStrategyStateStore {
    path: PathBuf,
    snapshot: Arc<StrategyState>,
    journal: Journal,
}

impl FileStrategyStateStore {
    pub fn create(
        path: impl Into<PathBuf>,
        snapshot: StrategyState,
    ) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        create_persisted(&path, &PersistedStrategyState::Active(Box::new(snapshot)))?;
        Self::load(path)
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, StrategyStoreError> {
        let path = path.into();
        let bytes = fs::read(&path).map_err(StrategyStoreError::Read)?;
        Self::from_bytes(path, &bytes)
    }

    fn from_legacy(
        path: PathBuf,
        snapshot: StrategyState,
        bytes: &[u8],
    ) -> Result<Self, StrategyStoreError> {
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        let journal = Journal::legacy(bytes, &path).map_err(StrategyStoreError::Journal)?;
        Ok(Self {
            path,
            snapshot: Arc::new(snapshot),
            journal,
        })
    }

    fn from_bytes(path: PathBuf, bytes: &[u8]) -> Result<Self, StrategyStoreError> {
        let (snapshot, journal) =
            if journal::is_checkpoint(bytes).map_err(StrategyStoreError::InvalidJson)? {
                Journal::read::<StrategyState>(bytes, &path).map_err(StrategyStoreError::Journal)?
            } else {
                let PersistedStrategyState::Active(snapshot) =
                    serde_json::from_slice(bytes).map_err(StrategyStoreError::InvalidJson)?
                else {
                    return Err(StrategyStoreError::UnexpectedArmedState);
                };
                return Self::from_legacy(path, *snapshot, bytes);
            };
        snapshot
            .validate()
            .map_err(StrategyStoreError::InvalidState)?;
        Ok(Self {
            path,
            snapshot: Arc::new(snapshot),
            journal,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn relocate(&mut self, path: impl Into<PathBuf>) {
        self.path = path.into();
    }

    fn commit_snapshot(&mut self, snapshot: Arc<StrategyState>) -> Result<(), StrategyStoreError> {
        self.journal
            .append(&self.path, self.snapshot.as_ref(), snapshot)
            .map_err(StrategyStoreError::Journal)
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
        snapshot.validate()?;
        create_persisted(
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn relocate(&mut self, path: impl Into<PathBuf>) {
        self.path = path.into();
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
        self.activate_prepared(active)
    }

    pub fn activate_prepared(
        self,
        active: StrategyState,
    ) -> Result<FileStrategyStateStore, StrategyStoreError> {
        let mut store = self;
        store.try_activate_prepared(&active)
    }

    pub fn try_activate_prepared(
        &mut self,
        active: &StrategyState,
    ) -> Result<FileStrategyStateStore, StrategyStoreError> {
        self.snapshot.validate_active_successor(active)?;
        commit_persisted(
            &self.path,
            &PersistedStrategyState::Active(Box::new(active.clone())),
        )?;
        FileStrategyStateStore::load(self.path.clone())
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

    fn snapshot_is_known_valid(&self) -> bool {
        true
    }

    fn replace(&mut self, next: StrategyState) -> Result<(), StrategyStoreError> {
        if self.snapshot.revision.checked_add(1) != Some(next.revision) {
            return Err(StrategyStoreError::RevisionMismatch);
        }
        next.validate().map_err(StrategyStoreError::InvalidState)?;
        let next = Arc::new(next);
        self.commit_snapshot(next.clone())?;
        self.snapshot = next;
        Ok(())
    }

    fn replace_validated(
        &mut self,
        next: ValidatedStrategyState,
    ) -> Result<(), StrategyStoreError> {
        let next = next.into_inner();
        if self.snapshot.revision.checked_add(1) != Some(next.revision) {
            return Err(StrategyStoreError::RevisionMismatch);
        }
        let next = Arc::new(next);
        self.commit_snapshot(next.clone())?;
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
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            ClientOrderId, Direction, Exchange, GridConfig, GridMode, InitialOrderType,
            InstrumentRules, PositionSizingMode, QuantityRules,
        },
        engine::{
            MarketSnapshot, PositionBaseline, ReplacementObligation, ReplacementObligationKind,
            StrategyOrderPurpose, StrategyRunId, StrategyStateError, build_grid_plan,
        },
    };

    fn instrument_rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(2, 3),
            max_price_significant_digits: None,
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
    fn warm_strategy_commit_replays_without_rewriting_full_checkpoint() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut store = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = store.snapshot().clone();
        next.revision += 1;
        next.updated_at_ms += 1;
        store.replace(next).unwrap();
        let checkpoint = fs::read(&path).unwrap();
        let mut next = store.snapshot().clone();
        next.revision += 1;
        next.updated_at_ms += 1;
        store.replace(next.clone()).unwrap();
        assert!(
            fs::read(&path).unwrap() == checkpoint,
            "warm commit rewrote the full checkpoint"
        );
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            &next
        );
    }

    #[test]
    fn atomic_file_store_round_trips_exact_strategy_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let original = state();

        let store = FileStrategyStateStore::create(&path, original.clone()).unwrap();
        let restored = FileStrategyStateStore::load(&path).unwrap();

        assert_eq!(store.path(), path);
        assert!(store.snapshot_is_known_valid());
        assert!(restored.snapshot_is_known_valid());
        assert_eq!(restored.snapshot(), &original);
        let persisted = fs::read_to_string(&path).unwrap();
        assert!(persisted.ends_with('\n'));
        assert_eq!(persisted.lines().count(), 1);
    }

    #[test]
    fn journal_readers_are_read_only_and_writer_is_exclusive() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = writer.snapshot().clone();
        next.revision += 1;
        writer.replace(next).unwrap();
        let wal = journal::sidecar(&path);
        let before = fs::read(&wal).unwrap();
        let mut reader = FileStrategyStateStore::load(&path).unwrap();
        assert_eq!(reader.snapshot(), writer.snapshot());
        assert!(fs::read(&wal).unwrap() == before);
        let mut next = reader.snapshot().clone();
        next.revision += 1;
        assert!(reader.replace(next).is_err());
        assert!(fs::read(&wal).unwrap() == before);
    }

    #[test]
    fn journal_missing_during_live_write_must_not_publish_or_allow_another_write() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = writer.snapshot().clone();
        next.revision += 1;
        writer.replace(next.clone()).unwrap();
        let saved = writer.snapshot().clone();
        fs::remove_file(journal::sidecar(&path)).unwrap();
        next.revision += 1;
        assert!(
            writer.replace(next.clone()).is_err(),
            "writer published into an unlinked WAL"
        );
        assert_eq!(writer.snapshot(), &saved);
        assert!(
            writer.replace(next).is_err(),
            "ambiguous failure did not poison writer"
        );
    }

    #[test]
    fn journal_recovery_ignores_only_an_incomplete_eof_and_next_writer_repairs_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut store = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = store.snapshot().clone();
        next.revision += 1;
        store.replace(next.clone()).unwrap();
        drop(store);
        let wal = journal::sidecar(&path);
        let committed = fs::read(&wal).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        let torn = fs::read(&wal).unwrap();
        let mut restored = FileStrategyStateStore::load(&path).unwrap();
        assert_eq!(restored.snapshot(), &next);
        assert!(
            fs::read(&wal).unwrap() == torn,
            "read-only load repaired a file"
        );
        next.revision += 1;
        restored.replace(next.clone()).unwrap();
        let bytes = fs::read(&wal).unwrap();
        assert!(bytes.starts_with(&committed));
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            &next
        );
    }

    #[test]
    fn journal_crash_at_each_frame_boundary_preserves_previous_complete_revision() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut first = writer.snapshot().clone();
        first.revision += 1;
        writer.replace(first.clone()).unwrap();
        let wal = journal::sidecar(&path);
        let prefix = fs::read(&wal).unwrap();
        let mut second = first.clone();
        second.revision += 1;
        writer.replace(second.clone()).unwrap();
        let full = fs::read(&wal).unwrap();
        drop(writer);
        let frame_len = full.len() - prefix.len();
        for cut in [0, 1, 8, 32, 56, 87, 88, 89, frame_len - 1, frame_len] {
            let torn = &full[..prefix.len() + cut];
            fs::write(&wal, torn).unwrap();
            let recovered = FileStrategyStateStore::load(&path).unwrap();
            assert_eq!(
                recovered.snapshot(),
                if cut == frame_len { &second } else { &first }
            );
            assert!(fs::read(&wal).unwrap() == torn);
        }
        let mut reordered = full[..32].to_vec();
        reordered.extend_from_slice(&full[prefix.len()..]);
        reordered.extend_from_slice(&full[32..prefix.len()]);
        fs::write(&wal, reordered).unwrap();
        assert!(
            FileStrategyStateStore::load(&path).is_err(),
            "out-of-order valid frames were accepted"
        );
    }

    #[test]
    fn journal_corruption_or_missing_wal_fails_closed_without_changing_evidence() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut store = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = store.snapshot().clone();
        next.revision += 1;
        store.replace(next).unwrap();
        drop(store);
        let wal = journal::sidecar(&path);
        let committed = fs::read(&wal).unwrap();
        for offset in [8, 32, 32 + 8, 32 + 24, 32 + 88] {
            let mut corrupt = committed.clone();
            corrupt[offset] ^= 1;
            fs::write(&wal, &corrupt).unwrap();
            assert!(
                FileStrategyStateStore::load(&path).is_err(),
                "corruption at {offset} was ignored"
            );
            assert!(fs::read(&wal).unwrap() == corrupt);
        }
        fs::remove_file(&wal).unwrap();
        assert!(FileStrategyStateStore::load(&path).is_err());
    }

    #[test]
    fn journal_format_rejects_legacy_readers_and_stale_legacy_writers() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut stale = FileStrategyStateStore::load(&path).unwrap();
        let mut next = writer.snapshot().clone();
        next.revision += 1;
        writer.replace(next.clone()).unwrap();
        drop(writer);
        let bytes = fs::read(&path).unwrap();
        assert!(serde_json::from_slice::<PersistedStrategyState>(&bytes).is_err());
        assert!(stale.replace(next).is_err());
        assert!(fs::read(&path).unwrap() == bytes);
    }

    #[test]
    fn journal_review_stale_torn_tail_cannot_erase_a_same_length_committed_successor() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut first = writer.snapshot().clone();
        first.revision += 1;
        writer.replace(first.clone()).unwrap();
        let mut long = first.clone();
        long.revision += 1;
        long.updated_at_ms = 1000;
        writer.replace(long).unwrap();
        drop(writer);
        let wal = journal::sidecar(&path);
        let full = fs::read(&wal).unwrap();
        fs::write(&wal, &full[..full.len() - 1]).unwrap();
        let old_length = fs::metadata(&wal).unwrap().len();
        let mut stale = FileStrategyStateStore::load(&path).unwrap();
        let mut winner = FileStrategyStateStore::load(&path).unwrap();
        let mut next = first;
        next.revision += 1;
        next.updated_at_ms = 101;
        winner.replace(next.clone()).unwrap();
        drop(winner);
        assert_eq!(
            fs::metadata(&wal).unwrap().len(),
            old_length,
            "test must exercise the equal-length ABA case"
        );
        let committed = fs::read(&wal).unwrap();
        let mut conflicting = next.clone();
        conflicting.updated_at_ms = 102;
        assert!(
            stale.replace(conflicting).is_err(),
            "stale recovery erased another writer's commit"
        );
        assert!(fs::read(&wal).unwrap() == committed);
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            &next
        );
    }

    #[test]
    fn journal_review_supported_state_version_upgrade_is_replayable() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        let mut next = writer.snapshot().clone();
        next.revision += 1;
        next.version = 2;
        next.validate().unwrap();
        writer.replace(next.clone()).unwrap();
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            &next
        );
    }

    #[test]
    fn journal_review_interrupted_header_creation_recovers_from_untouched_legacy_json() {
        for length in [0, 1, 31, 32] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("strategy.json");
            FileStrategyStateStore::create(&path, state()).unwrap();
            let legacy = fs::read(&path).unwrap();
            let wal = journal::sidecar(&path);
            fs::write(&wal, vec![0; length]).unwrap();
            let mut recovered = FileStrategyStateStore::load(&path).unwrap();
            assert!(fs::read(&path).unwrap() == legacy);
            assert_eq!(fs::metadata(&wal).unwrap().len(), length as u64);
            let mut next = recovered.snapshot().clone();
            next.revision += 1;
            recovered.replace(next.clone()).unwrap();
            assert_eq!(
                FileStrategyStateStore::load(&path).unwrap().snapshot(),
                &next
            );
        }
    }

    #[test]
    fn periodic_background_checkpoint_and_later_records_recover_exactly() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut writer = FileStrategyStateStore::create(&path, state()).unwrap();
        for _ in 0..80 {
            let mut next = writer.snapshot().clone();
            next.revision += 1;
            next.updated_at_ms += 1;
            writer.replace(next).unwrap();
            assert_eq!(
                FileStrategyStateStore::load(&path).unwrap().snapshot(),
                writer.snapshot()
            );
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let json: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            if json["state"]["revision"].as_u64().unwrap() >= 64 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background checkpoint did not finish"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            writer.snapshot()
        );
    }

    #[test]
    #[ignore = "requires GRID_BENCH_STRATEGY_FILE; never runs against a production path"]
    fn representative_journal_persistence_benchmark() {
        use std::time::Instant;
        let input =
            std::env::var_os("GRID_BENCH_STRATEGY_FILE").expect("offline copied fixture required");
        let original = FileStrategyStateStore::load(PathBuf::from(input))
            .unwrap()
            .snapshot()
            .clone();
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let legacy_path = directory.path().join("legacy.json");
        let mut store = FileStrategyStateStore::create(&path, original.clone()).unwrap();
        let mut next = original.clone();
        next.revision += 1;
        next.updated_at_ms += 1;
        let cold = Instant::now();
        store
            .replace_validated(ValidatedStrategyState::new(next).unwrap())
            .unwrap();
        let cold_ms = cold.elapsed().as_secs_f64() * 1000.0;
        let mut journal_ms = Vec::new();
        let mut legacy_ms = Vec::new();
        let wal_bytes_before = fs::metadata(journal::sidecar(&path)).unwrap().len();
        let mut legacy_bytes = 0u64;
        for _ in 0..20 {
            let mut next = store.snapshot().clone();
            next.revision += 1;
            next.updated_at_ms += 1;
            let validated = ValidatedStrategyState::new(next.clone()).unwrap();
            let start = Instant::now();
            commit_persisted(
                &legacy_path,
                &PersistedStrategyState::Active(Box::new(next)),
            )
            .unwrap();
            legacy_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            legacy_bytes += fs::metadata(&legacy_path).unwrap().len();
            let start = Instant::now();
            store.replace_validated(validated).unwrap();
            journal_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let wal_bytes = fs::metadata(journal::sidecar(&path)).unwrap().len() - wal_bytes_before;
        assert_eq!(
            FileStrategyStateStore::load(&path).unwrap().snapshot(),
            store.snapshot()
        );
        assert!(
            wal_bytes * 10 < legacy_bytes,
            "incremental writes are not substantially smaller"
        );
        println!(
            "{}",
            serde_json::json!({"orders": original.orders.len(), "state_bytes": serde_json::to_vec(&original).unwrap().len(),
            "cold_migration_and_validation_ms": cold_ms, "samples": 20,
            "legacy_persistence_ms": legacy_ms, "journal_persistence_ms": journal_ms,
            "legacy_bytes_written": legacy_bytes, "journal_bytes_written": wal_bytes})
        );
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
    fn concurrent_first_creation_has_exactly_one_winner_and_never_overwrites() {
        let directory = tempdir().unwrap();
        let path = Arc::new(directory.path().join("strategy.json"));
        let barrier = Arc::new(Barrier::new(16));
        let handles = (0..16)
            .map(|_| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                let snapshot = state();
                thread::spawn(move || {
                    barrier.wait();
                    FileStrategyStateStore::create(path.as_ref().clone(), snapshot)
                })
            })
            .collect::<Vec<_>>();

        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(StrategyStoreError::AlreadyExists)))
                .count(),
            15
        );
        assert_eq!(
            FileStrategyStateStore::load(path.as_ref().clone())
                .unwrap()
                .snapshot(),
            &state()
        );
    }

    #[test]
    fn prepared_bootstrap_state_is_exposed_only_after_durable_creation() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("active.json");
        let active = FilePreparedStrategyStore::create(
            &active_path,
            PreparedStrategy::Active(Box::new(state())),
        )
        .unwrap();
        assert!(matches!(&active, FilePreparedStrategyStore::Active(_)));
        assert_eq!(active.path(), active_path);
        assert!(FileStrategyStateStore::load(&active_path).is_ok());
        assert!(matches!(
            FilePreparedStrategyStore::load(&active_path),
            Ok(FilePreparedStrategyStore::Active(_))
        ));

        let armed_path = directory.path().join("armed.json");
        let armed = FilePreparedStrategyStore::create(
            &armed_path,
            PreparedStrategy::Armed(Box::new(armed_state())),
        )
        .unwrap();
        assert!(matches!(&armed, FilePreparedStrategyStore::Armed(_)));
        assert_eq!(armed.path(), armed_path);
        assert!(FileArmedStrategyStateStore::load(&armed_path).is_ok());
        assert!(matches!(
            FilePreparedStrategyStore::load(&armed_path),
            Ok(FilePreparedStrategyStore::Armed(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn first_state_file_is_owner_read_write_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        FileStrategyStateStore::create(&path, state()).unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
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
    fn drifted_initial_grid_ledger_is_retained_and_rejected_on_load() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut corrupted = state();
        let quantity_step = corrupted.instrument_rules.limit_quantity.step;
        corrupted
            .orders
            .values_mut()
            .find(|order| matches!(order.purpose, StrategyOrderPurpose::InitialGrid { .. }))
            .unwrap()
            .shape
            .quantity += quantity_step;
        let bytes = serde_json::to_vec_pretty(&PersistedStrategyState::Active(Box::new(corrupted)))
            .unwrap();
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidState(
                StrategyStateError::InitialGridOrderMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn drifted_order_identity_is_retained_and_rejected_on_load() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut corrupted = state();
        let previous = corrupted
            .orders
            .values()
            .find(|order| matches!(order.purpose, StrategyOrderPurpose::InitialGrid { .. }))
            .unwrap()
            .client_order_id
            .clone();
        let replacement = ClientOrderId::parse(previous.as_str().replacen("g_", "r_", 1)).unwrap();
        let mut order = corrupted.orders.remove(&previous).unwrap();
        order.client_order_id = replacement.clone();
        corrupted.orders.insert(replacement, order);
        let bytes = serde_json::to_vec_pretty(&PersistedStrategyState::Active(Box::new(corrupted)))
            .unwrap();
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidState(
                StrategyStateError::OrderSequenceMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn drifted_aggregate_accounting_is_retained_and_rejected_on_load() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut corrupted = state();
        corrupted.total_volume = Decimal::ONE;
        let bytes = serde_json::to_vec_pretty(&PersistedStrategyState::Active(Box::new(corrupted)))
            .unwrap();
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidState(
                StrategyStateError::AggregateAccountingMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn incomplete_inventory_event_ledger_is_retained_and_rejected_on_load() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut corrupted = state();
        corrupted.next_inventory_event_sequence = 2;
        let bytes = serde_json::to_vec_pretty(&PersistedStrategyState::Active(Box::new(corrupted)))
            .unwrap();
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidState(
                StrategyStateError::InventoryEventLedgerMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn fabricated_replacement_obligation_is_retained_and_rejected_on_load() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strategy.json");
        let mut corrupted = state();
        let source = corrupted
            .orders
            .values()
            .find_map(|order| match order.purpose {
                StrategyOrderPurpose::InitialGrid { level_index, .. } => Some((
                    order.client_order_id.clone(),
                    level_index,
                    order.shape.clone(),
                )),
                _ => None,
            })
            .unwrap();
        corrupted.replacement_obligations.insert(
            1,
            ReplacementObligation {
                id: 1,
                kind: ReplacementObligationKind::Counter,
                source_client_order_id: source.0,
                level_index: source.1,
                shape: source.2,
                retired_quantity: Decimal::ZERO,
                created_at_ms: corrupted.updated_at_ms,
                assigned_client_order_id: None,
            },
        );
        corrupted.next_obligation_sequence = 2;
        let bytes = serde_json::to_vec_pretty(&PersistedStrategyState::Active(Box::new(corrupted)))
            .unwrap();
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileStrategyStateStore::load(&path),
            Err(StrategyStoreError::InvalidState(
                StrategyStateError::ReplacementObligationLedgerMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
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
        let document = serde_json::from_str::<serde_json::Value>(&json).unwrap();

        assert_eq!(restored.snapshot(), &original);
        assert_eq!(document["runtime_state"], "armed");
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
        let document = serde_json::from_str::<serde_json::Value>(&json).unwrap();

        assert_eq!(active.snapshot(), restored.snapshot());
        assert_eq!(active.snapshot().revision, 1);
        assert_eq!(active.snapshot().triggered_at_ms, Some(102));
        assert_eq!(document["runtime_state"], "active");
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
    fn prepared_activation_must_be_the_exact_armed_successor() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("armed.json");
        let armed = armed_state();
        let mut store = FileArmedStrategyStateStore::create(&path, armed.clone()).unwrap();
        let bytes_before = fs::read(&path).unwrap();
        let mut active = armed
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
        active.trigger_observed_price = Some(Decimal::new(404, 3));

        assert!(matches!(
            store.try_activate_prepared(&active),
            Err(StrategyStoreError::ArmedStrategy(
                crate::engine::ArmedStrategyError::ActiveSuccessorMismatch
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
        assert_eq!(store.snapshot(), &armed);

        let valid = armed
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
        assert!(store.try_activate_prepared(&valid).is_ok());
        assert!(FileStrategyStateStore::load(path).is_ok());
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
