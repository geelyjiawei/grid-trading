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

fn create_persisted(
    path: &Path,
    snapshot: &PersistedStrategyState,
) -> Result<(), StrategyStoreError> {
    let mut bytes = serde_json::to_vec_pretty(snapshot).map_err(StrategyStoreError::Serialize)?;
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
        match read_persisted(&path)? {
            PersistedStrategyState::Armed(snapshot) => {
                let snapshot = *snapshot;
                snapshot.validate()?;
                Ok(Self::Armed(Box::new(FileArmedStrategyStateStore {
                    path,
                    snapshot,
                })))
            }
            PersistedStrategyState::Active(snapshot) => {
                let snapshot = *snapshot;
                snapshot
                    .validate()
                    .map_err(StrategyStoreError::InvalidState)?;
                Ok(Self::Active(Box::new(FileStrategyStateStore {
                    path,
                    snapshot,
                })))
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
    snapshot: StrategyState,
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
        let store = Self { path, snapshot };
        create_persisted(
            &store.path,
            &PersistedStrategyState::Active(Box::new(store.snapshot.clone())),
        )?;
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
        Ok(FileStrategyStateStore {
            path: self.path.clone(),
            snapshot: active.clone(),
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

    fn snapshot_is_known_valid(&self) -> bool {
        true
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

    fn replace_validated(
        &mut self,
        next: ValidatedStrategyState,
    ) -> Result<(), StrategyStoreError> {
        let next = next.into_inner();
        if self.snapshot.revision.checked_add(1) != Some(next.revision) {
            return Err(StrategyStoreError::RevisionMismatch);
        }
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
        assert!(store.snapshot_is_known_valid());
        assert!(restored.snapshot_is_known_valid());
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
