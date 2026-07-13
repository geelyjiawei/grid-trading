use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{ClientOrderId, IntentState, OrderIntent};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerSnapshot {
    pub version: u8,
    pub revision: u64,
    pub intents: BTreeMap<ClientOrderId, OrderIntent>,
}

impl Default for LedgerSnapshot {
    fn default() -> Self {
        Self {
            version: 1,
            revision: 0,
            intents: BTreeMap::new(),
        }
    }
}

pub trait IntentStore {
    fn snapshot(&self) -> &LedgerSnapshot;
    fn insert_prepared(&mut self, intent: OrderIntent) -> Result<(), LedgerError>;
    fn transition(
        &mut self,
        client_order_id: &ClientOrderId,
        next_state: IntentState,
        now_ms: u64,
    ) -> Result<(), LedgerError>;
}

#[derive(Debug)]
pub struct FileOrderIntentStore {
    path: PathBuf,
    snapshot: LedgerSnapshot,
}

impl FileOrderIntentStore {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, LedgerError> {
        let path = path.into();
        let snapshot = if path.exists() {
            let bytes = fs::read(&path).map_err(LedgerError::Read)?;
            serde_json::from_slice(&bytes).map_err(LedgerError::InvalidJson)?
        } else {
            LedgerSnapshot::default()
        };
        validate_snapshot(&snapshot)?;
        Ok(Self { path, snapshot })
    }

    fn commit_snapshot(&self, snapshot: &LedgerSnapshot) -> Result<(), LedgerError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(LedgerError::CreateDirectory)?;
        }
        let mut file = AtomicWriteFile::options()
            .open(&self.path)
            .map_err(LedgerError::OpenAtomic)?;
        serde_json::to_writer_pretty(&mut file, snapshot).map_err(LedgerError::Serialize)?;
        file.write_all(b"\n").map_err(LedgerError::Write)?;
        file.commit().map_err(LedgerError::Commit)?;
        sync_parent(&self.path)?;
        Ok(())
    }

    fn replace(&mut self, mut next: LedgerSnapshot) -> Result<(), LedgerError> {
        next.revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or(LedgerError::RevisionOverflow)?;
        validate_snapshot(&next)?;
        self.commit_snapshot(&next)?;
        self.snapshot = next;
        Ok(())
    }
}

impl IntentStore for FileOrderIntentStore {
    fn snapshot(&self) -> &LedgerSnapshot {
        &self.snapshot
    }

    fn insert_prepared(&mut self, intent: OrderIntent) -> Result<(), LedgerError> {
        if intent.state != IntentState::Prepared {
            return Err(LedgerError::NewIntentNotPrepared);
        }
        let mut next = self.snapshot.clone();
        if next.intents.contains_key(&intent.client_order_id) {
            return Err(LedgerError::DuplicateClientOrderId(
                intent.client_order_id.as_str().to_owned(),
            ));
        }
        next.intents.insert(intent.client_order_id.clone(), intent);
        self.replace(next)
    }

    fn transition(
        &mut self,
        client_order_id: &ClientOrderId,
        next_state: IntentState,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        let mut next = self.snapshot.clone();
        let intent = next
            .intents
            .get_mut(client_order_id)
            .ok_or_else(|| LedgerError::MissingIntent(client_order_id.as_str().to_owned()))?;
        validate_transition(&intent.state, &next_state)?;
        if now_ms < intent.updated_at_ms {
            return Err(LedgerError::TimestampRegression);
        }
        intent.state = next_state;
        intent.updated_at_ms = now_ms;
        self.replace(next)
    }
}

#[derive(Debug, Default)]
pub struct MemoryOrderIntentStore {
    snapshot: LedgerSnapshot,
    write_attempts: u64,
    fail_write_attempt: Option<u64>,
}

impl MemoryOrderIntentStore {
    pub fn fail_next_write(&mut self) {
        self.fail_write_attempt = Some(self.write_attempts + 1);
    }

    pub fn fail_on_write(&mut self, attempt: u64) {
        self.fail_write_attempt = Some(attempt);
    }

    fn before_write(&mut self) -> Result<(), LedgerError> {
        self.write_attempts = self
            .write_attempts
            .checked_add(1)
            .ok_or(LedgerError::RevisionOverflow)?;
        if self.fail_write_attempt == Some(self.write_attempts) {
            self.fail_write_attempt = None;
            return Err(LedgerError::InjectedWriteFailure);
        }
        Ok(())
    }
}

impl IntentStore for MemoryOrderIntentStore {
    fn snapshot(&self) -> &LedgerSnapshot {
        &self.snapshot
    }

    fn insert_prepared(&mut self, intent: OrderIntent) -> Result<(), LedgerError> {
        self.before_write()?;
        if intent.state != IntentState::Prepared {
            return Err(LedgerError::NewIntentNotPrepared);
        }
        if self.snapshot.intents.contains_key(&intent.client_order_id) {
            return Err(LedgerError::DuplicateClientOrderId(
                intent.client_order_id.as_str().to_owned(),
            ));
        }
        self.snapshot.revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or(LedgerError::RevisionOverflow)?;
        self.snapshot
            .intents
            .insert(intent.client_order_id.clone(), intent);
        Ok(())
    }

    fn transition(
        &mut self,
        client_order_id: &ClientOrderId,
        next_state: IntentState,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        self.before_write()?;
        let next_revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or(LedgerError::RevisionOverflow)?;
        let intent = self
            .snapshot
            .intents
            .get_mut(client_order_id)
            .ok_or_else(|| LedgerError::MissingIntent(client_order_id.as_str().to_owned()))?;
        validate_transition(&intent.state, &next_state)?;
        if now_ms < intent.updated_at_ms {
            return Err(LedgerError::TimestampRegression);
        }
        intent.state = next_state;
        intent.updated_at_ms = now_ms;
        self.snapshot.revision = next_revision;
        Ok(())
    }
}

fn validate_snapshot(snapshot: &LedgerSnapshot) -> Result<(), LedgerError> {
    if snapshot.version != 1 {
        return Err(LedgerError::UnsupportedVersion(snapshot.version));
    }
    for (key, intent) in &snapshot.intents {
        if key != &intent.client_order_id {
            return Err(LedgerError::IdentityMismatch);
        }
        intent.validate().map_err(LedgerError::InvalidIntent)?;
    }
    Ok(())
}

fn validate_transition(current: &IntentState, next: &IntentState) -> Result<(), LedgerError> {
    let allowed = matches!(
        (current, next),
        (
            IntentState::Prepared,
            IntentState::SubmitUnknown { .. }
                | IntentState::Accepted { .. }
                | IntentState::Rejected { .. }
                | IntentState::OwnershipConflict { .. }
        ) | (
            IntentState::SubmitUnknown { .. },
            IntentState::Accepted { .. }
                | IntentState::Rejected { .. }
                | IntentState::OwnershipConflict { .. }
        ) | (
            IntentState::Accepted { .. },
            IntentState::Terminal { .. } | IntentState::OwnershipConflict { .. }
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(LedgerError::InvalidTransition)
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), LedgerError> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(LedgerError::SyncDirectory)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<(), LedgerError> {
    Ok(())
}

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("failed to read ledger: {0}")]
    Read(std::io::Error),
    #[error("ledger contains invalid JSON: {0}")]
    InvalidJson(serde_json::Error),
    #[error("unsupported ledger version {0}")]
    UnsupportedVersion(u8),
    #[error("ledger key does not match the embedded client order ID")]
    IdentityMismatch,
    #[error("ledger contains an invalid order intent: {0}")]
    InvalidIntent(crate::domain::OrderIntentError),
    #[error("failed to create ledger directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to open atomic ledger writer: {0}")]
    OpenAtomic(std::io::Error),
    #[error("failed to serialize ledger: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to write ledger: {0}")]
    Write(std::io::Error),
    #[error("failed to commit ledger: {0}")]
    Commit(std::io::Error),
    #[error("failed to sync ledger directory: {0}")]
    SyncDirectory(std::io::Error),
    #[error("ledger revision overflow")]
    RevisionOverflow,
    #[error("new order intent must be prepared")]
    NewIntentNotPrepared,
    #[error("duplicate client order ID {0}")]
    DuplicateClientOrderId(String),
    #[error("missing order intent {0}")]
    MissingIntent(String),
    #[error("invalid order intent state transition")]
    InvalidTransition,
    #[error("order intent timestamp moved backwards")]
    TimestampRegression,
    #[error("injected ledger write failure")]
    InjectedWriteFailure,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{Exchange, OrderKind, OrderShape, OrderSide, TimeInForce};

    fn intent(client_order_id: &str) -> OrderIntent {
        OrderIntent::prepare(
            ClientOrderId::parse(client_order_id).unwrap(),
            Exchange::Binance,
            OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(1011, 0)),
                quantity: Decimal::new(2, 1),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            100,
        )
        .unwrap()
    }

    #[test]
    fn atomic_file_store_round_trips_decimal_intent() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("order-ledger.json");
        let mut store = FileOrderIntentStore::load(&path).unwrap();
        let original = intent("g_1_S_roundtrip");
        store.insert_prepared(original.clone()).unwrap();

        let restored = FileOrderIntentStore::load(&path).unwrap();
        assert_eq!(
            restored.snapshot().intents.get(&original.client_order_id),
            Some(&original)
        );
        assert_eq!(restored.snapshot().revision, 1);
    }

    #[test]
    fn duplicate_identity_does_not_change_durable_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("order-ledger.json");
        let mut store = FileOrderIntentStore::load(&path).unwrap();
        let original = intent("g_1_S_duplicate");
        store.insert_prepared(original.clone()).unwrap();
        let before = fs::read(&path).unwrap();

        assert!(matches!(
            store.insert_prepared(original),
            Err(LedgerError::DuplicateClientOrderId(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn corrupt_json_is_never_reset_to_an_empty_ledger() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("order-ledger.json");
        fs::write(&path, b"{not-json").unwrap();

        assert!(matches!(
            FileOrderIntentStore::load(&path),
            Err(LedgerError::InvalidJson(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), b"{not-json");
    }
}
