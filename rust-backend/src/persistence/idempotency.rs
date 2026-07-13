use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const IDEMPOTENCY_RECORD_VERSION: u8 = 1;
const MIN_KEY_BYTES: usize = 16;
const MAX_KEY_BYTES: usize = 128;
const MAX_TARGET_BYTES: usize = 2_048;
const MAX_RECORD_BYTES: usize = 128 * 1_024;
const MAX_RESPONSE_BYTES: usize = 64 * 1_024;
const RECORD_FILE_NAME: &str = "record.json";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn parse(value: &str) -> Result<Self, IdempotencyError> {
        if value.len() < MIN_KEY_BYTES {
            return Err(IdempotencyError::KeyTooShort);
        }
        if value.len() > MAX_KEY_BYTES {
            return Err(IdempotencyError::KeyTooLong);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        {
            return Err(IdempotencyError::InvalidKeyCharacter);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn storage_name(&self) -> String {
        hex::encode(Sha256::digest(self.0.as_bytes()))
    }

    fn validate(&self) -> Result<(), IdempotencyError> {
        Self::parse(&self.0).map(|_| ())
    }
}

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("IdempotencyKey")
            .field(&self.storage_name())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFingerprint {
    method: String,
    target: String,
    content_type: String,
    body_sha256: String,
}

impl RequestFingerprint {
    pub fn new(
        method: &str,
        target: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<Self, IdempotencyError> {
        let fingerprint = Self {
            method: method.to_owned(),
            target: target.to_owned(),
            content_type: content_type.to_ascii_lowercase(),
            body_sha256: hex::encode(Sha256::digest(body)),
        };
        fingerprint.validate()?;
        Ok(fingerprint)
    }

    fn validate(&self) -> Result<(), IdempotencyError> {
        if self.method.is_empty()
            || self.method.len() > 16
            || !self
                .method
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte == b'-')
        {
            return Err(IdempotencyError::InvalidFingerprint(
                "method is not canonical",
            ));
        }
        if !self.target.starts_with('/')
            || self.target.len() > MAX_TARGET_BYTES
            || !self.target.is_ascii()
        {
            return Err(IdempotencyError::InvalidFingerprint(
                "request target is not canonical",
            ));
        }
        if self.content_type != "application/json" {
            return Err(IdempotencyError::InvalidFingerprint(
                "content type is not canonical JSON",
            ));
        }
        if self.body_sha256.len() != 64
            || !self
                .body_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(IdempotencyError::InvalidFingerprint(
                "body digest is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredCommandResponse {
    status: u16,
    body: Value,
}

impl StoredCommandResponse {
    pub fn new(status: u16, body: Value) -> Result<Self, IdempotencyError> {
        let response = Self { status, body };
        response.validate()?;
        Ok(response)
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn body(&self) -> &Value {
        &self.body
    }

    fn validate(&self) -> Result<(), IdempotencyError> {
        if !(200..=599).contains(&self.status) {
            return Err(IdempotencyError::InvalidResponseStatus(self.status));
        }
        let encoded = serde_json::to_vec(&self.body).map_err(IdempotencyError::Serialize)?;
        if encoded.len() > MAX_RESPONSE_BYTES {
            return Err(IdempotencyError::ResponseTooLarge);
        }
        Ok(())
    }
}

impl fmt::Debug for StoredCommandResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body_bytes = serde_json::to_vec(&self.body).map_or(0, |body| body.len());
        formatter
            .debug_struct("StoredCommandResponse")
            .field("status", &self.status)
            .field("body", &"[REDACTED]")
            .field("body_bytes", &body_bytes)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BeginIdempotency {
    Started,
    InProgress,
    Completed(StoredCommandResponse),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompleteIdempotency {
    Completed,
    AlreadyCompleted,
}

pub trait IdempotencyStore: Send + Sync {
    fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: &RequestFingerprint,
        started_at_ms: u64,
    ) -> Result<BeginIdempotency, IdempotencyError>;

    fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: &RequestFingerprint,
        response: &StoredCommandResponse,
        completed_at_ms: u64,
    ) -> Result<CompleteIdempotency, IdempotencyError>;
}

#[derive(Clone)]
pub struct FileIdempotencyStore {
    root: PathBuf,
    operation_lock: Arc<Mutex<()>>,
}

impl FileIdempotencyStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            operation_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn ensure_root(&self) -> Result<(), IdempotencyError> {
        match fs::symlink_metadata(&self.root) {
            Ok(metadata) => validate_directory(&metadata, true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&self.root).map_err(IdempotencyError::CreateRoot)?;
                set_directory_permissions(&self.root)?;
                let metadata =
                    fs::symlink_metadata(&self.root).map_err(IdempotencyError::RootMetadata)?;
                validate_directory(&metadata, true)
            }
            Err(error) => Err(IdempotencyError::RootMetadata(error)),
        }
    }

    fn reservation_path(&self, key: &IdempotencyKey) -> PathBuf {
        self.root.join(key.storage_name())
    }

    fn load_record(&self, key: &IdempotencyKey) -> Result<IdempotencyRecord, IdempotencyError> {
        let reservation = self.reservation_path(key);
        let reservation_metadata =
            fs::symlink_metadata(&reservation).map_err(IdempotencyError::ReservationMetadata)?;
        validate_directory(&reservation_metadata, false)?;

        let record_path = reservation.join(RECORD_FILE_NAME);
        let metadata = match fs::symlink_metadata(&record_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(IdempotencyError::IncompleteReservation);
            }
            Err(error) => return Err(IdempotencyError::RecordMetadata(error)),
        };
        validate_record_file(&metadata)?;
        if metadata.len() > MAX_RECORD_BYTES as u64 {
            return Err(IdempotencyError::RecordTooLarge);
        }

        let bytes = fs::read(&record_path).map_err(IdempotencyError::ReadRecord)?;
        let record: IdempotencyRecord =
            serde_json::from_slice(&bytes).map_err(IdempotencyError::InvalidJson)?;
        record.validate()?;
        if &record.key != key {
            return Err(IdempotencyError::KeyHashCollision);
        }
        Ok(record)
    }

    fn write_new_record(
        &self,
        reservation: &Path,
        record: &IdempotencyRecord,
    ) -> Result<(), IdempotencyError> {
        let mut bytes = serde_json::to_vec_pretty(record).map_err(IdempotencyError::Serialize)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(IdempotencyError::RecordTooLarge);
        }

        let path = reservation.join(RECORD_FILE_NAME);
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .map_err(IdempotencyError::CreateRecord)?;
        file.write_all(&bytes)
            .map_err(IdempotencyError::WriteRecord)?;
        file.sync_all().map_err(IdempotencyError::SyncRecord)?;
        sync_directory(reservation)?;
        Ok(())
    }

    fn replace_record(
        &self,
        key: &IdempotencyKey,
        record: &IdempotencyRecord,
    ) -> Result<(), IdempotencyError> {
        let reservation = self.reservation_path(key);
        let path = reservation.join(RECORD_FILE_NAME);
        let metadata = fs::symlink_metadata(&path).map_err(IdempotencyError::RecordMetadata)?;
        validate_record_file(&metadata)?;

        let mut file = AtomicWriteFile::options()
            .open(&path)
            .map_err(IdempotencyError::OpenAtomic)?;
        serde_json::to_writer_pretty(&mut file, record).map_err(IdempotencyError::Serialize)?;
        file.write_all(b"\n")
            .map_err(IdempotencyError::WriteRecord)?;
        file.commit().map_err(IdempotencyError::CommitRecord)?;
        sync_directory(&reservation)?;
        Ok(())
    }
}

impl IdempotencyStore for FileIdempotencyStore {
    fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: &RequestFingerprint,
        started_at_ms: u64,
    ) -> Result<BeginIdempotency, IdempotencyError> {
        key.validate()?;
        fingerprint.validate()?;
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| IdempotencyError::LockPoisoned)?;
        self.ensure_root()?;

        let reservation = self.reservation_path(key);
        match fs::create_dir(&reservation) {
            Ok(()) => {
                set_directory_permissions(&reservation)?;
                sync_directory(&self.root)?;
                let record =
                    IdempotencyRecord::in_progress(key.clone(), fingerprint.clone(), started_at_ms);
                self.write_new_record(&reservation, &record)?;
                Ok(BeginIdempotency::Started)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let record = self.load_record(key)?;
                if record.fingerprint != *fingerprint {
                    return Err(IdempotencyError::FingerprintConflict);
                }
                match record.state {
                    IdempotencyState::InProgress { .. } => Ok(BeginIdempotency::InProgress),
                    IdempotencyState::Completed { response, .. } => {
                        Ok(BeginIdempotency::Completed(response))
                    }
                }
            }
            Err(error) => Err(IdempotencyError::CreateReservation(error)),
        }
    }

    fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: &RequestFingerprint,
        response: &StoredCommandResponse,
        completed_at_ms: u64,
    ) -> Result<CompleteIdempotency, IdempotencyError> {
        key.validate()?;
        fingerprint.validate()?;
        response.validate()?;
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| IdempotencyError::LockPoisoned)?;
        self.ensure_root()?;

        let current = self.load_record(key)?;
        if current.fingerprint != *fingerprint {
            return Err(IdempotencyError::FingerprintConflict);
        }
        match current.state {
            IdempotencyState::InProgress { started_at_ms } => {
                if completed_at_ms < started_at_ms {
                    return Err(IdempotencyError::TimestampRegression);
                }
                let completed = IdempotencyRecord::completed(
                    key.clone(),
                    fingerprint.clone(),
                    started_at_ms,
                    completed_at_ms,
                    response.clone(),
                );
                self.replace_record(key, &completed)?;
                Ok(CompleteIdempotency::Completed)
            }
            IdempotencyState::Completed {
                response: stored, ..
            } if stored == *response => Ok(CompleteIdempotency::AlreadyCompleted),
            IdempotencyState::Completed { .. } => Err(IdempotencyError::CompletionConflict),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdempotencyRecord {
    version: u8,
    key: IdempotencyKey,
    fingerprint: RequestFingerprint,
    state: IdempotencyState,
}

impl IdempotencyRecord {
    fn in_progress(
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
        started_at_ms: u64,
    ) -> Self {
        Self {
            version: IDEMPOTENCY_RECORD_VERSION,
            key,
            fingerprint,
            state: IdempotencyState::InProgress { started_at_ms },
        }
    }

    fn completed(
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
        started_at_ms: u64,
        completed_at_ms: u64,
        response: StoredCommandResponse,
    ) -> Self {
        Self {
            version: IDEMPOTENCY_RECORD_VERSION,
            key,
            fingerprint,
            state: IdempotencyState::Completed {
                started_at_ms,
                completed_at_ms,
                response,
            },
        }
    }

    fn validate(&self) -> Result<(), IdempotencyError> {
        if self.version != IDEMPOTENCY_RECORD_VERSION {
            return Err(IdempotencyError::UnsupportedRecordVersion(self.version));
        }
        self.key.validate()?;
        self.fingerprint.validate()?;
        match &self.state {
            IdempotencyState::InProgress { .. } => Ok(()),
            IdempotencyState::Completed {
                started_at_ms,
                completed_at_ms,
                response,
            } => {
                if completed_at_ms < started_at_ms {
                    return Err(IdempotencyError::TimestampRegression);
                }
                response.validate()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum IdempotencyState {
    InProgress {
        started_at_ms: u64,
    },
    Completed {
        started_at_ms: u64,
        completed_at_ms: u64,
        response: StoredCommandResponse,
    },
}

fn validate_directory(metadata: &fs::Metadata, root: bool) -> Result<(), IdempotencyError> {
    if metadata.file_type().is_symlink() {
        return if root {
            Err(IdempotencyError::RootSymbolicLink)
        } else {
            Err(IdempotencyError::ReservationSymbolicLink)
        };
    }
    if !metadata.is_dir() {
        return if root {
            Err(IdempotencyError::RootNotDirectory)
        } else {
            Err(IdempotencyError::ReservationNotDirectory)
        };
    }
    Ok(())
}

fn validate_record_file(metadata: &fs::Metadata) -> Result<(), IdempotencyError> {
    if metadata.file_type().is_symlink() {
        return Err(IdempotencyError::RecordSymbolicLink);
    }
    if !metadata.is_file() {
        return Err(IdempotencyError::RecordNotFile);
    }
    Ok(())
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<(), IdempotencyError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(IdempotencyError::SetPermissions)
}

#[cfg(not(unix))]
fn set_directory_permissions(_: &Path) -> Result<(), IdempotencyError> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), IdempotencyError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(IdempotencyError::SyncDirectory)
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<(), IdempotencyError> {
    Ok(())
}

#[derive(Debug, Error)]
pub enum IdempotencyError {
    #[error("idempotency key must contain at least 16 bytes")]
    KeyTooShort,
    #[error("idempotency key must contain at most 128 bytes")]
    KeyTooLong,
    #[error("idempotency key contains an unsupported character")]
    InvalidKeyCharacter,
    #[error("invalid request fingerprint: {0}")]
    InvalidFingerprint(&'static str),
    #[error("stored response status {0} is invalid")]
    InvalidResponseStatus(u16),
    #[error("stored response exceeds the 64 KiB limit")]
    ResponseTooLarge,
    #[error("idempotency operation lock is poisoned")]
    LockPoisoned,
    #[error("failed to inspect idempotency root: {0}")]
    RootMetadata(std::io::Error),
    #[error("idempotency root must not be a symbolic link")]
    RootSymbolicLink,
    #[error("idempotency root is not a directory")]
    RootNotDirectory,
    #[error("failed to create idempotency root: {0}")]
    CreateRoot(std::io::Error),
    #[error("failed to set private idempotency directory permissions: {0}")]
    SetPermissions(std::io::Error),
    #[error("failed to create idempotency reservation: {0}")]
    CreateReservation(std::io::Error),
    #[error("failed to inspect idempotency reservation: {0}")]
    ReservationMetadata(std::io::Error),
    #[error("idempotency reservation must not be a symbolic link")]
    ReservationSymbolicLink,
    #[error("idempotency reservation is not a directory")]
    ReservationNotDirectory,
    #[error("idempotency reservation is incomplete and its outcome is unknown")]
    IncompleteReservation,
    #[error("failed to inspect idempotency record: {0}")]
    RecordMetadata(std::io::Error),
    #[error("idempotency record must not be a symbolic link")]
    RecordSymbolicLink,
    #[error("idempotency record is not a regular file")]
    RecordNotFile,
    #[error("idempotency record exceeds the 128 KiB limit")]
    RecordTooLarge,
    #[error("failed to read idempotency record: {0}")]
    ReadRecord(std::io::Error),
    #[error("idempotency record is not valid JSON: {0}")]
    InvalidJson(serde_json::Error),
    #[error("idempotency record version {0} is unsupported")]
    UnsupportedRecordVersion(u8),
    #[error("idempotency key digest collision detected")]
    KeyHashCollision,
    #[error("idempotency key was already used for a different request")]
    FingerprintConflict,
    #[error("idempotency request was already completed with a different response")]
    CompletionConflict,
    #[error("idempotency completion timestamp precedes its reservation")]
    TimestampRegression,
    #[error("failed to serialize idempotency record: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to create idempotency record: {0}")]
    CreateRecord(std::io::Error),
    #[error("failed to write idempotency record: {0}")]
    WriteRecord(std::io::Error),
    #[error("failed to sync idempotency record: {0}")]
    SyncRecord(std::io::Error),
    #[error("failed to open atomic idempotency writer: {0}")]
    OpenAtomic(std::io::Error),
    #[error("failed to commit idempotency record: {0}")]
    CommitRecord(std::io::Error),
    #[error("failed to sync idempotency directory: {0}")]
    SyncDirectory(std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn key() -> IdempotencyKey {
        IdempotencyKey::parse("01J2X0W2F8E4Q8MNNNNNNNNNNN").unwrap()
    }

    fn fingerprint(body: &[u8]) -> RequestFingerprint {
        RequestFingerprint::new("POST", "/api/v1/grid/start", "application/json", body).unwrap()
    }

    fn response() -> StoredCommandResponse {
        StoredCommandResponse::new(201, json!({"run_id": "run-safe-1"})).unwrap()
    }

    #[test]
    fn key_validation_rejects_short_long_and_path_characters() {
        assert!(matches!(
            IdempotencyKey::parse("short"),
            Err(IdempotencyError::KeyTooShort)
        ));
        assert!(matches!(
            IdempotencyKey::parse(&"a".repeat(129)),
            Err(IdempotencyError::KeyTooLong)
        ));
        assert!(matches!(
            IdempotencyKey::parse("123456789012345/6"),
            Err(IdempotencyError::InvalidKeyCharacter)
        ));
    }

    #[test]
    fn durable_begin_complete_and_replay_are_exact() {
        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        let fingerprint = fingerprint(br#"{"symbol":"MUUSDT"}"#);
        let response = response();

        assert_eq!(
            store.begin(&key, &fingerprint, 100).unwrap(),
            BeginIdempotency::Started
        );
        assert_eq!(
            store.begin(&key, &fingerprint, 101).unwrap(),
            BeginIdempotency::InProgress
        );
        assert_eq!(
            store.complete(&key, &fingerprint, &response, 102).unwrap(),
            CompleteIdempotency::Completed
        );
        assert_eq!(
            store.begin(&key, &fingerprint, 103).unwrap(),
            BeginIdempotency::Completed(response.clone())
        );
        assert_eq!(
            store.complete(&key, &fingerprint, &response, 104).unwrap(),
            CompleteIdempotency::AlreadyCompleted
        );
    }

    #[test]
    fn same_key_with_different_body_is_a_conflict() {
        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        store
            .begin(&key, &fingerprint(br#"{"qty":"1"}"#), 100)
            .unwrap();

        assert!(matches!(
            store.begin(&key, &fingerprint(br#"{"qty":"2"}"#), 101),
            Err(IdempotencyError::FingerprintConflict)
        ));
    }

    #[test]
    fn request_payload_is_not_persisted_in_the_idempotency_record() {
        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        let secret_marker = "THIS_REQUEST_BODY_MUST_NOT_BE_STORED";
        let body = format!(r#"{{"note":"{secret_marker}"}}"#);
        store
            .begin(&key, &fingerprint(body.as_bytes()), 100)
            .unwrap();

        let bytes = fs::read(store.reservation_path(&key).join(RECORD_FILE_NAME)).unwrap();
        let persisted = String::from_utf8(bytes).unwrap();
        assert!(!persisted.contains(secret_marker));
        assert!(!persisted.contains(&body));
    }

    #[test]
    fn one_concurrent_request_wins_and_all_others_observe_in_progress() {
        let directory = tempdir().unwrap();
        let store = Arc::new(FileIdempotencyStore::new(
            directory.path().join("idempotency"),
        ));
        let barrier = Arc::new(Barrier::new(17));
        let key = key();
        let fingerprint = fingerprint(br#"{"symbol":"ANSEMUSDT"}"#);
        let mut handles = Vec::new();

        for index in 0..16 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            let fingerprint = fingerprint.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                store.begin(&key, &fingerprint, 100 + index)
            }));
        }
        barrier.wait();

        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == BeginIdempotency::Started)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == BeginIdempotency::InProgress)
                .count(),
            15
        );
    }

    #[test]
    fn incomplete_or_corrupt_reservation_fails_closed() {
        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        store.ensure_root().unwrap();
        fs::create_dir(store.reservation_path(&key)).unwrap();

        assert!(matches!(
            store.begin(&key, &fingerprint(b"{}"), 100),
            Err(IdempotencyError::IncompleteReservation)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_root_reservation_and_record_are_rejected() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let linked_root = directory.path().join("linked-root");
        symlink(outside.path(), &linked_root).unwrap();
        let linked_root_store = FileIdempotencyStore::new(&linked_root);
        assert!(matches!(
            linked_root_store.begin(&key(), &fingerprint(b"{}"), 100),
            Err(IdempotencyError::RootSymbolicLink)
        ));

        let reservation_store =
            FileIdempotencyStore::new(directory.path().join("reservation-root"));
        reservation_store.ensure_root().unwrap();
        symlink(outside.path(), reservation_store.reservation_path(&key())).unwrap();
        assert!(matches!(
            reservation_store.begin(&key(), &fingerprint(b"{}"), 100),
            Err(IdempotencyError::ReservationSymbolicLink)
        ));

        let record_store = FileIdempotencyStore::new(directory.path().join("record-root"));
        record_store.ensure_root().unwrap();
        let reservation = record_store.reservation_path(&key());
        fs::create_dir(&reservation).unwrap();
        let outside_record = outside.path().join("record.json");
        fs::write(&outside_record, b"{}").unwrap();
        symlink(&outside_record, reservation.join(RECORD_FILE_NAME)).unwrap();
        assert!(matches!(
            record_store.begin(&key(), &fingerprint(b"{}"), 100),
            Err(IdempotencyError::RecordSymbolicLink)
        ));
    }

    #[test]
    fn response_debug_is_redacted_and_size_is_bounded() {
        let secret = "response-should-not-appear-in-debug";
        let response = StoredCommandResponse::new(200, json!({"secret": secret})).unwrap();
        let debug = format!("{response:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("[REDACTED]"));

        assert!(matches!(
            StoredCommandResponse::new(200, json!({"value": "x".repeat(MAX_RESPONSE_BYTES)})),
            Err(IdempotencyError::ResponseTooLarge)
        ));
    }

    #[test]
    fn timestamp_regression_and_changed_completion_are_rejected() {
        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        let fingerprint = fingerprint(b"{}");
        store.begin(&key, &fingerprint, 200).unwrap();
        assert!(matches!(
            store.complete(&key, &fingerprint, &response(), 199),
            Err(IdempotencyError::TimestampRegression)
        ));
        store
            .complete(&key, &fingerprint, &response(), 201)
            .unwrap();
        let changed = StoredCommandResponse::new(200, json!({"ok": true})).unwrap();
        assert!(matches!(
            store.complete(&key, &fingerprint, &changed, 202),
            Err(IdempotencyError::CompletionConflict)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn runtime_directories_and_record_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let store = FileIdempotencyStore::new(directory.path().join("idempotency"));
        let key = key();
        let fingerprint = fingerprint(b"{}");
        store.begin(&key, &fingerprint, 100).unwrap();
        store
            .complete(&key, &fingerprint, &response(), 101)
            .unwrap();
        let reservation = store.reservation_path(&key);
        let record = reservation.join(RECORD_FILE_NAME);

        assert_eq!(
            fs::metadata(store.root()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(reservation).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(record).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
