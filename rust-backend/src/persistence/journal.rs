use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use super::StrategyRuntimeLease;

const MAGIC: &[u8; 8] = b"GRIDWAL1";
const HEADER_SIZE: u64 = 32;
const FRAME_HEADER_SIZE: usize = 88;
const MAX_DELTA_BYTES: u64 = 64 * 1024 * 1024;
const FORMAT: &str = "grid-ledger-checkpoint-v1";
const CHECKPOINT_COMMITS: u64 = 64;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) trait JournalState:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Delta: Serialize + DeserializeOwned;
    const KIND: &'static str;
    fn revision(&self) -> u64;
    fn validate_successor(&self, next: &Self) -> io::Result<()>;
    fn delta(&self, next: &Self) -> Self::Delta;
    fn apply(&mut self, delta: Self::Delta) -> io::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    id: String,
    offset: u64,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint<S> {
    format: String,
    kind: String,
    journal: Cursor,
    state: S,
}

#[derive(Deserialize)]
struct FormatProbe {
    format: Option<String>,
}

pub(crate) fn is_checkpoint(bytes: &[u8]) -> Result<bool, serde_json::Error> {
    Ok(serde_json::from_slice::<FormatProbe>(bytes)?
        .format
        .is_some())
}

pub(crate) fn sidecar(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".wal");
    PathBuf::from(name)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn regular_file(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_file() => Ok(()),
        Ok(_) => Err(invalid("journal path must be a regular non-symlink file")),
        Err(error) => Err(error),
    }
}

fn open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options
}

fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    let _ = path;
    Ok(())
}

fn header<S: JournalState>(id: &str) -> io::Result<[u8; HEADER_SIZE as usize]> {
    let mut result = [0; HEADER_SIZE as usize];
    result[..8].copy_from_slice(MAGIC);
    let id = hex::decode(id).map_err(|_| invalid("invalid journal identity"))?;
    if id.len() != 16 {
        return Err(invalid("invalid journal identity length"));
    }
    result[8..24].copy_from_slice(&id);
    result[24..].copy_from_slice(&Sha256::digest(S::KIND.as_bytes())[..8]);
    Ok(result)
}

fn write_checkpoint<S: JournalState>(path: &Path, state: &S, cursor: &Cursor) -> io::Result<()> {
    let snapshot = Checkpoint {
        format: FORMAT.to_owned(),
        kind: S::KIND.to_owned(),
        journal: cursor.clone(),
        state,
    };
    let mut file = AtomicWriteFile::options().open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    serde_json::to_writer(&mut file, &snapshot).map_err(json_error)?;
    file.write_all(b"\n")?;
    file.commit()?;
    sync_parent(path)
}

#[derive(Debug)]
pub(crate) struct Journal {
    cursor: Option<Cursor>,
    // Expected physical length includes a possible incomplete final frame.
    observed_len: u64,
    legacy_hash: Option<[u8; 32]>,
    lease: Option<Arc<StrategyRuntimeLease>>,
    initialized: bool,
    poisoned: bool,
    checkpoint_busy: Arc<AtomicBool>,
    checkpoint_at: Instant,
    commits: u64,
}

impl Journal {
    pub(crate) fn legacy(bytes: &[u8], path: &Path) -> io::Result<Self> {
        let wal = sidecar(path);
        // An orphan header from an interrupted migration is safe; records without
        // a checkpoint are not. Never silently discard committed evidence.
        if wal.exists() {
            regular_file(&wal)?;
            if fs::metadata(&wal)?.len() > HEADER_SIZE {
                return Err(invalid("legacy checkpoint has an unreferenced journal"));
            }
        }
        Ok(Self {
            cursor: None,
            observed_len: 0,
            legacy_hash: Some(Sha256::digest(bytes).into()),
            lease: None,
            initialized: false,
            poisoned: false,
            checkpoint_busy: Arc::new(AtomicBool::new(false)),
            checkpoint_at: Instant::now(),
            commits: 0,
        })
    }

    pub(crate) fn read<S: JournalState>(bytes: &[u8], path: &Path) -> io::Result<(S, Self)> {
        let checkpoint: Checkpoint<S> = serde_json::from_slice(bytes).map_err(json_error)?;
        if checkpoint.format != FORMAT || checkpoint.kind != S::KIND {
            return Err(invalid("unsupported journal checkpoint format or kind"));
        }
        let mut state = checkpoint.state;
        let mut cursor = checkpoint.journal;
        let expected_header = header::<S>(&cursor.id)?;
        let wal = sidecar(path);
        regular_file(&wal)?;
        let mut file = open_options().open(&wal)?;
        let observed_len = file.metadata()?.len();
        let mut actual_header = [0; HEADER_SIZE as usize];
        file.read_exact(&mut actual_header)?;
        if actual_header != expected_header
            || cursor.offset < HEADER_SIZE
            || cursor.offset > observed_len
        {
            return Err(invalid("journal identity or checkpoint offset mismatch"));
        }
        file.seek(SeekFrom::Start(cursor.offset))?;
        // Bound the read to one observed length so concurrent appends cannot
        // expose half a later frame or keep a read-only catalog scan alive forever.
        let mut reader = file.take(observed_len - cursor.offset);
        while observed_len - cursor.offset >= FRAME_HEADER_SIZE as u64 {
            let mut frame = [0; FRAME_HEADER_SIZE];
            reader.read_exact(&mut frame)?;
            let mut digest = Sha256::new();
            digest.update(expected_header);
            digest.update(&frame[..56]);
            if digest.finalize()[..] != frame[56..] {
                return Err(invalid("journal frame header checksum mismatch"));
            }
            let length = u64::from_le_bytes(frame[..8].try_into().unwrap());
            let base = u64::from_le_bytes(frame[8..16].try_into().unwrap());
            let revision = u64::from_le_bytes(frame[16..24].try_into().unwrap());
            if length > MAX_DELTA_BYTES
                || base != state.revision()
                || base.checked_add(1) != Some(revision)
            {
                return Err(invalid("journal frame length or revision mismatch"));
            }
            if observed_len - cursor.offset - (FRAME_HEADER_SIZE as u64) < length {
                break;
            }
            let mut payload = vec![0; length as usize];
            reader.read_exact(&mut payload)?;
            if Sha256::digest(&payload)[..] != frame[24..56] {
                return Err(invalid("journal payload checksum mismatch"));
            }
            state.apply(serde_json::from_slice(&payload).map_err(json_error)?)?;
            if state.revision() != revision {
                return Err(invalid("journal delta revision mismatch"));
            }
            cursor.offset += FRAME_HEADER_SIZE as u64 + length;
        }
        Ok((
            state,
            Self {
                cursor: Some(cursor),
                observed_len,
                legacy_hash: None,
                lease: None,
                initialized: false,
                poisoned: false,
                checkpoint_busy: Arc::new(AtomicBool::new(false)),
                checkpoint_at: Instant::now(),
                commits: 0,
            },
        ))
    }

    fn initialize<S: JournalState>(&mut self, path: &Path, state: &S) -> io::Result<()> {
        let wal = sidecar(path);
        let mut lock = wal.as_os_str().to_owned();
        lock.push(".lock");
        let lease = Arc::new(
            StrategyRuntimeLease::acquire(PathBuf::from(lock))
                .map_err(|error| io::Error::other(error.to_string()))?,
        );
        if self.cursor.is_some() {
            // A same-length repair/commit can replace an incomplete tail after
            // read-only load. Replay again UNDER the lease before truncating any
            // byte; physical length and journal identity alone are not a fence.
            let bytes = fs::read(path)?;
            let (current, recovered) = Self::read::<S>(&bytes, path)?;
            if &current != state {
                return Err(invalid("stale recovered journal writer revision or state"));
            }
            self.cursor = recovered.cursor;
            self.observed_len = recovered.observed_len;
        }
        if self.cursor.is_none() {
            let bytes = match fs::read(path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(error),
            };
            if self.legacy_hash != Some(Sha256::digest(&bytes).into()) {
                return Err(invalid("stale legacy journal writer"));
            }
            let exists = wal.exists();
            if exists {
                regular_file(&wal)?;
                if fs::metadata(&wal)?.len() > HEADER_SIZE {
                    return Err(invalid("unreferenced journal records"));
                }
            }
            // No migrated mutation can have committed while the authoritative
            // checkpoint is legacy and the orphan contains only header bytes.
            let mut id = [0; 16];
            getrandom::fill(&mut id).map_err(|error| io::Error::other(error.to_string()))?;
            let id = hex::encode(id);
            let mut options = open_options();
            options.write(true);
            let mut file = if exists {
                options.truncate(true).open(&wal)?
            } else {
                options.create_new(true).open(&wal)?
            };
            file.write_all(&header::<S>(&id)?)?;
            file.sync_all()?;
            sync_parent(&wal)?;
            let cursor = Cursor {
                id,
                offset: HEADER_SIZE,
            };
            write_checkpoint(path, state, &cursor)?;
            self.cursor = Some(cursor);
            self.observed_len = HEADER_SIZE;
            self.legacy_hash = None;
        }
        let cursor = self.cursor.as_ref().unwrap();
        regular_file(&wal)?;
        let mut writer = open_options().write(true).open(&wal)?;
        if writer.metadata()?.len() != self.observed_len {
            return Err(invalid("stale journal writer length"));
        }
        let mut actual = [0; HEADER_SIZE as usize];
        writer.read_exact(&mut actual)?;
        if actual != header::<S>(&cursor.id)? {
            return Err(invalid("stale journal writer identity"));
        }
        if self.observed_len != cursor.offset {
            // Only a recovered, incomplete EOF frame can be discarded, and only
            // after obtaining the exclusive writer lease and fencing its length.
            writer.set_len(cursor.offset)?;
            writer.sync_all()?;
            self.observed_len = cursor.offset;
        }
        writer.seek(SeekFrom::End(0))?;
        self.initialized = true;
        self.lease = Some(lease);
        Ok(())
    }

    pub(crate) fn append<S: JournalState>(
        &mut self,
        path: &Path,
        previous: &S,
        next: Arc<S>,
    ) -> io::Result<()> {
        if self.poisoned || previous.revision().checked_add(1) != Some(next.revision()) {
            return Err(invalid(
                "journal writer is poisoned or revision did not advance once",
            ));
        }
        previous.validate_successor(&next)?;
        let payload = serde_json::to_vec(&previous.delta(&next)).map_err(json_error)?;
        if payload.len() as u64 > MAX_DELTA_BYTES {
            return Err(invalid("journal delta exceeds maximum frame size"));
        }
        // Any ambiguous write/sync failure requires a fresh replay, never a blind
        // retry on the old in-memory revision.
        self.poisoned = true;
        if !self.initialized {
            self.initialize(path, previous)?;
        }
        let cursor = self.cursor.as_mut().unwrap();
        // Reopen the canonical path under the lease for every durable append.
        // A cached descriptor could otherwise successfully fsync an unlinked WAL.
        let wal = sidecar(path);
        regular_file(path)?;
        regular_file(&wal)?;
        let mut writer = open_options().write(true).open(&wal)?;
        if writer.metadata()?.len() != cursor.offset {
            return Err(invalid("journal length changed under writer"));
        }
        let mut actual = [0; HEADER_SIZE as usize];
        writer.read_exact(&mut actual)?;
        if actual != header::<S>(&cursor.id)? {
            return Err(invalid("journal identity changed under writer"));
        }
        writer.seek(SeekFrom::Start(cursor.offset))?;
        let mut frame = [0; FRAME_HEADER_SIZE];
        frame[..8].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        frame[8..16].copy_from_slice(&previous.revision().to_le_bytes());
        frame[16..24].copy_from_slice(&next.revision().to_le_bytes());
        frame[24..56].copy_from_slice(&Sha256::digest(&payload));
        let mut digest = Sha256::new();
        digest.update(header::<S>(&cursor.id)?);
        digest.update(&frame[..56]);
        frame[56..].copy_from_slice(&digest.finalize());
        writer.write_all(&frame)?;
        writer.write_all(&payload)?;
        writer.sync_all()?;
        cursor.offset += FRAME_HEADER_SIZE as u64 + payload.len() as u64;
        self.observed_len = cursor.offset;
        self.poisoned = false;
        self.commits += 1;
        self.maybe_checkpoint(path, next);
        Ok(())
    }

    fn maybe_checkpoint<S: JournalState>(&mut self, path: &Path, state: Arc<S>) {
        if (self.commits < CHECKPOINT_COMMITS && self.checkpoint_at.elapsed() < CHECKPOINT_INTERVAL)
            || self
                .checkpoint_busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        let cursor = self.cursor.as_ref().unwrap().clone();
        let path = path.to_owned();
        let busy = self.checkpoint_busy.clone();
        // Retain the writer lease until the last old-generation checkpoint ends.
        let lease = self.lease.as_ref().unwrap().clone();
        let result = std::thread::Builder::new().name("ledger-checkpoint".into()).spawn(move || {
            let _lease = lease;
            if let Err(error) = write_checkpoint(&path, state.as_ref(), &cursor) {
                tracing::error!(kind = S::KIND, %error, "ledger checkpoint failed; durable WAL retained");
            }
            busy.store(false, Ordering::Release);
        });
        if let Err(error) = result {
            self.checkpoint_busy.store(false, Ordering::Release);
            tracing::error!(%error, "could not schedule ledger checkpoint; durable WAL retained");
        }
        self.commits = 0;
        self.checkpoint_at = Instant::now();
    }
}
