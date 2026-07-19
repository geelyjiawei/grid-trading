use std::{
    collections::BTreeMap,
    fmt, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use atomic_write_file::AtomicWriteFile;
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use fernet::Fernet;
use pbkdf2::pbkdf2_hmac;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::domain::Exchange;

const CONFIG_VERSION: u8 = 2;
const CONFIG_BACKEND: &str = "fernet";
const PBKDF2_ITERATIONS: u32 = 390_000;
const MAX_CONFIG_BYTES: u64 = 1_048_576;
const DEFAULT_SALT_CONTEXT: &[u8] = b"grid-trading-api-config-v1";

pub struct StoredExchangeConfiguration {
    exchange: Exchange,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    testnet: bool,
}

impl StoredExchangeConfiguration {
    pub fn new(
        exchange: Exchange,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        testnet: bool,
    ) -> Result<Self, ExchangeConfigStoreError> {
        Ok(Self {
            exchange,
            api_key: validate_secret(api_key.into(), "API key")?,
            api_secret: validate_secret(api_secret.into(), "API secret")?,
            testnet,
        })
    }

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    pub fn api_key(&self) -> &str {
        self.api_key.as_str()
    }

    pub fn api_secret(&self) -> &str {
        self.api_secret.as_str()
    }

    pub fn testnet(&self) -> bool {
        self.testnet
    }
}

impl fmt::Debug for StoredExchangeConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredExchangeConfiguration")
            .field("exchange", &self.exchange)
            .field("credentials", &"[REDACTED]")
            .field("testnet", &self.testnet)
            .finish()
    }
}

fn validate_secret(
    value: String,
    field: &'static str,
) -> Result<Zeroizing<String>, ExchangeConfigStoreError> {
    let value = Zeroizing::new(value);
    if value.is_empty() || value.trim() != value.as_str() {
        return Err(ExchangeConfigStoreError::InvalidCredential(field));
    }
    if ['\r', '\n', '\0']
        .into_iter()
        .any(|character| value.contains(character))
    {
        return Err(ExchangeConfigStoreError::InvalidCredential(field));
    }
    Ok(value)
}

#[derive(Clone)]
pub struct EncryptedExchangeConfigStore {
    path: PathBuf,
    cipher: Arc<Fernet>,
    state: Arc<Mutex<StoreState>>,
}

#[derive(Default)]
struct StoreState {
    file_was_present: bool,
}

impl EncryptedExchangeConfigStore {
    pub fn new(
        path: impl Into<PathBuf>,
        master_key: Zeroizing<String>,
        salt: Option<Zeroizing<String>>,
    ) -> Result<Self, ExchangeConfigStoreError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(ExchangeConfigStoreError::RelativePath);
        }
        if master_key.trim().is_empty() || master_key.trim() != master_key.as_str() {
            return Err(ExchangeConfigStoreError::InvalidMasterKey);
        }
        let cipher = Fernet::new(master_key.as_str()).or_else(|| {
            let salt = salt
                .as_deref()
                .map(|value| value.as_bytes())
                .map(Vec::from)
                .unwrap_or_else(|| Sha256::digest(DEFAULT_SALT_CONTEXT).to_vec());
            let mut derived = Zeroizing::new([0_u8; 32]);
            pbkdf2_hmac::<Sha256>(
                master_key.as_bytes(),
                &salt,
                PBKDF2_ITERATIONS,
                derived.as_mut_slice(),
            );
            Fernet::new(&URL_SAFE.encode(derived.as_slice()))
        });
        let cipher = cipher.ok_or(ExchangeConfigStoreError::InvalidMasterKey)?;
        Ok(Self {
            path,
            cipher: Arc::new(cipher),
            state: Arc::new(Mutex::new(StoreState::default())),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn backend(&self) -> &'static str {
        CONFIG_BACKEND
    }

    pub fn load(&self) -> Result<Vec<StoredExchangeConfiguration>, ExchangeConfigStoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExchangeConfigStoreError::LockPoisoned)?;
        self.load_unlocked(&mut state)
    }

    pub fn upsert(
        &self,
        configuration: &StoredExchangeConfiguration,
    ) -> Result<(), ExchangeConfigStoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExchangeConfigStoreError::LockPoisoned)?;
        let mut configurations = self.load_unlocked(&mut state)?;
        configurations.retain(|item| item.exchange() != configuration.exchange());
        configurations.push(StoredExchangeConfiguration::new(
            configuration.exchange(),
            configuration.api_key().to_owned(),
            configuration.api_secret().to_owned(),
            configuration.testnet(),
        )?);
        configurations.sort_by_key(|item| exchange_name(item.exchange()));
        self.write_unlocked(configurations.iter(), &mut state)
    }

    fn load_unlocked(
        &self,
        state: &mut StoreState,
    ) -> Result<Vec<StoredExchangeConfiguration>, ExchangeConfigStoreError> {
        let Some(bytes) = read_config_file(&self.path)? else {
            if state.file_was_present {
                return Err(ExchangeConfigStoreError::FileDisappeared);
            }
            return Ok(Vec::new());
        };
        state.file_was_present = true;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(ExchangeConfigStoreError::Deserialize)?;
        let root = value
            .as_object()
            .ok_or(ExchangeConfigStoreError::InvalidStructure)?;
        let entries = if root.contains_key("configs") {
            let envelope: PersistedEnvelope =
                serde_json::from_value(value).map_err(ExchangeConfigStoreError::Deserialize)?;
            if envelope.version != CONFIG_VERSION
                || !envelope.encrypted
                || envelope.backend != CONFIG_BACKEND
                || envelope.configs.is_empty()
            {
                return Err(ExchangeConfigStoreError::InvalidStructure);
            }
            envelope.configs
        } else {
            let entry: PersistedEntry =
                serde_json::from_value(value).map_err(ExchangeConfigStoreError::Deserialize)?;
            let exchange = parse_exchange(&entry.exchange)?;
            BTreeMap::from([(exchange_name(exchange).to_owned(), entry)])
        };

        entries
            .into_iter()
            .map(|(key, entry)| self.decrypt_entry(&key, entry))
            .collect()
    }

    fn decrypt_entry(
        &self,
        key: &str,
        entry: PersistedEntry,
    ) -> Result<StoredExchangeConfiguration, ExchangeConfigStoreError> {
        let exchange = parse_exchange(key)?;
        if parse_exchange(&entry.exchange)? != exchange
            || !entry.encrypted
            || entry.backend.as_deref().unwrap_or(CONFIG_BACKEND) != CONFIG_BACKEND
        {
            return Err(ExchangeConfigStoreError::InvalidStructure);
        }
        let api_key = decrypt_text(&self.cipher, &entry.api_key)?;
        let api_secret = decrypt_text(&self.cipher, &entry.api_secret)?;
        StoredExchangeConfiguration::new(exchange, api_key, api_secret, entry.testnet)
    }

    fn write_unlocked<'a>(
        &self,
        configurations: impl Iterator<Item = &'a StoredExchangeConfiguration>,
        state: &mut StoreState,
    ) -> Result<(), ExchangeConfigStoreError> {
        validate_destination(&self.path, state.file_was_present)?;
        let mut entries = BTreeMap::new();
        for configuration in configurations {
            let exchange = exchange_name(configuration.exchange()).to_owned();
            entries.insert(
                exchange.clone(),
                PersistedEntry {
                    encrypted: true,
                    backend: Some(CONFIG_BACKEND.to_owned()),
                    exchange,
                    api_key: self.cipher.encrypt(configuration.api_key().as_bytes()),
                    api_secret: self.cipher.encrypt(configuration.api_secret().as_bytes()),
                    testnet: configuration.testnet(),
                },
            );
        }
        if entries.is_empty() {
            return Err(ExchangeConfigStoreError::InvalidStructure);
        }
        let envelope = PersistedEnvelope {
            version: CONFIG_VERSION,
            encrypted: true,
            backend: CONFIG_BACKEND.to_owned(),
            configs: entries,
        };
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(ExchangeConfigStoreError::CreateDirectory)?;
            reject_symbolic_link(parent)?;
        }

        let options = private_atomic_options();
        let mut file = options
            .open(&self.path)
            .map_err(ExchangeConfigStoreError::OpenAtomic)?;
        serde_json::to_writer_pretty(&mut file, &envelope)
            .map_err(ExchangeConfigStoreError::Serialize)?;
        file.write_all(b"\n")
            .map_err(ExchangeConfigStoreError::Write)?;
        file.commit().map_err(ExchangeConfigStoreError::Commit)?;
        sync_parent(&self.path)?;
        state.file_was_present = true;
        Ok(())
    }
}

impl fmt::Debug for EncryptedExchangeConfigStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedExchangeConfigStore")
            .field("path", &self.path)
            .field("cipher", &"[REDACTED]")
            .finish()
    }
}

fn decrypt_text(cipher: &Fernet, token: &str) -> Result<String, ExchangeConfigStoreError> {
    let bytes = Zeroizing::new(
        cipher
            .decrypt(token)
            .map_err(|_| ExchangeConfigStoreError::Decrypt)?,
    );
    String::from_utf8(bytes.to_vec()).map_err(|_| ExchangeConfigStoreError::InvalidPlaintext)
}

fn read_config_file(path: &Path) -> Result<Option<Vec<u8>>, ExchangeConfigStoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ExchangeConfigStoreError::Inspect(error)),
    };
    if metadata.file_type().is_symlink() {
        return Err(ExchangeConfigStoreError::SymbolicLink);
    }
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err(ExchangeConfigStoreError::InvalidFile);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ExchangeConfigStoreError::InsecurePermissions);
        }
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(ExchangeConfigStoreError::Open)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(ExchangeConfigStoreError::Read)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(ExchangeConfigStoreError::InvalidFile);
    }
    Ok(Some(bytes))
}

#[cfg(unix)]
fn private_atomic_options() -> atomic_write_file::OpenOptions {
    use atomic_write_file::unix::OpenOptionsExt as AtomicOpenOptionsExt;
    use std::os::unix::fs::OpenOptionsExt as UnixOpenOptionsExt;

    let mut options = AtomicWriteFile::options();
    options.preserve_mode(false).mode(0o600);
    options
}

#[cfg(not(unix))]
fn private_atomic_options() -> atomic_write_file::OpenOptions {
    AtomicWriteFile::options()
}

fn validate_destination(
    path: &Path,
    expected_present: bool,
) -> Result<(), ExchangeConfigStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(ExchangeConfigStoreError::SymbolicLink)
        }
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(ExchangeConfigStoreError::InvalidFile),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && expected_present => {
            Err(ExchangeConfigStoreError::FileDisappeared)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExchangeConfigStoreError::Inspect(error)),
    }
}

fn reject_symbolic_link(path: &Path) -> Result<(), ExchangeConfigStoreError> {
    let metadata = fs::symlink_metadata(path).map_err(ExchangeConfigStoreError::Inspect)?;
    if metadata.file_type().is_symlink() {
        return Err(ExchangeConfigStoreError::SymbolicLink);
    }
    if !metadata.is_dir() {
        return Err(ExchangeConfigStoreError::InvalidFile);
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), ExchangeConfigStoreError> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(ExchangeConfigStoreError::SyncDirectory)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> Result<(), ExchangeConfigStoreError> {
    Ok(())
}

fn parse_exchange(value: &str) -> Result<Exchange, ExchangeConfigStoreError> {
    match value {
        "binance" => Ok(Exchange::Binance),
        "aster" => Ok(Exchange::Aster),
        "bybit" => Ok(Exchange::Bybit),
        "trade_xyz" => Ok(Exchange::TradeXyz),
        _ => Err(ExchangeConfigStoreError::UnsupportedExchange),
    }
}

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Binance => "binance",
        Exchange::Aster => "aster",
        Exchange::Bybit => "bybit",
        Exchange::TradeXyz => "trade_xyz",
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedEnvelope {
    version: u8,
    encrypted: bool,
    backend: String,
    configs: BTreeMap<String, PersistedEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedEntry {
    encrypted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backend: Option<String>,
    exchange: String,
    api_key: String,
    api_secret: String,
    #[serde(default)]
    testnet: bool,
}

#[derive(Debug, Error)]
pub enum ExchangeConfigStoreError {
    #[error("exchange config path must be absolute")]
    RelativePath,
    #[error("exchange config master key is invalid")]
    InvalidMasterKey,
    #[error("{0} is invalid")]
    InvalidCredential(&'static str),
    #[error("exchange config lock is unavailable")]
    LockPoisoned,
    #[error("exchange config file metadata cannot be inspected: {0}")]
    Inspect(std::io::Error),
    #[error("exchange config file must be a regular private file")]
    InvalidFile,
    #[error("exchange config file must not be a symbolic link")]
    SymbolicLink,
    #[error("exchange config file permissions are not private")]
    InsecurePermissions,
    #[error("exchange config file disappeared after it was loaded")]
    FileDisappeared,
    #[error("exchange config file cannot be opened: {0}")]
    Open(std::io::Error),
    #[error("exchange config file cannot be read: {0}")]
    Read(std::io::Error),
    #[error("exchange config file is not valid JSON: {0}")]
    Deserialize(serde_json::Error),
    #[error("exchange config file has an invalid structure")]
    InvalidStructure,
    #[error("exchange config contains an unsupported exchange")]
    UnsupportedExchange,
    #[error("exchange config cannot be decrypted")]
    Decrypt,
    #[error("exchange config plaintext is invalid")]
    InvalidPlaintext,
    #[error("exchange config directory cannot be created: {0}")]
    CreateDirectory(std::io::Error),
    #[error("exchange config atomic file cannot be opened: {0}")]
    OpenAtomic(std::io::Error),
    #[error("exchange config cannot be serialized: {0}")]
    Serialize(serde_json::Error),
    #[error("exchange config cannot be written: {0}")]
    Write(std::io::Error),
    #[error("exchange config cannot be committed atomically: {0}")]
    Commit(std::io::Error),
    #[error("exchange config directory cannot be synchronized: {0}")]
    SyncDirectory(std::io::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn store(path: &Path) -> EncryptedExchangeConfigStore {
        EncryptedExchangeConfigStore::new(path, Zeroizing::new(Fernet::generate_key()), None)
            .unwrap()
    }

    #[test]
    fn exchange_config_round_trip_preserves_all_exchanges_without_plaintext() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("api_config.json");
        let store = store(&path);
        store
            .upsert(
                &StoredExchangeConfiguration::new(
                    Exchange::Binance,
                    "binance-visible-key",
                    "binance-secret-value",
                    false,
                )
                .unwrap(),
            )
            .unwrap();
        store
            .upsert(
                &StoredExchangeConfiguration::new(
                    Exchange::Aster,
                    "0x1111111111111111111111111111111111111111",
                    "aster-private-key",
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        store
            .upsert(
                &StoredExchangeConfiguration::new(
                    Exchange::Bybit,
                    "bybit-visible-key",
                    "bybit-secret-value",
                    false,
                )
                .unwrap(),
            )
            .unwrap();
        store
            .upsert(
                &StoredExchangeConfiguration::new(
                    Exchange::TradeXyz,
                    format!("0x{}", "2".repeat(40)),
                    "trade-xyz-agent-private-key",
                    false,
                )
                .unwrap(),
            )
            .unwrap();

        let text = fs::read_to_string(&path).unwrap();
        assert!(!text.contains("binance-visible-key"));
        assert!(!text.contains("binance-secret-value"));
        assert!(!text.contains("aster-private-key"));
        assert!(!text.contains("bybit-secret-value"));
        assert!(!text.contains("trade-xyz-agent-private-key"));
        let loaded = store.load().unwrap();
        assert_eq!(loaded.len(), 4);
        let binance = loaded
            .iter()
            .find(|item| item.exchange() == Exchange::Binance)
            .unwrap();
        let aster = loaded
            .iter()
            .find(|item| item.exchange() == Exchange::Aster)
            .unwrap();
        let trade_xyz = loaded
            .iter()
            .find(|item| item.exchange() == Exchange::TradeXyz)
            .unwrap();
        assert_eq!(binance.api_key(), "binance-visible-key");
        assert!(aster.testnet());
        assert_eq!(trade_xyz.api_secret(), "trade-xyz-agent-private-key");
    }

    #[test]
    fn exchange_config_tampering_blocks_updates_and_preserves_source_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("api_config.json");
        let store = store(&path);
        let first = StoredExchangeConfiguration::new(
            Exchange::Binance,
            "binance-key",
            "binance-secret",
            false,
        )
        .unwrap();
        store.upsert(&first).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value["configs"]["binance"]["api_secret"] = serde_json::Value::String("tampered".into());
        fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        let before = fs::read(&path).unwrap();

        let replacement =
            StoredExchangeConfiguration::new(Exchange::Bybit, "bybit-key", "bybit-secret", false)
                .unwrap();
        assert!(matches!(
            store.upsert(&replacement),
            Err(ExchangeConfigStoreError::Decrypt)
        ));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn exchange_config_disappearance_blocks_partial_replacement() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("api_config.json");
        let store = store(&path);
        store
            .upsert(
                &StoredExchangeConfiguration::new(
                    Exchange::Binance,
                    "binance-key",
                    "binance-secret",
                    false,
                )
                .unwrap(),
            )
            .unwrap();
        fs::remove_file(&path).unwrap();

        let replacement =
            StoredExchangeConfiguration::new(Exchange::Bybit, "bybit-key", "bybit-secret", false)
                .unwrap();
        assert!(matches!(
            store.upsert(&replacement),
            Err(ExchangeConfigStoreError::FileDisappeared)
        ));
        assert!(!path.exists());
    }
}
