use std::{
    fs,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::engine::StrategyRunId;

pub(crate) const STRATEGY_CATALOG_LEASE_FILE_NAME: &str = ".catalog.lock";
pub(crate) const STRATEGY_START_STAGING_DIRECTORY_NAME: &str = ".start-reservations";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyFilePaths {
    run_id: StrategyRunId,
    directory: PathBuf,
    state: PathBuf,
    intents: PathBuf,
    lease: PathBuf,
}

impl StrategyFilePaths {
    pub fn new(
        root: impl Into<PathBuf>,
        run_id: StrategyRunId,
    ) -> Result<Self, StrategyFilePathError> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(StrategyFilePathError::EmptyRoot);
        }
        let directory = root.join(run_id.as_str());
        Ok(Self {
            run_id,
            state: directory.join("strategy.json"),
            intents: directory.join("intents.json"),
            lease: directory.join("runtime.lock"),
            directory,
        })
    }

    pub fn run_id(&self) -> &StrategyRunId {
        &self.run_id
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn state(&self) -> &Path {
        &self.state
    }

    pub fn intents(&self) -> &Path {
        &self.intents
    }

    pub fn lease(&self) -> &Path {
        &self.lease
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StrategyFilePathError {
    #[error("strategy file root is empty")]
    EmptyRoot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyDiscoveryReport {
    pub strategies: Vec<StrategyFilePaths>,
    pub anomalies: Vec<StrategyDiscoveryAnomaly>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyDiscoveryAnomaly {
    pub path: PathBuf,
    pub kind: StrategyDiscoveryAnomalyKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyDiscoveryAnomalyKind {
    InvalidRunDirectoryName,
    SymbolicLink,
    UnexpectedEntryType,
    MissingStrategyState,
    OrphanIntentLedger,
}

pub fn discover_strategy_files(
    root: impl Into<PathBuf>,
) -> Result<StrategyDiscoveryReport, StrategyDiscoveryError> {
    let root = root.into();
    if root.as_os_str().is_empty() {
        return Err(StrategyDiscoveryError::EmptyRoot);
    }
    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StrategyDiscoveryReport {
                strategies: Vec::new(),
                anomalies: Vec::new(),
            });
        }
        Err(error) => return Err(StrategyDiscoveryError::InspectRoot(error)),
    };
    if metadata.file_type().is_symlink() {
        return Err(StrategyDiscoveryError::RootIsSymbolicLink);
    }
    if !metadata.is_dir() {
        return Err(StrategyDiscoveryError::RootIsNotDirectory);
    }

    let mut report = StrategyDiscoveryReport {
        strategies: Vec::new(),
        anomalies: Vec::new(),
    };
    for entry in fs::read_dir(&root).map_err(StrategyDiscoveryError::ReadRoot)? {
        let entry = entry.map_err(StrategyDiscoveryError::ReadEntry)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(StrategyDiscoveryError::EntryType)?;
        if file_type.is_symlink() {
            report.anomalies.push(StrategyDiscoveryAnomaly {
                path,
                kind: StrategyDiscoveryAnomalyKind::SymbolicLink,
            });
            continue;
        }
        if entry.file_name() == STRATEGY_CATALOG_LEASE_FILE_NAME {
            if !file_type.is_file() {
                report.anomalies.push(StrategyDiscoveryAnomaly {
                    path,
                    kind: StrategyDiscoveryAnomalyKind::UnexpectedEntryType,
                });
            }
            continue;
        }
        if entry.file_name() == STRATEGY_START_STAGING_DIRECTORY_NAME {
            if !file_type.is_dir() {
                report.anomalies.push(StrategyDiscoveryAnomaly {
                    path,
                    kind: StrategyDiscoveryAnomalyKind::UnexpectedEntryType,
                });
            }
            continue;
        }
        if !file_type.is_dir() {
            report.anomalies.push(StrategyDiscoveryAnomaly {
                path,
                kind: StrategyDiscoveryAnomalyKind::UnexpectedEntryType,
            });
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            report.anomalies.push(StrategyDiscoveryAnomaly {
                path,
                kind: StrategyDiscoveryAnomalyKind::InvalidRunDirectoryName,
            });
            continue;
        };
        let Ok(run_id) = StrategyRunId::parse(name) else {
            report.anomalies.push(StrategyDiscoveryAnomaly {
                path,
                kind: StrategyDiscoveryAnomalyKind::InvalidRunDirectoryName,
            });
            continue;
        };
        let paths = StrategyFilePaths::new(&root, run_id)?;
        let state = inspect_runtime_file(paths.state())?;
        let intents = inspect_runtime_file(paths.intents())?;
        let lease = inspect_runtime_file(paths.lease())?;
        let mut invalid_runtime_file = false;
        for (inspection, path) in [
            (state, paths.state()),
            (intents, paths.intents()),
            (lease, paths.lease()),
        ] {
            if let Some(kind) = inspection.anomaly {
                invalid_runtime_file = true;
                report.anomalies.push(StrategyDiscoveryAnomaly {
                    path: path.to_path_buf(),
                    kind,
                });
            }
        }
        if invalid_runtime_file {
            continue;
        }
        if !state.exists {
            report.anomalies.push(StrategyDiscoveryAnomaly {
                path: paths.directory().to_path_buf(),
                kind: if intents.exists {
                    StrategyDiscoveryAnomalyKind::OrphanIntentLedger
                } else {
                    StrategyDiscoveryAnomalyKind::MissingStrategyState
                },
            });
            continue;
        }
        report.strategies.push(paths);
    }
    report
        .strategies
        .sort_by(|left, right| left.run_id.cmp(&right.run_id));
    report
        .anomalies
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(report)
}

#[derive(Debug, Clone, Copy)]
struct InspectedRuntimeFile {
    exists: bool,
    anomaly: Option<StrategyDiscoveryAnomalyKind>,
}

fn inspect_runtime_file(path: &Path) -> Result<InspectedRuntimeFile, StrategyDiscoveryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Ok(InspectedRuntimeFile {
            exists: true,
            anomaly: Some(StrategyDiscoveryAnomalyKind::SymbolicLink),
        }),
        Ok(metadata) if metadata.is_file() => Ok(InspectedRuntimeFile {
            exists: true,
            anomaly: None,
        }),
        Ok(_) => Ok(InspectedRuntimeFile {
            exists: true,
            anomaly: Some(StrategyDiscoveryAnomalyKind::UnexpectedEntryType),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(InspectedRuntimeFile {
            exists: false,
            anomaly: None,
        }),
        Err(error) => Err(StrategyDiscoveryError::InspectEntry(error)),
    }
}

#[derive(Debug, Error)]
pub enum StrategyDiscoveryError {
    #[error("strategy discovery root is empty")]
    EmptyRoot,
    #[error("strategy discovery root must not be a symbolic link")]
    RootIsSymbolicLink,
    #[error("strategy discovery root is not a directory")]
    RootIsNotDirectory,
    #[error("failed to inspect strategy discovery root: {0}")]
    InspectRoot(std::io::Error),
    #[error("failed to read strategy discovery root: {0}")]
    ReadRoot(std::io::Error),
    #[error("failed to read a strategy discovery entry: {0}")]
    ReadEntry(std::io::Error),
    #[error("failed to inspect a strategy discovery entry type: {0}")]
    EntryType(std::io::Error),
    #[error("failed to inspect a strategy runtime file: {0}")]
    InspectEntry(std::io::Error),
    #[error(transparent)]
    Paths(#[from] StrategyFilePathError),
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn one_validated_run_id_owns_one_fixed_file_set() {
        let paths = StrategyFilePaths::new(
            "/var/lib/grid-trading/strategies",
            StrategyRunId::parse("MU000001").unwrap(),
        )
        .unwrap();

        assert_eq!(paths.run_id().as_str(), "MU000001");
        assert_eq!(
            paths.directory(),
            Path::new("/var/lib/grid-trading/strategies/MU000001")
        );
        assert_eq!(
            paths.state(),
            Path::new("/var/lib/grid-trading/strategies/MU000001/strategy.json")
        );
        assert_eq!(
            paths.intents(),
            Path::new("/var/lib/grid-trading/strategies/MU000001/intents.json")
        );
        assert_eq!(
            paths.lease(),
            Path::new("/var/lib/grid-trading/strategies/MU000001/runtime.lock")
        );
    }

    #[test]
    fn empty_root_and_traversal_run_ids_never_create_paths() {
        assert_eq!(
            StrategyFilePaths::new(PathBuf::new(), StrategyRunId::parse("MU000001").unwrap()),
            Err(StrategyFilePathError::EmptyRoot)
        );
        assert!(StrategyRunId::parse("../MU001").is_err());
    }

    #[test]
    fn missing_discovery_root_is_an_empty_first_start() {
        let directory = tempdir().unwrap();
        let report = discover_strategy_files(directory.path().join("not-created")).unwrap();

        assert!(report.strategies.is_empty());
        assert!(report.anomalies.is_empty());
    }

    #[test]
    fn catalog_lease_is_ignored_only_when_it_is_a_regular_file() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        fs::create_dir(&root).unwrap();
        let lease = root.join(STRATEGY_CATALOG_LEASE_FILE_NAME);
        fs::write(&lease, b"").unwrap();

        let clean = discover_strategy_files(&root).unwrap();
        assert!(clean.strategies.is_empty());
        assert!(clean.anomalies.is_empty());

        fs::remove_file(&lease).unwrap();
        fs::create_dir(&lease).unwrap();
        let malformed = discover_strategy_files(&root).unwrap();
        assert_eq!(malformed.anomalies.len(), 1);
        assert_eq!(malformed.anomalies[0].path, lease);
        assert_eq!(
            malformed.anomalies[0].kind,
            StrategyDiscoveryAnomalyKind::UnexpectedEntryType
        );
    }

    #[test]
    fn start_staging_is_ignored_only_when_it_is_a_real_directory() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        fs::create_dir(&root).unwrap();
        let staging = root.join(STRATEGY_START_STAGING_DIRECTORY_NAME);
        let interrupted = staging.join("ABORTED1");
        fs::create_dir_all(&interrupted).unwrap();
        fs::write(interrupted.join("runtime.lock"), b"").unwrap();
        fs::write(interrupted.join("strategy.json"), b"partial").unwrap();

        let clean = discover_strategy_files(&root).unwrap();
        assert!(clean.strategies.is_empty());
        assert!(clean.anomalies.is_empty());

        fs::remove_dir_all(&staging).unwrap();
        fs::write(&staging, b"not a directory").unwrap();
        let malformed = discover_strategy_files(&root).unwrap();
        assert_eq!(malformed.anomalies.len(), 1);
        assert_eq!(malformed.anomalies[0].path, staging);
        assert_eq!(
            malformed.anomalies[0].kind,
            StrategyDiscoveryAnomalyKind::UnexpectedEntryType
        );
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_start_staging_is_never_ignored() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        let outside = directory.path().join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&outside).unwrap();
        let staging = root.join(STRATEGY_START_STAGING_DIRECTORY_NAME);
        symlink(&outside, &staging).unwrap();

        let report = discover_strategy_files(&root).unwrap();
        assert!(report.strategies.is_empty());
        assert_eq!(report.anomalies.len(), 1);
        assert_eq!(report.anomalies[0].path, staging);
        assert_eq!(
            report.anomalies[0].kind,
            StrategyDiscoveryAnomalyKind::SymbolicLink
        );
    }

    #[test]
    fn discovery_returns_valid_runs_and_reports_every_incomplete_entry() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        fs::create_dir(&root).unwrap();

        let valid = root.join("VALID001");
        fs::create_dir(&valid).unwrap();
        fs::write(valid.join("strategy.json"), b"{}").unwrap();

        let orphan = root.join("ORPHAN01");
        fs::create_dir(&orphan).unwrap();
        fs::write(orphan.join("intents.json"), b"{}").unwrap();

        fs::create_dir(root.join("MISSING1")).unwrap();
        fs::create_dir(root.join("bad-name")).unwrap();
        fs::write(root.join("notes.txt"), b"unexpected").unwrap();

        let bad_lock = root.join("BADLOCK1");
        fs::create_dir(&bad_lock).unwrap();
        fs::write(bad_lock.join("strategy.json"), b"{}").unwrap();
        fs::create_dir(bad_lock.join("intents.json")).unwrap();
        fs::create_dir(bad_lock.join("runtime.lock")).unwrap();

        let report = discover_strategy_files(&root).unwrap();

        assert_eq!(report.strategies.len(), 1);
        assert_eq!(report.strategies[0].run_id().as_str(), "VALID001");
        assert_eq!(report.anomalies.len(), 6);
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == orphan
                && anomaly.kind == StrategyDiscoveryAnomalyKind::OrphanIntentLedger
        }));
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == root.join("MISSING1")
                && anomaly.kind == StrategyDiscoveryAnomalyKind::MissingStrategyState
        }));
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == root.join("bad-name")
                && anomaly.kind == StrategyDiscoveryAnomalyKind::InvalidRunDirectoryName
        }));
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == root.join("notes.txt")
                && anomaly.kind == StrategyDiscoveryAnomalyKind::UnexpectedEntryType
        }));
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == bad_lock.join("intents.json")
                && anomaly.kind == StrategyDiscoveryAnomalyKind::UnexpectedEntryType
        }));
        assert!(report.anomalies.iter().any(|anomaly| {
            anomaly.path == bad_lock.join("runtime.lock")
                && anomaly.kind == StrategyDiscoveryAnomalyKind::UnexpectedEntryType
        }));
    }

    #[test]
    fn non_directory_discovery_root_fails_closed() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        fs::write(&root, b"not a directory").unwrap();

        assert!(matches!(
            discover_strategy_files(root),
            Err(StrategyDiscoveryError::RootIsNotDirectory)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_runtime_entry_is_never_discovered() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let root = directory.path().join("strategies");
        let outside = directory.path().join("outside.json");
        let run = root.join("SYMLINK1");
        fs::create_dir_all(&run).unwrap();
        fs::write(&outside, b"{}").unwrap();
        symlink(&outside, run.join("strategy.json")).unwrap();

        let report = discover_strategy_files(root).unwrap();

        assert!(report.strategies.is_empty());
        assert_eq!(report.anomalies.len(), 1);
        assert_eq!(
            report.anomalies[0].kind,
            StrategyDiscoveryAnomalyKind::SymbolicLink
        );
    }
}
