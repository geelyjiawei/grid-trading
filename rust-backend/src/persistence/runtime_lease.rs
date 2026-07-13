use std::{
    fs,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[derive(Debug)]
pub struct StrategyRuntimeLease {
    path: PathBuf,
    _file: fs::File,
}

impl StrategyRuntimeLease {
    pub fn acquire(path: impl Into<PathBuf>) -> Result<Self, RuntimeLeaseError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(RuntimeLeaseError::InvalidPath);
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(RuntimeLeaseError::CreateDirectory)?;
        }

        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path).map_err(RuntimeLeaseError::Open)?;
        restrict_permissions(&file)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { path, _file: file }),
            Err(fs::TryLockError::WouldBlock) => Err(RuntimeLeaseError::AlreadyHeld),
            Err(fs::TryLockError::Error(error)) => Err(RuntimeLeaseError::Lock(error)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
fn restrict_permissions(file: &fs::File) -> Result<(), RuntimeLeaseError> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(RuntimeLeaseError::SetPermissions)
}

#[cfg(not(unix))]
fn restrict_permissions(_: &fs::File) -> Result<(), RuntimeLeaseError> {
    Ok(())
}

#[derive(Debug, Error)]
pub enum RuntimeLeaseError {
    #[error("strategy runtime lease path is empty")]
    InvalidPath,
    #[error("strategy runtime lease is already held")]
    AlreadyHeld,
    #[error("failed to create strategy runtime lease directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to open strategy runtime lease: {0}")]
    Open(std::io::Error),
    #[error("failed to restrict strategy runtime lease permissions: {0}")]
    SetPermissions(std::io::Error),
    #[error("failed to lock strategy runtime lease: {0}")]
    Lock(std::io::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn one_runtime_lease_owner_is_allowed_and_drop_releases_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("runtime.lock");
        let first = StrategyRuntimeLease::acquire(&path).unwrap();

        assert_eq!(first.path(), path);
        assert!(matches!(
            StrategyRuntimeLease::acquire(&path),
            Err(RuntimeLeaseError::AlreadyHeld)
        ));

        drop(first);
        assert!(StrategyRuntimeLease::acquire(path).is_ok());
    }

    #[test]
    fn separate_strategy_paths_do_not_block_each_other() {
        let directory = tempdir().unwrap();
        let first = StrategyRuntimeLease::acquire(directory.path().join("A.lock")).unwrap();
        let second = StrategyRuntimeLease::acquire(directory.path().join("B.lock")).unwrap();

        assert_ne!(first.path(), second.path());
    }

    #[test]
    fn empty_lease_path_is_rejected_before_opening_any_file() {
        assert!(matches!(
            StrategyRuntimeLease::acquire(PathBuf::new()),
            Err(RuntimeLeaseError::InvalidPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn lease_file_is_owner_read_write_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let path = directory.path().join("runtime.lock");
        let _lease = StrategyRuntimeLease::acquire(&path).unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
