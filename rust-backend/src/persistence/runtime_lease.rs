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
            ensure_parent_directory(parent)?;
        }

        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(RuntimeLeaseError::SymbolicLink);
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(RuntimeLeaseError::UnexpectedType);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(RuntimeLeaseError::Inspect(error)),
        }

        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
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

fn ensure_parent_directory(parent: &Path) -> Result<(), RuntimeLeaseError> {
    let metadata = match fs::symlink_metadata(parent) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(parent).map_err(RuntimeLeaseError::CreateDirectory)?;
            fs::symlink_metadata(parent).map_err(RuntimeLeaseError::InspectDirectory)?
        }
        Err(error) => return Err(RuntimeLeaseError::InspectDirectory(error)),
    };
    if metadata.file_type().is_symlink() {
        return Err(RuntimeLeaseError::ParentSymbolicLink);
    }
    if !metadata.is_dir() {
        return Err(RuntimeLeaseError::ParentUnexpectedType);
    }
    Ok(())
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
    #[error("failed to inspect strategy runtime lease directory: {0}")]
    InspectDirectory(std::io::Error),
    #[error("strategy runtime lease directory must not be a symbolic link")]
    ParentSymbolicLink,
    #[error("strategy runtime lease parent must be a directory")]
    ParentUnexpectedType,
    #[error("failed to inspect strategy runtime lease: {0}")]
    Inspect(std::io::Error),
    #[error("strategy runtime lease must not be a symbolic link")]
    SymbolicLink,
    #[error("strategy runtime lease must be a regular file")]
    UnexpectedType,
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

    #[test]
    fn directory_cannot_be_used_as_a_runtime_lease() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("runtime.lock");
        fs::create_dir(&path).unwrap();

        assert!(matches!(
            StrategyRuntimeLease::acquire(path),
            Err(RuntimeLeaseError::UnexpectedType)
        ));
    }

    #[test]
    fn regular_file_cannot_be_used_as_a_runtime_lease_parent() {
        let directory = tempdir().unwrap();
        let parent = directory.path().join("not-a-directory");
        fs::write(&parent, b"evidence").unwrap();

        assert!(matches!(
            StrategyRuntimeLease::acquire(parent.join("runtime.lock")),
            Err(RuntimeLeaseError::ParentUnexpectedType)
        ));
        assert_eq!(fs::read(parent).unwrap(), b"evidence");
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_cannot_be_used_as_a_runtime_lease_parent() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("lease-parent");
        fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        assert!(matches!(
            StrategyRuntimeLease::acquire(link.join("runtime.lock")),
            Err(RuntimeLeaseError::ParentSymbolicLink)
        ));
        assert!(!target.join("runtime.lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_cannot_be_used_as_a_runtime_lease() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let target = directory.path().join("target.lock");
        let link = directory.path().join("runtime.lock");
        fs::write(&target, b"external evidence").unwrap();
        symlink(&target, &link).unwrap();

        assert!(matches!(
            StrategyRuntimeLease::acquire(link),
            Err(RuntimeLeaseError::SymbolicLink)
        ));
        assert_eq!(fs::read(target).unwrap(), b"external evidence");
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
