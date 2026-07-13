use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::engine::StrategyRunId;

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

#[cfg(test)]
mod tests {
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
}
