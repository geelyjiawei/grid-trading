use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    domain::Exchange,
    engine::{
        ArmedStrategyLifecycle, ArmedStrategyState, StrategyLifecycle, StrategyState,
        StrategyStateStore, validate_cross_ledger_ownership,
    },
};

use super::{
    FileOrderIntentStore, FilePreparedStrategyStore, IntentStore, StrategyDiscoveryAnomalyKind,
    StrategyDiscoveryError, discover_strategy_files,
};

#[derive(Debug, Clone, PartialEq)]
pub enum StrategyCatalogSnapshot {
    Armed(Box<ArmedStrategyState>),
    Active(Box<StrategyState>),
}

impl StrategyCatalogSnapshot {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Armed(state) => state.exchange,
            Self::Active(state) => state.exchange,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Armed(state) => &state.symbol,
            Self::Active(state) => &state.symbol,
        }
    }

    pub fn run_id(&self) -> &str {
        match self {
            Self::Armed(state) => state.run_id.as_str(),
            Self::Active(state) => state.run_id.as_str(),
        }
    }

    pub fn is_live(&self) -> bool {
        match self {
            Self::Armed(state) => state.lifecycle == ArmedStrategyLifecycle::WaitingTrigger,
            Self::Active(state) => !matches!(
                state.lifecycle,
                StrategyLifecycle::Stopped | StrategyLifecycle::Closed
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyCatalogAnomaly {
    pub path: PathBuf,
    pub kind: StrategyCatalogAnomalyKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyCatalogAnomalyKind {
    Discovery(StrategyDiscoveryAnomalyKind),
    StateLoadFailed,
    RunIdentityMismatch,
    IntentLedgerLoadFailed,
    IntentLedgerMismatch,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyCatalog {
    entries: Vec<StrategyCatalogSnapshot>,
    anomalies: Vec<StrategyCatalogAnomaly>,
}

impl StrategyCatalog {
    pub fn entries(&self) -> &[StrategyCatalogSnapshot] {
        &self.entries
    }

    pub fn anomalies(&self) -> &[StrategyCatalogAnomaly] {
        &self.anomalies
    }

    pub fn select_live(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Option<StrategyCatalogSnapshot>, StrategyCatalogSelectionError> {
        let matches = self
            .entries
            .iter()
            .filter(|entry| {
                entry.is_live() && entry.exchange() == exchange && entry.symbol() == symbol
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Ok(None),
            [entry] => Ok(Some((*entry).clone())),
            _ => Err(StrategyCatalogSelectionError::MultipleLiveStrategies {
                run_ids: matches
                    .iter()
                    .map(|entry| entry.run_id().to_owned())
                    .collect(),
            }),
        }
    }
}

pub fn load_strategy_catalog(
    root: impl AsRef<Path>,
) -> Result<StrategyCatalog, StrategyCatalogError> {
    let discovery = discover_strategy_files(root.as_ref().to_path_buf())?;
    let mut anomalies = discovery
        .anomalies
        .into_iter()
        .map(|anomaly| StrategyCatalogAnomaly {
            path: anomaly.path,
            kind: StrategyCatalogAnomalyKind::Discovery(anomaly.kind),
        })
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(discovery.strategies.len());

    for paths in discovery.strategies {
        let snapshot = match FilePreparedStrategyStore::load(paths.state()) {
            Ok(FilePreparedStrategyStore::Armed(store)) => {
                StrategyCatalogSnapshot::Armed(Box::new(store.snapshot().clone()))
            }
            Ok(FilePreparedStrategyStore::Active(store)) => {
                StrategyCatalogSnapshot::Active(Box::new(store.snapshot().clone()))
            }
            Err(error) => {
                tracing::warn!(path = %paths.state().display(), error = %error, "strategy state catalog entry could not be loaded");
                anomalies.push(StrategyCatalogAnomaly {
                    path: paths.state().to_path_buf(),
                    kind: StrategyCatalogAnomalyKind::StateLoadFailed,
                });
                continue;
            }
        };
        if snapshot.run_id() != paths.run_id().as_str() {
            anomalies.push(StrategyCatalogAnomaly {
                path: paths.state().to_path_buf(),
                kind: StrategyCatalogAnomalyKind::RunIdentityMismatch,
            });
            continue;
        }
        let intent_store = match FileOrderIntentStore::load(paths.intents()) {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(path = %paths.intents().display(), error = %error, "strategy intent catalog entry could not be loaded");
                anomalies.push(StrategyCatalogAnomaly {
                    path: paths.intents().to_path_buf(),
                    kind: StrategyCatalogAnomalyKind::IntentLedgerLoadFailed,
                });
                continue;
            }
        };
        let ledger_matches = match &snapshot {
            StrategyCatalogSnapshot::Armed(_) => {
                intent_store.snapshot().intents.is_empty()
                    && intent_store.snapshot().cancellations.is_empty()
            }
            StrategyCatalogSnapshot::Active(state) => {
                validate_cross_ledger_ownership(state, intent_store.snapshot()).is_ok()
            }
        };
        if !ledger_matches {
            anomalies.push(StrategyCatalogAnomaly {
                path: paths.intents().to_path_buf(),
                kind: StrategyCatalogAnomalyKind::IntentLedgerMismatch,
            });
            continue;
        }
        entries.push(snapshot);
    }

    entries.sort_by(|left, right| left.run_id().cmp(right.run_id()));
    anomalies.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(StrategyCatalog { entries, anomalies })
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StrategyCatalogSelectionError {
    #[error("multiple live strategy states exist for one exchange and symbol")]
    MultipleLiveStrategies { run_ids: Vec<String> },
}

#[derive(Debug, Error)]
pub enum StrategyCatalogError {
    #[error(transparent)]
    Discovery(#[from] StrategyDiscoveryError),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            Direction, GridConfig, GridMode, InitialOrderType, InstrumentRules, IntentState,
            PositionSizingMode, QuantityRules,
        },
        engine::{
            MarketSnapshot, PositionBaseline, StrategyOrderPurpose, StrategyOrderTracking,
            StrategyRunId, build_grid_plan,
        },
        persistence::{FileArmedStrategyStateStore, FileStrategyStateStore, StrategyFilePaths},
    };

    fn rules() -> InstrumentRules {
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

    fn config(symbol: &str, trigger_price: Option<Decimal>) -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Aster),
            symbol: symbol.into(),
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
            initial_order_price: trigger_price.is_none().then_some(Decimal::new(40, 2)),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn active(run_id: &str, symbol: &str) -> StrategyState {
        let config = config(symbol, None);
        let rules = rules();
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
            StrategyRunId::parse(run_id).unwrap(),
            config,
            rules,
            plan,
            PositionBaseline::flat(),
            100,
        )
        .unwrap()
    }

    fn persist_active(root: &Path, state: StrategyState) {
        let paths = StrategyFilePaths::new(root, state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
    }

    #[test]
    fn exact_live_strategy_is_selected_and_unrelated_symbols_are_ignored() {
        let directory = tempdir().unwrap();
        persist_active(directory.path(), active("ASTER001", "ANSEMUSDT"));
        persist_active(directory.path(), active("ASTER002", "MUUSDT"));

        let catalog = load_strategy_catalog(directory.path()).unwrap();
        let selected = catalog
            .select_live(Exchange::Aster, "ANSEMUSDT")
            .unwrap()
            .unwrap();

        assert_eq!(selected.run_id(), "ASTER001");
        assert!(catalog.anomalies().is_empty());
    }

    #[test]
    fn cancelled_armed_strategy_is_not_live() {
        let directory = tempdir().unwrap();
        let config = config("ANSEMUSDT", Some(Decimal::new(405, 3)));
        let state = ArmedStrategyState::new(
            StrategyRunId::parse("ARMED001").unwrap(),
            config,
            &MarketSnapshot {
                last_price: Decimal::new(40, 2),
                mark_price: Decimal::new(40, 2),
            },
            100,
        )
        .unwrap();
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        let mut store = FileArmedStrategyStateStore::create(paths.state(), state).unwrap();
        store.cancel(101).unwrap();

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert!(
            catalog
                .select_live(Exchange::Aster, "ANSEMUSDT")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn duplicate_live_states_fail_closed_instead_of_selecting_arbitrarily() {
        let directory = tempdir().unwrap();
        persist_active(directory.path(), active("ASTER001", "ANSEMUSDT"));
        persist_active(directory.path(), active("ASTER002", "ANSEMUSDT"));

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert_eq!(
            catalog.select_live(Exchange::Aster, "ANSEMUSDT"),
            Err(StrategyCatalogSelectionError::MultipleLiveStrategies {
                run_ids: vec!["ASTER001".into(), "ASTER002".into()],
            })
        );
    }

    #[test]
    fn malformed_state_is_retained_as_a_catalog_anomaly() {
        let directory = tempdir().unwrap();
        let run = StrategyRunId::parse("BROKEN01").unwrap();
        let paths = StrategyFilePaths::new(directory.path(), run).unwrap();
        fs::create_dir_all(paths.directory()).unwrap();
        fs::write(paths.state(), b"{").unwrap();

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert!(catalog.entries().is_empty());
        assert_eq!(catalog.anomalies().len(), 1);
        assert_eq!(
            catalog.anomalies()[0].kind,
            StrategyCatalogAnomalyKind::StateLoadFailed
        );
    }

    #[test]
    fn directory_and_state_run_identity_mismatch_is_never_loaded() {
        let directory = tempdir().unwrap();
        let state = active("ASTER001", "ANSEMUSDT");
        let paths =
            StrategyFilePaths::new(directory.path(), StrategyRunId::parse("ASTER002").unwrap())
                .unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert!(catalog.entries().is_empty());
        assert_eq!(catalog.anomalies().len(), 1);
        assert_eq!(
            catalog.anomalies()[0].kind,
            StrategyCatalogAnomalyKind::RunIdentityMismatch
        );
    }

    #[test]
    fn malformed_intent_ledger_is_retained_as_a_catalog_anomaly() {
        let directory = tempdir().unwrap();
        let state = active("ASTER001", "ANSEMUSDT");
        let paths = StrategyFilePaths::new(directory.path(), state.run_id.clone()).unwrap();
        FileStrategyStateStore::create(paths.state(), state).unwrap();
        fs::write(paths.intents(), b"{").unwrap();

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert!(catalog.entries().is_empty());
        assert_eq!(catalog.anomalies().len(), 1);
        assert_eq!(
            catalog.anomalies()[0].kind,
            StrategyCatalogAnomalyKind::IntentLedgerLoadFailed
        );
    }

    #[test]
    fn strategy_and_intent_ledger_mismatch_is_never_selected() {
        let directory = tempdir().unwrap();
        let mut state = active("ASTER001", "ANSEMUSDT");
        let order = state
            .orders
            .values_mut()
            .find(|order| order.purpose == StrategyOrderPurpose::Opening)
            .unwrap();
        order.tracking = StrategyOrderTracking::Intent {
            state: IntentState::Accepted {
                exchange_order_id: "accepted-without-ledger".into(),
            },
        };
        order.exchange_order_id = Some("accepted-without-ledger".into());
        state.validate().unwrap();
        persist_active(directory.path(), state);

        let catalog = load_strategy_catalog(directory.path()).unwrap();

        assert!(catalog.entries().is_empty());
        assert_eq!(catalog.anomalies().len(), 1);
        assert_eq!(
            catalog.anomalies()[0].kind,
            StrategyCatalogAnomalyKind::IntentLedgerMismatch
        );
    }
}
