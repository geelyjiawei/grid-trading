use std::{path::PathBuf, sync::Arc};

use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    domain::{Exchange, GridConfig, GridConfigError},
    exchange::{
        ExchangeIdentityGateway, ExecutionSnapshotGateway, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageGateway, MarketSnapshotGateway, OrderCancellationGateway,
        OrderLookupGateway, OrderPlacementGateway, PositionSnapshotGateway, TradingFeeRateGateway,
    },
    persistence::{
        StrategyCatalog, StrategyCatalogError, StrategyCatalogSelectionError,
        StrategyDiscoveryError, StrategyFilePaths, discover_strategy_files, load_strategy_catalog,
    },
};

use super::{
    FileStrategyStartError, PreparedStrategyLifecycle, PreparedStrategyStep,
    PreparedStrategyStopOutcome, RuntimeRecoveryProvider, RuntimeRegistration, RuntimeRegistry,
    RuntimeRegistryAdvanceError, RuntimeRegistryEntry, RuntimeRegistryStopError, RuntimeSettings,
    RuntimeStartupReport, StrategyRunId, prepare_leased_file_strategy,
    recover_discovered_strategies,
};

pub trait RuntimeExchangeGateway:
    Clone
    + ExchangeIdentityGateway
    + TradingFeeRateGateway
    + LeverageGateway
    + PositionSnapshotGateway
    + MarketSnapshotGateway
    + InstrumentRulesGateway
    + OrderPlacementGateway
    + OrderCancellationGateway
    + OrderLookupGateway
    + ExecutionSnapshotGateway
    + HistoricalPriceGateway
    + Send
    + Sync
    + 'static
{
}

impl<T> RuntimeExchangeGateway for T where
    T: Clone
        + ExchangeIdentityGateway
        + TradingFeeRateGateway
        + LeverageGateway
        + PositionSnapshotGateway
        + MarketSnapshotGateway
        + InstrumentRulesGateway
        + OrderPlacementGateway
        + OrderCancellationGateway
        + OrderLookupGateway
        + ExecutionSnapshotGateway
        + HistoricalPriceGateway
        + Send
        + Sync
        + 'static
{
}

pub struct RuntimeCoordinator<G> {
    strategy_root: PathBuf,
    settings: RuntimeSettings,
    registry: Arc<RuntimeRegistry<G>>,
    mutation_guard: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStartReceipt {
    pub run_id: StrategyRunId,
    pub exchange: Exchange,
    pub symbol: String,
    pub lifecycle: PreparedStrategyLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStopReceipt {
    pub run_id: StrategyRunId,
    pub exchange: Exchange,
    pub symbol: String,
    pub outcome: PreparedStrategyStopOutcome,
}

pub struct RuntimeAdvanceResult {
    pub run_id: StrategyRunId,
    pub result: Result<PreparedStrategyStep, RuntimeRegistryAdvanceError>,
}

impl<G> RuntimeCoordinator<G> {
    pub fn new(strategy_root: PathBuf, settings: RuntimeSettings) -> Self {
        Self {
            strategy_root,
            settings,
            registry: Arc::new(RuntimeRegistry::new()),
            mutation_guard: Mutex::new(()),
        }
    }

    pub fn strategy_root(&self) -> &PathBuf {
        &self.strategy_root
    }

    pub fn settings(&self) -> &RuntimeSettings {
        &self.settings
    }

    pub fn registry(&self) -> Arc<RuntimeRegistry<G>> {
        Arc::clone(&self.registry)
    }

    pub async fn entries(&self) -> Vec<RuntimeRegistryEntry> {
        self.registry.entries().await
    }

    pub async fn recover<P>(
        &self,
        provider: &P,
    ) -> Result<RuntimeStartupReport<P::Error>, RuntimeRecoveryError>
    where
        P: RuntimeRecoveryProvider<Gateway = G>,
    {
        let _guard = self.mutation_guard.lock().await;
        let root = self.strategy_root.clone();
        let discovery = tokio::task::spawn_blocking(move || discover_strategy_files(root))
            .await
            .map_err(|_| RuntimeRecoveryError::DiscoveryTask)??;
        Ok(recover_discovered_strategies(&self.registry, discovery, provider).await)
    }
}

impl<G> RuntimeCoordinator<G>
where
    G: RuntimeExchangeGateway,
{
    pub async fn start(
        &self,
        gateway: G,
        config: GridConfig,
        now_ms: u64,
    ) -> Result<RuntimeStartReceipt, RuntimeCoordinatorError> {
        config.validate()?;
        let exchange = config
            .exchange
            .ok_or(RuntimeCoordinatorError::MissingExchange)?;
        if gateway.exchange() != exchange {
            return Err(RuntimeCoordinatorError::GatewayMismatch {
                expected: exchange,
                actual: gateway.exchange(),
            });
        }
        if !has_quote_asset(&config.symbol, self.settings.quote_asset()) {
            return Err(RuntimeCoordinatorError::UnsupportedQuoteAsset {
                symbol: config.symbol,
                quote_asset: self.settings.quote_asset().to_owned(),
            });
        }

        let _guard = self.mutation_guard.lock().await;
        self.registry.prune_terminal().await;
        let catalog = self.load_catalog().await?;
        ensure_catalog_is_clean(&catalog)?;
        if let Some(existing) = catalog.select_live(exchange, &config.symbol)? {
            return Err(RuntimeCoordinatorError::MarketAlreadyOwned {
                exchange,
                symbol: config.symbol,
                owner_run_id: existing.run_id().to_owned(),
            });
        }
        if let Some(owner_run_id) = self
            .registry
            .owner_for_market(exchange, &config.symbol)
            .await
        {
            return Err(RuntimeCoordinatorError::RegistryCatalogMismatch {
                exchange,
                symbol: config.symbol,
                owner_run_id,
            });
        }

        let run_id = self.generate_run_id()?;
        let prepared = prepare_leased_file_strategy(
            gateway,
            self.strategy_root.clone(),
            run_id.clone(),
            config.clone(),
            now_ms,
            self.settings.clone(),
        )
        .await?;
        let lifecycle = prepared.lifecycle();
        match self.registry.register(prepared).await {
            RuntimeRegistration::Registered => Ok(RuntimeStartReceipt {
                run_id,
                exchange,
                symbol: config.symbol,
                lifecycle,
            }),
            RuntimeRegistration::Terminal(rejected) | RuntimeRegistration::Duplicate(rejected) => {
                drop(rejected);
                Err(RuntimeCoordinatorError::RegistrationInvariant)
            }
            RuntimeRegistration::MarketAlreadyOwned {
                owner_run_id,
                rejected,
            } => {
                drop(rejected);
                Err(RuntimeCoordinatorError::ConcurrentMarketOwner {
                    exchange,
                    symbol: config.symbol,
                    owner_run_id,
                })
            }
        }
    }

    pub async fn request_stop(
        &self,
        exchange: Exchange,
        symbol: &str,
        now_ms: u64,
    ) -> Result<RuntimeStopReceipt, RuntimeCoordinatorError> {
        let _guard = self.mutation_guard.lock().await;
        self.registry.prune_terminal().await;
        let catalog = self.load_catalog().await?;
        ensure_catalog_is_clean(&catalog)?;
        let selected = catalog.select_live(exchange, symbol)?;
        let owner = self.registry.owner_for_market(exchange, symbol).await;
        let (Some(selected), Some(run_id)) = (selected, owner) else {
            return Err(RuntimeCoordinatorError::MarketNotRunning {
                exchange,
                symbol: symbol.to_owned(),
            });
        };
        if selected.run_id() != run_id.as_str() {
            return Err(RuntimeCoordinatorError::RegistryCatalogMismatch {
                exchange,
                symbol: symbol.to_owned(),
                owner_run_id: run_id,
            });
        }
        let outcome = self.registry.request_stop(&run_id, now_ms).await?;
        Ok(RuntimeStopReceipt {
            run_id,
            exchange,
            symbol: symbol.to_owned(),
            outcome,
        })
    }

    pub async fn advance_all(&self, now_ms: u64) -> Vec<RuntimeAdvanceResult> {
        let entries = self.registry.entries().await;
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry
                .lifecycle
                .is_some_and(PreparedStrategyLifecycle::is_terminal)
            {
                continue;
            }
            results.push(RuntimeAdvanceResult {
                run_id: entry.run_id.clone(),
                result: self.registry.advance(&entry.run_id, now_ms).await,
            });
        }
        self.registry.prune_terminal().await;
        results
    }

    async fn load_catalog(&self) -> Result<StrategyCatalog, RuntimeCoordinatorError> {
        let root = self.strategy_root.clone();
        tokio::task::spawn_blocking(move || load_strategy_catalog(root))
            .await
            .map_err(|_| RuntimeCoordinatorError::CatalogTask)?
            .map_err(Into::into)
    }

    fn generate_run_id(&self) -> Result<StrategyRunId, RuntimeCoordinatorError> {
        for _ in 0..32 {
            let mut random = [0_u8; 6];
            getrandom::fill(&mut random).map_err(|_| RuntimeCoordinatorError::Entropy)?;
            let run_id = StrategyRunId::parse(hex::encode(random))
                .map_err(|_| RuntimeCoordinatorError::GeneratedRunId)?;
            let paths = StrategyFilePaths::new(self.strategy_root.clone(), run_id.clone())
                .map_err(|_| RuntimeCoordinatorError::GeneratedRunId)?;
            if !paths.directory().exists() {
                return Ok(run_id);
            }
        }
        Err(RuntimeCoordinatorError::RunIdExhausted)
    }
}

fn has_quote_asset(symbol: &str, quote_asset: &str) -> bool {
    symbol
        .strip_suffix(quote_asset)
        .is_some_and(|base| !base.is_empty())
}

fn ensure_catalog_is_clean(catalog: &StrategyCatalog) -> Result<(), RuntimeCoordinatorError> {
    if catalog.anomalies().is_empty() {
        Ok(())
    } else {
        Err(RuntimeCoordinatorError::CatalogAnomalies {
            count: catalog.anomalies().len(),
        })
    }
}

#[derive(Debug, Error)]
pub enum RuntimeRecoveryError {
    #[error("strategy discovery task failed")]
    DiscoveryTask,
    #[error(transparent)]
    Discovery(#[from] StrategyDiscoveryError),
}

#[derive(Debug, Error)]
pub enum RuntimeCoordinatorError {
    #[error(transparent)]
    InvalidConfig(#[from] GridConfigError),
    #[error("grid exchange is required")]
    MissingExchange,
    #[error("gateway belongs to {actual:?}, but the grid requires {expected:?}")]
    GatewayMismatch {
        expected: Exchange,
        actual: Exchange,
    },
    #[error("symbol {symbol} does not use configured quote asset {quote_asset}")]
    UnsupportedQuoteAsset { symbol: String, quote_asset: String },
    #[error("strategy catalog task failed")]
    CatalogTask,
    #[error(transparent)]
    Catalog(#[from] StrategyCatalogError),
    #[error("strategy catalog contains {count} unresolved anomalies")]
    CatalogAnomalies { count: usize },
    #[error(transparent)]
    CatalogSelection(#[from] StrategyCatalogSelectionError),
    #[error("{exchange:?}/{symbol} is already owned by strategy {owner_run_id}")]
    MarketAlreadyOwned {
        exchange: Exchange,
        symbol: String,
        owner_run_id: String,
    },
    #[error(
        "runtime registry and durable catalog disagree for {exchange:?}/{symbol}; registry owner is {owner_run_id:?}"
    )]
    RegistryCatalogMismatch {
        exchange: Exchange,
        symbol: String,
        owner_run_id: StrategyRunId,
    },
    #[error("another start concurrently claimed {exchange:?}/{symbol} as {owner_run_id:?}")]
    ConcurrentMarketOwner {
        exchange: Exchange,
        symbol: String,
        owner_run_id: StrategyRunId,
    },
    #[error("{exchange:?}/{symbol} is not running")]
    MarketNotRunning { exchange: Exchange, symbol: String },
    #[error("secure random generation is unavailable")]
    Entropy,
    #[error("secure run identity generation failed")]
    GeneratedRunId,
    #[error("could not generate a unique strategy run identity")]
    RunIdExhausted,
    #[error(transparent)]
    Start(#[from] FileStrategyStartError),
    #[error("prepared strategy violated the runtime registration invariant")]
    RegistrationInvariant,
    #[error(transparent)]
    Stop(#[from] RuntimeRegistryStopError),
}
