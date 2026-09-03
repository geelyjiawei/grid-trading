pub mod api;
pub mod domain;
pub mod engine;
pub mod exchange;
pub mod persistence;
pub mod security;
pub mod web_auth;

use std::{
    collections::{BTreeSet, HashMap},
    env, fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::Router;
use thiserror::Error;
use tower_http::{services::ServeDir, trace::TraceLayer};
use zeroize::Zeroizing;

use crate::web_auth::{WebAuthConfiguration, WebAuthConfigurationError, WebAuthService};
use crate::{
    domain::Exchange,
    engine::{
        PreparedStrategyStep, RuntimeBuildError, RuntimeCoordinator, RuntimeRecoveryError,
        RuntimeRecoveryProvider, RuntimeSettings, SubmissionResult,
    },
    exchange::{
        aster::LocalEip712Signer,
        configured::{
            ExchangeCredentials, ExchangeEnvironment, ExchangeGatewayBuildError,
            ExchangeGatewayFactory, SharedConfiguredExchangeGateway,
        },
        realtime::{ExecutionWakeup, subscribe_execution_wakeups},
        registry::{ExchangeGatewayRegistry, RegistryError},
    },
    persistence::{
        EncryptedExchangeConfigStore, ExchangeConfigStoreError, StoredExchangeConfiguration,
    },
    security::{AdminTokenError, AdminTokenVerifier},
};

const DEFAULT_CONTROL_ROOT: &str = "/app/data/rust-control/idempotency";
const DEFAULT_STRATEGY_ROOT: &str = "/app/data/rust-control/strategies";
const DEFAULT_WEB_ROOT: &str = "/app/web";
const DEFAULT_CONFIG_FILE: &str = "/app/data/api_config.json";
const DEFAULT_RUNTIME_TICK_MS: u64 = 1_000;
const TRADE_XYZ_RUNTIME_AUDIT_TICK_MS: u64 = 5_000;
const DEFAULT_MARKET_MAX_AGE_MS: u64 = 15_000;
const DEFAULT_MARKET_FUTURE_SKEW_MS: u64 = 1_000;
const DEFAULT_SUBMISSIONS_PER_TICK: usize = 100;
const EXECUTION_EVENT_RETRY_DELAY: Duration = Duration::from_millis(125);
const EXECUTION_WAKEUP_BATCH_WINDOW: Duration = Duration::from_millis(2);
const MAX_EXECUTION_WAKEUPS_PER_BATCH: usize = 256;
const MAX_CONCURRENT_EXECUTION_DISPATCHES_PER_MARKET: usize = 4;
const REPLACEMENT_ACK_SLO_MS: u64 = 1_000;

pub fn app() -> Router {
    build_app(
        None,
        WebAuthService::disabled(),
        ExchangeGatewayRegistry::default(),
        None,
        PathBuf::from(DEFAULT_CONTROL_ROOT),
        PathBuf::from(DEFAULT_STRATEGY_ROOT),
        PathBuf::from(DEFAULT_WEB_ROOT),
        None,
    )
}

pub async fn app_from_environment() -> Result<Router, AppConfigurationError> {
    let trading_enabled = parse_env_flag("GRID_RUST_TRADING_ENABLED")?;

    let admin_token = match env::var("GRID_RUST_ADMIN_TOKEN") {
        Ok(secret) => optional_admin_token(secret)?,
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeAdminToken);
        }
    };
    let web_authentication = WebAuthService::from_configuration(WebAuthConfiguration {
        required: parse_env_flag("AUTH_REQUIRED")?,
        username: read_env_text("ADMIN_USERNAME")?.unwrap_or_else(|| "admin".to_owned()),
        password_hash: Zeroizing::new(read_env_text("ADMIN_PASSWORD_HASH")?.unwrap_or_default()),
        totp_secret: Zeroizing::new(read_env_text("TOTP_SECRET")?.unwrap_or_default()),
        cookie_secure: parse_env_flag("AUTH_COOKIE_SECURE")?,
    })?;
    if trading_enabled && (!web_authentication.required() || !web_authentication.configured()) {
        return Err(AppConfigurationError::TradingRequiresWebAuthentication);
    }
    let control_root = match env::var("GRID_RUST_CONTROL_ROOT") {
        Ok(value) if value.trim().is_empty() => {
            return Err(AppConfigurationError::EmptyControlRoot);
        }
        Ok(value) => PathBuf::from(value),
        Err(env::VarError::NotPresent) => PathBuf::from(DEFAULT_CONTROL_ROOT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeControlRoot);
        }
    };
    if !control_root.is_absolute() {
        return Err(AppConfigurationError::RelativeControlRoot);
    }
    let strategy_root = match env::var("GRID_RUST_STRATEGY_ROOT") {
        Ok(value) if value.trim().is_empty() => {
            return Err(AppConfigurationError::EmptyStrategyRoot);
        }
        Ok(value) => PathBuf::from(value),
        Err(env::VarError::NotPresent) => PathBuf::from(DEFAULT_STRATEGY_ROOT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeStrategyRoot);
        }
    };
    if !strategy_root.is_absolute() {
        return Err(AppConfigurationError::RelativeStrategyRoot);
    }
    let web_root = match env::var("GRID_WEB_ROOT") {
        Ok(value) if value.trim().is_empty() => return Err(AppConfigurationError::EmptyWebRoot),
        Ok(value) => PathBuf::from(value),
        Err(env::VarError::NotPresent) => PathBuf::from(DEFAULT_WEB_ROOT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::NonUnicodeWebRoot);
        }
    };
    if !web_root.is_absolute() {
        return Err(AppConfigurationError::RelativeWebRoot);
    }
    let exchange_config_store = exchange_config_store_from_environment()?;
    let exchange_gateways = exchange_gateways_from_environment()?;
    if let Some(store) = &exchange_config_store {
        register_stored_exchange_gateways(&exchange_gateways, store)?;
    }
    exchange_gateways.select_configured_preferred();
    let runtime = if trading_enabled {
        let settings = RuntimeSettings::new(
            "USDT",
            DEFAULT_MARKET_MAX_AGE_MS,
            DEFAULT_MARKET_FUTURE_SKEW_MS,
            DEFAULT_SUBMISSIONS_PER_TICK,
        )?;
        let runtime = Arc::new(RuntimeCoordinator::new(
            strategy_root.clone(),
            settings.clone(),
        ));
        let provider = ConfiguredRuntimeProvider {
            exchange_gateways: exchange_gateways.clone(),
            settings,
        };
        let report = runtime.recover(&provider).await?;
        if !report.discovery_anomalies.is_empty() || !report.failures.is_empty() {
            return Err(AppConfigurationError::UnsafeRuntimeRecovery {
                anomaly_count: report.discovery_anomalies.len(),
                failure_count: report.failures.len(),
            });
        }
        tracing::info!(
            recovered = report.registered.len(),
            skipped_terminal = report.skipped_terminal.len(),
            "Rust trading runtime recovery completed"
        );
        spawn_runtime_scheduler(Arc::clone(&runtime));
        Some(runtime)
    } else {
        None
    };
    Ok(build_app(
        admin_token,
        web_authentication,
        exchange_gateways,
        exchange_config_store,
        control_root,
        strategy_root,
        web_root,
        runtime,
    ))
}

fn optional_admin_token(secret: String) -> Result<Option<AdminTokenVerifier>, AdminTokenError> {
    if secret.is_empty() {
        return Ok(None);
    }
    AdminTokenVerifier::from_secret(Zeroizing::new(secret)).map(Some)
}

#[allow(clippy::too_many_arguments)]
fn build_app(
    admin_token: Option<AdminTokenVerifier>,
    web_authentication: WebAuthService,
    exchange_gateways: ExchangeGatewayRegistry,
    exchange_config_store: Option<EncryptedExchangeConfigStore>,
    control_root: PathBuf,
    strategy_root: PathBuf,
    web_root: PathBuf,
    runtime: Option<Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>>,
) -> Router {
    Router::new()
        .merge(api::router(
            admin_token,
            web_authentication,
            exchange_gateways,
            exchange_config_store,
            control_root,
            strategy_root,
            runtime,
        ))
        .fallback_service(ServeDir::new(web_root))
        .layer(TraceLayer::new_for_http())
}

#[derive(Clone)]
struct ConfiguredRuntimeProvider {
    exchange_gateways: ExchangeGatewayRegistry,
    settings: RuntimeSettings,
}

impl RuntimeRecoveryProvider for ConfiguredRuntimeProvider {
    type Gateway = SharedConfiguredExchangeGateway;
    type Error = RegistryError;

    fn runtime_for(
        &self,
        exchange: Exchange,
        _run_id: &engine::StrategyRunId,
    ) -> Result<(Self::Gateway, RuntimeSettings), Self::Error> {
        Ok((
            self.exchange_gateways.trading_gateway(exchange)?,
            self.settings.clone(),
        ))
    }
}

fn spawn_runtime_scheduler(runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(DEFAULT_RUNTIME_TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut trade_xyz_audit_ticker =
            tokio::time::interval(Duration::from_millis(TRADE_XYZ_RUNTIME_AUDIT_TICK_MS));
        trade_xyz_audit_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut execution_wakeups = subscribe_execution_wakeups();
        let (execution_completion_sender, mut execution_completions) =
            tokio::sync::mpsc::unbounded_channel();
        let mut execution_dispatches = HashMap::<String, ExecutionDispatchState>::new();
        let full_tick_in_flight = Arc::new(AtomicBool::new(false));
        let trade_xyz_audit_in_flight = Arc::new(AtomicBool::new(false));
        loop {
            tokio::select! {
                biased;
                event = execution_wakeups.recv() => {
                    match event {
                        Ok(event) => {
                            let mut wakeup_batch = HashMap::new();
                            merge_execution_wakeup(&mut wakeup_batch, event);
                            // Hyperliquid can publish several order updates from one match
                            // back-to-back. A tiny window lets one authoritative snapshot
                            // materialize every corresponding replacement in one exchange action.
                            tokio::time::sleep(EXECUTION_WAKEUP_BATCH_WINDOW).await;
                            while wakeup_batch.values().map(|pending| pending.event_count).sum::<usize>()
                                < MAX_EXECUTION_WAKEUPS_PER_BATCH
                            {
                                match execution_wakeups.try_recv() {
                                    Ok(event) => merge_execution_wakeup(&mut wakeup_batch, event),
                                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                                        tracing::warn!(skipped, "execution wakeup receiver lagged; REST fallback remains active");
                                    }
                                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return,
                                }
                            }
                            for (key, pending) in wakeup_batch {
                                queue_execution_wakeups(
                                    &runtime,
                                    &mut execution_dispatches,
                                    &execution_completion_sender,
                                    key,
                                    pending,
                                );
                            }
                            // Let the execution task register its per-strategy waiter before a
                            // simultaneously-ready full tick can claim the same strategy.
                            tokio::task::yield_now().await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "execution wakeup receiver lagged; REST fallback remains active");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
                Some(key) = execution_completions.recv() => {
                    complete_execution_dispatch(
                        &runtime,
                        &mut execution_dispatches,
                        &execution_completion_sender,
                        key,
                    );
                }
                _ = ticker.tick() => {
                    let Some(tick_lease) = RuntimeTickLease::try_acquire(&full_tick_in_flight) else {
                        continue;
                    };
                    let Some(now_ms) = system_time_ms() else {
                        tracing::error!("system clock is unavailable; runtime tick skipped");
                        continue;
                    };
                    let runtime = Arc::clone(&runtime);
                    tokio::spawn(async move {
                        let _tick_lease = tick_lease;
                        for advance in runtime.advance_excluding_exchange(Exchange::TradeXyz, now_ms).await {
                            log_runtime_advance(advance, false, None, None, 0);
                        }
                    });
                }
                _ = trade_xyz_audit_ticker.tick() => {
                    let Some(tick_lease) = RuntimeTickLease::try_acquire(&trade_xyz_audit_in_flight) else {
                        continue;
                    };
                    let Some(now_ms) = system_time_ms() else {
                        tracing::error!("system clock is unavailable; TRADE.XYZ audit tick skipped");
                        continue;
                    };
                    let runtime = Arc::clone(&runtime);
                    tokio::spawn(async move {
                        let _tick_lease = tick_lease;
                        for advance in runtime.advance_exchange(Exchange::TradeXyz, now_ms).await {
                            log_runtime_advance(advance, false, None, None, 0);
                        }
                    });
                }
            }
        }
    });
}

#[derive(Debug, Default)]
struct ExecutionDispatchState {
    in_flight: usize,
    pending: Option<PendingExecutionWakeups>,
}

impl ExecutionDispatchState {
    fn dispatch_or_queue(
        &mut self,
        pending: PendingExecutionWakeups,
    ) -> Option<PendingExecutionWakeups> {
        let maximum_concurrency = execution_dispatch_concurrency(&pending);
        if self.in_flight < maximum_concurrency {
            self.in_flight += 1;
            return Some(pending);
        }
        match &mut self.pending {
            Some(existing) => existing.merge_pending(pending),
            None => self.pending = Some(pending),
        }
        None
    }

    fn complete(&mut self) -> Option<PendingExecutionWakeups> {
        debug_assert!(self.in_flight > 0);
        self.in_flight = self.in_flight.saturating_sub(1);
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| self.in_flight < execution_dispatch_concurrency(pending))
        {
            self.in_flight += 1;
            return self.pending.take();
        }
        None
    }

    fn is_idle(&self) -> bool {
        self.in_flight == 0 && self.pending.is_none()
    }
}

fn execution_dispatch_concurrency(pending: &PendingExecutionWakeups) -> usize {
    match pending.event.exchange {
        Exchange::TradeXyz if !pending.requires_full_scan => {
            MAX_CONCURRENT_EXECUTION_DISPATCHES_PER_MARKET
        }
        Exchange::Binance | Exchange::Aster | Exchange::Bybit | Exchange::TradeXyz => 1,
    }
}

#[derive(Debug)]
struct PendingExecutionWakeups {
    event: ExecutionWakeup,
    event_count: usize,
    exchange_order_ids: BTreeSet<String>,
    requires_full_scan: bool,
}

impl PendingExecutionWakeups {
    fn new(event: ExecutionWakeup) -> Self {
        let exchange_order_ids = event.exchange_order_id.iter().cloned().collect();
        let requires_full_scan = event.exchange_order_id.is_none();
        Self {
            event,
            event_count: 1,
            exchange_order_ids,
            requires_full_scan,
        }
    }

    fn merge(&mut self, event: ExecutionWakeup) {
        self.event_count = self.event_count.saturating_add(1);
        self.merge_event(event);
    }

    fn merge_pending(&mut self, pending: Self) {
        self.event_count = self.event_count.saturating_add(pending.event_count);
        self.exchange_order_ids.extend(pending.exchange_order_ids);
        self.requires_full_scan |= pending.requires_full_scan;
        self.merge_event_metadata(pending.event);
    }

    fn merge_event(&mut self, event: ExecutionWakeup) {
        match &event.exchange_order_id {
            Some(exchange_order_id) => {
                self.exchange_order_ids.insert(exchange_order_id.clone());
            }
            None => self.requires_full_scan = true,
        }
        self.merge_event_metadata(event);
    }

    fn merge_event_metadata(&mut self, event: ExecutionWakeup) {
        debug_assert_eq!(self.event.exchange, event.exchange);
        debug_assert_eq!(self.event.symbol, event.symbol);
        self.event.observed_at_ms = self.event.observed_at_ms.min(event.observed_at_ms);
        self.event.exchange_event_time_ms = match (
            self.event.exchange_event_time_ms,
            event.exchange_event_time_ms,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
    }
}

struct ExecutionDispatchCompletion {
    sender: tokio::sync::mpsc::UnboundedSender<String>,
    key: Option<String>,
}

impl ExecutionDispatchCompletion {
    fn new(sender: tokio::sync::mpsc::UnboundedSender<String>, key: String) -> Self {
        Self {
            sender,
            key: Some(key),
        }
    }
}

impl Drop for ExecutionDispatchCompletion {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let _ = self.sender.send(key);
        }
    }
}

fn execution_dispatch_key(event: &ExecutionWakeup) -> String {
    format!("{:?}:{}", event.exchange, event.symbol)
}

fn merge_execution_wakeup(
    wakeups: &mut HashMap<String, PendingExecutionWakeups>,
    event: ExecutionWakeup,
) {
    let key = execution_dispatch_key(&event);
    match wakeups.get_mut(&key) {
        Some(pending) => pending.merge(event),
        None => {
            wakeups.insert(key, PendingExecutionWakeups::new(event));
        }
    }
}

fn queue_execution_wakeups(
    runtime: &Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    dispatches: &mut HashMap<String, ExecutionDispatchState>,
    completion_sender: &tokio::sync::mpsc::UnboundedSender<String>,
    key: String,
    pending: PendingExecutionWakeups,
) {
    let dispatch = dispatches.entry(key.clone()).or_default();
    let Some(pending) = dispatch.dispatch_or_queue(pending) else {
        return;
    };
    spawn_execution_dispatch(Arc::clone(runtime), completion_sender.clone(), key, pending);
}

fn complete_execution_dispatch(
    runtime: &Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    dispatches: &mut HashMap<String, ExecutionDispatchState>,
    completion_sender: &tokio::sync::mpsc::UnboundedSender<String>,
    key: String,
) {
    let Some(dispatch) = dispatches.get_mut(&key) else {
        return;
    };
    let pending = dispatch.complete();
    let remove = dispatch.is_idle();
    if remove {
        dispatches.remove(&key);
    }
    if let Some(pending) = pending {
        spawn_execution_dispatch(Arc::clone(runtime), completion_sender.clone(), key, pending);
    }
}

fn spawn_execution_dispatch(
    runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    completion_sender: tokio::sync::mpsc::UnboundedSender<String>,
    key: String,
    pending: PendingExecutionWakeups,
) {
    tokio::spawn(async move {
        let _completion = ExecutionDispatchCompletion::new(completion_sender, key);
        advance_execution_events(runtime, pending).await;
    });
}

struct RuntimeTickLease(Arc<AtomicBool>);

impl RuntimeTickLease {
    fn try_acquire(in_flight: &Arc<AtomicBool>) -> Option<Self> {
        in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self(Arc::clone(in_flight)))
    }
}

impl Drop for RuntimeTickLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn advance_execution_events(
    runtime: Arc<RuntimeCoordinator<SharedConfiguredExchangeGateway>>,
    pending: PendingExecutionWakeups,
) {
    let PendingExecutionWakeups {
        event,
        event_count: coalesced_events,
        exchange_order_ids,
        requires_full_scan,
    } = pending;
    let exchange_order_ids = exchange_order_ids.into_iter().collect::<Vec<_>>();
    let targeted_order_ids = (!requires_full_scan).then_some(exchange_order_ids.as_slice());
    let Some(now_ms) = system_time_ms() else {
        tracing::error!("system clock is unavailable; execution wakeup skipped");
        return;
    };
    let event_queue_ms = now_ms.checked_sub(event.observed_at_ms);
    let Some(advance) = runtime
        .advance_execution_events(event.exchange, &event.symbol, targeted_order_ids, now_ms)
        .await
    else {
        return;
    };
    let retry = matches!(
        &advance.result,
        Ok(PreparedStrategyStep::Active(report)) if execution_event_needs_retry(report)
    );
    log_runtime_advance(
        advance,
        true,
        Some(&event),
        event_queue_ms,
        coalesced_events,
    );
    if !retry {
        return;
    }
    tokio::time::sleep(EXECUTION_EVENT_RETRY_DELAY).await;
    let Some(now_ms) = system_time_ms() else {
        return;
    };
    if let Some(retry) = runtime
        .advance_execution_events(event.exchange, &event.symbol, targeted_order_ids, now_ms)
        .await
    {
        log_runtime_advance(retry, true, Some(&event), event_queue_ms, coalesced_events);
    }
}

fn execution_event_needs_retry(report: &crate::engine::RuntimeTickReport) -> bool {
    report.submissions.is_empty() && report.execution_syncs > 0 && !report.is_blocked()
}

fn log_runtime_advance(
    advance: engine::RuntimeAdvanceResult,
    execution_event: bool,
    event: Option<&ExecutionWakeup>,
    event_queue_ms: Option<u64>,
    coalesced_events: usize,
) {
    match advance.result {
        Ok(PreparedStrategyStep::WaitingForTrigger) => {}
        Ok(PreparedStrategyStep::Activated) => {
            tracing::info!(run_id = advance.run_id.as_str(), "strategy activated");
        }
        Ok(PreparedStrategyStep::Active(report)) => {
            if execution_event {
                let completed_at_ms = system_time_ms();
                let event_to_submit_ms =
                    event.and_then(|event| completed_at_ms?.checked_sub(event.observed_at_ms));
                let exchange_to_observed_ms = event.and_then(|event| {
                    event
                        .exchange_event_time_ms
                        .and_then(|exchange_time| event.observed_at_ms.checked_sub(exchange_time))
                });
                let exchange_to_submit_ms = event.and_then(|event| {
                    event
                        .exchange_event_time_ms
                        .and_then(|exchange_time| completed_at_ms?.checked_sub(exchange_time))
                });
                let accepted_submissions = report
                    .submissions
                    .iter()
                    .filter(|submission| {
                        matches!(&submission.result, SubmissionResult::Accepted { .. })
                    })
                    .count();
                tracing::info!(
                    run_id = advance.run_id.as_str(),
                    exchange = ?event.map(|event| event.exchange),
                    symbol = event.map(|event| event.symbol.as_str()).unwrap_or("-"),
                    execution_syncs = report.execution_syncs,
                    submissions = report.submissions.len(),
                    accepted_submissions,
                    event_to_submit_ms,
                    exchange_to_observed_ms,
                    exchange_to_submit_ms,
                    event_queue_ms,
                    coalesced_events,
                    "execution WebSocket wakeup processed"
                );
                if accepted_submissions > 0
                    && event_to_submit_ms.is_some_and(|elapsed| elapsed > REPLACEMENT_ACK_SLO_MS)
                {
                    tracing::warn!(
                        run_id = advance.run_id.as_str(),
                        exchange = ?event.map(|event| event.exchange),
                        symbol = event.map(|event| event.symbol.as_str()).unwrap_or("-"),
                        event_to_submit_ms,
                        replacement_ack_slo_ms = REPLACEMENT_ACK_SLO_MS,
                        "replacement acknowledgement exceeded latency SLO"
                    );
                }
            }
            if report.is_blocked() {
                let representative = report
                    .blockers
                    .first()
                    .expect("a blocked report must contain a blocker");
                tracing::warn!(
                    run_id = advance.run_id.as_str(),
                    blocker_count = report.blockers.len(),
                    blocker_stage = ?representative.stage,
                    client_order_id = representative
                        .client_order_id
                        .as_ref()
                        .map(|client_order_id| client_order_id.as_str())
                        .unwrap_or("-"),
                    blocker_message = representative.message.as_str(),
                    "strategy tick is blocked pending authoritative reconciliation"
                );
            }
        }
        Err(error) => {
            tracing::error!(
                run_id = advance.run_id.as_str(),
                error = %error,
                execution_event,
                "strategy tick failed closed"
            );
        }
    }
}

fn system_time_ms() -> Option<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

fn exchange_gateways_from_environment() -> Result<ExchangeGatewayRegistry, AppConfigurationError> {
    let preferred = preferred_exchange_from_environment()?;
    let mut registry = ExchangeGatewayRegistry::empty(preferred);

    if let Some((api_key, api_secret)) =
        read_credential_pair("BINANCE_API_KEY", "BINANCE_API_SECRET", "Binance")?
    {
        let masked = mask_identifier(&api_key);
        let environment = exchange_environment("BINANCE_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::binance(api_key, api_secret)?)?;
        registry.register_configured(gateway, environment, "env", Some(masked))?;
    }

    if let Some(private_key) = read_aster_private_key()? {
        let environment = exchange_environment("ASTER_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::aster(private_key)?)?;
        registry.register_configured(
            gateway,
            environment,
            "env",
            Some("wallet configured".into()),
        )?;
    }

    if let Some((api_key, api_secret)) =
        read_credential_pair("BYBIT_API_KEY", "BYBIT_API_SECRET", "Bybit")?
    {
        let masked = mask_identifier(&api_key);
        let environment = exchange_environment("BYBIT_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?
            .build(ExchangeCredentials::bybit(api_key, api_secret)?)?;
        registry.register_configured(gateway, environment, "env", Some(masked))?;
    }

    if let Some((account_address, agent_private_key)) = read_credential_pair(
        "TRADE_XYZ_ACCOUNT_ADDRESS",
        "TRADE_XYZ_AGENT_PRIVATE_KEY",
        "TRADE.XYZ",
    )? {
        let masked = mask_identifier(&account_address);
        let environment = exchange_environment("TRADE_XYZ_TESTNET")?;
        let gateway = ExchangeGatewayFactory::standard(environment)?.build(
            ExchangeCredentials::trade_xyz(account_address, agent_private_key)?,
        )?;
        registry.register_configured(gateway, environment, "env", Some(masked))?;
    }

    Ok(registry)
}

fn exchange_config_store_from_environment()
-> Result<Option<EncryptedExchangeConfigStore>, AppConfigurationError> {
    let path = read_nonempty_env_text("GRID_CONFIG_FILE")?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE));
    let Some(master_key) = read_nonempty_env_text("GRID_CONFIG_KEY")? else {
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(AppConfigurationError::MissingExchangeConfigKey),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(AppConfigurationError::InspectExchangeConfig(error)),
        }
    };
    let salt = read_nonempty_env_text("GRID_CONFIG_SALT")?.map(Zeroizing::new);
    EncryptedExchangeConfigStore::new(path, Zeroizing::new(master_key), salt)
        .map(Some)
        .map_err(AppConfigurationError::from)
}

fn register_stored_exchange_gateways(
    registry: &ExchangeGatewayRegistry,
    store: &EncryptedExchangeConfigStore,
) -> Result<(), AppConfigurationError> {
    for configuration in store.load()? {
        let environment = if configuration.testnet() {
            ExchangeEnvironment::Testnet
        } else {
            ExchangeEnvironment::Production
        };
        let masked_identifier = Some(mask_identifier(configuration.api_key()));
        if configuration.exchange() == Exchange::Aster {
            let signer = LocalEip712Signer::from_private_key(configuration.api_secret())
                .map_err(ExchangeGatewayBuildError::from)?;
            if !signer
                .signer_address()
                .eq_ignore_ascii_case(configuration.api_key())
            {
                return Err(AppConfigurationError::AsterStoredWalletMismatch);
            }
        }
        let credentials = credentials_from_stored_configuration(&configuration)?;
        let gateway = ExchangeGatewayFactory::standard(environment)?.build(credentials)?;
        registry.replace_configured(gateway, environment, "file", masked_identifier)?;
    }
    Ok(())
}

fn credentials_from_stored_configuration(
    configuration: &StoredExchangeConfiguration,
) -> Result<ExchangeCredentials, AppConfigurationError> {
    let credentials = match configuration.exchange() {
        Exchange::Binance => ExchangeCredentials::binance(
            configuration.api_key().to_owned(),
            configuration.api_secret().to_owned(),
        )?,
        Exchange::Aster => ExchangeCredentials::aster(configuration.api_secret().to_owned())?,
        Exchange::Bybit => ExchangeCredentials::bybit(
            configuration.api_key().to_owned(),
            configuration.api_secret().to_owned(),
        )?,
        Exchange::TradeXyz => ExchangeCredentials::trade_xyz(
            configuration.api_key().to_owned(),
            configuration.api_secret().to_owned(),
        )?,
    };
    Ok(credentials)
}

fn preferred_exchange_from_environment() -> Result<Exchange, AppConfigurationError> {
    let value = read_nonempty_env_text("GRID_EXCHANGE")?.or(read_nonempty_env_text("EXCHANGE")?);
    parse_preferred_exchange(value)
}

fn parse_preferred_exchange(value: Option<String>) -> Result<Exchange, AppConfigurationError> {
    let value = value.unwrap_or_else(|| "bybit".into());
    match value.to_ascii_lowercase().as_str() {
        "binance" => Ok(Exchange::Binance),
        "aster" => Ok(Exchange::Aster),
        "bybit" => Ok(Exchange::Bybit),
        "trade_xyz" | "tradexyz" | "trade.xyz" => Ok(Exchange::TradeXyz),
        _ => Err(AppConfigurationError::InvalidExchange),
    }
}

fn read_credential_pair(
    key_name: &'static str,
    secret_name: &'static str,
    exchange: &'static str,
) -> Result<Option<(String, String)>, AppConfigurationError> {
    complete_credential_pair(
        read_nonempty_env_text(key_name)?,
        read_nonempty_env_text(secret_name)?,
        exchange,
    )
}

fn complete_credential_pair(
    key: Option<String>,
    secret: Option<String>,
    exchange: &'static str,
) -> Result<Option<(String, String)>, AppConfigurationError> {
    match (key, secret) {
        (None, None) => Ok(None),
        (Some(key), Some(secret)) => Ok(Some((key, secret))),
        _ => Err(AppConfigurationError::IncompleteExchangeCredentials(
            exchange,
        )),
    }
}

fn read_aster_private_key() -> Result<Option<String>, AppConfigurationError> {
    let current = read_nonempty_env_text("ASTER_SIGNER_PRIVATE_KEY")?;
    let legacy = read_nonempty_env_text("ASTER_API_SECRET")?;
    select_aster_private_key(current, legacy)
}

fn select_aster_private_key(
    current: Option<String>,
    legacy: Option<String>,
) -> Result<Option<String>, AppConfigurationError> {
    match (current, legacy) {
        (Some(current), Some(legacy)) if current != legacy => {
            Err(AppConfigurationError::ConflictingAsterPrivateKeys)
        }
        (Some(current), _) => Ok(Some(current)),
        (None, legacy) => Ok(legacy),
    }
}

fn read_nonempty_env_text(name: &'static str) -> Result<Option<String>, AppConfigurationError> {
    normalize_nonempty_env_text(name, read_env_text(name)?)
}

fn normalize_nonempty_env_text(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<String>, AppConfigurationError> {
    match value {
        None => Ok(None),
        Some(value) if value.trim().is_empty() => Ok(None),
        Some(value) if value.trim() != value => {
            Err(AppConfigurationError::EnvironmentWhitespace(name))
        }
        Some(value) => Ok(Some(value)),
    }
}

fn exchange_environment(
    testnet_flag: &'static str,
) -> Result<ExchangeEnvironment, AppConfigurationError> {
    Ok(if parse_env_flag(testnet_flag)? {
        ExchangeEnvironment::Testnet
    } else {
        ExchangeEnvironment::Production
    })
}

fn mask_identifier(value: &str) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    if characters.len() <= 8 {
        return "****".into();
    }
    let prefix = characters[..4].iter().collect::<String>();
    let suffix = characters[characters.len() - 4..]
        .iter()
        .collect::<String>();
    format!("{prefix}****{suffix}")
}

fn read_env_text(name: &'static str) -> Result<Option<String>, AppConfigurationError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(AppConfigurationError::NonUnicodeEnvironment(name))
        }
    }
}

fn parse_env_flag(name: &'static str) -> Result<bool, AppConfigurationError> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(false),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(AppConfigurationError::InvalidBoolean(name));
        }
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        _ => Err(AppConfigurationError::InvalidBoolean(name)),
    }
}

#[derive(Debug, Error)]
pub enum AppConfigurationError {
    #[error("GRID_RUST_ADMIN_TOKEN is not valid Unicode")]
    NonUnicodeAdminToken,
    #[error("GRID_RUST_ADMIN_TOKEN is invalid: {0}")]
    InvalidAdminToken(#[from] AdminTokenError),
    #[error("{0} is not valid Unicode")]
    NonUnicodeEnvironment(&'static str),
    #[error("web authentication configuration is invalid: {0}")]
    InvalidWebAuthentication(#[from] WebAuthConfigurationError),
    #[error("GRID_RUST_CONTROL_ROOT must not be empty")]
    EmptyControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT is not valid Unicode")]
    NonUnicodeControlRoot,
    #[error("GRID_RUST_CONTROL_ROOT must be an absolute path")]
    RelativeControlRoot,
    #[error("GRID_RUST_STRATEGY_ROOT must not be empty")]
    EmptyStrategyRoot,
    #[error("GRID_RUST_STRATEGY_ROOT is not valid Unicode")]
    NonUnicodeStrategyRoot,
    #[error("GRID_RUST_STRATEGY_ROOT must be an absolute path")]
    RelativeStrategyRoot,
    #[error("GRID_WEB_ROOT must not be empty")]
    EmptyWebRoot,
    #[error("GRID_WEB_ROOT is not valid Unicode")]
    NonUnicodeWebRoot,
    #[error("GRID_WEB_ROOT must be an absolute path")]
    RelativeWebRoot,
    #[error("{0} must be a boolean value")]
    InvalidBoolean(&'static str),
    #[error("{0} must not contain leading or trailing whitespace")]
    EnvironmentWhitespace(&'static str),
    #[error("GRID_EXCHANGE or EXCHANGE must be binance, aster, bybit, or trade_xyz")]
    InvalidExchange,
    #[error("GRID_CONFIG_KEY is required when GRID_CONFIG_FILE already exists")]
    MissingExchangeConfigKey,
    #[error("GRID_CONFIG_FILE cannot be inspected: {0}")]
    InspectExchangeConfig(std::io::Error),
    #[error("encrypted exchange configuration is invalid: {0}")]
    InvalidExchangeConfig(#[from] ExchangeConfigStoreError),
    #[error("{0} API credentials must provide both key and secret")]
    IncompleteExchangeCredentials(&'static str),
    #[error("ASTER_SIGNER_PRIVATE_KEY conflicts with legacy ASTER_API_SECRET")]
    ConflictingAsterPrivateKeys,
    #[error("stored Aster wallet address does not match its private key")]
    AsterStoredWalletMismatch,
    #[error("exchange credential is invalid: {0}")]
    InvalidExchangeCredential(#[from] exchange::configured::CredentialError),
    #[error("exchange gateway initialization failed: {0}")]
    ExchangeGateway(#[from] ExchangeGatewayBuildError),
    #[error("exchange gateway registry is invalid: {0}")]
    ExchangeRegistry(#[from] RegistryError),
    #[error("Rust trading requires configured, mandatory web authentication")]
    TradingRequiresWebAuthentication,
    #[error("Rust trading runtime settings are invalid: {0}")]
    InvalidRuntimeSettings(#[from] RuntimeBuildError),
    #[error("Rust trading runtime recovery failed: {0}")]
    RuntimeRecovery(#[from] RuntimeRecoveryError),
    #[error(
        "Rust trading recovery is unsafe: {anomaly_count} discovery anomalies and {failure_count} recovery failures"
    )]
    UnsafeRuntimeRecovery {
        anomaly_count: usize,
        failure_count: usize,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn runtime_tick_lease_coalesces_overlapping_full_ticks() {
        let in_flight = Arc::new(AtomicBool::new(false));
        let lease = RuntimeTickLease::try_acquire(&in_flight).unwrap();

        assert!(RuntimeTickLease::try_acquire(&in_flight).is_none());
        drop(lease);
        assert!(RuntimeTickLease::try_acquire(&in_flight).is_some());
    }

    #[test]
    fn repeated_execution_wakeups_for_one_order_keep_the_targeted_fast_path() {
        let mut pending =
            PendingExecutionWakeups::new(execution_wakeup(Some("42"), Some(1_000), 1_010));
        pending.merge(execution_wakeup(Some("42"), Some(1_005), 1_015));

        assert_eq!(pending.event_count, 2);
        assert_eq!(pending.event.exchange_order_id.as_deref(), Some("42"));
        assert_eq!(
            pending.exchange_order_ids.into_iter().collect::<Vec<_>>(),
            vec!["42"]
        );
        assert!(!pending.requires_full_scan);
        assert_eq!(pending.event.exchange_event_time_ms, Some(1_000));
        assert_eq!(pending.event.observed_at_ms, 1_010);
    }

    #[test]
    fn different_order_wakeups_keep_the_targeted_fast_path() {
        let mut pending =
            PendingExecutionWakeups::new(execution_wakeup(Some("42"), Some(1_000), 1_010));
        pending.merge(execution_wakeup(Some("43"), Some(1_005), 1_015));
        pending.merge(execution_wakeup(Some("44"), Some(1_006), 1_016));

        assert_eq!(pending.event_count, 3);
        assert_eq!(pending.event.exchange_order_id.as_deref(), Some("42"));
        assert_eq!(
            pending.exchange_order_ids.into_iter().collect::<Vec<_>>(),
            vec!["42", "43", "44"]
        );
        assert!(!pending.requires_full_scan);
        assert_eq!(pending.event.exchange_event_time_ms, Some(1_000));
        assert_eq!(pending.event.observed_at_ms, 1_010);
    }

    #[test]
    fn scheduler_microbatch_groups_only_wakeups_for_the_same_market() {
        let first = execution_wakeup(Some("42"), Some(1_000), 1_010);
        let first_key = execution_dispatch_key(&first);
        let mut other_market = execution_wakeup(Some("99"), Some(1_008), 1_018);
        other_market.symbol = "CASHCATUSDC".into();
        let other_key = execution_dispatch_key(&other_market);
        let mut wakeups = HashMap::new();

        merge_execution_wakeup(&mut wakeups, first);
        merge_execution_wakeup(
            &mut wakeups,
            execution_wakeup(Some("43"), Some(1_005), 1_015),
        );
        merge_execution_wakeup(&mut wakeups, other_market);

        assert_eq!(wakeups.len(), 2);
        let same_market = wakeups.get(&first_key).unwrap();
        assert_eq!(same_market.event_count, 2);
        assert_eq!(same_market.event.exchange_order_id.as_deref(), Some("42"));
        assert_eq!(
            same_market
                .exchange_order_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["42", "43"]
        );
        assert!(!same_market.requires_full_scan);
        assert_eq!(same_market.event.exchange_event_time_ms, Some(1_000));
        assert_eq!(same_market.event.observed_at_ms, 1_010);
        assert_eq!(wakeups.get(&other_key).unwrap().event_count, 1);
    }

    #[test]
    fn idless_wakeup_explicitly_keeps_the_full_scan_fallback() {
        let mut pending =
            PendingExecutionWakeups::new(execution_wakeup(Some("42"), Some(1_000), 1_010));
        pending.merge(execution_wakeup(None, Some(1_005), 1_015));

        assert!(pending.requires_full_scan);
        assert_eq!(
            pending.exchange_order_ids.into_iter().collect::<Vec<_>>(),
            vec!["42"]
        );
    }

    #[test]
    fn trade_xyz_dispatch_pipelines_bursts_up_to_the_market_limit() {
        let mut dispatch = ExecutionDispatchState::default();
        for order_id in ["41", "42", "43", "44"] {
            let pending =
                PendingExecutionWakeups::new(execution_wakeup(Some(order_id), Some(1_000), 1_010));
            assert!(dispatch.dispatch_or_queue(pending).is_some());
        }
        assert_eq!(dispatch.in_flight, 4);
        assert!(dispatch.pending.is_none());

        assert!(
            dispatch
                .dispatch_or_queue(PendingExecutionWakeups::new(execution_wakeup(
                    Some("45"),
                    Some(1_001),
                    1_011,
                )))
                .is_none()
        );
        assert!(
            dispatch
                .dispatch_or_queue(PendingExecutionWakeups::new(execution_wakeup(
                    Some("46"),
                    Some(1_002),
                    1_012,
                )))
                .is_none()
        );

        let promoted = dispatch.complete().expect("queued burst must be promoted");
        assert_eq!(promoted.event_count, 2);
        assert_eq!(
            promoted.exchange_order_ids.into_iter().collect::<Vec<_>>(),
            vec!["45", "46"]
        );
        assert_eq!(dispatch.in_flight, 4);
        assert!(dispatch.pending.is_none());

        for _ in 0..4 {
            assert!(dispatch.complete().is_none());
        }
        assert!(dispatch.is_idle());
    }

    #[test]
    fn binance_dispatch_keeps_a_single_in_flight_batch() {
        let mut dispatch = ExecutionDispatchState::default();
        let mut first = execution_wakeup(Some("41"), Some(1_000), 1_010);
        first.exchange = Exchange::Binance;
        let mut second = execution_wakeup(Some("42"), Some(1_001), 1_011);
        second.exchange = Exchange::Binance;
        assert!(
            dispatch
                .dispatch_or_queue(PendingExecutionWakeups::new(first))
                .is_some()
        );
        assert!(
            dispatch
                .dispatch_or_queue(PendingExecutionWakeups::new(second))
                .is_none()
        );

        let promoted = dispatch.complete().expect("queued wakeup must be promoted");
        assert_eq!(promoted.event_count, 1);
        assert_eq!(dispatch.in_flight, 1);
        assert!(dispatch.complete().is_none());
        assert!(dispatch.is_idle());
    }

    #[test]
    fn trade_xyz_full_scan_waits_for_exact_dispatches_to_drain() {
        let mut dispatch = ExecutionDispatchState::default();
        for order_id in ["41", "42", "43"] {
            assert!(
                dispatch
                    .dispatch_or_queue(PendingExecutionWakeups::new(execution_wakeup(
                        Some(order_id),
                        Some(1_000),
                        1_010,
                    )))
                    .is_some()
            );
        }
        assert!(
            dispatch
                .dispatch_or_queue(PendingExecutionWakeups::new(execution_wakeup(
                    None,
                    Some(1_001),
                    1_011,
                )))
                .is_none()
        );

        assert!(dispatch.complete().is_none());
        assert!(dispatch.complete().is_none());
        let full_scan = dispatch
            .complete()
            .expect("full scan must start only after exact dispatches finish");
        assert!(full_scan.requires_full_scan);
        assert_eq!(dispatch.in_flight, 1);
        assert!(dispatch.complete().is_none());
        assert!(dispatch.is_idle());
    }

    #[test]
    fn execution_event_retries_a_clean_snapshot_that_has_not_materialized_a_counter() {
        let report = crate::engine::RuntimeTickReport {
            ledger_reconciliations: 0,
            execution_syncs: 1,
            submissions: Vec::new(),
            cancellations: Vec::new(),
            blockers: Vec::new(),
        };

        assert!(execution_event_needs_retry(&report));
    }

    #[test]
    fn execution_event_does_not_retry_a_synchronized_safety_blocker() {
        let report = crate::engine::RuntimeTickReport {
            ledger_reconciliations: 0,
            execution_syncs: 1,
            submissions: Vec::new(),
            cancellations: Vec::new(),
            blockers: vec![crate::engine::RuntimeBlocker {
                stage: crate::engine::RuntimeStage::PositionReconciliation,
                client_order_id: None,
                message: "authoritative position is inconsistent".into(),
            }],
        };

        assert!(!execution_event_needs_retry(&report));
    }

    #[test]
    fn execution_event_does_not_retry_a_noop_snapshot() {
        let report = crate::engine::RuntimeTickReport {
            ledger_reconciliations: 0,
            execution_syncs: 0,
            submissions: Vec::new(),
            cancellations: Vec::new(),
            blockers: Vec::new(),
        };

        assert!(!execution_event_needs_retry(&report));
    }

    fn execution_wakeup(
        exchange_order_id: Option<&str>,
        exchange_event_time_ms: Option<u64>,
        observed_at_ms: u64,
    ) -> ExecutionWakeup {
        ExecutionWakeup {
            exchange: Exchange::TradeXyz,
            symbol: "CXMTUSDC".into(),
            exchange_order_id: exchange_order_id.map(str::to_owned),
            exchange_event_time_ms,
            observed_at_ms,
        }
    }

    #[test]
    fn an_exactly_empty_optional_admin_token_is_unconfigured() {
        assert!(optional_admin_token(String::new()).unwrap().is_none());
    }

    #[test]
    fn whitespace_or_weak_optional_admin_tokens_remain_invalid() {
        assert!(matches!(
            optional_admin_token("too-short".to_owned()),
            Err(AdminTokenError::TooShort)
        ));
        assert!(matches!(
            optional_admin_token(format!("{} ", "a".repeat(31))),
            Err(AdminTokenError::InvalidCharacter)
        ));
    }

    #[tokio::test]
    async fn rust_app_serves_the_built_vue_assets_without_a_python_runtime() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            None,
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            PathBuf::from(DEFAULT_STRATEGY_ROOT),
            web.path().to_path_buf(),
            None,
        )
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/html");
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            "<main>vue shell</main>"
        );
    }

    #[tokio::test]
    async fn unknown_api_path_is_never_rewritten_to_the_vue_index() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            None,
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            PathBuf::from(DEFAULT_STRATEGY_ROOT),
            web.path().to_path_buf(),
            None,
        )
        .oneshot(
            Request::builder()
                .uri("/api/not-a-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_api_post_is_a_not_found_instead_of_a_static_method_error() {
        let web = tempdir().unwrap();
        fs::write(web.path().join("index.html"), "<main>vue shell</main>").unwrap();
        let response = build_app(
            None,
            WebAuthService::disabled(),
            ExchangeGatewayRegistry::default(),
            None,
            PathBuf::from(DEFAULT_CONTROL_ROOT),
            PathBuf::from(DEFAULT_STRATEGY_ROOT),
            web.path().to_path_buf(),
            None,
        )
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/not-a-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn exchange_identifier_masking_never_returns_the_original_value() {
        assert_eq!(mask_identifier("1234567890abcdef"), "1234****cdef");
        assert_eq!(mask_identifier("short"), "****");
    }

    #[test]
    fn exchange_environment_text_fails_early_on_surrounding_whitespace() {
        assert_eq!(normalize_nonempty_env_text("KEY", None).unwrap(), None);
        assert_eq!(
            normalize_nonempty_env_text("KEY", Some("   ".into())).unwrap(),
            None
        );
        assert_eq!(
            normalize_nonempty_env_text("KEY", Some("exact-value".into())).unwrap(),
            Some("exact-value".into())
        );
        assert!(matches!(
            normalize_nonempty_env_text("KEY", Some(" exact-value ".into())),
            Err(AppConfigurationError::EnvironmentWhitespace("KEY"))
        ));
    }

    #[test]
    fn preferred_exchange_parser_is_deterministic_and_fails_closed() {
        assert_eq!(parse_preferred_exchange(None).unwrap(), Exchange::Bybit);
        assert_eq!(
            parse_preferred_exchange(Some("BINANCE".into())).unwrap(),
            Exchange::Binance
        );
        assert_eq!(
            parse_preferred_exchange(Some("aster".into())).unwrap(),
            Exchange::Aster
        );
        assert_eq!(
            parse_preferred_exchange(Some("TRADE.XYZ".into())).unwrap(),
            Exchange::TradeXyz
        );
        assert!(matches!(
            parse_preferred_exchange(Some("unknown".into())),
            Err(AppConfigurationError::InvalidExchange)
        ));
    }

    #[test]
    fn exchange_credentials_must_be_complete_before_gateway_construction() {
        assert_eq!(
            complete_credential_pair(None, None, "Binance").unwrap(),
            None
        );
        assert_eq!(
            complete_credential_pair(Some("key".into()), Some("secret".into()), "Binance").unwrap(),
            Some(("key".into(), "secret".into()))
        );
        assert!(matches!(
            complete_credential_pair(Some("key".into()), None, "Binance"),
            Err(AppConfigurationError::IncompleteExchangeCredentials(
                "Binance"
            ))
        ));
        assert!(matches!(
            complete_credential_pair(None, Some("secret".into()), "Binance"),
            Err(AppConfigurationError::IncompleteExchangeCredentials(
                "Binance"
            ))
        ));
    }

    #[test]
    fn aster_current_and_legacy_private_keys_cannot_disagree() {
        assert_eq!(select_aster_private_key(None, None).unwrap(), None);
        assert_eq!(
            select_aster_private_key(Some("same".into()), Some("same".into())).unwrap(),
            Some("same".into())
        );
        assert!(matches!(
            select_aster_private_key(Some("current".into()), Some("legacy".into())),
            Err(AppConfigurationError::ConflictingAsterPrivateKeys)
        ));
    }
}
