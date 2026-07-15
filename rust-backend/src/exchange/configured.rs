use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{
    domain::{ClientOrderId, Exchange, InstrumentRules, OrderIntent},
    exchange::{
        AccountBalanceSnapshot, AccountBalanceSnapshotGateway, CancellationAcknowledgement,
        CancellationError, ExchangeIdentityGateway, ExchangeMarketSnapshot, ExecutionSnapshotError,
        ExecutionSnapshotGateway, HistoricalMinutePrice, HistoricalOrder, HistoricalPriceGateway,
        InstrumentRulesGateway, LeverageAcknowledgement, LeverageError, LeverageGateway,
        LookupError, MarketSnapshotGateway, OpenOrderSnapshotGateway, OrderCancellationGateway,
        OrderExecutionSnapshot, OrderHistorySnapshotGateway, OrderLookup, OrderLookupGateway,
        OrderPlacementGateway, PlacementAcknowledgement, PlacementError, PositionSnapshot,
        PositionSnapshotGateway, SnapshotError, TradingFeeRateGateway, TradingFeeRates,
        aster::{AsterAdapter, AsterSignatureError, LocalEip712Signer},
        binance::{BinanceAdapter, HmacSha256Signer, SignatureError},
        bybit::{BybitAdapter, BybitHmacSha256Signer, BybitSignatureError},
        protocol::{MonotonicMicrosecondNonce, ReqwestTransport, SystemClock, TransportBuildError},
    },
};

type BinanceGateway = BinanceAdapter<ReqwestTransport, HmacSha256Signer, SystemClock>;
type AsterGateway = AsterAdapter<ReqwestTransport, LocalEip712Signer, MonotonicMicrosecondNonce>;
type BybitGateway = BybitAdapter<ReqwestTransport, BybitHmacSha256Signer, SystemClock>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeEnvironment {
    Production,
    Testnet,
}

pub enum ExchangeCredentials {
    Binance {
        api_key: Zeroizing<String>,
        api_secret: Zeroizing<String>,
    },
    Aster {
        private_key: Zeroizing<String>,
    },
    Bybit {
        api_key: Zeroizing<String>,
        api_secret: Zeroizing<String>,
    },
}

impl ExchangeCredentials {
    pub fn binance(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, CredentialError> {
        let api_key = required_secret(api_key, "Binance API key")?;
        let api_secret = required_secret(api_secret, "Binance API secret")?;
        Ok(Self::Binance {
            api_key,
            api_secret,
        })
    }

    pub fn aster(private_key: impl Into<String>) -> Result<Self, CredentialError> {
        Ok(Self::Aster {
            private_key: required_secret(private_key, "Aster private key")?,
        })
    }

    pub fn bybit(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, CredentialError> {
        let api_key = required_secret(api_key, "Bybit API key")?;
        let api_secret = required_secret(api_secret, "Bybit API secret")?;
        Ok(Self::Bybit {
            api_key,
            api_secret,
        })
    }

    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Binance { .. } => Exchange::Binance,
            Self::Aster { .. } => Exchange::Aster,
            Self::Bybit { .. } => Exchange::Bybit,
        }
    }
}

impl fmt::Debug for ExchangeCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExchangeCredentials")
            .field("exchange", &self.exchange())
            .field("secret_material", &"[REDACTED]")
            .finish()
    }
}

fn required_secret(
    value: impl Into<String>,
    field: &'static str,
) -> Result<Zeroizing<String>, CredentialError> {
    let value = Zeroizing::new(value.into());
    if value.trim().is_empty() {
        return Err(CredentialError::Missing(field));
    }
    if value.contains('\r') || value.contains('\n') || value.contains('\0') {
        return Err(CredentialError::Invalid(field));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialError {
    #[error("{0} is required")]
    Missing(&'static str),
    #[error("{0} contains forbidden control characters")]
    Invalid(&'static str),
}

pub struct ExchangeGatewayFactory {
    environment: ExchangeEnvironment,
    transport: ReqwestTransport,
}

impl ExchangeGatewayFactory {
    pub fn new(
        environment: ExchangeEnvironment,
        timeout: Duration,
    ) -> Result<Self, ExchangeGatewayBuildError> {
        Ok(Self {
            environment,
            transport: ReqwestTransport::new(timeout)?,
        })
    }

    pub fn standard(environment: ExchangeEnvironment) -> Result<Self, ExchangeGatewayBuildError> {
        Self::new(environment, Duration::from_secs(10))
    }

    pub fn environment(&self) -> ExchangeEnvironment {
        self.environment
    }

    pub fn build(
        &self,
        credentials: ExchangeCredentials,
    ) -> Result<ConfiguredExchangeGateway, ExchangeGatewayBuildError> {
        Ok(match credentials {
            ExchangeCredentials::Binance {
                api_key,
                api_secret,
            } => {
                let signer = HmacSha256Signer::new(api_secret.as_bytes())?;
                let adapter = match self.environment {
                    ExchangeEnvironment::Production => BinanceAdapter::production(
                        self.transport.clone(),
                        signer,
                        SystemClock,
                        api_key.as_str().to_owned(),
                    ),
                    ExchangeEnvironment::Testnet => BinanceAdapter::testnet(
                        self.transport.clone(),
                        signer,
                        SystemClock,
                        api_key.as_str().to_owned(),
                    ),
                };
                ConfiguredExchangeGateway::Binance(adapter)
            }
            ExchangeCredentials::Aster { private_key } => {
                let adapter = match self.environment {
                    ExchangeEnvironment::Production => AsterAdapter::production_wallet(
                        self.transport.clone(),
                        MonotonicMicrosecondNonce::default(),
                        private_key.as_str(),
                    )?,
                    ExchangeEnvironment::Testnet => AsterAdapter::testnet_wallet(
                        self.transport.clone(),
                        MonotonicMicrosecondNonce::default(),
                        private_key.as_str(),
                    )?,
                };
                ConfiguredExchangeGateway::Aster(adapter)
            }
            ExchangeCredentials::Bybit {
                api_key,
                api_secret,
            } => {
                let signer = BybitHmacSha256Signer::new(api_secret.as_bytes())?;
                let adapter = match self.environment {
                    ExchangeEnvironment::Production => BybitAdapter::production(
                        self.transport.clone(),
                        signer,
                        SystemClock,
                        api_key.as_str().to_owned(),
                    ),
                    ExchangeEnvironment::Testnet => BybitAdapter::testnet(
                        self.transport.clone(),
                        signer,
                        SystemClock,
                        api_key.as_str().to_owned(),
                    ),
                };
                ConfiguredExchangeGateway::Bybit(adapter)
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum ExchangeGatewayBuildError {
    #[error(transparent)]
    Transport(#[from] TransportBuildError),
    #[error(transparent)]
    Binance(#[from] SignatureError),
    #[error(transparent)]
    Aster(#[from] AsterSignatureError),
    #[error(transparent)]
    Bybit(#[from] BybitSignatureError),
}

pub enum ConfiguredExchangeGateway {
    Binance(BinanceGateway),
    Aster(AsterGateway),
    Bybit(BybitGateway),
}

impl ConfiguredExchangeGateway {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Binance(_) => Exchange::Binance,
            Self::Aster(_) => Exchange::Aster,
            Self::Bybit(_) => Exchange::Bybit,
        }
    }

    pub fn shared(self) -> SharedConfiguredExchangeGateway {
        SharedConfiguredExchangeGateway {
            inner: Arc::new(self),
        }
    }
}

/// Cloneable handle to one configured adapter instance. Cloning the handle
/// never duplicates credential material or exchange nonce state.
#[derive(Clone)]
pub struct SharedConfiguredExchangeGateway {
    inner: Arc<ConfiguredExchangeGateway>,
}

impl SharedConfiguredExchangeGateway {
    pub fn exchange(&self) -> Exchange {
        self.inner.exchange()
    }
}

impl fmt::Debug for SharedConfiguredExchangeGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedConfiguredExchangeGateway")
            .field("exchange", &self.exchange())
            .field("credential_material", &"[REDACTED]")
            .finish()
    }
}

impl ExchangeIdentityGateway for ConfiguredExchangeGateway {
    fn exchange(&self) -> Exchange {
        ConfiguredExchangeGateway::exchange(self)
    }
}

#[async_trait]
impl OrderPlacementGateway for ConfiguredExchangeGateway {
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        match self {
            Self::Binance(gateway) => gateway.place_order(intent).await,
            Self::Aster(gateway) => gateway.place_order(intent).await,
            Self::Bybit(gateway) => gateway.place_order(intent).await,
        }
    }
}

#[async_trait]
impl LeverageGateway for ConfiguredExchangeGateway {
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        match self {
            Self::Binance(gateway) => gateway.set_leverage(exchange, symbol, leverage).await,
            Self::Aster(gateway) => gateway.set_leverage(exchange, symbol, leverage).await,
            Self::Bybit(gateway) => gateway.set_leverage(exchange, symbol, leverage).await,
        }
    }
}

#[async_trait]
impl TradingFeeRateGateway for ConfiguredExchangeGateway {
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.trading_fee_rates(exchange, symbol).await,
            Self::Aster(gateway) => gateway.trading_fee_rates(exchange, symbol).await,
            Self::Bybit(gateway) => gateway.trading_fee_rates(exchange, symbol).await,
        }
    }
}

#[async_trait]
impl AccountBalanceSnapshotGateway for ConfiguredExchangeGateway {
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.account_balance_snapshot(exchange).await,
            Self::Aster(gateway) => gateway.account_balance_snapshot(exchange).await,
            Self::Bybit(gateway) => gateway.account_balance_snapshot(exchange).await,
        }
    }
}

#[async_trait]
impl OrderCancellationGateway for ConfiguredExchangeGateway {
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        match self {
            Self::Binance(gateway) => {
                gateway
                    .cancel_order(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
            Self::Aster(gateway) => {
                gateway
                    .cancel_order(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
            Self::Bybit(gateway) => {
                gateway
                    .cancel_order(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
        }
    }
}

#[async_trait]
impl OrderLookupGateway for ConfiguredExchangeGateway {
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        match self {
            Self::Binance(gateway) => {
                gateway
                    .lookup_order_by_client_id(exchange, symbol, client_order_id)
                    .await
            }
            Self::Aster(gateway) => {
                gateway
                    .lookup_order_by_client_id(exchange, symbol, client_order_id)
                    .await
            }
            Self::Bybit(gateway) => {
                gateway
                    .lookup_order_by_client_id(exchange, symbol, client_order_id)
                    .await
            }
        }
    }
}

#[async_trait]
impl OpenOrderSnapshotGateway for ConfiguredExchangeGateway {
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<crate::exchange::AuthoritativeOrder>, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.open_orders_snapshot(exchange, symbol).await,
            Self::Aster(gateway) => gateway.open_orders_snapshot(exchange, symbol).await,
            Self::Bybit(gateway) => gateway.open_orders_snapshot(exchange, symbol).await,
        }
    }
}

#[async_trait]
impl OrderHistorySnapshotGateway for ConfiguredExchangeGateway {
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError> {
        match self {
            Self::Binance(gateway) => {
                gateway
                    .order_history_snapshot(exchange, symbol, limit)
                    .await
            }
            Self::Aster(gateway) => {
                gateway
                    .order_history_snapshot(exchange, symbol, limit)
                    .await
            }
            Self::Bybit(gateway) => {
                gateway
                    .order_history_snapshot(exchange, symbol, limit)
                    .await
            }
        }
    }
}

#[async_trait]
impl ExecutionSnapshotGateway for ConfiguredExchangeGateway {
    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        match self {
            Self::Binance(gateway) => {
                gateway
                    .execution_snapshot(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
            Self::Aster(gateway) => {
                gateway
                    .execution_snapshot(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
            Self::Bybit(gateway) => {
                gateway
                    .execution_snapshot(exchange, symbol, client_order_id, exchange_order_id)
                    .await
            }
        }
    }
}

#[async_trait]
impl HistoricalPriceGateway for ConfiguredExchangeGateway {
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError> {
        match self {
            Self::Binance(gateway) => {
                gateway
                    .historical_minute_open(exchange, symbol, minute_start_ms)
                    .await
            }
            Self::Aster(gateway) => {
                gateway
                    .historical_minute_open(exchange, symbol, minute_start_ms)
                    .await
            }
            Self::Bybit(gateway) => {
                gateway
                    .historical_minute_open(exchange, symbol, minute_start_ms)
                    .await
            }
        }
    }
}

#[async_trait]
impl MarketSnapshotGateway for ConfiguredExchangeGateway {
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.market_snapshot(exchange, symbol).await,
            Self::Aster(gateway) => gateway.market_snapshot(exchange, symbol).await,
            Self::Bybit(gateway) => gateway.market_snapshot(exchange, symbol).await,
        }
    }
}

#[async_trait]
impl InstrumentRulesGateway for ConfiguredExchangeGateway {
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<InstrumentRules, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.instrument_rules(exchange, symbol).await,
            Self::Aster(gateway) => gateway.instrument_rules(exchange, symbol).await,
            Self::Bybit(gateway) => gateway.instrument_rules(exchange, symbol).await,
        }
    }
}

#[async_trait]
impl PositionSnapshotGateway for ConfiguredExchangeGateway {
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        match self {
            Self::Binance(gateway) => gateway.position_snapshot(exchange, symbol).await,
            Self::Aster(gateway) => gateway.position_snapshot(exchange, symbol).await,
            Self::Bybit(gateway) => gateway.position_snapshot(exchange, symbol).await,
        }
    }
}

impl ExchangeIdentityGateway for SharedConfiguredExchangeGateway {
    fn exchange(&self) -> Exchange {
        self.exchange()
    }
}

#[async_trait]
impl OrderPlacementGateway for SharedConfiguredExchangeGateway {
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        self.inner.place_order(intent).await
    }
}

#[async_trait]
impl LeverageGateway for SharedConfiguredExchangeGateway {
    async fn set_leverage(
        &self,
        exchange: Exchange,
        symbol: &str,
        leverage: u16,
    ) -> Result<LeverageAcknowledgement, LeverageError> {
        self.inner.set_leverage(exchange, symbol, leverage).await
    }
}

#[async_trait]
impl TradingFeeRateGateway for SharedConfiguredExchangeGateway {
    async fn trading_fee_rates(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<TradingFeeRates, SnapshotError> {
        self.inner.trading_fee_rates(exchange, symbol).await
    }
}

#[async_trait]
impl AccountBalanceSnapshotGateway for SharedConfiguredExchangeGateway {
    async fn account_balance_snapshot(
        &self,
        exchange: Exchange,
    ) -> Result<AccountBalanceSnapshot, SnapshotError> {
        self.inner.account_balance_snapshot(exchange).await
    }
}

#[async_trait]
impl OrderCancellationGateway for SharedConfiguredExchangeGateway {
    async fn cancel_order(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<CancellationAcknowledgement, CancellationError> {
        self.inner
            .cancel_order(exchange, symbol, client_order_id, exchange_order_id)
            .await
    }
}

#[async_trait]
impl OrderLookupGateway for SharedConfiguredExchangeGateway {
    async fn lookup_order_by_client_id(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
    ) -> Result<OrderLookup, LookupError> {
        self.inner
            .lookup_order_by_client_id(exchange, symbol, client_order_id)
            .await
    }
}

#[async_trait]
impl OpenOrderSnapshotGateway for SharedConfiguredExchangeGateway {
    async fn open_orders_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<Vec<crate::exchange::AuthoritativeOrder>, SnapshotError> {
        self.inner.open_orders_snapshot(exchange, symbol).await
    }
}

#[async_trait]
impl OrderHistorySnapshotGateway for SharedConfiguredExchangeGateway {
    async fn order_history_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<HistoricalOrder>, SnapshotError> {
        self.inner
            .order_history_snapshot(exchange, symbol, limit)
            .await
    }
}

#[async_trait]
impl ExecutionSnapshotGateway for SharedConfiguredExchangeGateway {
    async fn execution_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
        client_order_id: &ClientOrderId,
        exchange_order_id: &str,
    ) -> Result<OrderExecutionSnapshot, ExecutionSnapshotError> {
        self.inner
            .execution_snapshot(exchange, symbol, client_order_id, exchange_order_id)
            .await
    }
}

#[async_trait]
impl HistoricalPriceGateway for SharedConfiguredExchangeGateway {
    async fn historical_minute_open(
        &self,
        exchange: Exchange,
        symbol: &str,
        minute_start_ms: u64,
    ) -> Result<HistoricalMinutePrice, SnapshotError> {
        self.inner
            .historical_minute_open(exchange, symbol, minute_start_ms)
            .await
    }
}

#[async_trait]
impl MarketSnapshotGateway for SharedConfiguredExchangeGateway {
    async fn market_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
        self.inner.market_snapshot(exchange, symbol).await
    }
}

#[async_trait]
impl InstrumentRulesGateway for SharedConfiguredExchangeGateway {
    async fn instrument_rules(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<InstrumentRules, SnapshotError> {
        self.inner.instrument_rules(exchange, symbol).await
    }
}

#[async_trait]
impl PositionSnapshotGateway for SharedConfiguredExchangeGateway {
    async fn position_snapshot(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> Result<PositionSnapshot, SnapshotError> {
        self.inner.position_snapshot(exchange, symbol).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_debug_never_discloses_secret_material() {
        let credentials =
            ExchangeCredentials::binance("api-key-value", "api-secret-value").unwrap();
        let rendered = format!("{credentials:?}");

        assert!(rendered.contains("Binance"));
        assert!(!rendered.contains("api-key-value"));
        assert!(!rendered.contains("api-secret-value"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn credentials_reject_empty_or_control_character_values() {
        assert!(matches!(
            ExchangeCredentials::bybit("", "secret"),
            Err(CredentialError::Missing("Bybit API key"))
        ));
        assert!(matches!(
            ExchangeCredentials::aster("secret\nleak"),
            Err(CredentialError::Invalid("Aster private key"))
        ));
    }

    #[test]
    fn factory_builds_all_supported_gateways_without_exposing_credentials() {
        let factory = ExchangeGatewayFactory::standard(ExchangeEnvironment::Testnet).unwrap();
        let binance = factory
            .build(ExchangeCredentials::binance("key", "secret").unwrap())
            .unwrap();
        let aster = factory
            .build(ExchangeCredentials::aster("1".repeat(64)).unwrap())
            .unwrap();
        let bybit = factory
            .build(ExchangeCredentials::bybit("key", "secret").unwrap())
            .unwrap();

        assert_eq!(factory.environment(), ExchangeEnvironment::Testnet);
        assert_eq!(binance.exchange(), Exchange::Binance);
        assert_eq!(aster.exchange(), Exchange::Aster);
        assert_eq!(bybit.exchange(), Exchange::Bybit);
    }

    #[test]
    fn invalid_aster_key_fails_before_any_network_request() {
        let factory = ExchangeGatewayFactory::standard(ExchangeEnvironment::Production).unwrap();
        let result = factory.build(ExchangeCredentials::aster("not-a-key").unwrap());
        assert!(matches!(result, Err(ExchangeGatewayBuildError::Aster(_))));
    }

    #[test]
    fn zero_timeout_is_rejected() {
        assert!(matches!(
            ExchangeGatewayFactory::new(ExchangeEnvironment::Production, Duration::ZERO),
            Err(ExchangeGatewayBuildError::Transport(
                TransportBuildError::InvalidTimeout
            ))
        ));
    }
}
