use std::{
    fmt,
    sync::{Arc, RwLock},
};

use serde::Serialize;
use thiserror::Error;

use crate::{
    domain::Exchange,
    exchange::{
        AccountBalanceSnapshotGateway, ExchangeIdentityGateway, MarketSnapshotGateway,
        OpenOrderSnapshotGateway, OrderHistorySnapshotGateway, OrderLookupGateway,
        PositionSnapshotGateway, TradingFeeRateGateway,
        configured::{
            ConfiguredExchangeGateway, ExchangeEnvironment, SharedConfiguredExchangeGateway,
        },
    },
};

pub trait ReadOnlyExchangeGateway:
    ExchangeIdentityGateway
    + AccountBalanceSnapshotGateway
    + MarketSnapshotGateway
    + TradingFeeRateGateway
    + PositionSnapshotGateway
    + OpenOrderSnapshotGateway
    + OrderHistorySnapshotGateway
    + OrderLookupGateway
{
}

impl<T> ReadOnlyExchangeGateway for T where
    T: ExchangeIdentityGateway
        + AccountBalanceSnapshotGateway
        + MarketSnapshotGateway
        + TradingFeeRateGateway
        + PositionSnapshotGateway
        + OpenOrderSnapshotGateway
        + OrderHistorySnapshotGateway
        + OrderLookupGateway
{
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExchangeConfigurationSummary {
    pub exchange: Exchange,
    pub configured: bool,
    pub testnet: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

#[derive(Clone)]
struct GatewayEntry {
    gateway: Arc<dyn ReadOnlyExchangeGateway>,
    trading_gateway: Option<SharedConfiguredExchangeGateway>,
    environment: ExchangeEnvironment,
    source: String,
    masked_identifier: Option<String>,
}

impl fmt::Debug for GatewayEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayEntry")
            .field("exchange", &self.gateway.exchange())
            .field("environment", &self.environment)
            .field("source", &self.source)
            .field(
                "masked_identifier",
                &self.masked_identifier.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

struct RegistryState {
    preferred: Exchange,
    binance: Option<GatewayEntry>,
    aster: Option<GatewayEntry>,
    bybit: Option<GatewayEntry>,
    trade_xyz: Option<GatewayEntry>,
}

#[derive(Clone)]
pub struct ExchangeGatewayRegistry {
    inner: Arc<RwLock<RegistryState>>,
}

impl ExchangeGatewayRegistry {
    pub fn empty(preferred: Exchange) -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryState {
                preferred,
                binance: None,
                aster: None,
                bybit: None,
                trade_xyz: None,
            })),
        }
    }

    pub fn preferred(&self) -> Exchange {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .preferred
    }

    pub fn set_preferred(&self, preferred: Exchange) {
        self.inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .preferred = preferred;
    }

    pub fn register_configured(
        &mut self,
        gateway: ConfiguredExchangeGateway,
        environment: ExchangeEnvironment,
        source: impl Into<String>,
        masked_identifier: Option<String>,
    ) -> Result<(), RegistryError> {
        let gateway = gateway.shared();
        self.register_entry(
            Arc::new(gateway.clone()),
            Some(gateway),
            environment,
            source,
            masked_identifier,
        )
    }

    pub fn register_gateway(
        &mut self,
        gateway: Arc<dyn ReadOnlyExchangeGateway>,
        environment: ExchangeEnvironment,
        source: impl Into<String>,
        masked_identifier: Option<String>,
    ) -> Result<(), RegistryError> {
        self.register_entry(gateway, None, environment, source, masked_identifier)
    }

    fn register_entry(
        &mut self,
        gateway: Arc<dyn ReadOnlyExchangeGateway>,
        trading_gateway: Option<SharedConfiguredExchangeGateway>,
        environment: ExchangeEnvironment,
        source: impl Into<String>,
        masked_identifier: Option<String>,
    ) -> Result<(), RegistryError> {
        let source = source.into();
        if source.trim().is_empty() {
            return Err(RegistryError::InvalidSource);
        }
        if masked_identifier
            .as_deref()
            .is_some_and(|identifier| identifier.trim().is_empty())
        {
            return Err(RegistryError::InvalidMaskedIdentifier);
        }
        let exchange = gateway.exchange();
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let slot = state.slot_mut(exchange);
        if slot.is_some() {
            return Err(RegistryError::Duplicate(exchange));
        }
        *slot = Some(GatewayEntry {
            gateway,
            trading_gateway,
            environment,
            source,
            masked_identifier,
        });
        Ok(())
    }

    pub fn replace_configured(
        &self,
        gateway: ConfiguredExchangeGateway,
        environment: ExchangeEnvironment,
        source: impl Into<String>,
        masked_identifier: Option<String>,
    ) -> Result<(), RegistryError> {
        let source = source.into();
        validate_entry_metadata(&source, masked_identifier.as_deref())?;
        let gateway = gateway.shared();
        let exchange = gateway.exchange();
        let entry = GatewayEntry {
            gateway: Arc::new(gateway.clone()),
            trading_gateway: Some(gateway),
            environment,
            source,
            masked_identifier,
        };
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .slot_mut(exchange) = Some(entry);
        Ok(())
    }

    pub fn gateway(
        &self,
        exchange: Exchange,
    ) -> Result<Arc<dyn ReadOnlyExchangeGateway>, RegistryError> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .slot(exchange)
            .map(|entry| Arc::clone(&entry.gateway))
            .ok_or(RegistryError::NotConfigured(exchange))
    }

    pub fn trading_gateway(
        &self,
        exchange: Exchange,
    ) -> Result<SharedConfiguredExchangeGateway, RegistryError> {
        let state = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = state
            .slot(exchange)
            .ok_or(RegistryError::NotConfigured(exchange))?;
        entry
            .trading_gateway
            .clone()
            .ok_or(RegistryError::TradingUnavailable(exchange))
    }

    pub fn is_configured(&self, exchange: Exchange) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .slot(exchange)
            .is_some()
    }

    pub fn summary(&self, exchange: Exchange) -> ExchangeConfigurationSummary {
        let state = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.slot(exchange) {
            Some(entry) => ExchangeConfigurationSummary {
                exchange,
                configured: true,
                testnet: entry.environment == ExchangeEnvironment::Testnet,
                source: Some(entry.source.clone()),
                api_key: entry.masked_identifier.clone(),
            },
            None => ExchangeConfigurationSummary {
                exchange,
                configured: false,
                testnet: false,
                source: None,
                api_key: None,
            },
        }
    }

    pub fn summaries(&self) -> [ExchangeConfigurationSummary; 4] {
        [
            self.summary(Exchange::Binance),
            self.summary(Exchange::Aster),
            self.summary(Exchange::Bybit),
            self.summary(Exchange::TradeXyz),
        ]
    }
}

impl RegistryState {
    fn slot(&self, exchange: Exchange) -> Option<&GatewayEntry> {
        match exchange {
            Exchange::Binance => self.binance.as_ref(),
            Exchange::Aster => self.aster.as_ref(),
            Exchange::Bybit => self.bybit.as_ref(),
            Exchange::TradeXyz => self.trade_xyz.as_ref(),
        }
    }

    fn slot_mut(&mut self, exchange: Exchange) -> &mut Option<GatewayEntry> {
        match exchange {
            Exchange::Binance => &mut self.binance,
            Exchange::Aster => &mut self.aster,
            Exchange::Bybit => &mut self.bybit,
            Exchange::TradeXyz => &mut self.trade_xyz,
        }
    }
}

fn validate_entry_metadata(
    source: &str,
    masked_identifier: Option<&str>,
) -> Result<(), RegistryError> {
    if source.trim().is_empty() {
        return Err(RegistryError::InvalidSource);
    }
    if masked_identifier.is_some_and(|identifier| identifier.trim().is_empty()) {
        return Err(RegistryError::InvalidMaskedIdentifier);
    }
    Ok(())
}

impl Default for ExchangeGatewayRegistry {
    fn default() -> Self {
        Self::empty(Exchange::Bybit)
    }
}

impl fmt::Debug for ExchangeGatewayRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        formatter
            .debug_struct("ExchangeGatewayRegistry")
            .field("preferred", &state.preferred)
            .field("binance_configured", &state.binance.is_some())
            .field("aster_configured", &state.aster.is_some())
            .field("bybit_configured", &state.bybit.is_some())
            .field("trade_xyz_configured", &state.trade_xyz.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistryError {
    #[error("{0:?} exchange gateway is already configured")]
    Duplicate(Exchange),
    #[error("{0:?} exchange gateway is not configured")]
    NotConfigured(Exchange),
    #[error("{0:?} exchange gateway is read-only")]
    TradingUnavailable(Exchange),
    #[error("exchange configuration source is invalid")]
    InvalidSource,
    #[error("masked exchange identifier is invalid")]
    InvalidMaskedIdentifier,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{configured::ExchangeCredentials, configured::ExchangeGatewayFactory};

    #[test]
    fn summaries_are_deterministic_and_never_render_gateway_secrets() {
        let factory = ExchangeGatewayFactory::standard(ExchangeEnvironment::Testnet).unwrap();
        let gateway = factory
            .build(ExchangeCredentials::binance("visible-key", "secret-value").unwrap())
            .unwrap();
        let mut registry = ExchangeGatewayRegistry::empty(Exchange::Binance);
        registry
            .register_configured(
                gateway,
                ExchangeEnvironment::Testnet,
                "env",
                Some("visi****-key".into()),
            )
            .unwrap();

        assert!(registry.is_configured(Exchange::Binance));
        assert!(!registry.is_configured(Exchange::Aster));
        assert_eq!(registry.summaries().len(), 4);
        assert_eq!(registry.summaries()[3].exchange, Exchange::TradeXyz);
        assert_eq!(
            registry.summaries()[0].api_key.as_deref(),
            Some("visi****-key")
        );
        let debug = format!("{registry:?}");
        assert!(!debug.contains("visible-key"));
        assert!(!debug.contains("secret-value"));

        let trading = registry.trading_gateway(Exchange::Binance).unwrap();
        assert_eq!(trading.exchange(), Exchange::Binance);
        let trading_debug = format!("{trading:?}");
        assert!(!trading_debug.contains("visible-key"));
        assert!(!trading_debug.contains("secret-value"));
        assert!(trading_debug.contains("[REDACTED]"));
    }

    #[test]
    fn duplicate_registration_fails_closed() {
        let factory = ExchangeGatewayFactory::standard(ExchangeEnvironment::Production).unwrap();
        let first = factory
            .build(ExchangeCredentials::binance("first", "secret").unwrap())
            .unwrap();
        let second = factory
            .build(ExchangeCredentials::binance("second", "secret").unwrap())
            .unwrap();
        let mut registry = ExchangeGatewayRegistry::default();
        registry
            .register_configured(first, ExchangeEnvironment::Production, "env", None)
            .unwrap();

        assert_eq!(
            registry.register_configured(
                second,
                ExchangeEnvironment::Production,
                "encrypted_file",
                None,
            ),
            Err(RegistryError::Duplicate(Exchange::Binance))
        );
    }

    #[test]
    fn cloned_registry_observes_atomic_gateway_replacement_and_preference() {
        let factory = ExchangeGatewayFactory::standard(ExchangeEnvironment::Production).unwrap();
        let first = factory
            .build(ExchangeCredentials::binance("first", "secret").unwrap())
            .unwrap();
        let replacement = factory
            .build(ExchangeCredentials::binance("second", "secret").unwrap())
            .unwrap();
        let mut registry = ExchangeGatewayRegistry::empty(Exchange::Bybit);
        registry
            .register_configured(first, ExchangeEnvironment::Production, "env", None)
            .unwrap();
        let observer = registry.clone();

        registry
            .replace_configured(
                replacement,
                ExchangeEnvironment::Testnet,
                "file",
                Some("seco****alue".into()),
            )
            .unwrap();
        registry.set_preferred(Exchange::Binance);

        assert_eq!(observer.preferred(), Exchange::Binance);
        let summary = observer.summary(Exchange::Binance);
        assert!(summary.configured);
        assert!(summary.testnet);
        assert_eq!(summary.source.as_deref(), Some("file"));
        assert_eq!(summary.api_key.as_deref(), Some("seco****alue"));
    }
}
