use std::{fmt, sync::Arc};

use serde::Serialize;
use thiserror::Error;

use crate::{
    domain::Exchange,
    exchange::{
        AccountBalanceSnapshotGateway, ExchangeIdentityGateway, MarketSnapshotGateway,
        OpenOrderSnapshotGateway, OrderLookupGateway, PositionSnapshotGateway,
        TradingFeeRateGateway,
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

#[derive(Clone)]
pub struct ExchangeGatewayRegistry {
    preferred: Exchange,
    binance: Option<GatewayEntry>,
    aster: Option<GatewayEntry>,
    bybit: Option<GatewayEntry>,
}

impl ExchangeGatewayRegistry {
    pub fn empty(preferred: Exchange) -> Self {
        Self {
            preferred,
            binance: None,
            aster: None,
            bybit: None,
        }
    }

    pub fn preferred(&self) -> Exchange {
        self.preferred
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
        let slot = self.slot_mut(exchange);
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

    pub fn gateway(
        &self,
        exchange: Exchange,
    ) -> Result<Arc<dyn ReadOnlyExchangeGateway>, RegistryError> {
        self.slot(exchange)
            .map(|entry| Arc::clone(&entry.gateway))
            .ok_or(RegistryError::NotConfigured(exchange))
    }

    pub fn trading_gateway(
        &self,
        exchange: Exchange,
    ) -> Result<SharedConfiguredExchangeGateway, RegistryError> {
        let entry = self
            .slot(exchange)
            .ok_or(RegistryError::NotConfigured(exchange))?;
        entry
            .trading_gateway
            .clone()
            .ok_or(RegistryError::TradingUnavailable(exchange))
    }

    pub fn is_configured(&self, exchange: Exchange) -> bool {
        self.slot(exchange).is_some()
    }

    pub fn summary(&self, exchange: Exchange) -> ExchangeConfigurationSummary {
        match self.slot(exchange) {
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

    pub fn summaries(&self) -> [ExchangeConfigurationSummary; 3] {
        [
            self.summary(Exchange::Binance),
            self.summary(Exchange::Aster),
            self.summary(Exchange::Bybit),
        ]
    }

    fn slot(&self, exchange: Exchange) -> Option<&GatewayEntry> {
        match exchange {
            Exchange::Binance => self.binance.as_ref(),
            Exchange::Aster => self.aster.as_ref(),
            Exchange::Bybit => self.bybit.as_ref(),
        }
    }

    fn slot_mut(&mut self, exchange: Exchange) -> &mut Option<GatewayEntry> {
        match exchange {
            Exchange::Binance => &mut self.binance,
            Exchange::Aster => &mut self.aster,
            Exchange::Bybit => &mut self.bybit,
        }
    }
}

impl Default for ExchangeGatewayRegistry {
    fn default() -> Self {
        Self::empty(Exchange::Bybit)
    }
}

impl fmt::Debug for ExchangeGatewayRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExchangeGatewayRegistry")
            .field("preferred", &self.preferred)
            .field("binance_configured", &self.binance.is_some())
            .field("aster_configured", &self.aster.is_some())
            .field("bybit_configured", &self.bybit.is_some())
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
}
