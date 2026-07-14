use thiserror::Error;

use crate::{
    domain::{Exchange, InstrumentRules},
    exchange::{
        InstrumentRulesGateway, MarketSnapshotGateway, PositionSnapshotGateway, SnapshotError,
    },
};

use super::{MarketSnapshot, PositionBaseline, StrategyStateError};

#[derive(Debug, Clone, PartialEq)]
pub struct AuthoritativeStrategyInputs {
    pub exchange: Exchange,
    pub symbol: String,
    pub market: MarketSnapshot,
    pub instrument_rules: InstrumentRules,
    /// The current authoritative exchange position. It becomes the immutable
    /// baseline only while a new strategy is being activated.
    pub position: PositionBaseline,
}

pub struct StrategyInputService<G> {
    gateway: G,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
}

impl<G> StrategyInputService<G> {
    pub fn new(
        gateway: G,
        maximum_market_age_ms: u64,
        maximum_future_skew_ms: u64,
    ) -> Result<Self, StrategyInputError> {
        if maximum_market_age_ms == 0 {
            return Err(StrategyInputError::InvalidFreshnessWindow);
        }
        Ok(Self {
            gateway,
            maximum_market_age_ms,
            maximum_future_skew_ms,
        })
    }

    pub fn gateway(&self) -> &G {
        &self.gateway
    }
}

impl<G> StrategyInputService<G>
where
    G: MarketSnapshotGateway + InstrumentRulesGateway + PositionSnapshotGateway,
{
    pub async fn load(
        &self,
        exchange: Exchange,
        symbol: &str,
        now_ms: u64,
    ) -> Result<AuthoritativeStrategyInputs, StrategyInputError> {
        load_strategy_inputs(
            &self.gateway,
            exchange,
            symbol,
            now_ms,
            self.maximum_market_age_ms,
            self.maximum_future_skew_ms,
        )
        .await
    }
}

pub async fn load_strategy_inputs<G>(
    gateway: &G,
    exchange: Exchange,
    symbol: &str,
    now_ms: u64,
    maximum_market_age_ms: u64,
    maximum_future_skew_ms: u64,
) -> Result<AuthoritativeStrategyInputs, StrategyInputError>
where
    G: MarketSnapshotGateway + InstrumentRulesGateway + PositionSnapshotGateway,
{
    if maximum_market_age_ms == 0 {
        return Err(StrategyInputError::InvalidFreshnessWindow);
    }
    if symbol.trim().is_empty() || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(StrategyInputError::InvalidSymbol);
    }
    let symbol = symbol.to_ascii_uppercase();
    let (market, instrument_rules, position) = tokio::try_join!(
        gateway.market_snapshot(exchange, &symbol),
        gateway.instrument_rules(exchange, &symbol),
        gateway.position_snapshot(exchange, &symbol),
    )?;

    if market.exchange != exchange
        || market.symbol != symbol
        || position.exchange != exchange
        || position.symbol != symbol
    {
        return Err(StrategyInputError::IdentityMismatch);
    }
    market.ensure_fresh(now_ms, maximum_market_age_ms, maximum_future_skew_ms)?;
    instrument_rules
        .validate()
        .map_err(|error| StrategyInputError::InvalidInstrument(error.to_string()))?;
    let (signed_quantity, entry_price) = position.one_way_position()?;
    let position = PositionBaseline::from_authoritative_position(signed_quantity, entry_price)?;

    Ok(AuthoritativeStrategyInputs {
        exchange,
        symbol,
        market: MarketSnapshot {
            last_price: market.last_price,
            mark_price: market.mark_price,
        },
        instrument_rules,
        position,
    })
}

#[derive(Debug, Error)]
pub enum StrategyInputError {
    #[error("market freshness window must be positive")]
    InvalidFreshnessWindow,
    #[error("strategy input symbol is invalid")]
    InvalidSymbol,
    #[error("exchange input identity does not match the request")]
    IdentityMismatch,
    #[error("exchange input snapshot failed: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("exchange instrument rules are invalid: {0}")]
    InvalidInstrument(String),
    #[error("exchange position cannot form a strategy baseline: {0}")]
    InvalidBaseline(#[from] StrategyStateError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::QuantityRules,
        exchange::{ExchangeMarketSnapshot, PositionLeg, PositionSide, PositionSnapshot},
    };

    #[derive(Clone)]
    struct FakeGateway {
        market: Result<ExchangeMarketSnapshot, SnapshotError>,
        rules: Result<InstrumentRules, SnapshotError>,
        position: Result<PositionSnapshot, SnapshotError>,
    }

    #[async_trait]
    impl MarketSnapshotGateway for FakeGateway {
        async fn market_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<ExchangeMarketSnapshot, SnapshotError> {
            self.market.clone()
        }
    }

    #[async_trait]
    impl InstrumentRulesGateway for FakeGateway {
        async fn instrument_rules(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<InstrumentRules, SnapshotError> {
            self.rules.clone()
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for FakeGateway {
        async fn position_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            self.position.clone()
        }
    }

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(1, 1),
            limit_quantity: QuantityRules {
                step: Decimal::new(1, 2),
                min: Decimal::new(1, 2),
                max: Some(Decimal::new(100, 0)),
            },
            market_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: Some(Decimal::new(50, 0)),
            },
            min_notional: Decimal::new(5, 0),
        }
    }

    fn one_way_position() -> PositionSnapshot {
        PositionSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            legs: vec![PositionLeg {
                side: PositionSide::Both,
                signed_quantity: Decimal::new(-3, 0),
                entry_price: Some(Decimal::new(101125, 2)),
                mark_price: Decimal::new(1010, 0),
                unrealized_profit: Decimal::new(375, 2),
                leverage: Some(5),
            }],
        }
    }

    fn gateway() -> FakeGateway {
        FakeGateway {
            market: Ok(ExchangeMarketSnapshot {
                exchange: Exchange::Binance,
                symbol: "MUUSDT".into(),
                last_price: Decimal::new(1011, 0),
                mark_price: Decimal::new(1010, 0),
                observed_at_ms: 10_000,
            }),
            rules: Ok(rules()),
            position: Ok(one_way_position()),
        }
    }

    #[tokio::test]
    async fn complete_exchange_inputs_preserve_existing_position_as_baseline() {
        let inputs = StrategyInputService::new(gateway(), 1_000, 100)
            .unwrap()
            .load(Exchange::Binance, "muusdt", 10_500)
            .await
            .unwrap();

        assert_eq!(inputs.symbol, "MUUSDT");
        assert_eq!(inputs.position.signed_quantity, Decimal::new(-3, 0));
        assert_eq!(inputs.position.entry_price, Some(Decimal::new(101125, 2)));
        assert_eq!(inputs.market.mark_price, Decimal::new(1010, 0));
        assert_eq!(inputs.instrument_rules, rules());
    }

    #[tokio::test]
    async fn stale_market_prevents_any_strategy_input_bundle() {
        let result = StrategyInputService::new(gateway(), 100, 10)
            .unwrap()
            .load(Exchange::Binance, "MUUSDT", 10_500)
            .await;
        assert!(matches!(result, Err(StrategyInputError::Snapshot(_))));
    }

    #[tokio::test]
    async fn hedge_mode_never_becomes_a_netted_baseline() {
        let mut gateway = gateway();
        gateway.position = Ok(PositionSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            legs: vec![
                PositionLeg {
                    side: PositionSide::Long,
                    signed_quantity: Decimal::new(2, 0),
                    entry_price: Some(Decimal::new(1000, 0)),
                    mark_price: Decimal::new(1010, 0),
                    unrealized_profit: Decimal::new(20, 0),
                    leverage: Some(5),
                },
                PositionLeg {
                    side: PositionSide::Short,
                    signed_quantity: Decimal::new(-1, 0),
                    entry_price: Some(Decimal::new(1020, 0)),
                    mark_price: Decimal::new(1010, 0),
                    unrealized_profit: Decimal::new(10, 0),
                    leverage: Some(5),
                },
            ],
        });

        let result = StrategyInputService::new(gateway, 1_000, 100)
            .unwrap()
            .load(Exchange::Binance, "MUUSDT", 10_500)
            .await;
        assert!(matches!(result, Err(StrategyInputError::Snapshot(_))));
    }

    #[tokio::test]
    async fn mismatched_exchange_identity_fails_closed() {
        let mut gateway = gateway();
        gateway.market.as_mut().unwrap().exchange = Exchange::Aster;
        let result = StrategyInputService::new(gateway, 1_000, 100)
            .unwrap()
            .load(Exchange::Binance, "MUUSDT", 10_500)
            .await;
        assert!(matches!(result, Err(StrategyInputError::IdentityMismatch)));
    }

    #[tokio::test]
    async fn invalid_symbol_is_rejected_before_any_snapshot_can_be_bundled() {
        let result = StrategyInputService::new(gateway(), 1_000, 100)
            .unwrap()
            .load(Exchange::Binance, "MU/USDT", 10_500)
            .await;
        assert!(matches!(result, Err(StrategyInputError::InvalidSymbol)));
    }
}
