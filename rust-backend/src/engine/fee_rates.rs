use thiserror::Error;

use crate::{
    domain::GridConfig,
    exchange::{SnapshotError, TradingFeeRateGateway, TradingFeeRates},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AuthoritativeFeeConfig {
    pub config: GridConfig,
    pub rates: TradingFeeRates,
}

pub async fn load_authoritative_fee_config<G>(
    gateway: &G,
    config: &GridConfig,
) -> Result<AuthoritativeFeeConfig, FeeRateConfigError>
where
    G: TradingFeeRateGateway,
{
    config
        .validate()
        .map_err(|error| FeeRateConfigError::InvalidConfig(error.to_string()))?;
    let exchange = config.exchange.ok_or(FeeRateConfigError::MissingExchange)?;
    let rates = gateway.trading_fee_rates(exchange, &config.symbol).await?;
    rates.validate()?;
    if rates.exchange != exchange || rates.symbol != config.symbol {
        return Err(FeeRateConfigError::IdentityMismatch);
    }

    let mut effective = config.clone();
    effective.maker_fee_rate = Some(rates.maker_rate);
    effective.taker_fee_rate = Some(rates.taker_rate);
    effective.fee_rate = Some(rates.taker_rate);
    Ok(AuthoritativeFeeConfig {
        config: effective,
        rates,
    })
}

#[derive(Debug, Error)]
pub enum FeeRateConfigError {
    #[error("grid configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("grid configuration must identify an exchange")]
    MissingExchange,
    #[error("fee-rate response identity does not match the grid")]
    IdentityMismatch,
    #[error("fee-rate snapshot failed: {0}")]
    Snapshot(#[from] SnapshotError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{Direction, Exchange, GridMode, InitialOrderType, PositionSizingMode},
        exchange::TradingFeeRateGateway,
    };

    struct FakeGateway {
        rates: Result<TradingFeeRates, SnapshotError>,
    }

    #[async_trait]
    impl TradingFeeRateGateway for FakeGateway {
        async fn trading_fee_rates(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<TradingFeeRates, SnapshotError> {
            self.rates.clone()
        }
    }

    fn config() -> GridConfig {
        GridConfig {
            exchange: Some(Exchange::Binance),
            symbol: "MUUSDT".into(),
            direction: Direction::Short,
            upper_price: Decimal::new(1020, 0),
            lower_price: Decimal::new(1000, 0),
            grid_count: 20,
            total_investment: Decimal::ZERO,
            leverage: 5,
            position_sizing_mode: PositionSizingMode::FixedGridQty,
            grid_order_qty: Some(Decimal::new(2, 1)),
            fee_rate: Some(Decimal::new(9, 3)),
            maker_fee_rate: Some(Decimal::new(9, 3)),
            taker_fee_rate: Some(Decimal::new(9, 3)),
            initial_order_type: InitialOrderType::Limit,
            initial_order_price: Some(Decimal::new(1014, 0)),
            grid_order_post_only: false,
            grid_mode: GridMode::Arithmetic,
            trigger_price: None,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    fn rates() -> TradingFeeRates {
        TradingFeeRates {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            maker_rate: Decimal::new(2, 4),
            taker_rate: Decimal::new(5, 4),
        }
    }

    #[tokio::test]
    async fn exchange_rates_replace_every_user_supplied_fee_guess() {
        let effective =
            load_authoritative_fee_config(&FakeGateway { rates: Ok(rates()) }, &config())
                .await
                .unwrap();

        assert_eq!(effective.config.maker_fee_rate, Some(Decimal::new(2, 4)));
        assert_eq!(effective.config.taker_fee_rate, Some(Decimal::new(5, 4)));
        assert_eq!(effective.config.fee_rate, Some(Decimal::new(5, 4)));
        assert_eq!(effective.rates, rates());
    }

    #[tokio::test]
    async fn absent_or_foreign_fee_snapshot_never_produces_effective_config() {
        let unavailable = load_authoritative_fee_config(
            &FakeGateway {
                rates: Err(SnapshotError::new("unavailable")),
            },
            &config(),
        )
        .await;
        assert!(matches!(unavailable, Err(FeeRateConfigError::Snapshot(_))));

        let mut foreign = rates();
        foreign.exchange = Exchange::Aster;
        assert!(matches!(
            load_authoritative_fee_config(&FakeGateway { rates: Ok(foreign) }, &config()).await,
            Err(FeeRateConfigError::IdentityMismatch)
        ));
    }
}
