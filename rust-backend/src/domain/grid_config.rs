use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Aster,
    Bybit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Long,
    Short,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GridMode {
    Arithmetic,
    Geometric,
}

fn default_grid_mode() -> GridMode {
    GridMode::Arithmetic
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSizingMode {
    Investment,
    FixedGridQty,
}

fn default_sizing_mode() -> PositionSizingMode {
    PositionSizingMode::Investment
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialOrderType {
    Market,
    Limit,
    PostOnly,
}

fn default_initial_order_type() -> InitialOrderType {
    InitialOrderType::Market
}

fn one() -> u16 {
    1
}

fn zero() -> Decimal {
    Decimal::ZERO
}

fn default_maker_fee() -> Option<Decimal> {
    Some(Decimal::new(2, 4))
}

fn default_taker_fee() -> Option<Decimal> {
    Some(Decimal::new(5, 4))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridConfig {
    pub exchange: Option<Exchange>,
    pub symbol: String,
    pub direction: Direction,
    pub upper_price: Decimal,
    pub lower_price: Decimal,
    pub grid_count: u16,
    #[serde(default = "zero")]
    pub total_investment: Decimal,
    #[serde(default = "one")]
    pub leverage: u16,
    #[serde(default = "default_sizing_mode")]
    pub position_sizing_mode: PositionSizingMode,
    pub grid_order_qty: Option<Decimal>,
    #[serde(default = "default_taker_fee")]
    pub fee_rate: Option<Decimal>,
    #[serde(default = "default_maker_fee")]
    pub maker_fee_rate: Option<Decimal>,
    #[serde(default = "default_taker_fee")]
    pub taker_fee_rate: Option<Decimal>,
    #[serde(default = "default_initial_order_type")]
    pub initial_order_type: InitialOrderType,
    pub initial_order_price: Option<Decimal>,
    #[serde(default)]
    pub grid_order_post_only: bool,
    #[serde(default = "default_grid_mode")]
    pub grid_mode: GridMode,
    pub trigger_price: Option<Decimal>,
    pub stop_loss_price: Option<Decimal>,
    pub take_profit_price: Option<Decimal>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GridConfigError {
    #[error("symbol must contain only uppercase ASCII letters and digits")]
    InvalidSymbol,
    #[error("price bounds must be positive and upper_price must exceed lower_price")]
    InvalidRange,
    #[error("grid_count must be between 2 and 100")]
    InvalidGridCount,
    #[error("leverage must be greater than zero")]
    InvalidLeverage,
    #[error("investment sizing requires positive total_investment")]
    InvalidInvestment,
    #[error("fixed quantity sizing requires positive grid_order_qty")]
    InvalidGridQuantity,
    #[error("optional prices must be positive")]
    InvalidOptionalPrice,
}

impl GridConfig {
    pub fn validate(&self) -> Result<(), GridConfigError> {
        if self.symbol.is_empty()
            || !self
                .symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(GridConfigError::InvalidSymbol);
        }
        if self.lower_price <= Decimal::ZERO || self.upper_price <= self.lower_price {
            return Err(GridConfigError::InvalidRange);
        }
        if !(2..=100).contains(&self.grid_count) {
            return Err(GridConfigError::InvalidGridCount);
        }
        if self.leverage == 0 {
            return Err(GridConfigError::InvalidLeverage);
        }
        match self.position_sizing_mode {
            PositionSizingMode::Investment if self.total_investment <= Decimal::ZERO => {
                return Err(GridConfigError::InvalidInvestment);
            }
            PositionSizingMode::FixedGridQty
                if self.grid_order_qty.unwrap_or_default() <= Decimal::ZERO =>
            {
                return Err(GridConfigError::InvalidGridQuantity);
            }
            _ => {}
        }
        if [
            self.initial_order_price,
            self.trigger_price,
            self.stop_loss_price,
            self.take_profit_price,
        ]
        .into_iter()
        .flatten()
        .any(|price| price <= Decimal::ZERO)
        {
            return Err(GridConfigError::InvalidOptionalPrice);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_config() -> GridConfig {
        serde_json::from_value(serde_json::json!({
            "exchange": "binance",
            "symbol": "MUUSDT",
            "direction": "short",
            "upper_price": 1020,
            "lower_price": 1000,
            "grid_count": 20,
            "position_sizing_mode": "fixed_grid_qty",
            "grid_order_qty": 0.2,
            "leverage": 5
        }))
        .unwrap()
    }

    #[test]
    fn parses_existing_python_request_without_binary_float_math() {
        let config = fixed_config();
        assert_eq!(config.grid_order_qty, Some(Decimal::new(2, 1)));
        assert_eq!(config.upper_price - config.lower_price, Decimal::new(20, 0));
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn rejects_missing_fixed_grid_quantity() {
        let mut config = fixed_config();
        config.grid_order_qty = None;
        assert_eq!(config.validate(), Err(GridConfigError::InvalidGridQuantity));
    }

    #[test]
    fn rejects_collapsed_price_range() {
        let mut config = fixed_config();
        config.lower_price = config.upper_price;
        assert_eq!(config.validate(), Err(GridConfigError::InvalidRange));
    }
}
