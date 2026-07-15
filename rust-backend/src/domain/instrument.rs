use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantityRules {
    pub step: Decimal,
    pub min: Decimal,
    pub max: Option<Decimal>,
}

impl QuantityRules {
    fn validate(&self, prefix: &'static str) -> Result<(), InstrumentRulesError> {
        if self.step <= Decimal::ZERO {
            return Err(InstrumentRulesError::NonPositive { field: prefix });
        }
        if self.min <= Decimal::ZERO {
            return Err(InstrumentRulesError::NonPositive {
                field: if prefix == "limit quantity step" {
                    "limit minimum quantity"
                } else {
                    "market minimum quantity"
                },
            });
        }
        if let Some(maximum) = self.max {
            if maximum <= Decimal::ZERO {
                return Err(InstrumentRulesError::NonPositive {
                    field: if prefix == "limit quantity step" {
                        "limit maximum quantity"
                    } else {
                        "market maximum quantity"
                    },
                });
            }
            if maximum < self.min {
                return Err(if prefix == "market quantity step" {
                    InstrumentRulesError::MarketMaximumBelowMinimum
                } else {
                    InstrumentRulesError::LimitMaximumBelowMinimum
                });
            }
        }
        Ok(())
    }

    pub fn is_aligned(&self, quantity: Decimal) -> bool {
        quantity > Decimal::ZERO
            && quantity
                .checked_div(self.step)
                .is_some_and(|steps| steps.fract().is_zero())
    }

    pub fn floor(&self, quantity: Decimal) -> Option<Decimal> {
        quantity
            .checked_div(self.step)?
            .floor()
            .checked_mul(self.step)
    }

    pub fn accepts(&self, quantity: Decimal) -> bool {
        self.is_aligned(quantity)
            && quantity >= self.min
            && self.max.is_none_or(|maximum| quantity <= maximum)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentRules {
    pub tick_size: Decimal,
    pub limit_quantity: QuantityRules,
    pub market_quantity: QuantityRules,
    #[serde(default)]
    pub min_notional: Decimal,
}

impl InstrumentRules {
    pub fn validate(&self) -> Result<(), InstrumentRulesError> {
        if self.tick_size <= Decimal::ZERO {
            return Err(InstrumentRulesError::NonPositive { field: "tick size" });
        }
        if self.min_notional < Decimal::ZERO {
            return Err(InstrumentRulesError::NegativeMinimumNotional);
        }
        self.limit_quantity.validate("limit quantity step")?;
        self.market_quantity.validate("market quantity step")?;
        Ok(())
    }

    pub fn floor_price(&self, price: Decimal) -> Option<Decimal> {
        price
            .checked_div(self.tick_size)?
            .floor()
            .checked_mul(self.tick_size)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InstrumentRulesError {
    #[error("{field} must be positive")]
    NonPositive { field: &'static str },
    #[error("minimum notional must not be negative")]
    NegativeMinimumNotional,
    #[error("limit maximum quantity is below its minimum")]
    LimitMaximumBelowMinimum,
    #[error("market maximum quantity is below its minimum")]
    MarketMaximumBelowMinimum,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> InstrumentRules {
        InstrumentRules {
            tick_size: Decimal::new(1, 1),
            limit_quantity: QuantityRules {
                step: Decimal::new(1, 2),
                min: Decimal::new(1, 2),
                max: None,
            },
            market_quantity: QuantityRules {
                step: Decimal::new(1, 1),
                min: Decimal::new(1, 1),
                max: Some(Decimal::new(100, 0)),
            },
            min_notional: Decimal::new(5, 0),
        }
    }

    #[test]
    fn quantity_alignment_never_rounds_up() {
        let rules = rules();
        assert_eq!(
            rules.limit_quantity.floor(Decimal::new(314, 2)),
            Some(Decimal::new(314, 2))
        );
        assert_eq!(
            rules.limit_quantity.floor(Decimal::new(3145, 3)),
            Some(Decimal::new(314, 2))
        );
        assert!(!rules.limit_quantity.is_aligned(Decimal::new(3145, 3)));
    }

    #[test]
    fn invalid_rules_fail_before_planning() {
        let mut rules = rules();
        rules.market_quantity.max = Some(Decimal::new(5, 2));
        assert_eq!(
            rules.validate(),
            Err(InstrumentRulesError::MarketMaximumBelowMinimum)
        );
    }
}
