use thiserror::Error;

use crate::{
    domain::Exchange,
    exchange::{
        LeverageAcknowledgement, LeverageError, LeverageGateway, PositionSnapshotGateway,
        SnapshotError,
    },
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeveragePreflightResult {
    AlreadyConfigured,
    ConfiguredAndVerified,
    ReconciledAfterUnknown,
}

pub async fn ensure_symbol_leverage<G>(
    gateway: &G,
    exchange: Exchange,
    symbol: &str,
    expected_leverage: u16,
) -> Result<LeveragePreflightResult, LeveragePreflightError>
where
    G: LeverageGateway + PositionSnapshotGateway,
{
    if symbol.trim().is_empty()
        || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || !(1..=125).contains(&expected_leverage)
    {
        return Err(LeveragePreflightError::InvalidInput);
    }
    let symbol = symbol.to_ascii_uppercase();
    let (position_quantity, before) =
        observed_position_and_leverage(gateway, exchange, &symbol).await?;
    if before == expected_leverage {
        return Ok(LeveragePreflightResult::AlreadyConfigured);
    }
    if !position_quantity.is_zero() {
        return Err(LeveragePreflightError::ExistingPositionLeverageMismatch {
            observed: before,
            requested: expected_leverage,
        });
    }

    let acknowledgement = match gateway
        .set_leverage(exchange, &symbol, expected_leverage)
        .await
    {
        Ok(acknowledgement) => acknowledgement,
        Err(LeverageError::Invalid { message }) => {
            return Err(LeveragePreflightError::InvalidRequest(message));
        }
        Err(LeverageError::Definitive { code, message }) => {
            return Err(LeveragePreflightError::Definitive { code, message });
        }
        Err(LeverageError::Unknown { message }) => {
            return match observed_position_and_leverage(gateway, exchange, &symbol).await {
                Ok((_, observed)) if observed == expected_leverage => {
                    Ok(LeveragePreflightResult::ReconciledAfterUnknown)
                }
                Ok((_, observed)) => Err(LeveragePreflightError::Unknown {
                    message: format!(
                        "{message}; verification still reports leverage {observed}, expected {expected_leverage}"
                    ),
                }),
                Err(error) => Err(LeveragePreflightError::Unknown {
                    message: format!("{message}; verification failed: {error}"),
                }),
            };
        }
    };
    validate_acknowledgement(&acknowledgement, exchange, &symbol, expected_leverage)?;
    let (_, observed) = observed_position_and_leverage(gateway, exchange, &symbol).await?;
    if observed != expected_leverage {
        return Err(LeveragePreflightError::NotVerified {
            expected: expected_leverage,
            observed,
        });
    }
    Ok(LeveragePreflightResult::ConfiguredAndVerified)
}

async fn observed_position_and_leverage<G>(
    gateway: &G,
    exchange: Exchange,
    symbol: &str,
) -> Result<(Decimal, u16), LeveragePreflightError>
where
    G: PositionSnapshotGateway,
{
    let snapshot = gateway.position_snapshot(exchange, symbol).await?;
    if snapshot.exchange != exchange || snapshot.symbol != symbol {
        return Err(LeveragePreflightError::Snapshot(SnapshotError::new(
            "position leverage snapshot identity mismatch",
        )));
    }
    let (signed_quantity, _) = snapshot
        .one_way_position()
        .map_err(LeveragePreflightError::Snapshot)?;
    let leverage = snapshot
        .one_way_leverage()
        .map_err(LeveragePreflightError::Snapshot)?;
    Ok((signed_quantity, leverage))
}

fn validate_acknowledgement(
    acknowledgement: &LeverageAcknowledgement,
    exchange: Exchange,
    symbol: &str,
    expected_leverage: u16,
) -> Result<(), LeveragePreflightError> {
    if acknowledgement.exchange != exchange
        || acknowledgement.symbol != symbol
        || acknowledgement.leverage != expected_leverage
    {
        return Err(LeveragePreflightError::AcknowledgementMismatch);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum LeveragePreflightError {
    #[error("leverage preflight input is invalid")]
    InvalidInput,
    #[error(
        "existing position leverage is {observed}; refusing to change it to requested leverage {requested}"
    )]
    ExistingPositionLeverageMismatch { observed: u16, requested: u16 },
    #[error("leverage request is invalid: {0}")]
    InvalidRequest(String),
    #[error("exchange definitively rejected leverage: {message}")]
    Definitive {
        code: Option<String>,
        message: String,
    },
    #[error("leverage outcome remains unknown: {message}")]
    Unknown { message: String },
    #[error("leverage acknowledgement identity does not match the request")]
    AcknowledgementMismatch,
    #[error(
        "leverage change was acknowledged but not verified: expected {expected}, observed {observed}"
    )]
    NotVerified { expected: u16, observed: u16 },
    #[error("leverage snapshot failed: {0}")]
    Snapshot(#[from] SnapshotError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::exchange::{PositionLeg, PositionSide, PositionSnapshot};

    #[derive(Clone)]
    struct FakeGateway {
        state: Arc<Mutex<FakeState>>,
    }

    struct FakeState {
        snapshots: VecDeque<Result<PositionSnapshot, SnapshotError>>,
        setting: Result<LeverageAcknowledgement, LeverageError>,
        set_calls: usize,
    }

    impl FakeGateway {
        fn new(
            leverages: impl IntoIterator<Item = u16>,
            setting: Result<LeverageAcknowledgement, LeverageError>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    snapshots: leverages
                        .into_iter()
                        .map(|leverage| Ok(position(Some(leverage))))
                        .collect(),
                    setting,
                    set_calls: 0,
                })),
            }
        }

        fn set_calls(&self) -> usize {
            self.state.lock().unwrap().set_calls
        }
    }

    #[async_trait]
    impl PositionSnapshotGateway for FakeGateway {
        async fn position_snapshot(
            &self,
            _exchange: Exchange,
            _symbol: &str,
        ) -> Result<PositionSnapshot, SnapshotError> {
            self.state
                .lock()
                .unwrap()
                .snapshots
                .pop_front()
                .unwrap_or_else(|| Err(SnapshotError::new("missing test snapshot")))
        }
    }

    #[async_trait]
    impl LeverageGateway for FakeGateway {
        async fn set_leverage(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            _leverage: u16,
        ) -> Result<LeverageAcknowledgement, LeverageError> {
            let mut state = self.state.lock().unwrap();
            state.set_calls += 1;
            state.setting.clone()
        }
    }

    fn acknowledgement(leverage: u16) -> LeverageAcknowledgement {
        LeverageAcknowledgement {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            leverage,
        }
    }

    fn position(leverage: Option<u16>) -> PositionSnapshot {
        position_with_quantity(leverage, Decimal::ZERO)
    }

    fn position_with_quantity(leverage: Option<u16>, signed_quantity: Decimal) -> PositionSnapshot {
        PositionSnapshot {
            exchange: Exchange::Binance,
            symbol: "MUUSDT".into(),
            legs: vec![PositionLeg {
                side: PositionSide::Both,
                signed_quantity,
                entry_price: (!signed_quantity.is_zero()).then_some(Decimal::new(1011, 0)),
                mark_price: Decimal::new(1010, 0),
                unrealized_profit: Decimal::ZERO,
                leverage,
            }],
        }
    }

    #[tokio::test]
    async fn already_configured_leverage_never_sends_a_write() {
        let gateway = FakeGateway::new([5], Ok(acknowledgement(5)));
        let result = ensure_symbol_leverage(&gateway, Exchange::Binance, "muusdt", 5)
            .await
            .unwrap();
        assert_eq!(result, LeveragePreflightResult::AlreadyConfigured);
        assert_eq!(gateway.set_calls(), 0);
    }

    #[tokio::test]
    async fn existing_position_leverage_is_never_changed_by_strategy_startup() {
        let gateway = FakeGateway {
            state: Arc::new(Mutex::new(FakeState {
                snapshots: [Ok(position_with_quantity(Some(3), Decimal::new(-3, 0)))]
                    .into_iter()
                    .collect(),
                setting: Ok(acknowledgement(5)),
                set_calls: 0,
            })),
        };

        assert!(matches!(
            ensure_symbol_leverage(&gateway, Exchange::Binance, "MUUSDT", 5).await,
            Err(LeveragePreflightError::ExistingPositionLeverageMismatch {
                observed: 3,
                requested: 5
            })
        ));
        assert_eq!(gateway.set_calls(), 0);
    }

    #[tokio::test]
    async fn acknowledged_change_requires_a_second_authoritative_snapshot() {
        let gateway = FakeGateway::new([3, 5], Ok(acknowledgement(5)));
        let result = ensure_symbol_leverage(&gateway, Exchange::Binance, "MUUSDT", 5)
            .await
            .unwrap();
        assert_eq!(result, LeveragePreflightResult::ConfiguredAndVerified);
        assert_eq!(gateway.set_calls(), 1);
    }

    #[tokio::test]
    async fn unknown_write_is_resolved_only_by_observing_the_exact_value() {
        let gateway = FakeGateway::new(
            [3, 5],
            Err(LeverageError::Unknown {
                message: "timeout".into(),
            }),
        );
        let result = ensure_symbol_leverage(&gateway, Exchange::Binance, "MUUSDT", 5)
            .await
            .unwrap();
        assert_eq!(result, LeveragePreflightResult::ReconciledAfterUnknown);

        let unresolved = FakeGateway::new(
            [3, 3],
            Err(LeverageError::Unknown {
                message: "timeout".into(),
            }),
        );
        assert!(matches!(
            ensure_symbol_leverage(&unresolved, Exchange::Binance, "MUUSDT", 5).await,
            Err(LeveragePreflightError::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn mismatched_acknowledgement_and_unavailable_leverage_fail_closed() {
        let mismatched = FakeGateway::new([3], Ok(acknowledgement(4)));
        assert!(matches!(
            ensure_symbol_leverage(&mismatched, Exchange::Binance, "MUUSDT", 5).await,
            Err(LeveragePreflightError::AcknowledgementMismatch)
        ));

        let unavailable = FakeGateway {
            state: Arc::new(Mutex::new(FakeState {
                snapshots: [Ok(position(None))].into_iter().collect(),
                setting: Ok(acknowledgement(5)),
                set_calls: 0,
            })),
        };
        assert!(matches!(
            ensure_symbol_leverage(&unavailable, Exchange::Binance, "MUUSDT", 5).await,
            Err(LeveragePreflightError::Snapshot(_))
        ));
        assert_eq!(unavailable.set_calls(), 0);
    }
}
