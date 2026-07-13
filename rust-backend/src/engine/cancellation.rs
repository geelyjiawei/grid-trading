use thiserror::Error;

use crate::{
    domain::{
        CancellationIntent, CancellationState, ClientOrderId, IntentState, TerminalOrderStatus,
    },
    exchange::{CancellationError, OrderCancellationGateway},
    persistence::{IntentStore, LedgerError},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationResult {
    Acknowledged,
    SubmitUnknown,
    Rejected,
    AlreadyAcknowledged,
    AlreadyResolved { status: TerminalOrderStatus },
}

pub async fn cancel_with<G, S>(
    gateway: &G,
    store: &mut S,
    target: CancellationIntent,
    now_ms: u64,
) -> Result<CancellationResult, CancellationServiceError>
where
    G: OrderCancellationGateway,
    S: IntentStore,
{
    target.validate()?;
    if target.state != CancellationState::Prepared {
        return Err(CancellationServiceError::TargetNotPrepared);
    }
    let client_order_id = target.client_order_id.clone();
    if let Some(existing) = store
        .snapshot()
        .cancellations
        .get(&client_order_id)
        .cloned()
    {
        if !existing.has_same_target(&target) {
            return Err(CancellationServiceError::TargetMismatch);
        }
        match existing.state {
            CancellationState::Acknowledged => {
                return Ok(CancellationResult::AlreadyAcknowledged);
            }
            CancellationState::Resolved { status } => {
                return Ok(CancellationResult::AlreadyResolved { status });
            }
            CancellationState::Rejected { .. } => {
                return Ok(CancellationResult::Rejected);
            }
            CancellationState::Prepared | CancellationState::SubmitUnknown { .. } => {
                validate_order_target(store, &target)?;
            }
        }
    } else {
        validate_order_target(store, &target)?;
        store.insert_cancellation_prepared(target.clone())?;
    }

    match gateway
        .cancel_order(
            target.exchange,
            &target.symbol,
            &target.client_order_id,
            &target.exchange_order_id,
        )
        .await
    {
        Ok(acknowledgement)
            if acknowledgement.client_order_id == target.client_order_id
                && acknowledgement.exchange_order_id == target.exchange_order_id =>
        {
            store.transition_cancellation(
                &client_order_id,
                CancellationState::Acknowledged,
                now_ms,
            )?;
            Ok(CancellationResult::Acknowledged)
        }
        Ok(_) => {
            store.transition_cancellation(
                &client_order_id,
                CancellationState::SubmitUnknown {
                    message: "cancellation acknowledgement identity is mismatched".into(),
                },
                now_ms,
            )?;
            Ok(CancellationResult::SubmitUnknown)
        }
        Err(CancellationError::Unknown { message }) => {
            store.transition_cancellation(
                &client_order_id,
                CancellationState::SubmitUnknown { message },
                now_ms,
            )?;
            Ok(CancellationResult::SubmitUnknown)
        }
        Err(CancellationError::Invalid { message }) => {
            store.transition_cancellation(
                &client_order_id,
                CancellationState::Rejected { message },
                now_ms,
            )?;
            Ok(CancellationResult::Rejected)
        }
    }
}

pub fn resolve_cancellation_with<S: IntentStore>(
    store: &mut S,
    client_order_id: &ClientOrderId,
    status: TerminalOrderStatus,
    now_ms: u64,
) -> Result<bool, CancellationServiceError> {
    let Some(cancellation) = store.snapshot().cancellations.get(client_order_id).cloned() else {
        return Ok(false);
    };
    if cancellation.state == (CancellationState::Resolved { status }) {
        return Ok(false);
    }
    if cancellation.state.is_resolved()
        || matches!(cancellation.state, CancellationState::Rejected { .. })
    {
        return Err(CancellationServiceError::InvalidResolution);
    }
    let order = store
        .snapshot()
        .intents
        .get(client_order_id)
        .ok_or(CancellationServiceError::MissingOrderIntent)?;
    if !matches!(order.state, IntentState::Terminal { status: order_status } if order_status == status)
    {
        return Err(CancellationServiceError::InvalidResolution);
    }
    store.transition_cancellation(
        client_order_id,
        CancellationState::Resolved { status },
        now_ms,
    )?;
    Ok(true)
}

fn validate_order_target<S: IntentStore>(
    store: &S,
    target: &CancellationIntent,
) -> Result<(), CancellationServiceError> {
    let order = store
        .snapshot()
        .intents
        .get(&target.client_order_id)
        .ok_or(CancellationServiceError::MissingOrderIntent)?;
    if order.exchange != target.exchange || order.shape.symbol != target.symbol {
        return Err(CancellationServiceError::TargetMismatch);
    }
    match &order.state {
        IntentState::Accepted { exchange_order_id }
            if exchange_order_id == &target.exchange_order_id =>
        {
            Ok(())
        }
        _ => Err(CancellationServiceError::OrderNotAccepted),
    }
}

#[derive(Debug, Error)]
pub enum CancellationServiceError {
    #[error("cancellation target is invalid: {0}")]
    InvalidTarget(#[from] crate::domain::CancellationIntentError),
    #[error("new cancellation target must be prepared")]
    TargetNotPrepared,
    #[error("cancellation target differs from the immutable order")]
    TargetMismatch,
    #[error("cancellation has no matching order intent")]
    MissingOrderIntent,
    #[error("only an accepted exchange order can be cancelled")]
    OrderNotAccepted,
    #[error("resolved or rejected cancellation cannot change terminal status")]
    InvalidResolution,
    #[error("cancellation ledger persistence failed: {0}")]
    Persistence(#[from] LedgerError),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{Exchange, OrderIntent, OrderKind, OrderShape, OrderSide, TimeInForce},
        exchange::CancellationAcknowledgement,
        persistence::MemoryOrderIntentStore,
    };

    type CancellationCalls = Arc<Mutex<Vec<(ClientOrderId, String)>>>;

    #[derive(Clone)]
    struct FakeGateway {
        calls: CancellationCalls,
        result: Arc<Mutex<Result<CancellationAcknowledgement, CancellationError>>>,
    }

    #[async_trait]
    impl OrderCancellationGateway for FakeGateway {
        async fn cancel_order(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
            exchange_order_id: &str,
        ) -> Result<CancellationAcknowledgement, CancellationError> {
            self.calls
                .lock()
                .unwrap()
                .push((client_order_id.clone(), exchange_order_id.to_owned()));
            self.result.lock().unwrap().clone()
        }
    }

    fn accepted_store() -> (MemoryOrderIntentStore, OrderIntent) {
        let mut store = MemoryOrderIntentStore::default();
        let intent = OrderIntent::prepare(
            ClientOrderId::parse("g_1_S_cancel").unwrap(),
            Exchange::Binance,
            OrderShape {
                symbol: "MUUSDT".into(),
                side: OrderSide::Sell,
                price: Some(Decimal::new(1015, 0)),
                quantity: Decimal::new(2, 1),
                reduce_only: false,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            100,
        )
        .unwrap();
        store.insert_prepared(intent.clone()).unwrap();
        store
            .transition(
                &intent.client_order_id,
                IntentState::Accepted {
                    exchange_order_id: "exchange-1".into(),
                },
                101,
            )
            .unwrap();
        (store, intent)
    }

    fn target(intent: &OrderIntent) -> CancellationIntent {
        CancellationIntent::prepare(
            intent.client_order_id.clone(),
            "exchange-1",
            intent.exchange,
            intent.shape.symbol.clone(),
            110,
        )
        .unwrap()
    }

    fn gateway(
        result: Result<CancellationAcknowledgement, CancellationError>,
    ) -> (FakeGateway, CancellationCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            FakeGateway {
                calls: calls.clone(),
                result: Arc::new(Mutex::new(result)),
            },
            calls,
        )
    }

    #[tokio::test]
    async fn write_ahead_failure_prevents_cancellation_request() {
        let (mut store, order) = accepted_store();
        store.fail_next_write();
        let (gateway, calls) = gateway(Ok(CancellationAcknowledgement {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-1".into(),
        }));

        assert!(matches!(
            cancel_with(&gateway, &mut store, target(&order), 111).await,
            Err(CancellationServiceError::Persistence(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_cancellation_retries_only_the_same_immutable_target() {
        let (mut store, order) = accepted_store();
        let (gateway, calls) = gateway(Err(CancellationError::Unknown {
            message: "timeout".into(),
        }));

        assert_eq!(
            cancel_with(&gateway, &mut store, target(&order), 111)
                .await
                .unwrap(),
            CancellationResult::SubmitUnknown
        );
        assert_eq!(
            cancel_with(&gateway, &mut store, target(&order), 112)
                .await
                .unwrap(),
            CancellationResult::SubmitUnknown
        );
        assert_eq!(calls.lock().unwrap().len(), 2);

        let mut foreign = target(&order);
        foreign.exchange_order_id = "exchange-2".into();
        assert!(matches!(
            cancel_with(&gateway, &mut store, foreign, 113).await,
            Err(CancellationServiceError::TargetMismatch)
        ));
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn authoritative_acknowledgement_is_not_sent_twice() {
        let (mut store, order) = accepted_store();
        let (gateway, calls) = gateway(Ok(CancellationAcknowledgement {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-1".into(),
        }));

        assert_eq!(
            cancel_with(&gateway, &mut store, target(&order), 111)
                .await
                .unwrap(),
            CancellationResult::Acknowledged
        );
        assert_eq!(
            cancel_with(&gateway, &mut store, target(&order), 112)
                .await
                .unwrap(),
            CancellationResult::AlreadyAcknowledged
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_acknowledgement_commit_retries_only_the_same_cancellation_target() {
        let (mut store, order) = accepted_store();
        store.fail_on_write(4);
        let (gateway, calls) = gateway(Ok(CancellationAcknowledgement {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-1".into(),
        }));

        assert!(matches!(
            cancel_with(&gateway, &mut store, target(&order), 111).await,
            Err(CancellationServiceError::Persistence(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert_eq!(
            store
                .snapshot()
                .cancellations
                .get(&order.client_order_id)
                .unwrap()
                .state,
            CancellationState::Prepared
        );
        assert_eq!(
            cancel_with(&gateway, &mut store, target(&order), 112)
                .await
                .unwrap(),
            CancellationResult::Acknowledged
        );
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn cancellation_cannot_resolve_before_the_order_is_authoritatively_terminal() {
        let (mut store, order) = accepted_store();
        let (gateway, _) = gateway(Ok(CancellationAcknowledgement {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-1".into(),
        }));
        cancel_with(&gateway, &mut store, target(&order), 111)
            .await
            .unwrap();

        assert!(matches!(
            resolve_cancellation_with(
                &mut store,
                &order.client_order_id,
                TerminalOrderStatus::Cancelled,
                112,
            ),
            Err(CancellationServiceError::InvalidResolution)
        ));
        assert_eq!(
            store
                .snapshot()
                .cancellations
                .get(&order.client_order_id)
                .unwrap()
                .state,
            CancellationState::Acknowledged
        );
    }

    #[tokio::test]
    async fn terminal_lookup_resolves_but_never_assumes_cancelled_status() {
        let (mut store, order) = accepted_store();
        let (gateway, _) = gateway(Ok(CancellationAcknowledgement {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-1".into(),
        }));
        cancel_with(&gateway, &mut store, target(&order), 111)
            .await
            .unwrap();
        store
            .transition(
                &order.client_order_id,
                IntentState::Terminal {
                    status: TerminalOrderStatus::Filled,
                },
                112,
            )
            .unwrap();

        assert!(
            resolve_cancellation_with(
                &mut store,
                &order.client_order_id,
                TerminalOrderStatus::Filled,
                113,
            )
            .unwrap()
        );
        assert_eq!(
            store
                .snapshot()
                .cancellations
                .get(&order.client_order_id)
                .unwrap()
                .state,
            CancellationState::Resolved {
                status: TerminalOrderStatus::Filled
            }
        );
    }
}
