use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::{
    domain::{
        CancellationIntent, CancellationState, ClientOrderId, IntentState, TerminalOrderStatus,
    },
    exchange::{CancellationError, OrderCancellationGateway, OrderCancellationTarget},
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

pub async fn cancel_many_with<G, S>(
    gateway: &G,
    store: &mut S,
    targets: Vec<CancellationIntent>,
    now_ms: u64,
) -> Result<Vec<(ClientOrderId, CancellationResult)>, CancellationServiceError>
where
    G: OrderCancellationGateway,
    S: IntentStore,
{
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let exchange = targets[0].exchange;
    let symbol = targets[0].symbol.clone();
    let mut target_order = Vec::with_capacity(targets.len());
    let mut seen = BTreeSet::new();
    let mut prepared = Vec::new();
    let mut dispatch = Vec::new();
    let mut results = BTreeMap::new();

    for target in targets {
        if target.state != CancellationState::Prepared {
            return Err(CancellationServiceError::TargetNotPrepared);
        }
        target.validate()?;
        if target.exchange != exchange || target.symbol != symbol {
            return Err(CancellationServiceError::TargetMismatch);
        }
        let client_order_id = target.client_order_id.clone();
        if !seen.insert(client_order_id.clone()) {
            return Err(CancellationServiceError::DuplicateTarget);
        }
        target_order.push(client_order_id.clone());
        match store
            .snapshot()
            .cancellations
            .get(&client_order_id)
            .cloned()
        {
            Some(existing) if !existing.has_same_target(&target) => {
                return Err(CancellationServiceError::TargetMismatch);
            }
            Some(existing) => match existing.state {
                CancellationState::Acknowledged => {
                    results.insert(client_order_id, CancellationResult::AlreadyAcknowledged);
                }
                CancellationState::Resolved { status } => {
                    results.insert(
                        client_order_id,
                        CancellationResult::AlreadyResolved { status },
                    );
                }
                CancellationState::Rejected { .. } => {
                    results.insert(client_order_id, CancellationResult::Rejected);
                }
                CancellationState::Prepared | CancellationState::SubmitUnknown { .. } => {
                    validate_order_target(store, &target)?;
                    dispatch.push(target);
                }
            },
            None => {
                validate_order_target(store, &target)?;
                prepared.push(target.clone());
                dispatch.push(target);
            }
        }
    }

    store.insert_cancellations_prepared(prepared)?;
    let requests = dispatch
        .iter()
        .map(|target| OrderCancellationTarget {
            client_order_id: target.client_order_id.clone(),
            exchange_order_id: target.exchange_order_id.clone(),
        })
        .collect::<Vec<_>>();
    let mut outcomes = gateway.cancel_orders(exchange, &symbol, &requests).await;
    if outcomes.len() != dispatch.len() {
        outcomes = dispatch
            .iter()
            .map(|_| {
                Err(CancellationError::Unknown {
                    message: "batch cancellation returned an incomplete result set".into(),
                })
            })
            .collect();
    }

    let mut transitions = Vec::with_capacity(dispatch.len());
    for (target, outcome) in dispatch.into_iter().zip(outcomes) {
        let client_order_id = target.client_order_id.clone();
        let (state, result) = match outcome {
            Ok(acknowledgement)
                if acknowledgement.client_order_id == target.client_order_id
                    && acknowledgement.exchange_order_id == target.exchange_order_id =>
            {
                (
                    CancellationState::Acknowledged,
                    CancellationResult::Acknowledged,
                )
            }
            Ok(_) => (
                CancellationState::SubmitUnknown {
                    message: "cancellation acknowledgement identity is mismatched".into(),
                },
                CancellationResult::SubmitUnknown,
            ),
            Err(CancellationError::Unknown { message }) => (
                CancellationState::SubmitUnknown { message },
                CancellationResult::SubmitUnknown,
            ),
            Err(CancellationError::Invalid { message }) => (
                CancellationState::Rejected { message },
                CancellationResult::Rejected,
            ),
        };
        transitions.push((client_order_id.clone(), state));
        results.insert(client_order_id, result);
    }
    store.transition_cancellations(transitions, now_ms)?;

    target_order
        .into_iter()
        .map(|client_order_id| {
            let result = results
                .remove(&client_order_id)
                .ok_or(CancellationServiceError::IncompleteBatchResult)?;
            Ok((client_order_id, result))
        })
        .collect()
}

pub fn resolve_cancellation_with<S: IntentStore>(
    store: &mut S,
    client_order_id: &ClientOrderId,
    status: TerminalOrderStatus,
    now_ms: u64,
) -> Result<bool, CancellationServiceError> {
    Ok(resolve_cancellations_with(store, vec![(client_order_id.clone(), status)], now_ms)? == 1)
}

pub fn resolve_cancellations_with<S: IntentStore>(
    store: &mut S,
    resolutions: Vec<(ClientOrderId, TerminalOrderStatus)>,
    now_ms: u64,
) -> Result<usize, CancellationServiceError> {
    let mut transitions = Vec::new();
    let mut seen = BTreeSet::new();
    for (client_order_id, status) in resolutions {
        if !seen.insert(client_order_id.clone()) {
            return Err(CancellationServiceError::DuplicateTarget);
        }
        let Some(cancellation) = store
            .snapshot()
            .cancellations
            .get(&client_order_id)
            .cloned()
        else {
            continue;
        };
        if cancellation.state == (CancellationState::Resolved { status }) {
            continue;
        }
        if cancellation.state.is_resolved()
            || matches!(cancellation.state, CancellationState::Rejected { .. })
        {
            return Err(CancellationServiceError::InvalidResolution);
        }
        let order = store
            .snapshot()
            .intents
            .get(&client_order_id)
            .ok_or(CancellationServiceError::MissingOrderIntent)?;
        if !matches!(
            order.state,
            IntentState::Terminal {
                status: order_status,
                exchange_order_id: Some(ref exchange_order_id),
            } if order_status == status && exchange_order_id == &cancellation.exchange_order_id
        ) {
            return Err(CancellationServiceError::InvalidResolution);
        }
        transitions.push((client_order_id, CancellationState::Resolved { status }));
    }
    let resolved = transitions.len();
    store.transition_cancellations(transitions, now_ms)?;
    Ok(resolved)
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
    #[error("duplicate cancellation target in one batch")]
    DuplicateTarget,
    #[error("batch cancellation result set is incomplete")]
    IncompleteBatchResult,
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
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{Exchange, OrderIntent, OrderKind, OrderShape, OrderSide, TimeInForce},
        exchange::CancellationAcknowledgement,
        persistence::{FileOrderIntentStore, MemoryOrderIntentStore},
    };

    type CancellationCalls = Arc<Mutex<Vec<(ClientOrderId, String)>>>;

    #[derive(Clone)]
    struct FakeGateway {
        calls: CancellationCalls,
        result: Arc<Mutex<Result<CancellationAcknowledgement, CancellationError>>>,
    }

    #[derive(Clone, Default)]
    struct BatchGateway {
        calls: Arc<Mutex<Vec<Vec<OrderCancellationTarget>>>>,
    }

    #[async_trait]
    impl OrderCancellationGateway for BatchGateway {
        async fn cancel_order(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            _client_order_id: &ClientOrderId,
            _exchange_order_id: &str,
        ) -> Result<CancellationAcknowledgement, CancellationError> {
            panic!("batch cancellation must not fall back to single-order requests")
        }

        async fn cancel_orders(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            targets: &[OrderCancellationTarget],
        ) -> Vec<Result<CancellationAcknowledgement, CancellationError>> {
            self.calls.lock().unwrap().push(targets.to_vec());
            targets
                .iter()
                .map(|target| {
                    Ok(CancellationAcknowledgement {
                        client_order_id: target.client_order_id.clone(),
                        exchange_order_id: target.exchange_order_id.clone(),
                    })
                })
                .collect()
        }
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
    async fn batch_cancellation_dispatches_once_and_persists_twice_for_many_orders() {
        let mut store = MemoryOrderIntentStore::default();
        let mut targets = Vec::new();
        for index in 0..3 {
            let client_order_id = ClientOrderId::parse(format!("g_{index}_S_batch")).unwrap();
            let exchange_order_id = format!("exchange-{index}");
            let intent = OrderIntent::prepare(
                client_order_id.clone(),
                Exchange::Binance,
                OrderShape {
                    symbol: "MUUSDT".into(),
                    side: OrderSide::Sell,
                    price: Some(Decimal::new(1015 + index, 0)),
                    quantity: Decimal::new(2, 1),
                    reduce_only: false,
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::Gtc,
                },
                100,
            )
            .unwrap();
            store.insert_prepared(intent).unwrap();
            store
                .transition(
                    &client_order_id,
                    IntentState::Accepted {
                        exchange_order_id: exchange_order_id.clone(),
                    },
                    101,
                )
                .unwrap();
            targets.push(
                CancellationIntent::prepare(
                    client_order_id,
                    exchange_order_id,
                    Exchange::Binance,
                    "MUUSDT",
                    110,
                )
                .unwrap(),
            );
        }
        let revision_before = store.snapshot().revision;
        let gateway = BatchGateway::default();
        let mut invalid_targets = targets.clone();
        invalid_targets[0].state = CancellationState::Acknowledged;

        assert!(matches!(
            cancel_many_with(&gateway, &mut store, invalid_targets, 111).await,
            Err(CancellationServiceError::TargetNotPrepared)
        ));
        assert_eq!(store.snapshot().revision, revision_before);
        assert!(gateway.calls.lock().unwrap().is_empty());

        let results = cancel_many_with(&gateway, &mut store, targets, 111)
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|(_, result)| { matches!(result, CancellationResult::Acknowledged) })
        );
        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        assert_eq!(store.snapshot().revision, revision_before + 2);
        assert!(
            store
                .snapshot()
                .cancellations
                .values()
                .all(|intent| { intent.state == CancellationState::Acknowledged })
        );
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
                    exchange_order_id: Some("exchange-1".into()),
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

    #[tokio::test]
    async fn legacy_terminal_without_exchange_identity_cannot_resolve_cancellation() {
        let (store, order) = accepted_store();
        let mut snapshot = store.snapshot().clone();
        snapshot
            .intents
            .get_mut(&order.client_order_id)
            .unwrap()
            .state = IntentState::Terminal {
            status: TerminalOrderStatus::Cancelled,
            exchange_order_id: None,
        };
        let mut cancellation = target(&order);
        cancellation.state = CancellationState::Acknowledged;
        cancellation.updated_at_ms = 111;
        snapshot
            .cancellations
            .insert(order.client_order_id.clone(), cancellation);
        let directory = tempdir().unwrap();
        let path = directory.path().join("legacy-cancellation-ledger.json");
        fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();
        let mut restored = FileOrderIntentStore::load(&path).unwrap();
        let before = restored.snapshot().clone();

        assert!(matches!(
            resolve_cancellation_with(
                &mut restored,
                &order.client_order_id,
                TerminalOrderStatus::Cancelled,
                112,
            ),
            Err(CancellationServiceError::InvalidResolution)
        ));
        assert_eq!(restored.snapshot(), &before);
    }
}
