use thiserror::Error;

use crate::{
    domain::{IntentState, OrderIntent},
    exchange::{OrderPlacementGateway, PlacementError},
    persistence::{IntentStore, LedgerError},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionResult {
    Accepted { exchange_order_id: String },
    SubmitUnknown,
    Rejected,
}

pub struct SubmissionService<G, S> {
    gateway: G,
    store: S,
}

impl<G, S> SubmissionService<G, S>
where
    G: OrderPlacementGateway,
    S: IntentStore,
{
    pub fn new(gateway: G, store: S) -> Self {
        Self { gateway, store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn submit(
        &mut self,
        intent: OrderIntent,
        result_time_ms: u64,
    ) -> Result<SubmissionResult, SubmissionError> {
        submit_with(&self.gateway, &mut self.store, intent, result_time_ms).await
    }
}

pub async fn submit_with<G, S>(
    gateway: &G,
    store: &mut S,
    intent: OrderIntent,
    result_time_ms: u64,
) -> Result<SubmissionResult, SubmissionError>
where
    G: OrderPlacementGateway,
    S: IntentStore,
{
    let client_order_id = intent.client_order_id.clone();
    store.insert_prepared(intent.clone())?;

    match gateway.place_order(&intent).await {
        Ok(acknowledgement) => {
            if acknowledgement.client_order_id != client_order_id
                || acknowledgement.exchange_order_id.is_empty()
            {
                store.transition(
                    &client_order_id,
                    IntentState::SubmitUnknown {
                        message: "placement acknowledgement identity is missing or mismatched"
                            .into(),
                    },
                    result_time_ms,
                )?;
                return Ok(SubmissionResult::SubmitUnknown);
            }
            let exchange_order_id = acknowledgement.exchange_order_id;
            store.transition(
                &client_order_id,
                IntentState::Accepted {
                    exchange_order_id: exchange_order_id.clone(),
                },
                result_time_ms,
            )?;
            Ok(SubmissionResult::Accepted { exchange_order_id })
        }
        Err(PlacementError::Unknown { message }) => {
            store.transition(
                &client_order_id,
                IntentState::SubmitUnknown { message },
                result_time_ms,
            )?;
            Ok(SubmissionResult::SubmitUnknown)
        }
        Err(PlacementError::Definitive { code, message }) => {
            store.transition(
                &client_order_id,
                IntentState::Rejected { code, message },
                result_time_ms,
            )?;
            Ok(SubmissionResult::Rejected)
        }
    }
}

#[derive(Debug, Error)]
pub enum SubmissionError {
    #[error("order intent persistence failed: {0}")]
    Persistence(#[from] LedgerError),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{ClientOrderId, Exchange, OrderKind, OrderShape, OrderSide, TimeInForce},
        exchange::PlacementAcknowledgement,
        persistence::MemoryOrderIntentStore,
    };

    struct FakeGateway {
        calls: Arc<Mutex<Vec<String>>>,
        result: Result<PlacementAcknowledgement, PlacementError>,
    }

    #[async_trait]
    impl OrderPlacementGateway for FakeGateway {
        async fn place_order(
            &self,
            intent: &OrderIntent,
        ) -> Result<PlacementAcknowledgement, PlacementError> {
            self.calls
                .lock()
                .unwrap()
                .push(intent.client_order_id.as_str().to_owned());
            self.result.clone()
        }
    }

    fn intent(client_order_id: &str) -> OrderIntent {
        OrderIntent::prepare(
            ClientOrderId::parse(client_order_id).unwrap(),
            Exchange::Aster,
            OrderShape {
                symbol: "ANSEMUSDT".into(),
                side: OrderSide::Buy,
                price: Some(Decimal::new(38, 2)),
                quantity: Decimal::new(100, 0),
                reduce_only: true,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            100,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn unknown_exchange_result_is_durable_and_never_auto_retried() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gateway = FakeGateway {
            calls: calls.clone(),
            result: Err(PlacementError::Unknown {
                message: "connection reset after request body".into(),
            }),
        };
        let mut service = SubmissionService::new(gateway, MemoryOrderIntentStore::default());
        let order = intent("g_0_B_unknown");

        assert_eq!(
            service.submit(order.clone(), 101).await.unwrap(),
            SubmissionResult::SubmitUnknown
        );
        assert!(matches!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::SubmitUnknown { .. }
        ));

        let retry = service.submit(order, 102).await;
        assert!(matches!(
            retry,
            Err(SubmissionError::Persistence(
                LedgerError::DuplicateClientOrderId(_)
            ))
        ));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn write_ahead_failure_prevents_the_exchange_call() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let order = intent("g_0_B_writefail");
        let gateway = FakeGateway {
            calls: calls.clone(),
            result: Ok(PlacementAcknowledgement {
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: "123".into(),
            }),
        };
        let mut store = MemoryOrderIntentStore::default();
        store.fail_next_write();
        let mut service = SubmissionService::new(gateway, store);

        let result = service.submit(order, 101).await;

        assert!(matches!(
            result,
            Err(SubmissionError::Persistence(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert!(calls.lock().unwrap().is_empty());
        assert!(service.store().snapshot().intents.is_empty());
    }

    #[tokio::test]
    async fn definitive_rejection_is_durable_and_cannot_reuse_the_identity() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gateway = FakeGateway {
            calls: calls.clone(),
            result: Err(PlacementError::Definitive {
                code: Some("-2010".into()),
                message: "order rejected".into(),
            }),
        };
        let mut service = SubmissionService::new(gateway, MemoryOrderIntentStore::default());
        let order = intent("g_0_B_rejected");

        assert_eq!(
            service.submit(order.clone(), 101).await.unwrap(),
            SubmissionResult::Rejected
        );
        assert!(matches!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Rejected { .. }
        ));
        assert!(matches!(
            service.submit(order, 102).await,
            Err(SubmissionError::Persistence(
                LedgerError::DuplicateClientOrderId(_)
            ))
        ));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn matching_acknowledgement_is_durably_accepted() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let order = intent("g_0_B_accepted");
        let gateway = FakeGateway {
            calls,
            result: Ok(PlacementAcknowledgement {
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: "789".into(),
            }),
        };
        let mut service = SubmissionService::new(gateway, MemoryOrderIntentStore::default());

        assert_eq!(
            service.submit(order.clone(), 101).await.unwrap(),
            SubmissionResult::Accepted {
                exchange_order_id: "789".into()
            }
        );
        assert_eq!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Accepted {
                exchange_order_id: "789".into()
            }
        );
    }

    #[tokio::test]
    async fn mismatched_success_acknowledgement_becomes_unknown() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gateway = FakeGateway {
            calls,
            result: Ok(PlacementAcknowledgement {
                client_order_id: ClientOrderId::parse("g_9_B_other").unwrap(),
                exchange_order_id: "123".into(),
            }),
        };
        let mut service = SubmissionService::new(gateway, MemoryOrderIntentStore::default());

        assert_eq!(
            service.submit(intent("g_0_B_mismatch"), 101).await.unwrap(),
            SubmissionResult::SubmitUnknown
        );
    }

    #[tokio::test]
    async fn accepted_order_with_failed_final_persist_remains_prepared_for_reconciliation() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let order = intent("g_0_B_persistfail");
        let gateway = FakeGateway {
            calls: calls.clone(),
            result: Ok(PlacementAcknowledgement {
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: "456".into(),
            }),
        };
        let mut store = MemoryOrderIntentStore::default();
        store.fail_on_write(2);
        let mut service = SubmissionService::new(gateway, store);

        let client_order_id = order.client_order_id.clone();
        let result = service.submit(order, 101).await;

        assert!(matches!(
            result,
            Err(SubmissionError::Persistence(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(
            service
                .store()
                .snapshot()
                .intents
                .get(&client_order_id)
                .unwrap()
                .state,
            IntentState::Prepared
        );
    }
}
