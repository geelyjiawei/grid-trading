use thiserror::Error;

use crate::{
    domain::{ClientOrderId, Exchange, IntentState, OrderIntent, TerminalOrderStatus},
    exchange::{AuthoritativeOrder, OrderLifecycle, OrderLookup, OrderLookupGateway},
    persistence::{IntentStore, LedgerError},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationResult {
    Accepted {
        exchange_order_id: String,
    },
    Terminal {
        exchange_order_id: String,
        status: TerminalOrderStatus,
    },
    StillUnknown {
        message: String,
    },
    NotSubmitted {
        message: String,
    },
    OwnershipConflict {
        message: String,
    },
    AlreadyFinal,
}

pub struct ReconciliationService<G, S> {
    gateway: G,
    store: S,
}

impl<G, S> ReconciliationService<G, S>
where
    G: OrderLookupGateway,
    S: IntentStore,
{
    pub fn new(gateway: G, store: S) -> Self {
        Self { gateway, store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn reconcile(
        &mut self,
        client_order_id: &ClientOrderId,
        now_ms: u64,
    ) -> Result<ReconciliationResult, ReconciliationError> {
        reconcile_with(&self.gateway, &mut self.store, client_order_id, now_ms).await
    }
}

pub async fn reconcile_with<G, S>(
    gateway: &G,
    store: &mut S,
    client_order_id: &ClientOrderId,
    now_ms: u64,
) -> Result<ReconciliationResult, ReconciliationError>
where
    G: OrderLookupGateway,
    S: IntentStore,
{
    let intent = store
        .snapshot()
        .intents
        .get(client_order_id)
        .cloned()
        .ok_or_else(|| LedgerError::MissingIntent(client_order_id.as_str().to_owned()))?;

    if !intent_requires_lookup(&intent) {
        return Ok(ReconciliationResult::AlreadyFinal);
    }

    let lookup = gateway
        .lookup_order_by_client_id(
            intent.exchange,
            &intent.shape.symbol,
            &intent.client_order_id,
        )
        .await;

    reconcile_lookup_with(store, client_order_id, lookup, now_ms)
}

pub(crate) fn intent_requires_lookup(intent: &OrderIntent) -> bool {
    if matches!(intent.state, IntentState::RetryableNotSubmitted { .. }) {
        return false;
    }
    !matches!(
        intent.state,
        IntentState::Rejected { .. } | IntentState::OwnershipConflict { .. }
    ) && !matches!(
        intent.state,
        IntentState::Terminal {
            exchange_order_id: Some(_),
            ..
        }
    )
}

pub(crate) fn reconcile_lookup_with<S>(
    store: &mut S,
    client_order_id: &ClientOrderId,
    lookup: Result<OrderLookup, crate::exchange::LookupError>,
    now_ms: u64,
) -> Result<ReconciliationResult, ReconciliationError>
where
    S: IntentStore,
{
    let intent = store
        .snapshot()
        .intents
        .get(client_order_id)
        .cloned()
        .ok_or_else(|| LedgerError::MissingIntent(client_order_id.as_str().to_owned()))?;
    if !intent_requires_lookup(&intent) {
        return Ok(ReconciliationResult::AlreadyFinal);
    }

    match lookup {
        Ok(OrderLookup::Found(snapshot)) => reconcile_found(store, intent, snapshot, now_ms),
        Ok(OrderLookup::NotFound) if legacy_local_cooldown_was_not_submitted(&intent) => {
            let message =
                "local Binance cooldown prevented submission; authoritative lookup confirms the order is absent"
                    .to_owned();
            store.transition(
                &intent.client_order_id,
                IntentState::RetryableNotSubmitted {
                    message: message.clone(),
                },
                now_ms,
            )?;
            Ok(ReconciliationResult::NotSubmitted { message })
        }
        Ok(OrderLookup::NotFound) => preserve_unknown(
            store,
            intent,
            "order is not currently visible in the authoritative exchange lookup",
            now_ms,
        ),
        Err(error) => preserve_unknown(store, intent, &error.message, now_ms),
    }
}

fn legacy_local_cooldown_was_not_submitted(intent: &OrderIntent) -> bool {
    let IntentState::SubmitUnknown { message } = &intent.state else {
        return false;
    };
    if intent.exchange != Exchange::Binance {
        return false;
    }
    let Some(delay) = message
        .strip_prefix("Binance request cooldown is active; retry after ")
        .and_then(|value| value.strip_suffix(" ms"))
    else {
        return false;
    };
    !delay.is_empty() && delay.bytes().all(|byte| byte.is_ascii_digit())
}

fn preserve_unknown<S: IntentStore>(
    store: &mut S,
    intent: OrderIntent,
    message: &str,
    now_ms: u64,
) -> Result<ReconciliationResult, ReconciliationError> {
    if intent.state == IntentState::Prepared {
        store.transition(
            &intent.client_order_id,
            IntentState::SubmitUnknown {
                message: message.to_owned(),
            },
            now_ms,
        )?;
    }
    Ok(ReconciliationResult::StillUnknown {
        message: message.to_owned(),
    })
}

fn reconcile_found<S: IntentStore>(
    store: &mut S,
    intent: OrderIntent,
    snapshot: AuthoritativeOrder,
    now_ms: u64,
) -> Result<ReconciliationResult, ReconciliationError> {
    if let Some(message) = ownership_conflict(&intent, &snapshot) {
        if matches!(intent.state, IntentState::Terminal { .. }) {
            return Ok(ReconciliationResult::OwnershipConflict { message });
        }
        store.transition(
            &intent.client_order_id,
            IntentState::OwnershipConflict {
                message: message.clone(),
            },
            now_ms,
        )?;
        return Ok(ReconciliationResult::OwnershipConflict { message });
    }

    let exchange_order_id = snapshot.exchange_order_id;
    if let IntentState::Terminal {
        status: persisted_status,
        exchange_order_id: None,
    } = intent.state
    {
        let OrderLifecycle::Terminal(authoritative_status) = snapshot.lifecycle else {
            return Ok(ReconciliationResult::OwnershipConflict {
                message: "legacy terminal intent regressed to an active exchange order".into(),
            });
        };
        if persisted_status != authoritative_status {
            return Ok(ReconciliationResult::OwnershipConflict {
                message: "legacy terminal intent status differs from the exchange".into(),
            });
        }
        store.transition(
            &intent.client_order_id,
            IntentState::Terminal {
                status: authoritative_status,
                exchange_order_id: Some(exchange_order_id.clone()),
            },
            now_ms,
        )?;
        return Ok(ReconciliationResult::Terminal {
            exchange_order_id,
            status: authoritative_status,
        });
    }
    if !matches!(intent.state, IntentState::Accepted { .. }) {
        store.transition(
            &intent.client_order_id,
            IntentState::Accepted {
                exchange_order_id: exchange_order_id.clone(),
            },
            now_ms,
        )?;
    }

    match snapshot.lifecycle {
        OrderLifecycle::Active(_) => Ok(ReconciliationResult::Accepted { exchange_order_id }),
        OrderLifecycle::Terminal(status) => {
            store.transition(
                &intent.client_order_id,
                IntentState::Terminal {
                    status,
                    exchange_order_id: Some(exchange_order_id.clone()),
                },
                now_ms,
            )?;
            Ok(ReconciliationResult::Terminal {
                exchange_order_id,
                status,
            })
        }
    }
}

fn ownership_conflict(intent: &OrderIntent, snapshot: &AuthoritativeOrder) -> Option<String> {
    if snapshot.client_order_id != intent.client_order_id {
        return Some("exchange returned a different client order ID".into());
    }
    if snapshot.exchange_order_id.is_empty() {
        return Some("exchange returned an empty order ID".into());
    }
    if snapshot.exchange != intent.exchange {
        return Some("exchange identity does not match the persisted intent".into());
    }
    if snapshot.shape != intent.shape {
        return Some("exchange order shape does not match the persisted intent".into());
    }
    if let Some(exchange_order_id) = intent.state.exchange_order_id()
        && exchange_order_id != snapshot.exchange_order_id
    {
        return Some("exchange order ID changed after acceptance".into());
    }
    None
}

#[derive(Debug, Error)]
pub enum ReconciliationError {
    #[error("order reconciliation persistence failed: {0}")]
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
        domain::{Exchange, OrderKind, OrderShape, OrderSide, TimeInForce},
        exchange::{ActiveOrderStatus, LookupError},
        persistence::{FileOrderIntentStore, LedgerSnapshot, MemoryOrderIntentStore},
    };

    struct FakeLookupGateway {
        calls: Arc<Mutex<Vec<String>>>,
        result: Result<OrderLookup, LookupError>,
    }

    #[async_trait]
    impl OrderLookupGateway for FakeLookupGateway {
        async fn lookup_order_by_client_id(
            &self,
            _exchange: Exchange,
            _symbol: &str,
            client_order_id: &ClientOrderId,
        ) -> Result<OrderLookup, LookupError> {
            self.calls
                .lock()
                .unwrap()
                .push(client_order_id.as_str().to_owned());
            self.result.clone()
        }
    }

    fn intent_on(client_order_id: &str, exchange: Exchange) -> OrderIntent {
        OrderIntent::prepare(
            ClientOrderId::parse(client_order_id).unwrap(),
            exchange,
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

    fn intent(client_order_id: &str) -> OrderIntent {
        intent_on(client_order_id, Exchange::Aster)
    }

    fn found(order: &OrderIntent, lifecycle: OrderLifecycle) -> OrderLookup {
        OrderLookup::Found(AuthoritativeOrder {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: "exchange-123".into(),
            exchange: order.exchange,
            shape: order.shape.clone(),
            lifecycle,
            executed_quantity: None,
        })
    }

    fn service(
        order: &OrderIntent,
        result: Result<OrderLookup, LookupError>,
    ) -> (
        ReconciliationService<FakeLookupGateway, MemoryOrderIntentStore>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut store = MemoryOrderIntentStore::default();
        store.insert_prepared(order.clone()).unwrap();
        (
            ReconciliationService::new(
                FakeLookupGateway {
                    calls: calls.clone(),
                    result,
                },
                store,
            ),
            calls,
        )
    }

    #[tokio::test]
    async fn prepared_order_found_live_is_accepted_without_resubmission() {
        let order = intent("g_0_B_found");
        let (mut service, calls) = service(
            &order,
            Ok(found(
                &order,
                OrderLifecycle::Active(ActiveOrderStatus::New),
            )),
        );

        assert_eq!(
            service
                .reconcile(&order.client_order_id, 101)
                .await
                .unwrap(),
            ReconciliationResult::Accepted {
                exchange_order_id: "exchange-123".into()
            }
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Accepted {
                exchange_order_id: "exchange-123".into()
            }
        );
    }

    #[tokio::test]
    async fn terminal_order_is_durably_accepted_before_terminal_transition() {
        let order = intent("g_0_B_terminal");
        let (mut service, _) = service(
            &order,
            Ok(found(
                &order,
                OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
            )),
        );

        assert_eq!(
            service
                .reconcile(&order.client_order_id, 101)
                .await
                .unwrap(),
            ReconciliationResult::Terminal {
                exchange_order_id: "exchange-123".into(),
                status: TerminalOrderStatus::Filled,
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
            IntentState::Terminal {
                status: TerminalOrderStatus::Filled,
                exchange_order_id: Some("exchange-123".into()),
            }
        );
        assert_eq!(service.store().snapshot().revision, 3);
    }

    #[tokio::test]
    async fn not_found_never_authorizes_a_replacement_order() {
        let order = intent("g_0_B_missing");
        let (mut service, calls) = service(&order, Ok(OrderLookup::NotFound));

        assert!(matches!(
            service
                .reconcile(&order.client_order_id, 101)
                .await
                .unwrap(),
            ReconciliationResult::StillUnknown { .. }
        ));
        let revision = service.store().snapshot().revision;
        assert!(matches!(
            service
                .reconcile(&order.client_order_id, 102)
                .await
                .unwrap(),
            ReconciliationResult::StillUnknown { .. }
        ));
        assert_eq!(service.store().snapshot().revision, revision);
        assert_eq!(calls.lock().unwrap().len(), 2);
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
    }

    #[tokio::test]
    async fn authoritative_not_found_migrates_the_legacy_local_binance_cooldown() {
        let order = intent_on("g_0_B_legacy_cooldown", Exchange::Binance);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut store = MemoryOrderIntentStore::default();
        store.insert_prepared(order.clone()).unwrap();
        store
            .transition(
                &order.client_order_id,
                IntentState::SubmitUnknown {
                    message: "Binance request cooldown is active; retry after 5 ms".into(),
                },
                101,
            )
            .unwrap();
        let mut service = ReconciliationService::new(
            FakeLookupGateway {
                calls: calls.clone(),
                result: Ok(OrderLookup::NotFound),
            },
            store,
        );

        assert!(matches!(
            service
                .reconcile(&order.client_order_id, 102)
                .await
                .unwrap(),
            ReconciliationResult::NotSubmitted { .. }
        ));
        assert!(matches!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::RetryableNotSubmitted { .. }
        ));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn legacy_cooldown_migration_rejects_other_exchanges_and_inexact_messages() {
        for (exchange, message) in [
            (
                Exchange::Aster,
                "Binance request cooldown is active; retry after 5 ms",
            ),
            (
                Exchange::Binance,
                "Binance request cooldown is active; retry after unknown ms",
            ),
            (Exchange::Binance, "Too many requests"),
        ] {
            let order = intent_on("g_0_B_not_legacy_cooldown", exchange);
            let mut store = MemoryOrderIntentStore::default();
            store.insert_prepared(order.clone()).unwrap();
            store
                .transition(
                    &order.client_order_id,
                    IntentState::SubmitUnknown {
                        message: message.into(),
                    },
                    101,
                )
                .unwrap();
            let mut service = ReconciliationService::new(
                FakeLookupGateway {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    result: Ok(OrderLookup::NotFound),
                },
                store,
            );

            assert!(matches!(
                service
                    .reconcile(&order.client_order_id, 102)
                    .await
                    .unwrap(),
                ReconciliationResult::StillUnknown { .. }
            ));
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
        }
    }

    #[tokio::test]
    async fn lookup_transport_failure_stays_unknown() {
        let order = intent("g_0_B_lookupfail");
        let (mut service, _) = service(
            &order,
            Err(LookupError {
                message: "query timed out".into(),
            }),
        );

        assert_eq!(
            service
                .reconcile(&order.client_order_id, 101)
                .await
                .unwrap(),
            ReconciliationResult::StillUnknown {
                message: "query timed out".into()
            }
        );
    }

    #[tokio::test]
    async fn shape_mismatch_is_a_durable_ownership_conflict() {
        let order = intent("g_0_B_conflict");
        let mut snapshot = match found(
            &order,
            OrderLifecycle::Active(ActiveOrderStatus::PartiallyFilled),
        ) {
            OrderLookup::Found(snapshot) => snapshot,
            OrderLookup::NotFound => unreachable!(),
        };
        snapshot.shape.quantity = Decimal::new(70, 0);
        let (mut service, calls) = service(&order, Ok(OrderLookup::Found(snapshot)));

        assert!(matches!(
            service
                .reconcile(&order.client_order_id, 101)
                .await
                .unwrap(),
            ReconciliationResult::OwnershipConflict { .. }
        ));
        assert!(matches!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::OwnershipConflict { .. }
        ));
        assert_eq!(
            service
                .reconcile(&order.client_order_id, 102)
                .await
                .unwrap(),
            ReconciliationResult::AlreadyFinal
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn crash_between_acceptance_and_terminal_write_recovers_as_accepted() {
        let order = intent("g_0_B_terminalpersist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gateway = FakeLookupGateway {
            calls,
            result: Ok(found(
                &order,
                OrderLifecycle::Terminal(TerminalOrderStatus::Cancelled),
            )),
        };
        let mut store = MemoryOrderIntentStore::default();
        store.insert_prepared(order.clone()).unwrap();
        store.fail_on_write(3);
        let mut service = ReconciliationService::new(gateway, store);

        assert!(matches!(
            service.reconcile(&order.client_order_id, 101).await,
            Err(ReconciliationError::Persistence(
                LedgerError::InjectedWriteFailure
            ))
        ));
        assert_eq!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Accepted {
                exchange_order_id: "exchange-123".into()
            }
        );
    }

    #[tokio::test]
    async fn legacy_terminal_without_exchange_id_is_authoritatively_enriched() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("legacy-terminal-ledger.json");
        let mut order = intent("g_0_B_legacyterminal");
        order.state = IntentState::Terminal {
            status: TerminalOrderStatus::Filled,
            exchange_order_id: None,
        };
        order.updated_at_ms = 101;
        let mut snapshot = LedgerSnapshot {
            revision: 1,
            ..LedgerSnapshot::default()
        };
        snapshot
            .intents
            .insert(order.client_order_id.clone(), order.clone());
        fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();
        let gateway = FakeLookupGateway {
            calls: Arc::new(Mutex::new(Vec::new())),
            result: Ok(found(
                &order,
                OrderLifecycle::Terminal(TerminalOrderStatus::Filled),
            )),
        };
        let mut service =
            ReconciliationService::new(gateway, FileOrderIntentStore::load(&path).unwrap());

        assert_eq!(
            service
                .reconcile(&order.client_order_id, 102)
                .await
                .unwrap(),
            ReconciliationResult::Terminal {
                exchange_order_id: "exchange-123".into(),
                status: TerminalOrderStatus::Filled,
            }
        );
        assert!(matches!(
            service
                .store()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Terminal {
                status: TerminalOrderStatus::Filled,
                exchange_order_id: Some(ref exchange_order_id),
            } if exchange_order_id == "exchange-123"
        ));
        drop(service);
        assert!(matches!(
            FileOrderIntentStore::load(&path)
                .unwrap()
                .snapshot()
                .intents
                .get(&order.client_order_id)
                .unwrap()
                .state,
            IntentState::Terminal {
                exchange_order_id: Some(_),
                ..
            }
        ));
    }
}
