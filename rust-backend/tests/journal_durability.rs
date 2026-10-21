use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use grid_trading_server::{
    domain::{
        ClientOrderId, Exchange, IntentState, OrderIntent, OrderKind, OrderShape, OrderSide,
        TimeInForce,
    },
    engine::{SubmissionResult, submit_with},
    exchange::{OrderPlacementGateway, PlacementAcknowledgement, PlacementError},
    persistence::{FileOrderIntentStore, IntentStore},
};
use rust_decimal::Decimal;

struct DiskCheckingGateway {
    path: PathBuf,
    calls: AtomicUsize,
    unknown: bool,
}

#[async_trait]
impl OrderPlacementGateway for DiskCheckingGateway {
    async fn place_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<PlacementAcknowledgement, PlacementError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let recovered = FileOrderIntentStore::load(&self.path).unwrap();
        let durable = &recovered.snapshot().intents[&intent.client_order_id];
        assert_eq!(
            durable, intent,
            "gateway observed an intent not recoverable from disk"
        );
        assert_eq!(durable.state, IntentState::Prepared);
        if self.unknown {
            Err(PlacementError::Unknown {
                message: "injected lost ACK".into(),
            })
        } else {
            Ok(PlacementAcknowledgement {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: "exchange-1".into(),
            })
        }
    }
}

fn intent(id: &str) -> OrderIntent {
    OrderIntent::prepare(
        ClientOrderId::parse(id).unwrap(),
        Exchange::Binance,
        OrderShape {
            symbol: "MUUSDT".into(),
            side: OrderSide::Sell,
            price: Some(Decimal::from(1010)),
            quantity: Decimal::new(2, 1),
            reduce_only: false,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::Gtc,
        },
        100,
    )
    .unwrap()
}

#[tokio::test]
async fn warm_prepared_intent_is_replayable_before_gateway_and_duplicate_never_resubmits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intents.json");
    let mut store = FileOrderIntentStore::load(&path).unwrap();
    store.insert_prepared(intent("seed")).unwrap();
    let gateway = DiskCheckingGateway {
        path: path.clone(),
        calls: AtomicUsize::new(0),
        unknown: false,
    };
    let order = intent("new_order");
    assert!(matches!(
        submit_with(&gateway, &mut store, order.clone(), 101)
            .await
            .unwrap(),
        SubmissionResult::Accepted { .. }
    ));
    assert_eq!(
        FileOrderIntentStore::load(&path).unwrap().snapshot(),
        store.snapshot()
    );
    assert!(submit_with(&gateway, &mut store, order, 102).await.is_err());
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_ack_is_durable_and_cannot_be_retried_without_reconciliation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intents.json");
    let mut store = FileOrderIntentStore::load(&path).unwrap();
    let gateway = DiskCheckingGateway {
        path: path.clone(),
        calls: AtomicUsize::new(0),
        unknown: true,
    };
    let order = intent("unknown_order");
    assert_eq!(
        submit_with(&gateway, &mut store, order.clone(), 101)
            .await
            .unwrap(),
        SubmissionResult::SubmitUnknown
    );
    assert_eq!(
        FileOrderIntentStore::load(&path).unwrap().snapshot(),
        store.snapshot()
    );
    assert!(submit_with(&gateway, &mut store, order, 102).await.is_err());
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_wal_write_never_reaches_gateway_or_publishes_prepared_intent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intents.json");
    let mut store = FileOrderIntentStore::load(&path).unwrap();
    store.insert_prepared(intent("seed")).unwrap();
    let before = store.snapshot().clone();
    std::fs::remove_file(path.with_file_name("intents.json.wal")).unwrap();
    let gateway = DiskCheckingGateway {
        path,
        calls: AtomicUsize::new(0),
        unknown: false,
    };
    assert!(
        submit_with(&gateway, &mut store, intent("must_not_send"), 101)
            .await
            .is_err()
    );
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.snapshot(), &before);
}
