use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_tls_with_config, tungstenite::Message,
};
use zeroize::Zeroizing;

use crate::exchange::protocol::HttpResponse;

const QUEUE_CAPACITY: usize = 64;
const MAX_PENDING: usize = 64;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_MIN: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(15);
const LIFETIME_CHECK: Duration = Duration::from_millis(100);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Reply = oneshot::Sender<Result<HttpResponse, BybitOrderError>>;

#[derive(Debug)]
pub(super) enum BybitOrderError {
    NotSent,
    Unknown,
}

struct Command {
    payload: Value,
    recv_window_ms: u64,
    deadline: Instant,
    reply: Reply,
}

struct Pending {
    deadline: Instant,
    reply: Reply,
}

#[derive(Clone)]
pub(crate) struct BybitOrderRelay {
    connected: Arc<AtomicBool>,
    sender: mpsc::Sender<Command>,
    timeout: Duration,
}

impl BybitOrderRelay {
    pub(super) async fn post(
        &self,
        payload: Value,
        recv_window_ms: u64,
    ) -> Result<HttpResponse, BybitOrderError> {
        if !self.connected.load(Ordering::Acquire) || recv_window_ms == 0 {
            return Err(BybitOrderError::NotSent);
        }
        let (reply, receiver) = oneshot::channel();
        let deadline = Instant::now() + self.timeout;
        self.sender
            .try_send(Command {
                payload,
                recv_window_ms,
                deadline,
                reply,
            })
            .map_err(|_| BybitOrderError::NotSent)?;
        // A queued request may already be in the socket writer. Only the writer
        // can prove NotSent; caller timeout/channel loss is always ambiguous.
        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            _ => Err(BybitOrderError::Unknown),
        }
    }
}

#[cfg(not(test))]
pub(crate) fn spawn_bybit_order_stream(
    testnet: bool,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
) -> Option<BybitOrderRelay> {
    spawn_at(
        if testnet {
            "wss://stream-testnet.bybit.com/v5/trade"
        } else {
            "wss://stream.bybit.com/v5/trade"
        }
        .into(),
        api_key,
        api_secret,
        lifetime,
        Duration::from_secs(10),
    )
}

// Factory tests must not connect to an exchange; transport tests below exercise
// the same actor against a local WebSocket server through spawn_at.
#[cfg(test)]
pub(crate) fn spawn_bybit_order_stream(
    _testnet: bool,
    _api_key: Zeroizing<String>,
    _api_secret: Zeroizing<String>,
    _lifetime: Weak<()>,
) -> Option<BybitOrderRelay> {
    None
}

fn spawn_at(
    url: String,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
    timeout: Duration,
) -> Option<BybitOrderRelay> {
    if api_key.trim().is_empty() || api_secret.is_empty() || lifetime.upgrade().is_none() {
        return None;
    }
    let runtime = tokio::runtime::Handle::try_current().ok()?;
    let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
    let relay = BybitOrderRelay {
        connected: Arc::new(AtomicBool::new(false)),
        sender,
        timeout,
    };
    runtime.spawn(run(
        url,
        api_key,
        api_secret,
        lifetime,
        relay.connected.clone(),
        receiver,
    ));
    Some(relay)
}

struct ConnectionFlag(Arc<AtomicBool>);

impl Drop for ConnectionFlag {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis()
        .try_into()
        .ok()
}

async fn authenticate(
    socket: &mut Socket,
    api_key: &str,
    api_secret: &str,
) -> Result<(), Option<i64>> {
    let expires = now_ms().ok_or(None)?.checked_add(10_000).ok_or(None)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|_| None)?;
    mac.update(format!("GET/realtime{expires}").as_bytes());
    let signature = Zeroizing::new(hex::encode(mac.finalize().into_bytes()));
    #[derive(Serialize)]
    struct Auth<'a> {
        op: &'static str,
        args: (&'a str, u64, &'a str),
    }
    let frame = Zeroizing::new(
        serde_json::to_string(&Auth {
            op: "auth",
            args: (api_key, expires, signature.as_str()),
        })
        .map_err(|_| None)?,
    );
    socket
        .send(Message::Text(frame.as_str().into()))
        .await
        .map_err(|_| None)?;
    loop {
        match socket.next().await.ok_or(None)?.map_err(|_| None)? {
            Message::Text(text) => {
                let response: Value = serde_json::from_str(&text).map_err(|_| None)?;
                return if response["op"] == "auth" && response["retCode"].as_i64() == Some(0) {
                    Ok(())
                } else {
                    Err(response["retCode"].as_i64())
                };
            }
            Message::Ping(data) => socket.send(Message::Pong(data)).await.map_err(|_| None)?,
            Message::Pong(_) => {}
            _ => return Err(None),
        }
    }
}

async fn reject_during_backoff(
    commands: &mut mpsc::Receiver<Command>,
    lifetime: &Weak<()>,
    delay: Duration,
) {
    let deadline = Instant::now() + delay;
    loop {
        if lifetime.upgrade().is_none() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return,
            _ = tokio::time::sleep(LIFETIME_CHECK) => {},
            command = commands.recv() => match command {
                Some(command) => { let _ = command.reply.send(Err(BybitOrderError::NotSent)); }
                None => return,
            }
        }
    }
}

async fn run(
    url: String,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    lifetime: Weak<()>,
    connected: Arc<AtomicBool>,
    mut commands: mpsc::Receiver<Command>,
) {
    let _reset_on_exit = ConnectionFlag(connected.clone());
    let mut backoff = RECONNECT_MIN;
    let mut authenticated_once = false;
    while lifetime.upgrade().is_some() && !commands.is_closed() {
        let connection = tokio::time::timeout(
            CONNECT_TIMEOUT,
            connect_async_tls_with_config(url.as_str(), None, true, None),
        )
        .await;
        let mut socket = match connection {
            Ok(Ok((socket, _))) => socket,
            _ => {
                tracing::warn!(
                    stage = "connect",
                    reconnect_delay_ms = backoff.as_millis() as u64,
                    "Bybit trading WebSocket connection failed"
                );
                reject_during_backoff(&mut commands, &lifetime, backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        let authentication = tokio::time::timeout(
            AUTH_TIMEOUT,
            authenticate(&mut socket, api_key.as_str(), api_secret.as_str()),
        )
        .await;
        if !matches!(&authentication, Ok(Ok(()))) {
            // Never log credentials, auth frames, or remote error text.
            if let Ok(Err(Some(ret_code))) = authentication {
                tracing::warn!(
                    stage = "auth",
                    ret_code,
                    reconnect_delay_ms = backoff.as_millis() as u64,
                    "Bybit trading WebSocket authentication rejected"
                );
            } else {
                tracing::warn!(
                    stage = "auth",
                    reconnect_delay_ms = backoff.as_millis() as u64,
                    "Bybit trading WebSocket authentication failed or timed out"
                );
            }
            reject_during_backoff(&mut commands, &lifetime, backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
            continue;
        }
        if lifetime.upgrade().is_none() {
            break;
        }
        connected.store(true, Ordering::Release);
        tracing::info!(
            reconnected = authenticated_once,
            "authenticated Bybit trading WebSocket connected"
        );
        authenticated_once = true;
        let session_started = Instant::now();
        let mut pending: HashMap<String, Pending> = HashMap::new();
        let mut tick = tokio::time::interval(LIFETIME_CHECK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_ping = Instant::now() + HEARTBEAT_INTERVAL;
        let mut pong_deadline = None;
        loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { break; };
                    if command.reply.is_closed() { continue; }
                    if lifetime.upgrade().is_none() || Instant::now() >= command.deadline || pending.len() >= MAX_PENDING {
                        let _ = command.reply.send(Err(BybitOrderError::NotSent));
                        continue;
                    }
                    let mut random_id = [0u8; 16];
                    let Some(timestamp) = now_ms() else {
                        let _ = command.reply.send(Err(BybitOrderError::NotSent));
                        continue;
                    };
                    if getrandom::fill(&mut random_id).is_err() {
                        let _ = command.reply.send(Err(BybitOrderError::NotSent));
                        continue;
                    }
                    let req_id = hex::encode(random_id);
                    if pending.contains_key(&req_id) {
                        let _ = command.reply.send(Err(BybitOrderError::NotSent));
                        continue;
                    }
                    let frame = json!({
                        "op":"order.create", "reqId":req_id,
                        "header":{"X-BAPI-TIMESTAMP":timestamp.to_string(), "X-BAPI-RECV-WINDOW":command.recv_window_ms.to_string()},
                        "args":[command.payload]
                    });
                    pending.insert(req_id.clone(), Pending { deadline: command.deadline, reply: command.reply });
                    // From the first write attempt onwards there is no safe REST
                    // fallback, even if the socket reports an immediate failure.
                    let write_deadline = command.deadline.min(Instant::now() + WRITE_TIMEOUT);
                    if !matches!(tokio::time::timeout_at(write_deadline, socket.send(Message::Text(frame.to_string().into()))).await, Ok(Ok(()))) {
                        tracing::warn!(req_id, "Bybit trading WebSocket write failed after submission began; outcome unknown");
                        break;
                    }
                }
                message = socket.next() => {
                    let Some(Ok(message)) = message else { break; };
                    match message {
                        Message::Text(text) => {
                            let Ok(response) = serde_json::from_str::<Value>(&text) else { break; };
                            if response["op"] == "pong" && response["retCode"].as_i64() == Some(0) {
                                pong_deadline = None;
                                continue;
                            }
                            let Some(req_id) = response.get("reqId").and_then(Value::as_str) else { break; };
                            let Some(waiting) = pending.remove(req_id) else {
                                if let Some(ret_code) = response["retCode"].as_i64() {
                                    tracing::warn!(ret_code, "Bybit trading WebSocket ignored an uncorrelated response");
                                }
                                continue;
                            };
                            let result = if response["op"] == "order.create" && response["retCode"].as_i64().is_some() {
                                Ok(HttpResponse { status: 200, body: json!({"retCode":response["retCode"], "retMsg":"Bybit WebSocket order response", "result":response["data"]}).to_string() })
                            } else {
                                tracing::warn!(req_id, "Bybit trading WebSocket received an invalid correlated acknowledgement");
                                Err(BybitOrderError::Unknown)
                            };
                            let _ = waiting.reply.send(result);
                        }
                        Message::Ping(data) => {
                            if !matches!(tokio::time::timeout(WRITE_TIMEOUT, socket.send(Message::Pong(data))).await, Ok(Ok(()))) { break; }
                        }
                        Message::Pong(_) => {},
                        Message::Close(_) | Message::Binary(_) | Message::Frame(_) => break,
                    }
                }
                _ = tick.tick() => {
                    if lifetime.upgrade().is_none() || commands.is_closed() { break; }
                    let now = Instant::now();
                    pending.retain(|_, waiting| !waiting.reply.is_closed() && now < waiting.deadline);
                    // Expired reply senders are dropped: post reports Unknown.
                    if pong_deadline.is_some_and(|deadline| now >= deadline) {
                        tracing::warn!("Bybit trading WebSocket heartbeat timed out");
                        break;
                    }
                    if now >= next_ping {
                        if !matches!(tokio::time::timeout(WRITE_TIMEOUT, socket.send(Message::Text(json!({"op":"ping"}).to_string().into()))).await, Ok(Ok(()))) { break; }
                        pong_deadline = Some(now + HEARTBEAT_TIMEOUT);
                        next_ping = now + HEARTBEAT_INTERVAL;
                    }
                }
            }
        }
        connected.store(false, Ordering::Release);
        tracing::info!(
            pending_orders = pending.len(),
            queued_orders = commands.len(),
            "Bybit trading WebSocket disconnected; submitted requests will not be replayed"
        );
        for waiting in pending.into_values() {
            let _ = waiting.reply.send(Err(BybitOrderError::Unknown));
        }
        while let Ok(command) = commands.try_recv() {
            let _ = command.reply.send(Err(BybitOrderError::NotSent));
        }
        if session_started.elapsed() >= HEARTBEAT_INTERVAL {
            backoff = RECONNECT_MIN;
        }
        reject_during_backoff(&mut commands, &lifetime, backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use hmac::{Hmac, Mac};
    use serde_json::json;
    use sha2::Sha256;
    use std::sync::{Mutex, atomic::AtomicUsize};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[derive(Clone, Copy)]
    pub(crate) enum Reply {
        Ack,
        Disconnect,
        Silence,
        ForeignClient,
        ForeignRequest,
        WrongOp,
        Reject,
        Malformed,
        AuthRejected,
    }

    pub(crate) struct Fixture {
        pub relay: BybitOrderRelay,
        pub lifetime: Arc<()>,
        pub orders: Arc<Mutex<Vec<Value>>>,
        pub connections: Arc<AtomicUsize>,
        pub pings: Arc<AtomicUsize>,
        server: JoinHandle<()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl Fixture {
        pub async fn stop(&mut self) {
            self.lifetime = Arc::new(());
            tokio::time::timeout(Duration::from_secs(3), async {
                while !self.relay.sender.is_closed() || self.relay.connected.load(Ordering::Acquire)
                {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("expired weak lifetime must stop the writer while relay handles remain");
        }

        pub async fn ready(&self) {
            tokio::time::timeout(Duration::from_secs(3), async {
                while !self.relay.connected.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("mock Bybit trade connection must authenticate");
        }
    }

    pub(crate) async fn mock_relay(reply: Reply) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v5/trade", listener.local_addr().unwrap());
        let orders = Arc::new(Mutex::new(Vec::new()));
        let captured = orders.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let pings = Arc::new(AtomicUsize::new(0));
        let heartbeat_count = pings.clone();
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::AcqRel);
                let mut socket = accept_async(stream).await.unwrap();
                let auth: Value =
                    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(auth["op"], "auth");
                assert_eq!(auth["args"][0], "test-key");
                let expires = auth["args"][1].as_u64().unwrap();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                assert!(expires > now && expires <= now + 30_000);
                let mut mac = Hmac::<Sha256>::new_from_slice(b"test-secret").unwrap();
                mac.update(format!("GET/realtime{expires}").as_bytes());
                assert_eq!(auth["args"][2], hex::encode(mac.finalize().into_bytes()));
                socket.send(Message::Text(json!({"op":"auth","retCode": if matches!(reply, Reply::AuthRejected) {10004} else {0}}).to_string().into())).await.unwrap();
                while let Some(Ok(message)) = socket.next().await {
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    if frame["op"] == "ping" {
                        heartbeat_count.fetch_add(1, Ordering::AcqRel);
                        if socket
                            .send(Message::Text(
                                json!({"op":"pong","retCode":0,"data":["1"]})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    assert_eq!(frame["op"], "order.create");
                    assert!(
                        !matches!(reply, Reply::AuthRejected),
                        "order before successful authentication"
                    );
                    captured.lock().unwrap().push(frame.clone());
                    if matches!(reply, Reply::Disconnect) {
                        let _ = socket.close(None).await;
                        break;
                    }
                    if matches!(reply, Reply::Silence) {
                        continue;
                    }
                    if matches!(reply, Reply::Malformed) {
                        let _ = socket.send(Message::Text("not-json".into())).await;
                        continue;
                    }
                    let response = json!({
                        "op": if matches!(reply, Reply::WrongOp) {"order.cancel"} else {"order.create"},
                        "reqId": if matches!(reply, Reply::ForeignRequest) { json!("foreign-request") } else { frame["reqId"].clone() },
                        "retCode": if matches!(reply, Reply::Reject) {10001} else {0},
                        "retMsg":"OK",
                        "data":{"orderId":"ws-order-91", "orderLinkId":if matches!(reply, Reply::ForeignClient) {json!("g_8_S_foreign")} else {frame["args"][0]["orderLinkId"].clone()}}
                    });
                    if socket
                        .send(Message::Text(response.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });
        let lifetime = Arc::new(());
        let relay = spawn_at(
            url,
            Zeroizing::new("test-key".into()),
            Zeroizing::new("test-secret".into()),
            Arc::downgrade(&lifetime),
            Duration::from_millis(150),
        )
        .unwrap();
        Fixture {
            relay,
            lifetime,
            orders,
            connections,
            pings,
            server,
        }
    }

    #[tokio::test]
    async fn full_writer_queue_and_closed_writer_are_provably_not_sent() {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let relay = BybitOrderRelay {
            connected: Arc::new(AtomicBool::new(true)),
            sender,
            timeout: Duration::from_millis(150),
        };
        for _ in 0..QUEUE_CAPACITY {
            let (reply, _) = oneshot::channel();
            relay
                .sender
                .try_send(Command {
                    payload: json!({}),
                    recv_window_ms: 5000,
                    deadline: Instant::now() + Duration::from_secs(1),
                    reply,
                })
                .unwrap();
        }
        assert!(matches!(
            relay.post(json!({}), 5000).await,
            Err(BybitOrderError::NotSent)
        ));
        assert_eq!(receiver.len(), QUEUE_CAPACITY);
        drop(receiver);
        assert!(matches!(
            relay.post(json!({}), 5000).await,
            Err(BybitOrderError::NotSent)
        ));
    }
}
