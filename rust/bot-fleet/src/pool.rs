use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream};

/// Reconnect backoff: first retry after this long, doubling up to the cap.
const RECONNECT_BASE_MS: u64 = 10;
const RECONNECT_CAP_MS: u64 = 2_000;

/// One outgoing WebSocket per pool connection. `bots/N` virtual bots share
/// each socket. Routing inbound messages back to a specific bot is done by
/// parsing `client_order_id` (which encodes the bot index).
///
/// Each connection is owned by a **supervisor task** that survives transient
/// engine outages: if the socket drops mid-run it reconnects with capped
/// exponential backoff and resumes draining the SAME bot channels, so the bots
/// are oblivious to the blip (they just experience backpressure while the
/// socket is down and resume on reconnect). The number of recoveries is counted
/// in `reconnects` and surfaced in the fleet summary.
///
/// Latency stamps are taken AT THE WIRE, not in the bot task: the supervisor
/// records the send `Instant` into `wire_sent` immediately after `write.send`
/// completes, and the reader captures the recv `Instant` right after
/// `read.next()` returns, before parse/clone/inbox hops. Stamping in the bot
/// task (before the outbound channel hop, after the inbound one) padded the
/// reported p99 with fleet-internal queueing: measured -0.55ms p50 /
/// -0.65 to -1.17ms p99 at 200x20 just from moving the stamps, reaching
/// direct-socket parity. A harness must not bill its own plumbing to the
/// engine under test.
pub struct ConnectionPool {
    senders: Vec<mpsc::Sender<(String, String)>>,
    bot_inboxes: Vec<Option<mpsc::UnboundedReceiver<(Instant, Value)>>>,
    reconnects: Arc<AtomicU64>,
    wire_sent: Arc<Mutex<HashMap<String, Instant>>>,
}

impl ConnectionPool {
    /// Open `n_conns` WebSocket connections to `target`, spin up a supervisor
    /// task per connection, and pre-allocate one inbox per virtual bot.
    ///
    /// `pod_offset` is this pod's global bot-index base (`pod_index * n_bots`).
    /// Order IDs encode the GLOBAL bot index so they're unique across a
    /// multi-pod Indexed Job; inbound routing subtracts the offset to land on
    /// the right LOCAL inbox, and silently drops messages whose counterparty
    /// lives on another pod (that pod owns its own side of the trade).
    pub async fn connect(
        target: &str,
        n_conns: usize,
        n_bots: usize,
        pod_offset: usize,
    ) -> Result<Self> {
        assert!(
            n_conns >= 1 && n_bots >= 1,
            "pool needs at least 1 conn and 1 bot"
        );

        let mut inboxes_tx: Vec<mpsc::UnboundedSender<(Instant, Value)>> =
            Vec::with_capacity(n_bots);
        let mut inboxes_rx: Vec<Option<mpsc::UnboundedReceiver<(Instant, Value)>>> =
            Vec::with_capacity(n_bots);
        for _ in 0..n_bots {
            let (tx, rx) = mpsc::unbounded_channel::<(Instant, Value)>();
            inboxes_tx.push(tx);
            inboxes_rx.push(Some(rx));
        }
        let inboxes_tx = Arc::new(inboxes_tx);
        let reconnects = Arc::new(AtomicU64::new(0));
        let wire_sent: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));

        let mut senders = Vec::with_capacity(n_conns);
        for _ in 0..n_conns {
            // Initial connect is still strict: if the engine is unreachable at
            // startup we fail the run (preserves the original semantics). Only
            // DROPS after a successful start are healed by the supervisor.
            let (ws_stream, _) = connect_async(target)
                .await
                .with_context(|| format!("pool connect {}", target))?;
            set_nodelay(&ws_stream);

            // Bots push (track_id, order text) into this bounded channel; the
            // supervisor owns the receiver for the whole run, across reconnects.
            let (tx_out, rx_out) = mpsc::channel::<(String, String)>(4096);
            senders.push(tx_out);

            tokio::spawn(supervise_connection(
                ws_stream,
                rx_out,
                Arc::clone(&inboxes_tx),
                target.to_string(),
                pod_offset,
                Arc::clone(&reconnects),
                Arc::clone(&wire_sent),
            ));
        }

        Ok(Self {
            senders,
            bot_inboxes: inboxes_rx,
            reconnects,
            wire_sent,
        })
    }

    pub fn sender_for(&self, bot_index: usize) -> mpsc::Sender<(String, String)> {
        let n = self.senders.len();
        self.senders[bot_index % n].clone()
    }

    /// Hand the inbox for a given bot to its task. Each inbox is consumed
    /// exactly once.
    pub fn take_inbox(&mut self, bot_index: usize) -> mpsc::UnboundedReceiver<(Instant, Value)> {
        self.bot_inboxes[bot_index]
            .take()
            .expect("inbox taken twice for the same bot")
    }

    /// A shared counter of how many pooled connections recovered from a drop
    /// during the run. Cloneable so the caller can read it after the run even
    /// once the pool itself has been dropped.
    pub fn reconnects_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.reconnects)
    }

    /// Wire-adjacent send stamps, written by the supervisor immediately after
    /// `write.send` completes, consumed by the bot when its ack arrives.
    pub fn wire_sent_handle(&self) -> Arc<Mutex<HashMap<String, Instant>>> {
        Arc::clone(&self.wire_sent)
    }
}

/// tokio leaves Nagle ON by default; a load generator measuring engine latency
/// must send each order frame immediately rather than letting the kernel
/// coalesce small writes (the classic trading-path latency fix).
fn set_nodelay(ws: &tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) {
    if let MaybeTlsStream::Plain(tcp) = ws.get_ref() {
        let _ = tcp.set_nodelay(true);
    }
}

/// Owns one pooled connection for the life of the run. Writes bots' outbound
/// frames and demuxes inbound engine messages; on a socket failure it
/// reconnects with capped exponential backoff and resumes on the SAME bot
/// channels. Returns only when all bots have finished (their senders dropped),
/// at which point it closes the socket gracefully.
async fn supervise_connection(
    initial: tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    mut rx_out: mpsc::Receiver<(String, String)>,
    inboxes_tx: Arc<Vec<mpsc::UnboundedSender<(Instant, Value)>>>,
    target: String,
    pod_offset: usize,
    reconnects: Arc<AtomicU64>,
    wire_sent: Arc<Mutex<HashMap<String, Instant>>>,
) {
    let mut current = Some(initial);
    let mut backoff = Duration::from_millis(RECONNECT_BASE_MS);

    loop {
        // Acquire a live stream: reuse the initial socket, else reconnect.
        let stream = match current.take() {
            Some(s) => s,
            None => match connect_async(&target).await {
                Ok((s, _)) => {
                    set_nodelay(&s);
                    backoff = Duration::from_millis(RECONNECT_BASE_MS);
                    let n = reconnects.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!("[pool] reconnected to {target} (recoveries={n})");
                    s
                }
                Err(err) => {
                    eprintln!(
                        "[pool] reconnect to {target} failed: {err}; retrying in {}ms",
                        backoff.as_millis()
                    );
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(RECONNECT_CAP_MS));
                    continue;
                }
            },
        };

        let (mut write, mut read) = stream.split();

        // Reader subtask: owns the read half plus a death token. When the
        // socket dies (Err / None / Close) the task returns, dropping
        // `dead_tx`, which the supervisor observes as a closed `dead_rx`.
        let (dead_tx, mut dead_rx) = mpsc::channel::<()>(1);
        let reader_inboxes = Arc::clone(&inboxes_tx);
        let reader = tokio::spawn(async move {
            let _dead_tx = dead_tx; // dropped on return -> signals socket death
            while let Some(msg) = read.next().await {
                // Recv stamp at the wire, BEFORE parse/clone/inbox hops: those
                // are harness costs and must not count as engine latency.
                let recv_instant = Instant::now();
                let text = match msg {
                    Ok(Message::Text(t)) => t,
                    Ok(Message::Binary(b)) => String::from_utf8_lossy(&b).to_string(),
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue, // ping/pong/frame
                    Err(_) => break,
                };
                let value: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let recipients: Vec<usize> = recipient_bots(&value, pod_offset)
                    .into_iter()
                    .filter(|&bot| bot < reader_inboxes.len())
                    .collect();
                // Clone for all but the last recipient; MOVE the Value to the
                // last — cloning the final copy too was pure waste (~250-320ns
                // + an allocation per message, measured).
                if let Some((&last, rest)) = recipients.split_last() {
                    for &bot in rest {
                        let _ = reader_inboxes[bot].send((recv_instant, value.clone()));
                    }
                    let _ = reader_inboxes[last].send((recv_instant, value));
                }
            }
        });

        // Writer loop: drain bots' outbound channel onto the socket until
        // either the socket dies or every bot has finished.
        let graceful = loop {
            tokio::select! {
                maybe_text = rx_out.recv() => match maybe_text {
                    Some((track_id, text)) => {
                        if write.send(Message::Text(text)).await.is_err() {
                            break false; // socket write failed -> reconnect
                        }
                        // Send stamp at the wire, right after the frame left.
                        wire_sent.lock().unwrap().insert(track_id, Instant::now());
                    }
                    None => {
                        let _ = write.close().await;
                        break true; // all bots done -> clean exit
                    }
                },
                _ = dead_rx.recv() => break false, // reader saw the socket die
            }
        };

        reader.abort();
        if graceful {
            return;
        }
        eprintln!("[pool] connection to {target} lost; reconnecting with backoff");
        // `current` is None -> the loop top reconnects with backoff.
    }
}

/// Decide which LOCAL bot inboxes a single inbound engine message should go to.
/// `ack` is routed by `client_order_id`. `fill` lacks one, so we route by
/// both `buy_order_id` and `sell_order_id`. Order IDs carry the GLOBAL bot
/// index; `pod_offset` is subtracted to get this pod's local index, and any
/// counterparty that maps below 0 (a bot on another pod) is dropped — that
/// pod delivers its own side. Other message types are dropped.
fn recipient_bots(value: &Value, pod_offset: usize) -> Vec<usize> {
    let local = |id: &str| -> Option<usize> {
        parse_bot_index(id).and_then(|global| global.checked_sub(pod_offset))
    };
    let msg_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    match msg_type {
        "ack" => value
            .get("client_order_id")
            .and_then(Value::as_str)
            .and_then(local)
            .map(|i| vec![i])
            .unwrap_or_default(),
        "fill" => {
            let mut out = Vec::with_capacity(2);
            let buy = value
                .get("buy_order_id")
                .and_then(Value::as_str)
                .and_then(local);
            let sell = value
                .get("sell_order_id")
                .and_then(Value::as_str)
                .and_then(local);
            if let Some(b) = buy {
                out.push(b);
            }
            if let Some(s) = sell {
                if !out.contains(&s) {
                    out.push(s);
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Parse "bot_{N}_{seq}" -> N - 1, the GLOBAL 0-based bot index (IDs are
/// 1-indexed on the wire). Returns None for any other shape.
fn parse_bot_index(client_order_id: &str) -> Option<usize> {
    let rest = client_order_id.strip_prefix("bot_")?;
    let underscore = rest.find('_')?;
    let n: usize = rest[..underscore].parse().ok()?;
    if n == 0 {
        return None;
    }
    Some(n - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_bot_index() {
        assert_eq!(parse_bot_index("bot_1_000001"), Some(0));
        assert_eq!(parse_bot_index("bot_42_000999"), Some(41));
        assert_eq!(parse_bot_index("bot_0_000001"), None);
        assert_eq!(parse_bot_index("garbage"), None);
        assert_eq!(parse_bot_index("bot_x_000001"), None);
    }

    #[test]
    fn ack_routes_to_one_bot() {
        let v = json!({ "type": "ack", "client_order_id": "bot_5_000001" });
        assert_eq!(recipient_bots(&v, 0), vec![4]);
    }

    #[test]
    fn fill_routes_to_both_sides() {
        let v = json!({
            "type": "fill",
            "buy_order_id": "bot_3_000001",
            "sell_order_id": "bot_7_000002",
        });
        let r = recipient_bots(&v, 0);
        assert!(r.contains(&2));
        assert!(r.contains(&6));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn fill_dedupes_self_trade_recipients() {
        let v = json!({
            "type": "fill",
            "buy_order_id": "bot_3_000001",
            "sell_order_id": "bot_3_000002",
        });
        assert_eq!(recipient_bots(&v, 0), vec![2]);
    }

    #[test]
    fn unknown_message_dropped() {
        let v = json!({ "type": "weird" });
        assert!(recipient_bots(&v, 0).is_empty());
    }

    #[test]
    fn pod_offset_maps_global_to_local_and_drops_foreign() {
        // Pod 1 owns global bots 1250..2499 (offset 1250). A fill between a
        // local bot (global 1251 -> local 1) and a bot on another pod
        // (global 5 -> below offset) routes only to the local side.
        let v = json!({
            "type": "fill",
            "buy_order_id": "bot_1252_000001", // global 1251 -> local 1
            "sell_order_id": "bot_6_000002",   // global 5  -> other pod
        });
        assert_eq!(recipient_bots(&v, 1250), vec![1]);

        // An ack for a global id below this pod's window is dropped.
        let ack = json!({ "type": "ack", "client_order_id": "bot_3_000001" });
        assert!(recipient_bots(&ack, 1250).is_empty());
    }
}
