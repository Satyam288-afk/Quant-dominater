use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message, MaybeTlsStream};

/// Reconnect backoff: first retry after this long, doubling up to the cap.
const RECONNECT_BASE_MS: u64 = 10;
const RECONNECT_CAP_MS: u64 = 2_000;
/// A socket that lives less than this before dying is treated as a *flap*, not
/// a recovery: a hostile engine that accepts the WS upgrade and then instantly
/// closes returns `Ok` from connect, so without this guard the supervisor would
/// re-establish with zero delay, spin a tight reconnect storm (~thousands/sec,
/// burning a core), and inflate the judge-facing `reconnects` counter. A flap
/// is backed off exponentially and is NOT counted as a recovery.
const RECONNECT_FLAP_MS: u64 = 500;

/// Wire-adjacent send stamps, striped across `SHARDS` independent mutexes so the
/// insert-per-send / remove-per-ack traffic does not convoy on one global lock.
///
/// Previously a single `Mutex<HashMap<String, Instant>>` was locked twice per
/// round-trip (insert at send, remove at ack) plus a periodic full-map
/// `retain()` sweep, which formed a lock convoy that capped fleet throughput
/// (measured ~107 mutex waits / 6s at 99k TPS). Striping by a stable hash of
/// the track id spreads that traffic across many locks; a send-stamp and its
/// matching ack-stamp always hash to the same shard, so semantics are
/// preserved. The bot keeps a local pending fallback (main.rs) regardless.
pub struct WireStampMap {
    shards: Vec<Mutex<HashMap<String, Instant>>>,
}

/// Number of independent stamp-map shards. Power of two; comfortably exceeds the
/// pool connection count at the scales we run so contention is rare.
const STAMP_SHARDS: usize = 64;

impl WireStampMap {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(STAMP_SHARDS);
        for _ in 0..STAMP_SHARDS {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self { shards }
    }

    #[inline]
    fn shard_for(&self, key: &str) -> &Mutex<HashMap<String, Instant>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) & (STAMP_SHARDS - 1);
        &self.shards[idx]
    }

    /// Record the wire send instant for a track id.
    pub fn insert(&self, key: String, sent: Instant) {
        self.shard_for(&key).lock().unwrap().insert(key, sent);
    }

    /// Take (and remove) the wire send instant for a track id, if present.
    pub fn remove(&self, key: &str) -> Option<Instant> {
        self.shard_for(key).lock().unwrap().remove(key)
    }

    /// Drop every stamp older than `ttl`, across all shards.
    fn sweep(&self, ttl: Duration) {
        for shard in &self.shards {
            shard.lock().unwrap().retain(|_, sent| sent.elapsed() < ttl);
        }
    }
}

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
    wire_sent: Arc<WireStampMap>,
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
    /// `stamp_ttl` bounds the wire-stamp map: entries older than this are
    /// swept (an order whose ack never came — timeout, lost counterparty pod —
    /// would otherwise leak its stamp forever; measured ~205 B/entry and a
    /// 180ms+ rehash stall inside the shared mutex at multi-million-entry
    /// growth on long saturation runs). Callers pass ack_timeout + grace, so
    /// no stamp that can still be consumed is ever dropped.
    pub async fn connect(
        target: &str,
        n_conns: usize,
        n_bots: usize,
        pod_offset: usize,
        stamp_ttl: Duration,
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
        let wire_sent: Arc<WireStampMap> = Arc::new(WireStampMap::new());

        // Stamp-map sweeper: drops entries older than stamp_ttl. Exits once
        // every other holder of the map (pool, supervisors, bots) is gone.
        // The sweep walks shards one at a time, so it never holds more than a
        // single shard lock and never blocks the whole map.
        {
            let map = Arc::clone(&wire_sent);
            let period = stamp_ttl.max(Duration::from_millis(250));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if Arc::strong_count(&map) <= 1 {
                        return;
                    }
                    map.sweep(stamp_ttl);
                }
            });
        }

        let mut senders = Vec::with_capacity(n_conns);
        for _ in 0..n_conns {
            // Initial connect retries briefly to absorb kube-proxy iptables lag
            // (the engine pod may be Ready before its ClusterIP is routable on
            // every worker node).  After the first successful handshake the
            // supervisor handles all further reconnects.
            let ws_stream = {
                const MAX_TRIES: u32 = 8;
                const RETRY_MS: u64 = 500;
                let mut last_err = anyhow::anyhow!("no attempts");
                let mut ws = None;
                for attempt in 0..MAX_TRIES {
                    match connect_async_with_config(target, Some(ws_config()), false).await {
                        Ok((s, _)) => { ws = Some(s); break; }
                        Err(e) => {
                            last_err = anyhow::anyhow!("{}", e);
                            if attempt + 1 < MAX_TRIES {
                                sleep(Duration::from_millis(RETRY_MS)).await;
                            }
                        }
                    }
                }
                ws.ok_or_else(|| last_err)
                    .with_context(|| format!("pool connect {}", target))?
            };
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
    pub fn wire_sent_handle(&self) -> Arc<WireStampMap> {
        Arc::clone(&self.wire_sent)
    }
}

/// Bound inbound WebSocket frames. Orders/acks/fills are a few hundred bytes;
/// tungstenite's defaults otherwise let a peer buffer a 64 MiB message
/// (16 MiB/frame) before we ever parse it, so a hostile engine could pin
/// gigabytes across many connections. A 256 KiB ceiling is wildly generous for
/// the contract yet caps what a misbehaving engine can force per connection.
pub fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    let mut cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    cfg.max_message_size = Some(256 * 1024);
    cfg.max_frame_size = Some(64 * 1024);
    cfg
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
    wire_sent: Arc<WireStampMap>,
) {
    let mut current = Some(initial);
    let mut backoff = Duration::from_millis(RECONNECT_BASE_MS);

    loop {
        // Acquire a live stream: reuse the initial socket, else reconnect.
        // `is_reconnect` distinguishes a freshly dialled socket (whose recovery
        // we may count, once it proves useful) from the initial one.
        let (stream, is_reconnect) = match current.take() {
            Some(s) => (s, false),
            None => match connect_async_with_config(target.as_str(), Some(ws_config()), false).await
            {
                Ok((s, _)) => {
                    set_nodelay(&s);
                    // Don't reset backoff or count a recovery yet: connect
                    // returning Ok only means the upgrade was accepted, which a
                    // flapping engine does on every attempt. Both happen below,
                    // and only once the socket has proven useful.
                    (s, true)
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

        let live_since = Instant::now();
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
                        wire_sent.insert(track_id, Instant::now());
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

        // A socket that carried traffic for a meaningful interval was a real
        // connection; one that died almost immediately was a flap (e.g. an
        // accept-then-close engine).
        let useful = live_since.elapsed() >= Duration::from_millis(RECONNECT_FLAP_MS);
        if useful {
            // Count the recovery exactly once, when a *reconnected* socket has
            // proven useful — including the graceful run-end below, so a genuine
            // outage+recovery still reports reconnects=1.
            if is_reconnect {
                let n = reconnects.fetch_add(1, Ordering::Relaxed) + 1;
                eprintln!("[pool] recovered connection to {target} (recoveries={n})");
            }
            // Reset the backoff so the next genuine outage gets a fast retry.
            backoff = Duration::from_millis(RECONNECT_BASE_MS);
        }

        if graceful {
            return;
        }

        if !useful {
            // Flapping socket: back off before retrying so a hostile or broken
            // engine can't drive a zero-delay reconnect storm. Not counted as a
            // recovery (the count above is gated on `useful`).
            sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(RECONNECT_CAP_MS));
        }
        eprintln!("[pool] connection to {target} lost; reconnecting with backoff");
        // `current` is None -> the loop top reconnects.
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
    fn wire_stamp_map_insert_remove_roundtrip() {
        // #3: a send-stamp and its matching ack-stamp share a key, so they hash
        // to the same shard — insert then remove must return the stamp, and a
        // second remove must be empty (consumed exactly once).
        let map = WireStampMap::new();
        let now = Instant::now();
        map.insert("bot_5_000001".to_string(), now);
        map.insert("bot_6_000002".to_string(), now);
        assert_eq!(map.remove("bot_5_000001"), Some(now));
        assert_eq!(map.remove("bot_5_000001"), None);
        assert_eq!(map.remove("bot_6_000002"), Some(now));
        // An id never inserted finds no stamp.
        assert_eq!(map.remove("bot_99_000009"), None);
    }

    #[test]
    fn wire_stamp_map_sweep_drops_stale_only() {
        // The sweeper drops entries older than the ttl across all shards while
        // leaving fresh ones intact.
        let map = WireStampMap::new();
        let stamp = Instant::now();
        for i in 0..1000u64 {
            map.insert(format!("bot_{i}_000001"), stamp);
        }
        // A generous ttl keeps everything: a representative key is still present.
        map.sweep(Duration::from_secs(3600));
        assert_eq!(map.remove("bot_500_000001"), Some(stamp));
        // A 0ns ttl evicts every remaining entry across all shards.
        map.sweep(Duration::from_nanos(0));
        assert_eq!(map.remove("bot_0_000001"), None);
        assert_eq!(map.remove("bot_999_000001"), None);
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
