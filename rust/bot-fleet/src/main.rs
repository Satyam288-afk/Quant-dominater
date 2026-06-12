use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bench_core::telemetry::{EventKind, FileSink, NullSink, TelemetryEvent, TelemetrySink};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message, MaybeTlsStream};

mod pool;

/// `zone!("name")` opens a Tracy profiling zone for the rest of the scope when
/// built with `--features profiling`, and compiles to nothing otherwise — so
/// the instrumentation has zero cost (and zero deps) in the shipping build.
#[cfg(feature = "profiling")]
macro_rules! zone {
    ($name:expr) => {
        let _zone = tracing::info_span!($name).entered();
    };
}
#[cfg(not(feature = "profiling"))]
macro_rules! zone {
    ($name:expr) => {};
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Backend {
    File,
    Live,
    None,
}

#[derive(Debug, Parser, Clone)]
#[command(about = "Deterministic async bot fleet for IICPC benchmark engines")]
struct Args {
    #[arg(long)]
    target: String,

    #[arg(long, default_value_t = 100)]
    bots: usize,

    #[arg(long, default_value_t = 5)]
    orders_per_sec: u64,

    #[arg(long, default_value_t = 60)]
    duration_sec: u64,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long, default_value = "run_local_001")]
    run_id: String,

    #[arg(long, default_value = "events.jsonl")]
    events_out: PathBuf,

    #[arg(long, default_value = "contestant_outputs.jsonl")]
    outputs_out: PathBuf,

    #[arg(long, default_value_t = 2_000)]
    ack_timeout_ms: u64,

    /// Number of distinct symbols to spread bots across. Default = bots
    /// (preserves legacy "one symbol per bot" behaviour). Lowering this
    /// causes multiple bots to share a symbol, enabling real fills at
    /// scale.
    #[arg(long, default_value_t = 0)]
    symbols: usize,

    /// Telemetry sink mode. `file` writes a JSONL stream of TelemetryEvent
    /// alongside events.jsonl. `live` publishes to Redpanda (requires the
    /// `kafka` cargo feature). `none` disables telemetry entirely.
    #[arg(long, value_enum, default_value_t = Backend::File)]
    backend: Backend,

    /// Destination for the telemetry JSONL stream when --backend=file.
    #[arg(long, default_value = "telemetry.jsonl")]
    telemetry_out: PathBuf,

    /// Redpanda/Kafka brokers (used when --backend=live).
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:19092")]
    kafka_brokers: String,

    /// Kafka topic for telemetry events.
    #[arg(
        long,
        env = "KAFKA_TELEMETRY_TOPIC",
        default_value = "telemetry.events.v1"
    )]
    kafka_topic: String,

    /// WebSocket connection pool size. 0 = one connection per bot (legacy,
    /// safe but unscalable past ~1k bots due to socket exhaustion). Any
    /// positive value multiplexes the requested number of virtual bots
    /// across N shared connections.
    #[arg(long, default_value_t = 0)]
    ws_connections: usize,

    /// Mid price for the limit-order ladder (integer ticks).
    #[arg(long, default_value_t = 10_025)]
    price_base: i64,

    /// Number of distinct price levels spread symmetrically around `price_base`.
    /// 1 (default) reproduces the legacy single-price book; higher values give
    /// the book real depth and a spread while orders still cross and fill.
    #[arg(long, default_value_t = 1)]
    price_levels: u64,

    /// Maximum order size. Each order draws a deterministic size in 1..=qty_max.
    #[arg(long, default_value_t = 5)]
    qty_max: u64,

    /// Share of orders sent as MARKET orders, in per-mille (‰). 0 = all limit
    /// (legacy). 100 = 10% market orders. Market orders cross whatever rests and
    /// any unfilled remainder is discarded (never rests).
    #[arg(long, default_value_t = 0)]
    market_per_mille: u64,

    /// Share of actions sent as cancels of a prior order, in per-mille (‰).
    /// 0 = never cancel (legacy). Cancels reference a real earlier order id so
    /// they exercise the engine's cancel path and price-time book maintenance.
    #[arg(long, default_value_t = 0)]
    cancel_per_mille: u64,

    /// This pod's index in a multi-pod Indexed Job. Order/bot IDs are offset by
    /// `pod_index * bots` so they're globally unique across the whole fleet and
    /// fills route to the right pod. Defaults from the Kubernetes Job env var;
    /// 0 (single process) is byte-identical to the legacy behaviour.
    #[arg(long, env = "JOB_COMPLETION_INDEX", default_value_t = 0)]
    pod_index: usize,

    /// Latency floor probe: each bot sends its next order the moment the
    /// previous one is acked, instead of on the --orders-per-sec timer. With
    /// the timer, a bot's task parks between sends and every round trip pays
    /// the scheduler wake-up (~100-250µs on this host) on top of the ~20µs
    /// WS+JSON transport floor — so open-loop percentiles measure the platform
    /// under realistic arrival processes, while closed-loop measures the floor
    /// itself (the wrk2-style open- vs closed-loop distinction). The wire
    /// protocol, order mix, measurement stamps and validation pipeline are
    /// identical; --orders-per-sec is ignored. Direct connections only
    /// (incompatible with --ws-connections: multiplexing would serialize the
    /// probes onto shared sockets and measure queueing, not the floor).
    #[arg(long, default_value_t = false)]
    closed_loop: bool,
}

#[derive(Clone, Debug)]
struct BotConfig {
    /// Globally-unique bot index across all pods (`pod_index * bots + local`).
    /// Drives every wire ID, timestamp, and symbol assignment so two pods never
    /// collide. The local loop index (used for pool inbox indexing) stays in
    /// `main` and is not threaded through here.
    global_bot_index: usize,
    num_symbols: usize,
    target: String,
    run_id: String,
    orders_per_sec: u64,
    duration: Duration,
    seed: u64,
    ack_timeout: Duration,
    price_base: i64,
    price_levels: u64,
    qty_max: u64,
    market_per_mille: u64,
    cancel_per_mille: u64,
}

#[derive(Debug, Default)]
struct BotStats {
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    connect_errors: u64,
    latencies_ns: Vec<u64>,
    /// Wall-clock receive time of every ack, used to compute peak (max) TPS in
    /// any 1-second window — the brief's "max TPS before failure" headline.
    ack_recv_ts_ns: Vec<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct NewOrder {
    #[serde(rename = "type")]
    message_type: &'static str,
    run_id: String,
    client_order_id: String,
    symbol: String,
    side: &'static str,
    price: i64,
    qty: i64,
    ts_ns: u64,
    order_type: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct CancelOrder {
    #[serde(rename = "type")]
    message_type: &'static str,
    run_id: String,
    client_order_id: String,
    orig_client_order_id: String,
    ts_ns: u64,
}

/// A prepared outbound action — either a new order or a cancel — ready to send
/// over either transport (direct WS or the pooled sender).
struct Prepared {
    /// JSON to put on the wire.
    text: String,
    /// Id to track for the ack (the request's own client_order_id).
    track_id: String,
    /// Event-log record for events.jsonl (the validator replays from this).
    event: Value,
    /// Send timestamp for telemetry.
    send_ts_ns: u64,
    /// True for a new order (it may rest and become cancellable).
    is_new: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "profiling")]
    {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default()),
        )
        .expect("set up tracy subscriber");
        eprintln!("tracy profiling enabled — launch the Tracy server to capture zones");
    }

    let mut args = Args::parse();
    if args.orders_per_sec == 0 {
        anyhow::bail!("--orders-per-sec must be greater than zero");
    }
    if args.closed_loop && args.ws_connections > 0 {
        anyhow::bail!(
            "--closed-loop is a per-connection latency probe and requires direct \
             connections (one per bot); drop --ws-connections"
        );
    }
    if args.symbols == 0 {
        // Default: one symbol per bot — byte-identical to legacy behaviour.
        args.symbols = args.bots.max(1);
    }

    let sink: Arc<dyn TelemetrySink> = build_sink(&args).await?;

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (output_tx, output_rx) = mpsc::unbounded_channel();

    let event_writer = tokio::spawn(jsonl_writer(args.events_out.clone(), event_rx));
    let output_writer = tokio::spawn(jsonl_writer(args.outputs_out.clone(), output_rx));

    let mut handles = Vec::with_capacity(args.bots);
    // Global ID base for this pod so order/bot IDs are unique fleet-wide.
    let pod_offset = args.pod_index * args.bots;
    if pod_offset > 0 {
        eprintln!(
            "pod {} owns global bots {}..{}",
            args.pod_index,
            pod_offset + 1,
            pod_offset + args.bots
        );
    }
    // Counts pooled-connection recoveries from mid-run engine outages. Read
    // after the run for the summary even though the pool itself is dropped.
    let mut reconnects_handle: Option<Arc<std::sync::atomic::AtomicU64>> = None;
    let pool_size = args.ws_connections;
    if pool_size > 0 {
        let n_conns = pool_size.min(args.bots);
        eprintln!("pooling {} bots over {} ws connections", args.bots, n_conns);
        // Stamp TTL = ack timeout + grace: after that an order is already a
        // timeout, so its wire stamp can never be consumed and gets swept.
        let stamp_ttl = Duration::from_millis(args.ack_timeout_ms) + Duration::from_secs(1);
        let mut pool =
            pool::ConnectionPool::connect(&args.target, n_conns, args.bots, pod_offset, stamp_ttl)
                .await?;
        reconnects_handle = Some(pool.reconnects_handle());
        let wire_sent = pool.wire_sent_handle();
        for bot_index in 0..args.bots {
            let config = BotConfig {
                global_bot_index: pod_offset + bot_index,
                num_symbols: args.symbols,
                target: args.target.clone(),
                run_id: args.run_id.clone(),
                orders_per_sec: args.orders_per_sec,
                duration: Duration::from_secs(args.duration_sec),
                seed: args.seed,
                ack_timeout: Duration::from_millis(args.ack_timeout_ms),
                price_base: args.price_base,
                price_levels: args.price_levels,
                qty_max: args.qty_max,
                market_per_mille: args.market_per_mille,
                cancel_per_mille: args.cancel_per_mille,
            };
            let sender = pool.sender_for(bot_index);
            let inbox = pool.take_inbox(bot_index);
            handles.push(tokio::spawn(run_bot_pooled(
                config,
                sender,
                inbox,
                Arc::clone(&wire_sent),
                event_tx.clone(),
                output_tx.clone(),
                Arc::clone(&sink),
            )));
        }
    } else {
        for bot_index in 0..args.bots {
            let config = BotConfig {
                global_bot_index: pod_offset + bot_index,
                num_symbols: args.symbols,
                target: args.target.clone(),
                run_id: args.run_id.clone(),
                orders_per_sec: args.orders_per_sec,
                duration: Duration::from_secs(args.duration_sec),
                seed: args.seed,
                ack_timeout: Duration::from_millis(args.ack_timeout_ms),
                price_base: args.price_base,
                price_levels: args.price_levels,
                qty_max: args.qty_max,
                market_per_mille: args.market_per_mille,
                cancel_per_mille: args.cancel_per_mille,
            };
            if args.closed_loop {
                handles.push(tokio::spawn(run_bot_closed_loop(
                    config,
                    event_tx.clone(),
                    output_tx.clone(),
                    Arc::clone(&sink),
                )));
            } else {
                handles.push(tokio::spawn(run_bot(
                    config,
                    event_tx.clone(),
                    output_tx.clone(),
                    Arc::clone(&sink),
                )));
            }
        }
    }

    let mut totals = BotStats::default();
    for handle in handles {
        match handle.await {
            Ok(Ok(stats)) => merge_stats(&mut totals, stats),
            Ok(Err(err)) => {
                eprintln!("bot failed: {err:#}");
                totals.connect_errors += 1;
            }
            Err(err) => {
                eprintln!("bot task join failed: {err:#}");
                totals.connect_errors += 1;
            }
        }
    }

    drop(event_tx);
    drop(output_tx);
    event_writer
        .await
        .context("event writer join failed")?
        .context("event writer failed")?;
    output_writer
        .await
        .context("output writer join failed")?
        .context("output writer failed")?;
    sink.flush().await.context("telemetry sink flush failed")?;

    totals.latencies_ns.sort_unstable();
    let tps = totals.acks_received as f64 / args.duration_sec.max(1) as f64;
    let peak = peak_tps(&mut totals.ack_recv_ts_ns, args.duration_sec);

    println!("run_id: {}", args.run_id);
    println!("bots: {}", args.bots);
    println!(
        "loop_mode: {}",
        if args.closed_loop {
            "closed (latency floor probe)"
        } else {
            "open"
        }
    );
    println!("orders_sent: {}", totals.orders_sent);
    println!("acks_received: {}", totals.acks_received);
    println!("fills_received: {}", totals.fills_received);
    println!("timeouts: {}", totals.timeouts);
    println!("connect_errors: {}", totals.connect_errors);
    let reconnects = reconnects_handle
        .map(|h| h.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0);
    println!("reconnects: {}", reconnects);
    println!("tps: {:.1}", tps);
    println!("peak_tps: {}", peak);
    println!("p50: {}", fmt_ms(percentile(&totals.latencies_ns, 0.50)));
    println!("p90: {}", fmt_ms(percentile(&totals.latencies_ns, 0.90)));
    println!("p99: {}", fmt_ms(percentile(&totals.latencies_ns, 0.99)));
    println!("events_out: {}", args.events_out.display());
    println!("outputs_out: {}", args.outputs_out.display());

    Ok(())
}

async fn build_sink(args: &Args) -> Result<Arc<dyn TelemetrySink>> {
    match args.backend {
        Backend::File => Ok(Arc::new(
            FileSink::create(args.telemetry_out.clone()).await?,
        )),
        Backend::None => Ok(Arc::new(NullSink::new())),
        Backend::Live => {
            #[cfg(feature = "kafka")]
            {
                let sink =
                    bench_core::telemetry::KafkaSink::new(&args.kafka_brokers, &args.kafka_topic)?;
                eprintln!(
                    "live telemetry → kafka brokers={} topic={}",
                    args.kafka_brokers, args.kafka_topic
                );
                return Ok(Arc::new(sink));
            }
            #[cfg(not(feature = "kafka"))]
            {
                eprintln!(
                    "binary built without `kafka` feature; degrading --backend live to file sink"
                );
                Ok(Arc::new(
                    FileSink::create(args.telemetry_out.clone()).await?,
                ))
            }
        }
    }
}

async fn run_bot(
    config: BotConfig,
    event_tx: mpsc::UnboundedSender<Value>,
    output_tx: mpsc::UnboundedSender<Value>,
    sink: Arc<dyn TelemetrySink>,
) -> Result<BotStats> {
    let bot_id = format!("bot_{}", config.global_bot_index + 1);
    let (ws_stream, _) =
        connect_async_with_config(config.target.as_str(), Some(pool::ws_config()), false)
            .await
            .with_context(|| format!("{bot_id} connect {}", config.target))?;
    // TCP_NODELAY: don't let Nagle buffer small order frames (see pool.rs).
    if let MaybeTlsStream::Plain(tcp) = ws_stream.get_ref() {
        let _ = tcp.set_nodelay(true);
    }
    let (mut write, mut read) = ws_stream.split();

    let mut stats = BotStats::default();
    let mut pending: HashMap<String, Instant> = HashMap::new();
    // Recent resting order ids this bot can cancel (bounded; only tracked when
    // cancels are enabled).
    let mut sent_orders: Vec<String> = Vec::new();
    let mut seq_no = 0_u64;
    let started = Instant::now();
    let deadline = started + config.duration;
    let drain_deadline = deadline + config.ack_timeout;
    let period = Duration::from_secs_f64(1.0 / config.orders_per_sec as f64);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick(), if Instant::now() < deadline => {
                seq_no += 1;
                let prepared = build_send(&config, &bot_id, seq_no, &sent_orders);
                write.send(Message::Text(prepared.text)).await?;
                pending.insert(prepared.track_id.clone(), Instant::now());
                stats.orders_sent += 1;
                if config.cancel_per_mille > 0 && prepared.is_new {
                    sent_orders.push(prepared.track_id.clone());
                    if sent_orders.len() > 512 {
                        sent_orders.remove(0);
                    }
                }

                let _ = event_tx.send(prepared.event);
                let _ = sink.emit(TelemetryEvent {
                    run_id: config.run_id.clone(),
                    bot_id: bot_id.clone(),
                    seq_no,
                    client_order_id: prepared.track_id,
                    event_type: EventKind::OrderSent,
                    send_ts_ns: prepared.send_ts_ns,
                    recv_ts_ns: 0,
                    latency_ns: 0,
                }).await;
            }
            maybe_msg = read.next(), if Instant::now() < drain_deadline => {
                match maybe_msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_engine_message(
                            &config.run_id,
                            &bot_id,
                            &text,
                            &mut pending,
                            &mut stats,
                            &output_tx,
                            &sink,
                        ).await?;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let text = String::from_utf8_lossy(&bytes);
                        handle_engine_message(
                            &config.run_id,
                            &bot_id,
                            &text,
                            &mut pending,
                            &mut stats,
                            &output_tx,
                            &sink,
                        ).await?;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        // Read error mid-run (engine crash / reset): stop reading
                        // and finish with the stats gathered so far instead of
                        // discarding this bot's whole contribution.
                        eprintln!("bot {bot_id} read error mid-run: {err}");
                        break;
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)), if Instant::now() >= deadline => {
                if pending.is_empty() || Instant::now() >= drain_deadline {
                    break;
                }
            }
        }
    }

    stats.timeouts = pending.len() as u64;
    Ok(stats)
}

/// Closed-loop sibling of [`run_bot`] (`--closed-loop`): the next order goes
/// out the moment the previous one is acked, no rate timer. The task only ever
/// parks on `read.next()` with its own ack already in flight (~tens of µs out),
/// so the measured round trip is the WS+JSON transport floor, not the cold
/// task wake-up an idle open-loop bot pays. Everything on the measurement path
/// is shared with the open loop — same [`build_send`], same wire-adjacent
/// stamp after `write.send`, same [`handle_engine_message`] — so the two modes
/// differ only in *when* a send is triggered.
///
/// One accounting divergence, conservative by construction: the ack timeout is
/// enforced *per order* (an ack arriving after it counts as a timeout and is
/// discarded), whereas the open loop only converts still-pending orders to
/// timeouts at run end. Under a degraded engine the probe therefore reports
/// more timeouts / fewer acks than the open loop would — it can never flatter
/// the floor numbers.
async fn run_bot_closed_loop(
    config: BotConfig,
    event_tx: mpsc::UnboundedSender<Value>,
    output_tx: mpsc::UnboundedSender<Value>,
    sink: Arc<dyn TelemetrySink>,
) -> Result<BotStats> {
    let bot_id = format!("bot_{}", config.global_bot_index + 1);
    let (ws_stream, _) =
        connect_async_with_config(config.target.as_str(), Some(pool::ws_config()), false)
            .await
            .with_context(|| format!("{bot_id} connect {}", config.target))?;
    // TCP_NODELAY: don't let Nagle buffer small order frames (see pool.rs).
    if let MaybeTlsStream::Plain(tcp) = ws_stream.get_ref() {
        let _ = tcp.set_nodelay(true);
    }
    let (mut write, mut read) = ws_stream.split();

    let mut stats = BotStats::default();
    let mut pending: HashMap<String, Instant> = HashMap::new();
    let mut sent_orders: Vec<String> = Vec::new();
    let mut seq_no = 0_u64;
    let started = Instant::now();
    let deadline = started + config.duration;

    while Instant::now() < deadline {
        seq_no += 1;
        let prepared = build_send(&config, &bot_id, seq_no, &sent_orders);
        let track_id = prepared.track_id.clone();
        write.send(Message::Text(prepared.text)).await?;
        pending.insert(track_id.clone(), Instant::now());
        stats.orders_sent += 1;
        if config.cancel_per_mille > 0 && prepared.is_new {
            sent_orders.push(track_id.clone());
            if sent_orders.len() > 512 {
                sent_orders.remove(0);
            }
        }

        let _ = event_tx.send(prepared.event);
        let _ = sink
            .emit(TelemetryEvent {
                run_id: config.run_id.clone(),
                bot_id: bot_id.clone(),
                seq_no,
                client_order_id: prepared.track_id,
                event_type: EventKind::OrderSent,
                send_ts_ns: prepared.send_ts_ns,
                recv_ts_ns: 0,
                latency_ns: 0,
            })
            .await;

        // Read until the order just sent is acked. Fills (for this or earlier
        // resting orders) arrive interleaved and are processed as normal.
        let wait_started = Instant::now();
        while pending.contains_key(&track_id) {
            let Some(remaining) = config.ack_timeout.checked_sub(wait_started.elapsed()) else {
                pending.remove(&track_id);
                stats.timeouts += 1;
                break;
            };
            match tokio::time::timeout(remaining, read.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    handle_engine_message(
                        &config.run_id,
                        &bot_id,
                        &text,
                        &mut pending,
                        &mut stats,
                        &output_tx,
                        &sink,
                    )
                    .await?;
                }
                Ok(Some(Ok(Message::Binary(bytes)))) => {
                    let text = String::from_utf8_lossy(&bytes);
                    handle_engine_message(
                        &config.run_id,
                        &bot_id,
                        &text,
                        &mut pending,
                        &mut stats,
                        &output_tx,
                        &sink,
                    )
                    .await?;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(err))) => {
                    // Read error mid-run (engine crash / reset): return the
                    // stats accumulated so far rather than discarding the whole
                    // bot's contribution. Still-pending orders are timeouts.
                    eprintln!("bot {bot_id} read error mid-run: {err}");
                    stats.timeouts += pending.len() as u64;
                    return Ok(stats);
                }
                Ok(None) => {
                    // Engine closed the connection: whatever is still pending
                    // can never be acked.
                    stats.timeouts += pending.len() as u64;
                    return Ok(stats);
                }
                Err(_elapsed) => {
                    pending.remove(&track_id);
                    stats.timeouts += 1;
                    break;
                }
            }
        }
    }

    // Mirror the open loop's ~10 ms post-deadline grace: fills the engine
    // emits after the final ack (e.g. a resting order crossed at run end)
    // would otherwise never be read into contestant_outputs.jsonl.
    let drain_until = Instant::now() + Duration::from_millis(10);
    loop {
        let now = Instant::now();
        if now >= drain_until {
            break;
        }
        match tokio::time::timeout(drain_until - now, read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                handle_engine_message(
                    &config.run_id,
                    &bot_id,
                    &text,
                    &mut pending,
                    &mut stats,
                    &output_tx,
                    &sink,
                )
                .await?;
            }
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                let text = String::from_utf8_lossy(&bytes);
                handle_engine_message(
                    &config.run_id,
                    &bot_id,
                    &text,
                    &mut pending,
                    &mut stats,
                    &output_tx,
                    &sink,
                )
                .await?;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }

    stats.timeouts += pending.len() as u64;
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
async fn run_bot_pooled(
    config: BotConfig,
    sender: mpsc::Sender<(String, String)>,
    mut inbox: mpsc::UnboundedReceiver<(Instant, Value)>,
    wire_sent: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
    event_tx: mpsc::UnboundedSender<Value>,
    output_tx: mpsc::UnboundedSender<Value>,
    sink: Arc<dyn TelemetrySink>,
) -> Result<BotStats> {
    let bot_id = format!("bot_{}", config.global_bot_index + 1);
    let mut stats = BotStats::default();
    let mut pending: HashMap<String, Instant> = HashMap::new();
    // Recent resting order ids this bot can cancel (bounded; only tracked when
    // cancels are enabled).
    let mut sent_orders: Vec<String> = Vec::new();
    let mut seq_no = 0_u64;
    let started = Instant::now();
    let deadline = started + config.duration;
    let drain_deadline = deadline + config.ack_timeout;
    let period = Duration::from_secs_f64(1.0 / config.orders_per_sec as f64);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick(), if Instant::now() < deadline => {
                seq_no += 1;
                let prepared = build_send(&config, &bot_id, seq_no, &sent_orders);
                if sender.send((prepared.track_id.clone(), prepared.text)).await.is_err() {
                    // Pool writer closed — bail.
                    break;
                }
                // Fallback stamp only: the authoritative send stamp is taken by
                // the pool supervisor at the wire (after write.send).
                pending.insert(prepared.track_id.clone(), Instant::now());
                stats.orders_sent += 1;
                if config.cancel_per_mille > 0 && prepared.is_new {
                    sent_orders.push(prepared.track_id.clone());
                    if sent_orders.len() > 512 {
                        sent_orders.remove(0);
                    }
                }

                let _ = event_tx.send(prepared.event);
                let _ = sink.emit(TelemetryEvent {
                    run_id: config.run_id.clone(),
                    bot_id: bot_id.clone(),
                    seq_no,
                    client_order_id: prepared.track_id,
                    event_type: EventKind::OrderSent,
                    send_ts_ns: prepared.send_ts_ns,
                    recv_ts_ns: 0,
                    latency_ns: 0,
                }).await;
            }
            maybe_msg = inbox.recv(), if Instant::now() < drain_deadline => {
                match maybe_msg {
                    Some((recv_instant, message)) => {
                        handle_engine_value(
                            &config.run_id,
                            &bot_id,
                            recv_instant,
                            message,
                            &wire_sent,
                            &mut pending,
                            &mut stats,
                            &output_tx,
                            &sink,
                        ).await?;
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)), if Instant::now() >= deadline => {
                if pending.is_empty() || Instant::now() >= drain_deadline {
                    break;
                }
            }
        }
    }

    stats.timeouts = pending.len() as u64;
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
async fn handle_engine_value(
    run_id: &str,
    bot_id: &str,
    recv_instant: Instant,
    message: Value,
    wire_sent: &Arc<std::sync::Mutex<HashMap<String, Instant>>>,
    pending: &mut HashMap<String, Instant>,
    stats: &mut BotStats,
    output_tx: &mpsc::UnboundedSender<Value>,
    sink: &Arc<dyn TelemetrySink>,
) -> Result<()> {
    // (async fn — instrument with #[tracing::instrument] rather than a scope
    // guard, which can't be held across .await in a Send future.)
    let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
    let recv_ts_ns = now_ns();

    match message_type {
        "ack" => {
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            // Wire-adjacent latency: the supervisor stamped send right after
            // write.send, the reader stamped recv right after read.next() —
            // fleet-internal channel hops are excluded from the measurement.
            // Fall back to the bot-side stamp only if the wire stamp is
            // missing (e.g. ack raced the supervisor's insert).
            let wire = wire_sent.lock().unwrap().remove(&client_order_id);
            let bot_side = pending.remove(&client_order_id);
            // Gate: only an ack that matches an order this bot actually had
            // outstanding counts. A duplicate re-delivery, or a hostile engine
            // flooding acks for ids it was never sent, finds neither stamp —
            // counting it would inflate throughput/stability and grow the
            // latency + output buffers without bound (a proven OOM lever). Drop.
            if wire.is_none() && bot_side.is_none() {
                return Ok(());
            }
            let latency_ns = wire
                .map(|sent| recv_instant.saturating_duration_since(sent).as_nanos() as u64)
                .or_else(|| bot_side.map(|sent| sent.elapsed().as_nanos() as u64))
                .unwrap_or_default();
            stats.acks_received += 1;
            stats.ack_recv_ts_ns.push(recv_ts_ns);
            if latency_ns > 0 {
                stats.latencies_ns.push(latency_ns);
            }
            let _ = output_tx.send(json!({
                "event_type": "ack_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "client_order_id": client_order_id,
                "recv_ts_ns": recv_ts_ns,
                "latency_ns": latency_ns,
                "message": message,
            }));
            let _ = sink
                .emit(TelemetryEvent {
                    run_id: run_id.to_string(),
                    bot_id: bot_id.to_string(),
                    seq_no: 0,
                    client_order_id,
                    event_type: EventKind::AckReceived,
                    send_ts_ns: 0,
                    recv_ts_ns,
                    latency_ns,
                })
                .await;
        }
        "fill" => {
            stats.fills_received += 1;
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let _ = output_tx.send(json!({
                "event_type": "fill_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "engine_seq": message.get("engine_seq").and_then(Value::as_u64),
                "recv_ts_ns": recv_ts_ns,
                "message": message,
            }));
            let _ = sink
                .emit(TelemetryEvent {
                    run_id: run_id.to_string(),
                    bot_id: bot_id.to_string(),
                    seq_no: 0,
                    client_order_id,
                    event_type: EventKind::FillReceived,
                    send_ts_ns: 0,
                    recv_ts_ns,
                    latency_ns: 0,
                })
                .await;
        }
        _ => {
            let _ = output_tx.send(json!({
                "event_type": "unknown_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "recv_ts_ns": recv_ts_ns,
                "message": message,
            }));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_engine_message(
    run_id: &str,
    bot_id: &str,
    text: &str,
    pending: &mut HashMap<String, Instant>,
    stats: &mut BotStats,
    output_tx: &mpsc::UnboundedSender<Value>,
    sink: &Arc<dyn TelemetrySink>,
) -> Result<()> {
    // Drop an unparseable frame rather than erroring out of the bot: a hostile
    // or buggy engine emitting one line of garbage must not tear down the whole
    // connection and discard the bot's accumulated, correctly-measured stats.
    // This is parity with the pooled reader (see pool.rs: `Err(_) => continue`).
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return Ok(());
    };
    let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
    let recv_ts_ns = now_ns();

    match message_type {
        "ack" => {
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            // Gate: drop acks for orders this bot never had outstanding
            // (duplicate re-delivery or a hostile engine fabricating acks) so
            // they can't inflate throughput/stability or grow buffers unbounded.
            let Some(sent) = pending.remove(&client_order_id) else {
                return Ok(());
            };
            let latency_ns = sent.elapsed().as_nanos() as u64;
            stats.acks_received += 1;
            stats.ack_recv_ts_ns.push(recv_ts_ns);
            if latency_ns > 0 {
                stats.latencies_ns.push(latency_ns);
            }
            let _ = output_tx.send(json!({
                "event_type": "ack_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "client_order_id": client_order_id,
                "recv_ts_ns": recv_ts_ns,
                "latency_ns": latency_ns,
                "message": message,
            }));
            let _ = sink
                .emit(TelemetryEvent {
                    run_id: run_id.to_string(),
                    bot_id: bot_id.to_string(),
                    seq_no: 0,
                    client_order_id,
                    event_type: EventKind::AckReceived,
                    send_ts_ns: 0,
                    recv_ts_ns,
                    latency_ns,
                })
                .await;
        }
        "fill" => {
            stats.fills_received += 1;
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let _ = output_tx.send(json!({
                "event_type": "fill_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "engine_seq": message.get("engine_seq").and_then(Value::as_u64),
                "recv_ts_ns": recv_ts_ns,
                "message": message,
            }));
            let _ = sink
                .emit(TelemetryEvent {
                    run_id: run_id.to_string(),
                    bot_id: bot_id.to_string(),
                    seq_no: 0,
                    client_order_id,
                    event_type: EventKind::FillReceived,
                    send_ts_ns: 0,
                    recv_ts_ns,
                    latency_ns: 0,
                })
                .await;
        }
        _ => {
            let _ = output_tx.send(json!({
                "event_type": "unknown_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "recv_ts_ns": recv_ts_ns,
                "message": message,
            }));
        }
    }

    Ok(())
}

async fn jsonl_writer(path: PathBuf, mut rx: mpsc::UnboundedReceiver<Value>) -> Result<()> {
    let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    while let Some(value) = rx.recv().await {
        serde_json::to_writer(&mut writer, &value)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

/// Deterministic 64-bit mixer (SplitMix64). Pure function of its input, so a
/// run with a fixed `--seed` is fully reproducible while every order still
/// varies in price, size, and type.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn make_order(config: &BotConfig, seq_no: u64) -> NewOrder {
    zone!("make_order");
    // Global index drives every externally-visible facet (ID, timestamp,
    // symbol) so two pods never collide; only pool inbox indexing uses local.
    let gbi = config.global_bot_index;
    let side = if (gbi as u64 + seq_no).is_multiple_of(2) {
        "BUY"
    } else {
        "SELL"
    };
    let base_ts = 1_770_000_000_000_000_000_u64 + config.seed.saturating_mul(1_000_000);
    let ts_ns = base_ts + seq_no.saturating_mul(1_000_000) + gbi as u64;
    let sym_idx = bench_core::shard::bot_to_symbol(gbi, config.num_symbols);

    // Per-order entropy keyed by (seed, bot, seq): reproducible yet varied.
    let h = splitmix64(
        config
            .seed
            .wrapping_add((gbi as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .wrapping_add(seq_no.wrapping_mul(0xD1B5_4A32_D192_ED03)),
    );

    // A fraction of orders are MARKET — they sweep whatever rests and discard
    // any unfilled remainder, simulating aggressive takers.
    let is_market = h % 1000 < config.market_per_mille;

    // Limit price ladder: spread price_levels ticks symmetrically around the
    // mid, so the book gains real depth and a spread while orders still cross.
    // price_levels == 1 (default) reproduces the legacy single-price book.
    let levels = config.price_levels.max(1) as i64;
    let tick = ((h >> 12) % levels as u64) as i64 - levels / 2;
    let price = if is_market {
        0
    } else {
        config.price_base + tick
    };

    // Order size in 1..=qty_max, deterministic.
    let qty = 1 + ((h >> 24) % config.qty_max.max(1)) as i64;

    NewOrder {
        message_type: "new_order",
        run_id: config.run_id.clone(),
        client_order_id: format!("bot_{}_{seq_no:06}", gbi + 1),
        symbol: format!("SYM_{}", sym_idx + 1),
        side,
        price,
        qty,
        ts_ns,
        order_type: if is_market { "MARKET" } else { "LIMIT" },
    }
}

/// Decide the next action for a bot and prepare it for sending. With
/// probability `cancel_per_mille` (and only once the bot has resting orders to
/// cancel) it emits a cancel of a previously-sent order; otherwise a new order.
fn build_send(config: &BotConfig, bot_id: &str, seq_no: u64, sent: &[String]) -> Prepared {
    zone!("build_send");
    let h = splitmix64(
        config
            .seed
            .wrapping_add(0xCA11_CA11_CA11_CA11)
            .wrapping_add((config.global_bot_index as u64).wrapping_mul(0x100_0000_01B3))
            .wrapping_add(seq_no.wrapping_mul(0x0100_0193)),
    );
    let do_cancel = !sent.is_empty() && h % 1000 < config.cancel_per_mille;

    if do_cancel {
        let orig = sent[(h >> 20) as usize % sent.len()].clone();
        let base_ts = 1_770_000_000_000_000_000_u64 + config.seed.saturating_mul(1_000_000);
        let ts_ns = base_ts + seq_no.saturating_mul(1_000_000) + config.global_bot_index as u64;
        let coid = format!("bot_{}_c{seq_no:06}", config.global_bot_index + 1);
        let cancel = CancelOrder {
            message_type: "cancel_order",
            run_id: config.run_id.clone(),
            client_order_id: coid.clone(),
            orig_client_order_id: orig,
            ts_ns,
        };
        let event = json!({
            "event_type": "cancel_sent",
            "run_id": config.run_id,
            "bot_id": bot_id,
            "seq_no": seq_no,
            "send_ts_ns": ts_ns,
            "order": cancel,
        });
        Prepared {
            text: serde_json::to_string(&cancel).unwrap_or_default(),
            track_id: coid,
            event,
            send_ts_ns: ts_ns,
            is_new: false,
        }
    } else {
        let order = make_order(config, seq_no);
        let coid = order.client_order_id.clone();
        let send_ts_ns = order.ts_ns;
        let event = json!({
            "event_type": "order_sent",
            "run_id": config.run_id,
            "bot_id": bot_id,
            "seq_no": seq_no,
            "send_ts_ns": send_ts_ns,
            "order": order,
        });
        Prepared {
            text: serde_json::to_string(&order).unwrap_or_default(),
            track_id: coid,
            event,
            send_ts_ns,
            is_new: true,
        }
    }
}

fn merge_stats(total: &mut BotStats, next: BotStats) {
    total.orders_sent += next.orders_sent;
    total.acks_received += next.acks_received;
    total.fills_received += next.fills_received;
    total.timeouts += next.timeouts;
    total.connect_errors += next.connect_errors;
    total.latencies_ns.extend(next.latencies_ns);
    total.ack_recv_ts_ns.extend(next.ack_recv_ts_ns);
}

/// Peak TPS = the largest number of acks landing in any aligned 1-second
/// window. This is "max sustained throughput", distinct from the average
/// (acks / duration), and is the headline the brief asks for.
fn peak_tps(ack_recv_ts_ns: &mut [u64], window_secs: u64) -> u64 {
    if ack_recv_ts_ns.is_empty() {
        return 0;
    }
    ack_recv_ts_ns.sort_unstable();
    let counter = bench_core::metrics::TpsCounter::new(window_secs as usize + 2);
    for ts in ack_recv_ts_ns.iter() {
        counter.record(*ts);
    }
    counter.peak_tps()
}

fn percentile(sorted: &[u64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((p * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    Some(sorted[idx] as f64 / 1_000_000.0)
}

fn fmt_ms(value: Option<f64>) -> String {
    match value {
        Some(ms) => format!("{ms:.2}ms"),
        None => "n/a".to_string(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
