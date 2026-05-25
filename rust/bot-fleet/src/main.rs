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
use tokio_tungstenite::{connect_async, tungstenite::Message};

mod pool;

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
    #[arg(long, env = "KAFKA_TELEMETRY_TOPIC", default_value = "telemetry.events.v1")]
    kafka_topic: String,

    /// WebSocket connection pool size. 0 = one connection per bot (legacy,
    /// safe but unscalable past ~1k bots due to socket exhaustion). Any
    /// positive value multiplexes the requested number of virtual bots
    /// across N shared connections.
    #[arg(long, default_value_t = 0)]
    ws_connections: usize,
}

#[derive(Clone, Debug)]
struct BotConfig {
    bot_index: usize,
    num_symbols: usize,
    target: String,
    run_id: String,
    orders_per_sec: u64,
    duration: Duration,
    seed: u64,
    ack_timeout: Duration,
}

#[derive(Debug, Default)]
struct BotStats {
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    connect_errors: u64,
    latencies_ns: Vec<u64>,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    if args.orders_per_sec == 0 {
        anyhow::bail!("--orders-per-sec must be greater than zero");
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
    let pool_size = args.ws_connections;
    if pool_size > 0 {
        let n_conns = pool_size.min(args.bots);
        eprintln!("pooling {} bots over {} ws connections", args.bots, n_conns);
        let mut pool = pool::ConnectionPool::connect(&args.target, n_conns, args.bots).await?;
        for bot_index in 0..args.bots {
            let config = BotConfig {
                bot_index,
                num_symbols: args.symbols,
                target: args.target.clone(),
                run_id: args.run_id.clone(),
                orders_per_sec: args.orders_per_sec,
                duration: Duration::from_secs(args.duration_sec),
                seed: args.seed,
                ack_timeout: Duration::from_millis(args.ack_timeout_ms),
            };
            let sender = pool.sender_for(bot_index);
            let inbox = pool.take_inbox(bot_index);
            handles.push(tokio::spawn(run_bot_pooled(
                config,
                sender,
                inbox,
                event_tx.clone(),
                output_tx.clone(),
                Arc::clone(&sink),
            )));
        }
    } else {
        for bot_index in 0..args.bots {
            let config = BotConfig {
                bot_index,
                num_symbols: args.symbols,
                target: args.target.clone(),
                run_id: args.run_id.clone(),
                orders_per_sec: args.orders_per_sec,
                duration: Duration::from_secs(args.duration_sec),
                seed: args.seed,
                ack_timeout: Duration::from_millis(args.ack_timeout_ms),
            };
            handles.push(tokio::spawn(run_bot(
                config,
                event_tx.clone(),
                output_tx.clone(),
                Arc::clone(&sink),
            )));
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

    println!("run_id: {}", args.run_id);
    println!("bots: {}", args.bots);
    println!("orders_sent: {}", totals.orders_sent);
    println!("acks_received: {}", totals.acks_received);
    println!("fills_received: {}", totals.fills_received);
    println!("timeouts: {}", totals.timeouts);
    println!("connect_errors: {}", totals.connect_errors);
    println!("tps: {:.1}", tps);
    println!("p50: {}", fmt_ms(percentile(&totals.latencies_ns, 0.50)));
    println!("p90: {}", fmt_ms(percentile(&totals.latencies_ns, 0.90)));
    println!("p99: {}", fmt_ms(percentile(&totals.latencies_ns, 0.99)));
    println!("events_out: {}", args.events_out.display());
    println!("outputs_out: {}", args.outputs_out.display());

    Ok(())
}

async fn build_sink(args: &Args) -> Result<Arc<dyn TelemetrySink>> {
    match args.backend {
        Backend::File => Ok(Arc::new(FileSink::create(args.telemetry_out.clone()).await?)),
        Backend::None => Ok(Arc::new(NullSink::new())),
        Backend::Live => {
            #[cfg(feature = "kafka")]
            {
                let sink = bench_core::telemetry::KafkaSink::new(
                    &args.kafka_brokers,
                    &args.kafka_topic,
                )?;
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
                Ok(Arc::new(FileSink::create(args.telemetry_out.clone()).await?))
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
    let bot_id = format!("bot_{}", config.bot_index + 1);
    let (ws_stream, _) = connect_async(config.target.as_str())
        .await
        .with_context(|| format!("{bot_id} connect {}", config.target))?;
    let (mut write, mut read) = ws_stream.split();

    let mut stats = BotStats::default();
    let mut pending: HashMap<String, Instant> = HashMap::new();
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
                let order = make_order(&config, seq_no);
                let client_order_id = order.client_order_id.clone();
                let text = serde_json::to_string(&order)?;
                write.send(Message::Text(text)).await?;
                pending.insert(client_order_id.clone(), Instant::now());
                stats.orders_sent += 1;

                let _ = event_tx.send(json!({
                    "event_type": "order_sent",
                    "run_id": config.run_id,
                    "bot_id": bot_id,
                    "seq_no": seq_no,
                    "send_ts_ns": order.ts_ns,
                    "order": order,
                }));
                let _ = sink.emit(TelemetryEvent {
                    run_id: config.run_id.clone(),
                    bot_id: bot_id.clone(),
                    seq_no,
                    client_order_id,
                    event_type: EventKind::OrderSent,
                    send_ts_ns: order.ts_ns,
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
                    Some(Err(err)) => return Err(err.into()),
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
async fn run_bot_pooled(
    config: BotConfig,
    sender: mpsc::Sender<String>,
    mut inbox: mpsc::UnboundedReceiver<Value>,
    event_tx: mpsc::UnboundedSender<Value>,
    output_tx: mpsc::UnboundedSender<Value>,
    sink: Arc<dyn TelemetrySink>,
) -> Result<BotStats> {
    let bot_id = format!("bot_{}", config.bot_index + 1);
    let mut stats = BotStats::default();
    let mut pending: HashMap<String, Instant> = HashMap::new();
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
                let order = make_order(&config, seq_no);
                let client_order_id = order.client_order_id.clone();
                let text = serde_json::to_string(&order)?;
                if sender.send(text).await.is_err() {
                    // Pool writer closed — bail.
                    break;
                }
                pending.insert(client_order_id.clone(), Instant::now());
                stats.orders_sent += 1;

                let _ = event_tx.send(json!({
                    "event_type": "order_sent",
                    "run_id": config.run_id,
                    "bot_id": bot_id,
                    "seq_no": seq_no,
                    "send_ts_ns": order.ts_ns,
                    "order": order,
                }));
                let _ = sink.emit(TelemetryEvent {
                    run_id: config.run_id.clone(),
                    bot_id: bot_id.clone(),
                    seq_no,
                    client_order_id,
                    event_type: EventKind::OrderSent,
                    send_ts_ns: order.ts_ns,
                    recv_ts_ns: 0,
                    latency_ns: 0,
                }).await;
            }
            maybe_msg = inbox.recv(), if Instant::now() < drain_deadline => {
                match maybe_msg {
                    Some(message) => {
                        handle_engine_value(
                            &config.run_id,
                            &bot_id,
                            message,
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
    message: Value,
    pending: &mut HashMap<String, Instant>,
    stats: &mut BotStats,
    output_tx: &mpsc::UnboundedSender<Value>,
    sink: &Arc<dyn TelemetrySink>,
) -> Result<()> {
    let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
    let recv_ts_ns = now_ns();

    match message_type {
        "ack" => {
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let latency_ns = pending
                .remove(&client_order_id)
                .map(|sent| sent.elapsed().as_nanos() as u64)
                .unwrap_or_default();
            stats.acks_received += 1;
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
            let _ = sink.emit(TelemetryEvent {
                run_id: run_id.to_string(),
                bot_id: bot_id.to_string(),
                seq_no: 0,
                client_order_id,
                event_type: EventKind::AckReceived,
                send_ts_ns: 0,
                recv_ts_ns,
                latency_ns,
            }).await;
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
            let _ = sink.emit(TelemetryEvent {
                run_id: run_id.to_string(),
                bot_id: bot_id.to_string(),
                seq_no: 0,
                client_order_id,
                event_type: EventKind::FillReceived,
                send_ts_ns: 0,
                recv_ts_ns,
                latency_ns: 0,
            }).await;
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
    let message: Value =
        serde_json::from_str(text).with_context(|| format!("decode engine message: {text}"))?;
    let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
    let recv_ts_ns = now_ns();

    match message_type {
        "ack" => {
            let client_order_id = message
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let latency_ns = pending
                .remove(&client_order_id)
                .map(|sent| sent.elapsed().as_nanos() as u64)
                .unwrap_or_default();
            stats.acks_received += 1;
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
            let _ = sink.emit(TelemetryEvent {
                run_id: run_id.to_string(),
                bot_id: bot_id.to_string(),
                seq_no: 0,
                client_order_id,
                event_type: EventKind::AckReceived,
                send_ts_ns: 0,
                recv_ts_ns,
                latency_ns,
            }).await;
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
            let _ = sink.emit(TelemetryEvent {
                run_id: run_id.to_string(),
                bot_id: bot_id.to_string(),
                seq_no: 0,
                client_order_id,
                event_type: EventKind::FillReceived,
                send_ts_ns: 0,
                recv_ts_ns,
                latency_ns: 0,
            }).await;
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

fn make_order(config: &BotConfig, seq_no: u64) -> NewOrder {
    let side = if (config.bot_index as u64 + seq_no) % 2 == 0 {
        "BUY"
    } else {
        "SELL"
    };
    let base_ts = 1_770_000_000_000_000_000_u64 + config.seed.saturating_mul(1_000_000);
    let ts_ns = base_ts + seq_no.saturating_mul(1_000_000) + config.bot_index as u64;
    let sym_idx = bench_core::shard::bot_to_symbol(config.bot_index, config.num_symbols);

    NewOrder {
        message_type: "new_order",
        run_id: config.run_id.clone(),
        client_order_id: format!("bot_{}_{seq_no:06}", config.bot_index + 1),
        symbol: format!("SYM_{}", sym_idx + 1),
        side,
        price: 10_025,
        qty: 1 + ((config.bot_index as i64 + seq_no as i64) % 5),
        ts_ns,
    }
}

fn merge_stats(total: &mut BotStats, next: BotStats) {
    total.orders_sent += next.orders_sent;
    total.acks_received += next.acks_received;
    total.fills_received += next.fills_received;
    total.timeouts += next.timeouts;
    total.connect_errors += next.connect_errors;
    total.latencies_ns.extend(next.latencies_ns);
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
