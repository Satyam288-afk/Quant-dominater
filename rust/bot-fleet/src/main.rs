use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

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
}

#[derive(Clone, Debug)]
struct BotConfig {
    bot_index: usize,
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
    let args = Args::parse();
    if args.orders_per_sec == 0 {
        anyhow::bail!("--orders-per-sec must be greater than zero");
    }

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (output_tx, output_rx) = mpsc::unbounded_channel();

    let event_writer = tokio::spawn(jsonl_writer(args.events_out.clone(), event_rx));
    let output_writer = tokio::spawn(jsonl_writer(args.outputs_out.clone(), output_rx));

    let mut handles = Vec::with_capacity(args.bots);
    for bot_index in 0..args.bots {
        let config = BotConfig {
            bot_index,
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
        )));
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

async fn run_bot(
    config: BotConfig,
    event_tx: mpsc::UnboundedSender<Value>,
    output_tx: mpsc::UnboundedSender<Value>,
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
                        )?;
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
                        )?;
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

fn handle_engine_message(
    run_id: &str,
    bot_id: &str,
    text: &str,
    pending: &mut HashMap<String, Instant>,
    stats: &mut BotStats,
    output_tx: &mpsc::UnboundedSender<Value>,
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
        }
        "fill" => {
            stats.fills_received += 1;
            let _ = output_tx.send(json!({
                "event_type": "fill_received",
                "run_id": run_id,
                "bot_id": bot_id,
                "engine_seq": message.get("engine_seq").and_then(Value::as_u64),
                "recv_ts_ns": recv_ts_ns,
                "message": message,
            }));
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

    NewOrder {
        message_type: "new_order",
        run_id: config.run_id.clone(),
        client_order_id: format!("bot_{}_{seq_no:06}", config.bot_index + 1),
        symbol: format!("SYM_{}", config.bot_index + 1),
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
