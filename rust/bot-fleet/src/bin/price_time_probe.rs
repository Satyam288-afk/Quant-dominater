use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Parser)]
#[command(about = "Send a deterministic price-time-priority probe to a benchmark engine")]
struct Args {
    #[arg(long)]
    target: String,

    #[arg(long, default_value = "run_price_time_probe")]
    run_id: String,

    #[arg(long, default_value = "events.jsonl")]
    events_out: PathBuf,

    #[arg(long, default_value = "contestant_outputs.jsonl")]
    outputs_out: PathBuf,

    #[arg(long, default_value_t = 2_000)]
    drain_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
struct NewOrder {
    #[serde(rename = "type")]
    message_type: &'static str,
    run_id: String,
    client_order_id: &'static str,
    symbol: &'static str,
    side: &'static str,
    price: i64,
    qty: i64,
    ts_ns: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut events = JsonlWriter::create(&args.events_out)?;
    let mut outputs = JsonlWriter::create(&args.outputs_out)?;
    let (mut ws, _) = connect_async(args.target.as_str())
        .await
        .with_context(|| format!("connect {}", args.target))?;

    let orders = vec![
        order(&args.run_id, "buy_late", "BUY", 1_770_000_000_000_000_002),
        order(&args.run_id, "buy_early", "BUY", 1_770_000_000_000_000_001),
        order(&args.run_id, "sell_1", "SELL", 1_770_000_000_000_000_003),
    ];

    let mut pending: HashMap<&str, u64> = HashMap::new();
    let mut ack_count = 0_u64;
    let mut fill_count = 0_u64;

    for (idx, order) in orders.iter().enumerate() {
        let seq_no = idx + 1;
        let text = serde_json::to_string(order)?;
        ws.send(Message::Text(text)).await?;
        pending.insert(order.client_order_id, order.ts_ns);
        events.write(json!({
            "event_type": "order_sent",
            "run_id": args.run_id,
            "bot_id": "priority_probe",
            "seq_no": seq_no,
            "send_ts_ns": order.ts_ns,
            "order": order,
        }))?;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_millis(args.drain_timeout_ms);
    while tokio::time::Instant::now() < deadline && (ack_count < 3 || fill_count < 1) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Some(message) = tokio::time::timeout(remaining, ws.next())
            .await
            .ok()
            .flatten()
        else {
            break;
        };
        let message = message?;
        let text = match message {
            Message::Text(text) => text,
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            _ => continue,
        };
        let value: Value = serde_json::from_str(&text)?;
        let message_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        match message_type {
            "ack" => {
                ack_count += 1;
                let client_order_id = value
                    .get("client_order_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let send_ts_ns = pending.remove(client_order_id).unwrap_or_default();
                outputs.write(json!({
                    "event_type": "ack_received",
                    "run_id": args.run_id,
                    "bot_id": "priority_probe",
                    "client_order_id": client_order_id,
                    "recv_ts_ns": now_ns(),
                    "latency_ns": now_ns().saturating_sub(send_ts_ns),
                    "message": value,
                }))?;
            }
            "fill" => {
                fill_count += 1;
                outputs.write(json!({
                    "event_type": "fill_received",
                    "run_id": args.run_id,
                    "bot_id": "priority_probe",
                    "engine_seq": value.get("engine_seq").and_then(Value::as_u64),
                    "recv_ts_ns": now_ns(),
                    "message": value,
                }))?;
            }
            _ => {}
        }
    }

    events.flush()?;
    outputs.flush()?;

    if ack_count != 3 {
        bail!("expected 3 acks, received {ack_count}");
    }
    if fill_count != 1 {
        bail!("expected 1 fill, received {fill_count}");
    }

    println!("run_id: {}", args.run_id);
    println!("orders_sent: 3");
    println!("acks_received: {ack_count}");
    println!("fills_received: {fill_count}");
    println!("events_out: {}", args.events_out.display());
    println!("outputs_out: {}", args.outputs_out.display());

    Ok(())
}

fn order(run_id: &str, id: &'static str, side: &'static str, ts_ns: u64) -> NewOrder {
    NewOrder {
        message_type: "new_order",
        run_id: run_id.to_string(),
        client_order_id: id,
        symbol: "PRIORITY_TEST",
        side,
        price: 10_025,
        qty: 5,
        ts_ns,
    }
}

struct JsonlWriter {
    writer: BufWriter<File>,
}

impl JsonlWriter {
    fn create(path: &PathBuf) -> Result<Self> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write(&mut self, value: Value) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &value)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
