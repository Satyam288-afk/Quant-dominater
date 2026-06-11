use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process;

use anyhow::{Context, Result};
use clap::Parser;
use reference_orderbook::Fill;
use serde_json::{json, Value};

mod compare;
mod edge_cases;
mod replay;

use edge_cases::ActualFill;
use replay::EventKind;

#[derive(Debug, Parser)]
#[command(about = "Replay benchmark inputs through the reference orderbook and compare fills")]
struct Args {
    #[arg(long, default_value = "events.jsonl")]
    events: PathBuf,

    #[arg(long, default_value = "contestant_outputs.jsonl")]
    contestant_outputs: PathBuf,

    /// Number of replay shards. 1 = sequential (legacy, byte-identical).
    /// Higher values partition by symbol via rayon. Defaults to 1.
    #[arg(long, default_value_t = 1)]
    shards: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (run_id, events) = replay::read_events(&args.events)?;
    if !events
        .iter()
        .any(|event| matches!(event.kind, EventKind::NewOrder(_)))
    {
        let result = json!({
            "run_id": run_id,
            "valid": false,
            "reason": "NO_BENCHMARK_EVENTS",
            "fills_checked": 0,
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
        process::exit(1);
    }
    // Reconstruct the engine's authoritative arrival order from the ack
    // engine_seq, then replay in that order. Under concurrent multi-bot load the
    // events.jsonl line order is the bots' *send* order, not the order the
    // engine serialised and processed orders in — replaying in send order
    // produces spurious price-time-priority diffs for a correct engine.
    let arrival_seq = read_arrival_order(&args.contestant_outputs)?;
    let events = replay::order_by_engine_seq(events, &arrival_seq);
    let expected: Vec<Fill> = replay::replay_expected_fills(&events, args.shards);
    let (raw_actual, deduped_actual) = read_actual_fills(&args.contestant_outputs)?;

    let violations = edge_cases::detect(&events, &raw_actual, &deduped_actual);
    let result = compare::compare(run_id, &expected, &deduped_actual, &violations);
    println!("{}", serde_json::to_string_pretty(&result)?);

    if !result["valid"].as_bool().unwrap_or(false) {
        process::exit(1);
    }
    Ok(())
}

fn read_actual_fills(path: &PathBuf) -> Result<(Vec<ActualFill>, Vec<ActualFill>)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut raw = Vec::new();
    let mut deduped = Vec::new();
    let mut seen_engine_seq = HashSet::new();
    let mut seen_fill_key = HashSet::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {}:{}", path.display(), line_no + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse {}:{}", path.display(), line_no + 1))?;
        let Some(fill_value) = extract_message(&value, "fill") else {
            continue;
        };

        let fill: Fill = serde_json::from_value(fill_value.clone())
            .with_context(|| format!("decode fill at line {}", line_no + 1))?;

        raw.push(ActualFill {
            engine_seq: fill.engine_seq,
            fill: fill.without_engine_seq(),
        });

        if let Some(engine_seq) = fill.engine_seq {
            if !seen_engine_seq.insert(engine_seq) {
                continue;
            }
        } else {
            let key = format!(
                "{}|{}|{}|{}",
                fill.buy_order_id, fill.sell_order_id, fill.price, fill.qty
            );
            if !seen_fill_key.insert(key) {
                continue;
            }
        }

        deduped.push(ActualFill {
            engine_seq: fill.engine_seq,
            fill: fill.without_engine_seq(),
        });
    }

    if deduped.iter().all(|f| f.engine_seq.is_some()) {
        deduped.sort_by_key(|f| f.engine_seq);
    }

    Ok((raw, deduped))
}

/// Build a map of `client_order_id -> engine_seq` from every ack the contestant
/// sent — new-order acks AND cancel acks, regardless of status. The engine
/// stamps a monotonic `engine_seq` on each ack at the moment it processes the
/// request, so this map is the engine's authoritative *arrival* sequence, which
/// the replay uses to reconstruct the true processing order under concurrency.
/// New-order and cancel ids never collide, so one map orders both.
fn read_arrival_order(path: &PathBuf) -> Result<HashMap<String, u64>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut arrival_seq = HashMap::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {}:{}", path.display(), line_no + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse {}:{}", path.display(), line_no + 1))?;
        let Some(ack) = extract_message(&value, "ack") else {
            continue;
        };
        let (Some(coid), Some(seq)) = (
            ack.get("client_order_id").and_then(Value::as_str),
            ack.get("engine_seq").and_then(Value::as_u64),
        ) else {
            continue;
        };
        // First ack wins (a re-ack would only ever be a duplicate delivery).
        arrival_seq.entry(coid.to_string()).or_insert(seq);
    }

    Ok(arrival_seq)
}

fn extract_message<'a>(value: &'a Value, expected_type: &str) -> Option<&'a Value> {
    let message = value.get("message").unwrap_or(value);
    let message_type = message.get("type").and_then(Value::as_str)?;
    if message_type == expected_type {
        Some(message)
    } else {
        None
    }
}
