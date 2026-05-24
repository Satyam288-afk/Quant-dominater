use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    process,
};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reference_orderbook::{Fill, NewOrder, OrderBook};
use serde_json::{json, Value};

#[derive(Debug, Parser)]
#[command(about = "Replay benchmark inputs through the reference orderbook and compare fills")]
struct Args {
    #[arg(long, default_value = "events.jsonl")]
    events: PathBuf,

    #[arg(long, default_value = "contestant_outputs.jsonl")]
    contestant_outputs: PathBuf,
}

#[derive(Debug)]
struct ActualFill {
    engine_seq: Option<u64>,
    fill: Fill,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (run_id, expected) = replay_expected_fills(&args.events)?;
    let actual = read_actual_fills(&args.contestant_outputs)?;

    let result = compare(run_id, &expected, &actual);
    println!("{}", serde_json::to_string_pretty(&result)?);

    if !result["valid"].as_bool().unwrap_or(false) {
        process::exit(1);
    }

    Ok(())
}

fn replay_expected_fills(path: &PathBuf) -> Result<(String, Vec<Fill>)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut expected = Vec::new();
    let mut run_id = String::from("unknown");

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {}:{}", path.display(), line_no + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse {}:{}", path.display(), line_no + 1))?;

        if let Some(order_value) = extract_message(&value, "new_order") {
            let order: NewOrder = serde_json::from_value(order_value.clone())
                .with_context(|| format!("decode new_order at line {}", line_no + 1))?;
            run_id = order.run_id.clone();
            let symbol = order
                .symbol
                .clone()
                .unwrap_or_else(|| "DEFAULT".to_string());
            let book = books.entry(symbol).or_default();
            let fills = book
                .process_new_order(order)
                .with_context(|| format!("reference replay failed at line {}", line_no + 1))?;
            expected.extend(fills.into_iter().map(|fill| fill.without_engine_seq()));
        } else if let Some(cancel_value) = extract_message(&value, "cancel_order") {
            if let Some(orig_id) = cancel_value
                .get("orig_client_order_id")
                .and_then(Value::as_str)
            {
                for book in books.values_mut() {
                    if book.cancel(orig_id) {
                        break;
                    }
                }
            } else {
                return Err(anyhow!(
                    "cancel_order missing orig_client_order_id at line {}",
                    line_no + 1
                ));
            }
        }
    }

    Ok((run_id, expected))
}

fn read_actual_fills(path: &PathBuf) -> Result<Vec<ActualFill>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut actual = Vec::new();
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

        actual.push(ActualFill {
            engine_seq: fill.engine_seq,
            fill: fill.without_engine_seq(),
        });
    }

    if actual.iter().all(|fill| fill.engine_seq.is_some()) {
        actual.sort_by_key(|fill| fill.engine_seq);
    }

    Ok(actual)
}

fn extract_message<'a>(value: &'a Value, expected_type: &str) -> Option<&'a Value> {
    let message = value.get("message").unwrap_or(value);
    let message = if expected_type == "new_order" {
        value.get("order").unwrap_or(message)
    } else {
        message
    };

    let message_type = message.get("type").and_then(Value::as_str)?;
    if message_type == expected_type {
        Some(message)
    } else {
        None
    }
}

fn compare(run_id: String, expected: &[Fill], actual: &[ActualFill]) -> Value {
    let actual_fills: Vec<Fill> = actual.iter().map(|entry| entry.fill.clone()).collect();
    if expected == actual_fills.as_slice() {
        return valid_result(run_id, expected.len());
    }

    let mut remaining_actual = fill_counts(&actual_fills);
    for (idx, expected_fill) in expected.iter().enumerate() {
        let key = fill_key(expected_fill);
        if decrement_count(&mut remaining_actual, &key) {
            continue;
        }

        if let Some(actual_candidate) = actual_fills
            .iter()
            .find(|fill| fill.price == expected_fill.price && fill.qty == expected_fill.qty)
        {
            return json!({
                "run_id": run_id,
                "valid": false,
                "reason": "PRICE_TIME_PRIORITY_VIOLATION",
                "first_bad_seq": idx + 1,
                "expected": expected_fill,
                "actual": actual_candidate,
            });
        }

        return json!({
            "run_id": run_id,
            "valid": false,
            "reason": "MISSING_FILL",
            "first_bad_seq": idx + 1,
            "expected": expected_fill,
            "actual": null,
        });
    }

    for actual_fill in &actual_fills {
        let key = fill_key(actual_fill);
        if decrement_count(&mut remaining_actual, &key) {
            return json!({
                "run_id": run_id,
                "valid": false,
                "reason": "UNEXPECTED_FILL",
                "first_bad_seq": expected.len() + 1,
                "expected": null,
                "actual": actual_fill,
            });
        }
    }

    valid_result(run_id, expected.len())
}

fn valid_result(run_id: String, fills_checked: usize) -> Value {
    json!({
        "run_id": run_id,
        "valid": true,
        "fills_checked": fills_checked,
    })
}

fn fill_counts(fills: &[Fill]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for fill in fills {
        *counts.entry(fill_key(fill)).or_insert(0) += 1;
    }
    counts
}

fn decrement_count(counts: &mut HashMap<String, usize>, key: &str) -> bool {
    let Some(count) = counts.get_mut(key) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}

fn fill_key(fill: &Fill) -> String {
    format!(
        "{}|{}|{}|{}",
        fill.buy_order_id, fill.sell_order_id, fill.price, fill.qty
    )
}
