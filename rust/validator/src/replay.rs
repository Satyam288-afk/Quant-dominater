use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use rayon::prelude::*;
use reference_orderbook::{Fill, NewOrder, OrderBook};
use serde_json::Value;

/// Parsed event from `events.jsonl`. `line_no` is preserved so a sharded
/// replay can be merged back into the same global order a sequential replay
/// would have produced.
#[derive(Clone, Debug)]
pub struct Event {
    pub line_no: usize,
    pub symbol: String,
    pub kind: EventKind,
}

#[derive(Clone, Debug)]
pub enum EventKind {
    NewOrder(NewOrder),
    Cancel { orig_client_order_id: String },
}

/// Reads the events file, returning the run_id (first one seen) and the
/// ordered list of parsed events.
pub fn read_events(path: &Path) -> Result<(String, Vec<Event>)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut run_id = String::from("unknown");

    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {}:{}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse {}:{}", path.display(), idx + 1))?;

        if let Some(order_value) = extract_message(&value, "new_order") {
            let order: NewOrder = serde_json::from_value(order_value.clone())
                .with_context(|| format!("decode new_order at line {}", idx + 1))?;
            if run_id == "unknown" {
                run_id = order.run_id.clone();
            }
            let symbol = order
                .symbol
                .clone()
                .unwrap_or_else(|| "DEFAULT".to_string());
            events.push(Event {
                line_no: idx + 1,
                symbol,
                kind: EventKind::NewOrder(order),
            });
        } else if let Some(cancel_value) = extract_message(&value, "cancel_order") {
            let orig = cancel_value
                .get("orig_client_order_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow!(
                        "cancel_order missing orig_client_order_id at line {}",
                        idx + 1
                    )
                })?;
            // Cancels are not symbol-tagged in the wire format; we replay
            // them across all books in the sequential merge step.
            events.push(Event {
                line_no: idx + 1,
                symbol: String::new(),
                kind: EventKind::Cancel {
                    orig_client_order_id: orig.to_string(),
                },
            });
        }
    }

    Ok((run_id, events))
}

/// Replay events in-order against a per-symbol set of orderbooks. Cancels
/// hit every book until one matches (mirrors the previous validator).
fn replay_sequential(events: &[Event]) -> Vec<Fill> {
    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut fills = Vec::new();
    for event in events {
        match &event.kind {
            EventKind::NewOrder(order) => {
                let book = books.entry(event.symbol.clone()).or_default();
                if let Ok(produced) = book.process_new_order(order.clone()) {
                    fills.extend(produced.into_iter().map(|f| f.without_engine_seq()));
                }
            }
            EventKind::Cancel { orig_client_order_id } => {
                for book in books.values_mut() {
                    if book.cancel(orig_client_order_id) {
                        break;
                    }
                }
            }
        }
    }
    fills
}

/// Parallel sharded replay: each symbol owns one OrderBook; symbols are
/// replayed in parallel via rayon, then fills are merged back in line_no
/// order so the final sequence matches what a sequential replay produces.
fn replay_sharded(events: &[Event]) -> Vec<Fill> {
    // Partition by symbol. Cancels lack a symbol, so we copy each cancel
    // into every shard — the shard whose book holds the id will resolve it,
    // the others no-op. This preserves the original "scan every book" behaviour.
    let mut by_symbol: HashMap<String, Vec<&Event>> = HashMap::new();
    let mut cancels: Vec<&Event> = Vec::new();
    let mut all_symbols: Vec<String> = Vec::new();
    for event in events {
        match &event.kind {
            EventKind::NewOrder(_) => {
                let entry = by_symbol.entry(event.symbol.clone()).or_default();
                if entry.is_empty() {
                    all_symbols.push(event.symbol.clone());
                }
                entry.push(event);
            }
            EventKind::Cancel { .. } => cancels.push(event),
        }
    }
    for sym in &all_symbols {
        let bucket = by_symbol.get_mut(sym).expect("bucket exists");
        // Splice in all cancels in their original order; the shard ignores
        // cancels whose id it doesn't know about.
        let mut merged: Vec<&Event> = Vec::with_capacity(bucket.len() + cancels.len());
        let mut i = 0;
        let mut j = 0;
        while i < bucket.len() && j < cancels.len() {
            if bucket[i].line_no < cancels[j].line_no {
                merged.push(bucket[i]);
                i += 1;
            } else {
                merged.push(cancels[j]);
                j += 1;
            }
        }
        merged.extend_from_slice(&bucket[i..]);
        merged.extend_from_slice(&cancels[j..]);
        *bucket = merged;
    }

    let results: Vec<Vec<(usize, Fill)>> = all_symbols
        .par_iter()
        .map(|sym| {
            let bucket = by_symbol.get(sym).expect("bucket");
            let mut book = OrderBook::new();
            let mut out: Vec<(usize, Fill)> = Vec::new();
            for ev in bucket.iter() {
                match &ev.kind {
                    EventKind::NewOrder(order) => {
                        if let Ok(fills) = book.process_new_order(order.clone()) {
                            for f in fills {
                                out.push((ev.line_no, f.without_engine_seq()));
                            }
                        }
                    }
                    EventKind::Cancel { orig_client_order_id } => {
                        book.cancel(orig_client_order_id);
                    }
                }
            }
            out
        })
        .collect();

    let mut all: Vec<(usize, Fill)> = results.into_iter().flatten().collect();
    all.sort_by_key(|(line_no, _)| *line_no);
    all.into_iter().map(|(_, f)| f).collect()
}

/// Replay strategy entrypoint. shards <= 1 runs the sequential path
/// (byte-identical to legacy validator); otherwise uses rayon's pool.
pub fn replay_expected_fills(events: &[Event], shards: usize) -> Vec<Fill> {
    if shards <= 1 {
        replay_sequential(events)
    } else {
        replay_sharded(events)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use reference_orderbook::{OrderType, Side};

    fn ev_new(line_no: usize, sym: &str, id: &str, side: Side, price: i64, qty: i64, ts: u64) -> Event {
        Event {
            line_no,
            symbol: sym.to_string(),
            kind: EventKind::NewOrder(NewOrder {
                message_type: Some("new_order".into()),
                run_id: "r".into(),
                client_order_id: id.into(),
                symbol: Some(sym.into()),
                side,
                price,
                qty,
                ts_ns: ts,
                order_type: OrderType::Limit,
            }),
        }
    }

    #[test]
    fn sequential_and_sharded_produce_same_fills() {
        let events = vec![
            ev_new(1, "A", "b1", Side::Buy, 100, 5, 1),
            ev_new(2, "A", "s1", Side::Sell, 100, 5, 2),
            ev_new(3, "B", "b2", Side::Buy, 200, 3, 3),
            ev_new(4, "B", "s2", Side::Sell, 200, 3, 4),
            ev_new(5, "A", "b3", Side::Buy, 100, 7, 5),
            ev_new(6, "A", "s3", Side::Sell, 100, 7, 6),
        ];
        let seq = replay_expected_fills(&events, 1);
        let par = replay_expected_fills(&events, 4);
        assert_eq!(seq, par);
        assert_eq!(seq.len(), 3);
    }
}
