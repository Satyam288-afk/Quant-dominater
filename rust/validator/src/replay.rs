use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rayon::prelude::*;
use reference_orderbook::{Fill, NewOrder, OrderBook};
use serde::Deserialize;
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
    Cancel {
        /// The cancel request's own id (used to look up its ack engine_seq so
        /// cancels are replayed in the engine's true arrival order). May be
        /// empty for legacy events that didn't carry one.
        client_order_id: String,
        orig_client_order_id: String,
    },
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
            // Deserialize straight from the borrowed sub-Value: `&Value`
            // implements `Deserializer`, so we skip the deep `clone()` the
            // old `from_value(order_value.clone())` paid on every line.
            let order = NewOrder::deserialize(order_value)
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
            let coid = cancel_value
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // Cancels are not symbol-tagged in the wire format; we replay
            // them across all books in the sequential merge step.
            events.push(Event {
                line_no: idx + 1,
                symbol: String::new(),
                kind: EventKind::Cancel {
                    client_order_id: coid.to_string(),
                    orig_client_order_id: orig.to_string(),
                },
            });
        }
    }

    Ok((run_id, events))
}

/// Build the lightweight `NewOrder` the orderbook actually consumes. The
/// matcher only reads `client_order_id`, `side`, `price`, `qty`, `ts_ns` and
/// `order_type`; the wire-only `run_id`/`symbol`/`message_type` strings are
/// never touched, so we clone just the one id string instead of deep-cloning
/// every `NewOrder` (which copied 3-4 Strings per order on the hot replay path).
fn matchable(order: &NewOrder) -> NewOrder {
    NewOrder {
        message_type: None,
        run_id: String::new(),
        client_order_id: order.client_order_id.clone(),
        symbol: None,
        side: order.side,
        price: order.price,
        qty: order.qty,
        ts_ns: order.ts_ns,
        order_type: order.order_type,
    }
}

/// Maps each `orig_client_order_id` to the single symbol whose book owns it,
/// i.e. the symbol of the (first) `new_order` that placed that id. A cancel is
/// resolved against exactly this one book.
///
/// Both the sequential and the sharded replay route cancels through this map,
/// which (a) makes the two paths produce identical fills even when the same id
/// is (degenerately) placed on more than one symbol — previously the sharded
/// path cancelled in *every* shard while the sequential path stopped at the
/// first, so the expected fills diverged depending on `--shards` — and (b)
/// turns the sharded cancel routing from O(symbols x cancels) into O(cancels).
/// For the normal case (an id lives on exactly one symbol) this is identical to
/// the old "scan every book, break on first match" behaviour.
fn build_cancel_owner(events: &[Event]) -> HashMap<&str, &str> {
    let mut owner: HashMap<&str, &str> = HashMap::new();
    for event in events {
        if let EventKind::NewOrder(order) = &event.kind {
            owner
                .entry(order.client_order_id.as_str())
                .or_insert(event.symbol.as_str());
        }
    }
    owner
}

/// Replay events in-order against a per-symbol set of orderbooks. Each cancel
/// is routed to the single book that owns its id (see `build_cancel_owner`).
fn replay_sequential(events: &[Event]) -> Vec<Fill> {
    let owner = build_cancel_owner(events);
    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut fills = Vec::new();
    for event in events {
        match &event.kind {
            EventKind::NewOrder(order) => {
                let book = books.entry(event.symbol.clone()).or_default();
                if let Ok(produced) = book.process_new_order(matchable(order)) {
                    fills.extend(produced.into_iter().map(|f| f.without_engine_seq()));
                }
            }
            EventKind::Cancel {
                orig_client_order_id,
                ..
            } => {
                if let Some(&sym) = owner.get(orig_client_order_id.as_str()) {
                    if let Some(book) = books.get_mut(sym) {
                        book.cancel(orig_client_order_id);
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
    // Each cancel is routed to the single symbol that owns its id, so it lands
    // in exactly one shard's bucket (in line_no order) instead of being copied
    // into every shard. This both removes the O(symbols x cancels) broadcast
    // and makes the sharded fills identical to the sequential ones for a
    // duplicated id (which the old broadcast cancelled in every shard).
    let owner = build_cancel_owner(events);
    let mut by_symbol: HashMap<&str, Vec<&Event>> = HashMap::new();
    let mut all_symbols: Vec<&str> = Vec::new();
    for event in events {
        let target = match &event.kind {
            EventKind::NewOrder(_) => Some(event.symbol.as_str()),
            // A cancel goes to its owning shard; an unknown id (never placed)
            // has no owner and is simply dropped — it would no-op anyway.
            EventKind::Cancel {
                orig_client_order_id,
                ..
            } => owner.get(orig_client_order_id.as_str()).copied(),
        };
        let Some(sym) = target else {
            continue;
        };
        let entry = by_symbol.entry(sym).or_default();
        if entry.is_empty() {
            all_symbols.push(sym);
        }
        entry.push(event);
    }
    // Each bucket is already in ascending line_no order: events are pushed in
    // file order and every event is appended to at most one bucket.

    let results: Vec<Vec<(usize, Fill)>> = all_symbols
        .par_iter()
        .map(|sym| {
            let bucket = by_symbol.get(sym).expect("bucket");
            let mut book = OrderBook::new();
            let mut out: Vec<(usize, Fill)> = Vec::new();
            for ev in bucket.iter() {
                match &ev.kind {
                    EventKind::NewOrder(order) => {
                        if let Ok(fills) = book.process_new_order(matchable(order)) {
                            for f in fills {
                                out.push((ev.line_no, f.without_engine_seq()));
                            }
                        }
                    }
                    EventKind::Cancel {
                        orig_client_order_id,
                        ..
                    } => {
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

/// Reorder events into the engine's *accepted* sequence.
///
/// The matching engine is the authoritative sequencer: every accepted order
/// carries a monotonic `engine_seq` in its ack. In a concurrent multi-bot run
/// the bot fleet's send order — i.e. the `events.jsonl` line order — is NOT the
/// order the engine processed orders in (many connections race, the engine
/// serialises them under its own lock). Replaying in line order therefore
/// produces *spurious* `PRICE_TIME_PRIORITY_VIOLATION`s for a perfectly correct
/// engine, because the reference book sees a different arrival interleaving.
///
/// We reconstruct the engine's true arrival order from the ack `engine_seq` and
/// renumber `line_no` to the new positions so a sharded replay still merges
/// correctly. We only do this when it is safe and unambiguous:
///   * every order has an accepted ack (so the sequence is total), and
///   * there are no cancels (cancel events carry only `orig_client_order_id`,
///     so they can't be mapped to an ack `engine_seq` — fall back to line order).
/// Otherwise the events are returned untouched (legacy line-order behaviour),
/// which keeps the ack-less fixtures and the cancel path byte-identical.
pub fn order_by_engine_seq(
    events: Vec<Event>,
    accept_seq: &HashMap<Arc<str>, u64>,
) -> Vec<Event> {
    if accept_seq.is_empty() {
        return events;
    }
    // The arrival sequence is total only if every event (new orders AND cancels)
    // carries an ack engine_seq. If any is missing one — e.g. legacy fixtures
    // without acks, or a cancel that pre-dates id-tagging — fall back to the
    // recorded line order rather than reorder on partial information.
    let arrival_key = |ev: &Event| -> Option<u64> {
        let id = match &ev.kind {
            EventKind::NewOrder(o) => &o.client_order_id,
            EventKind::Cancel {
                client_order_id, ..
            } => client_order_id,
        };
        if id.is_empty() {
            return None;
        }
        // Arc<str>: Borrow<str>, so look up the interned key by &str.
        accept_seq.get(id.as_str()).copied()
    };
    // Compute every arrival key in a single pass (each is a hashed-string map
    // lookup). If any event lacks one, bail before touching `events`. This
    // replaces the old all()-pre-pass + per-comparison lookup in the sort key,
    // which did ~N + N*log(N) lookups; we now do exactly N.
    let mut keys = Vec::with_capacity(events.len());
    for ev in &events {
        match arrival_key(ev) {
            Some(k) => keys.push(k),
            None => return events,
        }
    }

    // Pair each precomputed key with its event and sort once on (key, line_no);
    // no per-comparison map lookup.
    let mut keyed: Vec<(u64, Event)> = keys.into_iter().zip(events).collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.line_no.cmp(&b.1.line_no)));
    keyed
        .into_iter()
        .enumerate()
        .map(|(idx, (_, mut ev))| {
            ev.line_no = idx + 1;
            ev
        })
        .collect()
}

fn extract_message<'a>(value: &'a Value, expected_type: &str) -> Option<&'a Value> {
    // The fleet wraps both new orders and cancels under an "order" key; some
    // logs nest under "message". Prefer the payload, fall back to the value.
    let message = value
        .get("order")
        .or_else(|| value.get("message"))
        .unwrap_or(value);

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

    fn ev_new(
        line_no: usize,
        sym: &str,
        id: &str,
        side: Side,
        price: i64,
        qty: i64,
        ts: u64,
    ) -> Event {
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
    fn engine_seq_reorder_matches_engine_arrival_not_send_order() {
        // Send order (events.jsonl line order) ≠ engine arrival order. All on
        // symbol A at the same price, so only time/arrival priority decides the
        // pairing. A rests; whichever buy the engine processed FIRST crosses it.
        //
        // Engine processed: A_sell(seq1), then C_buy(seq2) → fill(C,A); B rests.
        // But the bots *logged* their sends as A, B, C (B before C).
        let send_order = vec![
            ev_new(1, "A", "A_sell", Side::Sell, 100, 5, 10),
            ev_new(2, "A", "B_buy", Side::Buy, 100, 5, 20),
            ev_new(3, "A", "C_buy", Side::Buy, 100, 5, 5),
        ];

        // Replaying in raw send order pairs the WRONG buy (B), because the
        // reference sees B before C.
        let send_fills = replay_expected_fills(&send_order, 1);
        assert_eq!(send_fills.len(), 1);
        assert_eq!(send_fills[0].buy_order_id, "B_buy");

        // Reorder by the engine's accepted sequence (from ack engine_seq), then
        // replay — now it pairs C, exactly what the engine produced.
        let accept: HashMap<Arc<str>, u64> = HashMap::from([
            (Arc::from("A_sell"), 1u64),
            (Arc::from("C_buy"), 2u64),
            (Arc::from("B_buy"), 3u64),
        ]);
        let engine_order = order_by_engine_seq(send_order, &accept);
        let engine_fills = replay_expected_fills(&engine_order, 1);
        assert_eq!(engine_fills.len(), 1);
        assert_eq!(engine_fills[0].buy_order_id, "C_buy");
        assert_eq!(engine_fills[0].sell_order_id, "A_sell");
    }

    fn ev_cancel(line_no: usize, coid: &str, orig: &str) -> Event {
        Event {
            line_no,
            symbol: String::new(),
            kind: EventKind::Cancel {
                client_order_id: coid.to_string(),
                orig_client_order_id: orig.to_string(),
            },
        }
    }

    #[test]
    fn cancel_is_replayed_in_engine_arrival_order() {
        // Engine order: a rests, b rests, cancel(a), c arrives (a is gone so it
        // does NOT trade with a), d crosses c. The reference must drop "a" at the
        // cancel's arrival slot, so the only fill pairs c with d — never c with a.
        let events = vec![
            ev_new(1, "A", "a", Side::Sell, 100, 5, 1),
            ev_new(2, "A", "b", Side::Buy, 90, 5, 2),
            ev_cancel(3, "ca", "a"),
            ev_new(4, "A", "c", Side::Buy, 100, 5, 4),
            ev_new(5, "A", "d", Side::Sell, 90, 5, 5),
        ];
        let arrival: HashMap<Arc<str>, u64> = HashMap::from([
            (Arc::from("a"), 1u64),
            (Arc::from("b"), 2),
            (Arc::from("ca"), 3),
            (Arc::from("c"), 4),
            (Arc::from("d"), 5),
        ]);
        let ordered = order_by_engine_seq(events, &arrival);
        let fills = replay_expected_fills(&ordered, 1);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].buy_order_id, "c");
        assert_eq!(fills[0].sell_order_id, "d");
    }

    #[test]
    fn engine_seq_reorder_falls_back_without_acks() {
        let events = vec![
            ev_new(1, "A", "b1", Side::Buy, 100, 5, 1),
            ev_new(2, "A", "s1", Side::Sell, 100, 5, 2),
        ];
        // Empty arrival map → untouched (line order preserved).
        let out = order_by_engine_seq(events.clone(), &HashMap::new());
        assert_eq!(out[0].line_no, 1);
        assert_eq!(out[1].line_no, 2);
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

    #[test]
    fn sequential_and_sharded_agree_on_duplicated_id_cancel() {
        // The SAME client_order_id "dup" rests on two different symbols. A
        // cancel for "dup" must hit exactly ONE book (its owning shard = the
        // first symbol that placed it, "A"). The old sharded path broadcast the
        // cancel into every shard, so it dropped BOTH resting orders and the
        // expected fills diverged from the sequential path depending on
        // --shards. Now both paths route the cancel to symbol A only.
        //
        // A: buy "dup"@100 (owner of "dup"); cancelled -> A has no resting buy,
        //    so A's sell@100 finds nothing.
        // B: buy "dup"@200 survives; B's sell@200 crosses it -> the single fill.
        let events = vec![
            ev_new(1, "A", "dup", Side::Buy, 100, 5, 1),
            ev_new(2, "B", "dup", Side::Buy, 200, 5, 2),
            ev_cancel(3, "c1", "dup"),
            ev_new(4, "A", "sa", Side::Sell, 100, 5, 4),
            ev_new(5, "B", "sb", Side::Sell, 200, 5, 5),
        ];
        let seq = replay_expected_fills(&events, 1);
        let par = replay_expected_fills(&events, 4);
        assert_eq!(seq, par, "sharded must equal sequential for a duplicated-id cancel");
        assert_eq!(seq.len(), 1);
        // The surviving fill is B's: sell "sb" against the still-resting "dup".
        assert_eq!(seq[0].buy_order_id, "dup");
        assert_eq!(seq[0].sell_order_id, "sb");
        assert_eq!(seq[0].price, 200);
    }
}
