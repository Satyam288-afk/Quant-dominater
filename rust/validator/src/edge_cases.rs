use std::collections::{HashMap, HashSet};

use crate::replay::{Event, EventKind};
use reference_orderbook::Fill;
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize)]
pub struct Violation {
    pub reason: &'static str,
    pub detail: Value,
}

pub struct ActualFill {
    pub engine_seq: Option<u64>,
    pub fill: Fill,
}

/// Detectors that look only at events.jsonl + contestant_outputs.jsonl.
/// Returns the ordered list of violations; the caller decides which becomes
/// the primary `reason`.
pub fn detect(
    events: &[Event],
    raw_actual: &[ActualFill],
    deduped_actual: &[ActualFill],
) -> Vec<Violation> {
    let mut out = Vec::new();

    // Known order ids and remaining qty per id (from new_order events).
    let mut placed_qty: HashMap<String, i64> = HashMap::new();
    let mut cancelled_after_line: HashMap<String, usize> = HashMap::new();
    for ev in events {
        match &ev.kind {
            EventKind::NewOrder(order) => {
                placed_qty.insert(order.client_order_id.clone(), order.qty);
            }
            EventKind::Cancel { orig_client_order_id } => {
                cancelled_after_line.insert(orig_client_order_id.clone(), ev.line_no);
            }
        }
    }

    // DUPLICATE_FILL: any raw fill that the main loop discarded as a dupe.
    if raw_actual.len() > deduped_actual.len() {
        out.push(Violation {
            reason: "DUPLICATE_FILL",
            detail: json!({
                "raw_count": raw_actual.len(),
                "unique_count": deduped_actual.len(),
            }),
        });
    }

    // OUT_OF_ORDER_SEQ: engine_seq must be monotonically non-decreasing.
    let mut last_seq: Option<u64> = None;
    let mut ooo_first: Option<(u64, u64)> = None;
    for entry in deduped_actual {
        if let Some(s) = entry.engine_seq {
            if let Some(prev) = last_seq {
                if s < prev {
                    ooo_first = Some((prev, s));
                    break;
                }
            }
            last_seq = Some(s);
        }
    }
    if let Some((prev, cur)) = ooo_first {
        out.push(Violation {
            reason: "OUT_OF_ORDER_SEQ",
            detail: json!({ "prev": prev, "current": cur }),
        });
    }

    // UNKNOWN_ORDER_FILL: fill references a client_order_id that was never
    // placed via new_order. Plus PARTIAL_FILL_OVER_QTY: cumulative fill
    // qty per side > original order qty.
    let mut buy_filled: HashMap<String, i64> = HashMap::new();
    let mut sell_filled: HashMap<String, i64> = HashMap::new();
    let mut unknown_id: Option<String> = None;
    let mut overfill: Option<(String, i64, i64)> = None;
    for entry in deduped_actual {
        let f = &entry.fill;
        for id in [&f.buy_order_id, &f.sell_order_id] {
            if !placed_qty.contains_key(id) {
                unknown_id.get_or_insert_with(|| id.clone());
            }
        }
        *buy_filled.entry(f.buy_order_id.clone()).or_insert(0) += f.qty;
        *sell_filled.entry(f.sell_order_id.clone()).or_insert(0) += f.qty;
    }
    if let Some(id) = unknown_id {
        out.push(Violation {
            reason: "UNKNOWN_ORDER_FILL",
            detail: json!({ "client_order_id": id }),
        });
    }
    for (id, qty) in buy_filled.iter().chain(sell_filled.iter()) {
        if let Some(&placed) = placed_qty.get(id) {
            if *qty > placed {
                overfill = Some((id.clone(), placed, *qty));
                break;
            }
        }
    }
    if let Some((id, placed, filled)) = overfill {
        out.push(Violation {
            reason: "PARTIAL_FILL_OVER_QTY",
            detail: json!({
                "client_order_id": id,
                "order_qty": placed,
                "filled_qty": filled,
            }),
        });
    }

    // CANCEL_RACE: a fill landing for an id whose cancel was emitted in the
    // events stream. We don't have ack timestamps here, but we have the
    // events file ordering: if a cancel exists for an id, ANY subsequent
    // fill for that id is racy. Reported once.
    let mut cancel_race_id: Option<String> = None;
    let cancelled_ids: HashSet<&String> = cancelled_after_line.keys().collect();
    for entry in deduped_actual {
        let f = &entry.fill;
        if cancelled_ids.contains(&f.buy_order_id) || cancelled_ids.contains(&f.sell_order_id) {
            cancel_race_id = Some(
                if cancelled_ids.contains(&f.buy_order_id) {
                    f.buy_order_id.clone()
                } else {
                    f.sell_order_id.clone()
                },
            );
            break;
        }
    }
    if let Some(id) = cancel_race_id {
        out.push(Violation {
            reason: "CANCEL_RACE",
            detail: json!({ "client_order_id": id }),
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use reference_orderbook::{NewOrder, OrderType, Side};

    fn order(id: &str, qty: i64) -> Event {
        Event {
            line_no: 1,
            symbol: "A".into(),
            kind: EventKind::NewOrder(NewOrder {
                message_type: Some("new_order".into()),
                run_id: "r".into(),
                client_order_id: id.into(),
                symbol: Some("A".into()),
                side: Side::Buy,
                price: 100,
                qty,
                ts_ns: 0,
                order_type: OrderType::Limit,
            }),
        }
    }

    fn fill(buy: &str, sell: &str, qty: i64, seq: Option<u64>) -> ActualFill {
        ActualFill {
            engine_seq: seq,
            fill: Fill {
                buy_order_id: buy.into(),
                sell_order_id: sell.into(),
                price: 100,
                qty,
                engine_seq: seq,
            },
        }
    }

    #[test]
    fn flags_unknown_order() {
        let events = vec![order("a", 5)];
        let actual = vec![fill("a", "ghost", 1, Some(1))];
        let v = detect(&events, &actual, &actual);
        assert!(v.iter().any(|x| x.reason == "UNKNOWN_ORDER_FILL"));
    }

    #[test]
    fn flags_overfill() {
        let events = vec![order("a", 5), order("b", 5)];
        let actual = vec![fill("a", "b", 6, Some(1))];
        let v = detect(&events, &actual, &actual);
        assert!(v.iter().any(|x| x.reason == "PARTIAL_FILL_OVER_QTY"));
    }

    #[test]
    fn flags_out_of_order_seq() {
        let events = vec![order("a", 5), order("b", 5)];
        let actual = vec![fill("a", "b", 1, Some(2)), fill("a", "b", 1, Some(1))];
        let v = detect(&events, &actual, &actual);
        assert!(v.iter().any(|x| x.reason == "OUT_OF_ORDER_SEQ"));
    }
}
