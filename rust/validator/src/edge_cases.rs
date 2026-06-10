use std::collections::HashMap;

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

    // Known order ids and remaining qty per id (from new_order events). Cancels
    // need no bookkeeping here — the diff validates them in arrival order.
    let mut placed_qty: HashMap<String, i64> = HashMap::new();
    for ev in events {
        if let EventKind::NewOrder(order) = &ev.kind {
            placed_qty.insert(order.client_order_id.clone(), order.qty);
        }
    }

    // INCONSISTENT_FILL: a fill (identified by engine_seq) is reported to BOTH
    // counterparties — and, through the bot fleet's connection pool, by every
    // connection that carries one of those counterparties. So the SAME fill
    // legitimately appears multiple times in contestant_outputs.jsonl; that is
    // correct two-sided execution reporting, not a bug. We dedup those identical
    // copies elsewhere by engine_seq. The real violation is when two reports
    // share an engine_seq but DISAGREE on the trade (buy/sell/price/qty) — that
    // means the engine emitted an inconsistent execution report. Only flag that.
    let mut by_seq: HashMap<u64, &Fill> = HashMap::new();
    for entry in raw_actual {
        let Some(seq) = entry.engine_seq else { continue };
        match by_seq.get(&seq) {
            Some(prev)
                if prev.buy_order_id != entry.fill.buy_order_id
                    || prev.sell_order_id != entry.fill.sell_order_id
                    || prev.price != entry.fill.price
                    || prev.qty != entry.fill.qty =>
            {
                out.push(Violation {
                    reason: "INCONSISTENT_FILL",
                    detail: json!({
                        "engine_seq": seq,
                        "first": prev,
                        "second": entry.fill,
                    }),
                });
                break;
            }
            Some(_) => {}
            None => {
                by_seq.insert(seq, &entry.fill);
            }
        }
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

    // NOTE: we deliberately do NOT flag a "cancel race" — a cancel that arrives
    // after its order already traded legitimately returns not_found, and the
    // fill stands. The authoritative check is the diff: the reference replays
    // cancels in the engine's true arrival order (by ack engine_seq), so if the
    // engine ever filled a genuinely-cancelled order the diff surfaces it as an
    // UNEXPECTED_FILL / PRICE_TIME_PRIORITY_VIOLATION. A heuristic that flags
    // any fill for a once-cancelled id only produces false positives here.

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
