use std::collections::HashMap;

use reference_orderbook::Fill;
use serde_json::{json, Value};

use crate::edge_cases::{ActualFill, Violation};

pub fn compare(
    run_id: String,
    expected: &[Fill],
    fills: &[ActualFill],
    deduped_idx: &[usize],
    extra_violations: &[Violation],
) -> Value {
    // The deduped fills selected by index out of the single materialised Vec.
    let actual_fills: Vec<Fill> = deduped_idx.iter().map(|&i| fills[i].fill.clone()).collect();

    // Diff logic mirrors the original validator. Edge case detectors only
    // run if the diff itself passes (i.e. fills match) — otherwise the diff
    // failure stays primary and the edge cases ride in `extra_violations`.
    let diff_result = diff(&run_id, expected, &actual_fills);

    let mut out = diff_result;
    if !extra_violations.is_empty() {
        let arr: Vec<Value> = extra_violations
            .iter()
            .map(|v| json!({ "reason": v.reason, "detail": v.detail.clone() }))
            .collect();
        // If diff passed but we have edge violations, demote `valid` to false
        // and let the first violation become the primary reason.
        if out.get("valid").and_then(Value::as_bool) == Some(true) {
            let primary = &extra_violations[0];
            out = json!({
                "run_id": run_id,
                "valid": false,
                "reason": primary.reason,
                "detail": primary.detail.clone(),
                "fills_checked": expected.len(),
                "extra_violations": arr,
            });
        } else if let Some(obj) = out.as_object_mut() {
            obj.insert("extra_violations".into(), Value::Array(arr));
        }
    }
    out
}

fn diff(run_id: &str, expected: &[Fill], actual_fills: &[Fill]) -> Value {
    if expected == actual_fills {
        return valid_result(run_id, expected.len());
    }

    let mut remaining_actual = fill_counts(actual_fills);
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
            "actual": Value::Null,
        });
    }

    for actual_fill in actual_fills {
        let key = fill_key(actual_fill);
        if decrement_count(&mut remaining_actual, &key) {
            return json!({
                "run_id": run_id,
                "valid": false,
                "reason": "UNEXPECTED_FILL",
                "first_bad_seq": expected.len() + 1,
                "expected": Value::Null,
                "actual": actual_fill,
            });
        }
    }

    valid_result(run_id, expected.len())
}

fn valid_result(run_id: &str, fills_checked: usize) -> Value {
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
