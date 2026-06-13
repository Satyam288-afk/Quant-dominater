use std::collections::HashMap;
use std::hash::{Hash, Hasher};

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
    // Diff logic mirrors the original validator. Edge case detectors only
    // run if the diff itself passes (i.e. fills match) — otherwise the diff
    // failure stays primary and the edge cases ride in `extra_violations`.
    // The deduped fills are addressed by index out of the single materialised
    // Vec, so we never clone a second full copy of every fill here.
    let diff_result = diff(&run_id, expected, fills, deduped_idx);

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

/// Multiset key for a fill: a 64-bit hash of its four identifying fields. The
/// previous implementation `format!`'d a `String` per fill (an allocation on
/// the hot diff path); hashing the borrowed fields in place is allocation-free.
/// The exact-equality fast path below short-circuits the all-correct case, so
/// hashing only feeds the order-insensitive multiset diff that runs once a
/// mismatch is already known to exist; a 64-bit collision there (~2^-64) is
/// far below the noise floor of the run itself.
type FillKey = u64;

fn diff(run_id: &str, expected: &[Fill], fills: &[ActualFill], deduped_idx: &[usize]) -> Value {
    // The deduped actual fills, addressed by index without copying.
    let actual = |i: usize| -> &Fill { &fills[deduped_idx[i]].fill };
    let n_actual = deduped_idx.len();

    if expected.len() == n_actual && expected.iter().enumerate().all(|(i, e)| e == actual(i)) {
        return valid_result(run_id, expected.len());
    }

    let mut remaining_actual = fill_counts(fills, deduped_idx);
    for (idx, expected_fill) in expected.iter().enumerate() {
        let key = fill_key(expected_fill);
        if decrement_count(&mut remaining_actual, &key) {
            continue;
        }

        if let Some(actual_candidate) = (0..n_actual)
            .map(actual)
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

    for i in 0..n_actual {
        let actual_fill = actual(i);
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

fn fill_counts(fills: &[ActualFill], deduped_idx: &[usize]) -> HashMap<FillKey, usize> {
    let mut counts = HashMap::new();
    for &i in deduped_idx {
        *counts.entry(fill_key(&fills[i].fill)).or_insert(0) += 1;
    }
    counts
}

fn decrement_count(counts: &mut HashMap<FillKey, usize>, key: &FillKey) -> bool {
    let Some(count) = counts.get_mut(key) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}

fn fill_key(fill: &Fill) -> FillKey {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    fill.buy_order_id.hash(&mut hasher);
    fill.sell_order_id.hash(&mut hasher);
    fill.price.hash(&mut hasher);
    fill.qty.hash(&mut hasher);
    hasher.finish()
}
