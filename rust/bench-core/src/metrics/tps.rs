use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Counts acks per absolute wall-clock second, keyed on `recv_ts_ns / 1e9`, so
/// `peak_tps` is the true per-second maximum **independent of arrival order**.
///
/// TM-1: the previous design bucketed by `(ts - first_seen_start) / 1s` into a
/// rolling Vec, which collapsed any out-of-order event (`ts < start`) into
/// bucket[0] and distorted the peak when partition-interleaved Kafka events
/// arrived non-monotonically. Bucketing by the absolute second key removes the
/// ordering dependency entirely — every event lands in the bucket for the
/// second it actually occurred in, regardless of when it was observed.
///
/// NOTE: `recv_ts_ns` is a per-host wall clock. Across pods this assumes
/// NTP/chrony keeps clocks aligned (a deployment precondition); a single-clock
/// rebucket is out of scope for this fix.
pub struct TpsCounter {
    /// second-key (`recv_ts_ns / 1e9`) -> ack count in that second.
    buckets: Mutex<HashMap<u64, u64>>,
    total: AtomicU64,
}

impl TpsCounter {
    /// `_window_secs` is retained for call-site/API compatibility but no longer
    /// pre-sizes a fixed ring: buckets are sparse and keyed by absolute second,
    /// so only seconds that actually saw an ack consume memory.
    pub fn new(_window_secs: usize) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            total: AtomicU64::new(0),
        }
    }

    pub fn record(&self, ts_ns: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let second = ts_ns / 1_000_000_000;
        let mut buckets = self.buckets.lock().unwrap();
        *buckets.entry(second).or_insert(0) += 1;
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn peak_tps(&self) -> u64 {
        let buckets = self.buckets.lock().unwrap();
        buckets.values().copied().max().unwrap_or(0)
    }

    pub fn avg_tps(&self, observed_secs: f64) -> f64 {
        if observed_secs <= 0.0 {
            return 0.0;
        }
        self.total() as f64 / observed_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_within_window() {
        let c = TpsCounter::new(3);
        let base = 1_000_000_000;
        c.record(base);
        c.record(base + 100);
        c.record(base + 1_500_000_000);
        c.record(base + 2_500_000_000);
        assert_eq!(c.total(), 4);
        assert!(c.peak_tps() >= 1);
    }

    #[test]
    fn peak_is_order_independent() {
        // TM-1: feeding events in REVERSE (and otherwise scrambled) order must
        // yield the same per-second peak as in-order, because we bucket by the
        // absolute second of each event, not by arrival sequence.
        // Layout: second 10 has 3 acks, second 11 has 1, second 12 has 2.
        // True per-second max = 3.
        let in_order: Vec<u64> = vec![
            10_000_000_000,
            10_300_000_000,
            10_600_000_000, // second 10 x3
            11_100_000_000, // second 11 x1
            12_200_000_000,
            12_800_000_000, // second 12 x2
        ];

        let forward = TpsCounter::new(8);
        for ts in &in_order {
            forward.record(*ts);
        }
        assert_eq!(forward.peak_tps(), 3);
        assert_eq!(forward.total(), 6);

        let reverse = TpsCounter::new(8);
        for ts in in_order.iter().rev() {
            reverse.record(*ts);
        }
        assert_eq!(reverse.peak_tps(), 3, "reverse order must match true peak");
        assert_eq!(reverse.total(), 6);
    }
}
