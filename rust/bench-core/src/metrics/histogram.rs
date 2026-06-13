use std::sync::Mutex;

use hdrhistogram::Histogram;

pub struct LatencyHistogram {
    inner: Mutex<Histogram<u64>>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // 1ns..=60s, 3 significant digits.
        let h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("hdrhistogram bounds invalid");
        Self {
            inner: Mutex::new(h),
        }
    }

    pub fn record_ns(&self, latency_ns: u64) {
        if latency_ns == 0 {
            return;
        }
        let mut guard = self.inner.lock().unwrap();
        let clamped = latency_ns.min(60_000_000_000);
        let _ = guard.record(clamped);
    }

    pub fn percentile_ms(&self, p: f64) -> f64 {
        let guard = self.inner.lock().unwrap();
        if guard.is_empty() {
            return 0.0;
        }
        let ns = guard.value_at_quantile(p) as f64;
        ns / 1_000_000.0
    }

    pub fn count(&self) -> u64 {
        self.inner.lock().unwrap().len()
    }

    /// Merge another histogram's recorded samples into this one (hdrhistogram
    /// `add`). Used to build a single run-wide histogram from the per-bot
    /// histograms so run-wide percentiles are computed from the *merged sample
    /// distribution* (mathematically correct) rather than ack-weight-averaging
    /// per-bot quantiles — the latter under-reports the tail (a 1-in-10 bot at
    /// 500ms gets averaged down toward the fast bots' 5ms, hiding the real
    /// p99/p999). All histograms here share identical bounds, so `add` never
    /// resizes; on the impossible mismatch we ignore the error rather than
    /// panic on a reporting path.
    pub fn merge(&mut self, other: &LatencyHistogram) {
        let src = other.inner.lock().unwrap();
        let mut dst = self.inner.lock().unwrap();
        let _ = dst.add(&*src);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_roughly_correct() {
        let h = LatencyHistogram::new();
        for i in 1u64..=1000 {
            h.record_ns(i * 1_000_000);
        }
        let p50 = h.percentile_ms(0.5);
        let p99 = h.percentile_ms(0.99);
        assert!((p50 - 500.0).abs() < 5.0, "p50 was {}", p50);
        assert!((p99 - 990.0).abs() < 5.0, "p99 was {}", p99);
    }

    #[test]
    fn merge_combines_sample_distributions() {
        // 9 "fast" bots each with 100 samples at 5ms, 1 "slow" bot with 100
        // samples at 500ms. Merged across all bots, the slow bot is 10% of the
        // sample mass, so the true p99/p999 sit in the slow region (~500ms),
        // NOT the ~54ms an ack-weighted average of per-bot quantiles produces.
        let mut merged = LatencyHistogram::new();
        for _ in 0..9 {
            let fast = LatencyHistogram::new();
            for _ in 0..100 {
                fast.record_ns(5_000_000);
            }
            merged.merge(&fast);
        }
        let slow = LatencyHistogram::new();
        for _ in 0..100 {
            slow.record_ns(500_000_000);
        }
        merged.merge(&slow);

        assert_eq!(merged.count(), 1000);
        let p50 = merged.percentile_ms(0.50);
        let p99 = merged.percentile_ms(0.99);
        // Bulk (90%) is at 5ms -> p50 stays fast.
        assert!((p50 - 5.0).abs() < 1.0, "p50 was {}", p50);
        // Tail (top 10%) is the 500ms bot -> p99 must reflect it, not be
        // diluted toward the fast bots.
        assert!(p99 >= 450.0, "p99 must surface the slow tail, was {}", p99);
    }
}
