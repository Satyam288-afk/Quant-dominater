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
}
