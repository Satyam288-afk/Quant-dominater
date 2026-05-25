use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub struct TpsCounter {
    buckets: Mutex<Vec<u64>>,
    start_ns: AtomicU64,
    bucket_ns: u64,
    total: AtomicU64,
}

impl TpsCounter {
    pub fn new(window_secs: usize) -> Self {
        let n = window_secs.max(1);
        Self {
            buckets: Mutex::new(vec![0u64; n]),
            start_ns: AtomicU64::new(0),
            bucket_ns: 1_000_000_000,
            total: AtomicU64::new(0),
        }
    }

    pub fn record(&self, ts_ns: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut buckets = self.buckets.lock().unwrap();
        let start = self.start_ns.load(Ordering::Relaxed);
        if start == 0 {
            self.start_ns.store(ts_ns, Ordering::Relaxed);
            buckets[0] += 1;
            return;
        }
        let idx = ((ts_ns.saturating_sub(start)) / self.bucket_ns) as usize;
        if idx >= buckets.len() {
            let shift = idx + 1 - buckets.len();
            if shift >= buckets.len() {
                buckets.iter_mut().for_each(|b| *b = 0);
            } else {
                buckets.drain(0..shift);
                buckets.extend(std::iter::repeat(0).take(shift));
            }
            self.start_ns
                .store(start + (shift as u64) * self.bucket_ns, Ordering::Relaxed);
            let last = buckets.len() - 1;
            buckets[last] += 1;
        } else {
            buckets[idx] += 1;
        }
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn peak_tps(&self) -> u64 {
        let buckets = self.buckets.lock().unwrap();
        *buckets.iter().max().unwrap_or(&0)
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
}
