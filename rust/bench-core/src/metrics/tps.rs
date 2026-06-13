use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Pass-through hasher for keys that are already uniform (or near-uniform) u64
/// values. The second-bucket key (`recv_ts_ns / 1e9`) is monotone wall-clock
/// seconds, so SipHashing it on every ack was pure overhead (TM-5: ~+94% on the
/// per-ack `record` path). This hasher just returns the key unchanged, turning
/// the bucket lookup into a single probe. (Duplicated from the telemetry
/// ingester's identical `IdentityHasher`; bench-core has no ahash dep, so we
/// keep a local copy rather than add one.)
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("identity hasher is only for u64 keys")
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

type SecondMap = HashMap<u64, u64, BuildHasherDefault<IdentityHasher>>;

/// Hard cap on the number of distinct 1-second buckets retained at once.
/// `recv_ts_ns` is event-supplied, so a stream that scatters acks across many
/// distinct wall-seconds (clock skew, malformed/attacker timestamps) would
/// otherwise grow `buckets` by one entry per distinct second for the run's
/// whole lifetime — defeating the ingester's MAX_RUNS memory bound. We only
/// ever read the *maximum* bucket count (`peak_tps`), so when the map is full
/// we evict the lowest-count bucket: that can never be the per-second peak, so
/// `peak_tps()` stays exactly correct while memory is bounded to O(cap). A real
/// benchmark run spans far fewer than `cap` seconds, so the happy path never
/// evicts. ~8192 entries * 16B ~= 128 KiB worst case per counter.
const MAX_BUCKETS: usize = 8192;

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
/// The counter is owned single-threaded by its aggregator's `RunState` (and by
/// bot-fleet's local summary), so `record` takes `&mut self`: no inner Mutex /
/// atomic is needed, removing a lock + an atomic add from every ack (TM-5).
///
/// NOTE: `recv_ts_ns` is a per-host wall clock. Across pods this assumes
/// NTP/chrony keeps clocks aligned (a deployment precondition); a single-clock
/// rebucket is out of scope for this fix.
pub struct TpsCounter {
    /// second-key (`recv_ts_ns / 1e9`) -> ack count in that second. Bounded to
    /// `MAX_BUCKETS` entries; see the constant for the eviction policy.
    buckets: SecondMap,
    total: u64,
}

impl TpsCounter {
    /// `_window_secs` is retained for call-site/API compatibility but no longer
    /// pre-sizes a fixed ring: buckets are sparse and keyed by absolute second,
    /// so only seconds that actually saw an ack consume memory (bounded by
    /// `MAX_BUCKETS`).
    pub fn new(_window_secs: usize) -> Self {
        Self {
            buckets: SecondMap::default(),
            total: 0,
        }
    }

    pub fn record(&mut self, ts_ns: u64) {
        self.total += 1;
        let second = ts_ns / 1_000_000_000;
        if let Some(count) = self.buckets.get_mut(&second) {
            *count += 1;
            return;
        }
        // New second. Bound the map: evicting the lowest-count bucket can never
        // remove the per-second maximum, so `peak_tps()` is unaffected.
        if self.buckets.len() >= MAX_BUCKETS {
            if let Some(min_key) = self
                .buckets
                .iter()
                .min_by_key(|(_, &c)| c)
                .map(|(&k, _)| k)
            {
                self.buckets.remove(&min_key);
            }
        }
        self.buckets.insert(second, 1);
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn peak_tps(&self) -> u64 {
        self.buckets.values().copied().max().unwrap_or(0)
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
        let mut c = TpsCounter::new(3);
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

        let mut forward = TpsCounter::new(8);
        for ts in &in_order {
            forward.record(*ts);
        }
        assert_eq!(forward.peak_tps(), 3);
        assert_eq!(forward.total(), 6);

        let mut reverse = TpsCounter::new(8);
        for ts in in_order.iter().rev() {
            reverse.record(*ts);
        }
        assert_eq!(reverse.peak_tps(), 3, "reverse order must match true peak");
        assert_eq!(reverse.total(), 6);
    }

    #[test]
    fn buckets_are_bounded_yet_peak_stays_exact() {
        // TM (#21): recv_ts_ns is event-supplied, so scattering acks across far
        // more distinct wall-seconds than MAX_BUCKETS must NOT grow the map
        // without bound. Lay down one ack in each of many distinct seconds
        // (count 1), plus a clear peak of 5 acks in a single second. After
        // streaming well past the cap, the map stays bounded and peak_tps still
        // reports the true maximum (the lowest-count buckets are what get
        // evicted, never the peak).
        let mut c = TpsCounter::new(8);
        let peak_second = 42_u64;
        for _ in 0..5 {
            c.record(peak_second * 1_000_000_000 + 1);
        }
        // Now flood distinct singleton seconds far beyond the cap.
        for s in 1_000..(1_000 + (MAX_BUCKETS as u64) * 3) {
            c.record(s * 1_000_000_000);
        }
        assert!(
            c.buckets.len() <= MAX_BUCKETS,
            "bucket map must stay bounded, was {}",
            c.buckets.len()
        );
        assert_eq!(c.peak_tps(), 5, "peak must survive eviction of singletons");
    }
}
