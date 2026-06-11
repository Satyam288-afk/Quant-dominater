use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

use bench_core::metrics::histogram::LatencyHistogram;
use bench_core::metrics::TpsCounter;
use bench_core::telemetry::{EventKind, TelemetryEvent};
use serde::Serialize;

/// Per-(run_id) aggregate. Within a run we track per-bot percentiles so
/// downstream consumers can drill in, plus a roll-up across all bots.
struct RunState {
    bots: HashMap<String, BotState>,
    first_ts_ns: u64,
    last_ts_ns: u64,
    /// 1-second ack buckets across the whole run, for peak (max) TPS.
    tps_counter: TpsCounter,
    /// Identity hashes of every event already counted for this run. Telemetry
    /// is delivered at-least-once (Kafka auto-commit + `earliest`), so after an
    /// ingester crash the broker re-delivers events; without this set a
    /// re-delivered ack would double-count `peak_tps` and inflate the percentile
    /// sample set that feeds the live leaderboard. Deduping here makes the
    /// aggregate idempotent under re-delivery. Bounded by the run's event count
    /// (cleared when the run is finalized). The set's keys are already uniform
    /// 64-bit hashes, so its hasher is a pass-through: SipHashing them a second
    /// time was measured at ~half the dedup cost (the full fix — ahash identity
    /// + identity set — took the path from 10.6-11.0 to 29.0-29.5 Mevents/s;
    /// either half alone gives only ~1.4x).
    seen: HashSet<u64, BuildHasherDefault<IdentityHasher>>,
    /// Keyed hasher for event identities, fixed for the run's lifetime so a
    /// re-delivered event always collides with its first delivery.
    identity: ahash::RandomState,
    /// How many re-delivered duplicates were dropped (surfaced in the summary
    /// so at-least-once re-delivery is observable, not silent).
    duplicates_dropped: u64,
}

impl RunState {
    fn new() -> Self {
        Self {
            bots: HashMap::new(),
            first_ts_ns: 0,
            last_ts_ns: 0,
            // Window large enough to retain every 1s bucket of any realistic
            // benchmark run, so peak_tps() is the true per-second maximum.
            tps_counter: TpsCounter::new(4096),
            seen: HashSet::default(),
            identity: ahash::RandomState::new(),
            duplicates_dropped: 0,
        }
    }
}

/// Pass-through hasher for keys that are already uniform u64 hashes.
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

/// A stable identity for a telemetry event, hashing every field. Kafka
/// re-delivery hands back the byte-identical event, so its identity collides
/// and we drop it; two genuinely distinct events differ in at least one field
/// (`seq_no`, `recv_ts_ns`, …) so they hash apart and are both kept — including
/// the multiple partial fills of one order, which share a `client_order_id` but
/// arrive at distinct `recv_ts_ns`.
fn event_identity(rs: &ahash::RandomState, e: &TelemetryEvent) -> u64 {
    // ahash instead of the std SipHash DefaultHasher: same full-field identity,
    // measured 37ns -> 9ns per event on the realistic event shape.
    rs.hash_one((
        &e.run_id,
        &e.bot_id,
        &e.client_order_id,
        e.seq_no,
        event_kind_tag(&e.event_type),
        e.send_ts_ns,
        e.recv_ts_ns,
        e.latency_ns,
    ))
}

fn event_kind_tag(k: &EventKind) -> u8 {
    match k {
        EventKind::OrderSent => 0,
        EventKind::AckReceived => 1,
        EventKind::FillReceived => 2,
        EventKind::Timeout => 3,
        EventKind::Error => 4,
    }
}

struct BotState {
    histogram: LatencyHistogram,
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    errors: u64,
}

impl Default for BotState {
    fn default() -> Self {
        Self {
            histogram: LatencyHistogram::new(),
            orders_sent: 0,
            acks_received: 0,
            fills_received: 0,
            timeouts: 0,
            errors: 0,
        }
    }
}

pub struct Aggregator {
    runs: HashMap<String, RunState>,
    filter: Option<String>,
}

impl Aggregator {
    pub fn new(filter: Option<String>) -> Self {
        Self {
            runs: HashMap::new(),
            filter,
        }
    }

    pub fn record(&mut self, event: &TelemetryEvent) {
        if let Some(filter) = &self.filter {
            if &event.run_id != filter {
                return;
            }
        }
        let run = self
            .runs
            .entry(event.run_id.clone())
            .or_insert_with(RunState::new);
        // Idempotency gate: drop at-least-once re-deliveries so they can't
        // double-count peak_tps or inflate the percentile sample set.
        let identity = event_identity(&run.identity, event);
        if !run.seen.insert(identity) {
            run.duplicates_dropped += 1;
            return;
        }
        // Only wallclock-truthy events (Ack/Fill carry real recv_ts_ns)
        // contribute to the run window. order_sent's send_ts_ns is the
        // engine-domain timestamp baked into the order, not real time.
        if event.recv_ts_ns > 0
            && matches!(
                event.event_type,
                EventKind::AckReceived | EventKind::FillReceived
            )
        {
            let ts = event.recv_ts_ns;
            if run.first_ts_ns == 0 || ts < run.first_ts_ns {
                run.first_ts_ns = ts;
            }
            if ts > run.last_ts_ns {
                run.last_ts_ns = ts;
            }
        }
        // An ack is one completed transaction; bucket it for peak TPS.
        if event.recv_ts_ns > 0 && matches!(event.event_type, EventKind::AckReceived) {
            run.tps_counter.record(event.recv_ts_ns);
        }
        let bot = run.bots.entry(event.bot_id.clone()).or_default();
        match event.event_type {
            EventKind::OrderSent => bot.orders_sent += 1,
            EventKind::AckReceived => {
                bot.acks_received += 1;
                if event.latency_ns > 0 {
                    bot.histogram.record_ns(event.latency_ns);
                }
            }
            EventKind::FillReceived => bot.fills_received += 1,
            EventKind::Timeout => bot.timeouts += 1,
            EventKind::Error => bot.errors += 1,
        }
    }

    pub fn finalize(self) -> RunSummary {
        let mut runs = Vec::with_capacity(self.runs.len());
        for (run_id, state) in self.runs.into_iter() {
            let duration_ns = state.last_ts_ns.saturating_sub(state.first_ts_ns);
            let duration_secs = (duration_ns as f64 / 1_000_000_000.0).max(0.001);
            let peak_tps = state.tps_counter.peak_tps() as f64;
            let duplicates_dropped = state.duplicates_dropped;

            let mut totals = BotTotals::default();
            let mut bots: Vec<BotSummary> = Vec::with_capacity(state.bots.len());
            for (bot_id, b) in state.bots.into_iter() {
                let p50 = b.histogram.percentile_ms(0.50);
                let p90 = b.histogram.percentile_ms(0.90);
                let p99 = b.histogram.percentile_ms(0.99);
                let p999 = b.histogram.percentile_ms(0.999);
                totals.orders_sent += b.orders_sent;
                totals.acks_received += b.acks_received;
                totals.fills_received += b.fills_received;
                totals.timeouts += b.timeouts;
                totals.errors += b.errors;
                totals.histogram_count += b.histogram.count();
                bots.push(BotSummary {
                    bot_id,
                    orders_sent: b.orders_sent,
                    acks_received: b.acks_received,
                    fills_received: b.fills_received,
                    timeouts: b.timeouts,
                    errors: b.errors,
                    p50_ms: p50,
                    p90_ms: p90,
                    p99_ms: p99,
                    p999_ms: p999,
                });
            }
            bots.sort_by(|a, b| a.bot_id.cmp(&b.bot_id));

            // Aggregate run-wide percentiles by merging per-bot quantile
            // estimates weighted by sample count. This is approximate but
            // good enough for a horizontal-slice surface; live mode reads
            // the authoritative quantiles from Timescale.
            let (p50, p90, p99, p999) = approx_global_percentiles(&bots);

            runs.push(RunAggregate {
                run_id,
                duration_secs,
                orders_sent: totals.orders_sent,
                acks_received: totals.acks_received,
                fills_received: totals.fills_received,
                timeouts: totals.timeouts,
                errors: totals.errors,
                tps: totals.acks_received as f64 / duration_secs,
                peak_tps,
                p50_ms: p50,
                p90_ms: p90,
                p99_ms: p99,
                p999_ms: p999,
                duplicates_dropped,
                bots,
            });
        }
        runs.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        RunSummary { runs }
    }
}

#[derive(Default)]
struct BotTotals {
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    errors: u64,
    histogram_count: u64,
}

fn approx_global_percentiles(bots: &[BotSummary]) -> (f64, f64, f64, f64) {
    // Weighted average of per-bot percentile values by ack count. Cheap and
    // close enough for horizontal-slice reporting; Timescale view supplies
    // the precise number in live mode.
    let mut sums = [0.0_f64; 4];
    let mut weight = 0_u64;
    for b in bots {
        let w = b.acks_received;
        if w == 0 {
            continue;
        }
        weight += w;
        sums[0] += b.p50_ms * w as f64;
        sums[1] += b.p90_ms * w as f64;
        sums[2] += b.p99_ms * w as f64;
        sums[3] += b.p999_ms * w as f64;
    }
    if weight == 0 {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let w = weight as f64;
    (sums[0] / w, sums[1] / w, sums[2] / w, sums[3] / w)
}

#[derive(Debug, Serialize)]
pub struct RunSummary {
    pub runs: Vec<RunAggregate>,
}

#[derive(Debug, Serialize)]
pub struct RunAggregate {
    pub run_id: String,
    pub duration_secs: f64,
    pub orders_sent: u64,
    pub acks_received: u64,
    pub fills_received: u64,
    pub timeouts: u64,
    pub errors: u64,
    pub tps: f64,
    pub peak_tps: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    /// At-least-once re-deliveries dropped by the idempotency gate. 0 in the
    /// happy path; non-zero after an ingester restart re-reads committed-but-
    /// reprocessed offsets — proof the dedup is doing its job.
    pub duplicates_dropped: u64,
    pub bots: Vec<BotSummary>,
}

#[derive(Debug, Serialize)]
pub struct BotSummary {
    pub bot_id: String,
    pub orders_sent: u64,
    pub acks_received: u64,
    pub fills_received: u64,
    pub timeouts: u64,
    pub errors: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt(run: &str, bot: &str, kind: EventKind, latency_ns: u64) -> TelemetryEvent {
        TelemetryEvent {
            run_id: run.to_string(),
            bot_id: bot.to_string(),
            seq_no: 0,
            client_order_id: "x".to_string(),
            event_type: kind,
            send_ts_ns: 100,
            recv_ts_ns: 100 + latency_ns,
            latency_ns,
        }
    }

    #[test]
    fn aggregates_basic_counts() {
        let mut agg = Aggregator::new(None);
        agg.record(&evt("r1", "b1", EventKind::OrderSent, 0));
        agg.record(&evt("r1", "b1", EventKind::AckReceived, 1_000_000));
        agg.record(&evt("r1", "b1", EventKind::FillReceived, 0));
        agg.record(&evt("r1", "b2", EventKind::OrderSent, 0));
        let summary = agg.finalize();
        assert_eq!(summary.runs.len(), 1);
        let r = &summary.runs[0];
        assert_eq!(r.orders_sent, 2);
        assert_eq!(r.acks_received, 1);
        assert_eq!(r.fills_received, 1);
        assert!(r.p99_ms >= 0.5 && r.p99_ms <= 2.0);
    }

    #[test]
    fn filter_drops_other_runs() {
        let mut agg = Aggregator::new(Some("r1".into()));
        agg.record(&evt("r1", "b1", EventKind::OrderSent, 0));
        agg.record(&evt("r2", "b1", EventKind::OrderSent, 0));
        let summary = agg.finalize();
        assert_eq!(summary.runs.len(), 1);
        assert_eq!(summary.runs[0].run_id, "r1");
    }

    #[test]
    fn dedups_redelivered_events() {
        // Simulate Kafka re-delivery: the SAME ack event recorded three times
        // (the byte-identical event the broker re-delivers after a crash).
        let mut agg = Aggregator::new(None);
        let ack = evt("r1", "b1", EventKind::AckReceived, 1_000_000);
        agg.record(&ack);
        agg.record(&ack); // duplicate
        agg.record(&ack); // duplicate
        let summary = agg.finalize();
        let r = &summary.runs[0];
        // Counted exactly once despite three deliveries; the two extras are
        // reported, not silently absorbed.
        assert_eq!(r.acks_received, 1);
        assert_eq!(r.peak_tps, 1.0);
        assert_eq!(r.duplicates_dropped, 2);
    }

    #[test]
    fn keeps_distinct_partial_fills_of_one_order() {
        // Two partial fills of the same order share a client_order_id but arrive
        // at distinct recv_ts_ns — they must NOT be treated as duplicates.
        let mut agg = Aggregator::new(None);
        let mut fill_a = evt("r1", "b1", EventKind::FillReceived, 0);
        fill_a.recv_ts_ns = 1_000;
        let mut fill_b = evt("r1", "b1", EventKind::FillReceived, 0);
        fill_b.recv_ts_ns = 2_000; // a later, distinct fill of the same order
        agg.record(&fill_a);
        agg.record(&fill_b);
        let summary = agg.finalize();
        let r = &summary.runs[0];
        assert_eq!(r.fills_received, 2);
        assert_eq!(r.duplicates_dropped, 0);
    }
}
