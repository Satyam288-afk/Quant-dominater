use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

use bench_core::metrics::histogram::LatencyHistogram;
use bench_core::metrics::TpsCounter;
use bench_core::telemetry::{EventKind, TelemetryEvent};
use serde::Serialize;

/// Cap on the active dedup generation. Kafka auto-commit (`enable.auto.commit`
/// plus the ~5s commit interval) means a restart only re-delivers the small
/// window of events processed since the last commit; the two-generation
/// rotation keeps at least that many recent identities live, so a re-delivery
/// is still recognised. Sized generously (a few seconds of peak ingest) while
/// bounding memory to O(window) instead of O(uptime) — the previous unbounded
/// set grew for the pod's whole lifetime and OOM'd against the 1Gi limit on a
/// persistent consumer.
const DEDUP_WINDOW: usize = 262_144;

/// Hard cap on the number of distinct `run_id`s held live at once. `run_id` is
/// attacker-controlled (it arrives on every telemetry event), and each new run
/// allocates a `RunState` (~32 KiB once its dedup/tps buffers warm up). Without
/// a cap, a flood of distinct run_ids grows `runs` for the pod's whole uptime
/// and OOMs against the 1Gi limit the dedup window above is already sized for
/// (50k run_ids alone is ~1.5 GiB). Once the cap is hit we drop+log new
/// run_ids instead of allocating; existing runs keep working unchanged. Each
/// admitted RunState eagerly allocates ~256 KiB (a 4096-slot TpsCounter plus
/// per-bot HDR histograms), so the cap also sets the absolute ceiling: 3000
/// runs ~= 768 MiB, comfortably under the 1Gi pod budget. A real benchmark
/// fans out to a handful of runs, far below this. Also reused by the redis sink
/// to bound its own per-run rollup map (the same attacker-controlled-run_id OOM
/// vector one layer down).
pub const MAX_RUNS: usize = 3000;

/// Per-(run_id) cap on distinct `bot_id`s. `bot_id` is likewise event-supplied,
/// so an unbounded `bots` map inside a single run is the same OOM vector at one
/// level down. A real run has a bounded bot fleet; beyond this we drop+log new
/// bots rather than allocating a `BotState` per attacker-chosen id.
const MAX_BOTS_PER_RUN: usize = 8192;

/// Per-(run_id) aggregate. Within a run we track per-bot percentiles so
/// downstream consumers can drill in, plus a roll-up across all bots.
struct RunState {
    bots: HashMap<Arc<str>, BotState>,
    first_ts_ns: u64,
    last_ts_ns: u64,
    /// 1-second ack buckets across the whole run, for peak (max) TPS.
    tps_counter: TpsCounter,
    /// Identity hashes of recently-counted events for this run. Telemetry is
    /// delivered at-least-once (Kafka auto-commit + `earliest`), so after an
    /// ingester crash the broker re-delivers the uncommitted window; without
    /// this a re-delivered ack would double-count `peak_tps` and inflate the
    /// percentile sample set that feeds the live leaderboard. Deduping here
    /// makes the aggregate idempotent under re-delivery. Bounded to a rolling
    /// window (see `WindowedDedup`) so a persistent consumer can't grow it
    /// without limit. The keys are already uniform 64-bit hashes, so the set's
    /// hasher is a pass-through: SipHashing them a second time was measured at
    /// ~half the dedup cost (the full fix — ahash identity + identity set —
    /// took the path from 10.6-11.0 to 29.0-29.5 Mevents/s; either half alone
    /// gives only ~1.4x).
    seen: WindowedDedup,
    /// Keyed hasher for event identities, fixed for the run's lifetime so a
    /// re-delivered event always collides with its first delivery.
    identity: ahash::RandomState,
    /// How many re-delivered duplicates were dropped (surfaced in the summary
    /// so at-least-once re-delivery is observable, not silent).
    duplicates_dropped: u64,
    /// Events dropped because this run's `bots` map already held
    /// `MAX_BOTS_PER_RUN` distinct bot_ids (an event-supplied-cardinality OOM
    /// guard). 0 in the happy path.
    bots_dropped: u64,
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
            seen: WindowedDedup::new(DEDUP_WINDOW),
            identity: ahash::RandomState::new(),
            duplicates_dropped: 0,
            bots_dropped: 0,
        }
    }
}

type IdentitySet = HashSet<u64, BuildHasherDefault<IdentityHasher>>;

/// Bounded, two-generation dedup over event identity hashes. Reads consult both
/// the `active` and the previous `aged` generation; writes only ever land in
/// `active`. When `active` fills to `cap`, it rotates into `aged` (dropping the
/// older `aged`) and a fresh `active` starts. This keeps memory at O(cap) for
/// the pod's whole uptime while still recognising any identity seen within the
/// last one-to-two generations — comfortably covering Kafka's small
/// auto-commit replay window, so re-delivered events are not double-counted.
struct WindowedDedup {
    active: IdentitySet,
    aged: IdentitySet,
    cap: usize,
}

impl WindowedDedup {
    fn new(cap: usize) -> Self {
        Self {
            active: IdentitySet::default(),
            aged: IdentitySet::default(),
            cap: cap.max(1),
        }
    }

    /// Returns true if `id` was newly inserted, false if it was already present
    /// in either generation (a re-delivery within the window).
    fn insert(&mut self, id: u64) -> bool {
        if self.active.contains(&id) || self.aged.contains(&id) {
            return false;
        }
        if self.active.len() >= self.cap {
            // Rotate: the current active becomes the aged generation (the old
            // aged is dropped), bounding total retained identities to ~2*cap.
            std::mem::swap(&mut self.active, &mut self.aged);
            self.active.clear();
        }
        self.active.insert(id);
        true
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

/// String interner mapping run_id/bot_id text to a shared `Arc<str>`. A hit is
/// a clone of the Arc (one atomic increment) instead of a heap allocation +
/// byte copy, which is the per-event cost the old `event.run_id.clone()` /
/// `event.bot_id.clone()` map keys paid on every single event.
#[derive(Default)]
struct Interner {
    table: HashMap<Box<str>, Arc<str>>,
}

impl Interner {
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(existing) = self.table.get(s) {
            return existing.clone();
        }
        let arc: Arc<str> = Arc::from(s);
        self.table.insert(Box::from(s), arc.clone());
        arc
    }
}

pub struct Aggregator {
    runs: HashMap<Arc<str>, RunState>,
    filter: Option<String>,
    /// Interns run_id/bot_id strings into shared `Arc<str>` so the per-event
    /// map keys are a refcount bump, not a fresh String allocation+copy on
    /// every event. On a steady stream the run/bot cardinality is tiny relative
    /// to event count, so the interner stays small and the clone is amortised
    /// away.
    interner: Interner,
    /// Events dropped because `runs` was already at `MAX_RUNS` distinct run_ids
    /// (the attacker-controlled-cardinality OOM guard). 0 in the happy path.
    runs_dropped: u64,
}

impl Aggregator {
    pub fn new(filter: Option<String>) -> Self {
        Self {
            runs: HashMap::new(),
            filter,
            interner: Interner::default(),
            runs_dropped: 0,
        }
    }

    pub fn record(&mut self, event: &TelemetryEvent) {
        if let Some(filter) = &self.filter {
            if &event.run_id != filter {
                return;
            }
        }
        // Cardinality guard: `run_id` is attacker-controlled, so a flood of
        // distinct run_ids would otherwise grow `runs` (and the interner)
        // without bound and OOM the pod. Check the cap BEFORE interning so a
        // dropped run_id allocates nothing — only run_ids we actually admit
        // ever touch the interner or the map. `contains_key` accepts a `&str`
        // because `Arc<str>: Borrow<str>`, so the lookup borrows the event's
        // existing string instead of allocating a key.
        if self.runs.len() >= MAX_RUNS && !self.runs.contains_key(event.run_id.as_str()) {
            self.runs_dropped += 1;
            tracing::warn!(
                run_id = %event.run_id,
                max_runs = MAX_RUNS,
                runs_dropped = self.runs_dropped,
                "run_id cardinality cap reached; dropping events for new run_id (possible run_id flood)"
            );
            return;
        }
        let run_key = self.interner.intern(&event.run_id);
        let run = self
            .runs
            .entry(run_key)
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
        // Same cardinality guard one level down: `bot_id` is event-supplied, so
        // cap distinct bots per run. Check before interning so a dropped bot_id
        // allocates nothing (`contains_key` borrows the event's `&str`).
        if run.bots.len() >= MAX_BOTS_PER_RUN && !run.bots.contains_key(event.bot_id.as_str()) {
            run.bots_dropped += 1;
            tracing::warn!(
                run_id = %event.run_id,
                bot_id = %event.bot_id,
                max_bots = MAX_BOTS_PER_RUN,
                bots_dropped = run.bots_dropped,
                "bot_id cardinality cap reached for run; dropping events for new bot_id"
            );
            return;
        }
        let bot_key = self.interner.intern(&event.bot_id);
        let bot = run.bots.entry(bot_key).or_default();
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
        let runs_dropped = self.runs_dropped;
        let mut runs = Vec::with_capacity(self.runs.len());
        for (run_id, state) in self.runs.into_iter() {
            let duration_ns = state.last_ts_ns.saturating_sub(state.first_ts_ns);
            let duration_secs = duration_ns as f64 / 1_000_000_000.0;
            let peak_tps = state.tps_counter.peak_tps() as f64;
            let duplicates_dropped = state.duplicates_dropped;
            let bots_dropped = state.bots_dropped;

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
                    bot_id: bot_id.to_string(),
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

            // Canonical average TPS (TM-4/TM-5/R5): acks over the OBSERVED span
            // (last_ack - first_ack), which is the same definition the bot-fleet
            // summary now uses. With fewer than 2 acks there is no span to
            // measure, and a sub-second span makes the rate explode (the old
            // `.max(0.001)` floor turned a single ack into 1000 TPS). In both
            // degenerate cases we floor the divisor at 1 second so the figure is
            // a rate over the run window, not a pathological 1 ms-window spike.
            let tps_divisor = if totals.acks_received >= 2 {
                duration_secs.max(1.0)
            } else {
                1.0
            };

            runs.push(RunAggregate {
                run_id: run_id.to_string(),
                duration_secs,
                orders_sent: totals.orders_sent,
                acks_received: totals.acks_received,
                fills_received: totals.fills_received,
                timeouts: totals.timeouts,
                errors: totals.errors,
                tps: totals.acks_received as f64 / tps_divisor,
                peak_tps,
                p50_ms: p50,
                p90_ms: p90,
                p99_ms: p99,
                p999_ms: p999,
                duplicates_dropped,
                bots_dropped,
                bots,
            });
        }
        runs.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        RunSummary {
            runs,
            runs_dropped,
        }
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
    /// Events dropped because `runs` was already at `MAX_RUNS` distinct run_ids
    /// (the attacker-controlled-cardinality OOM guard). 0 in the happy path;
    /// surfaced so the cap is observable rather than only log-warned.
    pub runs_dropped: u64,
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
    /// Events dropped because this run's `bots` map was already at
    /// `MAX_BOTS_PER_RUN` distinct bot_ids (the per-run cardinality OOM guard).
    /// 0 in the happy path; surfaced so the cap is observable, not only logged.
    pub bots_dropped: u64,
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

    #[test]
    fn windowed_dedup_recognizes_within_window_and_bounds_memory() {
        // cap=4 -> the active generation holds up to 4 ids, with one aged
        // generation behind it, so anything seen within the last ~cap..2*cap
        // inserts is still recognised. Memory never exceeds 2*cap regardless of
        // how many distinct ids stream through.
        let mut dedup = WindowedDedup::new(4);
        assert!(dedup.insert(1)); // new
        assert!(!dedup.insert(1)); // immediate re-delivery -> duplicate
        for id in 2..=4 {
            assert!(dedup.insert(id));
        }
        // active is now full {1,2,3,4}; the next insert rotates it into `aged`
        // and starts a fresh active. id 1 is still in `aged`, so still a dup.
        assert!(dedup.insert(5)); // triggers rotation, lands in fresh active
        assert!(!dedup.insert(1)); // recognised via the aged generation
        assert!(!dedup.insert(5)); // recognised via the active generation
        // Push enough distinct ids to rotate twice; the original window ages
        // out and total retained ids stay bounded by ~2*cap.
        for id in 100..200 {
            dedup.insert(id);
        }
        assert!(dedup.active.len() <= 4);
        assert!(dedup.aged.len() <= 4);
        // id 1 has now aged past both live generations: treated as new again.
        // This is the bounded-window trade-off — correct because Kafka only
        // re-delivers the small uncommitted window, never the whole run.
        assert!(dedup.insert(1));
    }

    #[test]
    fn dedups_across_many_distinct_events_within_window() {
        // End-to-end: interleave a re-delivered ack with a stream of distinct
        // events. As long as the re-delivery falls inside the dedup window
        // (it does here — far fewer than DEDUP_WINDOW events) it is dropped,
        // so peak_tps and ack counts stay idempotent.
        let mut agg = Aggregator::new(None);
        let ack = evt("r1", "b1", EventKind::AckReceived, 1_000_000);
        agg.record(&ack);
        for i in 0..1_000u64 {
            let mut fill = evt("r1", "b1", EventKind::FillReceived, 0);
            fill.recv_ts_ns = 10_000 + i; // each distinct
            agg.record(&fill);
        }
        agg.record(&ack); // re-delivery still within the window
        let summary = agg.finalize();
        let r = &summary.runs[0];
        assert_eq!(r.acks_received, 1);
        assert_eq!(r.fills_received, 1_000);
        assert_eq!(r.duplicates_dropped, 1);
    }

    #[test]
    fn caps_distinct_run_ids_at_max_runs() {
        // run_id is attacker-controlled: a flood of distinct run_ids must not
        // grow the map without bound. Fill exactly to the cap, then prove
        // further *new* run_ids are dropped (no RunState allocated) while the
        // already-admitted runs keep accepting events.
        let mut agg = Aggregator::new(None);
        for i in 0..MAX_RUNS {
            agg.record(&evt(&format!("run-{i}"), "b1", EventKind::OrderSent, 0));
        }
        assert_eq!(agg.runs.len(), MAX_RUNS);
        assert_eq!(agg.runs_dropped, 0);

        // New run_ids beyond the cap are dropped: the map does not grow and the
        // counter increments. No RunState is allocated for them.
        for i in 0..50 {
            agg.record(&evt(&format!("overflow-{i}"), "b1", EventKind::OrderSent, 0));
        }
        assert_eq!(agg.runs.len(), MAX_RUNS, "map must not grow past the cap");
        assert_eq!(agg.runs_dropped, 50);
        assert!(
            !agg.runs.contains_key("overflow-0"),
            "dropped run_id must not have been inserted"
        );

        // An already-admitted run still accepts events after the cap is hit
        // (existing runs keep working).
        agg.record(&evt("run-0", "b1", EventKind::AckReceived, 1_000_000));
        assert_eq!(agg.runs.len(), MAX_RUNS);
        assert_eq!(agg.runs_dropped, 50, "updating an existing run is not a drop");
        let summary = agg.finalize();
        // The drop count is surfaced in the summary (TM-3), not only log-warned.
        assert_eq!(summary.runs_dropped, 50);
        let r = summary
            .runs
            .iter()
            .find(|r| r.run_id == "run-0")
            .expect("admitted run survives");
        assert_eq!(r.acks_received, 1);
    }

    #[test]
    fn caps_distinct_bots_per_run() {
        // bot_id is event-supplied too: a single run flooding distinct bot_ids
        // must not grow its per-run bots map without bound.
        let mut agg = Aggregator::new(None);
        for i in 0..MAX_BOTS_PER_RUN {
            agg.record(&evt("r1", &format!("bot-{i}"), EventKind::OrderSent, 0));
        }
        let run = agg.runs.get("r1").expect("run admitted");
        assert_eq!(run.bots.len(), MAX_BOTS_PER_RUN);
        assert_eq!(run.bots_dropped, 0);

        for i in 0..25 {
            agg.record(&evt("r1", &format!("overflow-bot-{i}"), EventKind::OrderSent, 0));
        }
        let run = agg.runs.get("r1").expect("run admitted");
        assert_eq!(run.bots.len(), MAX_BOTS_PER_RUN, "bots map must not grow past the cap");
        assert_eq!(run.bots_dropped, 25);
        assert!(!run.bots.contains_key("overflow-bot-0"));

        // An already-admitted bot still accepts events.
        agg.record(&evt("r1", "bot-0", EventKind::AckReceived, 1_000_000));
        let run = agg.runs.get("r1").expect("run admitted");
        assert_eq!(run.bots_dropped, 25);
        assert_eq!(run.bots.len(), MAX_BOTS_PER_RUN);

        // The per-run drop count is surfaced in the summary (TM-3), not only
        // log-warned.
        let summary = agg.finalize();
        let r = summary
            .runs
            .iter()
            .find(|r| r.run_id == "r1")
            .expect("run survives");
        assert_eq!(r.bots_dropped, 25);
    }
}
