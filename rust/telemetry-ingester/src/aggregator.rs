use std::collections::HashMap;

use bench_core::metrics::histogram::LatencyHistogram;
use bench_core::telemetry::{EventKind, TelemetryEvent};
use serde::Serialize;

/// Per-(run_id) aggregate. Within a run we track per-bot percentiles so
/// downstream consumers can drill in, plus a roll-up across all bots.
#[derive(Default)]
struct RunState {
    bots: HashMap<String, BotState>,
    first_ts_ns: u64,
    last_ts_ns: u64,
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
        let run = self.runs.entry(event.run_id.clone()).or_default();
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
                p50_ms: p50,
                p90_ms: p90,
                p99_ms: p99,
                p999_ms: p999,
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
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
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
}
