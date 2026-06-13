use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bench_core::telemetry::{EventKind, TelemetryEvent};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::aggregator::MAX_RUNS;

/// Per-(run) cap on retained interval latency samples (R4). `run_id`/`bot_id`
/// are event-supplied, so without a bound a single hot or hostile run grows
/// `iv_latencies_ns` without limit between flushes (it's only cleared on
/// `reset_interval`, which never runs if the flush task wedges). The window is
/// reset every flush, so this only caps a pathological single interval; the
/// p50/p99 over a capped (still large) sample is unchanged for any realistic
/// rate.
const MAX_INTERVAL_LATENCIES: usize = 1_048_576;

/// Evict a run's rollup after this long with no new events (R4). `per_run` is
/// keyed by attacker-controlled `run_id`; combined with `MAX_RUNS` this bounds
/// the map to active runs only so a long-lived pod can't accumulate dead runs
/// for its whole uptime. Comfortably longer than any flush interval and any
/// realistic inter-event gap within a live run.
const RUN_IDLE_TTL: Duration = Duration::from_secs(900);

/// Periodically pushes a rolling "live metrics" snapshot per run into Redis.
/// The leaderboard UI watches `run:{id}:metrics` hash and a couple of XADD
/// streams capped via MAXLEN ~ 600 to keep the working set bounded.
pub async fn spawn(url: String, flush_ms: u64) -> Result<mpsc::Sender<Arc<TelemetryEvent>>> {
    let client = redis::Client::open(url.clone()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client)
        .await
        .with_context(|| format!("connecting redis {url}"))?;
    let (tx, mut rx) = mpsc::channel::<Arc<TelemetryEvent>>(16_384);

    let interval_secs = (flush_ms.max(250)) as f64 / 1000.0;
    tokio::spawn(async move {
        let mut per_run: HashMap<String, RunRollup> = HashMap::new();
        let mut runs_dropped: u64 = 0;
        let mut ticker = interval(Duration::from_millis(flush_ms.max(250)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(e) => {
                            // Cardinality guard (R4): `run_id` is event-supplied,
                            // so a flood of distinct run_ids would otherwise grow
                            // `per_run` for the pod's whole uptime and OOM it (the
                            // same bound the aggregator applies one layer up).
                            // Drop+log new run_ids once at the cap; existing runs
                            // keep working.
                            if per_run.len() >= MAX_RUNS && !per_run.contains_key(&e.run_id) {
                                runs_dropped += 1;
                                tracing::warn!(
                                    run_id = %e.run_id,
                                    max_runs = MAX_RUNS,
                                    runs_dropped,
                                    "redis sink run_id cardinality cap reached; dropping events for new run_id"
                                );
                                continue;
                            }
                            let r = per_run.entry(e.run_id.clone()).or_default();
                            r.observe(&e);
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    if let Err(err) = flush(&mut mgr, &mut per_run, interval_secs).await {
                        tracing::warn!(error=%err, "redis flush error");
                    }
                }
            }
        }
        let _ = flush(&mut mgr, &mut per_run, interval_secs).await;
    });
    Ok(tx)
}

struct RunRollup {
    // Cumulative since run start — drives the live `run:{id}:metrics` hash.
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    errors: u64,
    latency_sum_ns: u128,
    latency_count: u64,
    last_recv_ts_ns: u64,
    // Last values pushed to Redis, so each flush HINCRBYs only the DELTA since
    // the previous flush (R1). With >= 2 ingester pods sharing the consumer
    // group, every pod adds its own delta into the same `run:{id}:metrics`
    // counters, instead of each HSETting its partial absolute total and the
    // last writer clobbering the rest (which showed ~1/N of the true counts on
    // the live dashboard). Mirrors how the interval window already accumulates
    // and resets.
    flushed_orders_sent: u64,
    flushed_acks_received: u64,
    flushed_fills_received: u64,
    flushed_timeouts: u64,
    flushed_errors: u64,
    flushed_latency_sum_ns: u128,
    flushed_latency_count: u64,
    // Per-flush window — reset every flush so the time-series stream carries
    // *interval* rates and percentiles (so a chart shows latency degrading
    // under load, not a smoothed whole-run average).
    iv_latencies_ns: Vec<u64>,
    iv_acks: u64,
    iv_errors: u64,
    iv_timeouts: u64,
    // Idle-eviction bookkeeping (R4): when the last event for this run arrived,
    // so a long-lived pod can drop rollups for runs that have gone quiet.
    last_event_at: Instant,
}

impl Default for RunRollup {
    fn default() -> Self {
        Self {
            orders_sent: 0,
            acks_received: 0,
            fills_received: 0,
            timeouts: 0,
            errors: 0,
            latency_sum_ns: 0,
            latency_count: 0,
            last_recv_ts_ns: 0,
            flushed_orders_sent: 0,
            flushed_acks_received: 0,
            flushed_fills_received: 0,
            flushed_timeouts: 0,
            flushed_errors: 0,
            flushed_latency_sum_ns: 0,
            flushed_latency_count: 0,
            iv_latencies_ns: Vec::new(),
            iv_acks: 0,
            iv_errors: 0,
            iv_timeouts: 0,
            last_event_at: Instant::now(),
        }
    }
}

impl RunRollup {
    fn observe(&mut self, e: &TelemetryEvent) {
        self.last_event_at = Instant::now();
        match e.event_type {
            EventKind::OrderSent => self.orders_sent += 1,
            EventKind::AckReceived => {
                self.acks_received += 1;
                self.iv_acks += 1;
                if e.latency_ns > 0 {
                    self.latency_sum_ns += e.latency_ns as u128;
                    self.latency_count += 1;
                    // Bound the interval sample (R4): drop the oldest once the
                    // window is full so an event-supplied flood can't grow it
                    // unbounded if the flush task ever stalls.
                    if self.iv_latencies_ns.len() >= MAX_INTERVAL_LATENCIES {
                        self.iv_latencies_ns.remove(0);
                    }
                    self.iv_latencies_ns.push(e.latency_ns);
                }
            }
            EventKind::FillReceived => self.fills_received += 1,
            EventKind::Timeout => {
                self.timeouts += 1;
                self.iv_timeouts += 1;
            }
            EventKind::Error => {
                self.errors += 1;
                self.iv_errors += 1;
            }
        }
        if e.recv_ts_ns > self.last_recv_ts_ns {
            self.last_recv_ts_ns = e.recv_ts_ns;
        }
    }

    /// (p50, p99) latency in ms over just this flush window. Sorts the window's
    /// samples in place; cheap since a window holds at most one flush of acks.
    fn interval_percentiles_ms(&mut self) -> (f64, f64) {
        if self.iv_latencies_ns.is_empty() {
            return (0.0, 0.0);
        }
        self.iv_latencies_ns.sort_unstable();
        let n = self.iv_latencies_ns.len();
        let pick = |p: f64| {
            let idx = ((p * n as f64).ceil() as usize)
                .saturating_sub(1)
                .min(n - 1);
            self.iv_latencies_ns[idx] as f64 / 1_000_000.0
        };
        (pick(0.50), pick(0.99))
    }

    fn reset_interval(&mut self) {
        self.iv_latencies_ns.clear();
        self.iv_acks = 0;
        self.iv_errors = 0;
        self.iv_timeouts = 0;
    }

    /// True once a flush has pushed the current cumulative state — i.e. no new
    /// events landed since the last flush, so its delta is already 0. Used by
    /// idle eviction so a quiesced run can be dropped without losing counts.
    fn is_fully_flushed(&self) -> bool {
        self.flushed_orders_sent == self.orders_sent
            && self.flushed_acks_received == self.acks_received
            && self.flushed_fills_received == self.fills_received
            && self.flushed_timeouts == self.timeouts
            && self.flushed_errors == self.errors
            && self.flushed_latency_count == self.latency_count
    }
}

async fn flush(
    mgr: &mut ConnectionManager,
    per_run: &mut HashMap<String, RunRollup>,
    interval_secs: f64,
) -> Result<()> {
    for (run_id, r) in per_run.iter_mut() {
        let key = format!("run:{}:metrics", run_id);
        // Per-interval rate + percentiles for the time-series point.
        let (iv_p50, iv_p99) = r.interval_percentiles_ms();
        let iv_tps = if interval_secs > 0.0 {
            r.iv_acks as f64 / interval_secs
        } else {
            0.0
        };
        let iv_errors = r.iv_errors;
        let iv_timeouts = r.iv_timeouts;
        let iv_acks = r.iv_acks;

        // Deltas since the previous flush (R1). HINCRBY of the delta — rather
        // than HSET of the absolute total — means each ingester pod adds only
        // its own increment into the shared `run:{id}:metrics` counters, so two
        // pods in the consumer group sum to the true total instead of the last
        // writer clobbering the others' partials (the ~1/N undercount).
        let d_orders = r.orders_sent - r.flushed_orders_sent;
        let d_acks = r.acks_received - r.flushed_acks_received;
        let d_fills = r.fills_received - r.flushed_fills_received;
        let d_timeouts = r.timeouts - r.flushed_timeouts;
        let d_errors = r.errors - r.flushed_errors;
        let d_lat_sum = r.latency_sum_ns - r.flushed_latency_sum_ns;
        let d_lat_count = r.latency_count - r.flushed_latency_count;

        let mut pipe = redis::pipe();
        pipe.cmd("HINCRBY").arg(&key).arg("orders_sent").arg(d_orders as i64).ignore();
        pipe.cmd("HINCRBY").arg(&key).arg("acks_received").arg(d_acks as i64).ignore();
        pipe.cmd("HINCRBY").arg(&key).arg("fills_received").arg(d_fills as i64).ignore();
        pipe.cmd("HINCRBY").arg(&key).arg("timeouts").arg(d_timeouts as i64).ignore();
        pipe.cmd("HINCRBY").arg(&key).arg("errors").arg(d_errors as i64).ignore();
        // latency_sum_ns / latency_count are also merged via HINCRBY so the
        // run-wide average can be derived from the cross-pod totals; HSETting a
        // per-pod average would clobber the same way the counts did.
        pipe.cmd("HINCRBY").arg(&key).arg("latency_sum_ns").arg(d_lat_sum as i64).ignore();
        pipe.cmd("HINCRBY").arg(&key).arg("latency_count").arg(d_lat_count as i64).ignore();
        // last_recv_ts_ns is a wall-clock high-water mark, not a count: keep it
        // a (monotone) HSET. A racing pod can briefly clobber it with a slightly
        // earlier stamp, but it self-heals on the next flush and never undercounts.
        pipe.cmd("HSET").arg(&key).arg("last_recv_ts_ns").arg(r.last_recv_ts_ns).ignore();
        pipe.cmd("EXPIRE").arg(&key).arg(86_400).ignore();
        let hash_ok = match pipe.query_async::<()>(mgr).await {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(error=%err, "redis hash flush error");
                false
            }
        };

        // No materialized `avg_latency_ms` write here. The cross-pod cumulative
        // `latency_sum_ns` / `latency_count` are already HINCRBY-merged above, so
        // the leaderboard read path derives avg = latency_sum_ns / latency_count
        // (ns -> ms) from the same HGETALL it already does. Computing it on read
        // avoids the extra HMGET+HSET round-trips per run per flush and the race
        // where a pod's published avg reflected a snapshot taken before another
        // pod's HINCRBY (inconsistent with the published sum/count).

        // Advance the flushed high-water marks only after a successful push, so
        // a transient Redis error replays the delta on the next flush instead
        // of permanently dropping it.
        if hash_ok {
            r.flushed_orders_sent = r.orders_sent;
            r.flushed_acks_received = r.acks_received;
            r.flushed_fills_received = r.fills_received;
            r.flushed_timeouts = r.timeouts;
            r.flushed_errors = r.errors;
            r.flushed_latency_sum_ns = r.latency_sum_ns;
            r.flushed_latency_count = r.latency_count;
        }

        // Time-series: one bucket per absolute wall-clock second, carrying the
        // *interval* counts/latency. HINCRBY into a per-(run, second) hash so
        // every pod ADDS into the same bucket (R1) — the old per-pod XADD stream
        // appended N independent points per second, one per pod, so a chart
        // built from it showed each pod's partial rate, not the fleet total.
        // (No leaderboard read path consumes this key; the UI latency chart is
        // computed from Timescale, so changing the storage shape is safe.)
        let bucket_second = r.last_recv_ts_ns / 1_000_000_000;
        let ts_key = format!("run:{}:timeseries:{}", run_id, bucket_second);
        let mut ts_pipe = redis::pipe();
        ts_pipe.cmd("HINCRBY").arg(&ts_key).arg("acks").arg(iv_acks as i64).ignore();
        ts_pipe.cmd("HINCRBY").arg(&ts_key).arg("errors").arg(iv_errors as i64).ignore();
        ts_pipe.cmd("HINCRBY").arg(&ts_key).arg("timeouts").arg(iv_timeouts as i64).ignore();
        // tps within the bucket is derivable from `acks`; p50/p99 are per-pod
        // interval estimates, kept as a representative (last-writer) sample for
        // the chart rather than merged (percentiles don't add).
        ts_pipe.cmd("HSET").arg(&ts_key)
            .arg("tps").arg(iv_tps)
            .arg("p50_ms").arg(iv_p50)
            .arg("p99_ms").arg(iv_p99)
            .ignore();
        ts_pipe.cmd("EXPIRE").arg(&ts_key).arg(86_400).ignore();
        if let Err(err) = ts_pipe.query_async::<()>(mgr).await {
            tracing::warn!(error=%err, "redis timeseries flush error");
        }

        r.reset_interval();
    }

    // Idle eviction (R4): drop rollups for runs that have gone quiet for longer
    // than RUN_IDLE_TTL and whose counts are already fully flushed, so a
    // long-lived pod's `per_run` tracks only active runs. A fully-flushed idle
    // run has a 0 delta, so dropping it loses nothing — the shared Redis hash
    // already holds its totals.
    let now = Instant::now();
    per_run.retain(|_run_id, r| {
        let idle = now.duration_since(r.last_event_at) >= RUN_IDLE_TTL;
        !(idle && r.is_fully_flushed())
    });
    Ok(())
}

/// Helper used at finalize from main: write the per-run final score into
/// the global leaderboard ZSET. Keeps the leaderboard-api hot path simple.
#[allow(dead_code)]
pub async fn write_leaderboard(
    url: &str,
    run_id: &str,
    team_id: &str,
    final_score: f64,
) -> Result<()> {
    let client = redis::Client::open(url.to_string()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client).await?;
    let _: () = mgr.zadd("leaderboard:global", team_id, final_score).await?;
    let team_best_key = format!("team:{}:best_score", team_id);
    let prev: Option<f64> = mgr.get(&team_best_key).await.unwrap_or(None);
    if prev.map(|p| final_score > p).unwrap_or(true) {
        let _: () = mgr.set(&team_best_key, final_score).await?;
    }
    let _: () = mgr
        .set(&format!("submission:{}:last_run", team_id), run_id)
        .await?;
    Ok(())
}
