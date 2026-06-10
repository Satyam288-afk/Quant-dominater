use std::collections::HashMap;

use anyhow::{Context, Result};
use bench_core::telemetry::{EventKind, TelemetryEvent};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};

/// Periodically pushes a rolling "live metrics" snapshot per run into Redis.
/// The leaderboard UI watches `run:{id}:metrics` hash and a couple of XADD
/// streams capped via MAXLEN ~ 600 to keep the working set bounded.
pub async fn spawn(url: String, flush_ms: u64) -> Result<mpsc::Sender<TelemetryEvent>> {
    let client = redis::Client::open(url.clone()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client)
        .await
        .with_context(|| format!("connecting redis {url}"))?;
    let (tx, mut rx) = mpsc::channel::<TelemetryEvent>(16_384);

    let interval_secs = (flush_ms.max(250)) as f64 / 1000.0;
    tokio::spawn(async move {
        let mut per_run: HashMap<String, RunRollup> = HashMap::new();
        let mut ticker = interval(Duration::from_millis(flush_ms.max(250)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(e) => {
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

#[derive(Default)]
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
    // Per-flush window — reset every flush so the time-series stream carries
    // *interval* rates and percentiles (so a chart shows latency degrading
    // under load, not a smoothed whole-run average).
    iv_latencies_ns: Vec<u64>,
    iv_acks: u64,
    iv_errors: u64,
    iv_timeouts: u64,
}

impl RunRollup {
    fn observe(&mut self, e: &TelemetryEvent) {
        match e.event_type {
            EventKind::OrderSent => self.orders_sent += 1,
            EventKind::AckReceived => {
                self.acks_received += 1;
                self.iv_acks += 1;
                if e.latency_ns > 0 {
                    self.latency_sum_ns += e.latency_ns as u128;
                    self.latency_count += 1;
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

    fn avg_latency_ms(&self) -> f64 {
        if self.latency_count == 0 {
            0.0
        } else {
            (self.latency_sum_ns / self.latency_count as u128) as f64 / 1_000_000.0
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

        let mut pipe = redis::pipe();
        pipe.cmd("HSET")
            .arg(&key)
            .arg("orders_sent")
            .arg(r.orders_sent)
            .arg("acks_received")
            .arg(r.acks_received)
            .arg("fills_received")
            .arg(r.fills_received)
            .arg("timeouts")
            .arg(r.timeouts)
            .arg("errors")
            .arg(r.errors)
            .arg("avg_latency_ms")
            .arg(r.avg_latency_ms())
            .arg("last_recv_ts_ns")
            .arg(r.last_recv_ts_ns)
            .ignore();
        pipe.cmd("EXPIRE").arg(&key).arg(86_400).ignore();
        if let Err(err) = pipe.query_async::<()>(mgr).await {
            tracing::warn!(error=%err, "redis hash flush error");
        }

        // Time-series: one point per flush window carrying the *interval* TPS
        // and p50/p99 latency, so a chart shows throughput and latency moving
        // (and degrading) over the run — not just a final average. Issued as a
        // standalone command (not folded into the all-`ignore()` pipe above,
        // which the async ConnectionManager does not reliably round-trip).
        let stream_key = format!("run:{}:timeseries:tps", run_id);
        let xadd: redis::RedisResult<String> = redis::cmd("XADD")
            .arg(&stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(600)
            .arg("*")
            .arg("tps")
            .arg(iv_tps)
            .arg("p50_ms")
            .arg(iv_p50)
            .arg("p99_ms")
            .arg(iv_p99)
            .arg("errors")
            .arg(iv_errors)
            .arg("timeouts")
            .arg(iv_timeouts)
            .arg("acks_total")
            .arg(r.acks_received)
            .query_async(mgr)
            .await;
        if let Err(err) = xadd {
            tracing::warn!(error=%err, "redis timeseries XADD error");
        } else {
            let _: redis::RedisResult<()> = redis::cmd("EXPIRE")
                .arg(&stream_key)
                .arg(86_400)
                .query_async(mgr)
                .await;
        }

        r.reset_interval();
    }
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
