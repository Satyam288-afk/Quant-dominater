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
                    if let Err(err) = flush(&mut mgr, &mut per_run).await {
                        tracing::warn!(error=%err, "redis flush error");
                    }
                }
            }
        }
        let _ = flush(&mut mgr, &mut per_run).await;
    });
    Ok(tx)
}

#[derive(Default)]
struct RunRollup {
    orders_sent: u64,
    acks_received: u64,
    fills_received: u64,
    timeouts: u64,
    errors: u64,
    latency_sum_ns: u128,
    latency_count: u64,
    last_recv_ts_ns: u64,
}

impl RunRollup {
    fn observe(&mut self, e: &TelemetryEvent) {
        match e.event_type {
            EventKind::OrderSent => self.orders_sent += 1,
            EventKind::AckReceived => {
                self.acks_received += 1;
                if e.latency_ns > 0 {
                    self.latency_sum_ns += e.latency_ns as u128;
                    self.latency_count += 1;
                }
            }
            EventKind::FillReceived => self.fills_received += 1,
            EventKind::Timeout => self.timeouts += 1,
            EventKind::Error => self.errors += 1,
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
}

async fn flush(
    mgr: &mut ConnectionManager,
    per_run: &mut HashMap<String, RunRollup>,
) -> Result<()> {
    for (run_id, r) in per_run.iter() {
        let key = format!("run:{}:metrics", run_id);
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
        let stream_key = format!("run:{}:timeseries:tps", run_id);
        pipe.cmd("XADD")
            .arg(&stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(600)
            .arg("*")
            .arg("orders")
            .arg(r.orders_sent)
            .arg("acks")
            .arg(r.acks_received)
            .arg("fills")
            .arg(r.fills_received)
            .ignore();
        // execute; we ignore individual command errors at flush granularity
        let _: redis::RedisResult<()> = pipe.query_async(mgr).await;
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
    let _: () = mgr.set(&format!("submission:{}:last_run", team_id), run_id).await?;
    Ok(())
}
