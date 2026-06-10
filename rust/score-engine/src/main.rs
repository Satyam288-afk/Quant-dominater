use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use bench_core::score::{compose, CompositeScore, ScoreInputs};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use serde_json::Value;

#[cfg(feature = "live")]
mod live;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Backend {
    /// Read metrics from a JSON file (Person A's metrics.json or this
    /// project's telemetry-summary.json). Default — no infra needed.
    File,
    /// Pull metrics from TimescaleDB and (optionally) write the final
    /// score into the scores table + Redis leaderboard ZSET. Requires
    /// --features live at build time.
    Live,
}

#[derive(Debug, Parser)]
#[command(about = "Compute the final composite score from validation + telemetry artifacts")]
struct Args {
    /// Path to validation.json produced by the validator.
    #[arg(long)]
    validation: PathBuf,

    /// Path to a metrics JSON file. Only used when --backend=file.
    /// Two shapes are accepted:
    ///   * Go orchestrator's `metrics.json` (Person A's parseMetrics output)
    ///   * telemetry-ingester's `telemetry-summary.json` (this crate's siblings)
    /// The score-engine auto-detects which one is provided.
    #[arg(long)]
    metrics: Option<PathBuf>,

    /// run_id to score. If empty, taken from validation.json or the first
    /// run in telemetry-summary.
    #[arg(long, default_value = "")]
    run_id: String,

    /// Expected TPS for the throughput score denominator. Should match
    /// `bot_count * rate_per_bot` from the benchmark config. 0 disables
    /// throughput scoring (defaults to 100% credit).
    #[arg(long, default_value_t = 0.0)]
    expected_tps: f64,

    /// Output path for score.json.
    #[arg(long, default_value = "score.json")]
    out: PathBuf,

    /// Where to pull metrics from. `file` is the default safe path used by
    /// Person A's orchestrator. `live` queries TimescaleDB directly and
    /// optionally publishes to Redis. Live needs --features live.
    #[arg(long, value_enum, default_value_t = Backend::File)]
    backend: Backend,

    /// TimescaleDB connection URL (live mode).
    #[arg(long, env = "TIMESCALE_URL", default_value = "")]
    timescale_url: String,

    /// Redis URL. When set in live mode, the score is also written to the
    /// `scores` table + the `leaderboard:global` ZSET + the team's best
    /// score key, so the leaderboard-api can serve fresh data.
    #[arg(long, env = "REDIS_URL", default_value = "")]
    redis_url: String,

    /// Team id under which the score is recorded on the leaderboard.
    #[arg(long, env = "TEAM_ID", default_value = "unknown")]
    team_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ScoreJson {
    pub(crate) run_id: String,
    pub(crate) valid: bool,
    pub(crate) score: f64,
    pub(crate) latency_score: f64,
    pub(crate) throughput_score: f64,
    pub(crate) stability_score: f64,
    pub(crate) resource_score: f64,
    pub(crate) correctness_gate: &'static str,
    pub(crate) p50_ms: f64,
    pub(crate) p90_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) p999_ms: f64,
    pub(crate) tps: f64,
    pub(crate) peak_tps: f64,
    pub(crate) orders_sent: i64,
    pub(crate) acks_received: i64,
    pub(crate) timeouts: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let validation_bytes = tokio::fs::read(&args.validation)
        .await
        .with_context(|| format!("reading {:?}", args.validation))?;
    let validation: Value =
        serde_json::from_slice(&validation_bytes).context("decoding validation.json")?;

    let valid = validation
        .get("valid")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let failure_reason = validation
        .get("reason")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    // run_id resolution: explicit flag > validation.json > metrics file.
    let mut resolved_run_id = if !args.run_id.is_empty() {
        Some(args.run_id.clone())
    } else {
        validation
            .get("run_id")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
    };

    let metrics = match args.backend {
        Backend::File => {
            let path = args
                .metrics
                .as_ref()
                .ok_or_else(|| anyhow!("--metrics is required when --backend=file"))?;
            let metrics_bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("reading {:?}", path))?;
            let metrics_value: Value =
                serde_json::from_slice(&metrics_bytes).context("decoding metrics file")?;
            if resolved_run_id.is_none() {
                resolved_run_id = extract_first_run_id(&metrics_value);
            }
            let run_id = resolved_run_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let m = extract_metrics(&metrics_value, &run_id)
                .ok_or_else(|| anyhow!("metrics file did not contain run {run_id}"))?;
            resolved_run_id = Some(run_id);
            m
        }
        Backend::Live => {
            #[cfg(not(feature = "live"))]
            {
                return Err(anyhow!(
                    "binary built without `live` feature; rebuild with --features live"
                ));
            }
            #[cfg(feature = "live")]
            {
                if args.timescale_url.is_empty() {
                    return Err(anyhow!("--timescale-url required when --backend=live"));
                }
                let run_id = resolved_run_id.clone().ok_or_else(|| {
                    anyhow!("--run-id required when --backend=live and validation.json has none")
                })?;
                let m = live::pull_metrics(&args.timescale_url, &run_id)
                    .await
                    .with_context(|| format!("pulling live metrics for run {run_id}"))?;
                resolved_run_id = Some(run_id);
                m
            }
        }
    };

    let run_id = resolved_run_id.unwrap_or_else(|| "unknown".to_string());

    let composite: CompositeScore = compose(ScoreInputs {
        valid,
        p99_ms: metrics.p99_ms,
        tps: metrics.tps,
        expected_tps: args.expected_tps,
        orders_sent: metrics.orders_sent,
        timeouts: metrics.timeouts,
        connect_errors: metrics.connect_errors,
        cpu_pct: None,
        mem_mb: None,
    });

    let result = ScoreJson {
        run_id: run_id.clone(),
        valid,
        score: composite.final_score,
        latency_score: composite.latency_score,
        throughput_score: composite.throughput_score,
        stability_score: composite.stability_score,
        resource_score: composite.resource_score,
        correctness_gate: if valid { "passed" } else { "failed" },
        p50_ms: metrics.p50_ms,
        p90_ms: metrics.p90_ms,
        p99_ms: metrics.p99_ms,
        p999_ms: metrics.p999_ms,
        tps: metrics.tps,
        peak_tps: metrics.peak_tps,
        orders_sent: metrics.orders_sent,
        acks_received: metrics.acks_received,
        timeouts: metrics.timeouts,
        failure_reason: if valid { None } else { failure_reason },
    };

    let bytes = serde_json::to_vec_pretty(&result)?;
    if let Some(parent) = args.out.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
    }
    tokio::fs::write(&args.out, &bytes)
        .await
        .with_context(|| format!("writing {:?}", args.out))?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    // Live mode: persist into the scores table + leaderboard ZSET. Best
    // effort — a failed publish is logged but does not fail the run since
    // the local score.json is the source of truth.
    #[cfg(feature = "live")]
    if matches!(args.backend, Backend::Live) {
        if !args.timescale_url.is_empty() {
            if let Err(err) = live::upsert_score(&args.timescale_url, &args.team_id, &result).await
            {
                eprintln!("warning: scores table upsert failed: {err:#}");
            }
        }
        if !args.redis_url.is_empty() {
            if let Err(err) =
                live::publish_leaderboard(&args.redis_url, &args.team_id, &result).await
            {
                eprintln!("warning: redis leaderboard publish failed: {err:#}");
            }
        }
        if !args.timescale_url.is_empty() && !args.redis_url.is_empty() {
            if let Err(err) =
                live::publish_latency_series(&args.timescale_url, &args.redis_url, &run_id).await
            {
                eprintln!("warning: latency series publish failed: {err:#}");
            }
        }
    }

    Ok(())
}

pub(crate) struct ExtractedMetrics {
    pub(crate) p50_ms: f64,
    pub(crate) p90_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) p999_ms: f64,
    pub(crate) tps: f64,
    pub(crate) peak_tps: f64,
    pub(crate) orders_sent: i64,
    pub(crate) acks_received: i64,
    pub(crate) timeouts: i64,
    pub(crate) connect_errors: i64,
    #[allow(dead_code)]
    pub(crate) fills_received: i64,
}

fn extract_first_run_id(value: &Value) -> Option<String> {
    if let Some(s) = value.get("run_id").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(arr) = value.get("runs").and_then(Value::as_array) {
        if let Some(first) = arr.first() {
            return first
                .get("run_id")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
        }
    }
    None
}

fn extract_metrics(value: &Value, run_id: &str) -> Option<ExtractedMetrics> {
    // Shape A: Go orchestrator metrics.json (single object).
    if value.get("orders_sent").is_some() && value.get("tps").is_some() {
        return Some(ExtractedMetrics {
            p50_ms: number(value, "p50_ms"),
            p90_ms: number(value, "p90_ms"),
            p99_ms: number(value, "p99_ms"),
            p999_ms: 0.0,
            tps: number(value, "tps"),
            // Go metrics.json has no per-second peak; fall back to average.
            peak_tps: number_or(value, "peak_tps", number(value, "tps")),
            orders_sent: int(value, "orders_sent"),
            acks_received: int(value, "acks_received"),
            timeouts: int(value, "timeouts"),
            connect_errors: int(value, "connect_errors"),
            fills_received: int(value, "fills_received"),
        });
    }
    // Shape B: telemetry-ingester telemetry-summary.json with `runs` array.
    if let Some(runs) = value.get("runs").and_then(Value::as_array) {
        let target = runs
            .iter()
            .find(|r| r.get("run_id").and_then(Value::as_str) == Some(run_id));
        if let Some(r) = target.or_else(|| runs.first()) {
            return Some(ExtractedMetrics {
                p50_ms: number(r, "p50_ms"),
                p90_ms: number(r, "p90_ms"),
                p99_ms: number(r, "p99_ms"),
                p999_ms: number(r, "p999_ms"),
                tps: number(r, "tps"),
                peak_tps: number_or(r, "peak_tps", number(r, "tps")),
                orders_sent: int(r, "orders_sent"),
                acks_received: int(r, "acks_received"),
                timeouts: int(r, "timeouts"),
                connect_errors: int(r, "errors"),
                fills_received: int(r, "fills_received"),
            });
        }
    }
    None
}

fn number(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

/// Like `number`, but returns `default` when the key is missing (rather than 0),
/// so an older metrics file without `peak_tps` degrades to average TPS.
fn number_or(value: &Value, key: &str, default: f64) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(default)
}

fn int(value: &Value, key: &str) -> i64 {
    value
        .get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)))
        .unwrap_or(0)
}
