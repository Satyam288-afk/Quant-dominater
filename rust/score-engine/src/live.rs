// Live backend for score-engine. Pulls metrics from TimescaleDB and
// optionally publishes the final score to the `scores` table + Redis
// leaderboard. Compiled only with `--features live`.

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use crate::{ExtractedMetrics, ScoreJson};

pub async fn pull_metrics(timescale_url: &str, run_id: &str) -> Result<ExtractedMetrics> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(timescale_url)
        .await
        .with_context(|| format!("connecting timescale {timescale_url}"))?;

    // Single query: counters + duration + percentile_cont. Each FILTER
    // narrows by event_type; percentile_cont ignores zero-latency rows
    // (order_sent has latency_ns = 0, only acks carry real measurements).
    let row = sqlx::query(
        r#"
        SELECT
          COUNT(*) FILTER (WHERE event_type = 'order_sent')      AS orders_sent,
          COUNT(*) FILTER (WHERE event_type = 'ack_received')    AS acks_received,
          COUNT(*) FILTER (WHERE event_type = 'fill_received')   AS fills_received,
          COUNT(*) FILTER (WHERE event_type = 'timeout')         AS timeouts,
          COUNT(*) FILTER (WHERE event_type = 'error')           AS errors,
          EXTRACT(EPOCH FROM (MAX(time) FILTER (WHERE event_type IN ('ack_received','fill_received'))
                            - MIN(time) FILTER (WHERE event_type IN ('ack_received','fill_received'))))::DOUBLE PRECISION
              AS duration_secs,
          (percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ns) FILTER (WHERE latency_ns > 0))::DOUBLE PRECISION / 1e6  AS p50_ms,
          (percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ns) FILTER (WHERE latency_ns > 0))::DOUBLE PRECISION / 1e6  AS p90_ms,
          (percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ns) FILTER (WHERE latency_ns > 0))::DOUBLE PRECISION / 1e6  AS p99_ms,
          (percentile_cont(0.999) WITHIN GROUP (ORDER BY latency_ns) FILTER (WHERE latency_ns > 0))::DOUBLE PRECISION / 1e6 AS p999_ms
        FROM metrics_raw
        WHERE run_id = $1
        "#,
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .context("querying metrics_raw")?;

    let orders_sent: i64 = row.try_get("orders_sent")?;
    let acks_received: i64 = row.try_get("acks_received")?;
    let fills_received: i64 = row.try_get("fills_received")?;
    let timeouts: i64 = row.try_get("timeouts")?;
    let errors: i64 = row.try_get("errors")?;
    let duration_secs: Option<f64> = row.try_get("duration_secs").ok();
    let p50_ms: Option<f64> = row.try_get("p50_ms").ok();
    let p90_ms: Option<f64> = row.try_get("p90_ms").ok();
    let p99_ms: Option<f64> = row.try_get("p99_ms").ok();
    let p999_ms: Option<f64> = row.try_get("p999_ms").ok();

    let duration = duration_secs.unwrap_or(0.0).max(0.001);
    let tps = acks_received as f64 / duration;

    // Peak TPS = the busiest 1-second ack window — "max TPS before failure".
    // Best-effort: a failure here falls back to the average so scoring never
    // breaks on this secondary metric.
    let peak_tps: f64 = sqlx::query_scalar::<_, f64>(
        r#"
        SELECT COALESCE(MAX(c), 0)::DOUBLE PRECISION
        FROM (
            SELECT COUNT(*) AS c
            FROM metrics_raw
            WHERE run_id = $1 AND event_type = 'ack_received'
            GROUP BY time_bucket('1 second', time)
        ) s
        "#,
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap_or(tps);

    Ok(ExtractedMetrics {
        p50_ms: p50_ms.unwrap_or(0.0),
        p90_ms: p90_ms.unwrap_or(0.0),
        p99_ms: p99_ms.unwrap_or(0.0),
        p999_ms: p999_ms.unwrap_or(0.0),
        tps,
        peak_tps,
        orders_sent,
        acks_received,
        timeouts,
        connect_errors: errors,
        fills_received,
    })
}

pub async fn upsert_score(timescale_url: &str, team_id: &str, score: &ScoreJson) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(timescale_url)
        .await
        .context("connecting timescale for scores upsert")?;
    sqlx::query(
        r#"
        INSERT INTO scores (
            run_id, team_id, valid, final_score,
            latency_score, throughput_score, stability_score, resource_score,
            p50_ms, p90_ms, p99_ms, p999_ms, tps, failure_reason
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (run_id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            valid = EXCLUDED.valid,
            final_score = EXCLUDED.final_score,
            latency_score = EXCLUDED.latency_score,
            throughput_score = EXCLUDED.throughput_score,
            stability_score = EXCLUDED.stability_score,
            resource_score = EXCLUDED.resource_score,
            p50_ms = EXCLUDED.p50_ms,
            p90_ms = EXCLUDED.p90_ms,
            p99_ms = EXCLUDED.p99_ms,
            p999_ms = EXCLUDED.p999_ms,
            tps = EXCLUDED.tps,
            failure_reason = EXCLUDED.failure_reason
        "#,
    )
    .bind(&score.run_id)
    .bind(team_id)
    .bind(score.valid)
    .bind(score.score)
    .bind(score.latency_score)
    .bind(score.throughput_score)
    .bind(score.stability_score)
    .bind(score.resource_score)
    .bind(score.p50_ms)
    .bind(score.p90_ms)
    .bind(score.p99_ms)
    .bind(score.p999_ms)
    .bind(score.tps)
    .bind(score.failure_reason.clone())
    .execute(&pool)
    .await?;
    Ok(())
}

/// Compute the per-second latency/throughput series for a run straight from the
/// authoritative `metrics_raw` rows in Timescale and cache it in Redis as a JSON
/// array under `run:{id}:latency_series`, so the leaderboard UI can chart how
/// p50/p99 latency and TPS moved (and degraded) over the run. Timescale is the
/// source of truth here — reliable and exact — rather than sampling live.
pub async fn publish_latency_series(
    timescale_url: &str,
    redis_url: &str,
    run_id: &str,
) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(timescale_url)
        .await
        .context("connecting timescale for latency series")?;
    let rows = sqlx::query(
        r#"
        SELECT
          (EXTRACT(EPOCH FROM time_bucket('1 second', time)))::BIGINT AS sec,
          COUNT(*)                                                     AS acks,
          (percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ns))::DOUBLE PRECISION / 1e6 AS p50_ms,
          (percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ns))::DOUBLE PRECISION / 1e6 AS p99_ms
        FROM metrics_raw
        WHERE run_id = $1 AND event_type = 'ack_received' AND latency_ns > 0
        GROUP BY sec
        ORDER BY sec
        "#,
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .context("querying latency series")?;

    let mut base: Option<i64> = None;
    let mut points: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        let sec: i64 = row.try_get("sec")?;
        let acks: i64 = row.try_get("acks")?;
        let p50: f64 = row.try_get("p50_ms").unwrap_or(0.0);
        let p99: f64 = row.try_get("p99_ms").unwrap_or(0.0);
        let b = *base.get_or_insert(sec);
        points.push(serde_json::json!({
            "t": sec - b,
            "tps": acks,
            "p50_ms": (p50 * 100.0).round() / 100.0,
            "p99_ms": (p99 * 100.0).round() / 100.0,
        }));
    }
    let json = serde_json::to_string(&points)?;

    let client = redis::Client::open(redis_url.to_string()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client).await?;
    let _: () = redis::cmd("SET")
        .arg(format!("run:{}:latency_series", run_id))
        .arg(json)
        .arg("EX")
        .arg(86_400)
        .query_async(&mut mgr)
        .await?;
    Ok(())
}

pub async fn publish_leaderboard(redis_url: &str, team_id: &str, score: &ScoreJson) -> Result<()> {
    let client = redis::Client::open(redis_url.to_string()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client).await?;

    // Always reflect the latest run's score for the team in the global
    // leaderboard. Teams who beat their previous score climb naturally
    // since ZADD overwrites.
    let _: () = mgr.zadd("leaderboard:global", team_id, score.score).await?;

    // Track the team's best score separately so a regression run doesn't
    // demote them on the "personal best" view.
    let team_best_key = format!("team:{}:best_score", team_id);
    let prev: Option<f64> = mgr.get(&team_best_key).await.ok().flatten();
    if prev.map(|p| score.score > p).unwrap_or(true) {
        let _: () = mgr.set(&team_best_key, score.score).await?;
    }

    let _: () = mgr
        .set(
            format!("submission:{}:last_run", team_id),
            score.run_id.clone(),
        )
        .await?;

    // Full scorecard so the leaderboard-api can render rich rows (latency
    // percentiles + score breakdown) by reading Redis only — no Postgres on
    // the hot path. Keyed by team so the latest run wins.
    let scorecard_key = format!("team:{}:scorecard", team_id);
    let mut pipe = redis::pipe();
    pipe.cmd("HSET")
        .arg(&scorecard_key)
        .arg("run_id")
        .arg(&score.run_id)
        .arg("team_id")
        .arg(team_id)
        .arg("valid")
        .arg(if score.valid { 1 } else { 0 })
        .arg("score")
        .arg(score.score)
        .arg("latency_score")
        .arg(score.latency_score)
        .arg("throughput_score")
        .arg(score.throughput_score)
        .arg("stability_score")
        .arg(score.stability_score)
        .arg("resource_score")
        .arg(score.resource_score)
        .arg("p50_ms")
        .arg(score.p50_ms)
        .arg("p90_ms")
        .arg(score.p90_ms)
        .arg("p99_ms")
        .arg(score.p99_ms)
        .arg("p999_ms")
        .arg(score.p999_ms)
        .arg("tps")
        .arg(score.tps)
        .arg("peak_tps")
        .arg(score.peak_tps)
        .arg("orders_sent")
        .arg(score.orders_sent)
        .arg("acks_received")
        .arg(score.acks_received)
        .arg("timeouts")
        .arg(score.timeouts)
        .arg("failure_reason")
        .arg(score.failure_reason.clone().unwrap_or_default())
        .ignore();
    pipe.cmd("SADD")
        .arg("leaderboard:teams")
        .arg(team_id)
        .ignore();
    let _: redis::RedisResult<()> = pipe.query_async(&mut mgr).await;

    Ok(())
}
