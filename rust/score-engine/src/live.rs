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

    Ok(ExtractedMetrics {
        p50_ms: p50_ms.unwrap_or(0.0),
        p90_ms: p90_ms.unwrap_or(0.0),
        p99_ms: p99_ms.unwrap_or(0.0),
        p999_ms: p999_ms.unwrap_or(0.0),
        tps,
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

pub async fn publish_leaderboard(redis_url: &str, team_id: &str, score: &ScoreJson) -> Result<()> {
    let client = redis::Client::open(redis_url.to_string()).context("opening redis url")?;
    let mut mgr = ConnectionManager::new(client).await?;

    // Always reflect the latest run's score for the team in the global
    // leaderboard. Teams who beat their previous score climb naturally
    // since ZADD overwrites.
    let _: () = mgr
        .zadd("leaderboard:global", team_id, score.score)
        .await?;

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
    Ok(())
}
