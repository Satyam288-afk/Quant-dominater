-- TimescaleDB schema for the IICPC benchmarking platform. Loaded automatically
-- on first container start. Person B owns the metrics_raw / metrics_1s /
-- run_summary / run_resource / scores tables; Person C runs the compose.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw per-event telemetry. The high-cardinality source of truth for scoring;
-- aggressively compressed + aged out (see policies below) so the volume the
-- score-engine percentile-sorts stays bounded.
CREATE TABLE IF NOT EXISTS metrics_raw (
  time            TIMESTAMPTZ      NOT NULL,
  run_id          TEXT             NOT NULL,
  bot_id          TEXT             NOT NULL,
  event_type      TEXT             NOT NULL,
  client_order_id TEXT,
  seq_no          BIGINT,
  latency_ns      BIGINT,
  send_ts_ns      BIGINT,
  recv_ts_ns      BIGINT
);
SELECT create_hypertable('metrics_raw', 'time',
                         chunk_time_interval => INTERVAL '5 minutes',
                         if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS metrics_raw_run_time
  ON metrics_raw (run_id, time DESC);
-- Supports the score-engine percentile sort (ORDER BY latency_ns per run_id).
CREATE INDEX IF NOT EXISTS metrics_raw_run_latency
  ON metrics_raw (run_id, latency_ns);

-- Retention + compression keep the PVC from filling: segment by run_id so each
-- run's chunks compress as a unit, compress chunks older than an hour, and drop
-- raw telemetry after a day (scores/aggregates outlive it). Policy adds are
-- idempotent via if_not_exists so re-running this init script is safe.
ALTER TABLE metrics_raw SET (timescaledb.compress,
                             timescaledb.compress_segmentby = 'run_id');
SELECT add_compression_policy('metrics_raw', INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('metrics_raw', INTERVAL '1 day', if_not_exists => TRUE);

-- 1-second aggregates, Timescale-native: a continuous aggregate over metrics_raw
-- (no separate writer). Refreshed on a schedule below; this is what makes "data
-- is downsampled aggressively" actually true.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1s
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 second', time) AS bucket,
       run_id,
       count(*)        AS samples,
       avg(latency_ns) AS avg_latency_ns,
       min(latency_ns) AS min_latency_ns,
       max(latency_ns) AS max_latency_ns
FROM metrics_raw
GROUP BY bucket, run_id
WITH NO DATA;
-- Keep the rollup current: materialize buckets from a day ago up to a minute ago
-- (the open bucket is left to the next refresh), every 30s.
SELECT add_continuous_aggregate_policy('metrics_1s',
                                       start_offset => INTERVAL '1 day',
                                       end_offset   => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '30 seconds',
                                       if_not_exists => TRUE);

-- Resource samples (CPU / mem from sandbox runner, future hookup).
CREATE TABLE IF NOT EXISTS run_resource (
  time     TIMESTAMPTZ      NOT NULL,
  run_id   TEXT             NOT NULL,
  cpu_pct  DOUBLE PRECISION,
  mem_mb   DOUBLE PRECISION,
  rss_mb   DOUBLE PRECISION
);
SELECT create_hypertable('run_resource', 'time',
                         chunk_time_interval => INTERVAL '5 minutes',
                         if_not_exists => TRUE);
-- Same bounded-retention story as metrics_raw so this PVC can't fill either.
SELECT add_retention_policy('run_resource', INTERVAL '1 day', if_not_exists => TRUE);

-- Final scores. Read by leaderboard-api.
CREATE TABLE IF NOT EXISTS scores (
  run_id            TEXT             PRIMARY KEY,
  team_id           TEXT             NOT NULL DEFAULT 'unknown',
  valid             BOOLEAN          NOT NULL,
  final_score       DOUBLE PRECISION NOT NULL,
  latency_score     DOUBLE PRECISION,
  throughput_score  DOUBLE PRECISION,
  stability_score   DOUBLE PRECISION,
  resource_score    DOUBLE PRECISION,
  p50_ms            DOUBLE PRECISION,
  p90_ms            DOUBLE PRECISION,
  p99_ms            DOUBLE PRECISION,
  p999_ms           DOUBLE PRECISION,
  tps               DOUBLE PRECISION,
  failure_reason    TEXT,
  created_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS scores_team_final
  ON scores (team_id, final_score DESC);
