-- TimescaleDB schema for the IICPC benchmarking platform. Loaded automatically
-- on first container start. Person B owns the metrics_raw / metrics_1s /
-- run_summary / run_resource / scores tables; Person C runs the compose.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw per-event telemetry (downsampled aggressively).
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

-- 1-second per-bot aggregates written by the ingester.
CREATE TABLE IF NOT EXISTS metrics_1s (
  time            TIMESTAMPTZ      NOT NULL,
  run_id          TEXT             NOT NULL,
  bot_id          TEXT             NOT NULL,
  orders_sent     BIGINT           NOT NULL,
  acks_received   BIGINT           NOT NULL,
  fills_received  BIGINT           NOT NULL,
  timeouts        BIGINT           NOT NULL,
  errors          BIGINT           NOT NULL,
  p50_ns          BIGINT,
  p90_ns          BIGINT,
  p99_ns          BIGINT,
  p999_ns         BIGINT,
  tps             DOUBLE PRECISION
);
SELECT create_hypertable('metrics_1s', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS metrics_1s_run_bot_time
  ON metrics_1s (run_id, bot_id, time DESC);

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
