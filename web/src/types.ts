// Mirrors board.Entry from services/leaderboard-api. Optional fields are only
// populated when a run has been scored (the score-engine writes the scorecard).
export interface LeaderboardEntry {
  run_id: string;
  team_id: string;
  score: number;
  valid: boolean;
  status?: string;
  failure_reason?: string;
  p50_ms?: number;
  p90_ms?: number;
  p99_ms?: number;
  p999_ms?: number;
  tps?: number;
  peak_tps?: number;
  latency_score?: number;
  throughput_score?: number;
  stability_score?: number;
  resource_score?: number;
  orders_sent?: number;
  acks_received?: number;
  timeouts?: number;
  updated_at: string;
}

export type ConnectionState = "connecting" | "open" | "closed";

// One per-second point of a run's latency/throughput series, served by
// GET /runs/{id}/timeseries (computed by the score-engine from Timescale).
export interface TimeseriesPoint {
  t: number; // seconds since the run's first ack
  tps: number;
  p50_ms: number;
  p99_ms: number;
}
