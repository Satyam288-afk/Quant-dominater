package run

import "time"

type Status string

const (
	StatusQueued       Status = "QUEUED"
	StatusStarting     Status = "STARTING_ENGINE"
	StatusHealthCheck  Status = "HEALTHCHECKING"
	StatusBenchmarking Status = "BENCHMARKING"
	StatusValidating   Status = "VALIDATING"
	StatusScoring      Status = "SCORING"
	StatusFinished     Status = "FINISHED"
	StatusFailed       Status = "FAILED"
	StatusCancelled    Status = "CANCELLED"
)

type RunRequest struct {
	TeamID       string `json:"team_id"`
	EngineMode   string `json:"engine_mode"`
	BotCount     int    `json:"bot_count"`
	OrdersPerSec int    `json:"orders_per_sec"`
	DurationSec  int    `json:"duration_sec"`
	Seed         int64  `json:"seed"`
}

type BenchmarkRun struct {
	RunID        string     `json:"run_id"`
	TeamID       string     `json:"team_id"`
	Status       Status     `json:"status"`
	EngineMode   string     `json:"engine_mode"`
	BotCount     int        `json:"bot_count"`
	OrdersPerSec int        `json:"orders_per_sec"`
	DurationSec  int        `json:"duration_sec"`
	Seed         int64      `json:"seed"`
	ArtifactDir  string     `json:"artifact_dir"`
	StartedAt    time.Time  `json:"started_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`

	Valid         *bool             `json:"valid,omitempty"`
	Score         float64           `json:"score,omitempty"`
	FailureStage  string            `json:"failure_stage,omitempty"`
	FailureReason string            `json:"failure_reason,omitempty"`
	Metrics       *Metrics          `json:"metrics,omitempty"`
	Validation    *ValidationResult `json:"validation,omitempty"`
}

type Metrics struct {
	RunID         string  `json:"run_id"`
	Bots          int     `json:"bots"`
	OrdersSent    int     `json:"orders_sent"`
	AcksReceived  int     `json:"acks_received"`
	FillsReceived int     `json:"fills_received"`
	Timeouts      int     `json:"timeouts"`
	ConnectErrors int     `json:"connect_errors"`
	TPS           float64 `json:"tps"`
	P50MS         float64 `json:"p50_ms,omitempty"`
	P90MS         float64 `json:"p90_ms,omitempty"`
	P99MS         float64 `json:"p99_ms,omitempty"`
	EventsOut     string  `json:"events_out,omitempty"`
	OutputsOut    string  `json:"outputs_out,omitempty"`
	RawOutput     string  `json:"raw_output,omitempty"`
}

type ValidationResult struct {
	RunID        string         `json:"run_id"`
	Valid        bool           `json:"valid"`
	FillsChecked int            `json:"fills_checked,omitempty"`
	Reason       string         `json:"reason,omitempty"`
	FirstBadSeq  int            `json:"first_bad_seq,omitempty"`
	Expected     map[string]any `json:"expected,omitempty"`
	Actual       map[string]any `json:"actual,omitempty"`
	Raw          map[string]any `json:"raw,omitempty"`
}

type ScoreResult struct {
	RunID           string  `json:"run_id"`
	Score           float64 `json:"score"`
	Valid           bool    `json:"valid"`
	LatencyScore    float64 `json:"latency_score"`
	ThroughputScore float64 `json:"throughput_score"`
	StabilityScore  float64 `json:"stability_score"`
	ResourceScore   float64 `json:"resource_score"`
	CorrectnessGate string  `json:"correctness_gate"`
	FailureReason   string  `json:"failure_reason,omitempty"`
}
