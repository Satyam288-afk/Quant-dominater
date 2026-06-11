package scoring

type BenchmarkRunConfig struct {
	BotCount    int `json:"bot_count"`
	RatePerBot  int `json:"rate_per_bot"`
	DurationSec int `json:"duration_sec"`
}

type RunSpec struct {
	RunID         string             `json:"run_id"`
	TeamID        string             `json:"team_id,omitempty"`
	BenchmarkSeed int64              `json:"benchmark_seed,omitempty"`
	Config        BenchmarkRunConfig `json:"config"`
}

type Metrics struct {
	RunID         string  `json:"run_id"`
	OrdersSent    int     `json:"orders_sent"`
	Timeouts      int     `json:"timeouts"`
	ConnectErrors int     `json:"connect_errors"`
	TPS           float64 `json:"tps"`
	P99MS         float64 `json:"p99_ms,omitempty"`
	// Peak resource usage of the contestant engine (0 = not measured).
	CPUPctPeak float64 `json:"cpu_pct_peak,omitempty"`
	MemMBPeak  float64 `json:"mem_mb_peak,omitempty"`
}

type ValidationResult struct {
	RunID  string `json:"run_id"`
	Valid  bool   `json:"valid"`
	Reason string `json:"reason,omitempty"`
}

type ScoreResult struct {
	RunID           string  `json:"run_id"`
	TeamID          string  `json:"team_id,omitempty"`
	Score           float64 `json:"score"`
	Valid           bool    `json:"valid"`
	LatencyScore    float64 `json:"latency_score"`
	ThroughputScore float64 `json:"throughput_score"`
	StabilityScore  float64 `json:"stability_score"`
	ResourceScore   float64 `json:"resource_score"`
	CorrectnessGate string  `json:"correctness_gate"`
	FailureReason   string  `json:"failure_reason,omitempty"`
}

type Request struct {
	RunID       string             `json:"run_id,omitempty"`
	TeamID      string             `json:"team_id,omitempty"`
	ArtifactDir string             `json:"artifact_dir,omitempty"`
	Config      BenchmarkRunConfig `json:"config,omitempty"`
	Metrics     *Metrics           `json:"metrics,omitempty"`
	Validation  *ValidationResult  `json:"validation,omitempty"`
}
