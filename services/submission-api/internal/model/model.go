package model

import "time"

type Submission struct {
	SubmissionID  string             `json:"submission_id"`
	TeamID        string             `json:"team_id"`
	Language      string             `json:"language"`
	Protocol      string             `json:"protocol"`
	Artifact      SubmissionArtifact `json:"artifact"`
	CreatedAtUnix int64              `json:"created_at_unix"`
	CreatedAt     time.Time          `json:"created_at"`
}

type SubmissionArtifact struct {
	ArtifactID  string `json:"artifact_id"`
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type,omitempty"`
}

type RunStatus string

const (
	RunStatusQueued RunStatus = "QUEUED"
)

type SandboxSpec struct {
	CPULimit      string `json:"cpu_limit"`
	MemoryLimit   string `json:"memory_limit"`
	NetworkEgress bool   `json:"network_egress"`
}

type BenchmarkRunConfig struct {
	BotCount       int    `json:"bot_count"`
	RatePerBot     int    `json:"rate_per_bot"`
	DurationSec    int    `json:"duration_sec"`
	WarmupSec      int    `json:"warmup_sec"`
	EngineEndpoint string `json:"engine_endpoint,omitempty"`
}

type BenchmarkRun struct {
	RunID         string             `json:"run_id"`
	SubmissionID  string             `json:"submission_id"`
	TeamID        string             `json:"team_id"`
	Status        RunStatus          `json:"status"`
	BenchmarkSeed int64              `json:"benchmark_seed"`
	Sandbox       SandboxSpec        `json:"sandbox"`
	Config        BenchmarkRunConfig `json:"config"`
	CreatedAtUnix int64              `json:"created_at_unix"`
	UpdatedAtUnix int64              `json:"updated_at_unix"`
	CreatedAt     time.Time          `json:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at"`
}

type CreateRunRequest struct {
	BenchmarkSeed int64              `json:"benchmark_seed"`
	Sandbox       SandboxSpec        `json:"sandbox"`
	Config        BenchmarkRunConfig `json:"config"`
}

func DefaultRunRequest() CreateRunRequest {
	return CreateRunRequest{
		BenchmarkSeed: 42,
		Sandbox: SandboxSpec{
			CPULimit:      "1",
			MemoryLimit:   "512Mi",
			NetworkEgress: false,
		},
		Config: BenchmarkRunConfig{
			BotCount:    10,
			RatePerBot:  2,
			DurationSec: 5,
			WarmupSec:   0,
		},
	}
}
