package sandbox

import "time"

type BuildRequest struct {
	SubmissionID string `json:"submission_id"`
	ArtifactURI  string `json:"artifact_uri"`
	Language     string `json:"language"`
}

type ImageRef struct {
	ImageRef     string    `json:"image_ref"`
	SubmissionID string    `json:"submission_id"`
	ArtifactURI  string    `json:"artifact_uri"`
	Language     string    `json:"language"`
	BuiltAt      time.Time `json:"built_at"`
}

type SandboxSpec struct {
	CPULimit string `json:"cpu_limit"`
	// CpusetCpus pins the sandbox to specific physical cores (e.g. "2,3").
	// Pinning removes scheduler cross-talk between contestants so latency
	// numbers are fair and reproducible. Empty = no pinning.
	CpusetCpus    string `json:"cpuset_cpus,omitempty"`
	MemoryLimit   string `json:"memory_limit"`
	NetworkEgress bool   `json:"network_egress"`
}

type StartRequest struct {
	RunID      string      `json:"run_id"`
	ImageRef   string      `json:"image_ref"`
	EngineMode string      `json:"engine_mode"`
	EventsPath string      `json:"events_path,omitempty"`
	Spec       SandboxSpec `json:"spec"`
}

type SandboxHandle struct {
	SandboxID string      `json:"sandbox_id"`
	RunID     string      `json:"run_id"`
	ImageRef  string      `json:"image_ref"`
	Endpoint  string      `json:"endpoint"`
	HealthURL string      `json:"health_url"`
	Spec      SandboxSpec `json:"spec"`
	StartedAt time.Time   `json:"started_at"`
}

type Runner interface {
	Build(req BuildRequest) (ImageRef, error)
	Start(req StartRequest) (SandboxHandle, error)
	Stop(sandboxID string) error
	Get(sandboxID string) (SandboxHandle, bool)
	List() []SandboxHandle
}
