package model

import (
	"testing"

	benchmarkv1 "github.com/iicpc/benchmark-platform/shared/go/benchmark/v1"
)

func TestBenchmarkRunToProtoUsesSharedContract(t *testing.T) {
	run := &BenchmarkRun{
		RunID:         "run_1",
		SubmissionID:  "sub_1",
		TeamID:        "team_1",
		Status:        RunStatusQueued,
		BenchmarkSeed: 42,
		Sandbox: SandboxSpec{
			CPULimit:      "500m",
			MemoryLimit:   "512Mi",
			NetworkEgress: false,
		},
		Config: BenchmarkRunConfig{
			BotCount:       10,
			RatePerBot:     2,
			DurationSec:    5,
			WarmupSec:      1,
			EngineEndpoint: "ws://127.0.0.1:8080/ws",
		},
		CreatedAtUnix: 100,
		UpdatedAtUnix: 200,
	}

	pb := run.ToProto()
	if pb.GetStatus() != benchmarkv1.RunStatus_QUEUED {
		t.Fatalf("status = %v, want QUEUED", pb.GetStatus())
	}
	if pb.GetSandbox().GetCpuLimit() != "500m" {
		t.Fatalf("cpu_limit = %q", pb.GetSandbox().GetCpuLimit())
	}
	if pb.GetConfig().GetRatePerBot() != 2 {
		t.Fatalf("rate_per_bot = %d", pb.GetConfig().GetRatePerBot())
	}
}
