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
		Status:        RunStatusBenchmarking,
		BenchmarkSeed: 42,
		Sandbox: SandboxSpec{
			CPULimit:      "1",
			MemoryLimit:   "512Mi",
			NetworkEgress: false,
		},
		Config: BenchmarkRunConfig{
			BotCount:       10,
			RatePerBot:     2,
			DurationSec:    5,
			EngineEndpoint: "ws://127.0.0.1:8080/ws",
		},
		CreatedAtUnix: 100,
		UpdatedAtUnix: 200,
	}

	pb := run.ToProto()
	if pb.GetStatus() != benchmarkv1.RunStatus_BENCHMARKING {
		t.Fatalf("status = %v, want BENCHMARKING", pb.GetStatus())
	}
	if pb.GetConfig().GetEngineEndpoint() != "ws://127.0.0.1:8080/ws" {
		t.Fatalf("engine endpoint = %q", pb.GetConfig().GetEngineEndpoint())
	}
}
