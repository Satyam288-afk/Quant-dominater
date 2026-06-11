package executor

import (
	"os"
	"path/filepath"
	"testing"

	"orchestrator/internal/model"
)

func TestScoreRejectsValidButEmptyBenchmark(t *testing.T) {
	run := &model.BenchmarkRun{
		RunID:       "run_1",
		ArtifactDir: filepath.Join(t.TempDir(), "run_1"),
		Config: model.BenchmarkRunConfig{
			BotCount:   10,
			RatePerBot: 2,
		},
	}
	if err := createDir(run.ArtifactDir); err != nil {
		t.Fatal(err)
	}

	score, err := Score(run, &model.Metrics{}, &model.ValidationResult{RunID: "run_1", Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if score.Valid {
		t.Fatal("score should not remain valid when no benchmark orders were produced")
	}
	if score.Score != 0 {
		t.Fatalf("score = %v, want 0", score.Score)
	}
	if score.FailureReason != "no benchmark orders produced" {
		t.Fatalf("failure reason = %q", score.FailureReason)
	}
}

// Pin the live composite weights and sub-scores. This is the path the
// orchestrator actually runs (manager.go calls executor.Score), and it was
// previously untested — a 0.40->0.04 weight typo or a curve regression would
// have shipped silently.
func TestScoreWeightedComposite(t *testing.T) {
	run := &model.BenchmarkRun{
		RunID:       "run_w",
		ArtifactDir: filepath.Join(t.TempDir(), "run_w"),
		Config:      model.BenchmarkRunConfig{BotCount: 10, RatePerBot: 2}, // expected tps = 20
	}
	if err := createDir(run.ArtifactDir); err != nil {
		t.Fatal(err)
	}
	score, err := Score(run, &model.Metrics{
		OrdersSent: 100, Timeouts: 10, TPS: 10, P99MS: 5.0,
	}, &model.ValidationResult{RunID: "run_w", Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if score.LatencyScore != 37.05 { // log curve: 100*log10(50/5)/log10(50/0.1)
		t.Fatalf("latency = %v, want 37.05", score.LatencyScore)
	}
	if score.ThroughputScore != 50 { // 10/20
		t.Fatalf("throughput = %v, want 50", score.ThroughputScore)
	}
	if score.StabilityScore != 90 { // 1 - 10/100
		t.Fatalf("stability = %v, want 90", score.StabilityScore)
	}
	// 0.40*37.05 + 0.30*50 + 0.20*90 + 0.10*100 = 57.82
	if score.Score != 57.82 {
		t.Fatalf("composite = %v, want 57.82", score.Score)
	}
}

func TestLatencyScoreGuardsZeroAndDiscriminates(t *testing.T) {
	// p99=0 (parse/measure failure) must NOT earn the floor's 100.
	if got := latencyScore(0); got != 0 {
		t.Fatalf("latencyScore(0) = %v, want 0 (no credit for no measurement)", got)
	}
	if got := latencyScore(0.05); got != 100 {
		t.Fatalf("latencyScore(0.05) = %v, want 100", got)
	}
	if got := latencyScore(5.0); got != 37.05 {
		t.Fatalf("latencyScore(5.0) = %v, want 37.05", got)
	}
	if !(latencyScore(0.5) > latencyScore(2) && latencyScore(2) > latencyScore(10)) {
		t.Fatal("latency curve must strictly decrease")
	}
}

func createDir(path string) error {
	return os.MkdirAll(path, 0o755)
}
