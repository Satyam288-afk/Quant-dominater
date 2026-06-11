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

func createDir(path string) error {
	return os.MkdirAll(path, 0o755)
}
