package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"orchestrator/internal/model"
)

func TestValidatorUsesPackagedBinary(t *testing.T) {
	repoRoot := t.TempDir()
	run := &model.BenchmarkRun{
		RunID:       "run_1",
		ArtifactDir: filepath.Join(t.TempDir(), "run_1"),
	}
	if err := os.MkdirAll(run.ArtifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"events.jsonl", "contestant_outputs.jsonl"} {
		if err := os.WriteFile(filepath.Join(run.ArtifactDir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	bin := filepath.Join(t.TempDir(), "validator")
	if err := os.WriteFile(bin, []byte(`#!/bin/sh
set -eu
printf '{"run_id":"run_1","valid":true,"fills_checked":0}\n'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VALIDATOR_BIN", bin)

	result, err := NewValidator(repoRoot).Run(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.RunID != "run_1" {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(filepath.Join(run.ArtifactDir, "validation.json")); err != nil {
		t.Fatalf("expected validation artifact: %v", err)
	}
}
