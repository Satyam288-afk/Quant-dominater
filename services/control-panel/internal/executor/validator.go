package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"control-panel/internal/run"
)

type Validator struct {
	RepoRoot string
}

func (v *Validator) Run(ctx context.Context, r *run.BenchmarkRun) (*run.ValidationResult, error) {
	events := filepath.Join(r.ArtifactDir, "events.jsonl")
	outputs := filepath.Join(r.ArtifactDir, "contestant_outputs.jsonl")

	output := runLoggedCommand(
		ctx,
		r,
		v.RepoRoot,
		"cargo",
		"run", "-p", "validator", "--",
		"--events", events,
		"--contestant-outputs", outputs,
	)

	stdout := strings.TrimSpace(output.Stdout)
	if stdout == "" {
		if output.Err != nil {
			return nil, fmt.Errorf("validator failed: %w", output.Err)
		}
		return nil, fmt.Errorf("validator produced no JSON")
	}

	if err := os.WriteFile(filepath.Join(r.ArtifactDir, "validation.json"), []byte(stdout+"\n"), 0o644); err != nil {
		return nil, err
	}

	validation, err := parseValidation(stdout)
	if err != nil {
		if output.Err != nil {
			return nil, fmt.Errorf("validator failed: %w: %v", output.Err, err)
		}
		return nil, err
	}
	if validation.RunID == "" {
		validation.RunID = r.RunID
	}

	if output.Err != nil && validation.Valid {
		return nil, fmt.Errorf("validator failed despite valid result: %w", output.Err)
	}

	return validation, nil
}

func parseValidation(text string) (*run.ValidationResult, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, err
	}

	result := &run.ValidationResult{Raw: raw}
	if value, ok := raw["run_id"].(string); ok {
		result.RunID = value
	}
	if value, ok := raw["valid"].(bool); ok {
		result.Valid = value
	}
	if value, ok := raw["fills_checked"].(float64); ok {
		result.FillsChecked = int(value)
	}
	if value, ok := raw["reason"].(string); ok {
		result.Reason = value
	}
	if value, ok := raw["first_bad_seq"].(float64); ok {
		result.FirstBadSeq = int(value)
	}
	if value, ok := raw["expected"].(map[string]any); ok {
		result.Expected = value
	}
	if value, ok := raw["actual"].(map[string]any); ok {
		result.Actual = value
	}
	return result, nil
}
