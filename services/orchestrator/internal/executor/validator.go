package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"orchestrator/internal/model"
)

type Validator struct {
	repoRoot string
}

func NewValidator(repoRoot string) *Validator {
	return &Validator{repoRoot: repoRoot}
}

func (v *Validator) Run(ctx context.Context, run *model.BenchmarkRun) (*model.ValidationResult, error) {
	validatorArgs := []string{
		"--events", filepath.Join(run.ArtifactDir, "events.jsonl"),
		"--contestant-outputs", filepath.Join(run.ArtifactDir, "contestant_outputs.jsonl"),
	}
	// Same policy as the fleet spawn: never pay a cargo compile inside the
	// judged pipeline when a release binary is already built. Container images
	// can set VALIDATOR_BIN to a packaged binary.
	command := strings.TrimSpace(os.Getenv("VALIDATOR_BIN"))
	if command == "" {
		command = filepath.Join(v.repoRoot, "target", "release", "validator")
	}
	args := validatorArgs
	if _, err := os.Stat(command); err != nil {
		command = "cargo"
		args = append([]string{"run", "-p", "validator", "--"}, validatorArgs...)
	}
	output := runLoggedCommand(ctx, run, v.repoRoot, command, args...)

	stdout := strings.TrimSpace(output.Stdout)
	if stdout == "" {
		if output.Err != nil {
			return nil, fmt.Errorf("validator failed: %w", output.Err)
		}
		return nil, fmt.Errorf("validator produced no JSON")
	}
	if err := os.WriteFile(filepath.Join(run.ArtifactDir, "validation.json"), []byte(stdout+"\n"), 0o644); err != nil {
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
		validation.RunID = run.RunID
	}
	return validation, nil
}

func parseValidation(text string) (*model.ValidationResult, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, err
	}
	result := &model.ValidationResult{Raw: raw}
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
