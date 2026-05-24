package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"orchestrator/internal/model"
)

type BotFleet struct {
	repoRoot string
}

func NewBotFleet(repoRoot string) *BotFleet {
	return &BotFleet{repoRoot: repoRoot}
}

func (b *BotFleet) Run(ctx context.Context, run *model.BenchmarkRun, endpoint string) (*model.Metrics, error) {
	eventsOut := filepath.Join(run.ArtifactDir, "events.jsonl")
	outputsOut := filepath.Join(run.ArtifactDir, "contestant_outputs.jsonl")
	output := runLoggedCommand(
		ctx,
		run,
		b.repoRoot,
		"cargo",
		"run", "-p", "bot-fleet", "--bin", "bot-fleet", "--",
		"--target", endpoint,
		"--bots", strconv.Itoa(run.Config.BotCount),
		"--orders-per-sec", strconv.Itoa(run.Config.RatePerBot),
		"--duration-sec", strconv.Itoa(run.Config.DurationSec),
		"--seed", strconv.FormatInt(run.BenchmarkSeed, 10),
		"--run-id", run.RunID,
		"--events-out", eventsOut,
		"--outputs-out", outputsOut,
	)
	if output.Err != nil {
		return nil, fmt.Errorf("bot fleet failed: %w", output.Err)
	}

	metrics := parseMetrics(output.Stdout)
	metrics.RawOutput = output.Stdout
	if metrics.RunID == "" {
		metrics.RunID = run.RunID
	}
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(run.ArtifactDir, "metrics.json"), data, 0o644); err != nil {
		return nil, err
	}
	return metrics, nil
}

func parseMetrics(text string) *model.Metrics {
	metrics := &model.Metrics{}
	for _, line := range strings.Split(text, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "run_id":
			metrics.RunID = value
		case "bots":
			metrics.Bots = atoi(value)
		case "orders_sent":
			metrics.OrdersSent = atoi(value)
		case "acks_received":
			metrics.AcksReceived = atoi(value)
		case "fills_received":
			metrics.FillsReceived = atoi(value)
		case "timeouts":
			metrics.Timeouts = atoi(value)
		case "connect_errors":
			metrics.ConnectErrors = atoi(value)
		case "tps":
			metrics.TPS = atof(value)
		case "p50":
			metrics.P50MS = parseMS(value)
		case "p90":
			metrics.P90MS = parseMS(value)
		case "p99":
			metrics.P99MS = parseMS(value)
		case "events_out":
			metrics.EventsOut = value
		case "outputs_out":
			metrics.OutputsOut = value
		}
	}
	return metrics
}

func atoi(value string) int {
	parsed, _ := strconv.Atoi(strings.TrimSpace(value))
	return parsed
}

func atof(value string) float64 {
	parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed
}

func parseMS(value string) float64 {
	value = strings.TrimSpace(strings.TrimSuffix(value, "ms"))
	if value == "n/a" || value == "" {
		return 0
	}
	return atof(value)
}
