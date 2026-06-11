package executor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	if err := writeSplitArtifacts(eventsOut, outputsOut, run.ArtifactDir); err != nil {
		return nil, err
	}
	if err := validateBenchmarkLoad(metrics); err != nil {
		return metrics, err
	}
	return metrics, nil
}

func validateBenchmarkLoad(metrics *model.Metrics) error {
	if metrics == nil {
		return fmt.Errorf("bot fleet produced no metrics")
	}
	if metrics.OrdersSent <= 0 {
		if metrics.ConnectErrors > 0 {
			return fmt.Errorf("bot fleet produced no orders; connect_errors=%d", metrics.ConnectErrors)
		}
		return fmt.Errorf("bot fleet produced no orders")
	}
	return nil
}

func writeSplitArtifacts(eventsPath, outputsPath, artifactDir string) error {
	if err := copyFile(eventsPath, filepath.Join(artifactDir, "orders.jsonl")); err != nil {
		return err
	}
	return splitOutputs(outputsPath, artifactDir)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func splitOutputs(outputsPath, artifactDir string) error {
	in, err := os.Open(outputsPath)
	if err != nil {
		return err
	}
	defer in.Close()

	acks, err := os.Create(filepath.Join(artifactDir, "acks.jsonl"))
	if err != nil {
		return err
	}
	defer acks.Close()

	fills, err := os.Create(filepath.Join(artifactDir, "fills.jsonl"))
	if err != nil {
		return err
	}
	defer fills.Close()

	cancels, err := os.Create(filepath.Join(artifactDir, "cancels.jsonl"))
	if err != nil {
		return err
	}
	defer cancels.Close()

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Bytes()
		switch outputKind(line) {
		case "ack":
			if _, err := acks.Write(append(line, '\n')); err != nil {
				return err
			}
		case "fill":
			if _, err := fills.Write(append(line, '\n')); err != nil {
				return err
			}
		case "cancel":
			if _, err := cancels.Write(append(line, '\n')); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

func outputKind(line []byte) string {
	var raw struct {
		EventType string `json:"event_type"`
		Message   struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"message"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return ""
	}
	if strings.Contains(raw.EventType, "ack") || raw.Message.Type == "ack" {
		if raw.Message.Status == "canceled" {
			return "cancel"
		}
		return "ack"
	}
	if strings.Contains(raw.EventType, "fill") || raw.Message.Type == "fill" {
		return "fill"
	}
	if strings.Contains(raw.EventType, "cancel") || raw.Message.Type == "cancel_order" {
		return "cancel"
	}
	return ""
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
