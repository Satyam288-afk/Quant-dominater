package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"orchestrator/internal/model"
)

func TestValidateBenchmarkLoadRejectsNoOrders(t *testing.T) {
	err := validateBenchmarkLoad(&model.Metrics{ConnectErrors: 10})
	if err == nil {
		t.Fatal("expected no-order benchmark to be rejected")
	}
	if !strings.Contains(err.Error(), "no orders") {
		t.Fatalf("error = %q, want no orders", err.Error())
	}
}

func TestValidateBenchmarkLoadAllowsOrdersWithFailures(t *testing.T) {
	err := validateBenchmarkLoad(&model.Metrics{
		OrdersSent:    100,
		ConnectErrors: 2,
	})
	if err != nil {
		t.Fatalf("validateBenchmarkLoad() error = %v", err)
	}
}

func TestBotFleetUsesPackagedBinary(t *testing.T) {
	repoRoot := t.TempDir()
	run := &model.BenchmarkRun{
		RunID:         "run_1",
		ArtifactDir:   filepath.Join(t.TempDir(), "run_1"),
		BenchmarkSeed: 42,
		Config: model.BenchmarkRunConfig{
			BotCount:    1,
			RatePerBot:  1,
			DurationSec: 1,
		},
	}
	if err := os.MkdirAll(run.ArtifactDir, 0o755); err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "bot-fleet")
	if err := os.WriteFile(bin, []byte(`#!/bin/sh
set -eu
events=""
outputs=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --events-out) events="$2"; shift 2 ;;
    --outputs-out) outputs="$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '{"event_type":"order"}\n' > "$events"
printf '{"event_type":"ack","message":{"type":"ack"}}\n' > "$outputs"
cat <<'OUT'
run_id: run_1
bots: 1
orders_sent: 1
acks_received: 1
fills_received: 0
timeouts: 0
tps: 1
p50: 1ms
p90: 1ms
p99: 1ms
OUT
`), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BOT_FLEET_BIN", bin)

	metrics, err := NewBotFleet(repoRoot).Run(context.Background(), run, "ws://127.0.0.1:8080/ws")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.OrdersSent != 1 || metrics.AcksReceived != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if _, err := os.Stat(filepath.Join(run.ArtifactDir, "acks.jsonl")); err != nil {
		t.Fatalf("expected split ack artifact: %v", err)
	}
}
