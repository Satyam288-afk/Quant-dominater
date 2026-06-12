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

// parseMetrics turns the bot-fleet's stdout summary into model.Metrics that
// feed the scorer. A silent format drift here (a renamed key, a new line, a
// changed "0.47ms" format) would zero a field and mis-score every run, so pin
// the whole mapping against a captured-format fixture — including lines the
// scorer does NOT parse (loop_mode, reconnects) to prove unknown keys are
// ignored, not mishandled.
func TestParseMetricsGolden(t *testing.T) {
	const out = `run_id: r1
bots: 24
loop_mode: open
orders_sent: 624
acks_received: 624
fills_received: 412
timeouts: 0
connect_errors: 0
reconnects: 0
tps: 124.8
peak_tps: 140
p50: 0.26ms
p90: 0.38ms
p99: 0.47ms
events_out: events.jsonl
outputs_out: contestant_outputs.jsonl`

	m := parseMetrics(out)
	if m.RunID != "r1" || m.Bots != 24 {
		t.Fatalf("run_id/bots = %q/%d", m.RunID, m.Bots)
	}
	if m.OrdersSent != 624 || m.AcksReceived != 624 || m.FillsReceived != 412 {
		t.Fatalf("orders/acks/fills = %d/%d/%d", m.OrdersSent, m.AcksReceived, m.FillsReceived)
	}
	if m.Timeouts != 0 || m.ConnectErrors != 0 {
		t.Fatalf("timeouts/connect_errors = %d/%d", m.Timeouts, m.ConnectErrors)
	}
	if m.TPS != 124.8 || m.PeakTPS != 140 {
		t.Fatalf("tps/peak_tps = %v/%v", m.TPS, m.PeakTPS)
	}
	if m.P50MS != 0.26 || m.P90MS != 0.38 || m.P99MS != 0.47 {
		t.Fatalf("p50/p90/p99 = %v/%v/%v", m.P50MS, m.P90MS, m.P99MS)
	}
	if m.EventsOut != "events.jsonl" || m.OutputsOut != "contestant_outputs.jsonl" {
		t.Fatalf("events/outputs = %q/%q", m.EventsOut, m.OutputsOut)
	}
}

func TestParseMSHandlesMissing(t *testing.T) {
	if got := parseMS("n/a"); got != 0 {
		t.Fatalf("parseMS(n/a) = %v, want 0", got)
	}
	if got := parseMS(""); got != 0 {
		t.Fatalf("parseMS(empty) = %v, want 0", got)
	}
	if got := parseMS("1.23ms"); got != 1.23 {
		t.Fatalf("parseMS(1.23ms) = %v, want 1.23", got)
	}
}

func TestOutputKindBranches(t *testing.T) {
	cases := []struct{ line, want string }{
		{`{"message":{"type":"ack"}}`, "ack"},
		{`{"message":{"type":"ack","status":"canceled"}}`, "cancel"}, // spelling is load-bearing
		{`{"message":{"type":"fill"}}`, "fill"},
		{`{"event_type":"fill_received"}`, "fill"},
		{`{"message":{"type":"cancel_order"}}`, "cancel"},
		{`not json`, ""},
	}
	for _, c := range cases {
		if got := outputKind([]byte(c.line)); got != c.want {
			t.Fatalf("outputKind(%s) = %q, want %q", c.line, got, c.want)
		}
	}
}
