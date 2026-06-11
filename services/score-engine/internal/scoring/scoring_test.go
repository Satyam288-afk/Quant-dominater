package scoring

import "testing"

func TestScoreCorrectnessGate(t *testing.T) {
	got := Score(Request{
		RunID: "run_1",
		Validation: &ValidationResult{
			Valid:  false,
			Reason: "PRICE_TIME_PRIORITY_VIOLATION",
		},
		Metrics: &Metrics{P99MS: 1, TPS: 100},
	})

	if got.Score != 0 {
		t.Fatalf("score = %v, want 0", got.Score)
	}
	if got.CorrectnessGate != "failed" {
		t.Fatalf("correctness gate = %q", got.CorrectnessGate)
	}
	if got.FailureReason != "PRICE_TIME_PRIORITY_VIOLATION" {
		t.Fatalf("failure reason = %q", got.FailureReason)
	}
}

func TestScoreUsesWeightedFormula(t *testing.T) {
	got := Score(Request{
		RunID:      "run_1",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 2},
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent:    100,
			Timeouts:      10,
			ConnectErrors: 0,
			TPS:           10,
			P99MS:         52.5,
		},
	})

	if got.LatencyScore != 50 {
		t.Fatalf("latency score = %v, want 50", got.LatencyScore)
	}
	if got.ThroughputScore != 50 {
		t.Fatalf("throughput score = %v, want 50", got.ThroughputScore)
	}
	if got.StabilityScore != 90 {
		t.Fatalf("stability score = %v, want 90", got.StabilityScore)
	}
	if got.Score != 63 {
		t.Fatalf("score = %v, want 63", got.Score)
	}
}

func TestScoreRejectsValidButEmptyBenchmark(t *testing.T) {
	got := Score(Request{
		RunID:      "run_1",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 2},
		Validation: &ValidationResult{Valid: true},
		Metrics:    &Metrics{},
	})

	if got.Valid {
		t.Fatal("score should not remain valid when no benchmark orders were produced")
	}
	if got.Score != 0 {
		t.Fatalf("score = %v, want 0", got.Score)
	}
	if got.FailureReason != "no benchmark orders produced" {
		t.Fatalf("failure reason = %q", got.FailureReason)
	}
}
