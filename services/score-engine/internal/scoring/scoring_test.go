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

// resourceScore must match the Rust scorer's resource_efficiency_score exactly
// (rust/bench-core/src/score/formula.rs) so the Go and Rust paths never diverge.
func TestResourceScoreMatchesRustCurve(t *testing.T) {
	cases := []struct {
		cpu, mem, want float64
	}{
		{0, 999, 100},  // not sampled -> neutral
		{40, 100, 100}, // under both knees
		{50, 512, 100}, // exactly at the knees
		{70, 512, 70},  // 100 - (70-50)*1.5
		{100, 1012, 0}, // 100 - 75 - (500*0.05=25)
		{250, 0, 25},   // cpu capped at 100 -> 100 - 75
		{60, 1012, 60}, // 100 - (60-50)*1.5=15 - 25 = 60
	}
	for _, c := range cases {
		if got := resourceScore(c.cpu, c.mem); got != c.want {
			t.Fatalf("resourceScore(%.0f, %.0f) = %v, want %v", c.cpu, c.mem, got, c.want)
		}
	}
}

// A sampled resource cost must actually move the final score (no longer a stub).
func TestScoreUsesSampledResource(t *testing.T) {
	req := Request{
		RunID:      "run_1",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 2},
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 100, Timeouts: 0, ConnectErrors: 0,
			TPS: 20, P99MS: 2, // perfect latency/throughput/stability
			CPUPctPeak: 70, MemMBPeak: 512,
		},
	}
	got := Score(req)
	if got.ResourceScore != 70 {
		t.Fatalf("resource score = %v, want 70", got.ResourceScore)
	}
	// 0.40*100 + 0.30*100 + 0.20*100 + 0.10*70 = 97
	if got.Score != 97 {
		t.Fatalf("final score = %v, want 97 (resource term now real)", got.Score)
	}
}
