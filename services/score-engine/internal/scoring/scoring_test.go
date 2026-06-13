package scoring

import (
	"math"
	"testing"
)

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
			P99MS:         5.0,
		},
	})

	// p99=5ms on the log curve -> 100*log10(50/5)/log10(50/0.1) = 37.05.
	if got.LatencyScore != 37.05 {
		t.Fatalf("latency score = %v, want 37.05", got.LatencyScore)
	}
	if got.ThroughputScore != 50 {
		t.Fatalf("throughput score = %v, want 50", got.ThroughputScore)
	}
	if got.StabilityScore != 90 {
		t.Fatalf("stability score = %v, want 90", got.StabilityScore)
	}
	// 0.40*37.05 + 0.30*50 + 0.20*90 + 0.10*100 = 57.82. This also pins the
	// 0.40/0.30/0.20/0.10 weights (a 0.40->0.04 typo would fail here).
	if got.Score != 57.82 {
		t.Fatalf("score = %v, want 57.82", got.Score)
	}
}

func TestLatencyScoreLogCurve(t *testing.T) {
	cases := []struct{ p99, want float64 }{
		{0, 0},      // no measurement -> no credit (not a silent perfect score)
		{0.05, 100}, // below the 0.1ms floor
		{0.1, 100},  // at the floor
		{5.0, 37.05},
		{50, 0},  // at the cap
		{100, 0}, // beyond the cap
	}
	for _, c := range cases {
		if got := latencyScore(c.p99); got != c.want {
			t.Fatalf("latencyScore(%.2f) = %v, want %v", c.p99, got, c.want)
		}
	}
	// Strictly decreasing across the sub-5ms band the old flat rule tied at 100.
	if !(latencyScore(0.5) > latencyScore(1) && latencyScore(1) > latencyScore(2) && latencyScore(2) > latencyScore(5)) {
		t.Fatal("latency curve must strictly decrease across 0.5->5ms")
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

// DYN-2: non-finite inputs (NaN/Inf) must never produce a credit. p99=NaN ->
// latency 0, tps=NaN -> throughput 0, cpu=NaN -> resource neutral 100, mem=NaN
// -> treated as 0 (no penalty). Guards corrupt/poisoned metrics.
func TestNaNAndInfGuards(t *testing.T) {
	nan := math.NaN()
	inf := math.Inf(1)
	if got := latencyScore(nan); got != 0 {
		t.Fatalf("latencyScore(NaN) = %v, want 0", got)
	}
	if got := latencyScore(inf); got != 0 {
		t.Fatalf("latencyScore(+Inf) = %v, want 0", got)
	}
	if got := throughputScore(nan, 100); got != 0 {
		t.Fatalf("throughputScore(NaN) = %v, want 0", got)
	}
	if got := throughputScore(inf, 100); got != 0 {
		t.Fatalf("throughputScore(+Inf) = %v, want 0", got)
	}
	if got := resourceScore(nan, 512); got != 100 {
		t.Fatalf("resourceScore(cpu=NaN) = %v, want 100 (not measured)", got)
	}
	// mem=NaN must be treated as 0 -> no penalty, full 100 when cpu under knee.
	if got := resourceScore(40, nan); got != 100 {
		t.Fatalf("resourceScore(cpu=40, mem=NaN) = %v, want 100", got)
	}
}

// DYN-2 end-to-end: a poisoned p99 (NaN) drops the whole latency term, so the
// composite cannot inherit a NaN nor bank latency credit.
func TestScoreGuardsNaNLatency(t *testing.T) {
	got := Score(Request{
		RunID:      "run_nan",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 10}, // expected 100
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 100, Timeouts: 0, ConnectErrors: 0,
			TPS: 100, P99MS: math.NaN(),
		},
	})
	if got.LatencyScore != 0 {
		t.Fatalf("latency score = %v, want 0 for NaN p99", got.LatencyScore)
	}
	if math.IsNaN(got.Score) {
		t.Fatal("final score is NaN; NaN must not propagate into the composite")
	}
	// 0.40*0 + 0.30*100 + 0.20*100 + 0.10*100 = 60
	if got.Score != 60 {
		t.Fatalf("final score = %v, want 60", got.Score)
	}
}

// DYN-3: a "1 ack, ignore the rest" engine (1% completion) must not bank near
// the full 40% latency credit. With a floor-level p99 the raw latency is 100,
// but the continuous ramp factor = min(1, 0.01/0.5) = 0.02 scales it to
// round2(100*0.02)=2, so the final is ~11.3, nowhere near the ~40+ ungated.
func TestCompletionGateScalesLatency(t *testing.T) {
	got := Score(Request{
		RunID:      "run_gate",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 10}, // expected 100
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 100, Timeouts: 99, ConnectErrors: 0,
			TPS: 1, P99MS: 0.05, // floor-level latency -> raw 100 before the gate
		},
	})
	// completion 0.01 -> factor min(1, 0.01/0.5)=0.02 -> latency round2(100*0.02)=2.
	if got.LatencyScore != 2 {
		t.Fatalf("gated latency score = %v, want 2 (100 * 0.02 ramp factor)", got.LatencyScore)
	}
	// 0.40*2 + 0.30*1 (tps 1/100) + 0.20*1 (stability) + 0.10*100 (resource) = 11.3.
	if got.Score != 11.3 {
		t.Fatalf("final score = %v, want 11.3 (latency gated, not banked)", got.Score)
	}
	if got.Score == 50 {
		t.Fatal("a one-percent-completion engine must not score 50")
	}
}

// DYN-3 continuity: the old hard gate produced a ~20-point final-score cliff
// between completion 0.5 and 0.499 (latency kept 100 at 0.5, dropped to ~49.9
// just below). The continuous ramp must make 0.5 and 0.499 give nearly-equal
// final scores so the gate can no longer invert rankings.
func TestCompletionGateContinuousAtHalf(t *testing.T) {
	// 1000 orders: 500 timeouts -> completion 0.500; 501 -> completion 0.499.
	atHalf := Score(Request{
		RunID:      "run_half",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 10},
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 1000, Timeouts: 500, ConnectErrors: 0,
			TPS: 100, P99MS: 0.05,
		},
	})
	justBelow := Score(Request{
		RunID:      "run_below",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 10},
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 1000, Timeouts: 501, ConnectErrors: 0,
			TPS: 100, P99MS: 0.05,
		},
	})
	if d := math.Abs(atHalf.Score - justBelow.Score); d >= 0.1 {
		t.Fatalf("expected continuity at completion=0.5: %v vs %v (diff %v)", atHalf.Score, justBelow.Score, d)
	}
}

// A healthy engine (completion >= 0.5) must be unchanged by the gate.
func TestCompletionGateLeavesHealthyEngineUnchanged(t *testing.T) {
	got := Score(Request{
		RunID:      "run_ok",
		Config:     BenchmarkRunConfig{BotCount: 10, RatePerBot: 10},
		Validation: &ValidationResult{Valid: true},
		Metrics: &Metrics{
			OrdersSent: 100, Timeouts: 10, ConnectErrors: 0, // 90% completion
			TPS: 100, P99MS: 0.05,
		},
	})
	if got.LatencyScore != 100 {
		t.Fatalf("latency score = %v, want 100 (gate must not touch healthy engines)", got.LatencyScore)
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
			TPS: 20, P99MS: 0.05, // floor-level latency -> 100; perfect throughput/stability
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
