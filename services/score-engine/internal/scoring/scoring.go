package scoring

import "math"

func Score(req Request) ScoreResult {
	runID := req.RunID
	if runID == "" && req.Validation != nil {
		runID = req.Validation.RunID
	}
	if runID == "" && req.Metrics != nil {
		runID = req.Metrics.RunID
	}

	valid := req.Validation != nil && req.Validation.Valid
	result := ScoreResult{
		RunID:           runID,
		TeamID:          req.TeamID,
		Valid:           valid,
		CorrectnessGate: "passed",
	}

	if !valid {
		result.CorrectnessGate = "failed"
		if req.Validation != nil {
			result.FailureReason = req.Validation.Reason
		}
		if result.FailureReason == "" {
			result.FailureReason = "validation failed"
		}
		return result
	}

	metrics := req.Metrics
	if metrics == nil {
		result.Valid = false
		result.CorrectnessGate = "failed"
		result.FailureReason = "metrics missing"
		return result
	}
	if metrics.OrdersSent <= 0 {
		result.Valid = false
		result.CorrectnessGate = "failed"
		result.FailureReason = "no benchmark orders produced"
		return result
	}

	result.LatencyScore = latencyScore(metrics.P99MS)
	result.ThroughputScore = throughputScore(metrics.TPS, req.Config.BotCount*req.Config.RatePerBot)
	result.StabilityScore = stabilityScore(metrics.OrdersSent, metrics.Timeouts, metrics.ConnectErrors)
	result.ResourceScore = resourceScore(metrics.CPUPctPeak, metrics.MemMBPeak)
	result.Score = round2(
		0.40*result.LatencyScore +
			0.30*result.ThroughputScore +
			0.20*result.StabilityScore +
			0.10*result.ResourceScore,
	)
	return result
}

// Latency curve bounds — full credit at/below the ~0.1ms in-contract floor,
// zero at/above 50ms, log-scaled between (latency perception is logarithmic and
// real engines cluster in the sub-5ms band, where the old flat `<=5ms -> 100`
// rule tied every engine). Identical to the Rust scorer
// (rust/bench-core/src/score/formula.rs) and the orchestrator twin so all paths
// agree.
const (
	latencyFloorMS = 0.1
	latencyCapMS   = 50.0
)

func latencyScore(p99MS float64) float64 {
	if p99MS <= 0 {
		// No measured latency -> no credit, not the floor's 100 (which would
		// silently turn a parse/measurement failure into a perfect score).
		return 0
	}
	if p99MS <= latencyFloorMS {
		return 100
	}
	if p99MS >= latencyCapMS {
		return 0
	}
	num := math.Log10(latencyCapMS) - math.Log10(p99MS)
	den := math.Log10(latencyCapMS) - math.Log10(latencyFloorMS)
	return round2(100 * num / den)
}

func throughputScore(tps float64, expected int) float64 {
	if expected <= 0 {
		return 100
	}
	score := 100 * tps / float64(expected)
	if score > 100 {
		score = 100
	}
	if score < 0 {
		score = 0
	}
	return round2(score)
}

func stabilityScore(ordersSent, timeouts, connectErrors int) float64 {
	if ordersSent <= 0 {
		if connectErrors > 0 {
			return 0
		}
		return 100
	}
	score := 100 * (1 - float64(timeouts+connectErrors)/float64(ordersSent))
	if score < 0 {
		score = 0
	}
	return round2(score)
}

// resourceScore rewards engines that pass the correctness gate while using less
// CPU and memory. It is the exact twin of the Rust scorer's
// resource_efficiency_score (rust/bench-core/src/score/formula.rs) so the Go and
// Rust paths agree. cpuPct <= 0 means "not sampled" -> neutral 100 (the sandbox
// reports 0 when it couldn't measure). A soft linear penalty starts at 50% CPU
// and 512 MB: cpu is capped at 100% before penalising, so a busy single core
// caps the CPU penalty at (100-50)*1.5 = 75.
func resourceScore(cpuPct, memMB float64) float64 {
	if cpuPct <= 0 {
		return 100
	}
	cpu := math.Min(cpuPct, 100)
	mem := memMB
	if mem < 0 {
		mem = 0
	}
	cpuPenalty := math.Max(cpu-50, 0) * 1.5
	memPenalty := math.Max(mem-512, 0) * 0.05
	score := 100 - cpuPenalty - memPenalty
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return round2(score)
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}
