package executor

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"

	"orchestrator/internal/model"
)

func Score(run *model.BenchmarkRun, metrics *model.Metrics, validation *model.ValidationResult) (model.ScoreResult, error) {
	result := model.ScoreResult{
		RunID:           run.RunID,
		Valid:           validation != nil && validation.Valid,
		CorrectnessGate: "passed",
	}
	if !result.Valid {
		result.CorrectnessGate = "failed"
		if validation != nil {
			result.RunID = validation.RunID
			result.FailureReason = validation.Reason
		}
		if result.FailureReason == "" {
			result.FailureReason = "validation failed"
		}
		return writeScore(run, result)
	}
	if metrics == nil {
		result.Valid = false
		result.CorrectnessGate = "failed"
		result.FailureReason = "metrics missing"
		return writeScore(run, result)
	}
	if metrics.OrdersSent <= 0 {
		result.Valid = false
		result.CorrectnessGate = "failed"
		result.FailureReason = "no benchmark orders produced"
		return writeScore(run, result)
	}

	result.LatencyScore = latencyScore(metrics.P99MS)
	result.ThroughputScore = throughputScore(metrics.TPS, run.Config.BotCount*run.Config.RatePerBot)
	result.StabilityScore = stabilityScore(metrics.OrdersSent, metrics.Timeouts, metrics.ConnectErrors)
	result.ResourceScore = resourceScore(metrics.CPUPctPeak, metrics.MemMBPeak)
	// Completion gate (DYN-3): an engine that acks only a sliver of the offered
	// orders has a statistically meaningless latency sample. Below 50% completion
	// we scale the (40%-weighted) latency credit by the completion fraction so a
	// "1 ack, ignore the rest" engine can't bank near-full latency points.
	if completion := completionFraction(metrics.OrdersSent, metrics.Timeouts, metrics.ConnectErrors); completion < 0.5 {
		result.LatencyScore = round2(result.LatencyScore * completion)
	}
	result.Score = round2(
		0.40*result.LatencyScore +
			0.30*result.ThroughputScore +
			0.20*result.StabilityScore +
			0.10*result.ResourceScore,
	)
	return writeScore(run, result)
}

// completionFraction is the share of offered orders that completed without a
// timeout/connect error, clamped to [0,1]. ordersSent <= 0 -> 0 (no sample).
func completionFraction(ordersSent, timeouts, connectErrors int) float64 {
	if ordersSent <= 0 {
		return 0
	}
	c := 1 - float64(timeouts+connectErrors)/float64(ordersSent)
	if c < 0 {
		c = 0
	}
	if c > 1 {
		c = 1
	}
	return c
}

func writeScore(run *model.BenchmarkRun, score model.ScoreResult) (model.ScoreResult, error) {
	data, err := json.MarshalIndent(score, "", "  ")
	if err != nil {
		return score, err
	}
	data = append(data, '\n')
	return score, os.WriteFile(filepath.Join(run.ArtifactDir, "score.json"), data, 0o644)
}

// Latency curve bounds — full credit at/below the ~0.1ms in-contract floor,
// zero at/above 50ms, log-scaled between (latency perception is logarithmic and
// real engines cluster in the sub-5ms band, where the old flat `<=5ms -> 100`
// rule tied every engine). Identical to the Rust scorer
// (rust/bench-core/src/score/formula.rs) and the score-engine twin so all paths
// agree.
const (
	latencyFloorMS = 0.1
	latencyCapMS   = 50.0
)

func latencyScore(p99MS float64) float64 {
	if math.IsNaN(p99MS) || math.IsInf(p99MS, 0) {
		// Non-finite input (corrupt/poisoned metrics) -> no credit.
		return 0
	}
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
	if math.IsNaN(tps) || math.IsInf(tps, 0) {
		// Non-finite achieved throughput -> no credit.
		return 0
	}
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

// resourceScore is the exact twin of the Rust scorer's resource_efficiency_score
// (rust/bench-core/src/score/formula.rs) and the score-engine's resourceScore,
// so all scoring paths agree. cpuPct <= 0 means "not sampled" -> neutral 100.
// Soft linear penalty from 50% CPU / 512 MB; cpu capped at 100% first.
func resourceScore(cpuPct, memMB float64) float64 {
	if math.IsNaN(cpuPct) || math.IsInf(cpuPct, 0) {
		// Non-finite CPU sample -> treat as "not measured" -> neutral 100.
		return 100
	}
	if cpuPct <= 0 {
		return 100
	}
	cpu := math.Min(cpuPct, 100)
	mem := memMB
	if math.IsNaN(mem) || math.IsInf(mem, 0) {
		mem = 0
	}
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
