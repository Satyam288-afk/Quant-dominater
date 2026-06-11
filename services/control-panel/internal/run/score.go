package run

import "math"

func CalculateScore(r *BenchmarkRun) ScoreResult {
	valid := r.Valid != nil && *r.Valid
	result := ScoreResult{
		RunID:           r.RunID,
		Valid:           valid,
		CorrectnessGate: "passed",
	}

	if !valid {
		result.CorrectnessGate = "failed"
		if r.Validation != nil {
			result.FailureReason = r.Validation.Reason
		}
		if result.FailureReason == "" {
			result.FailureReason = "validation failed"
		}
		return result
	}

	metrics := r.Metrics
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
	result.ThroughputScore = throughputScore(metrics.TPS, r.BotCount*r.OrdersPerSec)
	result.StabilityScore = stabilityScore(metrics.OrdersSent, metrics.Timeouts, metrics.ConnectErrors)
	result.ResourceScore = 100
	result.Score = round2(
		0.40*result.LatencyScore +
			0.30*result.ThroughputScore +
			0.20*result.StabilityScore +
			0.10*result.ResourceScore,
	)

	return result
}

func latencyScore(p99MS float64) float64 {
	if p99MS <= 0 {
		return 100
	}
	if p99MS <= 5 {
		return 100
	}
	if p99MS >= 100 {
		return 0
	}
	return round2(100 * (100 - p99MS) / 95)
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
	failed := timeouts + connectErrors
	score := 100 * (1 - float64(failed)/float64(ordersSent))
	if score < 0 {
		score = 0
	}
	return round2(score)
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}
