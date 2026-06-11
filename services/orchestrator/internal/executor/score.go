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
	result.ResourceScore = 100
	result.Score = round2(
		0.40*result.LatencyScore +
			0.30*result.ThroughputScore +
			0.20*result.StabilityScore +
			0.10*result.ResourceScore,
	)
	return writeScore(run, result)
}

func writeScore(run *model.BenchmarkRun, score model.ScoreResult) (model.ScoreResult, error) {
	data, err := json.MarshalIndent(score, "", "  ")
	if err != nil {
		return score, err
	}
	data = append(data, '\n')
	return score, os.WriteFile(filepath.Join(run.ArtifactDir, "score.json"), data, 0o644)
}

func latencyScore(p99MS float64) float64 {
	if p99MS <= 0 || p99MS <= 5 {
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
	score := 100 * (1 - float64(timeouts+connectErrors)/float64(ordersSent))
	if score < 0 {
		score = 0
	}
	return round2(score)
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}
