package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"orchestrator/internal/model"
)

type LeaderboardPublisher struct {
	baseURL string
	client  *http.Client
	token   string
}

type leaderboardEntry struct {
	RunID           string    `json:"run_id"`
	TeamID          string    `json:"team_id"`
	Score           float64   `json:"score"`
	Valid           bool      `json:"valid"`
	Status          string    `json:"status,omitempty"`
	FailureReason   string    `json:"failure_reason,omitempty"`
	OrdersSent      int       `json:"orders_sent,omitempty"`
	AcksReceived    int       `json:"acks_received,omitempty"`
	FillsReceived   int       `json:"fills_received,omitempty"`
	Timeouts        int       `json:"timeouts,omitempty"`
	ConnectErrors   int       `json:"connect_errors,omitempty"`
	FillsChecked    int       `json:"fills_checked,omitempty"`
	P50MS           float64   `json:"p50_ms,omitempty"`
	P90MS           float64   `json:"p90_ms,omitempty"`
	P99MS           float64   `json:"p99_ms,omitempty"`
	TPS             float64   `json:"tps,omitempty"`
	PeakTPS         float64   `json:"peak_tps,omitempty"`
	LatencyScore    float64   `json:"latency_score,omitempty"`
	ThroughputScore float64   `json:"throughput_score,omitempty"`
	StabilityScore  float64   `json:"stability_score,omitempty"`
	ResourceScore   float64   `json:"resource_score,omitempty"`
	CorrectnessGate string    `json:"correctness_gate,omitempty"`
	ArtifactDir     string    `json:"artifact_dir,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func NewLeaderboardPublisher(baseURL string) *LeaderboardPublisher {
	return &LeaderboardPublisher{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
		token:   firstEnv("LEADERBOARD_AUTH_TOKEN", "SERVICE_AUTH_TOKEN"),
	}
}

func (p *LeaderboardPublisher) Publish(ctx context.Context, run *model.BenchmarkRun, metrics *model.Metrics, validation *model.ValidationResult, score model.ScoreResult) error {
	if p == nil || p.baseURL == "" {
		return nil
	}
	entry := leaderboardEntry{
		RunID:       run.RunID,
		TeamID:      run.TeamID,
		Score:       score.Score,
		Valid:       score.Valid,
		Status:      string(run.Status),
		ArtifactDir: run.ArtifactDir,
		UpdatedAt:   time.Now(),
	}
	if validation != nil {
		entry.FailureReason = validation.Reason
		entry.FillsChecked = validation.FillsChecked
	}
	if run.FailureReason != "" {
		entry.FailureReason = run.FailureReason
	}
	if metrics != nil {
		entry.OrdersSent = metrics.OrdersSent
		entry.AcksReceived = metrics.AcksReceived
		entry.FillsReceived = metrics.FillsReceived
		entry.Timeouts = metrics.Timeouts
		entry.ConnectErrors = metrics.ConnectErrors
		entry.P50MS = metrics.P50MS
		entry.P90MS = metrics.P90MS
		entry.P99MS = metrics.P99MS
		entry.TPS = metrics.TPS
		entry.PeakTPS = metrics.PeakTPS
		if entry.PeakTPS == 0 {
			entry.PeakTPS = metrics.TPS
		}
	}
	entry.LatencyScore = score.LatencyScore
	entry.ThroughputScore = score.ThroughputScore
	entry.StabilityScore = score.StabilityScore
	entry.ResourceScore = score.ResourceScore
	entry.CorrectnessGate = score.CorrectnessGate

	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/leaderboard/runs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("leaderboard returned %s", resp.Status)
	}
	return nil
}
