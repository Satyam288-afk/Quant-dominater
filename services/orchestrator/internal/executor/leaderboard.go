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
}

type leaderboardEntry struct {
	RunID         string    `json:"run_id"`
	TeamID        string    `json:"team_id"`
	Score         float64   `json:"score"`
	Valid         bool      `json:"valid"`
	Status        string    `json:"status,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	P50MS         float64   `json:"p50_ms,omitempty"`
	P90MS         float64   `json:"p90_ms,omitempty"`
	P99MS         float64   `json:"p99_ms,omitempty"`
	TPS           float64   `json:"tps,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func NewLeaderboardPublisher(baseURL string) *LeaderboardPublisher {
	return &LeaderboardPublisher{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (p *LeaderboardPublisher) Publish(ctx context.Context, run *model.BenchmarkRun, metrics *model.Metrics, validation *model.ValidationResult, score model.ScoreResult) error {
	if p == nil || p.baseURL == "" {
		return nil
	}
	entry := leaderboardEntry{
		RunID:     run.RunID,
		TeamID:    run.TeamID,
		Score:     score.Score,
		Valid:     score.Valid,
		Status:    string(run.Status),
		UpdatedAt: time.Now(),
	}
	if validation != nil {
		entry.FailureReason = validation.Reason
	}
	if run.FailureReason != "" {
		entry.FailureReason = run.FailureReason
	}
	if metrics != nil {
		entry.P50MS = metrics.P50MS
		entry.P90MS = metrics.P90MS
		entry.P99MS = metrics.P99MS
		entry.TPS = metrics.TPS
	}

	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/leaderboard/runs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

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
