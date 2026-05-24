package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EngineExecutor interface {
	Start(ctx context.Context, r *BenchmarkRun) (endpoint string, cleanup func(context.Context) error, err error)
}

type BotFleetExecutor interface {
	Run(ctx context.Context, r *BenchmarkRun, endpoint string) (*Metrics, error)
}

type ValidatorExecutor interface {
	Run(ctx context.Context, r *BenchmarkRun) (*ValidationResult, error)
}

type Store interface {
	Save(ctx context.Context, r *BenchmarkRun) error
	Get(ctx context.Context, runID string) (*BenchmarkRun, error)
	List(ctx context.Context) ([]*BenchmarkRun, error)
}

type Manager struct {
	engine       EngineExecutor
	botfleet     BotFleetExecutor
	validator    ValidatorExecutor
	store        Store
	artifactRoot string

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func NewManager(engine EngineExecutor, botfleet BotFleetExecutor, validator ValidatorExecutor, store Store, artifactRoot string) *Manager {
	return &Manager{
		engine:       engine,
		botfleet:     botfleet,
		validator:    validator,
		store:        store,
		artifactRoot: artifactRoot,
		cancels:      make(map[string]context.CancelFunc),
	}
}

func (m *Manager) CreateRun(ctx context.Context, req RunRequest) (*BenchmarkRun, error) {
	normalized, err := NormalizeRequest(req)
	if err != nil {
		return nil, err
	}

	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	artifactDir := filepath.Join(m.artifactRoot, runID)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return nil, err
	}
	if err := createArtifactPlaceholders(artifactDir); err != nil {
		return nil, err
	}

	now := time.Now()
	r := &BenchmarkRun{
		RunID:        runID,
		TeamID:       normalized.TeamID,
		Status:       StatusQueued,
		EngineMode:   normalized.EngineMode,
		BotCount:     normalized.BotCount,
		OrdersPerSec: normalized.OrdersPerSec,
		DurationSec:  normalized.DurationSec,
		Seed:         normalized.Seed,
		ArtifactDir:  artifactDir,
		StartedAt:    now,
		UpdatedAt:    now,
	}

	if err := m.store.Save(ctx, r); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(artifactDir, "run_spec.json"), r); err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancels[runID] = cancel
	m.mu.Unlock()

	go m.execute(runCtx, r)

	return r, nil
}

func (m *Manager) CancelRun(ctx context.Context, runID string) (*BenchmarkRun, error) {
	r, err := m.store.Get(ctx, runID)
	if err != nil {
		return nil, err
	}
	if IsTerminal(r.Status) {
		return r, nil
	}

	m.mu.Lock()
	cancel := m.cancels[runID]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	finishedAt := time.Now()
	r.Status = StatusCancelled
	r.FinishedAt = &finishedAt
	r.UpdatedAt = finishedAt
	if err := m.store.Save(ctx, r); err != nil {
		return nil, err
	}
	_ = appendRunLog(r, "run cancelled")
	return r, nil
}

func (m *Manager) execute(ctx context.Context, r *BenchmarkRun) {
	defer func() {
		m.mu.Lock()
		delete(m.cancels, r.RunID)
		m.mu.Unlock()
	}()

	slog.Info("run started", "run_id", r.RunID)
	_ = appendRunLog(r, "run started")

	endpoint, cleanup, err := m.startEngine(ctx, r)
	if err != nil {
		m.failOrCancel(ctx, r, "STARTING_ENGINE", err)
		return
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			_ = appendRunLog(r, "engine cleanup failed: "+err.Error())
		}
	}()

	metrics, err := m.runBots(ctx, r, endpoint)
	if err != nil {
		m.failOrCancel(ctx, r, "BENCHMARKING", err)
		return
	}
	r.Metrics = metrics
	if err := m.save(ctx, r); err != nil {
		m.failOrCancel(ctx, r, "SAVING_METRICS", err)
		return
	}

	validation, err := m.runValidator(ctx, r)
	if err != nil {
		m.failOrCancel(ctx, r, "VALIDATING", err)
		return
	}
	r.Validation = validation
	r.Valid = &validation.Valid

	r.Status = StatusScoring
	if err := m.save(ctx, r); err != nil {
		m.failOrCancel(ctx, r, "SCORING", err)
		return
	}

	score := CalculateScore(r)
	r.Score = score.Score
	if err := writeJSON(filepath.Join(r.ArtifactDir, "score.json"), score); err != nil {
		m.failOrCancel(ctx, r, "SCORING", err)
		return
	}

	finishedAt := time.Now()
	r.Status = StatusFinished
	r.FinishedAt = &finishedAt
	r.UpdatedAt = finishedAt
	_ = m.store.Save(ctx, r)
	_ = writeJSON(filepath.Join(r.ArtifactDir, "run_spec.json"), r)
	_ = appendRunLog(r, "run finished")
	slog.Info("run finished", "run_id", r.RunID, "valid", validation.Valid, "score", r.Score)
}

func (m *Manager) startEngine(ctx context.Context, r *BenchmarkRun) (string, func(context.Context) error, error) {
	r.Status = StatusStarting
	if err := m.save(ctx, r); err != nil {
		return "", nil, err
	}
	endpoint, cleanup, err := m.engine.Start(ctx, r)
	if err != nil {
		return "", nil, err
	}

	r.Status = StatusHealthCheck
	if err := m.save(ctx, r); err != nil {
		return "", cleanup, err
	}
	return endpoint, cleanup, nil
}

func (m *Manager) runBots(ctx context.Context, r *BenchmarkRun, endpoint string) (*Metrics, error) {
	r.Status = StatusBenchmarking
	if err := m.save(ctx, r); err != nil {
		return nil, err
	}
	return m.botfleet.Run(ctx, r, endpoint)
}

func (m *Manager) runValidator(ctx context.Context, r *BenchmarkRun) (*ValidationResult, error) {
	r.Status = StatusValidating
	if err := m.save(ctx, r); err != nil {
		return nil, err
	}
	return m.validator.Run(ctx, r)
}

func (m *Manager) failOrCancel(ctx context.Context, r *BenchmarkRun, stage string, err error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		r.Status = StatusCancelled
		r.FailureStage = stage
		r.FailureReason = "cancelled"
	} else {
		r.Status = StatusFailed
		r.FailureStage = stage
		r.FailureReason = err.Error()
	}
	finishedAt := time.Now()
	r.FinishedAt = &finishedAt
	r.UpdatedAt = finishedAt
	_ = m.store.Save(context.Background(), r)
	_ = writeJSON(filepath.Join(r.ArtifactDir, "run_spec.json"), r)
	_ = appendRunLog(r, fmt.Sprintf("run stopped at %s: %s", stage, r.FailureReason))

	slog.Error("run stopped", "run_id", r.RunID, "status", r.Status, "stage", stage, "err", err)
}

func (m *Manager) save(ctx context.Context, r *BenchmarkRun) error {
	r.UpdatedAt = time.Now()
	if err := m.store.Save(ctx, r); err != nil {
		return err
	}
	return writeJSON(filepath.Join(r.ArtifactDir, "run_spec.json"), r)
}

func createArtifactPlaceholders(artifactDir string) error {
	names := []string{
		"engine_outputs.jsonl",
		"events.jsonl",
		"contestant_outputs.jsonl",
		"metrics.json",
		"validation.json",
		"score.json",
		"run.log",
	}
	for _, name := range names {
		path := filepath.Join(artifactDir, name)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if file != nil {
			if err := file.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendRunLog(r *BenchmarkRun, message string) error {
	path := filepath.Join(r.ArtifactDir, "run.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%s %s\n", time.Now().Format(time.RFC3339), message)
	return err
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
