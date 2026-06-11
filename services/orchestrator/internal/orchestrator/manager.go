package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"orchestrator/internal/executor"
	"orchestrator/internal/model"
	"orchestrator/internal/store"
)

type Store interface {
	GetSubmission(ctx context.Context, submissionID string) (*model.Submission, error)
	GetRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	ListRuns(ctx context.Context) ([]*model.BenchmarkRun, error)
	SaveRun(ctx context.Context, run *model.BenchmarkRun) error
	ClaimRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	ClaimNextQueuedRun(ctx context.Context) (*model.BenchmarkRun, error)
}

type Engine interface {
	Build(ctx context.Context, run *model.BenchmarkRun, submission *model.Submission) error
	Start(ctx context.Context, run *model.BenchmarkRun) (executor.EngineHandle, error)
}

type BotFleet interface {
	Run(ctx context.Context, run *model.BenchmarkRun, endpoint string) (*model.Metrics, error)
}

type Validator interface {
	Run(ctx context.Context, run *model.BenchmarkRun) (*model.ValidationResult, error)
}

type LeaderboardPublisher interface {
	Publish(ctx context.Context, run *model.BenchmarkRun, metrics *model.Metrics, validation *model.ValidationResult, score model.ScoreResult) error
}

type Manager struct {
	store      Store
	engine     Engine
	botfleet   BotFleet
	validator  Validator
	publisher  LeaderboardPublisher
	runRoot    string
	runTimeout time.Duration

	mu      sync.Mutex
	cancels map[string]context.CancelFunc

	// sem bounds how many runs build + start a sandbox concurrently. The fleet
	// scales out, but every run funnels a docker build + container start through
	// this single orchestrator; without a cap a burst of K queued submissions
	// forks K simultaneous compiles + containers demanding 2K exclusive cores
	// with no queue (Pending-pod pileup / OOMKilled builds). Acquired before a
	// run is claimed, released when its execute goroutine returns.
	sem chan struct{}
}

func NewManager(store Store, engine Engine, botfleet BotFleet, validator Validator, runRoot string, runTimeout time.Duration) *Manager {
	if runTimeout <= 0 {
		runTimeout = 3 * time.Minute
	}
	return &Manager{
		store:      store,
		engine:     engine,
		botfleet:   botfleet,
		validator:  validator,
		runRoot:    runRoot,
		runTimeout: runTimeout,
		cancels:    make(map[string]context.CancelFunc),
		sem:        make(chan struct{}, buildConcurrency()),
	}
}

// buildConcurrency caps simultaneous build+run pipelines. Defaults to the host
// CPU count, overridable with ORCHESTRATOR_BUILD_CONCURRENCY for nodes whose
// real budget differs from their visible cores. Floored at 1.
func buildConcurrency() int {
	n := runtime.NumCPU()
	if v := os.Getenv("ORCHESTRATOR_BUILD_CONCURRENCY"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

func (m *Manager) SetLeaderboardPublisher(publisher LeaderboardPublisher) {
	m.publisher = publisher
}

func (m *Manager) StartRun(ctx context.Context, runID string) (*model.BenchmarkRun, error) {
	// Acquire a build slot before claiming so a cancel while we wait leaves the
	// run untouched (QUEUED) instead of stranded mid-BUILDING.
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	run, err := m.store.ClaimRun(ctx, runID)
	if err != nil {
		<-m.sem
		return run, err
	}
	if model.Terminal(run.Status) {
		<-m.sem
		return run, nil
	}

	runCtx, cancel := context.WithTimeout(context.Background(), m.runTimeout)
	m.mu.Lock()
	m.cancels[run.RunID] = cancel
	m.mu.Unlock()

	go m.execute(runCtx, run)
	return run, nil
}

func (m *Manager) StartNextQueued(ctx context.Context) (*model.BenchmarkRun, error) {
	// Block on a build slot first: when the pool is full the worker parks here
	// (cheap) instead of claiming more runs into BUILDING than it can execute.
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	run, err := m.store.ClaimNextQueuedRun(ctx)
	if err != nil {
		<-m.sem
		return nil, err
	}
	runCtx, cancel := context.WithTimeout(context.Background(), m.runTimeout)
	m.mu.Lock()
	m.cancels[run.RunID] = cancel
	m.mu.Unlock()

	go m.execute(runCtx, run)
	return run, nil
}

func (m *Manager) StartWorker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			m.claimAvailable(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (m *Manager) claimAvailable(ctx context.Context) {
	for {
		run, err := m.StartNextQueued(ctx)
		if err == nil {
			slog.Info("orchestrator worker claimed run", "run_id", run.RunID)
			continue
		}
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.Error("orchestrator worker failed to claim run", "err", err)
		return
	}
}

func (m *Manager) CancelRun(ctx context.Context, runID string) (*model.BenchmarkRun, error) {
	run, err := m.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if model.Terminal(run.Status) {
		return run, nil
	}
	m.mu.Lock()
	cancel := m.cancels[runID]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	now := time.Now()
	run.Status = model.RunStatusCancelled
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
	run.FinishedAt = &now
	run.FinishedAtUnix = now.Unix()
	_ = m.store.SaveRun(ctx, run)
	return run, nil
}

// Shutdown cancels every in-flight run so its goroutine unwinds and persists a
// terminal (cancelled) state instead of being orphaned past process exit, then
// waits — bounded by ctx — for those goroutines to drain. Run contexts are
// rooted at context.Background() (so a transient worker hiccup can't kill a
// healthy run), which is exactly why they need an explicit cancel on SIGTERM:
// without this, a `kill` mid-run leaves a goroutine (and its child engine
// process) running detached and the run wedged mid-"building" on disk.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	inflight := len(m.cancels)
	for _, cancel := range m.cancels {
		cancel()
	}
	m.mu.Unlock()
	if inflight == 0 {
		return
	}
	slog.Info("orchestrator shutdown: cancelling in-flight runs", "count", inflight)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		remaining := len(m.cancels)
		m.mu.Unlock()
		if remaining == 0 {
			slog.Info("orchestrator shutdown: all runs drained")
			return
		}
		select {
		case <-ctx.Done():
			slog.Warn("orchestrator shutdown grace elapsed with runs still draining", "remaining", remaining)
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) BenchmarkEndpoint(ctx context.Context, req model.DirectBenchmarkRequest) (*model.DirectBenchmarkResult, error) {
	endpoint := strings.TrimSpace(req.EndpointURL)
	if endpoint == "" {
		return nil, errors.New("endpoint_url is required")
	}
	if req.BenchmarkSeed == 0 {
		req.BenchmarkSeed = 42
	}
	normalizeConfig(&req.Config)

	timeout := m.runTimeout
	if req.TimeoutSec > 0 {
		timeout = time.Duration(req.TimeoutSec) * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	now := time.Now()
	run := &model.BenchmarkRun{
		RunID:         fmt.Sprintf("direct_%d", now.UnixNano()),
		TeamID:        "direct",
		Status:        model.RunStatusBenchmarking,
		BenchmarkSeed: req.BenchmarkSeed,
		Config:        req.Config,
		CreatedAtUnix: now.Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	run.Config.EngineEndpoint = endpoint
	run.ArtifactDir = filepath.Join(m.runRoot, run.RunID)
	if err := os.MkdirAll(run.ArtifactDir, 0o755); err != nil {
		return nil, err
	}
	if err := createArtifactPlaceholders(run.ArtifactDir); err != nil {
		return nil, err
	}
	if err := m.writeRunSpec(run); err != nil {
		return nil, err
	}
	if err := writeJSONFile(filepath.Join(run.ArtifactDir, "config.json"), req); err != nil {
		return nil, err
	}
	_ = appendRunLog(run, "direct benchmark started")

	metrics, err := m.botfleet.Run(runCtx, run, endpoint)
	if err != nil {
		return m.finishDirectFailure(runCtx, run, "BENCHMARKING", err, metrics, nil, nil), nil
	}
	mergeResourceSample(run.ArtifactDir, metrics)

	store.Touch(run, model.RunStatusValidating)
	_ = m.writeRunSpec(run)
	validation, err := m.validator.Run(runCtx, run)
	if err != nil {
		return m.finishDirectFailure(runCtx, run, "VALIDATING", err, metrics, validation, nil), nil
	}
	run.Valid = &validation.Valid

	store.Touch(run, model.RunStatusScoring)
	_ = m.writeRunSpec(run)
	score, err := executor.Score(run, metrics, validation)
	if err != nil {
		return m.finishDirectFailure(runCtx, run, "SCORING", err, metrics, validation, nil), nil
	}
	run.Score = score.Score

	now = time.Now()
	run.Status = model.RunStatusFinished
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
	run.FinishedAt = &now
	run.FinishedAtUnix = now.Unix()
	_ = m.writeRunSpec(run)
	_ = appendRunLog(run, "direct benchmark finished")
	m.publishLeaderboard(runCtx, run, metrics, validation, score)

	return &model.DirectBenchmarkResult{
		Run:        run,
		Metrics:    metrics,
		Validation: validation,
		Score:      &score,
	}, nil
}

func (m *Manager) execute(ctx context.Context, run *model.BenchmarkRun) {
	defer func() {
		m.mu.Lock()
		cancel := m.cancels[run.RunID]
		delete(m.cancels, run.RunID)
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		<-m.sem // release the build slot acquired by the spawning caller
	}()

	run.ArtifactDir = filepath.Join(m.runRoot, run.RunID)
	if err := os.MkdirAll(run.ArtifactDir, 0o755); err != nil {
		m.fail(ctx, run, "PREPARE_ARTIFACTS", err)
		return
	}
	if err := createArtifactPlaceholders(run.ArtifactDir); err != nil {
		m.fail(ctx, run, "PREPARE_ARTIFACTS", err)
		return
	}
	if err := writeJSONFile(filepath.Join(run.ArtifactDir, "config.json"), run.Config); err != nil {
		m.fail(ctx, run, "PREPARE_ARTIFACTS", err)
		return
	}
	_ = appendRunLog(run, "orchestrator run started")

	submission, err := m.store.GetSubmission(ctx, run.SubmissionID)
	if err != nil {
		m.fail(ctx, run, "LOAD_SUBMISSION", err)
		return
	}

	if err := m.transition(ctx, run, model.RunStatusBuilding); err != nil {
		m.fail(ctx, run, "BUILDING", err)
		return
	}
	if err := m.engine.Build(ctx, run, submission); err != nil {
		m.fail(ctx, run, "BUILDING", err)
		return
	}

	if err := m.transition(ctx, run, model.RunStatusSandboxStarting); err != nil {
		m.fail(ctx, run, "SANDBOX_STARTING", err)
		return
	}
	handle, err := m.engine.Start(ctx, run)
	if err != nil {
		m.fail(ctx, run, "SANDBOX_STARTING", err)
		return
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := handle.Stop(cleanupCtx); err != nil {
			_ = appendRunLog(run, "engine cleanup failed: "+err.Error())
		}
	}()

	run.Config.EngineEndpoint = handle.Endpoint
	if err := m.transition(ctx, run, model.RunStatusHealthChecking); err != nil {
		m.fail(ctx, run, "HEALTHCHECKING", err)
		return
	}

	if err := m.transition(ctx, run, model.RunStatusBenchmarking); err != nil {
		m.fail(ctx, run, "BENCHMARKING", err)
		return
	}
	metrics, err := m.botfleet.Run(ctx, run, handle.Endpoint)
	if err != nil {
		m.fail(ctx, run, "BENCHMARKING", err)
		return
	}
	mergeResourceSample(run.ArtifactDir, metrics)

	if err := m.transition(ctx, run, model.RunStatusValidating); err != nil {
		m.fail(ctx, run, "VALIDATING", err)
		return
	}
	validation, err := m.validator.Run(ctx, run)
	if err != nil {
		m.fail(ctx, run, "VALIDATING", err)
		return
	}
	run.Valid = &validation.Valid

	if err := m.transition(ctx, run, model.RunStatusScoring); err != nil {
		m.fail(ctx, run, "SCORING", err)
		return
	}
	score, err := executor.Score(run, metrics, validation)
	if err != nil {
		m.fail(ctx, run, "SCORING", err)
		return
	}
	run.Score = score.Score

	now := time.Now()
	run.Status = model.RunStatusFinished
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
	run.FinishedAt = &now
	run.FinishedAtUnix = now.Unix()
	_ = m.writeRunSpec(run)
	_ = m.store.SaveRun(context.Background(), run)
	_ = appendRunLog(run, "orchestrator run finished")
	m.publishLeaderboard(ctx, run, metrics, validation, score)
	slog.Info("orchestrator run finished", "run_id", run.RunID, "valid", validation.Valid, "score", run.Score)
}

func (m *Manager) publishLeaderboard(ctx context.Context, run *model.BenchmarkRun, metrics *model.Metrics, validation *model.ValidationResult, score model.ScoreResult) {
	if m.publisher == nil {
		return
	}
	if err := m.publisher.Publish(ctx, run, metrics, validation, score); err != nil {
		_ = appendRunLog(run, "leaderboard publish failed: "+err.Error())
		slog.Error("leaderboard publish failed", "run_id", run.RunID, "err", err)
	}
}

func (m *Manager) finishDirectFailure(ctx context.Context, run *model.BenchmarkRun, stage string, err error, metrics *model.Metrics, validation *model.ValidationResult, score *model.ScoreResult) *model.DirectBenchmarkResult {
	now := time.Now()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		run.Status = model.RunStatusTimedOut
		run.FailureReason = "run timed out"
	} else if errors.Is(ctx.Err(), context.Canceled) {
		run.Status = model.RunStatusCancelled
		run.FailureReason = "cancelled"
	} else {
		run.Status = model.RunStatusFailed
		run.FailureReason = err.Error()
	}
	run.FailureStage = stage
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
	run.FinishedAt = &now
	run.FinishedAtUnix = now.Unix()
	_ = m.writeRunSpec(run)
	_ = appendRunLog(run, fmt.Sprintf("direct benchmark stopped at %s: %s", stage, run.FailureReason))
	return &model.DirectBenchmarkResult{
		Run:        run,
		Metrics:    metrics,
		Validation: validation,
		Score:      score,
	}
}

func (m *Manager) transition(ctx context.Context, run *model.BenchmarkRun, status model.RunStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.Touch(run, status)
	if err := m.writeRunSpec(run); err != nil {
		return err
	}
	_ = appendRunLog(run, "status "+string(status))
	return m.store.SaveRun(ctx, run)
}

// Defense-in-depth load ceilings, mirrored from the submission-api. The values
// flow straight into the bot-fleet spawn, so clamp them here too in case a run
// reaches the orchestrator by any path other than the submission API.
const (
	maxBotCount    = 5000
	maxRatePerBot  = 2000
	maxDurationSec = 300
)

func normalizeConfig(config *model.BenchmarkRunConfig) {
	if config.BotCount <= 0 {
		config.BotCount = 10
	}
	if config.BotCount > maxBotCount {
		config.BotCount = maxBotCount
	}
	if config.RatePerBot <= 0 {
		config.RatePerBot = 2
	}
	if config.RatePerBot > maxRatePerBot {
		config.RatePerBot = maxRatePerBot
	}
	if config.DurationSec <= 0 {
		config.DurationSec = 5
	}
	if config.DurationSec > maxDurationSec {
		config.DurationSec = maxDurationSec
	}
	if config.WarmupSec < 0 {
		config.WarmupSec = 0
	}
}

func (m *Manager) fail(ctx context.Context, run *model.BenchmarkRun, stage string, err error) {
	now := time.Now()
	if errors.Is(ctx.Err(), context.Canceled) {
		run.Status = model.RunStatusCancelled
		run.FailureReason = "cancelled"
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		run.Status = model.RunStatusTimedOut
		run.FailureReason = "run timed out"
	} else {
		run.Status = model.RunStatusFailed
		run.FailureReason = err.Error()
	}
	run.FailureStage = stage
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
	run.FinishedAt = &now
	run.FinishedAtUnix = now.Unix()
	_ = m.writeRunSpec(run)
	_ = m.store.SaveRun(context.Background(), run)
	_ = appendRunLog(run, fmt.Sprintf("run stopped at %s: %s", stage, run.FailureReason))
	slog.Error("orchestrator run stopped", "run_id", run.RunID, "status", run.Status, "stage", stage, "err", err)
}

// mergeResourceSample folds the sandbox's resource.json (peak CPU%/memory of
// the contestant engine, written into the artifact dir by the sandbox sampler)
// into the metrics so the scorer's 10% resource term is real. Absent or
// unparseable -> left at zero, which the scorer treats as neutral (100), so a
// missing sample never penalises an engine.
func mergeResourceSample(artifactDir string, metrics *model.Metrics) {
	if metrics == nil || artifactDir == "" {
		return
	}
	data, err := os.ReadFile(filepath.Join(artifactDir, "resource.json"))
	if err != nil {
		return
	}
	var sample struct {
		CPUPctPeak float64 `json:"cpu_pct_peak"`
		MemMBPeak  float64 `json:"mem_mb_peak"`
	}
	if json.Unmarshal(data, &sample) == nil {
		metrics.CPUPctPeak = sample.CPUPctPeak
		metrics.MemMBPeak = sample.MemMBPeak
	}
}

func (m *Manager) writeRunSpec(run *model.BenchmarkRun) error {
	return writeJSONFile(filepath.Join(run.ArtifactDir, "run_spec.json"), run)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func createArtifactPlaceholders(dir string) error {
	names := []string{
		"config.json",
		"orders.jsonl",
		"acks.jsonl",
		"fills.jsonl",
		"cancels.jsonl",
		"run_spec.json",
		"build.json",
		"sandbox.json",
		"engine_outputs.jsonl",
		"events.jsonl",
		"contestant_outputs.jsonl",
		"metrics.json",
		"validation.json",
		"score.json",
		"run.log",
	}
	for _, name := range names {
		path := filepath.Join(dir, name)
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

func appendRunLog(run *model.BenchmarkRun, message string) error {
	if run.ArtifactDir == "" {
		return nil
	}
	file, err := os.OpenFile(filepath.Join(run.ArtifactDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%s %s\n", time.Now().Format(time.RFC3339), message)
	return err
}
