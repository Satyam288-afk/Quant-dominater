package orchestrator

import (
	"context"
	"testing"
	"time"

	"orchestrator/internal/executor"
	"orchestrator/internal/model"
	"orchestrator/internal/store"
)

// panicEngine panics inside Build, simulating an unexpected fault anywhere on
// the run pipeline.
type panicEngine struct{}

func (panicEngine) Build(context.Context, *model.BenchmarkRun, *model.Submission) error {
	panic("boom in Build")
}

func (panicEngine) Start(context.Context, *model.BenchmarkRun) (executor.EngineHandle, error) {
	return executor.EngineHandle{}, nil
}

// fakeStore is a no-op store that hands back a submission and accepts SaveRun.
type fakeStore struct{}

func (fakeStore) GetSubmission(_ context.Context, id string) (*model.Submission, error) {
	return &model.Submission{SubmissionID: id}, nil
}
func (fakeStore) GetRun(context.Context, string) (*model.BenchmarkRun, error) {
	return nil, store.ErrNotFound
}
func (fakeStore) ListRuns(context.Context) ([]*model.BenchmarkRun, error) { return nil, nil }
func (fakeStore) SaveRun(context.Context, *model.BenchmarkRun) error      { return nil }
func (fakeStore) ClaimRun(context.Context, string) (*model.BenchmarkRun, error) {
	return nil, store.ErrNotFound
}
func (fakeStore) ClaimNextQueuedRun(context.Context) (*model.BenchmarkRun, error) {
	return nil, store.ErrNotFound
}

// A panic on the detached execute() goroutine must be recovered into a FAILED
// run, not crash the whole orchestrator process (which would take down every
// other team's in-flight run). Without the recover() this test process itself
// dies with the panic.
func TestExecuteRecoversPanicIntoFailedRun(t *testing.T) {
	m := NewManager(fakeStore{}, panicEngine{}, nil, nil, t.TempDir(), time.Minute)

	run := &model.BenchmarkRun{
		RunID:        "run_panic",
		SubmissionID: "sub_panic",
		Status:       model.RunStatusBuilding,
	}

	// execute() releases a build slot in its defer, so acquire one first (as
	// the real spawn sites do).
	m.sem <- struct{}{}

	// Must return normally despite Build panicking.
	m.execute(context.Background(), run)

	if run.Status != model.RunStatusFailed {
		t.Fatalf("expected run marked FAILED after panic, got %s", run.Status)
	}
	if run.FailureStage != "PANIC" {
		t.Fatalf("expected FailureStage PANIC, got %q", run.FailureStage)
	}

	// The build slot must have been released (no leak), so a second acquire
	// succeeds immediately.
	select {
	case m.sem <- struct{}{}:
		<-m.sem
	default:
		t.Fatal("build slot leaked: sem not released after panic recovery")
	}
}
