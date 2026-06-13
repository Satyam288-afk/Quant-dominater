package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"control-panel/internal/run"
	"control-panel/internal/store"
)

// gatedValidator parks in Run() until released, giving the test a deterministic
// window in which CancelRun completes (persisting CANCELLED) before execute()
// is allowed to proceed toward its FINISHED write. It intentionally ignores
// ctx cancellation so the finish path actually attempts the FINISHED write and
// exercises both fix layers (execute()'s ctx.Err() recheck and the store's
// terminal-state guard) rather than bailing out at the validator.
type gatedValidator struct {
	entered chan struct{}
	release chan struct{}
}

func (g *gatedValidator) Run(_ context.Context, r *run.BenchmarkRun) (*run.ValidationResult, error) {
	select {
	case <-g.entered:
	default:
		close(g.entered)
	}
	<-g.release
	return &run.ValidationResult{RunID: r.RunID, Valid: true}, nil
}

type fakeEngine struct{}

func (fakeEngine) Start(_ context.Context, _ *run.BenchmarkRun) (string, func(context.Context) error, error) {
	return "127.0.0.1:0", func(context.Context) error { return nil }, nil
}

type fakeBotFleet struct{}

func (fakeBotFleet) Run(_ context.Context, r *run.BenchmarkRun, _ string) (*run.Metrics, error) {
	return &run.Metrics{RunID: r.RunID, Bots: 1, OrdersSent: 1, AcksReceived: 1}, nil
}

// TestCancelledRunStaysCancelled is the deterministic reproduction of finding
// #1: a cancelled run must not be silently overwritten to FINISHED. The
// validator gates execute() so CancelRun fully persists CANCELLED first, then
// execute() is released to race its FINISHED write against concurrent JSON
// encodes. With both fix layers the persisted state must remain CANCELLED.
// Run with -race.
func TestCancelledRunStaysCancelled(t *testing.T) {
	dir := t.TempDir()
	st := store.NewJSONStore(filepath.Join(dir, "runs.json"))

	for i := 0; i < 100; i++ {
		gv := &gatedValidator{entered: make(chan struct{}), release: make(chan struct{})}
		mgr := run.NewManager(fakeEngine{}, fakeBotFleet{}, gv, st, dir)

		created, err := mgr.CreateRun(context.Background(), run.RunRequest{
			TeamID:       "team",
			EngineMode:   "normal",
			BotCount:     1,
			OrdersPerSec: 1,
			DurationSec:  1,
		})
		if err != nil {
			t.Fatalf("CreateRun: %v", err)
		}

		// Wait until execute() is parked inside the validator (post-bench,
		// pre-finish). This establishes the happens-before: the cancel below
		// is fully persisted before execute() can attempt FINISHED.
		select {
		case <-gv.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("run %s never entered validator", created.RunID)
		}

		// Cancel and persist CANCELLED while execute() is parked.
		if _, err := mgr.CancelRun(context.Background(), created.RunID); err != nil {
			t.Fatalf("CancelRun: %v", err)
		}
		if r, err := st.Get(context.Background(), created.RunID); err != nil {
			t.Fatalf("Get after cancel: %v", err)
		} else if r.Status != run.StatusCancelled {
			t.Fatalf("expected CANCELLED after cancel, got %s", r.Status)
		}

		// Release execute() so it races its FINISHED write against concurrent
		// encodes. Both fix layers must keep the run CANCELLED.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			close(gv.release)
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				list, err := st.List(context.Background())
				if err != nil {
					t.Errorf("List: %v", err)
					return
				}
				if _, err := json.Marshal(list); err != nil {
					t.Errorf("Marshal: %v", err)
					return
				}
			}
		}()
		wg.Wait()

		// Give execute()'s goroutine time to attempt (and have refused) its
		// FINISHED write, then assert the persisted state is still CANCELLED.
		// The validator is released and the run was already terminal before the
		// finish path runs, so a short settle is sufficient and deterministic.
		time.Sleep(10 * time.Millisecond)
		r, err := st.Get(context.Background(), created.RunID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if r.Status != run.StatusCancelled {
			t.Fatalf("iteration %d: cancelled run was overwritten to %s (score=%v)", i, r.Status, r.Score)
		}
	}
}
