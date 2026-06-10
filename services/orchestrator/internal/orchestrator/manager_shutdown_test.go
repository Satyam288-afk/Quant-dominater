package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Shutdown must cancel every in-flight run and return once their goroutines
// have drained (here simulated by each cancel removing its own map entry, as
// the real execute() defer does).
func TestShutdownCancelsAndDrainsInflightRuns(t *testing.T) {
	m := NewManager(nil, nil, nil, nil, "", 0)

	var cancelled int32
	for _, id := range []string{"run_a", "run_b", "run_c"} {
		id := id
		m.cancels[id] = func() {
			atomic.AddInt32(&cancelled, 1)
			// Mimic execute()'s defer: the run goroutine removes itself once
			// it observes cancellation. Done in a goroutine because Shutdown
			// holds m.mu while invoking cancel funcs.
			go func() {
				m.mu.Lock()
				delete(m.cancels, id)
				m.mu.Unlock()
			}()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { m.Shutdown(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return within the grace window")
	}

	if got := atomic.LoadInt32(&cancelled); got != 3 {
		t.Fatalf("expected all 3 in-flight runs cancelled, got %d", got)
	}
	m.mu.Lock()
	remaining := len(m.cancels)
	m.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected cancels map drained, %d remaining", remaining)
	}
}

// If a run refuses to drain, Shutdown must still return when the grace ctx
// expires (bounded shutdown) rather than block forever.
func TestShutdownRespectsGraceDeadline(t *testing.T) {
	m := NewManager(nil, nil, nil, nil, "", 0)
	// A cancel that never removes its map entry (the run won't drain).
	m.cancels["stuck"] = func() {}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	m.Shutdown(ctx)
	elapsed := time.Since(start)

	if elapsed < 100*time.Millisecond {
		t.Fatalf("Shutdown returned too early (%s); should wait out the grace", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("Shutdown blocked past the grace deadline (%s)", elapsed)
	}
}
