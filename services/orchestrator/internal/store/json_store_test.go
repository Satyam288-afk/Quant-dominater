package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"orchestrator/internal/model"
)

// Two JSONStore instances on the same index.json model the real deployment:
// submission-api and orchestrator are separate OS processes with independent
// in-process mutexes. ClaimNextQueuedRun must never hand the same QUEUED run to
// two claimers — that is the cross-process double-execute failure the flock
// closes. Without the flock both instances load the same snapshot and claim the
// same run.
func TestClaimNextQueuedRunNoDoubleClaimAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.json")

	const n = 40
	seed := &snapshot{}
	base := time.Now()
	for i := 0; i < n; i++ {
		seed.Runs = append(seed.Runs, &model.BenchmarkRun{
			RunID:     fmt.Sprintf("run_%02d", i),
			Status:    model.RunStatusQueued,
			CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
		})
	}
	if err := NewJSONStore(path).writeLocked(seed); err != nil {
		t.Fatal(err)
	}

	// a and b have independent mutexes — only the cross-process flock serialises
	// them.
	a := NewJSONStore(path)
	b := NewJSONStore(path)

	var mu sync.Mutex
	claimed := map[string]int{}

	var wg sync.WaitGroup
	worker := func(s *JSONStore) {
		defer wg.Done()
		for {
			run, err := s.ClaimNextQueuedRun(context.Background())
			if err != nil {
				return // ErrNotFound: queue drained
			}
			mu.Lock()
			claimed[run.RunID]++
			mu.Unlock()
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go worker(a)
		} else {
			go worker(b)
		}
	}
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("claimed %d distinct runs, want %d", len(claimed), n)
	}
	for id, count := range claimed {
		if count != 1 {
			t.Fatalf("run %s claimed %d times (double-execute)", id, count)
		}
	}
}
