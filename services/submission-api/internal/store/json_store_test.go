package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"submission-api/internal/model"
)

// Two JSONStore instances on the same index.json model submission-api and
// orchestrator as separate processes. Concurrent SaveRun of distinct runs must
// not lose any to a last-rename-wins clobber — the cross-process flock
// serialises their read-modify-write so every write survives.
func TestConcurrentSaveRunNoLostUpdatesAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.json")
	a := NewJSONStore(path)
	b := NewJSONStore(path)

	const perWriter = 50
	var wg sync.WaitGroup
	write := func(s *JSONStore, prefix string) {
		defer wg.Done()
		for i := 0; i < perWriter; i++ {
			run := &model.BenchmarkRun{RunID: fmt.Sprintf("%s_%02d", prefix, i)}
			if err := s.SaveRun(context.Background(), run); err != nil {
				t.Errorf("SaveRun: %v", err)
				return
			}
		}
	}
	wg.Add(2)
	go write(a, "a")
	go write(b, "b")
	wg.Wait()

	runs, err := a.ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != perWriter*2 {
		t.Fatalf("got %d runs, want %d (lost updates from cross-process clobber)", len(runs), perWriter*2)
	}
}
