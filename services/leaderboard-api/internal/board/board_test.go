package board

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoardSortsByScore(t *testing.T) {
	b, err := New(filepath.Join(t.TempDir(), "leaderboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := b.Upsert(Entry{RunID: "run_low", TeamID: "team_low", Score: 12, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Upsert(Entry{RunID: "run_high", TeamID: "team_high", Score: 99, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	entries := b.List()
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[0].RunID != "run_high" {
		t.Fatalf("top run = %q, want run_high", entries[0].RunID)
	}
}

func TestBoardBroadcastsSnapshot(t *testing.T) {
	b, err := New(filepath.Join(t.TempDir(), "leaderboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	ch, cancel := b.Subscribe()
	defer cancel()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected initial snapshot")
	}

	if _, err := b.Upsert(Entry{RunID: "run_1", TeamID: "team_1", Score: 42}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected update snapshot")
	}
}

func TestBoardPersistsDetailedMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leaderboard.json")
	b, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Upsert(Entry{
		RunID:           "run_1",
		TeamID:          "team_1",
		Score:           87.5,
		Valid:           true,
		Status:          "FINISHED",
		OrdersSent:      1000,
		AcksReceived:    998,
		FillsReceived:   550,
		Timeouts:        2,
		FillsChecked:    550,
		P99MS:           12.5,
		TPS:             499,
		LatencyScore:    90,
		ThroughputScore: 95,
		StabilityScore:  99,
		ResourceScore:   75,
		CorrectnessGate: "passed",
		ArtifactDir:     "/tmp/run_1",
	})
	if err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := reloaded.List()
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	got := entries[0]
	if got.OrdersSent != 1000 || got.Timeouts != 2 || got.CorrectnessGate != "passed" || got.ArtifactDir != "/tmp/run_1" {
		t.Fatalf("detailed metrics were not persisted: %+v", got)
	}
}

// TestBoardSubscribeSeedNonBlockingWhenBufferFull is the deterministic
// regression test for the seed-deadlock bug (#18).
//
// Subscribe registers the new channel under the lock, unlocks, then seeds it.
// If a concurrent Upsert broadcast fills the freshly registered channel's
// 8-slot buffer in that unlock->seed window, the old blocking `ch <- payload`
// parks FOREVER: Subscribe never returns and the calling (WebSocket) goroutine
// plus its subscriber-map entry leak. The fix makes the seed non-blocking
// (`select { case ch <- payload: default: }`), matching the broadcast path.
//
// This reproduces the precondition deterministically (no timing race): it is an
// in-package test, so it registers a channel that is ALREADY full into the
// subscriber map, then runs the exact seed statement against it under a hard
// timeout. With the blocking send the goroutine never signals done; with the
// fix it returns immediately. It also asserts the live Subscribe() returns when
// its broadcast-fed buffer is saturated.
func TestBoardSubscribeSeedNonBlockingWhenBufferFull(t *testing.T) {
	b, err := New(filepath.Join(t.TempDir(), "leaderboard.json"))
	if err != nil {
		t.Fatal(err)
	}

	// Build a subscriber channel that is already full, exactly as a concurrent
	// broadcast burst would leave a freshly registered channel in the
	// unlock->seed window. Register it under the lock like Subscribe does.
	ch := make(chan []byte, 8)
	for i := 0; i < cap(ch); i++ {
		ch <- []byte("prefill")
	}
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	payload, _ := json.Marshal(b.snapshotLocked())
	b.mu.Unlock()

	// Call the actual production seed helper against the full channel. The
	// fixed non-blocking seed returns instantly; a blocking `ch <- payload`
	// would park here forever and `seeded` would never close. This binds the
	// regression test to the real code path used by Subscribe.
	seeded := make(chan struct{})
	go func() {
		seedSubscriber(ch, payload)
		close(seeded)
	}()
	select {
	case <-seeded:
	case <-time.After(2 * time.Second):
		t.Fatal("seed blocked on a full subscriber buffer (bug #18)")
	}

	b.mu.Lock()
	delete(b.subscribers, ch)
	b.mu.Unlock()

	// End-to-end: hammer Subscribe concurrently while flooding broadcasts so
	// every registered, undrained channel fills. Under -race this also
	// exercises the documented seed/broadcast data race. Every Subscribe must
	// return; a progress watchdog distinguishes a true permanent park (bug)
	// from mere throughput slowness.
	const saturated = 32
	heldCancels := make([]func(), 0, saturated)
	for i := 0; i < saturated; i++ {
		_, cancel := b.Subscribe() // never drained: stays full under flood
		heldCancels = append(heldCancels, cancel)
	}
	defer func() {
		for _, c := range heldCancels {
			c()
		}
	}()

	stop := make(chan struct{})
	var flooders sync.WaitGroup
	for i := 0; i < 8; i++ {
		flooders.Add(1)
		go func() {
			defer flooders.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = b.Upsert(Entry{RunID: "flood", Score: float64(n)})
					n++
				}
			}
		}()
	}
	defer func() {
		close(stop)
		flooders.Wait()
	}()

	baseline := runtime.NumGoroutine()

	const subscribers = 400
	var completed int64
	var cancelMu sync.Mutex
	cancels := make([]func(), 0, subscribers)
	var wg sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, cancel := b.Subscribe()
			atomic.AddInt64(&completed, 1)
			cancelMu.Lock()
			cancels = append(cancels, cancel)
			cancelMu.Unlock()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	const pollEvery = 2 * time.Second
	const hardCap = 30 * time.Second
	deadline := time.After(hardCap)
	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()
	var last int64
	for done != nil {
		select {
		case <-done:
			done = nil
		case <-ticker.C:
			now := atomic.LoadInt64(&completed)
			if now == last && now < subscribers {
				t.Fatalf("Subscribe deadlocked: seed wedged behind a full buffer "+
					"(stalled at %d/%d completions)", now, subscribers)
			}
			last = now
		case <-deadline:
			t.Fatalf("Subscribe did not complete within %s (%d/%d)",
				hardCap, atomic.LoadInt64(&completed), subscribers)
		}
	}

	cancelMu.Lock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
	cancelMu.Unlock()

	// No Subscribe goroutine should remain parked on a seed send. Allow slack
	// for scheduler/runtime goroutines; the bug would leak parked goroutines.
	if leaked := runtime.NumGoroutine() - baseline; leaked > 16 {
		t.Fatalf("goroutine count unbounded: leaked ~%d goroutines", leaked)
	}
}
