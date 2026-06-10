package board

import (
	"path/filepath"
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
