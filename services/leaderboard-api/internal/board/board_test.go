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
