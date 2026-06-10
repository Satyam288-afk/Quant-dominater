package board

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Entry struct {
	RunID         string    `json:"run_id"`
	TeamID        string    `json:"team_id"`
	Score         float64   `json:"score"`
	Valid         bool      `json:"valid"`
	Status        string    `json:"status,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	P50MS         float64   `json:"p50_ms,omitempty"`
	P90MS         float64   `json:"p90_ms,omitempty"`
	P99MS         float64   `json:"p99_ms,omitempty"`
	P999MS        float64   `json:"p999_ms,omitempty"`
	TPS           float64   `json:"tps,omitempty"`
	PeakTPS       float64   `json:"peak_tps,omitempty"`
	// Score breakdown — populated by the Redis backend from the score-engine
	// scorecard so the leaderboard UI can render the composite components.
	LatencyScore    float64 `json:"latency_score,omitempty"`
	ThroughputScore float64 `json:"throughput_score,omitempty"`
	StabilityScore  float64 `json:"stability_score,omitempty"`
	ResourceScore   float64 `json:"resource_score,omitempty"`
	OrdersSent      int64   `json:"orders_sent,omitempty"`
	AcksReceived    int64   `json:"acks_received,omitempty"`
	Timeouts        int64   `json:"timeouts,omitempty"`

	UpdatedAt time.Time `json:"updated_at"`
}

type Board struct {
	path string

	mu          sync.Mutex
	entries     map[string]Entry
	subscribers map[chan []byte]struct{}
}

func New(path string) (*Board, error) {
	b := &Board{
		path:        path,
		entries:     make(map[string]Entry),
		subscribers: make(map[chan []byte]struct{}),
	}
	if err := b.load(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Board) Upsert(entry Entry) ([]Entry, error) {
	if entry.RunID == "" {
		return nil, errors.New("run_id is required")
	}
	if entry.TeamID == "" {
		entry.TeamID = entry.RunID
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now()
	}

	b.mu.Lock()
	b.entries[entry.RunID] = entry
	snapshot := b.snapshotLocked()
	payload, marshalErr := json.Marshal(snapshot)
	writeErr := b.writeLocked(snapshot)
	subscribers := make([]chan []byte, 0, len(b.subscribers))
	for ch := range b.subscribers {
		subscribers = append(subscribers, ch)
	}
	b.mu.Unlock()

	if marshalErr == nil {
		for _, ch := range subscribers {
			select {
			case ch <- payload:
			default:
			}
		}
	}
	if writeErr != nil {
		return snapshot, writeErr
	}
	return snapshot, marshalErr
}

func (b *Board) List() []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

func (b *Board) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 8)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	snapshot := b.snapshotLocked()
	payload, _ := json.Marshal(snapshot)
	b.mu.Unlock()

	ch <- payload
	cancel := func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		close(ch)
		b.mu.Unlock()
	}
	return ch, cancel
}

func (b *Board) load() error {
	data, err := os.ReadFile(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.RunID != "" {
			b.entries[entry.RunID] = entry
		}
	}
	return nil
}

func (b *Board) writeLocked(entries []Entry) error {
	if err := os.MkdirAll(filepath.Dir(b.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := b.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, b.path)
}

func (b *Board) snapshotLocked() []Entry {
	entries := make([]Entry, 0, len(b.entries))
	for _, entry := range b.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score == entries[j].Score {
			return entries[i].UpdatedAt.Before(entries[j].UpdatedAt)
		}
		return entries[i].Score > entries[j].Score
	})
	return entries
}
