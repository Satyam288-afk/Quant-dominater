package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"control-panel/internal/run"
)

var ErrNotFound = errors.New("run not found")

type JSONStore struct {
	path string
	mu   sync.Mutex
}

func NewJSONStore(path string) *JSONStore {
	return &JSONStore{path: path}
}

func (s *JSONStore) Save(_ context.Context, r *run.BenchmarkRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	runs, err := s.loadLocked()
	if err != nil {
		return err
	}
	runs[r.RunID] = cloneRun(r)
	return s.writeLocked(runs)
}

func (s *JSONStore) Get(_ context.Context, runID string) (*run.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runs, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	r, ok := runs[runID]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneRun(r), nil
}

func (s *JSONStore) List(_ context.Context) ([]*run.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runs, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	list := make([]*run.BenchmarkRun, 0, len(runs))
	for _, r := range runs {
		list = append(list, cloneRun(r))
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].StartedAt.After(list[j].StartedAt)
	})
	return list, nil
}

func (s *JSONStore) loadLocked() (map[string]*run.BenchmarkRun, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]*run.BenchmarkRun{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return map[string]*run.BenchmarkRun{}, nil
	}

	var list []*run.BenchmarkRun
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}

	runs := make(map[string]*run.BenchmarkRun, len(list))
	for _, r := range list {
		if r != nil && r.RunID != "" {
			runs[r.RunID] = r
		}
	}
	return runs, nil
}

func (s *JSONStore) writeLocked(runs map[string]*run.BenchmarkRun) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}

	list := make([]*run.BenchmarkRun, 0, len(runs))
	for _, r := range runs {
		list = append(list, cloneRun(r))
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].StartedAt.After(list[j].StartedAt)
	})

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func cloneRun(r *run.BenchmarkRun) *run.BenchmarkRun {
	if r == nil {
		return nil
	}
	cp := *r
	if r.FinishedAt != nil {
		finishedAt := *r.FinishedAt
		cp.FinishedAt = &finishedAt
	}
	if r.Valid != nil {
		valid := *r.Valid
		cp.Valid = &valid
	}
	if r.Metrics != nil {
		metrics := *r.Metrics
		cp.Metrics = &metrics
	}
	if r.Validation != nil {
		validation := *r.Validation
		cp.Validation = &validation
	}
	return &cp
}
