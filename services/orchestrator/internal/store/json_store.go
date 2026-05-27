package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"orchestrator/internal/model"
)

var ErrNotFound = errors.New("record not found")

type JSONStore struct {
	path string
	mu   sync.Mutex
}

type snapshot struct {
	Submissions []*model.Submission   `json:"submissions"`
	Runs        []*model.BenchmarkRun `json:"runs"`
}

func NewJSONStore(path string) *JSONStore {
	return &JSONStore{path: path}
}

func (s *JSONStore) GetSubmission(_ context.Context, submissionID string) (*model.Submission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, submission := range snap.Submissions {
		if submission.SubmissionID == submissionID {
			cp := *submission
			return &cp, nil
		}
	}
	return nil, ErrNotFound
}

func (s *JSONStore) GetRun(_ context.Context, runID string) (*model.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, run := range snap.Runs {
		if run.RunID == runID {
			return cloneRun(run), nil
		}
	}
	return nil, ErrNotFound
}

func (s *JSONStore) ListRuns(_ context.Context) ([]*model.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	out := make([]*model.BenchmarkRun, 0, len(snap.Runs))
	for _, run := range snap.Runs {
		out = append(out, cloneRun(run))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

func (s *JSONStore) SaveRun(_ context.Context, run *model.BenchmarkRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return err
	}
	for idx, existing := range snap.Runs {
		if existing.RunID == run.RunID {
			snap.Runs[idx] = cloneRun(run)
			return s.writeLocked(snap)
		}
	}
	return ErrNotFound
}

func (s *JSONStore) ClaimRun(_ context.Context, runID string) (*model.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, run := range snap.Runs {
		if run.RunID != runID {
			continue
		}
		if model.Terminal(run.Status) {
			return cloneRun(run), nil
		}
		if run.Status != model.RunStatusQueued {
			return cloneRun(run), fmt.Errorf("run is already in progress: %s", run.Status)
		}
		Touch(run, model.RunStatusBuilding)
		if err := s.writeLocked(snap); err != nil {
			return nil, err
		}
		return cloneRun(run), nil
	}
	return nil, ErrNotFound
}

func (s *JSONStore) ClaimNextQueuedRun(_ context.Context) (*model.BenchmarkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	sort.Slice(snap.Runs, func(i, j int) bool {
		return snap.Runs[i].CreatedAt.Before(snap.Runs[j].CreatedAt)
	})
	for _, run := range snap.Runs {
		if run.Status == model.RunStatusQueued {
			Touch(run, model.RunStatusBuilding)
			if err := s.writeLocked(snap); err != nil {
				return nil, err
			}
			return cloneRun(run), nil
		}
	}
	return nil, ErrNotFound
}

func (s *JSONStore) loadLocked() (*snapshot, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return &snapshot{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return &snapshot{}, nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (s *JSONStore) writeLocked(snap *snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	sort.Slice(snap.Submissions, func(i, j int) bool {
		return snap.Submissions[i].CreatedAt.After(snap.Submissions[j].CreatedAt)
	})
	sort.Slice(snap.Runs, func(i, j int) bool {
		return snap.Runs[i].CreatedAt.After(snap.Runs[j].CreatedAt)
	})
	data, err := json.MarshalIndent(snap, "", "  ")
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

func cloneRun(run *model.BenchmarkRun) *model.BenchmarkRun {
	if run == nil {
		return nil
	}
	cp := *run
	if run.FinishedAt != nil {
		finishedAt := *run.FinishedAt
		cp.FinishedAt = &finishedAt
	}
	if run.Valid != nil {
		valid := *run.Valid
		cp.Valid = &valid
	}
	return &cp
}

func Touch(run *model.BenchmarkRun, status model.RunStatus) {
	now := time.Now()
	run.Status = status
	run.UpdatedAt = now
	run.UpdatedAtUnix = now.Unix()
}
