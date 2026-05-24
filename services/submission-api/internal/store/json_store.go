package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"submission-api/internal/model"
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

func (s *JSONStore) SaveSubmission(_ context.Context, submission *model.Submission) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return err
	}
	upsertSubmission(snap, cloneSubmission(submission))
	return s.writeLocked(snap)
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
			return cloneSubmission(submission), nil
		}
	}
	return nil, ErrNotFound
}

func (s *JSONStore) ListSubmissions(_ context.Context) ([]*model.Submission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	out := make([]*model.Submission, 0, len(snap.Submissions))
	for _, submission := range snap.Submissions {
		out = append(out, cloneSubmission(submission))
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
	upsertRun(snap, cloneRun(run))
	return s.writeLocked(snap)
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

func upsertSubmission(snap *snapshot, submission *model.Submission) {
	for idx, existing := range snap.Submissions {
		if existing.SubmissionID == submission.SubmissionID {
			snap.Submissions[idx] = submission
			return
		}
	}
	snap.Submissions = append(snap.Submissions, submission)
}

func upsertRun(snap *snapshot, run *model.BenchmarkRun) {
	for idx, existing := range snap.Runs {
		if existing.RunID == run.RunID {
			snap.Runs[idx] = run
			return
		}
	}
	snap.Runs = append(snap.Runs, run)
}

func cloneSubmission(submission *model.Submission) *model.Submission {
	if submission == nil {
		return nil
	}
	cp := *submission
	return &cp
}

func cloneRun(run *model.BenchmarkRun) *model.BenchmarkRun {
	if run == nil {
		return nil
	}
	cp := *run
	return &cp
}
