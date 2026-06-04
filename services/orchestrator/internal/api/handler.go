package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"orchestrator/internal/model"
	"orchestrator/internal/store"
)

type Manager interface {
	StartRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	StartNextQueued(ctx context.Context) (*model.BenchmarkRun, error)
	CancelRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	BenchmarkEndpoint(ctx context.Context, req model.DirectBenchmarkRequest) (*model.DirectBenchmarkResult, error)
}

type RunStore interface {
	GetRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	ListRuns(ctx context.Context) ([]*model.BenchmarkRun, error)
}

type Handler struct {
	manager Manager
	store   RunStore
}

func NewHandler(manager Manager, store RunStore) *Handler {
	return &Handler{manager: manager, store: store}
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) GetRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.store.GetRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) StartRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.manager.StartRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (h *Handler) StartNextQueued(w http.ResponseWriter, r *http.Request) {
	run, err := h.manager.StartNextQueued(r.Context())
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (h *Handler) CancelRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.manager.CancelRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) BenchmarkEndpoint(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req model.DirectBenchmarkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	result, err := h.manager.BenchmarkEndpoint(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func handleStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
