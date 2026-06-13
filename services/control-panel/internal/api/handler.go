package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"control-panel/internal/run"
	"control-panel/internal/store"
)

type Handler struct {
	manager *run.Manager
	store   run.Store
}

type Artifact struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	SizeBytes  int64     `json:"size_bytes"`
	ModifiedAt time.Time `json:"modified_at"`
}

func NewHandler(manager *run.Manager, store run.Store) *Handler {
	return &Handler{manager: manager, store: store}
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) CreateRun(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()

	var req run.RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	br, err := h.manager.CreateRun(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The pointer returned by CreateRun is the live run that the spawned
	// execute() goroutine mutates. Encoding it directly races with that
	// goroutine, so fetch a fresh clone from the store (which clones on Get)
	// and encode a value execute() never touches.
	if snapshot, gerr := h.store.Get(r.Context(), br.RunID); gerr == nil {
		br = snapshot
	}

	writeJSON(w, http.StatusAccepted, br)
}

func (h *Handler) ListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) GetRun(w http.ResponseWriter, r *http.Request) {
	br, err := h.store.Get(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, br)
}

func (h *Handler) GetLogs(w http.ResponseWriter, r *http.Request) {
	br, err := h.store.Get(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}

	data, err := os.ReadFile(filepath.Join(br.ArtifactDir, "run.log"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (h *Handler) GetArtifacts(w http.ResponseWriter, r *http.Request) {
	br, err := h.store.Get(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}

	entries, err := os.ReadDir(br.ArtifactDir)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	artifacts := make([]Artifact, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		artifacts = append(artifacts, Artifact{
			Name:       name,
			Path:       filepath.Join(br.ArtifactDir, name),
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime(),
		})
	}

	writeJSON(w, http.StatusOK, artifacts)
}

func (h *Handler) CancelRun(w http.ResponseWriter, r *http.Request) {
	br, err := h.manager.CancelRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, br)
}

func handleStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
