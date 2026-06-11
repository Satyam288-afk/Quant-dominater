package api

import (
	"encoding/json"
	"net/http"

	"sandbox-runner/internal/sandbox"
)

type Handler struct {
	runner sandbox.Runner
}

func NewHandler(runner sandbox.Runner) *Handler {
	return &Handler{runner: runner}
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Build(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req sandbox.BuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	image, err := h.runner.Build(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, image)
}

func (h *Handler) Start(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req sandbox.StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	handle, err := h.runner.Start(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, handle)
}

func (h *Handler) List(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.runner.List())
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	handle, ok := h.runner.Get(r.PathValue("sandbox_id"))
	if !ok {
		writeError(w, http.StatusNotFound, "sandbox not found")
		return
	}
	writeJSON(w, http.StatusOK, handle)
}

func (h *Handler) Stop(w http.ResponseWriter, r *http.Request) {
	if err := h.runner.Stop(r.Context(), r.PathValue("sandbox_id")); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
