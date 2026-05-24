package api

import "net/http"

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /runs", h.ListRuns)
	mux.HandleFunc("GET /runs/{run_id}", h.GetRun)
	mux.HandleFunc("POST /runs/{run_id}/start", h.StartRun)
	mux.HandleFunc("POST /runs/{run_id}/cancel", h.CancelRun)
	mux.HandleFunc("POST /runs/next", h.StartNextQueued)
}
