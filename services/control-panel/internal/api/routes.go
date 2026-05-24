package api

import "net/http"

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("POST /api/runs", h.CreateRun)
	mux.HandleFunc("GET /api/runs", h.ListRuns)
	mux.HandleFunc("GET /api/runs/{run_id}", h.GetRun)
	mux.HandleFunc("GET /api/runs/{run_id}/logs", h.GetLogs)
	mux.HandleFunc("GET /api/runs/{run_id}/artifacts", h.GetArtifacts)
	mux.HandleFunc("POST /api/runs/{run_id}/cancel", h.CancelRun)
}
