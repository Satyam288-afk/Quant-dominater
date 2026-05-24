package api

import "net/http"

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /submissions", h.CreateSubmission)
	mux.HandleFunc("GET /submissions", h.ListSubmissions)
	mux.HandleFunc("GET /submissions/{submission_id}", h.GetSubmission)
	mux.HandleFunc("POST /submissions/{submission_id}/runs", h.CreateRun)
	mux.HandleFunc("GET /runs", h.ListRuns)
	mux.HandleFunc("GET /runs/{run_id}", h.GetRun)
}
