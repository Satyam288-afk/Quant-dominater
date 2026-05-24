package api

import "net/http"

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /sandboxes/build", h.Build)
	mux.HandleFunc("POST /sandboxes/start", h.Start)
	mux.HandleFunc("GET /sandboxes", h.List)
	mux.HandleFunc("GET /sandboxes/{sandbox_id}", h.Get)
	mux.HandleFunc("POST /sandboxes/{sandbox_id}/stop", h.Stop)
}
