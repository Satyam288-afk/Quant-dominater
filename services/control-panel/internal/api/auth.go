package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func RequireServiceAuth(next http.Handler, token string) http.Handler {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only the unauthenticated health probe is exempt. Using `&&` (not `||`)
		// keeps every other GET (run configs, scores, seeds, host paths) behind
		// auth, matching the sibling services (submission-api, orchestrator).
		if r.Method == http.MethodGet && r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimSpace(r.Header.Get("Authorization"))
		want := "Bearer " + token
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}
