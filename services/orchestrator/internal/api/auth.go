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
		// Only the unauthenticated health probe is exempt. The bug this replaces
		// used `||`, which exempted EVERY GET — leaking all teams' run configs,
		// scores, seeds, and host paths when a token was configured. The sibling
		// services (submission-api, sandbox-runner) already use this `&&` form.
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
