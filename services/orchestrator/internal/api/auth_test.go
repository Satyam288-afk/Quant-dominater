package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func authResponse(t *testing.T, token, method, path, header string) int {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := RequireServiceAuth(next, token)
	req := httptest.NewRequest(method, path, nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// With a token configured, every request except the health probe must carry it
// — including GETs. The previous `||` form exempted all GETs, leaking rival
// teams' run data.
func TestRequireServiceAuthGatesGetRequests(t *testing.T) {
	const token = "secrettoken123"

	if code := authResponse(t, token, http.MethodGet, "/runs", ""); code != http.StatusUnauthorized {
		t.Fatalf("GET /runs without token = %d, want 401", code)
	}
	if code := authResponse(t, token, http.MethodGet, "/runs", "Bearer "+token); code != http.StatusOK {
		t.Fatalf("GET /runs with token = %d, want 200", code)
	}
	if code := authResponse(t, token, http.MethodGet, "/runs/run_1", ""); code != http.StatusUnauthorized {
		t.Fatalf("GET /runs/run_1 without token = %d, want 401", code)
	}
	// The health probe stays open so liveness checks need no credential.
	if code := authResponse(t, token, http.MethodGet, "/health", ""); code != http.StatusOK {
		t.Fatalf("GET /health without token = %d, want 200", code)
	}
	// A mutating request still needs the token.
	if code := authResponse(t, token, http.MethodPost, "/api/benchmark", ""); code != http.StatusUnauthorized {
		t.Fatalf("POST without token = %d, want 401", code)
	}
}

// With no token configured the middleware is a pass-through (local/dev posture).
func TestRequireServiceAuthDisabledWithoutToken(t *testing.T) {
	if code := authResponse(t, "", http.MethodGet, "/runs", ""); code != http.StatusOK {
		t.Fatalf("GET /runs (no token configured) = %d, want 200", code)
	}
	if code := authResponse(t, "", http.MethodPost, "/api/benchmark", ""); code != http.StatusOK {
		t.Fatalf("POST (no token configured) = %d, want 200", code)
	}
}
