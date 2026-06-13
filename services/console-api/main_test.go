package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestSafeArtifactDirAllowsRunsDirectory(t *testing.T) {
	repo := t.TempDir()
	want := filepath.Join(repo, ".runs", "run_1")
	got, err := safeArtifactDir(repo, want)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("safeArtifactDir() = %q, want %q", got, want)
	}
}

func TestSafeArtifactDirRejectsOutsideRunsDirectory(t *testing.T) {
	repo := t.TempDir()
	_, err := safeArtifactDir(repo, filepath.Join(repo, ".artifacts", "secret"))
	if err == nil {
		t.Fatal("expected artifact path outside .runs to be rejected")
	}
}

func TestSameOrigin(t *testing.T) {
	cases := []struct {
		name       string
		host       string
		origin     string
		secFetch   string
		wantStatus int
	}{
		{name: "no origin (non-browser/curl)", host: "localhost:9700", wantStatus: http.StatusOK},
		{name: "same origin", host: "localhost:9700", origin: "http://localhost:9700", wantStatus: http.StatusOK},
		{name: "cross origin", host: "localhost:9700", origin: "http://evil.example", wantStatus: http.StatusForbidden},
		{name: "cross origin same host different port", host: "localhost:9700", origin: "http://localhost:9999", wantStatus: http.StatusForbidden},
		{name: "sec-fetch same-origin", host: "localhost:9700", secFetch: "same-origin", wantStatus: http.StatusOK},
		{name: "sec-fetch cross-site", host: "localhost:9700", origin: "http://localhost:9700", secFetch: "cross-site", wantStatus: http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := sameOrigin(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			req := httptest.NewRequest(http.MethodPost, "http://"+tc.host+"/api/submissions", nil)
			req.Host = tc.host
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			rec := httptest.NewRecorder()
			h(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("sameOrigin status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}
