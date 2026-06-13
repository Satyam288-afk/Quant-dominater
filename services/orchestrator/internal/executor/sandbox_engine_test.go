package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"orchestrator/internal/model"
)

func TestSandboxEngineUsesRunContextForTimeouts(t *testing.T) {
	engine := NewSandboxEngine("http://127.0.0.1:9200")
	if engine.client.Timeout != 0 {
		t.Fatalf("sandbox engine client timeout = %s, want no fixed timeout", engine.client.Timeout)
	}
}

func TestSandboxEngineStartDoesNotSendDefaultEngineMode(t *testing.T) {
	var startReq map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sandboxes/build":
			writeTestJSON(t, w, sandboxImageRef{ImageRef: "docker://image"})
		case "/sandboxes/start":
			if err := json.NewDecoder(r.Body).Decode(&startReq); err != nil {
				t.Fatal(err)
			}
			writeTestJSON(t, w, sandboxHandle{
				SandboxID: "sandbox_1",
				Endpoint:  "ws://127.0.0.1:12345/ws",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	run := &model.BenchmarkRun{
		RunID:       "run_1",
		ArtifactDir: t.TempDir(),
	}
	submission := &model.Submission{
		SubmissionID: "sub_1",
		Language:     "go",
		Artifact: model.SubmissionArtifact{
			URI: "local://submissions/sub_1/engine.zip",
		},
	}
	engine := NewSandboxEngine(server.URL)
	if err := engine.Build(context.Background(), run, submission); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, ok := startReq["engine_mode"]; ok {
		t.Fatalf("start request should not include default engine_mode: %#v", startReq)
	}
	if _, err := os.Stat(filepath.Join(run.ArtifactDir, "sandbox.json")); err != nil {
		t.Fatalf("expected sandbox.json: %v", err)
	}
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
