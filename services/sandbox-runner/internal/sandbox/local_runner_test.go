package sandbox

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalRunnerBuildUsesSubmittedArtifact(t *testing.T) {
	repoRoot := t.TempDir()
	artifactDir := filepath.Join(repoRoot, ".artifacts", "submissions", "sub_1")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifactDir, "main.go"), []byte(`package main

import "net/http"

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	_ = http.ListenAndServe(":8080", nil)
}
`), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := NewLocalRunner(repoRoot, filepath.Join(t.TempDir(), "runs"))
	image, err := runner.Build(context.Background(), BuildRequest{
		SubmissionID: "sub_1",
		ArtifactURI:  "local://submissions/sub_1/main.go",
		Language:     "go",
	})
	if err != nil {
		t.Fatal(err)
	}
	if image.ImageRef == "" || image.ImageRef == "local://sub_1" {
		t.Fatalf("unexpected image ref %q", image.ImageRef)
	}
}

func TestLocalRunnerBuildHonorsCanceledContext(t *testing.T) {
	repoRoot := t.TempDir()
	artifactDir := filepath.Join(repoRoot, ".artifacts", "submissions", "sub_1")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifactDir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runner := NewLocalRunner(repoRoot, filepath.Join(t.TempDir(), "runs"))
	_, err := runner.Build(ctx, BuildRequest{
		SubmissionID: "sub_1",
		ArtifactURI:  "local://submissions/sub_1/main.go",
		Language:     "go",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Build error = %v, want context.Canceled", err)
	}
}

func TestLocalRunnerStartDoesNotTieProcessToRequestContext(t *testing.T) {
	repoRoot := t.TempDir()
	artifactDir := filepath.Join(repoRoot, ".artifacts", "submissions", "sub_1")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := `package main

import (
	"flag"
	"net/http"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.String("mode", "normal", "unused mode")
	flag.String("events", "events.jsonl", "unused events path")
	flag.Parse()

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	_ = http.ListenAndServe(*addr, nil)
}
`
	if err := os.WriteFile(filepath.Join(artifactDir, "main.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := NewLocalRunner(repoRoot, filepath.Join(t.TempDir(), "runs"))
	image, err := runner.Build(context.Background(), BuildRequest{
		SubmissionID: "sub_1",
		ArtifactURI:  "local://submissions/sub_1/main.go",
		Language:     "go",
	})
	if err != nil {
		t.Fatal(err)
	}

	startCtx, cancel := context.WithCancel(context.Background())
	handle, err := runner.Start(startCtx, StartRequest{
		RunID:    "run_1",
		ImageRef: image.ImageRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Stop(context.Background(), handle.SandboxID)

	cancel()
	time.Sleep(200 * time.Millisecond)

	resp, err := http.Get(handle.HealthURL)
	if err != nil {
		t.Fatalf("health after start context cancellation: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
