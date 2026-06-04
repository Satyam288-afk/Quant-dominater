package sandbox

import (
	"os"
	"path/filepath"
	"testing"
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
	image, err := runner.Build(BuildRequest{
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
