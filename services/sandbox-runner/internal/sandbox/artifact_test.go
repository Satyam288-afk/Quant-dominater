package sandbox

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveLocalArtifact(t *testing.T) {
	got, err := resolveLocalArtifact("/repo", "local://submissions/sub_1/engine.zip")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/repo", ".artifacts", "submissions", "sub_1", "engine.zip")
	if got != want {
		t.Fatalf("resolveLocalArtifact() = %q, want %q", got, want)
	}
}

func TestResolveLocalArtifactUsesCustomSubmissionRoot(t *testing.T) {
	t.Setenv("SUBMISSION_ARTIFACT_ROOT", "/tmp/demo-submissions")
	got, err := resolveLocalArtifact("/repo", "local://submissions/sub_1/engine.zip")
	if err != nil {
		t.Fatal(err)
	}
	want := "/tmp/demo-submissions/sub_1/engine.zip"
	if got != want {
		t.Fatalf("resolveLocalArtifact() = %q, want %q", got, want)
	}
}

func TestResolveLocalArtifactRejectsTraversal(t *testing.T) {
	_, err := resolveLocalArtifact("/repo", "local://submissions/../secret.zip")
	if err == nil {
		t.Fatal("expected traversal uri to be rejected")
	}
}

func TestPrepareBuildContextSingleGoFile(t *testing.T) {
	src := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(src, []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "build")
	if err := prepareBuildContext(src, dst, "go"); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"main.go", "go.mod", "Dockerfile"} {
		if !fileExists(filepath.Join(dst, name)) {
			t.Fatalf("expected %s in generated build context", name)
		}
	}
}

func TestDefaultDockerfilePerLanguage(t *testing.T) {
	cases := map[string]string{
		"go":     "golang:1.22-alpine",
		"rust":   "rust:1-slim",
		"cpp":    "gcc:13",
		"c++":    "gcc:13",
		"binary": "COPY engine /engine",
	}
	for language, marker := range cases {
		dir := t.TempDir()
		if err := writeDefaultDockerfile(dir, language); err != nil {
			t.Fatalf("language %q: %v", language, err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
		if err != nil {
			t.Fatalf("language %q: %v", language, err)
		}
		content := string(data)
		if !strings.Contains(content, marker) {
			t.Fatalf("language %q Dockerfile missing %q:\n%s", language, marker, content)
		}
		if !strings.Contains(content, "EXPOSE 8080") {
			t.Fatalf("language %q Dockerfile missing EXPOSE 8080", language)
		}
	}

	if err := writeDefaultDockerfile(t.TempDir(), "haskell"); err == nil {
		t.Fatal("expected unsupported language to error")
	}
}

func TestUnzipRejectsTraversal(t *testing.T) {
	src := filepath.Join(t.TempDir(), "bad.zip")
	file, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	zipWriter := zip.NewWriter(file)
	writer, err := zipWriter.Create("../escape.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("nope")); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	err = unzip(src, filepath.Join(t.TempDir(), "out"))
	if err == nil || !strings.Contains(err.Error(), "escapes build context") {
		t.Fatalf("expected zip traversal error, got %v", err)
	}
}
