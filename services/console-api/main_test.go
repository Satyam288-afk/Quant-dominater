package main

import (
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
