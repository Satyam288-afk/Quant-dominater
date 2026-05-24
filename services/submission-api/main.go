package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"submission-api/internal/api"
	"submission-api/internal/store"
)

func main() {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	artifactRoot := filepath.Join(repoRoot, ".artifacts", "submissions")
	indexPath := filepath.Join(artifactRoot, "index.json")

	st := store.NewJSONStore(indexPath)
	artifacts := store.NewLocalArtifactStore(artifactRoot)
	handler := api.NewHandler(st, artifacts)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("SUBMISSION_API_ADDR")
	if addr == "" {
		addr = ":9100"
	}

	log.Printf("submission API listening on %s repo_root=%s artifact_root=%s", addr, repoRoot, artifactRoot)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func resolveRepoRoot() (string, error) {
	if repoRoot := os.Getenv("REPO_ROOT"); repoRoot != "" {
		return filepath.Abs(repoRoot)
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if fileExists(filepath.Join(dir, "Cargo.toml")) &&
			fileExists(filepath.Join(dir, "proto", "benchmark.proto")) {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", errors.New("could not find repo root; set REPO_ROOT")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
