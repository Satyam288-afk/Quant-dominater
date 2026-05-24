package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"orchestrator/internal/api"
	"orchestrator/internal/executor"
	"orchestrator/internal/orchestrator"
	"orchestrator/internal/store"
)

func main() {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	st := store.NewJSONStore(filepath.Join(repoRoot, ".artifacts", "submissions", "index.json"))
	runner := orchestrator.NewManager(
		st,
		executor.NewLocalEngine(repoRoot),
		executor.NewBotFleet(repoRoot),
		executor.NewValidator(repoRoot),
		filepath.Join(repoRoot, ".runs"),
	)

	handler := api.NewHandler(runner, st)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("ORCHESTRATOR_ADDR")
	if addr == "" {
		addr = ":9300"
	}

	log.Printf("orchestrator listening on %s repo_root=%s", addr, repoRoot)
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
			fileExists(filepath.Join(dir, ".artifacts")) {
			return dir, nil
		}
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
