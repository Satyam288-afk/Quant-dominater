package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"control-panel/internal/api"
	"control-panel/internal/executor"
	"control-panel/internal/run"
	"control-panel/internal/store"
)

func main() {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	st := store.NewJSONStore(filepath.Join(repoRoot, ".runs", "index.json"))

	engine := &executor.Engine{RepoRoot: repoRoot}
	bots := &executor.BotFleet{RepoRoot: repoRoot}
	validator := &executor.Validator{RepoRoot: repoRoot}

	manager := run.NewManager(
		engine,
		bots,
		validator,
		st,
		filepath.Join(repoRoot, ".runs"),
	)

	handler := api.NewHandler(manager, st)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("CONTROL_PANEL_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	log.Printf("control panel API listening on %s repo_root=%s", addr, repoRoot)
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
			fileExists(filepath.Join(dir, "examples", "stub-engine", "go.mod")) {
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
