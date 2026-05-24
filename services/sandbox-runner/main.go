package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"sandbox-runner/internal/api"
	"sandbox-runner/internal/sandbox"
)

func main() {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	runRoot := filepath.Join(repoRoot, ".runs", "sandbox-runner")
	mode := os.Getenv("SANDBOX_RUNNER_MODE")
	if mode == "" {
		mode = "local"
	}

	var runner sandbox.Runner
	switch mode {
	case "local":
		runner = sandbox.NewLocalRunner(repoRoot, runRoot)
	case "docker":
		runner = sandbox.NewDockerRunner(repoRoot, runRoot)
	default:
		log.Fatalf("unsupported SANDBOX_RUNNER_MODE %q", mode)
	}

	handler := api.NewHandler(runner)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("SANDBOX_RUNNER_ADDR")
	if addr == "" {
		addr = ":9200"
	}

	log.Printf("sandbox runner listening on %s repo_root=%s mode=%s", addr, repoRoot, mode)
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
