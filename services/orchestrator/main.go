package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

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

	storePath := envPath("ORCHESTRATOR_STORE_PATH", "")
	if storePath == "" {
		storePath = envPath("SUBMISSION_INDEX_PATH", filepath.Join(repoRoot, ".artifacts", "submissions", "index.json"))
	}
	st := store.NewJSONStore(storePath)
	sandboxRunnerURL := os.Getenv("SANDBOX_RUNNER_URL")
	if sandboxRunnerURL == "" {
		sandboxRunnerURL = "http://127.0.0.1:9200"
	}
	runTimeout := 3 * time.Minute
	if value := os.Getenv("ORCHESTRATOR_RUN_TIMEOUT"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			log.Fatalf("invalid ORCHESTRATOR_RUN_TIMEOUT %q: %v", value, err)
		}
		runTimeout = parsed
	}
	runner := orchestrator.NewManager(
		st,
		executor.NewSandboxEngine(sandboxRunnerURL),
		executor.NewBotFleet(repoRoot),
		executor.NewValidator(repoRoot),
		filepath.Join(repoRoot, ".runs"),
		runTimeout,
	)
	autoStart := envBool("ORCHESTRATOR_AUTO_START", true)
	pollInterval := 2 * time.Second
	if value := os.Getenv("ORCHESTRATOR_POLL_INTERVAL"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			log.Fatalf("invalid ORCHESTRATOR_POLL_INTERVAL %q: %v", value, err)
		}
		pollInterval = parsed
	}
	if leaderboardURL := os.Getenv("LEADERBOARD_URL"); leaderboardURL != "" {
		runner.SetLeaderboardPublisher(executor.NewLeaderboardPublisher(leaderboardURL))
	}
	if autoStart {
		runner.StartWorker(context.Background(), pollInterval)
	}

	handler := api.NewHandler(runner, st)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("ORCHESTRATOR_ADDR")
	if addr == "" {
		addr = ":9300"
	}

	log.Printf("orchestrator listening on %s repo_root=%s sandbox_runner_url=%s run_timeout=%s auto_start=%t poll_interval=%s", addr, repoRoot, sandboxRunnerURL, runTimeout, autoStart, pollInterval)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
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

func envPath(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return value
	}
	return abs
}
