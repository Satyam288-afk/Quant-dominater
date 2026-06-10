package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"orchestrator/internal/api"
	"orchestrator/internal/executor"
	"orchestrator/internal/orchestrator"
	"orchestrator/internal/store"
)

func main() {
	// Cancelled on SIGTERM/SIGINT. Stops the claim worker from picking up new
	// runs and drives graceful drain of HTTP + in-flight runs.
	serverCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

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
		// Worker stops claiming new runs when serverCtx is cancelled.
		runner.StartWorker(serverCtx, pollInterval)
	}

	handler := api.NewHandler(runner, st)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("ORCHESTRATOR_ADDR")
	if addr == "" {
		addr = ":9300"
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("orchestrator listening on %s repo_root=%s sandbox_runner_url=%s run_timeout=%s auto_start=%t poll_interval=%s", addr, repoRoot, sandboxRunnerURL, runTimeout, autoStart, pollInterval)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-serverCtx.Done()
	stop() // restore default handling so a second signal force-quits
	log.Printf("shutdown signal received; stopping worker, draining HTTP, cancelling in-flight runs")

	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	// Cancel and drain in-flight runs (their goroutines persist a terminal
	// state) before the process exits — no orphaned runs or child processes.
	runner.Shutdown(shutCtx)
	log.Printf("orchestrator drained cleanly")
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
