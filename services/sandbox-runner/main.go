package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"sandbox-runner/internal/api"
	"sandbox-runner/internal/sandbox"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

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
		// Local mode runs untrusted contestant binaries directly on the host
		// with no container/cgroup isolation. Refuse to start unless an operator
		// has explicitly opted in to the unsafe path; the Docker mode is the
		// isolated, tested path for any shared/judged deployment.
		if os.Getenv("SANDBOX_ALLOW_UNSAFE_LOCAL") != "1" {
			log.Fatalf("refusing to start in 'local' mode: it runs untrusted contestant binaries on the host with NO isolation. " +
				"Use SANDBOX_RUNNER_MODE=docker for any shared/judged deployment, or set SANDBOX_ALLOW_UNSAFE_LOCAL=1 to explicitly opt in (development only).")
		}
		log.Printf("WARNING: sandbox runner starting in 'local' mode with SANDBOX_ALLOW_UNSAFE_LOCAL=1 — untrusted contestant binaries run directly on the host with NO isolation; use docker mode for shared/judged deployments")
		runner = sandbox.NewLocalRunner(repoRoot, runRoot)
	case "docker":
		dockerRunner, err := sandbox.NewDockerRunner(repoRoot, runRoot)
		if err != nil {
			log.Fatal(err)
		}
		defer dockerRunner.Close()
		runner = dockerRunner
	default:
		log.Fatalf("unsupported SANDBOX_RUNNER_MODE %q", mode)
	}

	handler := api.NewHandler(runner)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)
	token := firstEnv("SANDBOX_RUNNER_AUTH_TOKEN", "SERVICE_AUTH_TOKEN")
	if token == "" {
		if os.Getenv("REQUIRE_AUTH") == "1" {
			log.Fatalf("refusing to start: REQUIRE_AUTH=1 but no service auth token set")
		}
		log.Printf("WARNING: sandbox-runner starting WITHOUT service auth — mutating endpoints are open; set SANDBOX_RUNNER_AUTH_TOKEN (or SERVICE_AUTH_TOKEN) + REQUIRE_AUTH=1 for any shared/demo deployment")
	}
	httpHandler := api.RequireServiceAuth(mux, token)

	addr := os.Getenv("SANDBOX_RUNNER_ADDR")
	if addr == "" {
		addr = ":9200"
	}

	srv := &http.Server{Addr: addr, Handler: httpHandler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("sandbox runner listening on %s repo_root=%s mode=%s", addr, repoRoot, mode)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Printf("shutdown signal received; draining in-flight sandbox requests")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown: %v", err)
	} else {
		log.Printf("sandbox runner drained cleanly")
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

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
