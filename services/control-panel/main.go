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

	"control-panel/internal/api"
	"control-panel/internal/executor"
	"control-panel/internal/run"
	"control-panel/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

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

	authToken := firstEnv("CONTROL_PANEL_AUTH_TOKEN", "SERVICE_AUTH_TOKEN")
	if authToken == "" {
		if os.Getenv("REQUIRE_AUTH") == "1" {
			log.Fatalf("refusing to start: REQUIRE_AUTH=1 but no service auth token set")
		}
		log.Printf("WARNING: control-panel starting WITHOUT service auth — mutating endpoints are open; set SERVICE_AUTH_TOKEN + REQUIRE_AUTH=1 for any shared/demo deployment")
	}
	httpHandler := api.RequireServiceAuth(mux, authToken)

	addr := os.Getenv("CONTROL_PANEL_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	srv := &http.Server{Addr: addr, Handler: httpHandler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("control panel API listening on %s repo_root=%s", addr, repoRoot)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Printf("shutdown signal received; draining in-flight requests")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown: %v", err)
	} else {
		log.Printf("control panel API drained cleanly")
	}
	// Cancel any in-flight run goroutines (rooted at context.Background()) so
	// they unwind and persist a cancelled state instead of being orphaned.
	manager.Shutdown(shutCtx)
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
