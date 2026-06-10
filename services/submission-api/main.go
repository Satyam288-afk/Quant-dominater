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

	"submission-api/internal/api"
	"submission-api/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	artifactRoot := envPath("SUBMISSION_ARTIFACT_ROOT", filepath.Join(repoRoot, ".artifacts", "submissions"))
	indexPath := envPath("SUBMISSION_INDEX_PATH", filepath.Join(artifactRoot, "index.json"))

	st := store.NewJSONStore(indexPath)
	artifacts := store.NewLocalArtifactStore(artifactRoot)
	handler := api.NewHandler(st, artifacts)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	addr := os.Getenv("SUBMISSION_API_ADDR")
	if addr == "" {
		addr = ":9100"
	}

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("submission API listening on %s repo_root=%s artifact_root=%s", addr, repoRoot, artifactRoot)
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
		log.Printf("submission API drained cleanly")
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
