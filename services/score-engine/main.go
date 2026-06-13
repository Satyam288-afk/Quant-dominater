package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"score-engine/internal/scoring"
)

type Handler struct {
	runRoot string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	addr := os.Getenv("SCORE_ENGINE_ADDR")
	if addr == "" {
		addr = ":9400"
	}

	handler := &Handler{runRoot: filepath.Join(repoRoot, ".runs")}
	authToken := firstEnv("SCORE_ENGINE_AUTH_TOKEN", "SERVICE_AUTH_TOKEN")
	if strings.TrimSpace(authToken) == "" {
		if os.Getenv("REQUIRE_AUTH") == "1" {
			log.Fatalf("refusing to start: REQUIRE_AUTH=1 but no service auth token set")
		}
		log.Printf("WARNING: score-engine starting WITHOUT service auth — mutating endpoints are open; set SERVICE_AUTH_TOKEN + REQUIRE_AUTH=1 for any shared/demo deployment")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("POST /score", requireServiceAuth(handler.Score, authToken))
	mux.HandleFunc("POST /runs/{run_id}/score", requireServiceAuth(handler.ScoreRun, authToken))
	mux.HandleFunc("GET /runs/{run_id}/score", requireServiceAuth(handler.GetRunScore, authToken))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("score engine listening on %s run_root=%s", addr, handler.runRoot)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Printf("shutdown signal received; draining in-flight scoring requests")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown: %v", err)
	} else {
		log.Printf("score engine drained cleanly")
	}
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Score(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req scoring.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	result, err := h.scoreRequest(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// safeRunID extracts run_id from the path and rejects anything that isn't a
// single path segment (empty, "..", or containing a separator), so it can never
// escape h.runRoot via filepath.Join. Returns ok=false after writing a 400.
func safeRunID(w http.ResponseWriter, r *http.Request) (string, bool) {
	runID := r.PathValue("run_id")
	if runID == "" || filepath.Base(runID) != runID {
		writeError(w, http.StatusBadRequest, "invalid run_id")
		return "", false
	}
	return runID, true
}

func (h *Handler) ScoreRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := safeRunID(w, r)
	if !ok {
		return
	}
	result, err := h.scoreRequest(scoring.Request{
		RunID:       runID,
		ArtifactDir: filepath.Join(h.runRoot, runID),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) GetRunScore(w http.ResponseWriter, r *http.Request) {
	runID, ok := safeRunID(w, r)
	if !ok {
		return
	}
	var result scoring.ScoreResult
	path := filepath.Join(h.runRoot, runID, "score.json")
	if err := readJSON(path, &result); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) scoreRequest(req scoring.Request) (scoring.ScoreResult, error) {
	if req.ArtifactDir != "" {
		if err := hydrateFromArtifacts(&req); err != nil {
			return scoring.ScoreResult{}, err
		}
	}
	result := scoring.Score(req)
	if req.ArtifactDir != "" {
		if err := writeJSONFile(filepath.Join(req.ArtifactDir, "score.json"), result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func hydrateFromArtifacts(req *scoring.Request) error {
	if req.ArtifactDir == "" {
		return nil
	}
	var run scoring.RunSpec
	if err := readJSON(filepath.Join(req.ArtifactDir, "run_spec.json"), &run); err != nil {
		return err
	}
	req.RunID = firstNonEmpty(req.RunID, run.RunID)
	req.TeamID = firstNonEmpty(req.TeamID, run.TeamID)
	if req.Config.BotCount == 0 {
		req.Config = run.Config
	}
	if req.Metrics == nil {
		var metrics scoring.Metrics
		if err := readJSON(filepath.Join(req.ArtifactDir, "metrics.json"), &metrics); err != nil {
			return err
		}
		req.Metrics = &metrics
	}
	if req.Validation == nil {
		var validation scoring.ValidationResult
		if err := readJSON(filepath.Join(req.ArtifactDir, "validation.json"), &validation); err != nil {
			return err
		}
		req.Validation = &validation
	}
	return nil
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
		if fileExists(filepath.Join(dir, "Cargo.toml")) && fileExists(filepath.Join(dir, "proto", "benchmark.proto")) {
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

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func requireServiceAuth(next http.HandlerFunc, token string) http.HandlerFunc {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimSpace(r.Header.Get("Authorization"))
		want := "Bearer " + token
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
