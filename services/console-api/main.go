package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const maxSubmissionBytes = 128 << 20

type Config struct {
	Addr              string
	RepoRoot          string
	UIRoot            string
	SubmissionURL     string
	OrchestratorURL   string
	LeaderboardURL    string
	SubmissionToken   string
	OrchestratorToken string
	LeaderboardToken  string
}

type Handler struct {
	cfg    Config
	client *http.Client
}

type BenchmarkRun struct {
	RunID         string         `json:"run_id"`
	SubmissionID  string         `json:"submission_id"`
	TeamID        string         `json:"team_id"`
	Status        string         `json:"status"`
	ArtifactDir   string         `json:"artifact_dir,omitempty"`
	Valid         *bool          `json:"valid,omitempty"`
	Score         float64        `json:"score,omitempty"`
	FailureStage  string         `json:"failure_stage,omitempty"`
	FailureReason string         `json:"failure_reason,omitempty"`
	Config        map[string]any `json:"config,omitempty"`
	Sandbox       map[string]any `json:"sandbox,omitempty"`
}

type RunStartResponse struct {
	Run *BenchmarkRun `json:"run"`
}

type ArtifactFile struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	URL       string `json:"url"`
}

type ArtifactResponse struct {
	RunID       string          `json:"run_id"`
	ArtifactDir string          `json:"artifact_dir"`
	Files       []ArtifactFile  `json:"files"`
	Metrics     json.RawMessage `json:"metrics,omitempty"`
	Validation  json.RawMessage `json:"validation,omitempty"`
	Score       json.RawMessage `json:"score,omitempty"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	handler := &Handler{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.health)
	mux.HandleFunc("GET /api/health", handler.downstreamHealth)
	// Mutating routes are guarded by sameOrigin to blunt drive-by CSRF from a
	// browser: a cross-site page can POST here, but cannot set a trustworthy
	// Origin/Sec-Fetch-Site header, so those requests are rejected.
	mux.HandleFunc("POST /api/submissions", sameOrigin(handler.createSubmission))
	mux.HandleFunc("POST /api/submissions/{submission_id}/runs", sameOrigin(handler.createRun))
	mux.HandleFunc("GET /api/runs", handler.listRuns)
	mux.HandleFunc("GET /api/runs/{run_id}", handler.getRun)
	mux.HandleFunc("GET /api/runs/{run_id}/artifacts", handler.listArtifacts)
	mux.HandleFunc("GET /api/runs/{run_id}/artifacts/{name}", handler.downloadArtifact)
	mux.HandleFunc("GET /api/leaderboard", handler.leaderboard)
	mux.Handle("GET /", http.FileServer(http.Dir(cfg.UIRoot)))

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           limitInFlight(mux, maxInFlightRequests),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("console API listening on %s ui=%s submission=%s orchestrator=%s leaderboard=%s", cfg.Addr, cfg.UIRoot, cfg.SubmissionURL, cfg.OrchestratorURL, cfg.LeaderboardURL)
	log.Fatal(srv.ListenAndServe())
}

// maxInFlightRequests bounds concurrent requests to protect against connection
// exhaustion. golang.org/x/net/netutil is not a dependency, so we cap in-flight
// requests with a simple semaphore middleware instead of adding one.
const maxInFlightRequests = 256

// limitInFlight rejects requests beyond max concurrent in-flight requests with
// 503 instead of unbounded resource use.
func limitInFlight(next http.Handler, max int) http.Handler {
	sem := make(chan struct{}, max)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			writeError(w, http.StatusServiceUnavailable, "server busy: too many concurrent requests")
		}
	})
}

// sameOrigin guards mutating routes against drive-by CSRF from a browser. It
// rejects requests whose Origin header points at a different host than the one
// the request was sent to (cross-site), while allowing same-origin browser
// calls from the served console UI and non-browser clients (curl/tests) that
// send no Origin header.
//
// This is a defense-in-depth measure, not operator auth: production should
// front this service with real operator authentication.
func sameOrigin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Prefer the explicit Fetch Metadata signal when the browser sends it.
		switch r.Header.Get("Sec-Fetch-Site") {
		case "cross-site":
			writeError(w, http.StatusForbidden, "cross-site request rejected")
			return
		case "same-origin", "same-site", "none":
			next(w, r)
			return
		}
		// Fall back to the Origin header. Browsers attach it to cross-origin
		// (and most same-origin) POSTs; non-browser clients usually omit it.
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			// No Origin: not a browser cross-site request (curl, tests,
			// same-origin GETs upgraded by some browsers). Allow.
			next(w, r)
			return
		}
		if originMatchesHost(origin, r.Host) {
			next(w, r)
			return
		}
		writeError(w, http.StatusForbidden, "cross-origin request rejected")
	}
}

// originMatchesHost reports whether the scheme-host[:port] in an Origin header
// refers to the same host:port the request targeted (r.Host).
func originMatchesHost(origin string, host string) bool {
	// Origin is "scheme://host[:port]"; strip the scheme.
	if i := strings.Index(origin, "://"); i >= 0 {
		origin = origin[i+3:]
	}
	return origin != "" && origin == host
}

func loadConfig() (Config, error) {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		return Config{}, err
	}
	uiRoot := envPath("CONSOLE_UI_DIR", filepath.Join(repoRoot, "web", "console-ui"))
	return Config{
		// Default to loopback so the console is not reachable from the LAN.
		// SEC: this service attaches SERVICE_AUTH_TOKEN to every backend call and
		// has no operator auth of its own, so a LAN-reachable bind would let any
		// unauthenticated client create+start privileged runs (confused deputy).
		// Honor an explicit CONSOLE_API_ADDR override (e.g. :9700 / 0.0.0.0:9700)
		// for container deployments that front it with real operator auth.
		Addr:              envString("CONSOLE_API_ADDR", "127.0.0.1:9700"),
		RepoRoot:          repoRoot,
		UIRoot:            uiRoot,
		SubmissionURL:     strings.TrimRight(envString("SUBMISSION_API_URL", "http://127.0.0.1:9100"), "/"),
		OrchestratorURL:   strings.TrimRight(envString("ORCHESTRATOR_URL", "http://127.0.0.1:9300"), "/"),
		LeaderboardURL:    strings.TrimRight(envString("LEADERBOARD_URL", "http://127.0.0.1:9500"), "/"),
		SubmissionToken:   firstEnv("SUBMISSION_API_AUTH_TOKEN", "SERVICE_AUTH_TOKEN"),
		OrchestratorToken: firstEnv("ORCHESTRATOR_AUTH_TOKEN", "SERVICE_AUTH_TOKEN"),
		LeaderboardToken:  firstEnv("LEADERBOARD_AUTH_TOKEN", "SERVICE_AUTH_TOKEN"),
	}, nil
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "ok",
		"submission_url":   h.cfg.SubmissionURL,
		"orchestrator_url": h.cfg.OrchestratorURL,
		"leaderboard_url":  h.cfg.LeaderboardURL,
	})
}

func (h *Handler) downstreamHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"console":      serviceStatus{Status: "ok"},
		"submission":   h.checkHealth(r.Context(), h.cfg.SubmissionURL),
		"orchestrator": h.checkHealth(r.Context(), h.cfg.OrchestratorURL),
		"leaderboard":  h.checkHealth(r.Context(), h.cfg.LeaderboardURL),
	})
}

type serviceStatus struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func (h *Handler) checkHealth(ctx context.Context, baseURL string) serviceStatus {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return serviceStatus{Status: "error", Error: err.Error()}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return serviceStatus{Status: "error", Error: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return serviceStatus{Status: "error", Error: resp.Status}
	}
	return serviceStatus{Status: "ok"}
}

func (h *Handler) createSubmission(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSubmissionBytes)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.cfg.SubmissionURL+"/submissions", r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	if r.ContentLength > 0 {
		req.ContentLength = r.ContentLength
	}
	setBearerAuth(req, h.cfg.SubmissionToken)
	h.proxy(w, req)
}

func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	submissionID := strings.TrimSpace(r.PathValue("submission_id"))
	if submissionID == "" {
		writeError(w, http.StatusBadRequest, "submission_id is required")
		return
	}
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	createReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.cfg.SubmissionURL+"/submissions/"+submissionID+"/runs", bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	createReq.Header.Set("Content-Type", "application/json")
	setBearerAuth(createReq, h.cfg.SubmissionToken)

	var run BenchmarkRun
	if err := h.doJSON(createReq, http.StatusCreated, &run); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if run.RunID == "" {
		writeError(w, http.StatusBadGateway, "submission-api returned empty run_id")
		return
	}

	startReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.cfg.OrchestratorURL+"/runs/"+run.RunID+"/start", nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	setBearerAuth(startReq, h.cfg.OrchestratorToken)
	var started BenchmarkRun
	if err := h.doJSON(startReq, http.StatusAccepted, &started); err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("run created but orchestrator start failed: %s", err))
		return
	}
	if started.RunID == "" {
		started = run
	}
	writeJSON(w, http.StatusAccepted, RunStartResponse{Run: &started})
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, h.cfg.OrchestratorURL+"/runs", nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	setBearerAuth(req, h.cfg.OrchestratorToken)
	h.proxy(w, req)
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("run_id"))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, h.cfg.OrchestratorURL+"/runs/"+runID, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	setBearerAuth(req, h.cfg.OrchestratorToken)
	h.proxy(w, req)
}

func (h *Handler) leaderboard(w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, h.cfg.LeaderboardURL+"/leaderboard", nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	setBearerAuth(req, h.cfg.LeaderboardToken)
	h.proxy(w, req)
}

func (h *Handler) listArtifacts(w http.ResponseWriter, r *http.Request) {
	run, err := h.fetchRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	artifactDir, err := safeArtifactDir(h.cfg.RepoRoot, run.ArtifactDir)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	entries, err := os.ReadDir(artifactDir)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	files := make([]ArtifactFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		name := entry.Name()
		files = append(files, ArtifactFile{
			Name:      name,
			SizeBytes: info.Size(),
			URL:       "/api/runs/" + run.RunID + "/artifacts/" + name,
		})
	}

	writeJSON(w, http.StatusOK, ArtifactResponse{
		RunID:       run.RunID,
		ArtifactDir: run.ArtifactDir,
		Files:       files,
		Metrics:     readOptionalJSON(filepath.Join(artifactDir, "metrics.json")),
		Validation:  readOptionalJSON(filepath.Join(artifactDir, "validation.json")),
		Score:       readOptionalJSON(filepath.Join(artifactDir, "score.json")),
	})
}

func (h *Handler) downloadArtifact(w http.ResponseWriter, r *http.Request) {
	run, err := h.fetchRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	artifactDir, err := safeArtifactDir(h.cfg.RepoRoot, run.ArtifactDir)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := r.PathValue("name")
	if name == "" || filepath.Base(name) != name {
		writeError(w, http.StatusBadRequest, "invalid artifact name")
		return
	}
	http.ServeFile(w, r, filepath.Join(artifactDir, name))
}

func (h *Handler) fetchRun(ctx context.Context, runID string) (*BenchmarkRun, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("run_id is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.cfg.OrchestratorURL+"/runs/"+runID, nil)
	if err != nil {
		return nil, err
	}
	setBearerAuth(req, h.cfg.OrchestratorToken)
	var run BenchmarkRun
	if err := h.doJSON(req, http.StatusOK, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

func (h *Handler) proxy(w http.ResponseWriter, req *http.Request) {
	resp, err := h.client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (h *Handler) doJSON(req *http.Request, expectedStatus int, out any) error {
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != expectedStatus {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s returned %s: %s", req.URL.Host, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func safeArtifactDir(repoRoot string, artifactDir string) (string, error) {
	if strings.TrimSpace(artifactDir) == "" {
		return "", errors.New("run has no artifact_dir yet")
	}
	root, err := filepath.Abs(filepath.Join(repoRoot, ".runs"))
	if err != nil {
		return "", err
	}
	dir, err := filepath.Abs(artifactDir)
	if err != nil {
		return "", err
	}
	if dir != root && !strings.HasPrefix(dir, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifact_dir is outside .runs: %s", artifactDir)
	}
	return dir, nil
}

func readOptionalJSON(path string) json.RawMessage {
	data, err := os.ReadFile(path)
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if !json.Valid(data) {
		return nil
	}
	return json.RawMessage(data)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func envString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envPath(name string, fallback string) string {
	value := envString(name, fallback)
	abs, err := filepath.Abs(value)
	if err != nil {
		return value
	}
	return abs
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func setBearerAuth(req *http.Request, token string) {
	if token == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
}
