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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"leaderboard-api/internal/board"
	"leaderboard-api/internal/redisboard"
)

// Store is the backend-agnostic surface the HTTP handler needs. Both the
// file-backed board and the Redis-backed board satisfy it.
type Store interface {
	List() []board.Entry
	Upsert(board.Entry) ([]board.Entry, error)
	Subscribe() (<-chan []byte, func())
}

// ReadinessReporter is implemented by backends that depend on an external
// data store (the Redis board) so /ready can reflect dependency health —
// liveness (/health) vs readiness (/ready). The file board doesn't implement
// it, so /ready is trivially ready there (no external dependency).
type ReadinessReporter interface {
	Ready() (bool, map[string]any)
	StaleAgeMS() int64
}

// Compile-time guarantee that the Redis backend satisfies the readiness surface
// (so /ready and the freshness header light up in redis mode, not just compile).
var _ ReadinessReporter = (*redisboard.Board)(nil)

type Handler struct {
	store    Store
	live     *redisboard.Board // non-nil only in redis mode; serves /runs/{id}/live
	upgrader websocket.Upgrader
	// rootCtx is cancelled when the process receives SIGTERM/SIGINT; open
	// WebSocket loops watch it so they can send a Close frame and drain
	// cleanly instead of being severed mid-deploy.
	rootCtx context.Context
}

func main() {
	// Cancelled on SIGTERM/SIGINT (every k8s rolling deploy / pod eviction).
	// Drives graceful shutdown: stop accepting, drain in-flight HTTP + WS,
	// then run deferred cleanup (rb.Close) — none of which a log.Fatal on
	// ListenAndServe would allow.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	handler := &Handler{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		rootCtx: rootCtx,
	}

	// Repo root locates the static UI dir (served at /) and the file-backed store.
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	// Backend selection. `redis` serves the live leaderboard straight from the
	// data plane (ZSET + scorecards written by the score-engine). `file` is the
	// dependency-free fallback used by the local slice and unit tests.
	backend := os.Getenv("LEADERBOARD_BACKEND")
	redisURL := os.Getenv("REDIS_URL")
	if backend == "" && redisURL != "" {
		backend = "redis"
	}

	var source string
	switch backend {
	case "redis":
		if redisURL == "" {
			redisURL = "redis://localhost:56379/"
		}
		interval := 500 * time.Millisecond
		if v := os.Getenv("LEADERBOARD_POLL_MS"); v != "" {
			if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
				interval = time.Duration(ms) * time.Millisecond
			}
		}
		rb, err := redisboard.New(redisURL, interval)
		if err != nil {
			log.Fatalf("redis backend: %v", err)
		}
		defer rb.Close()
		handler.store = rb
		handler.live = rb
		source = "redis:" + redisURL
	default:
		path := os.Getenv("LEADERBOARD_STORE_PATH")
		if path == "" {
			path = filepath.Join(repoRoot, ".leaderboard", "leaderboard.json")
		}
		b, err := board.New(path)
		if err != nil {
			log.Fatal(err)
		}
		handler.store = b
		source = "file:" + path
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("GET /ready", handler.Ready)
	mux.HandleFunc("GET /leaderboard", handler.List)
	mux.HandleFunc("POST /leaderboard/runs", requireServiceAuth(handler.Upsert, firstEnv("LEADERBOARD_AUTH_TOKEN", "SERVICE_AUTH_TOKEN")))
	mux.HandleFunc("GET /runs/{id}/live", handler.LiveRun)
	mux.HandleFunc("GET /runs/{id}/timeseries", handler.RunTimeseries)
	mux.HandleFunc("GET /ws", handler.WS)
	uiDir := os.Getenv("LEADERBOARD_UI_DIR")
	if uiDir == "" {
		uiDir = filepath.Join(repoRoot, "web", "leaderboard-ui")
	}
	if fileExists(filepath.Join(uiDir, "index.html")) {
		mux.Handle("GET /", http.FileServer(http.Dir(uiDir)))
	}

	// Optionally serve the polished React board (Vite `web/dist`) when it has
	// been built, mirroring the static-console FileServer above. It coexists
	// with the legacy console at `/`: the SPA index is mounted at /board/ and
	// its hashed bundles at /assets/ (the absolute paths Vite emits). Absent a
	// build, this is a no-op, so headless / CI runs are unaffected.
	boardDir := os.Getenv("LEADERBOARD_BOARD_DIR")
	if boardDir == "" {
		boardDir = filepath.Join(repoRoot, "web", "dist")
	}
	if fileExists(filepath.Join(boardDir, "index.html")) {
		boardFS := http.FileServer(http.Dir(boardDir))
		mux.Handle("GET /board/", http.StripPrefix("/board/", boardFS))
		mux.Handle("GET /assets/", boardFS)
	}

	addr := os.Getenv("LEADERBOARD_API_ADDR")
	if addr == "" {
		addr = ":9500"
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("leaderboard API listening on %s store=%s", addr, source)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	// Block until SIGTERM/SIGINT, then drain.
	<-rootCtx.Done()
	stop() // restore default signal handling so a second Ctrl-C force-quits
	log.Printf("shutdown signal received; draining HTTP + WebSocket connections")

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("graceful shutdown timed out: %v", err)
	} else {
		log.Printf("http server drained cleanly")
	}
	// Deferred rb.Close() runs now (main returns), closing the Redis pool —
	// which a log.Fatal would have skipped.
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready is the dependency-aware readiness probe: 200 while live data is fresh,
// 503 when the backing store (Redis) has gone stale, so k8s pulls the pod from
// the load balancer without killing it (that's what /health, liveness, is for).
// The file backend has no external dependency, so it is always ready.
func (h *Handler) Ready(w http.ResponseWriter, _ *http.Request) {
	if rr, ok := h.store.(ReadinessReporter); ok {
		ready, detail := rr.Ready()
		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, detail)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (h *Handler) List(w http.ResponseWriter, _ *http.Request) {
	// Non-breaking freshness signal: clients/UI can read the data's age without
	// changing the array body they already parse.
	if rr, ok := h.store.(ReadinessReporter); ok {
		w.Header().Set("X-Leaderboard-Age-Ms", strconv.FormatInt(rr.StaleAgeMS(), 10))
	}
	writeJSON(w, http.StatusOK, h.store.List())
}

func (h *Handler) Upsert(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var entry board.Entry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	entries, err := h.store.Upsert(entry)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

// LiveRun exposes the ingester's in-flight counters for a run so the UI can
// show progress before a run is scored. Only available in redis backend mode.
func (h *Handler) LiveRun(w http.ResponseWriter, r *http.Request) {
	if h.live == nil {
		writeError(w, http.StatusNotImplemented, "live run metrics require the redis backend")
		return
	}
	runID := r.PathValue("id")
	if runID == "" {
		writeError(w, http.StatusBadRequest, "run id is required")
		return
	}
	metrics, err := h.live.LiveRunMetrics(runID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_id": runID, "metrics": metrics})
}

// RunTimeseries returns the per-interval latency/throughput time-series for a
// run so the UI can chart how latency and TPS move over the run. Redis-only.
func (h *Handler) RunTimeseries(w http.ResponseWriter, r *http.Request) {
	if h.live == nil {
		writeError(w, http.StatusNotImplemented, "time-series requires the redis backend")
		return
	}
	runID := r.PathValue("id")
	if runID == "" {
		writeError(w, http.StatusBadRequest, "run id is required")
		return
	}
	points, err := h.live.RunTimeseries(runID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_id": runID, "points": points})
}

func (h *Handler) WS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	updates, cancel := h.store.Subscribe()
	defer cancel()

	ctx := h.rootCtx
	if ctx == nil {
		ctx = context.Background()
	}

	// WebSocket connections are hijacked, so srv.Shutdown() does not drain
	// them — we watch rootCtx ourselves and send a proper Close frame on
	// shutdown so clients exit cleanly instead of hanging / retry-storming.
	for {
		select {
		case <-ctx.Done():
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
				time.Now().Add(time.Second),
			)
			return
		case payload, ok := <-updates:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		}
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
