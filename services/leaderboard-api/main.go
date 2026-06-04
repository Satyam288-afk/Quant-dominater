package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/websocket"

	"leaderboard-api/internal/board"
)

type Handler struct {
	board    *board.Board
	upgrader websocket.Upgrader
}

func main() {
	repoRoot, err := resolveRepoRoot()
	if err != nil {
		log.Fatal(err)
	}

	path := os.Getenv("LEADERBOARD_STORE_PATH")
	if path == "" {
		path = filepath.Join(repoRoot, ".leaderboard", "leaderboard.json")
	}
	b, err := board.New(path)
	if err != nil {
		log.Fatal(err)
	}

	handler := &Handler{
		board: b,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("GET /leaderboard", handler.List)
	mux.HandleFunc("POST /leaderboard/runs", handler.Upsert)
	mux.HandleFunc("GET /ws", handler.WS)

	addr := os.Getenv("LEADERBOARD_API_ADDR")
	if addr == "" {
		addr = ":9500"
	}

	log.Printf("leaderboard API listening on %s store=%s", addr, path)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) List(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.board.List())
}

func (h *Handler) Upsert(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var entry board.Entry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	entries, err := h.board.Upsert(entry)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *Handler) WS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	updates, cancel := h.board.Subscribe()
	defer cancel()

	for payload := range updates {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return
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
