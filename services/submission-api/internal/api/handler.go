package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"submission-api/internal/model"
	"submission-api/internal/store"
)

type SubmissionStore interface {
	SaveSubmission(ctx context.Context, submission *model.Submission) error
	GetSubmission(ctx context.Context, submissionID string) (*model.Submission, error)
	ListSubmissions(ctx context.Context) ([]*model.Submission, error)
	SaveRun(ctx context.Context, run *model.BenchmarkRun) error
	GetRun(ctx context.Context, runID string) (*model.BenchmarkRun, error)
	ListRuns(ctx context.Context) ([]*model.BenchmarkRun, error)
}

type ArtifactStore interface {
	Save(ctx context.Context, submissionID string, header *multipart.FileHeader) (model.SubmissionArtifact, error)
}

type Handler struct {
	store      SubmissionStore
	artifacts  ArtifactStore
	uploadSlot chan struct{}
}

const (
	// Hard ceiling on the total request body for an artifact upload.
	maxSubmissionBytes = 64 << 20
	// In-memory buffer used by ParseMultipartForm; parts larger than this spill
	// to temp files on disk instead of being held in RAM. Keep this small so a
	// 64MiB upload does not buffer near-fully in memory.
	maxSubmissionMemory = 1 << 20
	// Cap on concurrent in-flight artifact uploads to bound peak memory/disk/FD
	// pressure when many large submissions arrive at once.
	maxConcurrentSubmissions = 8
)

func NewHandler(store SubmissionStore, artifacts ArtifactStore) *Handler {
	return &Handler{
		store:      store,
		artifacts:  artifacts,
		uploadSlot: make(chan struct{}, maxConcurrentSubmissions),
	}
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) CreateSubmission(w http.ResponseWriter, r *http.Request) {
	// Bound concurrent uploads so a burst of large submissions cannot exhaust
	// host memory/disk/FDs. Reject (rather than queue) once the pool is full.
	select {
	case h.uploadSlot <- struct{}{}:
		defer func() { <-h.uploadSlot }()
	default:
		writeError(w, http.StatusServiceUnavailable, "too many concurrent submissions; retry shortly")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxSubmissionBytes)
	if err := r.ParseMultipartForm(maxSubmissionMemory); err != nil {
		writeError(w, http.StatusBadRequest, "expected multipart/form-data with artifact file")
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	teamID := strings.TrimSpace(r.FormValue("team_id"))
	if teamID == "" {
		writeError(w, http.StatusBadRequest, "team_id is required")
		return
	}

	file, fileHeader, err := r.FormFile("artifact")
	if err != nil {
		writeError(w, http.StatusBadRequest, "artifact file is required")
		return
	}
	_ = file.Close()

	language := defaultString(r.FormValue("language"), "go")
	protocol := defaultString(r.FormValue("protocol"), "ws-json")

	now := time.Now()
	submissionID := uniqueID("sub", now)
	artifact, err := h.artifacts.Save(r.Context(), submissionID, fileHeader)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	submission := &model.Submission{
		SubmissionID:  submissionID,
		TeamID:        teamID,
		Language:      language,
		Protocol:      protocol,
		Artifact:      artifact,
		CreatedAtUnix: now.Unix(),
		CreatedAt:     now,
	}
	if err := h.store.SaveSubmission(r.Context(), submission); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, submission)
}

func (h *Handler) ListSubmissions(w http.ResponseWriter, r *http.Request) {
	submissions, err := h.store.ListSubmissions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, submissions)
}

func (h *Handler) GetSubmission(w http.ResponseWriter, r *http.Request) {
	submission, err := h.store.GetSubmission(r.Context(), r.PathValue("submission_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, submission)
}

func (h *Handler) CreateRun(w http.ResponseWriter, r *http.Request) {
	submission, err := h.store.GetSubmission(r.Context(), r.PathValue("submission_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}

	req := model.DefaultRunRequest()
	if r.Body != nil {
		defer r.Body.Close()
		if r.ContentLength != 0 {
			// Cap the run-config JSON body; it is a tiny document, so 64KiB is
			// generous while preventing an unbounded read into memory.
			r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, "invalid json")
				return
			}
		}
	}
	normalizeRunRequest(&req)

	now := time.Now()
	run := &model.BenchmarkRun{
		RunID:         uniqueID("run", now),
		SubmissionID:  submission.SubmissionID,
		TeamID:        submission.TeamID,
		Status:        model.RunStatusQueued,
		BenchmarkSeed: req.BenchmarkSeed,
		Sandbox:       req.Sandbox,
		Config:        req.Config,
		CreatedAtUnix: now.Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := h.store.SaveRun(r.Context(), run); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, run)
}

func (h *Handler) ListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) GetRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.store.GetRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// Upper bounds on contestant-supplied load. Without these, a request could ask
// for millions of bots/rate/duration and make the orchestrator spawn a fleet
// that exhausts host CPU/memory/FDs. The ceilings stay well above any real
// benchmark shape (the saturation suite runs 500 bots x 100/s).
const (
	maxBotCount    = 5000
	maxRatePerBot  = 2000
	maxDurationSec = 300
)

func normalizeRunRequest(req *model.CreateRunRequest) {
	defaults := model.DefaultRunRequest()
	if req.BenchmarkSeed == 0 {
		req.BenchmarkSeed = defaults.BenchmarkSeed
	}
	if req.Sandbox.CPULimit == "" {
		req.Sandbox.CPULimit = defaults.Sandbox.CPULimit
	}
	if req.Sandbox.MemoryLimit == "" {
		req.Sandbox.MemoryLimit = defaults.Sandbox.MemoryLimit
	}
	// Network egress is an operator decision, never a contestant's: a submission
	// must not be able to grant its own sandbox internet access (exfiltration /
	// second-stage fetch). Operators enable it server-side when they need it.
	req.Sandbox.NetworkEgress = false
	if req.Config.BotCount <= 0 {
		req.Config.BotCount = defaults.Config.BotCount
	}
	if req.Config.BotCount > maxBotCount {
		req.Config.BotCount = maxBotCount
	}
	if req.Config.RatePerBot <= 0 {
		req.Config.RatePerBot = defaults.Config.RatePerBot
	}
	if req.Config.RatePerBot > maxRatePerBot {
		req.Config.RatePerBot = maxRatePerBot
	}
	if req.Config.DurationSec <= 0 {
		req.Config.DurationSec = defaults.Config.DurationSec
	}
	if req.Config.DurationSec > maxDurationSec {
		req.Config.DurationSec = maxDurationSec
	}
	if req.Config.WarmupSec < 0 {
		req.Config.WarmupSec = 0
	}
}

func handleStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

// uniqueID builds a collision-resistant identifier from a high-resolution
// timestamp plus a crypto/rand suffix. Two requests minted in the same
// nanosecond (or on platforms with coarse clock resolution) still get distinct
// IDs, so concurrent submissions/runs never clobber each other's artifact dir.
func uniqueID(prefix string, t time.Time) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read should never fail; fall back to the timestamp alone rather
		// than panic, accepting the (vanishingly small) collision risk.
		return fmt.Sprintf("%s_%d", prefix, t.UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, t.UnixNano(), hex.EncodeToString(b[:]))
}

func defaultString(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error":  message,
		"status": strconv.Itoa(status),
	})
}
