package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"orchestrator/internal/model"
)

type SandboxEngine struct {
	baseURL string
	client  *http.Client
	token   string

	mu        sync.Mutex
	imageRefs map[string]string
}

type sandboxBuildRequest struct {
	SubmissionID string `json:"submission_id"`
	ArtifactURI  string `json:"artifact_uri"`
	Language     string `json:"language"`
}

type sandboxImageRef struct {
	ImageRef     string    `json:"image_ref"`
	SubmissionID string    `json:"submission_id"`
	ArtifactURI  string    `json:"artifact_uri"`
	Language     string    `json:"language"`
	BuiltAt      time.Time `json:"built_at"`
}

type sandboxStartRequest struct {
	RunID      string            `json:"run_id"`
	ImageRef   string            `json:"image_ref"`
	EngineMode string            `json:"engine_mode"`
	EventsPath string            `json:"events_path,omitempty"`
	Spec       model.SandboxSpec `json:"spec"`
}

type sandboxHandle struct {
	SandboxID       string            `json:"sandbox_id"`
	RunID           string            `json:"run_id"`
	ImageRef        string            `json:"image_ref"`
	Endpoint        string            `json:"endpoint"`
	HealthURL       string            `json:"health_url"`
	Spec            model.SandboxSpec `json:"spec"`
	NetworkName     string            `json:"network_name,omitempty"`
	NetworkIsolated bool              `json:"network_isolated,omitempty"`
	StartedAt       time.Time         `json:"started_at"`
}

func NewSandboxEngine(baseURL string) *SandboxEngine {
	return &SandboxEngine{
		baseURL:   strings.TrimRight(baseURL, "/"),
		client:    &http.Client{Timeout: 200 * time.Second},
		token:     firstEnv("SANDBOX_RUNNER_AUTH_TOKEN", "SERVICE_AUTH_TOKEN"),
		imageRefs: make(map[string]string),
	}
}

func (e *SandboxEngine) Build(ctx context.Context, run *model.BenchmarkRun, submission *model.Submission) error {
	req := sandboxBuildRequest{
		SubmissionID: submission.SubmissionID,
		ArtifactURI:  submission.Artifact.URI,
		Language:     submission.Language,
	}

	var image sandboxImageRef
	if err := e.postJSON(ctx, "/sandboxes/build", req, &image); err != nil {
		return err
	}
	if image.ImageRef == "" {
		return fmt.Errorf("sandbox-runner returned empty image_ref")
	}

	e.mu.Lock()
	e.imageRefs[run.RunID] = image.ImageRef
	e.mu.Unlock()

	data, err := json.MarshalIndent(image, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(run.ArtifactDir, "build.json"), data, 0o644)
}

func (e *SandboxEngine) Start(ctx context.Context, run *model.BenchmarkRun) (EngineHandle, error) {
	e.mu.Lock()
	imageRef := e.imageRefs[run.RunID]
	e.mu.Unlock()
	if imageRef == "" {
		return EngineHandle{}, fmt.Errorf("missing image ref for run %s", run.RunID)
	}

	req := sandboxStartRequest{
		RunID:      run.RunID,
		ImageRef:   imageRef,
		EngineMode: "normal",
		EventsPath: filepath.Join(run.ArtifactDir, "engine_outputs.jsonl"),
		Spec:       run.Sandbox,
	}

	var handle sandboxHandle
	if err := e.postJSON(ctx, "/sandboxes/start", req, &handle); err != nil {
		return EngineHandle{}, err
	}
	if handle.Endpoint == "" {
		return EngineHandle{}, fmt.Errorf("sandbox-runner returned empty endpoint")
	}

	data, err := json.MarshalIndent(handle, "", "  ")
	if err != nil {
		return EngineHandle{}, err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(run.ArtifactDir, "sandbox.json"), data, 0o644); err != nil {
		return EngineHandle{}, err
	}

	return EngineHandle{
		Endpoint: handle.Endpoint,
		cleanup: func(ctx context.Context) error {
			if handle.SandboxID == "" {
				return nil
			}
			return e.postJSON(ctx, "/sandboxes/"+handle.SandboxID+"/stop", map[string]string{}, nil)
		},
	}, nil
}

func (e *SandboxEngine) postJSON(ctx context.Context, path string, req any, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+e.token)
	}

	httpResp, err := e.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		var apiErr map[string]string
		_ = json.NewDecoder(httpResp.Body).Decode(&apiErr)
		if message := apiErr["error"]; message != "" {
			return fmt.Errorf("sandbox-runner %s: %s", httpResp.Status, message)
		}
		return fmt.Errorf("sandbox-runner returned %s", httpResp.Status)
	}

	if resp == nil {
		return nil
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
