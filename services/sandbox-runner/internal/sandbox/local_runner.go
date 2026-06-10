package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type LocalRunner struct {
	repoRoot string
	runRoot  string

	mu        sync.Mutex
	images    map[string]*localImage
	sandboxes map[string]*localSandbox
}

type localImage struct {
	ref        ImageRef
	buildDir   string
	binaryPath string
	language   string
}

type localSandbox struct {
	handle  SandboxHandle
	cmd     *exec.Cmd
	done    chan error
	log     *os.File
	sampler *resourceSampler
}

func NewLocalRunner(repoRoot string, runRoot string) *LocalRunner {
	return &LocalRunner{
		repoRoot:  repoRoot,
		runRoot:   runRoot,
		images:    make(map[string]*localImage),
		sandboxes: make(map[string]*localSandbox),
	}
}

func (r *LocalRunner) Build(req BuildRequest) (ImageRef, error) {
	if req.SubmissionID == "" {
		return ImageRef{}, errors.New("submission_id is required")
	}
	if req.ArtifactURI == "" {
		return ImageRef{}, errors.New("artifact_uri is required")
	}
	if req.Language == "" {
		req.Language = "go"
	}
	if req.Language != "go" {
		return ImageRef{}, fmt.Errorf("local runner only supports go artifacts, got %q", req.Language)
	}

	artifactPath, err := resolveLocalArtifact(r.repoRoot, req.ArtifactURI)
	if err != nil {
		return ImageRef{}, err
	}

	buildID := fmt.Sprintf("%s_%d", sanitizeDockerTag(req.SubmissionID), time.Now().UnixNano())
	buildDir := filepath.Join(r.runRoot, "builds", buildID)
	if err := prepareBuildContext(artifactPath, buildDir, req.Language); err != nil {
		return ImageRef{}, err
	}

	logFile, err := os.OpenFile(filepath.Join(buildDir, "local-build.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return ImageRef{}, err
	}
	defer logFile.Close()

	if fileExists(filepath.Join(buildDir, "go.mod")) {
		_, _ = fmt.Fprintf(logFile, "$ (cd %s && go mod tidy)\n", buildDir)
		tidyCmd := exec.Command("go", "mod", "tidy")
		tidyCmd.Dir = buildDir
		tidyCmd.Stdout = logFile
		tidyCmd.Stderr = logFile
		if err := tidyCmd.Run(); err != nil {
			return ImageRef{}, fmt.Errorf("go mod tidy: %w", err)
		}
	}

	binaryPath := filepath.Join(buildDir, "engine")
	_, _ = fmt.Fprintf(logFile, "$ (cd %s && go build -o %s .)\n", buildDir, binaryPath)
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = buildDir
	buildCmd.Stdout = logFile
	buildCmd.Stderr = logFile
	if err := buildCmd.Run(); err != nil {
		return ImageRef{}, fmt.Errorf("go build: %w", err)
	}

	image := ImageRef{
		ImageRef:     "local://" + buildID,
		SubmissionID: req.SubmissionID,
		ArtifactURI:  req.ArtifactURI,
		Language:     req.Language,
		BuiltAt:      time.Now(),
	}

	r.mu.Lock()
	r.images[image.ImageRef] = &localImage{
		ref:        image,
		buildDir:   buildDir,
		binaryPath: binaryPath,
		language:   req.Language,
	}
	r.mu.Unlock()

	return image, nil
}

func (r *LocalRunner) Start(req StartRequest) (SandboxHandle, error) {
	if req.RunID == "" {
		return SandboxHandle{}, errors.New("run_id is required")
	}
	if req.ImageRef == "" {
		return SandboxHandle{}, errors.New("image_ref is required")
	}
	if req.EngineMode == "" {
		req.EngineMode = "normal"
	}

	r.mu.Lock()
	image := r.images[req.ImageRef]
	r.mu.Unlock()
	if image == nil {
		return SandboxHandle{}, fmt.Errorf("unknown image_ref %q; build it first", req.ImageRef)
	}

	port, err := freePort()
	if err != nil {
		return SandboxHandle{}, err
	}

	sandboxID := fmt.Sprintf("sandbox_%d", time.Now().UnixNano())
	dir := filepath.Join(r.runRoot, sandboxID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SandboxHandle{}, err
	}

	logFile, err := os.OpenFile(filepath.Join(dir, "sandbox.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return SandboxHandle{}, err
	}

	eventsPath := req.EventsPath
	if eventsPath == "" {
		eventsPath = filepath.Join(dir, "engine-events.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(eventsPath), 0o755); err != nil {
		_ = logFile.Close()
		return SandboxHandle{}, err
	}

	args := []string{
		"--addr", fmt.Sprintf(":%d", port),
		"--mode", req.EngineMode,
		"--events", eventsPath,
	}
	_, _ = fmt.Fprintf(logFile, "$ %s %v\n", image.binaryPath, args)

	cmd := exec.Command(image.binaryPath, args...)
	cmd.Dir = image.buildDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return SandboxHandle{}, err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		_ = logFile.Close()
	}()

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	if err := waitForHealth(context.Background(), healthURL); err != nil {
		_ = stopProcess(cmd, done)
		return SandboxHandle{}, err
	}

	handle := SandboxHandle{
		SandboxID: sandboxID,
		RunID:     req.RunID,
		ImageRef:  req.ImageRef,
		Endpoint:  fmt.Sprintf("ws://127.0.0.1:%d/ws", port),
		HealthURL: healthURL,
		Spec:      req.Spec,
		StartedAt: time.Now(),
	}

	// Sample the engine process's CPU/RSS for the run's resource score. Writes
	// resource.json into the artifact dir (next to engine outputs) so the
	// orchestrator can fold peak usage into the 10% resource term.
	sampler := startSampler("ps", filepath.Dir(eventsPath), 250*time.Millisecond,
		func() (float64, float64, bool) { return samplePID(cmd.Process.Pid) })

	r.mu.Lock()
	r.sandboxes[sandboxID] = &localSandbox{handle: handle, cmd: cmd, done: done, log: logFile, sampler: sampler}
	r.mu.Unlock()

	return handle, nil
}

func (r *LocalRunner) Stop(sandboxID string) error {
	r.mu.Lock()
	sandbox := r.sandboxes[sandboxID]
	delete(r.sandboxes, sandboxID)
	r.mu.Unlock()

	if sandbox == nil {
		return errors.New("sandbox not found")
	}
	if sandbox.sampler != nil {
		sandbox.sampler.Stop() // final resource.json flush
	}
	return stopProcess(sandbox.cmd, sandbox.done)
}

func (r *LocalRunner) Get(sandboxID string) (SandboxHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sandbox := r.sandboxes[sandboxID]
	if sandbox == nil {
		return SandboxHandle{}, false
	}
	return sandbox.handle, true
}

func (r *LocalRunner) List() []SandboxHandle {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]SandboxHandle, 0, len(r.sandboxes))
	for _, sandbox := range r.sandboxes {
		out = append(out, sandbox.handle)
	}
	return out
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitForHealth(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("healthcheck timed out: %s", url)
		case <-ticker.C:
			resp, err := client.Get(url)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return nil
				}
			}
		}
	}
}

func stopProcess(cmd *exec.Cmd, done <-chan error) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	select {
	case err := <-done:
		return ignoreExitError(err)
	default:
	}

	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-done:
		return ignoreExitError(err)
	case <-time.After(1500 * time.Millisecond):
		_ = cmd.Process.Kill()
	}

	select {
	case err := <-done:
		return ignoreExitError(err)
	case <-time.After(2 * time.Second):
		return errors.New("timed out waiting for sandbox process to stop")
	}
}

func ignoreExitError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*exec.ExitError); ok {
		return nil
	}
	return err
}
