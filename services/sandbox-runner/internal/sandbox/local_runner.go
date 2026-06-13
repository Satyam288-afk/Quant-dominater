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
	"syscall"
	"time"
)

type LocalRunner struct {
	repoRoot string
	runRoot  string

	// sem bounds concurrent compiles + process starts so a burst of requests
	// queues instead of forking unlimited concurrent builds on a single host.
	// See newBuildSem.
	sem chan struct{}

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
		sem:       newBuildSem(),
		images:    make(map[string]*localImage),
		sandboxes: make(map[string]*localSandbox),
	}
}

func (r *LocalRunner) Build(ctx context.Context, req BuildRequest) (ImageRef, error) {
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

	// Acquire a build slot before the heavy work (go mod tidy + go build);
	// blocks here so a burst queues instead of forking unlimited concurrent
	// compiles. Released when Build returns, on every path.
	r.sem <- struct{}{}
	defer func() { <-r.sem }()

	artifactPath, err := resolveLocalArtifact(r.repoRoot, req.ArtifactURI)
	if err != nil {
		return ImageRef{}, err
	}

	buildID := fmt.Sprintf("%s_%d", sanitizeDockerTag(req.SubmissionID), time.Now().UnixNano())
	buildDir := filepath.Join(r.runRoot, "builds", buildID)
	if _, err := prepareBuildContext(artifactPath, buildDir, req.Language); err != nil {
		return ImageRef{}, err
	}

	logFile, err := os.OpenFile(filepath.Join(buildDir, "local-build.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return ImageRef{}, err
	}
	defer logFile.Close()

	if fileExists(filepath.Join(buildDir, "go.mod")) {
		_, _ = fmt.Fprintf(logFile, "$ (cd %s && go mod tidy)\n", buildDir)
		if err := runBuildStep(ctx, buildDir, logFile, "go", "mod", "tidy"); err != nil {
			return ImageRef{}, fmt.Errorf("go mod tidy: %w", err)
		}
	}

	binaryPath := filepath.Join(buildDir, "engine")
	_, _ = fmt.Fprintf(logFile, "$ (cd %s && go build -o %s .)\n", buildDir, binaryPath)
	if err := runBuildStep(ctx, buildDir, logFile, "go", "build", "-o", binaryPath, "."); err != nil {
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

// buildStepTimeout bounds each compile/dependency step. Untrusted contestant
// code is being built on the host in local mode; a pathological source or a
// dependency-resolution hang must not wedge a build worker forever.
const buildStepTimeout = 120 * time.Second

// runBuildStep runs one build command against untrusted contestant code with
// three guards: a wall-clock timeout; CGO disabled, so a malicious `import "C"`
// can't drive the host C toolchain with attacker-controlled #cgo/#include
// directives at build time; and a dedicated process group killed as a unit on
// timeout — the go toolchain spawns compile/link children that a plain cancel
// would orphan.
func runBuildStep(parent context.Context, dir string, logFile *os.File, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(parent, buildStepTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("%s timed out after %s", name, buildStepTimeout)
		}
		return err
	}
	return nil
}

func (r *LocalRunner) Start(ctx context.Context, req StartRequest) (SandboxHandle, error) {
	if req.RunID == "" {
		return SandboxHandle{}, errors.New("run_id is required")
	}
	if req.ImageRef == "" {
		return SandboxHandle{}, errors.New("image_ref is required")
	}
	r.mu.Lock()
	image := r.images[req.ImageRef]
	r.mu.Unlock()
	if image == nil {
		return SandboxHandle{}, fmt.Errorf("unknown image_ref %q; build it first", req.ImageRef)
	}

	// Acquire a slot before spawning the engine process so a burst queues
	// instead of forking unlimited concurrent processes. Released when Start
	// returns, on every path.
	r.sem <- struct{}{}
	defer func() { <-r.sem }()

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
		"--events", eventsPath,
	}
	if req.EngineMode != "" {
		args = append(args, "--mode", req.EngineMode)
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
	if err := waitForHealth(ctx, healthURL); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = stopProcess(cleanupCtx, cmd, done)
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

func (r *LocalRunner) Stop(ctx context.Context, sandboxID string) error {
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
	return stopProcess(ctx, sandbox.cmd, sandbox.done)
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

func stopProcess(ctx context.Context, cmd *exec.Cmd, done <-chan error) error {
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
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1500 * time.Millisecond):
		_ = cmd.Process.Kill()
	}

	select {
	case err := <-done:
		return ignoreExitError(err)
	case <-ctx.Done():
		return ctx.Err()
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
