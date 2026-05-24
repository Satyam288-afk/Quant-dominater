package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const dockerContainerPort = "8080"

type DockerRunner struct {
	repoRoot string
	runRoot  string

	mu         sync.Mutex
	images     map[string]ImageRef
	containers map[string]*dockerSandbox
}

type dockerSandbox struct {
	handle      SandboxHandle
	containerID string
}

func NewDockerRunner(repoRoot string, runRoot string) *DockerRunner {
	return &DockerRunner{
		repoRoot:   repoRoot,
		runRoot:    runRoot,
		images:     make(map[string]ImageRef),
		containers: make(map[string]*dockerSandbox),
	}
}

func (r *DockerRunner) Build(req BuildRequest) (ImageRef, error) {
	if req.SubmissionID == "" {
		return ImageRef{}, errors.New("submission_id is required")
	}
	if req.ArtifactURI == "" {
		return ImageRef{}, errors.New("artifact_uri is required")
	}
	if req.Language == "" {
		req.Language = "go"
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return ImageRef{}, fmt.Errorf("docker is required for SANDBOX_RUNNER_MODE=docker: %w", err)
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

	imageTag := "iicpc-sandbox:" + buildID
	logPath := filepath.Join(buildDir, "docker-build.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return ImageRef{}, err
	}
	defer logFile.Close()

	cmd := exec.Command("docker", "build", "-t", imageTag, ".")
	cmd.Dir = buildDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		return ImageRef{}, fmt.Errorf("docker build failed: %w (see %s)", err, logPath)
	}

	image := ImageRef{
		ImageRef:     "docker://" + imageTag,
		SubmissionID: req.SubmissionID,
		ArtifactURI:  req.ArtifactURI,
		Language:     req.Language,
		BuiltAt:      time.Now(),
	}

	r.mu.Lock()
	r.images[image.ImageRef] = image
	r.mu.Unlock()

	return image, nil
}

func (r *DockerRunner) Start(req StartRequest) (SandboxHandle, error) {
	if req.RunID == "" {
		return SandboxHandle{}, errors.New("run_id is required")
	}
	if req.ImageRef == "" {
		return SandboxHandle{}, errors.New("image_ref is required")
	}

	imageTag := strings.TrimPrefix(req.ImageRef, "docker://")
	if imageTag == req.ImageRef {
		return SandboxHandle{}, fmt.Errorf("image_ref must use docker:// scheme")
	}

	sandboxID := fmt.Sprintf("sandbox_%d", time.Now().UnixNano())
	dir := filepath.Join(r.runRoot, "containers", sandboxID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SandboxHandle{}, err
	}

	eventsPath := req.EventsPath
	if eventsPath == "" {
		eventsPath = filepath.Join(dir, "engine-events.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(eventsPath), 0o755); err != nil {
		return SandboxHandle{}, err
	}

	containerEventsPath := "/artifacts/" + filepath.Base(eventsPath)
	args := []string{
		"run",
		"-d",
		"--rm",
		"--name", sandboxID,
		"-p", "127.0.0.1::" + dockerContainerPort,
		"-v", filepath.Dir(eventsPath) + ":/artifacts",
	}
	if req.Spec.CPULimit != "" {
		args = append(args, "--cpus", normalizeDockerCPUs(req.Spec.CPULimit))
	}
	if req.Spec.MemoryLimit != "" {
		args = append(args, "--memory", normalizeDockerMemory(req.Spec.MemoryLimit))
	}
	args = append(args, imageTag)
	args = append(args, "--addr", ":"+dockerContainerPort, "--events", containerEventsPath)
	if req.EngineMode != "" {
		args = append(args, "--mode", req.EngineMode)
	}

	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return SandboxHandle{}, fmt.Errorf("docker run failed: %w: %s", err, string(output))
	}
	containerID := strings.TrimSpace(string(output))
	if containerID == "" {
		return SandboxHandle{}, errors.New("docker run returned empty container id")
	}

	hostPort, err := dockerHostPort(containerID)
	if err != nil {
		_ = dockerStop(containerID)
		return SandboxHandle{}, err
	}

	healthURL := "http://127.0.0.1:" + hostPort + "/health"
	if err := waitForHealth(context.Background(), healthURL); err != nil {
		_ = dockerStop(containerID)
		return SandboxHandle{}, err
	}

	handle := SandboxHandle{
		SandboxID: sandboxID,
		RunID:     req.RunID,
		ImageRef:  req.ImageRef,
		Endpoint:  "ws://127.0.0.1:" + hostPort + "/ws",
		HealthURL: healthURL,
		Spec:      req.Spec,
		StartedAt: time.Now(),
	}

	r.mu.Lock()
	r.containers[sandboxID] = &dockerSandbox{handle: handle, containerID: containerID}
	r.mu.Unlock()

	return handle, nil
}

func (r *DockerRunner) Stop(sandboxID string) error {
	r.mu.Lock()
	container := r.containers[sandboxID]
	delete(r.containers, sandboxID)
	r.mu.Unlock()

	if container == nil {
		return errors.New("sandbox not found")
	}
	return dockerStop(container.containerID)
}

func (r *DockerRunner) Get(sandboxID string) (SandboxHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	container := r.containers[sandboxID]
	if container == nil {
		return SandboxHandle{}, false
	}
	return container.handle, true
}

func (r *DockerRunner) List() []SandboxHandle {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]SandboxHandle, 0, len(r.containers))
	for _, container := range r.containers {
		out = append(out, container.handle)
	}
	return out
}

func dockerHostPort(containerID string) (string, error) {
	output, err := exec.Command("docker", "port", containerID, dockerContainerPort+"/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker port failed: %w: %s", err, string(output))
	}
	text := strings.TrimSpace(string(output))
	if text == "" {
		return "", fmt.Errorf("docker returned no port mapping for container %s", containerID)
	}
	_, port, err := net.SplitHostPort(text)
	if err != nil {
		return "", err
	}
	return port, nil
}

func dockerStop(containerID string) error {
	output, err := exec.Command("docker", "rm", "-f", containerID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker rm failed: %w: %s", err, string(output))
	}
	return nil
}

var dockerTagPattern = regexp.MustCompile(`[^a-z0-9_.-]+`)

func sanitizeDockerTag(value string) string {
	value = strings.ToLower(value)
	value = dockerTagPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-")
	if value == "" {
		return "submission"
	}
	if len(value) > 80 {
		return value[:80]
	}
	return value
}

func normalizeDockerCPUs(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "m") {
		milli := strings.TrimSuffix(value, "m")
		if milli == "" {
			return value
		}
		var parsed float64
		if _, err := fmt.Sscanf(milli, "%f", &parsed); err == nil {
			return fmt.Sprintf("%.3g", parsed/1000)
		}
	}
	return value
}

func normalizeDockerMemory(value string) string {
	value = strings.TrimSpace(value)
	replacer := strings.NewReplacer(
		"Ki", "k",
		"Mi", "m",
		"Gi", "g",
		"Ti", "t",
		"ki", "k",
		"mi", "m",
		"gi", "g",
		"ti", "t",
	)
	return replacer.Replace(value)
}
