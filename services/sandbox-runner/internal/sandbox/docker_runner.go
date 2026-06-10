package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
	units "github.com/docker/go-units"
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
	sampler     *resourceSampler
	statsCli    *dockerclient.Client
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cli, err := newDockerClient()
	if err != nil {
		return ImageRef{}, err
	}
	defer cli.Close()

	buildContext, err := tarBuildContext(buildDir)
	if err != nil {
		return ImageRef{}, err
	}
	buildOptions := types.ImageBuildOptions{
		Tags:        []string{imageTag},
		Remove:      true,
		ForceRemove: true,
	}
	if dockerBuildKitEnabled() {
		buildOptions.Version = types.BuilderBuildKit
	}
	resp, err := cli.ImageBuild(ctx, buildContext, buildOptions)
	if err != nil {
		return ImageRef{}, fmt.Errorf("docker image build failed: %w", err)
	}
	defer resp.Body.Close()

	if err := writeDockerBuildLog(logFile, resp.Body); err != nil {
		return ImageRef{}, fmt.Errorf("docker image build failed: %w (see %s)", err, logPath)
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
		"--addr", ":" + dockerContainerPort,
		"--events", containerEventsPath,
	}
	if req.EngineMode != "" {
		args = append(args, "--mode", req.EngineMode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := newDockerClient()
	if err != nil {
		return SandboxHandle{}, err
	}
	defer cli.Close()

	port, err := nat.NewPort("tcp", dockerContainerPort)
	if err != nil {
		return SandboxHandle{}, err
	}

	pidsLimit := int64(512)
	memBytes := parseDockerMemoryBytes(req.Spec.MemoryLimit)
	nanoCPUs := parseDockerNanoCPUs(req.Spec.CPULimit)

	// CPU pinning: explicit cpuset from the spec or SANDBOX_CPUSET. Pinning a
	// contestant to a fixed core set removes scheduler cross-talk so latency
	// percentiles are fair and reproducible between runs.
	cpuset := strings.TrimSpace(req.Spec.CpusetCpus)
	if cpuset == "" {
		cpuset = strings.TrimSpace(os.Getenv("SANDBOX_CPUSET"))
	}
	// Honesty guard: Docker Desktop (macOS/Windows) runs containers inside a
	// LinuxKit VM that does not expose host cgroup cpuset controls, so a
	// CpusetCpus pin silently no-ops there. Rather than claim fairness we can't
	// deliver, log it and lean on the NanoCPUs quota (set below), which the VM
	// does honor, as the effective fairness control. On a real Linux host the
	// cpuset pin applies as intended.
	if cpuset != "" && runtime.GOOS != "linux" {
		log.Printf(
			"sandbox: CPU pinning (cpuset=%q) is unsupported on %s Docker; "+
				"falling back to the NanoCPUs quota for fair scheduling",
			cpuset, runtime.GOOS,
		)
		cpuset = ""
	}

	// Disable swap (MemorySwap == Memory, swappiness 0) so a contestant cannot
	// mask memory pressure by paging — cgroup memory.max is the hard wall.
	memSwap := int64(0)
	swappiness := int64(0)
	if memBytes > 0 {
		memSwap = memBytes
	}

	// Bound open files so a submission cannot exhaust host descriptors.
	const nofile = int64(4096)
	ulimits := []*units.Ulimit{{Name: "nofile", Soft: nofile, Hard: nofile}}

	// Read-only rootfs means the engine can write only to the mounted
	// artifacts dir and a small, locked-down tmpfs.
	tmpfs := map[string]string{"/tmp": "rw,noexec,nosuid,nodev,size=64m"}

	// Network: bridge with a loopback-bound published port for the bot fleet.
	// Internet / cross-contestant egress is denied in production by the per-cell
	// Kubernetes NetworkPolicy (infra/k8s). For Docker an operator can pin an
	// internal network via SANDBOX_DOCKER_NETWORK; with egress disabled (the
	// default) DNS is also black-holed as defense-in-depth.
	networkMode := container.NetworkMode("bridge")
	if n := strings.TrimSpace(os.Getenv("SANDBOX_DOCKER_NETWORK")); n != "" {
		networkMode = container.NetworkMode(n)
	}
	var dns []string
	if !req.Spec.NetworkEgress {
		dns = []string{"127.0.0.1"}
	}

	// no-new-privileges always; an explicit seccomp profile can be supplied via
	// SANDBOX_SECCOMP_PROFILE (otherwise Docker's default seccomp applies).
	securityOpt := []string{"no-new-privileges:true"}
	if profile := strings.TrimSpace(os.Getenv("SANDBOX_SECCOMP_PROFILE")); profile != "" {
		securityOpt = append(securityOpt, "seccomp="+profile)
	}

	createResp, err := cli.ContainerCreate(
		ctx,
		&container.Config{
			Image: imageTag,
			Cmd:   args,
			ExposedPorts: nat.PortSet{
				port: struct{}{},
			},
		},
		&container.HostConfig{
			AutoRemove: true,
			Binds: []string{
				filepath.Dir(eventsPath) + ":/artifacts",
			},
			CapDrop:        []string{"ALL"},
			NetworkMode:    networkMode,
			ReadonlyRootfs: true,
			Runtime:        strings.TrimSpace(os.Getenv("SANDBOX_DOCKER_RUNTIME")),
			DNS:            dns,
			Tmpfs:          tmpfs,
			Resources: container.Resources{
				Memory:           memBytes,
				MemorySwap:       memSwap,
				MemorySwappiness: &swappiness,
				NanoCPUs:         nanoCPUs,
				CpusetCpus:       cpuset,
				PidsLimit:        &pidsLimit,
				Ulimits:          ulimits,
			},
			PortBindings: nat.PortMap{
				port: []nat.PortBinding{{
					HostIP:   "127.0.0.1",
					HostPort: "",
				}},
			},
			SecurityOpt: securityOpt,
		},
		&network.NetworkingConfig{},
		nil,
		sandboxID,
	)
	if err != nil {
		return SandboxHandle{}, fmt.Errorf("docker container create failed: %w", err)
	}
	containerID := createResp.ID
	if containerID == "" {
		return SandboxHandle{}, errors.New("docker container create returned empty id")
	}
	if err := cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		_ = dockerStop(containerID)
		return SandboxHandle{}, fmt.Errorf("docker container start failed: %w", err)
	}

	hostPort, err := dockerHostPort(containerID, port)
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

	// Sample the contestant container's cgroup CPU/memory for the resource
	// score. A dedicated client (the build/start one is deferred-closed) feeds a
	// background sampler that writes resource.json into the mounted artifact dir.
	var sampler *resourceSampler
	statsCli, statsErr := newDockerClient()
	if statsErr == nil {
		var prevCPU, prevSystem uint64
		sampler = startSampler("docker", filepath.Dir(eventsPath), time.Second,
			func() (float64, float64, bool) {
				return sampleContainer(statsCli, containerID, &prevCPU, &prevSystem)
			})
	}

	r.mu.Lock()
	r.containers[sandboxID] = &dockerSandbox{handle: handle, containerID: containerID, sampler: sampler, statsCli: statsCli}
	r.mu.Unlock()

	return handle, nil
}

// sampleContainer reads one cgroup CPU%/memory(MB) sample for a container. CPU%
// is computed from the delta in total vs system CPU time across our own ticks
// (so it doesn't depend on the daemon populating precpu), scaled by online CPUs
// — the same math `docker stats` uses. The first tick has no delta and reports
// CPU 0 (memory is still valid).
func sampleContainer(cli *dockerclient.Client, containerID string, prevCPU, prevSystem *uint64) (float64, float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := cli.ContainerStats(ctx, containerID, false)
	if err != nil {
		return 0, 0, false
	}
	defer resp.Body.Close()

	var s types.StatsJSON
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return 0, 0, false
	}

	memMB := float64(s.MemoryStats.Usage) / (1024.0 * 1024.0)
	cur := s.CPUStats.CPUUsage.TotalUsage
	sys := s.CPUStats.SystemUsage

	cpu := 0.0
	if *prevCPU != 0 && sys > *prevSystem && cur > *prevCPU {
		online := float64(s.CPUStats.OnlineCPUs)
		if online == 0 {
			online = float64(len(s.CPUStats.CPUUsage.PercpuUsage))
		}
		if online == 0 {
			online = 1
		}
		cpu = float64(cur-*prevCPU) / float64(sys-*prevSystem) * online * 100.0
	}
	*prevCPU = cur
	*prevSystem = sys
	return cpu, memMB, true
}

func (r *DockerRunner) Stop(sandboxID string) error {
	r.mu.Lock()
	container := r.containers[sandboxID]
	delete(r.containers, sandboxID)
	r.mu.Unlock()

	if container == nil {
		return errors.New("sandbox not found")
	}
	if container.sampler != nil {
		container.sampler.Stop() // final resource.json flush
	}
	if container.statsCli != nil {
		_ = container.statsCli.Close()
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

func newDockerClient() (*dockerclient.Client, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return cli, nil
}

func dockerHostPort(containerID string, port nat.Port) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := newDockerClient()
	if err != nil {
		return "", err
	}
	defer cli.Close()

	var lastErr error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		inspect, err := cli.ContainerInspect(ctx, containerID)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		bindings := inspect.NetworkSettings.Ports[port]
		if len(bindings) > 0 && bindings[0].HostPort != "" {
			return bindings[0].HostPort, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return "", fmt.Errorf("docker inspect failed: %w", lastErr)
	}
	return "", fmt.Errorf("docker returned no port mapping for container %s", containerID)
}

func dockerStop(containerID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := newDockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	if err := cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("docker container remove failed: %w", err)
	}
	return nil
}

type dockerBuildMessage struct {
	Stream      string `json:"stream"`
	Error       string `json:"error"`
	ErrorDetail *struct {
		Message string `json:"message"`
	} `json:"errorDetail"`
}

func writeDockerBuildLog(logFile io.Writer, body io.Reader) error {
	decoder := json.NewDecoder(body)
	for {
		var message dockerBuildMessage
		if err := decoder.Decode(&message); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if message.Stream != "" {
			_, _ = io.WriteString(logFile, message.Stream)
		}
		if message.Error != "" {
			_, _ = io.WriteString(logFile, message.Error+"\n")
			return errors.New(message.Error)
		}
		if message.ErrorDetail != nil && message.ErrorDetail.Message != "" {
			_, _ = io.WriteString(logFile, message.ErrorDetail.Message+"\n")
			return errors.New(message.ErrorDetail.Message)
		}
	}
	return nil
}

func tarBuildContext(dir string) (io.Reader, error) {
	var buf bytes.Buffer
	writer := tar.NewWriter(&buf)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := writer.WriteHeader(header); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)
		return err
	})
	if closeErr := writer.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(buf.Bytes()), nil
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

func dockerBuildKitEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SANDBOX_DOCKER_BUILDKIT"))) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
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

func parseDockerNanoCPUs(value string) int64 {
	value = normalizeDockerCPUs(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return int64(parsed * 1_000_000_000)
}

func parseDockerMemoryBytes(value string) int64 {
	value = normalizeDockerMemory(value)
	if value == "" {
		return 0
	}
	multiplier := int64(1)
	suffix := strings.ToLower(value[len(value)-1:])
	switch suffix {
	case "k":
		multiplier = 1024
		value = value[:len(value)-1]
	case "m":
		multiplier = 1024 * 1024
		value = value[:len(value)-1]
	case "g":
		multiplier = 1024 * 1024 * 1024
		value = value[:len(value)-1]
	case "t":
		multiplier = 1024 * 1024 * 1024 * 1024
		value = value[:len(value)-1]
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return int64(parsed * float64(multiplier))
}
