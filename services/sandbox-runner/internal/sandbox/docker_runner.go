package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
	networkID   string
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

	networkPlan := dockerNetworkPlanFor(req.Spec, sandboxID)
	if networkPlan.isolated {
		created, err := cli.NetworkCreate(ctx, networkPlan.name, types.NetworkCreate{
			Driver:   "bridge",
			Internal: true,
			Labels: map[string]string{
				"iicpc.component":  "sandbox-runner",
				"iicpc.run_id":     req.RunID,
				"iicpc.sandbox_id": sandboxID,
			},
		})
		if err != nil {
			return SandboxHandle{}, fmt.Errorf("docker network create failed: %w", err)
		}
		networkPlan.id = created.ID
	}

	pidsLimit := int64(512)
	networkingConfig := &network.NetworkingConfig{}
	if networkPlan.isolated {
		networkingConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			networkPlan.name: {},
		}
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
			NetworkMode:    networkPlan.mode,
			ReadonlyRootfs: true,
			Runtime:        strings.TrimSpace(os.Getenv("SANDBOX_DOCKER_RUNTIME")),
			Resources: container.Resources{
				Memory:    parseDockerMemoryBytes(req.Spec.MemoryLimit),
				NanoCPUs:  parseDockerNanoCPUs(req.Spec.CPULimit),
				PidsLimit: &pidsLimit,
			},
			PortBindings: nat.PortMap{
				port: []nat.PortBinding{{
					HostIP:   "127.0.0.1",
					HostPort: "",
				}},
			},
			SecurityOpt: []string{"no-new-privileges:true"},
		},
		networkingConfig,
		nil,
		sandboxID,
	)
	if err != nil {
		_ = dockerRemoveNetwork(networkPlan.id)
		return SandboxHandle{}, fmt.Errorf("docker container create failed: %w", err)
	}
	containerID := createResp.ID
	if containerID == "" {
		_ = dockerRemoveNetwork(networkPlan.id)
		return SandboxHandle{}, errors.New("docker container create returned empty id")
	}
	if err := cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		_ = dockerStop(containerID)
		_ = dockerRemoveNetwork(networkPlan.id)
		return SandboxHandle{}, fmt.Errorf("docker container start failed: %w", err)
	}

	hostPort, err := dockerHostPort(containerID, port)
	if err != nil {
		_ = dockerStop(containerID)
		_ = dockerRemoveNetwork(networkPlan.id)
		return SandboxHandle{}, err
	}

	healthURL := "http://127.0.0.1:" + hostPort + "/health"
	if err := waitForHealth(context.Background(), healthURL); err != nil {
		_ = dockerStop(containerID)
		_ = dockerRemoveNetwork(networkPlan.id)
		return SandboxHandle{}, err
	}

	handle := SandboxHandle{
		SandboxID:       sandboxID,
		RunID:           req.RunID,
		ImageRef:        req.ImageRef,
		Endpoint:        "ws://127.0.0.1:" + hostPort + "/ws",
		HealthURL:       healthURL,
		Spec:            req.Spec,
		NetworkName:     networkPlan.name,
		NetworkIsolated: networkPlan.isolated,
		StartedAt:       time.Now(),
	}

	r.mu.Lock()
	r.containers[sandboxID] = &dockerSandbox{handle: handle, containerID: containerID, networkID: networkPlan.id}
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
	stopErr := dockerStop(container.containerID)
	networkErr := dockerRemoveNetwork(container.networkID)
	if stopErr != nil {
		return stopErr
	}
	return networkErr
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

type dockerNetworkPlan struct {
	mode     container.NetworkMode
	name     string
	id       string
	isolated bool
}

func dockerNetworkPlanFor(spec SandboxSpec, sandboxID string) dockerNetworkPlan {
	if spec.NetworkEgress {
		return dockerNetworkPlan{
			mode: "bridge",
			name: "bridge",
		}
	}

	name := "iicpc-" + sanitizeDockerTag(sandboxID)
	return dockerNetworkPlan{
		mode:     container.NetworkMode(name),
		name:     name,
		isolated: true,
	}
}

func dockerRemoveNetwork(networkID string) error {
	if networkID == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := newDockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	if err := cli.NetworkRemove(ctx, networkID); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("docker network remove failed: %w", err)
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
