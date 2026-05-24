package executor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"orchestrator/internal/model"
)

type LocalEngine struct {
	repoRoot string
}

type EngineHandle struct {
	Endpoint string
	cleanup  func(context.Context) error
}

func NewLocalEngine(repoRoot string) *LocalEngine {
	return &LocalEngine{repoRoot: repoRoot}
}

func (e *LocalEngine) Build(ctx context.Context, run *model.BenchmarkRun, submission *model.Submission) error {
	data := fmt.Sprintf(
		"{\n  \"submission_id\": %q,\n  \"artifact_uri\": %q,\n  \"language\": %q,\n  \"builder\": \"local-stub\"\n}\n",
		submission.SubmissionID,
		submission.Artifact.URI,
		submission.Language,
	)
	return os.WriteFile(filepath.Join(run.ArtifactDir, "build.json"), []byte(data), 0o644)
}

func (e *LocalEngine) Start(ctx context.Context, run *model.BenchmarkRun) (EngineHandle, error) {
	port, err := freePort()
	if err != nil {
		return EngineHandle{}, err
	}

	logFile, err := os.OpenFile(filepath.Join(run.ArtifactDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return EngineHandle{}, err
	}

	engineDir := filepath.Join(e.repoRoot, "examples", "stub-engine")
	binaryPath := filepath.Join(run.ArtifactDir, "stub-engine")
	_, _ = fmt.Fprintf(logFile, "\n$ (cd %s && go build -o %s .)\n", engineDir, binaryPath)
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = engineDir
	buildCmd.Stdout = logFile
	buildCmd.Stderr = logFile
	if err := buildCmd.Run(); err != nil {
		_ = logFile.Close()
		return EngineHandle{}, fmt.Errorf("build stub engine: %w", err)
	}

	args := []string{
		"--addr", fmt.Sprintf(":%d", port),
		"--mode", "normal",
		"--events", filepath.Join(run.ArtifactDir, "engine_outputs.jsonl"),
	}
	_, _ = fmt.Fprintf(logFile, "\n$ %s %v\n", binaryPath, args)

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = engineDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return EngineHandle{}, err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		_ = logFile.Close()
	}()

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	if err := waitForHealth(ctx, healthURL); err != nil {
		_ = stopProcess(context.Background(), cmd, done)
		return EngineHandle{}, err
	}

	return EngineHandle{
		Endpoint: fmt.Sprintf("ws://127.0.0.1:%d/ws", port),
		cleanup: func(ctx context.Context) error {
			return stopProcess(ctx, cmd, done)
		},
	}, nil
}

func (h EngineHandle) Stop(ctx context.Context) error {
	if h.cleanup == nil {
		return nil
	}
	return h.cleanup(ctx)
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
	case <-time.After(1500 * time.Millisecond):
		_ = cmd.Process.Kill()
	}
	select {
	case err := <-done:
		return ignoreExitError(err)
	case <-ctx.Done():
		return ctx.Err()
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
