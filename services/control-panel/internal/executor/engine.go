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

	"control-panel/internal/run"
)

type Engine struct {
	RepoRoot string
}

func (e *Engine) Start(ctx context.Context, r *run.BenchmarkRun) (string, func(context.Context) error, error) {
	port, err := freePort()
	if err != nil {
		return "", nil, err
	}

	eventsPath := filepath.Join(r.ArtifactDir, "engine_outputs.jsonl")
	logFile, err := os.OpenFile(filepath.Join(r.ArtifactDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", nil, err
	}

	args := []string{
		"run", ".",
		"--addr", fmt.Sprintf(":%d", port),
		"--mode", r.EngineMode,
		"--events", eventsPath,
	}
	_, _ = fmt.Fprintf(logFile, "\n$ (cd %s && go %v)\n", filepath.Join(e.RepoRoot, "examples", "stub-engine"), args)

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = filepath.Join(e.RepoRoot, "examples", "stub-engine")
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return "", nil, fmt.Errorf("start stub engine: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		_ = logFile.Close()
	}()

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	if err := waitForHealth(ctx, healthURL); err != nil {
		_ = stopProcess(context.Background(), cmd, done)
		return "", nil, err
	}

	cleanup := func(ctx context.Context) error {
		return stopProcess(ctx, cmd, done)
	}

	return fmt.Sprintf("ws://127.0.0.1:%d/ws", port), cleanup, nil
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
			return fmt.Errorf("engine healthcheck timed out: %s", url)
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
	if cmd.Process == nil {
		return nil
	}

	select {
	case err := <-done:
		return ignoreExpectedExit(err)
	default:
	}

	_ = cmd.Process.Signal(os.Interrupt)

	select {
	case err := <-done:
		return ignoreExpectedExit(err)
	case <-time.After(1500 * time.Millisecond):
		_ = cmd.Process.Kill()
	}

	select {
	case err := <-done:
		return ignoreExpectedExit(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func ignoreExpectedExit(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*exec.ExitError); ok {
		return nil
	}
	return err
}
