package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"orchestrator/internal/model"
)

type CommandOutput struct {
	Stdout string
	Stderr string
	Err    error
}

func runLoggedCommand(ctx context.Context, run *model.BenchmarkRun, dir string, name string, args ...string) CommandOutput {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	logFile, err := os.OpenFile(filepath.Join(run.ArtifactDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return CommandOutput{Err: err}
	}
	defer logFile.Close()

	_, _ = fmt.Fprintf(logFile, "\n$ (cd %s && %s %s)\n", dir, name, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = io.MultiWriter(logFile, &stdout)
	cmd.Stderr = io.MultiWriter(logFile, &stderr)
	err = cmd.Run()

	return CommandOutput{Stdout: stdout.String(), Stderr: stderr.String(), Err: err}
}
