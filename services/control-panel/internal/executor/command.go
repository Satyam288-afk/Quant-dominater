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

	"control-panel/internal/run"
)

type commandOutput struct {
	Stdout string
	Stderr string
	Err    error
}

func runLoggedCommand(ctx context.Context, r *run.BenchmarkRun, dir string, name string, args ...string) commandOutput {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	logFile, err := os.OpenFile(filepath.Join(r.ArtifactDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return commandOutput{Err: err}
	}
	defer logFile.Close()

	_, _ = fmt.Fprintf(logFile, "\n$ (cd %s && %s %s)\n", dir, name, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = io.MultiWriter(logFile, &stdout)
	cmd.Stderr = io.MultiWriter(logFile, &stderr)

	err = cmd.Run()
	return commandOutput{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}
}
