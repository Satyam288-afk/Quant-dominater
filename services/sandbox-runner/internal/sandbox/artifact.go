package sandbox

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func resolveLocalArtifact(repoRoot string, artifactURI string) (string, error) {
	if artifactURI == "" {
		return "", errors.New("artifact_uri is required")
	}
	if strings.HasPrefix(artifactURI, "local://submissions/") {
		rel := strings.TrimPrefix(artifactURI, "local://submissions/")
		cleanRel := filepath.Clean(filepath.FromSlash(rel))
		if cleanRel == "." ||
			cleanRel == ".." ||
			filepath.IsAbs(cleanRel) ||
			strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
			return "", fmt.Errorf("invalid artifact uri: %s", artifactURI)
		}
		return filepath.Join(repoRoot, ".artifacts", "submissions", cleanRel), nil
	}
	if filepath.IsAbs(artifactURI) {
		return artifactURI, nil
	}
	return filepath.Join(repoRoot, artifactURI), nil
}

func prepareBuildContext(src string, dst string, language string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	switch {
	case info.IsDir():
		if err := copyDir(src, dst); err != nil {
			return err
		}
	case strings.EqualFold(filepath.Ext(src), ".zip"):
		if err := unzip(src, dst); err != nil {
			return err
		}
	default:
		if err := copyFile(src, filepath.Join(dst, filepath.Base(src))); err != nil {
			return err
		}
		if language == "go" && strings.EqualFold(filepath.Ext(src), ".go") && !fileExists(filepath.Join(dst, "go.mod")) {
			if err := os.WriteFile(filepath.Join(dst, "go.mod"), []byte("module contestant\n\ngo 1.22\n"), 0o644); err != nil {
				return err
			}
		}
	}

	if !fileExists(filepath.Join(dst, "Dockerfile")) {
		return writeDefaultDockerfile(dst, language)
	}
	return nil
}

func writeDefaultDockerfile(dir string, language string) error {
	switch language {
	case "", "go":
		return os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(`FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go mod tidy
RUN go build -o /engine .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`), 0o644)
	default:
		return fmt.Errorf("no default Dockerfile for language %q; include a Dockerfile in the artifact", language)
	}
}

func unzip(src string, dst string) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		target := filepath.Join(dst, file.Name)
		cleanDst, err := filepath.Abs(dst)
		if err != nil {
			return err
		}
		cleanTarget, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(cleanTarget, cleanDst+string(os.PathSeparator)) && cleanTarget != cleanDst {
			return fmt.Errorf("zip entry escapes build context: %s", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		srcFile, err := file.Open()
		if err != nil {
			return err
		}
		err = copyReaderToFile(srcFile, target, file.FileInfo().Mode())
		_ = srcFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func copyDir(src string, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(path, target)
	})
}

func copyFile(src string, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	return copyReaderToFile(srcFile, dst, info.Mode())
}

func copyReaderToFile(src io.Reader, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, src)
	return err
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
