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
		return filepath.Join(submissionArtifactRoot(repoRoot), cleanRel), nil
	}
	if filepath.IsAbs(artifactURI) {
		return artifactURI, nil
	}
	return filepath.Join(repoRoot, artifactURI), nil
}

func submissionArtifactRoot(repoRoot string) string {
	if root := os.Getenv("SUBMISSION_ARTIFACT_ROOT"); root != "" {
		abs, err := filepath.Abs(root)
		if err == nil {
			return abs
		}
		return root
	}
	return filepath.Join(repoRoot, ".artifacts", "submissions")
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

// writeDefaultDockerfile emits a hardened multi-stage Dockerfile when the
// contestant did not ship their own. Each language follows a documented
// convention so the resulting image exposes the engine on :8080. Contestants
// who need anything custom simply include a Dockerfile in their artifact, which
// always takes precedence (see prepareBuildContext).
func writeDefaultDockerfile(dir string, language string) error {
	var content string
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "", "go":
		content = `FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go mod tidy
RUN go build -o /engine .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`
	case "rust":
		// Convention: the crate produces exactly one binary; we take the first
		// top-level executable in target/release as the engine.
		content = `FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked || cargo build --release
RUN mkdir -p /out && \
    find target/release -maxdepth 1 -type f -perm -111 ! -name '*.d' -exec cp {} /out/engine \; -quit

FROM debian:stable-slim
WORKDIR /app
COPY --from=build /out/engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`
	case "cpp", "c++", "cxx":
		// Convention: CMake target or Makefile producing a binary, else all
		// translation units are compiled straight to /engine.
		content = `FROM gcc:13 AS build
WORKDIR /src
COPY . .
RUN set -eux; \
    if [ -f CMakeLists.txt ]; then \
        apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*; \
        cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j; \
        find build -maxdepth 3 -type f -perm -111 -exec cp {} /engine \; -quit; \
    elif [ -f Makefile ]; then \
        make && cp engine /engine; \
    else \
        g++ -O2 -std=c++20 -pthread -o /engine $(find . -name '*.cpp' -o -name '*.cc'); \
    fi

FROM debian:stable-slim
WORKDIR /app
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`
	case "binary", "bin":
		// Pre-compiled Linux binary shipped in the artifact as `engine`.
		content = `FROM debian:stable-slim
WORKDIR /app
COPY engine /engine
RUN chmod +x /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`
	default:
		return fmt.Errorf("no default Dockerfile for language %q; supported: go, rust, cpp, binary — or include a Dockerfile in the artifact", language)
	}
	return os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0o644)
}

// Untrusted archive limits: a contestant ZIP (capped at 64 MiB on upload) must
// not be able to exhaust the host's inodes or disk during extraction. Cap the
// entry count, the per-entry declared compression ratio, and the total
// uncompressed bytes actually written (a classic zip bomb expands a few MiB to
// tens of GB).
const (
	maxZipEntries    = 10000
	maxZipRatio      = 200
	maxZipTotalBytes = 512 << 20 // 512 MiB uncompressed across the whole archive
)

func unzip(src string, dst string) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer reader.Close()

	if len(reader.File) > maxZipEntries {
		return fmt.Errorf("zip has too many entries (%d > %d)", len(reader.File), maxZipEntries)
	}

	var total int64
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
		// Cheap screen: reject an entry whose declared ratio is absurd.
		if file.CompressedSize64 > 0 && file.UncompressedSize64/file.CompressedSize64 > maxZipRatio {
			return fmt.Errorf("zip entry %q exceeds max compression ratio", file.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		srcFile, err := file.Open()
		if err != nil {
			return err
		}
		// Hard cap on bytes actually written, so a lying header can't slip the
		// declared-size screen above. +1 so hitting the cap exactly still trips.
		written, err := copyReaderToFile(io.LimitReader(srcFile, maxZipTotalBytes-total+1), target, file.FileInfo().Mode())
		_ = srcFile.Close()
		if err != nil {
			return err
		}
		total += written
		if total > maxZipTotalBytes {
			return fmt.Errorf("zip expands beyond %d bytes; refusing to extract", int64(maxZipTotalBytes))
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
	_, err = copyReaderToFile(srcFile, dst, info.Mode())
	return err
}

// copyReaderToFile streams src to dst and returns the number of bytes written
// so callers extracting untrusted archives can enforce a cumulative size cap.
func copyReaderToFile(src io.Reader, dst string, mode os.FileMode) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, err
	}
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()
	return io.Copy(dstFile, src)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
