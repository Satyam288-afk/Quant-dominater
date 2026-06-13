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

// prepareBuildContext extracts the artifact into the build dir and ensures a
// Dockerfile is present. It returns contestantDockerfile=true when the artifact
// shipped its OWN Dockerfile (which takes precedence) — the caller treats that
// build as untrusted (no network, see DockerRunner.Build). A contestant
// Dockerfile is validated to reject build-time remote fetches (`ADD <url>`),
// which the daemon performs OUTSIDE the build network namespace and so survives
// even a network=none build.
func prepareBuildContext(src string, dst string, language string) (contestantDockerfile bool, err error) {
	info, err := os.Stat(src)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return false, err
	}

	switch {
	case info.IsDir():
		if err := copyDir(src, dst); err != nil {
			return false, err
		}
	case strings.EqualFold(filepath.Ext(src), ".zip"):
		if err := unzip(src, dst); err != nil {
			return false, err
		}
	default:
		if err := copyFile(src, filepath.Join(dst, filepath.Base(src))); err != nil {
			return false, err
		}
		if language == "go" && strings.EqualFold(filepath.Ext(src), ".go") && !fileExists(filepath.Join(dst, "go.mod")) {
			if err := os.WriteFile(filepath.Join(dst, "go.mod"), []byte("module contestant\n\ngo 1.22\n"), 0o644); err != nil {
				return false, err
			}
		}
	}

	if err := normalizeBuildContextRoot(dst); err != nil {
		return false, err
	}

	dockerfilePath := filepath.Join(dst, "Dockerfile")
	if !fileExists(dockerfilePath) {
		return false, writeDefaultDockerfile(dst, language)
	}
	if err := validateUntrustedDockerfile(dockerfilePath); err != nil {
		return true, err
	}
	return true, nil
}

// normalizeBuildContextRoot accepts the common "zip the project folder" shape:
// engine.zip -> engine/go.mod + engine/main.go. The Docker build context should
// be the project root, not the wrapper directory, so move a single build-root
// directory up one level. Archives that already have root-level build files are
// left untouched.
func normalizeBuildContextRoot(dst string) error {
	if hasBuildRootMarker(dst) {
		return nil
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		return err
	}
	var candidate os.DirEntry
	for _, entry := range entries {
		if ignoredArchiveRootEntry(entry.Name()) {
			continue
		}
		if candidate != nil {
			return nil
		}
		candidate = entry
	}
	if candidate == nil || !candidate.IsDir() {
		return nil
	}

	srcRoot := filepath.Join(dst, candidate.Name())
	if !hasBuildRootMarker(srcRoot) {
		return nil
	}
	children, err := os.ReadDir(srcRoot)
	if err != nil {
		return err
	}
	for _, child := range children {
		from := filepath.Join(srcRoot, child.Name())
		to := filepath.Join(dst, child.Name())
		if fileExists(to) {
			return fmt.Errorf("cannot unwrap build context %q: target %q already exists", candidate.Name(), child.Name())
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	return os.Remove(srcRoot)
}

func hasBuildRootMarker(dir string) bool {
	for _, name := range []string{"Dockerfile", "go.mod", "Cargo.toml", "CMakeLists.txt", "Makefile", "engine"} {
		if fileExists(filepath.Join(dir, name)) {
			return true
		}
	}
	return false
}

func ignoredArchiveRootEntry(name string) bool {
	return name == "__MACOSX" || name == ".DS_Store"
}

// validateUntrustedDockerfile rejects a contestant Dockerfile that fetches a
// remote source at build time (`ADD http://...`, `ADD https://...`). Docker's
// legacy builder fetches such URLs daemon-side, outside the build container's
// network namespace, so they exfiltrate / SSRF even when the build runs with
// NetworkMode=none. Base-image pulls (FROM) and local COPY/ADD are unaffected.
func validateUntrustedDockerfile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.EqualFold(fields[0], "ADD") {
			continue
		}
		for _, arg := range fields[1:] {
			low := strings.ToLower(arg)
			if strings.HasPrefix(low, "http://") || strings.HasPrefix(low, "https://") {
				return fmt.Errorf("contestant Dockerfile uses remote ADD (%q); build-time network fetches are not allowed", arg)
			}
		}
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
		// declared-size screen above. Write at most the remaining budget, then
		// probe one more byte: if the entry still had data, it exceeds the cap —
		// so we refuse WITHOUT having let the over-cap byte reach disk (the prior
		// `+1` sentinel let exactly one byte past the cap land before refusing).
		remaining := maxZipTotalBytes - total
		written, err := copyReaderToFile(io.LimitReader(srcFile, remaining), target, file.FileInfo().Mode())
		if err != nil {
			_ = srcFile.Close()
			return err
		}
		var probe [1]byte
		n, _ := srcFile.Read(probe[:])
		_ = srcFile.Close()
		total += written
		if n > 0 {
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
