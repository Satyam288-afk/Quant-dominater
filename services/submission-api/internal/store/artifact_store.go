package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"time"

	"submission-api/internal/model"
)

type LocalArtifactStore struct {
	root string
}

func NewLocalArtifactStore(root string) *LocalArtifactStore {
	return &LocalArtifactStore{root: root}
}

func (s *LocalArtifactStore) Save(ctx context.Context, submissionID string, header *multipart.FileHeader) (model.SubmissionArtifact, error) {
	src, err := header.Open()
	if err != nil {
		return model.SubmissionArtifact{}, err
	}
	defer src.Close()

	artifactID := fmt.Sprintf("art_%d", time.Now().UnixNano())
	filename := cleanFilename(header.Filename)
	if filename == "" {
		filename = artifactID + ".bin"
	}

	dir := filepath.Join(s.root, submissionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return model.SubmissionArtifact{}, err
	}

	storedName := artifactID + "_" + filename
	path := filepath.Join(dir, storedName)
	dst, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return model.SubmissionArtifact{}, err
	}
	defer dst.Close()

	hasher := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(dst, hasher), src)
	if err != nil {
		return model.SubmissionArtifact{}, err
	}

	return model.SubmissionArtifact{
		ArtifactID:  artifactID,
		URI:         "local://submissions/" + submissionID + "/" + storedName,
		SHA256:      hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes:   written,
		Filename:    filename,
		ContentType: header.Header.Get("Content-Type"),
	}, nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 128*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			m, writeErr := dst.Write(buf[:n])
			written += int64(m)
			if writeErr != nil {
				return written, writeErr
			}
			if m != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

func cleanFilename(name string) string {
	name = filepath.Base(name)
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
	return name
}
