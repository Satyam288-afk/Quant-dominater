package store

import (
	"os"
	"path/filepath"
	"syscall"
)

// fileLock is a cross-process advisory lock over a sidecar "<path>.lock" file.
//
// The orchestrator and submission-api run as two separate OS processes that
// share a single index.json. An in-process sync.RWMutex cannot serialise them,
// so without this an interleaved load -> mutate -> atomic-rename would silently
// drop one process's write (last rename wins) or let both processes claim and
// double-execute the same queued run. flock(LOCK_EX) is held for the entire
// read-modify-write critical section so the two processes serialise
// cluster-locally on the shared host/volume. Pure reads take LOCK_SH so they
// never observe a writer mid-update yet still run concurrently with each other.
type fileLock struct {
	f *os.File
}

func acquireFileLock(path string, exclusive bool) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	how := syscall.LOCK_EX
	if !exclusive {
		how = syscall.LOCK_SH
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}
