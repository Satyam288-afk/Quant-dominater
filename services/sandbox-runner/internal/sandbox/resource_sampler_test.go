package sandbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// samplePID must read a real RSS for a live process (this test binary).
func TestSamplePIDReadsSelf(t *testing.T) {
	cpu, memMB, ok := samplePID(os.Getpid())
	if !ok {
		t.Skip("ps unavailable on this host")
	}
	if memMB <= 0 {
		t.Fatalf("expected positive RSS for self, got %v MB", memMB)
	}
	if cpu < 0 {
		t.Fatalf("negative cpu%% %v", cpu)
	}
}

// The sampler must track the peak across ticks and keep resource.json current.
func TestSamplerTracksPeakAndWritesJSON(t *testing.T) {
	dir := t.TempDir()
	seq := []float64{10, 80, 30} // peak CPU should converge to 80
	var mu sync.Mutex
	var i int
	s := startSampler("test", dir, 5*time.Millisecond, func() (float64, float64, bool) {
		mu.Lock()
		defer mu.Unlock()
		v := seq[i%len(seq)]
		i++
		return v, v * 2, true
	})

	// Let it gather several samples (enough to see the 80).
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		mu.Lock()
		n := i
		mu.Unlock()
		if n >= len(seq) || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	final := s.Stop()

	if final.PeakCPUPct < 80 {
		t.Fatalf("peak cpu = %v, want >= 80", final.PeakCPUPct)
	}
	if final.PeakMemMB < 160 {
		t.Fatalf("peak mem = %v, want >= 160", final.PeakMemMB)
	}
	if final.Samples == 0 {
		t.Fatalf("no samples recorded")
	}

	data, err := os.ReadFile(filepath.Join(dir, "resource.json"))
	if err != nil {
		t.Fatalf("resource.json not written: %v", err)
	}
	var got ResourceSample
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("resource.json bad json: %v", err)
	}
	if got.PeakCPUPct != final.PeakCPUPct {
		t.Fatalf("json peak %v != returned %v", got.PeakCPUPct, final.PeakCPUPct)
	}
	if got.Source != "test" {
		t.Fatalf("source = %q, want test", got.Source)
	}
}

// A nil sampler's Stop is a safe no-op (the path the local/docker runners hit
// when sampling never started).
func TestNilSamplerStopIsSafe(t *testing.T) {
	var s *resourceSampler
	_ = s.Stop()
}
