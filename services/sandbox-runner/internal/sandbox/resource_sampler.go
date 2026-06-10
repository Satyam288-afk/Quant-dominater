package sandbox

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ResourceSample is the peak resource usage of a contestant engine over a run.
// It is written to `resource.json` in the run's artifact directory so the
// orchestrator/score-engine can fold it into the 10% resource term of the
// score. Zero CPU means "not measured" — the scorer then treats resource as
// neutral (100), so a sampling failure never unfairly penalises an engine.
type ResourceSample struct {
	PeakCPUPct float64 `json:"cpu_pct_peak"`
	PeakMemMB  float64 `json:"mem_mb_peak"`
	Samples    int     `json:"samples"`
	Source     string  `json:"source"` // "ps" (local) | "docker" (cgroup)
}

// sampleFunc returns one CPU%/memory(MB) reading, or ok=false to skip.
type sampleFunc func() (cpuPct, memMB float64, ok bool)

// resourceSampler polls a sampleFunc on an interval, tracks the peak, and keeps
// resource.json current so the reader sees fresh numbers even before Stop.
type resourceSampler struct {
	dir  string
	stop chan struct{}
	done chan struct{}

	mu   sync.Mutex
	peak ResourceSample
}

func startSampler(source, dir string, interval time.Duration, fn sampleFunc) *resourceSampler {
	s := &resourceSampler{
		dir:  dir,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	s.peak.Source = source
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				cpu, mem, ok := fn()
				if !ok {
					continue
				}
				s.mu.Lock()
				s.peak.Samples++
				if cpu > s.peak.PeakCPUPct {
					s.peak.PeakCPUPct = cpu
				}
				if mem > s.peak.PeakMemMB {
					s.peak.PeakMemMB = mem
				}
				snapshot := s.peak
				s.mu.Unlock()
				_ = writeResourceJSON(s.dir, snapshot)
			}
		}
	}()
	return s
}

// Stop ends sampling and returns the final peak (also flushed to resource.json).
func (s *resourceSampler) Stop() ResourceSample {
	if s == nil {
		return ResourceSample{}
	}
	close(s.stop)
	<-s.done
	s.mu.Lock()
	final := s.peak
	s.mu.Unlock()
	_ = writeResourceJSON(s.dir, final)
	return final
}

// samplePID reads %CPU and RSS for a pid via `ps` — cross-platform (macOS +
// Linux), no cgroup/proc dependency. ps's %CPU is a lifetime-weighted average
// on Linux and a decaying recent average on macOS; for a steady benchmark
// window either is a fair efficiency proxy, and the Docker path uses
// cgroup-accurate stats for the production number. RSS is reported in KiB.
func samplePID(pid int) (cpuPct, memMB float64, ok bool) {
	out, err := exec.Command("ps", "-o", "%cpu=,rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, 0, false
	}
	fields := strings.Fields(string(out))
	if len(fields) < 2 {
		return 0, 0, false
	}
	cpu, err1 := strconv.ParseFloat(fields[0], 64)
	rssKiB, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return cpu, rssKiB / 1024.0, true
}

func writeResourceJSON(dir string, sample ResourceSample) error {
	if dir == "" {
		return nil
	}
	data, err := json.MarshalIndent(sample, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(dir, "resource.json"), data, 0o644)
}
