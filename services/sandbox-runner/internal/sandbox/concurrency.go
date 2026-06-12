package sandbox

import (
	"os"
	"runtime"
	"strconv"
)

// newBuildSem returns the semaphore that bounds how many Build/Start operations
// fork a docker build + container start (or local compile + process spawn) at
// once. The fleet scales out, but every request funnels its build/start through
// this single runner; without a cap a burst of K requests forks K simultaneous
// compiles + containers contending for one 1-CPU host with no queue (thrashed
// builds / OOM). Acquire before the build/start work, release when it returns; a
// buffered channel blocks (queues) callers rather than rejecting them when full.
func newBuildSem() chan struct{} {
	return make(chan struct{}, buildConcurrency())
}

// buildConcurrency caps simultaneous build+start pipelines. Defaults to the host
// CPU count, overridable with SANDBOX_BUILD_CONCURRENCY for hosts whose real
// budget differs from their visible cores. Floored at 1.
func buildConcurrency() int {
	n := runtime.NumCPU()
	if v := os.Getenv("SANDBOX_BUILD_CONCURRENCY"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}
