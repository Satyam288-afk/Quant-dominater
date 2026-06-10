# Security And Sandbox

The sandbox-runner ([services/sandbox-runner](../services/sandbox-runner)) builds a
contestant submission into an OCI image and runs it under a strict, locked-down
container profile. The same `Runner` interface has a `local` mode (no Docker, for
the dev loop) and a `docker` mode (the real isolation boundary).

## Submission → Run Pipeline

```text
POST /api/submit (submission-api)
  -> artifact stored (local:// today, MinIO/S3 in cloud)
  -> sandbox-runner Build:  artifact -> build context -> multi-stage image
  -> sandbox-runner Start:  hardened container, loopback-bound :8080
  -> health check GET /health
  -> bot fleet drives ws://.../ws
```

Supported source languages get a generated multi-stage Dockerfile when the
artifact has none (a contestant Dockerfile always wins): **go**, **rust**, **cpp**,
plus **binary** for a pre-compiled engine. See `writeDefaultDockerfile`.

## Hardening Checklist

| Layer | Control | Status |
|---|---|---|
| Build isolation | BuildKit (`SANDBOX_DOCKER_BUILDKIT=1`), build logs captured, ZIP path-traversal rejected | ✅ implemented |
| Runtime isolation | pluggable OCI runtime via `SANDBOX_DOCKER_RUNTIME` (e.g. `runsc`/gVisor) | ✅ hook |
| CPU fairness | cgroups `NanoCPUs` quota (always) + **CPU pinning** (`CpusetCpus` / `SANDBOX_CPUSET`) on Linux hosts | ✅ Linux / quota-only on Docker Desktop |
| Memory fairness | cgroups `Memory` cap, **swap disabled** (`MemorySwap==Memory`, swappiness 0) | ✅ implemented |
| Process limits | `PidsLimit=512`, `nofile` ulimit 4096 | ✅ implemented |
| Syscalls | `no-new-privileges`, all caps dropped (`CapDrop: ALL`); optional profile via `SANDBOX_SECCOMP_PROFILE` (Docker default otherwise) | ✅ implemented |
| Network | loopback-bound published port; egress denied by per-cell K8s NetworkPolicy ([infra/k8s](../infra/k8s)); Docker DNS black-holed when `network_egress=false`; `SANDBOX_DOCKER_NETWORK` to pin an internal net | ✅ implemented / ✅ K8s |
| Filesystem | **read-only rootfs** + locked-down tmpfs `/tmp` (`noexec,nosuid,nodev,size=64m`); writes only to mounted artifacts dir | ✅ implemented |
| Privileges | no privileged containers, no host network, no host PID | ✅ implemented |
| Cleanup | `AutoRemove`, force-remove on stop, build/start context timeouts | ✅ implemented |

> **CPU pinning portability.** `CpusetCpus` requires host cgroup cpuset access,
> which Docker Desktop's LinuxKit VM (macOS/Windows) does not expose — a pin
> there silently no-ops. The runner detects a non-Linux host, logs it, and drops
> the cpuset so we don't claim fairness we can't deliver; the `NanoCPUs` quota
> (which the VM honors) remains the effective control. On a Linux host / EKS
> node the pin applies as intended (`docker_runner.go`).

## Allowed Network Paths

```text
Bot Fleet -> Contestant Engine
Contestant Engine -> optional telemetry endpoint
Contestant Engine -x Internet
Contestant Engine -x Other contestants
```

## Why This Comes Later

Sandboxing is required for production, but it should not be the first implementation step. The benchmark core must first prove:

1. deterministic traffic generation
2. latency/TPS measurement
3. replayable input logs
4. correctness validation
5. score generation

