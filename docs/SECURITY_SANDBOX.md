# Security And Sandbox Plan

The local vertical slice intentionally runs the stub engine without sandboxing. Sandbox work starts after bots, metrics, reference orderbook, and validator are working.

## First Sandbox Version

```text
POST /api/submit
  -> store ZIP in MinIO
  -> build Docker image
  -> run container
  -> health check /health
```

## Hardening Checklist

| Layer | Control |
|---|---|
| Build isolation | rootless BuildKit |
| Runtime isolation | gVisor / runsc |
| CPU fairness | cgroups v2 `cpu.max`, CPU pinning |
| Memory fairness | cgroups v2 `memory.max` |
| Syscalls | seccomp-bpf profile |
| Network | no internet egress, per-cell NetworkPolicy |
| Filesystem | read-only root filesystem where possible |
| Privileges | no privileged containers, no host network |
| Cleanup | per-run timeout and resource cleanup |

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

