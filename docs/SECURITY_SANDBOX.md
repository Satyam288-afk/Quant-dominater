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
| Build isolation | ZIP path-traversal **and** zip-bomb (ratio / total-size / entry-count) rejected; a **contestant-supplied Dockerfile builds with `--network=none`** and remote `ADD <url>` is rejected (no build-time egress / SSRF / payload pull); **every build is memory/CPU/pid(`nproc`)/fd capped** (legacy builder); local-mode build runs `CGO_ENABLED=0` + wall-clock timeout + process-group kill | ✅ implemented |
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

## Current Docker Enforcement

Docker mode currently applies:

```text
cap_drop=ALL
no-new-privileges=true
read-only root filesystem
PID limit
CPU limit from sandbox.cpu_limit
memory limit from sandbox.memory_limit
localhost-only published engine port
per-sandbox internal Docker network when network_egress=false
container/network cleanup after run completion
```

This is still not equivalent to a hardened multi-tenant cloud sandbox. The next
security step is to run the same container under gVisor/rootless BuildKit and
prove egress denial with an automated malicious-submission fixture.

## Hostile-Submission & Measurement-Plane Hardening

A contestant controls two things: the bytes in their **ZIP** and the bytes their
**engine sends back** over the WebSocket. Both are treated as adversarial. The
controls below were added after a red-team sweep that reproduced each issue.

**Upload → build (untrusted archive + untrusted source).**
- **Zip-bomb defence** (`sandbox/artifact.go`): the extractor caps entry count
  (10k), per-entry declared compression ratio (200×), and the cumulative
  uncompressed bytes actually written (512 MiB, via an `io.LimitReader` so a
  lying header can't slip the screen). Reproduced bombs: a ~204 KB archive that
  expanded to 200 MB, and a 9.6 MB archive of 100k files — both now rejected.
  Path-traversal / absolute / symlink entries were already neutralised.
- **Build-time RCE containment** (`sandbox/local_runner.go`): building untrusted
  Go is arbitrary code execution *at build time* via cgo (`import "C"` driving
  the host C toolchain with attacker `#cgo`/`#include` flags). Local-mode builds
  now run with `CGO_ENABLED=0`, a 120 s per-step timeout, and a dedicated process
  group killed as a unit on timeout (the go toolchain spawns children a plain
  cancel would orphan). Docker mode remains the real isolation boundary; this
  hardens the dependency-free local path the demos use.
- **Load clamps** (`submission-api`, mirrored in `orchestrator`): `bot_count`,
  `rate_per_bot`, `duration_sec` are clamped to sane ceilings (5000 / 2000 /
  300) so a request can't make the orchestrator spawn a host-exhausting fleet;
  `network_egress` is forced off for contestant-supplied specs (an operator
  decision, never a submission's).

**Measurement plane (untrusted engine output).**
- **Ack gating** (`bot-fleet`): latency/throughput/stability now count *only*
  acks that match an order the bot actually had outstanding. A hostile engine
  flooding fabricated or duplicate acks previously inflated peak-TPS / stability
  and grew the per-bot latency buffers without bound (a proven OOM — 20 M acks ≈
  152 MB/bot). Fabricated acks are now dropped at the door.
- **WebSocket frame caps** (`bot-fleet`): inbound messages are capped at 256 KiB
  (64 KiB/frame); tungstenite's 64 MiB default otherwise let one engine pin
  gigabytes across many connections.
- **Validator robustness** (`validator/edge_cases.rs`): fill quantities are
  validated (`qty > 0`) and accumulated with saturating arithmetic. A crafted
  `qty` near `i64::MAX` previously panicked the single-threaded validator (no
  `validation.json` at all) in debug and wrapped silently in release; a negative
  `qty` could "subtract" its way out of an over-fill. Both are now flagged
  (`INVALID_FILL_QTY`) and cannot corrupt the over-fill check.

### Known trust boundary — `engine_seq` arrival order

Under concurrent multi-bot load the bots' *send* order is not the order the
engine processed orders in (connections race), so replaying the reference book
in send order would flag a *correct* engine for price-time violations. The
validator therefore reconstructs arrival order from the engine's own ack
`engine_seq` (`validator/replay.rs`). That means a contestant who deliberately
mis-pairs two **same-price** trades *and* forges a self-consistent `engine_seq`
to match can make the reference reproduce the wrong pairing — the engine is
trusted as its own sequencer.

This is a deliberate, documented limitation, not an oversight. The honest fix is
architectural — the fleet must stamp an authoritative *receive* sequence at the
wire so arrival order no longer depends on engine-supplied data — and is too
invasive to land safely under the hackathon deadline without risking false
positives against correct engines (the worse failure). It does **not** weaken
the demonstrable correctness story: a *broken* engine is still caught
(`run-price-time-proof.sh`), and `engine_seq` monotonicity, two-sided fill
consistency, unknown-order fills and over-fills are all independently checked.

## Live Docker red-team (the build phase, the scoring channel, control-plane authz)

A four-front adversarial audit drove the **real** Docker isolation boundary
(Docker 29.1.5) as a malicious contestant — escape, host-read, exfil, resource
DoS, score gaming, and API abuse — with reproducible PoCs. The **runtime**
container held on every probe: `CapDrop ALL` (CapEff=0), `no-new-privileges`,
read-only rootfs, internal network with black-holed DNS (egress fully blocked),
no `docker.sock`, no host-FS traversal, and every resource cap (mem OOM, pid
512, tmpfs 64m, CPU quota) triggered. Archive extraction held too. The holes
were elsewhere and are now closed:

- **Build ran unsandboxed** (`docker_runner.go`). A contestant Dockerfile always
  wins, and its `RUN` steps executed as root with **full internet egress** and
  **no resource limits** — build-time RCE, exfiltration/SSRF, and a fork-bomb /
  memory-balloon that could take down the whole host VM. Now: a contestant
  Dockerfile builds with `NetworkMode=none`, remote `ADD <url>` (which the
  daemon fetches *outside* the build netns, surviving `network=none`) is
  rejected at `prepareBuildContext`, and **every** build is capped
  (memory, CPU, `nproc` fork-bomb guard, `nofile`). Trusted *generated* default
  Dockerfiles keep egress (they legitimately pull modules/base layers).
- **Forgeable scoring input** (`docker_runner.go`, `manager.go`). The artifact
  dir was bind-mounted read-write, and the resource sampler wrote the trusted
  `resource.json` (the 10 % `ResourceScore`) into that same dir — so a hostile
  root engine could overwrite it with flattering numbers (a deterministic
  +7.5 score swing). The engine's writable mount is now a **dedicated
  `engine-out/` subdir**; `resource.json` lives in the parent run dir, outside
  the container's reach.
- **Auth bypassed for every GET** (`orchestrator/internal/api/auth.go`). A
  one-character bug (`||` where the sibling services use `&&`) exempted *all*
  GET requests from the bearer-token check, leaking every team's run configs,
  scores, seeds and host paths. Fixed and covered by a regression test.
- **Zip byte-cap off-by-one** (`artifact.go`): the cumulative 512 MiB cap let
  exactly one over-cap byte reach disk before refusing; now it refuses without
  writing the over-cap byte.

### Residual / documented (not closed under the deadline)

- **BuildKit drops the build memory/CPU caps.** The `nproc`/`nofile` ulimits and
  `network=none` hold under BuildKit, but `SANDBOX_DOCKER_BUILDKIT=1` makes the
  daemon ignore the `Memory`/`CPUQuota` build options. The **default** path
  (legacy builder) applies all caps; enabling BuildKit needs a `buildkitd`
  `--memory` path before it is equivalent.
- **Writable artifact mount has no disk quota.** `engine-out/` is host-VM-disk
  backed; a hostile engine can still fill it (host disk DoS) — size it with a
  quota-backed volume or a bounded tmpfs in production.
- **Docker mode runs the engine as uid 0** (no `--user`), unlike the K8s
  template's `runAsUser 65532`. Well-mitigated by `CapDrop ALL` +
  `no-new-privileges` + seccomp (every privileged op denied), so it is
  defense-in-depth only; the doc's "1:1 parity" claim is the gap.
- **`engine_seq` arrival-order gaming** — the boundary documented above. A
  `ts_ns`-primary replay was considered and **rejected**: `ts_ns` is fleet
  *send* time, which under concurrency is not arrival order, so making it
  authoritative would false-positive correct engines — the worse failure.

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
