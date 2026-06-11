# Known Residuals & Risk Register

One consolidated register of every honestly-disclosed residual in the platform.
Each item is something we found, understood, and made a deliberate call to defer
under the hackathon deadline — gathered here so the discipline is visible in one
place instead of scattered across the design docs. Severity is the *residual*
risk after the listed mitigation, in the demo/benchmark context.

| # | Residual | Rubric pillar | Current mitigation | Severity | Why deferred |
|---|---|---|---|---|---|
| 1 | **BuildKit drops the build memory/CPU caps.** `SANDBOX_DOCKER_BUILDKIT=1` makes the daemon ignore the `Memory`/`CPUQuota` build options. | Security / sandbox isolation | The **default** (legacy) builder applies all caps; under BuildKit the `nproc`/`nofile` ulimits and `network=none` still hold. ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md#residual--documented-not-closed-under-the-deadline)) | Low | Equivalence needs a `buildkitd --memory` path; the default path is already capped, so no demo runs unprotected. |
| 2 | **Writable artifact mount has no disk quota.** A hostile engine can fill `engine-out/` (host-VM-disk DoS). | Security / resource fairness | Mount is a dedicated `engine-out/` subdir isolated from the trusted `resource.json`; mem/CPU/PID/tmpfs caps still bound the process. ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md#residual--documented-not-closed-under-the-deadline)) | Low | Production fix is a quota-backed volume or bounded tmpfs — an infra change, not a code change. |
| 3 | **Docker mode runs the engine as uid 0** (no `--user`), unlike the K8s template's `runAsUser 65532`. | Security / sandbox isolation | `CapDrop: ALL` + `no-new-privileges` + seccomp deny every privileged op, so this is defense-in-depth only; the K8s path already runs non-root. ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md#residual--documented-not-closed-under-the-deadline)) | Low | Adding a non-root `--user` to the Docker path is safe but untested against every supported build (go/rust/cpp/binary) under the deadline; the cap-drop makes uid 0 non-privileged regardless. |
| 4 | **`engine_seq` arrival-order trust boundary.** A contestant who forges a self-consistent `engine_seq` could make the reference reproduce a wrong same-price pairing — the engine is trusted as its own sequencer. | Correctness gate | A *broken* engine is still caught (`run-price-time-proof.sh`); `engine_seq` monotonicity, two-sided fill consistency, unknown-order fills and over-fills are independently checked. ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md#known-trust-boundary--engine_seq-arrival-order)) | Medium | The honest fix is architectural (a wire-stamped authoritative *receive* sequence). A `ts_ns`-primary replay was considered and **rejected** because it would false-positive *correct* engines under concurrency — the worse failure. |
| 5 | **Multi-node bot-fleet scale-out is measured on `kind`, but not at the 10k-bot ceiling.** The Indexed-Job scale-out was load-tested on a 4-node cluster (2/4/8 pods → linear ~8k orders/s, zero drops, disjoint pod-index shards); the full 10k-bot / ~200k-orders/s regime still extrapolates from that. | Throughput / scale | Demonstrated linear cross-pod scaling + globally-unique `--pod-index` IDs on real K8s nodes ([BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#multi-node-horizontal-scale-measured-on-kind), [scripts/run-kind-scale-proof.sh](../scripts/run-kind-scale-proof.sh)); single-host ceiling (~250k orders/s) also measured. | Low | A 10k-bot run needs a multi-core cluster beyond the laptop; the kind sweep proves linearity, the absolute ceiling rises with node/per-pod count. |
| 6 | **Multi-node ingester throughput is unmeasured**, and the single Redpanda broker (`--smp=2`) is the current write ceiling. | Throughput / scale | Ingester is a Kafka consumer group with an HPA to the partition count; broker is multi-core. Partitions parallelize consumers but a single broker bounds writes. ([infra/k8s/README.md](../infra/k8s/README.md)) | Low | Multi-broker scale needs the Redpanda Operator (production note already in the K8s README); not exercised under the deadline. |
| 7 | **No soak / long-duration run.** Results are 5–60 s windows; steady-state behavior over hours is uncharacterized (e.g. resting book-depth grows with run length). | Stability | TimescaleDB compresses after 1 h and retains 1 day so volume can't fill; the depth-vs-duration relationship is documented. ([BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#correctness-under-concurrency)) | Low | Soak testing is a time budget item, not a missing capability; the retention policy already bounds the failure mode. |
| 8 | **No real multi-tenant isolation at the control plane.** Shared-token auth is a deployment guard, not per-team identity; control-plane state lives in file-locked JSON stores. | Security / multi-tenancy | Opt-in `SERVICE_AUTH_TOKEN` protects mutation endpoints; load specs are clamped server-side; the sandbox *runtime* boundary is fully isolated per run. ([PRODUCTION_GAP_ANALYSIS.md](PRODUCTION_GAP_ANALYSIS.md#what-is-not-production-level-yet)) | Medium | Real RBAC + a durable transactional store is the P0 production milestone; out of scope for a single-operator demo. |
| 9 | **Malicious-submission coverage is manual, not automated.** The runtime boundary is proven by a reproducible red-team, but the PoCs aren't yet a regression suite, and gVisor/rootless BuildKit are hooks not defaults. | Security / sandbox isolation | Four-front red-team drove the real Docker boundary (escape/exfil/DoS/score-gaming) and every probe held; holes found were closed and some covered by regression tests. ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md#live-docker-red-team-the-build-phase-the-scoring-channel-control-plane-authz)) | Medium | Codifying every PoC as a fixture + flipping gVisor to default is a P0 item; the manual audit already demonstrates the boundary holds. |

## How to read this

- **Severity** is residual risk *after* mitigation, for the benchmark/demo
  context — not the raw severity of an unmitigated bug.
- Every row links to the detailed write-up that found and reasoned about it.
- Items 1–4 are the sandbox/security residuals from
  [SECURITY_SANDBOX.md](SECURITY_SANDBOX.md); items 5–7 are the unmeasured-scale
  and soak residuals from [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) and the
  [infra/k8s README](../infra/k8s/README.md); items 8–9 are the multi-tenancy and
  automated-coverage gaps tracked in
  [PRODUCTION_GAP_ANALYSIS.md](PRODUCTION_GAP_ANALYSIS.md).
- Nothing here is a surprise at demo time: each is a known boundary with a
  mitigation in place and a clear path to closure.
</content>
