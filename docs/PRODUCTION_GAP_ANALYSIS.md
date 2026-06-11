# Production Gap Analysis

This is an honest, code-verified status of the platform against the problem
brief. The benchmark core, the full data plane, the cloud-native IaC, and the
browser console are all built and exercised. What remains is the work that
separates a demonstrable platform from a hardened multi-tenant production
service: durable databases, identity-aware auth, an automated malicious-code
fixture suite, observability, and CI/CD. The consolidated residual list lives in
[RESIDUALS.md](RESIDUALS.md).

## Current Status Against Problem Expectations

| Requirement | Current status | Evidence |
|---|---|---|
| Submission and sandboxing engine | Working | `submission-api`, `sandbox-runner` (`local` + `docker` runner modes), orchestrator FSM |
| Strict CPU and memory limits | Working | Docker runner maps `cpu_limit`/`memory_limit` to cgroups, swap disabled, PID/nofile caps; CPU pinning on Linux hosts |
| No internet egress | Working | Docker mode black-holes DNS + per-sandbox internal network when `network_egress=false`; K8s `NetworkPolicy` default-deny |
| gVisor / stronger sandbox escape resistance | Hook + manual proof | `SANDBOX_DOCKER_RUNTIME=runsc` supported; runtime boundary red-teamed by hand ([SECURITY_SANDBOX.md](SECURITY_SANDBOX.md)); automated fixture suite is still open |
| Distributed load generator | Working | Rust Tokio `bot-fleet`, deterministic order stream, WebSocket connection pooling, mixed limit/market/cancel flow |
| Thousands of bots / horizontal scale | Working core + cloud design | Single-process fleet multiplexes 10k virtual bots; `--pod-index` offsets global IDs so an Indexed K8s Job shards across pods (`infra/k8s/31-bot-fleet-job.yaml`). Multi-node figures are designed but unmeasured (see [RESIDUALS.md](RESIDUALS.md)) |
| Limit orders, market orders, cancels | Working | Fleet sends a realistic mix across a price ladder; reference orderbook and validator are market/cancel aware ([BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md)) |
| FIX / REST / WebSocket adapters | Working (brief's "OR" satisfied) | WebSocket is the benchmark order path; REST is a documented fallback on the stub path; FIX is a deliberate scope decision, not a gap |
| Telemetry and validation ingester | Working | `bot-fleet --backend live` → Redpanda → `telemetry-ingester` → TimescaleDB (`metrics_raw`) + Redis live state; wired end-to-end by `scripts/run-live-demo.sh` |
| Correctness gate | Working | Reference orderbook catches price-time-priority violations; invalid engines score zero; empty/no-load runs rejected (`NO_BENCHMARK_EVENTS`) |
| Real-time leaderboard and analytics | Working | Go leaderboard API, WebSocket fanout, React UI, per-run p99 sparkline + per-second timeseries from Timescale |
| Replay/audit links | Working | `console-api` exposes `GET /api/runs/{id}/artifacts` (list) and `/artifacts/{name}` (download); the browser console inspects the run's artifact set |
| Docker Compose from scratch | Working | `infra/docker-compose` brings up Redpanda + TimescaleDB + Redis; `make live-demo` drives the full path |
| Kubernetes manifests | Working (validated cell) | `infra/k8s`: namespaces, RBAC, data plane, control plane, HPAs, NetworkPolicy, per-run Job/Pod templates — 32 resources pass `kubeconform -strict` (k8s 1.30) |
| Terraform cloud provisioning | Working (validated) | `infra/terraform`: AWS VPC + EKS (platform + tainted sandbox node groups) + ECR; `tofu validate` passes, `tofu fmt` clean |

## What Is Production-Like Now

- Deterministic benchmark lifecycle with a real orchestrator FSM.
- Real submission → sandbox → bot fleet → validator → score → leaderboard path,
  driven both by scripted demos and an interactive browser console.
- Full live data plane: Redpanda → telemetry-ingester → TimescaleDB + Redis →
  score-engine → leaderboard-api, verified end-to-end by `scripts/run-live-demo.sh`.
- Reproducible run artifacts under `.runs/{run_id}`, listable and downloadable
  through the console-api artifact endpoints.
- Correctness-first scoring where invalid engines score zero.
- Empty/no-load benchmark artifacts are rejected. The validator returns
  `NO_BENCHMARK_EVENTS`, bot-fleet execution fails when no orders are emitted,
  and scoring demotes otherwise-valid zero-order runs to score zero.
- Docker sandbox resource controls, local egress isolation, and a hand-driven
  red-team audit of the runtime boundary (escape / exfil / resource DoS / score
  gaming) — see [SECURITY_SANDBOX.md](SECURITY_SANDBOX.md).
- Hostile-input hardening on the measurement plane: zip-bomb / path-traversal
  rejection, build-time RCE containment (`CGO_ENABLED=0` + process-group kill),
  fabricated-ack gating, WebSocket frame caps, validator overflow safety.
- Sandbox build/start/stop paths are context-aware, so cancelled requests and
  orchestrator timeouts stop Docker and local runner work instead of leaving
  long-running builds detached.
- Docker mode uses a runner-owned Docker client instead of creating a new client
  for every build/start/inspect/cleanup call.
- Long-running Go services install SIGTERM/SIGINT handlers and drain in-flight
  HTTP requests; the orchestrator also cancels active runs and waits boundedly
  for them to persist terminal state.
- Service mutation endpoints support opt-in bearer-token protection through
  `SERVICE_AUTH_TOKEN` or service-specific token env vars; orchestrator and
  console clients forward those tokens.
- Validated cloud-native IaC: `make k8s-validate` (kustomize + `kubeconform
  -strict`, 32 resources) and `make tf-validate` (`tofu fmt` + `init
  -backend=false` + `validate`) both pass with no cluster or cloud account.
- Tests for Rust core and Go services.

## What Is Not Production-Level Yet

These are the honest remaining gaps. Each is tracked with its mitigation and
deferral rationale in [RESIDUALS.md](RESIDUALS.md).

- No real user authentication, team registration, or RBAC. Current token auth is
  a shared deployment guard, not identity-aware access control.
- No durable database-backed control plane. Submission and run state live in
  file-locked JSON stores (`internal/store/json_store.go`), not Postgres/Timescale.
- The cloud-native K8s sandbox runner is a validated Job/Pod template, but the
  *shipped* runtime is the single-host subprocess fleet proven by the demos; the
  `kubernetes` runner mode is a documented next step, not running code (the
  runner supports `local` and `docker` today).
- No automated malicious-submission fixture suite. The runtime boundary is
  proven by a reproducible hand-driven red-team (SECURITY_SANDBOX.md), but the
  PoCs are not yet codified as a regression suite, and gVisor/rootless BuildKit
  are hooks rather than the default path.
- No production observability stack: traces, structured logs, service metrics,
  alerting.
- No CI workflow covering Go, Rust, frontend build, IaC validation, or security
  scanning (`make k8s-validate` / `make tf-validate` run locally today).
- Multi-node bot-fleet and ingester throughput figures are designed-for but not
  measured; reported numbers are single-host (see [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md)).

## Recommended Next Milestones

The lifecycle and resource-management fixes from earlier review rounds are done:
Docker builds now receive caller cancellation through the `Runner` interface, and
Docker mode reuses a runner-owned client. Those removed real risks but do not by
themselves make the platform production-ready — that still requires the items
below.

### P0 Before Production

1. Replace the file-locked JSON control-plane stores with a durable,
   transactional Postgres/Timescale-backed store.
2. Replace local artifact storage with S3/MinIO/object storage, including
   checksum validation, retention policy, and lifecycle cleanup
   (`proto.SubmissionArtifact.uri` already models the URI + sha256 + size).
3. Replace shared-token auth with real authentication, team registration, RBAC,
   per-team rate limits, audit logs, and scoped service identities.
4. Harden service-to-service authentication with mTLS or signed workload
   identity instead of shared bearer tokens.
5. Codify the red-team PoCs as an automated malicious-submission fixture suite
   (path traversal, egress denial, CPU/memory/PID enforcement, read-only
   filesystem, cleanup) and make gVisor/rootless BuildKit the default.
6. Instantiate the K8s `kubernetes` sandbox runner mode against the validated
   Job/Pod templates so cloud runs use the same code path as the cell.

### P1 Hardening

1. Add CI for Go tests, Rust tests, frontend build, IaC validation, linting, and
   dependency/security scanning.
2. Add Prometheus/OpenTelemetry metrics, structured logs, tracing, dashboards,
   and alerting.
3. Promote the single Redpanda/Timescale/Redis instances to the managed /
   operator-backed stores the Terraform node groups are sized for.
4. Add remote Terraform state and environment-specific variables.
5. Run and publish multi-node benchmark results.
6. Add a production runbook covering deploy, rollback, cleanup, incident
   response, and replay/audit workflows.
</content>
</invoke>
