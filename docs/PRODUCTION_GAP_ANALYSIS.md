# Production Gap Analysis

This project is an end-to-end local prototype, not a fully production-ready
multi-tenant cloud platform yet.

## Current Status Against Problem Expectations

| Requirement | Current status | Evidence |
|---|---|---|
| Submission and sandboxing engine | Partial production, working local path | `submission-api`, `sandbox-runner`, local and Docker runner modes |
| Strict CPU and memory limits | Partial | Docker runner maps `cpu_limit` and `memory_limit` into Docker resources |
| No internet egress | Partial | Docker mode creates an internal per-sandbox network when `network_egress=false` |
| gVisor / stronger sandbox escape resistance | Planned | `SANDBOX_DOCKER_RUNTIME=runsc` is supported when host Docker is configured |
| Distributed load generator | Working local core | Rust Tokio `bot-fleet`, deterministic order stream, WebSocket connection pooling |
| Thousands of bots / horizontal scale | Partial | Local async fleet exists; Kubernetes sharding is not implemented yet |
| Limit orders, market orders, cancels | Partial | Limit orders are exercised in the platform demo; market orders and cancel-heavy load need stronger automated coverage |
| FIX / REST / WebSocket adapters | Partial | WebSocket is primary; REST fallback exists on stub path; FIX is not implemented |
| Telemetry and validation ingester | Partial | JSONL telemetry, validator, reference orderbook, telemetry ingester; Redpanda/Timescale path is not fully wired into platform demo |
| Correctness gate | Working | Reference orderbook catches price-time priority violations; invalid engines score zero |
| Real-time leaderboard and analytics | Working local UI | Go leaderboard API, WebSocket fanout, static benchmark console |
| Replay/audit links | Partial | Run artifact directory exists; browser artifact download endpoints are not implemented |
| Docker Compose from scratch | Partial | Compose files exist for backing services; one-command full infra demo still needs hardening |
| Kubernetes manifests | Minimal placeholder | `infra/k8s` needs real deployments, services, network policies, resource limits |
| Terraform cloud provisioning | Minimal placeholder | `infra/terraform` needs real modules and environment docs |

## What Is Production-Like Now

- Deterministic benchmark lifecycle.
- Real submission -> sandbox -> bot fleet -> validator -> score -> leaderboard path.
- Reproducible run artifacts under `.runs/{run_id}`.
- Correctness-first scoring where invalid engines score zero.
- Empty/no-load benchmark artifacts are rejected. The validator returns
  `NO_BENCHMARK_EVENTS`, bot-fleet execution fails when no orders are emitted,
  and scoring demotes otherwise-valid zero-order runs to score zero.
- Docker sandbox resource controls and local egress isolation.
- Sandbox build/start/stop paths are context-aware, so cancelled requests and
  orchestrator timeouts can stop Docker and local runner work instead of leaving
  long-running builds detached.
- Local sandbox process lifetime is decoupled from the HTTP `/sandboxes/start`
  request context after readiness. The platform demo now produces real traffic
  instead of a false valid run with zero orders.
- Docker mode uses a runner-owned Docker client instead of creating a new client
  for every build/start/inspect/cleanup call.
- Long-running Go services install SIGTERM/SIGINT handlers and drain in-flight
  HTTP requests; the orchestrator also cancels active runs and waits boundedly
  for them to persist terminal state.
- Service mutation endpoints support opt-in bearer-token protection through
  `SERVICE_AUTH_TOKEN` or service-specific token env vars; orchestrator and
  console clients forward those tokens.
- Tests for Rust core and Go services.

## What Is Not Production-Level Yet

- No real user authentication, team registration, or RBAC. Current token auth is
  a shared deployment guard, not identity-aware access control.
- No durable database-backed control plane.
- No fully wired Redpanda -> TimescaleDB/Redis telemetry path in the main demo.
- No Kubernetes cell orchestration for horizontal bot scaling.
- `infra/k8s` is documentation-only in this branch; there is no implemented
  Kubernetes sandbox runner or deployable manifest set yet.
- No verified gVisor/rootless BuildKit malicious-code fixture suite.
- No Terraform cloud deployment.
- No artifact download API for browser replay/audit links.
- No production observability stack: traces, structured logs, service metrics, alerting.
- No CI workflow covering Go, Rust, frontend build, IaC validation, or security
  scanning.

## Review Notes Verified

The external review's two immediate code-level points were correct for this
branch and have been implemented:

- Docker builds now receive caller cancellation through the `Runner` interface.
- Docker mode now reuses a runner-owned Docker client across operations.

The broader claim that these two fixes alone make the project production-ready
is not correct. They remove important lifecycle and resource-management risks,
but production readiness still requires durable state, authenticated service
boundaries, real cloud orchestration, malicious-submission tests, observability,
and CI/CD.

## Recommended Next Milestones

### P0 Before Production

1. Replace JSON control-plane stores with a durable, transactional
   Postgres/Timescale-backed store.
2. Replace local artifact storage with S3/MinIO/object storage, including
   checksum validation, retention policy, and lifecycle cleanup.
3. Replace shared-token auth with real authentication, team registration, RBAC,
   per-team rate limits, audit logs, and scoped service identities.
4. Harden service-to-service authentication with mTLS or signed workload
   identity instead of shared bearer tokens.
5. Add malicious-submission fixtures proving path traversal rejection, egress
   denial, CPU/memory/PID enforcement, read-only filesystem, and cleanup.
6. Implement the Kubernetes sandbox runner and deployable manifests, or clearly
   document Kubernetes as future work only.

### P1 Hardening

1. Make the Redpanda -> TimescaleDB/Redis telemetry path the default production
   path, with the JSONL path kept as local/dev fallback.
2. Add CI for Go tests, Rust tests, frontend build, IaC validation, linting, and
   dependency/security scanning.
3. Add Prometheus/OpenTelemetry metrics, structured logs, tracing, dashboards,
   and alerting.
4. Add managed cloud stores or clearly document in-cluster stores as dev-only.
5. Add remote Terraform state and environment-specific variables.
6. Run and publish multi-node benchmark results.
7. Add a production runbook covering deploy, rollback, cleanup, incident
   response, and replay/audit workflows.
