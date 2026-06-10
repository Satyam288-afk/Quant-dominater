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
| Limit orders, market orders, cancels | Partial | Limit orders and cancel messages exist; market-order and cancel-heavy demos need stronger coverage |
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
- Docker sandbox resource controls and local egress isolation.
- Tests for Rust core and Go services.

## What Is Not Production-Level Yet

- No real authentication, team registration, or RBAC.
- No durable database-backed control plane.
- No fully wired Redpanda -> TimescaleDB/Redis telemetry path in the main demo.
- No Kubernetes cell orchestration for horizontal bot scaling.
- No verified gVisor/rootless BuildKit malicious-code fixture.
- No Terraform cloud deployment.
- No artifact download API for browser replay/audit links.
- No production observability stack: traces, structured logs, service metrics, alerting.

## Recommended Next Milestones

1. Add artifact download endpoints to leaderboard/orchestrator.
2. Wire Redpanda telemetry into the platform demo.
3. Add a malicious-submission sandbox test fixture.
4. Run and record 10/50/100/250/500/1000 bot benchmark results.
5. Build real Docker Compose one-command infra demo.
6. Add Kubernetes manifests with per-run namespace, resource limits, and NetworkPolicy.
7. Add Terraform skeleton that provisions the target cloud shape.
