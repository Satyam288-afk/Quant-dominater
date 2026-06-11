# Production Readiness Status

This repository is a strong hackathon-ready prototype with real local and
Docker-backed execution paths. It is not yet a fully production-ready
multi-tenant cloud service. The project should be presented as a production-
oriented benchmark platform prototype, with the gaps below stated explicitly.

## Verified Working Paths

| Area | Status | Evidence |
|---|---|---|
| Upload-to-score pipeline | Working | `scripts/run-platform-demo.sh` uploads an artifact, creates a run, starts the sandbox, runs the bot fleet, validates outputs, scores, publishes leaderboard state, and writes artifacts. |
| Local sandbox execution | Working | `LocalRunner` builds submitted Go artifacts and starts a health-checked engine process with cleanup. |
| Docker sandbox execution | Working implementation | `DockerRunner` builds images, applies CPU/memory/PID/readonly-rootfs/no-new-privileges controls, supports isolated internal networks, and reuses one Docker client. |
| Cancellation and shutdown | Working | Build/start paths accept caller context; long-running Go services drain on SIGTERM/SIGINT. |
| Bot fleet | Working | Rust Tokio fleet generates pooled WebSocket traffic with limit orders, market-order mix, cancels, deterministic sharding, and telemetry artifacts. |
| Validation | Working | Reference orderbook catches price-time priority and fill accuracy violations; invalid runs score zero. |
| Scoring | Working | Composite speed/stability/resource/correctness scoring is shared between Rust and Go tests. |
| Leaderboard | Working | Go leaderboard API supports file and Redis backends plus WebSocket fanout; React UI builds successfully. |
| Live local data plane | Working prototype | Docker Compose brings up Redpanda, TimescaleDB, and Redis for the live telemetry path. |
| IaC renderability | Partially verified | `kubectl kustomize infra/k8s` renders the active base manifests. Strict `kubeconform` and Terraform validation require local tools. |

## Deliberately Not Claimed

The repository no longer claims a fully working Kubernetes upload-to-sandbox
runtime. The base Kubernetes kustomization deploys the shared data plane and
live leaderboard read path only. The upload-driven control-plane manifests are
kept as disabled templates because the current Go services still need two real
production pieces before that cloud path is honest:

1. A durable artifact store, such as S3 or MinIO, shared by submission-api,
   sandbox-runner, and orchestrator.
2. A Kubernetes sandbox runner that builds/pushes contestant images and creates
   per-run Pod/Service resources through the Kubernetes API.

This avoids the previous fake-infrastructure problem where
`SANDBOX_RUNNER_MODE=kubernetes` appeared in manifests even though the binary did
not implement that mode.

## Remaining P0 Before Real Production

1. Replace JSON control-plane stores with a transactional Postgres/Timescale
   control-plane store.
2. Replace local artifact storage with S3/MinIO/object storage, including
   checksum validation, retention, lifecycle cleanup, and per-team prefixes.
3. Replace shared bearer-token guards with user auth, team registration, RBAC,
   per-team rate limits, audit logs, and scoped service identities.
4. Implement service-to-service identity with mTLS, SPIFFE/SPIRE, AWS IAM Roles
   for Service Accounts, or signed workload tokens.
5. Implement the Kubernetes sandbox runner and bot-fleet executor, or keep the
   Kubernetes path documented as data-plane IaC only.
6. Add malicious-submission integration fixtures that prove egress denial,
   CPU/memory/PID enforcement, readonly root filesystem, cleanup, and build-time
   zip-bomb/path-traversal rejection under Docker mode.
7. Add production observability: structured logs, Prometheus/OpenTelemetry
   metrics, traces, dashboards, and alerts.
8. Publish a multi-node benchmark report from a real cluster; the current
   published benchmark is single-host/local-data-plane evidence.

## P1 Hardening

1. Make Redpanda -> TimescaleDB/Redis the default production telemetry path,
   with JSONL kept as the deterministic local fallback.
2. Add image build/push automation for every service image referenced by
   Kubernetes manifests.
3. Add remote Terraform state with locking and environment-specific variables.
4. Replace in-cluster demo data stores with managed stores or operator-managed
   clusters.
5. Add a production runbook for deploy, rollback, cleanup, incident response,
   replay, and audit workflows.

## Submission Positioning

Accurate wording:

> A production-oriented distributed benchmark platform prototype with verified
> local/Docker execution, real telemetry/validation/scoring, live leaderboard,
> and renderable cloud/data-plane IaC.

Do not call it a fully production-ready multi-tenant cloud service until the P0
items above are complete and measured in a real cluster.
