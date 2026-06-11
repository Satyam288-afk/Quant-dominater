# Architecture

The platform is a deterministic benchmark system for contestant trading engines. It has a control plane for submissions, sandboxing, orchestration, scoring, and leaderboard updates, plus a data plane for high-frequency order traffic and validation. This doc captures the design rationale and lifecycle; the full system-design contract is in [BLUEPRINT.md](BLUEPRINT.md).

It was **built bottom-up**, starting from the local vertical slice below and
layering the data plane, control plane, and cloud-native IaC on top — all of
which now exist (see [PRODUCTION_GAP_ANALYSIS.md](PRODUCTION_GAP_ANALYSIS.md) for
the current code-verified status). The local slice:

```text
Rust Bot Fleet -> Contestant Engine Stub
       |                 |
       v                 v
 events.jsonl     contestant_outputs.jsonl
       |                 |
       +------ Validator +
               |
               v
          VALID / INVALID
```

## Core Decisions

| Decision | Reason |
|---|---|
| WebSocket first | Low-latency full-duplex order/ack/fill path |
| JSONL first | Easy replay and audit before introducing Redpanda |
| Rust bot fleet | Tokio async sessions scale to many lightweight bots |
| Reference orderbook | Correctness is measured, not trusted |
| Correctness gate | A fast incorrect engine gets score `0` |
| Infra after the core | Terraform/Kubernetes were built once the benchmark core was proven, so the IaC encodes a working system rather than a guess |

## Production Direction (now realized)

The diagram below is the shape the platform was driving toward; the live data
plane (Redpanda → ingester → Timescale/Redis → score-engine → leaderboard) and
the validated EKS/K8s IaC that realize it are now in the repo. Object storage
uses the `local://` artifact store today, with MinIO/S3 as the cloud-mode swap.

```text
Submission API -> Sandbox Runner -> Contest Orchestrator
       |                |                 |
       v                v                 v
     MinIO       Contestant Sandbox    Bot Fleet
                                      /    |    \
                                     v     v     v
                                Redpanda Validator Telemetry
                                     |       |       |
                                     v       v       v
                                 Timescale  S3     Redis
                                               \     |
                                                v    v
                                             Score Engine
                                                  |
                                                  v
                                           Leaderboard API
```

## Benchmark Lifecycle

```text
QUEUED
BUILDING
SANDBOX_STARTING
HEALTHCHECKING
BENCHMARKING
VALIDATING
SCORING
FINISHED
```

Failure states:

```text
BUILD_FAILED
HEALTHCHECK_FAILED
SANDBOX_CRASHED
BOT_FLEET_FAILED
TIMEOUT
VALIDATION_FAILED
INFRA_FAILED
```

## Local Data Files

| File | Written by | Contains |
|---|---|---|
| `events.jsonl` | bot fleet | canonical input orders |
| `contestant_outputs.jsonl` | bot fleet | acks/fills received from engine |
| `engine-events.jsonl` | stub engine | engine-side input/output audit log |

## Build Sequence (completed)

The layers were added on top of the local path in this order, and all are now in
the repo:

1. ✅ Redpanda topics for telemetry.
2. ✅ Leaderboard API and Redis live state.
3. ✅ Submission API and Docker build path.
4. ✅ Sandbox hardening with cgroups/network policy (gVisor as a runtime hook).
5. ✅ Orchestrator state machine.
6. ✅ Kubernetes (validated cell + HPA + NetworkPolicy) and Terraform (VPC + EKS + ECR).

What is intentionally *not* yet production-grade — durable DB control plane,
identity-aware auth, an automated malicious-code fixture suite, observability,
CI/CD, and the in-cluster `kubernetes` runner mode — is tracked honestly in
[PRODUCTION_GAP_ANALYSIS.md](PRODUCTION_GAP_ANALYSIS.md) and
[RESIDUALS.md](RESIDUALS.md).

