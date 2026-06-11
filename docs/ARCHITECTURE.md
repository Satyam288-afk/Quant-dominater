# Architecture

The platform is a deterministic benchmark system for contestant trading engines. The final production shape has a control plane for submissions, sandboxing, orchestration, scoring, and leaderboard updates, plus a data plane for high-frequency order traffic and validation.

The verified local vertical slice is:

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
| Infra later | Terraform/Kubernetes only matter after the benchmark core works |

## Target Production Direction

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
SCORED
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

## Current State And Next Layers

Already implemented:

1. Local upload-to-score pipeline.
2. Docker sandbox build/start path with resource controls.
3. Redpanda/Timescale/Redis local data-plane demo.
4. Leaderboard API, Redis backend, and React UI.
5. Submission API and orchestrator state machine.
6. Kubernetes/Terraform IaC for the shared data plane.

Still required before real production:

1. Durable Postgres/Timescale control-plane store.
2. S3/MinIO artifact store shared by services.
3. Kubernetes sandbox runner and Kubernetes bot-fleet executor.
4. Real auth/RBAC/team isolation/rate limits.
5. Production observability, CI-gated image builds, and multi-node benchmark
   evidence.
