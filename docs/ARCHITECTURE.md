# Architecture

The platform is a deterministic benchmark system for contestant trading engines. The final production shape has a control plane for submissions, sandboxing, orchestration, scoring, and leaderboard updates, plus a data plane for high-frequency order traffic and validation.

This repo starts with a local vertical slice:

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

## Final Production Direction

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

## Next Layers

After the local path works:

1. Add Redpanda topics for telemetry.
2. Add leaderboard API and Redis live state.
3. Add submission API and Docker build path.
4. Add sandbox hardening with gVisor/cgroups/network policy.
5. Add orchestrator state machine.
6. Add Kubernetes and Terraform.

