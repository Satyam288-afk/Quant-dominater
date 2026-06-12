# Architecture Blueprint — IICPC Distributed Benchmarking Platform

This is the comprehensive system-design document for the platform: the
microservices, the protocols between them, the data stores, the isolation
strategy, and how the whole thing scales horizontally. It is the contract the
code in this repo implements.

> Companion docs: [ARCHITECTURE.md](ARCHITECTURE.md) (rationale & lifecycle),
> [API_CONTRACT.md](API_CONTRACT.md) (wire messages), [SCORING.md](SCORING.md)
> (formula), [SECURITY_SANDBOX.md](SECURITY_SANDBOX.md) (isolation controls).

## 1. What the platform does

```
Code Upload ─▶ Containerized Deployment ─▶ Distributed Load Test ─▶ Real-Time Scoring
```

A contestant uploads a matching engine / orderbook. The platform builds it into
an isolated container, exposes it, slams it with a distributed fleet of trading
bots, measures latency / throughput / correctness, and streams a live ranking.

## 2. Component map

```
                          ┌──────────────────────── CONTROL PLANE (ns: iicpc) ────────────────────────┐
   contestant ──upload──▶ │ submission-api(9100) ─▶ orchestrator(9300) ─▶ sandbox-runner(9200)         │
                          │      │ artifact            │ lifecycle FSM        │ build + run             │
                          └──────┼─────────────────────┼──────────────────────┼─────────────────────────┘
                                 ▼                      ▼                      ▼
                            object store          per-run state        ┌─ SANDBOX (ns: iicpc-sandbox) ─┐
                          (local:// / S3)                              │ contestant engine pod :8080    │
                                                                       │ caps dropped · ro-rootfs ·     │
                                                                       │ cpu-pinned · NetworkPolicy     │
                                                                       └───────────────▲────────────────┘
                          ┌─────────── DATA GENERATION ───────────┐                    │ ws orders/acks/fills
                          │ bot-fleet Job (N pods × M bots)  ──────┼────────────────────┘
                          │   emits TelemetryEvent ─────────────┐  │
                          └─────────────────────────────────────┼──┘
                                                                 ▼
                          ┌──────────────────────── DATA PLANE ───────────────────────────────────────┐
                          │ Redpanda (Kafka API)  topic telemetry.events.v1 (4p)                        │
                          │      │                                                                       │
                          │      ▼ consumer group                                                        │
                          │ telemetry-ingester (HPA→partitions) ─▶ TimescaleDB (metrics_raw)            │
                          │      └─────────────────────────────────▶ Redis (live run metrics)           │
                          │ score-engine ─pull metrics─▶ TimescaleDB(scores) + Redis(ZSET+scorecard)    │
                          │ leaderboard-api(9500, HPA) ─poll Redis─▶ WebSocket ─▶ web UI                 │
                          └────────────────────────────────────────────────────────────────────────────┘
```

| Service | Lang | Port | Responsibility |
|---|---|---|---|
| `submission-api` | Go | 9100 | accept uploads, store artifact, queue run |
| `orchestrator` | Go | 9300 | drive the benchmark lifecycle FSM |
| `sandbox-runner` | Go | 9200 | build image, run hardened sandbox (Docker / K8s) |
| `bot-fleet` | Rust | — | distributed load generator (Tokio), telemetry producer |
| `telemetry-ingester` | Rust | — | Kafka consumer → percentiles → Timescale + Redis |
| `score-engine` | Rust/Go | 9400 | composite score, gated by correctness |
| `validator` | Rust | — | replay vs reference orderbook, correctness verdict |
| `reference-orderbook` | Rust | — | deterministic price-time-priority matcher |
| `leaderboard-api` | Go | 9500 | serve live ranking, WebSocket fanout |
| `web` | TS/React | 5173 | real-time leaderboard UI |

## 3. Inter-service communication

| Path | Protocol | Why |
|---|---|---|
| bot ↔ contestant engine | **WebSocket** (JSON orders/acks/fills) | low-latency full-duplex order path |
| bot-fleet → ingester | **Kafka/Redpanda** (`telemetry.events.v1`, keyed `run:bot`) | decoupled, partitioned, replayable at scale |
| ingester/score → stores | **SQL** (Timescale), **RESP** (Redis) | durable analytics + low-latency live state |
| leaderboard-api → UI | **WebSocket** (ranked snapshot, push-on-change) | live leaderboard |
| control-plane services | **HTTP/JSON** today; proto message contracts defined (`proto/benchmark.proto`) and ready for a gRPC migration — gRPC not yet wired | simple now; `benchmark.proto` defines messages/enums only (no `service`/`rpc`), so a gRPC cutover is a follow-up |

**WebSocket is the benchmark order path; REST is a documented fallback** on the
stub-engine path (`examples/stub-engine` serves both), and **FIX is a deliberate
scope decision** — the brief asks for "FIX, REST, OR WebSocket", and WebSocket
(plus the REST fallback) satisfies that *or*. We chose WebSocket as primary
because a full-duplex socket is the lowest-overhead way to drive a high-rate,
bidirectional order/ack/fill loop without per-message HTTP framing.

Kafka topics (created by `redpanda-init`): `telemetry.events.v1` (4p),
`bench.orders.v1` (4p), `bench.fills.v1` (4p), `bench.scores.v1` (2p).

## 4. Data stores

**TimescaleDB** (durable, `infra/docker-compose/init-timescale.sql`):
- `metrics_raw` — hypertable, every telemetry event (raw, for percentiles);
  compressed after 1 h, retained 1 day so the volume cannot fill.
- `metrics_1s` — continuous aggregate over `metrics_raw` (per-run 1 s rollups,
  refreshed every 30 s).
- `run_resource` — CPU/mem samples per run (1-day retention).
- `scores` — final score per run (read for history/audit).

**Redis** (live, ephemeral — rebuildable from Timescale):

| Key | Type | Writer | Reader |
|---|---|---|---|
| `leaderboard:global` | ZSET (team→score) | score-engine | leaderboard-api |
| `team:{id}:scorecard` | HASH (full ScoreJson) | score-engine | leaderboard-api |
| `team:{id}:best_score` | string | score-engine | — |
| `run:{id}:metrics` | HASH (live counters) | ingester | leaderboard-api `/runs/{id}/live` |
| `run:{id}:timeseries:tps` | STREAM (capped, per-interval tps/p50/p99) | ingester | live charts |
| `run:{id}:latency_series` | string (JSON: per-second tps/p50/p99 from Timescale) | score-engine | leaderboard-api `/runs/{id}/timeseries` → UI sparkline |

**Object store** — artifacts (`local://` locally, S3/MinIO in cloud;
`proto.SubmissionArtifact.uri` already models the URI + sha256 + size).

## 5. Benchmark lifecycle (orchestrator FSM)

```
QUEUED ─▶ BUILDING ─▶ SANDBOX_STARTING ─▶ HEALTHCHECKING ─▶ BENCHMARKING
       ─▶ VALIDATING ─▶ SCORING ─▶ FINISHED
failure: BUILD_FAILED · HEALTHCHECK_FAILED · SANDBOX_CRASHED · BOT_FLEET_FAILED
         · TIMEOUT · VALIDATION_FAILED · INFRA_FAILED
```

Per run the orchestrator: builds the image, applies a sandbox Pod + Service into
`iicpc-sandbox`, health-checks `/health`, launches the bot-fleet Job pointed at
the engine, then validates and scores.

## 6. Isolation strategy

The sandbox boundary is **substantially the same** in Docker and Kubernetes mode
(see [SECURITY_SANDBOX.md](SECURITY_SANDBOX.md)): all capabilities dropped, no
privilege escalation, read-only rootfs + locked tmpfs, memory cap with swap
disabled, **CPU pinning** for fair latency, pids/nofile limits, optional
gVisor runtime. One documented difference, not a full 1:1 parity: the K8s
template runs the engine as `runAsUser 65532` (non-root), while Docker mode
currently runs it as uid 0 — made non-privileged by `CapDrop: ALL` +
`no-new-privileges` + seccomp, so it is defense-in-depth only (tracked as
[RESIDUALS.md](RESIDUALS.md) item 3). Network is default-deny: a contestant pod
is reachable **only** by the bot fleet on `:8080` and has **no** egress (internet
or cross-contestant), enforced by `NetworkPolicy` in K8s and DNS black-holing +
scoped networks in Docker. RBAC scopes the runner to manage pods only in
`iicpc-sandbox`.

## 7. Scoring

Correctness is a hard gate — a fast but incorrect engine scores **0**.

```
final = 0.40·latency + 0.30·throughput + 0.20·stability + 0.10·resource   (each 0..100)
        latency:    100 if p99 ≤ 5ms, linear to 0 at 100ms
        throughput: 100 · tps / expected_tps  (capped)
        stability:  100 · (1 − (timeouts+errors)/orders_sent)
        resource:   CPU/mem efficiency (soft penalty)
```

Correctness comes from replaying the canonical input through the deterministic
`reference-orderbook` and diffing fills — `PRICE_TIME_PRIORITY_VIOLATION`,
`MISSING_FILL`, `UNEXPECTED_FILL`, `FILL_MISMATCH`. Proven by
`scripts/run-price-time-proof.sh` (a correct engine passes, a deliberately
broken one fails).

## 8. How it scales horizontally

**Scaling model.** The measured single-node ceiling is **~250k orders/s** (mutex
engine, one `bot-fleet` process on a 12-core laptop, zero timeouts, single-digit-ms
p99 — see [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md)); at that wall the binding
constraint is JSON encoding + per-message socket writes (the transport), not the
matcher. The design scales past that node linearly with **partitions and pods**,
and the IaC realizes exactly that: more `bot-fleet` Job pods (each with a
`--pod-index`-offset global ID range) generate more load, more Kafka partitions
plus ingester HPA replicas absorb the telemetry, and the cluster autoscaler adds
sandbox/platform nodes as concurrent runs grow. The multi-node figures are
designed-for, not yet measured (tracked in [RESIDUALS.md](RESIDUALS.md)).

- **Load** — bot-fleet is an Indexed K8s Job: `parallelism × BOTS_PER_POD` bots
  across nodes (default 8 × 1250 = 10,000). The Rust fleet also multiplexes many
  virtual bots over a connection pool, so per-pod density is high.
- **Ingestion** — telemetry-ingester is a Kafka consumer group; its HPA scales
  replicas to the partition count. Throughput grows with partitions + replicas.
- **Live API** — leaderboard-api is stateless (reads Redis); HPA 2→10 on CPU.
- **Sandboxes** — a dedicated, tainted, compute-optimized node group; the
  cluster autoscaler adds nodes as concurrent runs increase.
- **Substrate** — `infra/terraform` provisions the VPC + EKS + node groups + ECR;
  `infra/k8s` deploys the whole cell with one `kubectl apply -k`.

## 9. Tech choices (why)

| Choice | Reason |
|---|---|
| Rust for hot paths (bots, ingester, validator, orderbook) | predictable low-latency, fearless concurrency at scale |
| Go for control plane | fast iteration, great HTTP/concurrency, simple ops |
| Redpanda over Kafka | Kafka API, no ZooKeeper/JVM, lower latency |
| TimescaleDB | Postgres + hypertables → percentiles over huge event volumes |
| Redis | sub-ms live leaderboard state, ZSET == ranking primitive |
| WebSocket | full-duplex for both the order path and the live UI |
| JSONL-first, then Kafka | replayable/auditable dev loop that upgrades to a real bus |
| Correctness as a gate | the platform measures correctness, never trusts it |
