# IICPC Benchmark Platform

A distributed benchmarking & hosting platform that evaluates contestant-submitted
trading engines: **Code Upload → Containerized Deployment → Distributed Load Test
→ Real-Time Scoring**.

```text
contestant engine (sandboxed)  ◀── ws orders/acks/fills ──  Rust bot fleet (10k+ bots)
        │                                                          │ TelemetryEvent
        │ correctness                                              ▼
   reference orderbook ── validator ──┐                    Redpanda (Kafka)
        (price-time priority)         │                           │
                                      ▼                           ▼ consumer group
                                  score-engine ◀── metrics ── telemetry-ingester
                                      │                       │            │
                                      ▼                       ▼            ▼
                            Redis ZSET + scorecard      TimescaleDB     Redis live
                                      │
                                      ▼
                          leaderboard-api ── WebSocket ──▶ live web UI
```

It runs three ways: a **dependency-free local slice** (JSONL files), a **full
local data plane** (Redpanda + TimescaleDB + Redis via docker-compose), and a
**cloud cell** (Terraform EKS + `kubectl apply -k`).

## Hackathon Deliverables

| Deliverable | Where |
|---|---|
| **1. Working prototype** (upload → deploy → load test → scoring) | `services/*`, `rust/*`, `web/`, `scripts/run-live-demo.sh` — measured numbers in [docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) (~50k orders/s driven, p99 + peak-TPS curve, correctness held over 191k fills) |
| **2. Architecture Blueprint** | [docs/BLUEPRINT.md](docs/BLUEPRINT.md) (+ ARCHITECTURE, API_CONTRACT, SCORING, SECURITY_SANDBOX, [RESILIENCE](docs/RESILIENCE.md), [PROFILING](docs/PROFILING.md)) |
| **3. Infrastructure as Code** | [infra/k8s](infra/k8s) (32 validated resources, HPA, NetworkPolicy), [infra/terraform](infra/terraform) (EKS/ECR/VPC), [infra/docker-compose](infra/docker-compose) |

## Current Components

| Path | Purpose |
|---|---|
| `docs/API_CONTRACT.md` | WebSocket/REST message contract |
| `docs/PRODUCTION_GAP_ANALYSIS.md` | Current production-readiness status and remaining work |
| `proto/benchmark.proto` | Protobuf version of the benchmark messages |
| `examples/stub-engine` | Go WebSocket/REST contestant engine stub |
| `examples/rust-engine` | Rust WebSocket contestant engine stub |
| `services/submission-api` | Go API for storing submissions and queued run records |
| `services/sandbox-runner` | Go sandbox service boundary with local and Docker modes |
| `services/orchestrator` | Go lifecycle manager for queued benchmark runs |
| `services/score-engine` | Go scoring API for local run artifacts |
| `services/leaderboard-api` | Go live leaderboard API with WebSocket fanout |
| `services/console-api` | Browser-facing gateway for upload, run control, leaderboard, and artifacts |
| `rust/bot-fleet` | Rust Tokio bot fleet and local metrics collector |
| `rust/reference-orderbook` | Deterministic price-time reference matcher |
| `rust/validator` | Replays inputs and compares contestant fills |
| `services/control-panel` | Go API for creating and tracking local benchmark runs |
| `rust/telemetry-ingester` | Rust Kafka consumer → percentiles → TimescaleDB + Redis |
| `web` | React/TS real-time leaderboard UI (WebSocket) |
| `infra/k8s` | Kubernetes benchmark cell (deployments, HPA, NetworkPolicy) |
| `infra/terraform` | Terraform: AWS VPC + EKS + ECR |
| `infra/docker-compose` | Redpanda + TimescaleDB + Redis for the full local data plane |
| `docs/BLUEPRINT.md` | Comprehensive architecture blueprint |
| `fixtures` | Tiny validation fixtures |
| `scripts/run-local-demo.sh` | One-command local slice demo (JSONL) |
| `scripts/run-live-demo.sh` | Full data-plane demo (Redpanda→Timescale→Redis→leaderboard) |
| `scripts/run-price-time-proof.sh` | Correct engine vs intentionally broken engine proof |
| `scripts/run-chaos-demo.sh` | Failure-injection demo: kill the engine mid-run → fleet reconnects; SIGTERM a service → graceful drain (no Docker needed) |
| `scripts/run-platform-demo.sh` | Submission-to-leaderboard local platform demo |
| `scripts/run-console-stack.sh` | Interactive browser console stack |

## Prerequisites

Install Go and Rust locally. The Docker-backed sandbox runner uses the Docker
SDK dependency set and should be built with Go 1.25+.

```bash
go version
rustc --version
cargo --version
```

## Run The Local Slice

Terminal 1:

```bash
cd examples/stub-engine
go run . --addr :8080 --events engine-events.jsonl
```

Terminal 2:

```bash
cargo run -p bot-fleet --bin bot-fleet -- \
  --target ws://localhost:8080/ws \
  --bots 100 \
  --orders-per-sec 5 \
  --duration-sec 60 \
  --seed 42 \
  --events-out events.jsonl \
  --outputs-out contestant_outputs.jsonl
```

Validate:

```bash
cargo run -p validator -- \
  --events events.jsonl \
  --contestant-outputs contestant_outputs.jsonl
```

Or run the short scripted demo:

```bash
./scripts/run-local-demo.sh
```

## Run The Control Panel

The control panel wraps the same local slice with an HTTP API:

```bash
make control-panel
```

Create a run:

```bash
curl -X POST http://localhost:9000/api/runs \
  -H "Content-Type: application/json" \
  -d '{"team_id":"team_1","engine_mode":"normal","bot_count":10,"orders_per_sec":5,"duration_sec":5,"seed":42}'
```

List runs:

```bash
curl http://localhost:9000/api/runs
```

## Run The Platform Services

Start the three control-plane services in separate terminals:

```bash
make submission-api
make sandbox-runner
make orchestrator
```

Optional services:

```bash
make score-engine
make leaderboard-api
```

The leaderboard API serves a minimal live UI at:

```text
http://localhost:9500/
```

## Run The Browser Console

This starts an interactive local platform console instead of only running a
scripted demo. The browser talks to one gateway service, then the gateway calls
the submission API, orchestrator, and leaderboard API.

```bash
make console-stack
```

Open:

```text
http://localhost:9700/
```

The script also creates an example upload artifact:

```text
.runs/console-stack/stub-engine.zip
```

Use the console to upload that ZIP, configure bots/rate/duration/resource
limits, start the benchmark, watch the lifecycle timeline, inspect artifacts,
and see the leaderboard update.

Use Docker-backed sandboxing when Docker is running:

```bash
SANDBOX_RUNNER_MODE=docker make sandbox-runner
```

## Run The Full Live Data Plane

Brings up Redpanda + TimescaleDB + Redis, runs the fleet with the live backend,
ingests telemetry into Timescale/Redis, scores it, and serves the live
leaderboard (requires Docker):

```bash
make live-demo        # ./scripts/run-live-demo.sh
```

The script publishes telemetry to Redpanda, the ingester writes `metrics_raw`
(Timescale) + live run metrics (Redis), the score-engine writes the
`leaderboard:global` ZSET + per-team scorecard, and the leaderboard-api serves
them. The **full path is verified end-to-end** — a typical run lands ~11.5k
events in Timescale, validates clean, and the leaderboard-api returns a scored
entry, e.g.:

```json
{"run_id":"run_20260610_092925","team_id":"team_demo","score":78.35,"valid":true,
 "p99_ms":56.4,"tps":260.9,"peak_tps":251,"throughput_score":100,"latency_score":45.87,
 "stability_score":100,"resource_score":100,"orders_sent":1300,"acks_received":1300}
```

Each invocation uses a fresh `run_id` so a previous run's rows can't pollute the
new run's throughput/latency. (Tip: `CARGO_PROFILE=debug make live-demo` skips
the release build for a faster loop.)

## Run The Leaderboard UI

```bash
# 1) leaderboard-api in redis mode
cd services/leaderboard-api && LEADERBOARD_BACKEND=redis REDIS_URL=redis://localhost:56379/ go run .
# 2) the web app (proxies /leaderboard, /runs, /ws to :9500)
make web    # http://localhost:5173
```

## Validate The Infrastructure (no cloud / cluster needed)

```bash
make k8s-validate   # render with kustomize + kubeconform (strict, k8s 1.30)
make tf-validate    # tofu/terraform fmt + init -backend=false + validate
```

Deploy to a real cluster:

```bash
cd infra/terraform && tofu init && tofu apply      # VPC + EKS + ECR
$(cd infra/terraform && tofu output -raw configure_kubectl)
kubectl apply -k infra/k8s                          # the benchmark cell
```

In Docker mode, `network_egress=false` creates a per-sandbox internal Docker
bridge network. The engine is still reachable by the local bot fleet through a
random localhost port, but the contestant container does not get normal outbound
internet access.

## Run The Upload-To-Leaderboard Demo

This starts the local submission API, sandbox runner, orchestrator, and leaderboard API, uploads the example Go engine as a ZIP, creates a benchmark run, waits for scoring, and prints the generated artifact set.

```bash
./scripts/run-platform-demo.sh
```

Keep the demo services running after the run if you want to open the live UI:

```bash
KEEP_SERVICES=1 \
SUBMISSION_API_ADDR=:9610 \
SANDBOX_RUNNER_ADDR=:9620 \
ORCHESTRATOR_ADDR=:9630 \
LEADERBOARD_API_ADDR=:9650 \
./scripts/run-platform-demo.sh
```

Then open `http://localhost:9650/` in a browser.

The demo owns ports `9100`, `9200`, `9300`, and `9500`. If one is already in use,
the script fails fast instead of publishing to stale local services. For a clean
demo board:

```bash
make reset-demo-state
```

The script uses a private submission index and artifact root under
`.runs/platform-demo/`, then explicitly starts the run through its own
orchestrator. This avoids interference from any long-running local orchestrator
watching the default `.artifacts/submissions/index.json` store.

Expected run artifact shape:

```text
.runs/{run_id}/
├── config.json
├── orders.jsonl
├── acks.jsonl
├── fills.jsonl
├── cancels.jsonl
├── metrics.json
├── validation.json
├── score.json
├── build.json
├── sandbox.json
├── run_spec.json
└── run.log
```

## Prove The Validator Is Real

Run the price-time-priority proof:

```bash
./scripts/run-price-time-proof.sh
```

This starts the stub engine twice:

```text
mode=normal
mode=broken-price-time-priority
```

The probe sends this exact sequence on one symbol:

```text
1. buy_late  price=10025 ts=...002
2. buy_early price=10025 ts=...001
3. sell_1    price=10025 ts=...003
```

The reference orderbook expects `buy_early` to fill first because same-price orders use earliest timestamp priority. Normal mode passes. Broken mode intentionally sorts same-price resting orders by later timestamp first, so it fails:

```json
{
  "valid": false,
  "reason": "PRICE_TIME_PRIORITY_VIOLATION",
  "expected": {
    "buy_order_id": "buy_early",
    "sell_order_id": "sell_1",
    "price": 10025,
    "qty": 5
  },
  "actual": {
    "buy_order_id": "buy_late",
    "sell_order_id": "sell_1",
    "price": 10025,
    "qty": 5
  }
}
```

Artifacts are written to:

```text
.runs/price-time-proof/
```

## Expected Bot Fleet Output

```text
run_id: run_local_001
bots: 100
orders_sent: 30000
acks_received: 30000
fills_received: 15000
timeouts: 0
connect_errors: 0
tps: 500.0
peak_tps: 540
p50: 1.2ms
p90: 3.8ms
p99: 11.4ms
events_out: events.jsonl
outputs_out: contestant_outputs.jsonl
```

`tps` is the average; `peak_tps` is the busiest 1-second window (the brief's
"max TPS before failure"). The fleet sends a realistic mix — **limit** orders
across a multi-level price ladder, **market** orders, and **cancels** of resting
orders — over a connection pool, with many bots sharing symbols so they trade
against each other. Flags: `--symbols`, `--price-levels`, `--qty-max`,
`--market-per-mille`, `--cancel-per-mille`, `--ws-connections`, `--pod-index`.

## Implementation Status

1. ✅ API contract
2. ✅ stub engine (limit + market + cancel matching); two interchangeable cores — `--engine mutex` (sharded per-symbol locks, default) and `--engine disruptor` (lock-free LMAX-style MPSC ring + single matcher per shard, ~2× lower p99) — see [docs/PROFILING.md](docs/PROFILING.md)
3. ✅ bot fleet (pooled, 10k bots): limit/market/cancel orders, price-ladder depth, shared symbols → real cross-bot trading; globally-unique IDs across pods (`--pod-index`); **resilient** — a dropped pooled connection auto-reconnects with capped exponential backoff and resumes on the same channels, so the fleet survives an engine blip mid-run (recovery surfaced as `reconnects: N`)
4. ✅ latency (p50/p90/p99/p999) + throughput (avg **and** peak TPS = max acks in any 1s window) + **per-second latency/TPS time-series** (`GET /runs/{id}/timeseries`, computed from Timescale) so you can watch p99 degrade under load
5. ✅ reference orderbook (price-time priority, market & cancel)
6. ✅ validator (+ proof): replays in the engine's **accepted arrival order** (ack `engine_seq`), so a correct engine validates clean under concurrent multi-bot load; market & cancel aware
7. ✅ telemetry stream (Redpanda → ingester → Timescale + Redis), peak-TPS aggregation; **idempotent under at-least-once re-delivery** (aggregator dedups on a full-event identity hash → `duplicates_dropped` in the summary; proven: feeding the stream twice yields identical counts) + **graceful SIGTERM** (commits the Kafka offset, drains the in-flight buffer)
8. ✅ leaderboard (Redis ZSET + WebSocket API + live web UI with score breakdown, avg/peak TPS, and a **per-run p99-latency sparkline** that shows degradation over time); **graceful SIGTERM shutdown** (drains HTTP, Close-frames WS clients, releases Redis) + a **dependency-aware `/ready`** probe that flips to 503 on stale Redis while `/health` (liveness) stays green
9. ✅ submission API
10. ✅ sandbox runner (hardened: cpu-pin on Linux/quota fallback on Docker Desktop, ro-rootfs, caps dropped, NetworkPolicy; go/rust/cpp/binary builds)
11. ✅ orchestrator (lifecycle FSM) with **graceful SIGTERM shutdown** — stops the claim worker, drains HTTP, and cancels in-flight runs (which previously ran off `context.Background()` and survived a kill); same `signal.NotifyContext` + `srv.Shutdown` drain added to submission-api / sandbox-runner / control-panel
12. ✅ Kubernetes (validated cell + HPA) / Terraform (EKS/ECR/VPC)
13. ✅ resilience & failure-mode doc ([docs/RESILIENCE.md](docs/RESILIENCE.md)) + a dependency-free **chaos demo** ([scripts/run-chaos-demo.sh](scripts/run-chaos-demo.sh)) that injects an engine crash (fleet reconnects) and a SIGTERM (graceful drain) and asserts recovery
