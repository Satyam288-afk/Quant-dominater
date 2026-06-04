# IICPC Benchmark Platform

Vertical-slice implementation for the IICPC distributed trading engine benchmark.

This repo started with the smallest working benchmark core and now layers Go
control-plane services on top of it:

```text
Stub trading engine
  -> Rust bot fleet sends deterministic orders
  -> local JSONL telemetry captures latency/TPS
  -> Rust reference orderbook replays inputs
  -> validator prints VALID / INVALID
```

Kubernetes, Terraform, Redpanda, Redis, and MinIO are later layers. The current
milestone is a real local platform loop: submission metadata, sandbox boundary,
orchestration, benchmark execution, validation, and scoring artifacts.

## Current Components

| Path | Purpose |
|---|---|
| `docs/API_CONTRACT.md` | WebSocket/REST message contract |
| `proto/benchmark.proto` | Protobuf version of the benchmark messages |
| `examples/stub-engine` | Go WebSocket/REST contestant engine stub |
| `examples/rust-engine` | Rust WebSocket contestant engine stub |
| `services/submission-api` | Go API for storing submissions and queued run records |
| `services/sandbox-runner` | Go sandbox service boundary with local and Docker modes |
| `services/orchestrator` | Go lifecycle manager for queued benchmark runs |
| `services/score-engine` | Go scoring API for local run artifacts |
| `services/leaderboard-api` | Go live leaderboard API with WebSocket fanout |
| `rust/bot-fleet` | Rust Tokio bot fleet and local metrics collector |
| `rust/reference-orderbook` | Deterministic price-time reference matcher |
| `rust/validator` | Replays inputs and compares contestant fills |
| `services/control-panel` | Go API for creating and tracking local benchmark runs |
| `fixtures` | Tiny validation fixtures |
| `scripts/run-local-demo.sh` | One-command local slice demo |
| `scripts/run-price-time-proof.sh` | Correct engine vs intentionally broken engine proof |

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

Use Docker-backed sandboxing when Docker is running:

```bash
SANDBOX_RUNNER_MODE=docker make sandbox-runner
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
p50: 1.2ms
p90: 3.8ms
p99: 11.4ms
events_out: events.jsonl
outputs_out: contestant_outputs.jsonl
```

## Implementation Priority

1. API contract
2. stub engine
3. bot fleet
4. latency measurement
5. reference orderbook
6. validator
7. telemetry stream
8. leaderboard
9. submission API
10. sandbox runner
11. orchestrator
12. Kubernetes/Terraform
