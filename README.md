# IICPC Benchmark Platform

Vertical-slice implementation for the IICPC distributed trading engine benchmark.

This repo starts with the smallest working benchmark core:

```text
Stub trading engine
  -> Rust bot fleet sends deterministic orders
  -> local JSONL telemetry captures latency/TPS
  -> Rust reference orderbook replays inputs
  -> validator prints VALID / INVALID
```

Kubernetes, Terraform, Redpanda, Redis, MinIO, and sandboxing are intentionally left as later layers. The first milestone is proving that the platform can measure speed and gate correctness.

## Current Components

| Path | Purpose |
|---|---|
| `docs/API_CONTRACT.md` | WebSocket/REST message contract |
| `proto/benchmark.proto` | Protobuf version of the benchmark messages |
| `examples/stub-engine` | Go WebSocket/REST contestant engine stub |
| `rust/bot-fleet` | Rust Tokio bot fleet and local metrics collector |
| `rust/reference-orderbook` | Deterministic price-time reference matcher |
| `rust/validator` | Replays inputs and compares contestant fills |
| `fixtures` | Tiny validation fixtures |
| `scripts/run-local-demo.sh` | One-command local slice demo |

## Prerequisites

Install Go and Rust locally:

```bash
go version
rustc --version
cargo --version
```

## Run The Local Slice

Terminal 1:

```bash
cd /Users/satyamkumar/iicpc/examples/stub-engine
go run . --addr :8080 --events engine-events.jsonl
```

Terminal 2:

```bash
cd /Users/satyamkumar/iicpc
cargo run -p bot-fleet -- \
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
