# Orchestrator

Go service that owns the benchmark lifecycle state machine.

The orchestrator reads queued run records from the submission API local metadata
store at `.artifacts/submissions/index.json`, advances each run through the
benchmark lifecycle, and writes per-run artifacts under `.runs/{run_id}`.

This is still local-first: it reads the submission API's local JSON metadata and
runs the Rust bot-fleet/validator directly. Engine build/start/stop already goes
through the sandbox-runner HTTP boundary so that Docker/gVisor can replace the
sandbox internals later.

## Run

```bash
make orchestrator
```

Start `sandbox-runner` first in another terminal:

```bash
make sandbox-runner
```

The API listens on `:9300` by default. Override with:

```bash
ORCHESTRATOR_ADDR=:9301 SANDBOX_RUNNER_URL=http://127.0.0.1:9200 make orchestrator
```

Runs are capped by `ORCHESTRATOR_RUN_TIMEOUT`, defaulting to `3m`:

```bash
ORCHESTRATOR_RUN_TIMEOUT=90s make orchestrator
```

## Endpoints

```text
GET  /health
GET  /runs
GET  /runs/{run_id}
POST /runs/{run_id}/start
POST /runs/{run_id}/cancel
POST /runs/next
POST /api/benchmark
```

## Local Flow

1. Create a submission with `services/submission-api`.
2. Create a queued run with `POST /submissions/{submission_id}/runs`.
3. Start the next queued run:

```bash
curl -X POST http://localhost:9300/runs/next
```

The run moves through:

```text
QUEUED
BUILDING
SANDBOX_STARTING
HEALTHCHECKING
BENCHMARKING
VALIDATING
SCORING
FINISHED
TIMED_OUT
```

Artifacts are written to:

```text
.runs/{run_id}/
```

## Direct Benchmark

Judges or teammates can benchmark a running engine without creating a
submission:

```bash
curl -X POST http://localhost:9300/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{
    "endpoint_url": "ws://localhost:8080/ws",
    "benchmark_seed": 42,
    "config": {
      "bot_count": 10,
      "rate_per_bot": 2,
      "duration_sec": 5
    }
  }'
```

The orchestrator still writes a `.runs/direct_.../` artifact directory and
returns metrics, validation, and score in the response.
