# Orchestrator

Go service that owns the benchmark lifecycle state machine.

The orchestrator reads queued run records from the submission API local metadata
store at `.artifacts/submissions/index.json`, advances each run through the
benchmark lifecycle, and writes per-run artifacts under `.runs/{run_id}`.

This is still local-first: it starts the existing Go stub engine and Rust
bot-fleet/validator directly. The service boundary is shaped so the local engine
starter can later be replaced by the sandbox-runner Docker/gVisor path.

## Run

```bash
make orchestrator
```

The API listens on `:9300` by default. Override with:

```bash
ORCHESTRATOR_ADDR=:9301 make orchestrator
```

## Endpoints

```text
GET  /health
GET  /runs
GET  /runs/{run_id}
POST /runs/{run_id}/start
POST /runs/{run_id}/cancel
POST /runs/next
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
```

Artifacts are written to:

```text
.runs/{run_id}/
```
