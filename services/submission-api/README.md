# Submission API

Local-first Go service for accepting contestant artifacts and creating queued
benchmark run records.

This is the real entry point for the platform, but it deliberately starts with a
filesystem artifact store instead of MinIO. The next orchestrator layer can read
the records written here and decide how to build, sandbox, benchmark, validate,
and score each run.

## Run

From the repo root:

```bash
make submission-api
```

Or directly:

```bash
cd services/submission-api
REPO_ROOT=../.. go run .
```

The API listens on `:9100` by default. Override with:

```bash
SUBMISSION_API_ADDR=:9101 make submission-api
```

For isolated local demos or tests, override the metadata/artifact paths:

```bash
SUBMISSION_ARTIFACT_ROOT=.runs/platform-demo/submissions \
SUBMISSION_INDEX_PATH=.runs/platform-demo/submissions/index.json \
make submission-api
```

## Endpoints

```text
GET  /health
POST /submissions
GET  /submissions
GET  /submissions/{submission_id}
POST /submissions/{submission_id}/runs
GET  /runs
GET  /runs/{run_id}
```

## Create A Submission

```bash
curl -X POST http://localhost:9100/submissions \
  -F team_id=team_1 \
  -F language=go \
  -F protocol=ws-json \
  -F artifact=@examples/stub-engine/main.go
```

Artifacts are stored under:

```text
.artifacts/submissions/{submission_id}/
```

Submission and queued run metadata are indexed in:

```text
.artifacts/submissions/index.json
```

## Create A Queued Run

```bash
curl -X POST http://localhost:9100/submissions/{submission_id}/runs \
  -H "Content-Type: application/json" \
  -d '{
    "benchmark_seed": 42,
    "sandbox": {
      "cpu_limit": "1",
      "memory_limit": "512Mi",
      "network_egress": false
    },
    "config": {
      "bot_count": 10,
      "rate_per_bot": 2,
      "duration_sec": 5,
      "warmup_sec": 0
    }
  }'
```

The run is only queued here. The orchestrator service will consume this contract
and drive sandbox/build/benchmark state transitions.
