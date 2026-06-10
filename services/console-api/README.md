# Console API

Browser-facing gateway for the local benchmark platform.

The console UI should not call every control-plane service directly. This
service gives the browser one local origin and coordinates:

```text
browser -> console-api -> submission-api
                     -> orchestrator
                     -> leaderboard-api
                     -> .runs artifact files
```

## Run

Start the full interactive stack from the repo root:

```bash
make console-stack
```

Or run this service directly after starting the dependencies:

```bash
CONSOLE_API_ADDR=:9700 \
SUBMISSION_API_URL=http://127.0.0.1:9110 \
ORCHESTRATOR_URL=http://127.0.0.1:9310 \
LEADERBOARD_URL=http://127.0.0.1:9510 \
make console-api
```

Open:

```text
http://localhost:9700/
```

## Endpoints

```text
GET  /health
GET  /api/health
POST /api/submissions
POST /api/submissions/{submission_id}/runs
GET  /api/runs
GET  /api/runs/{run_id}
GET  /api/runs/{run_id}/artifacts
GET  /api/runs/{run_id}/artifacts/{name}
GET  /api/leaderboard
```

Artifact downloads are restricted to paths under the repo `.runs/` directory.
