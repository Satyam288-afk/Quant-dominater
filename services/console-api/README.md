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

## Security

This service has no operator auth of its own and attaches `SERVICE_AUTH_TOKEN`
to every backend call, so it must not be exposed beyond the local operator:

- It binds to `127.0.0.1:9700` by default so it is not reachable from the LAN.
  Set `CONSOLE_API_ADDR` (e.g. `:9700` / `0.0.0.0:9700`) only when fronting it
  with real operator authentication.
- Mutating `POST` routes reject cross-site browser requests (Origin /
  `Sec-Fetch-Site` allowlist) to blunt drive-by CSRF; same-origin calls from the
  served UI and non-browser clients (curl) still work.

Production deployments should front this service with real operator auth.
