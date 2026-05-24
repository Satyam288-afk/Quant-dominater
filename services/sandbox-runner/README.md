# Sandbox Runner

Go service boundary for building and running contestant engines.

This is intentionally local-first but now has two runner modes:

```text
local  - starts the built-in Go stub engine directly
docker - builds a submitted artifact into a Docker image and runs it
```

The Docker runner is the first real sandbox step. BuildKit and gVisor are still
future hardening layers.

## Run

```bash
make sandbox-runner
```

The API listens on `:9200` by default. Override with:

```bash
SANDBOX_RUNNER_ADDR=:9201 make sandbox-runner
```

Use Docker mode with:

```bash
SANDBOX_RUNNER_MODE=docker make sandbox-runner
```

## Endpoints

```text
GET  /health
POST /sandboxes/build
POST /sandboxes/start
GET  /sandboxes
GET  /sandboxes/{sandbox_id}
POST /sandboxes/{sandbox_id}/stop
```

## Local Runner Contract

```bash
curl -X POST http://localhost:9200/sandboxes/build \
  -H "Content-Type: application/json" \
  -d '{"submission_id":"sub_1","artifact_uri":"local://submissions/sub_1/artifact.zip","language":"go"}'
```

```bash
curl -X POST http://localhost:9200/sandboxes/start \
  -H "Content-Type: application/json" \
  -d '{"run_id":"run_1","image_ref":"local://sub_1","engine_mode":"normal","events_path":".runs/run_1/engine_outputs.jsonl"}'
```

This starts a local stub engine and returns a WebSocket endpoint.

In Docker mode, use the `image_ref` returned from `/sandboxes/build`; it will
use the `docker://...` scheme.

## Docker Artifact Format

For Docker mode, submit either:

```text
a zip file containing go.mod + main.go
a directory containing go.mod + main.go
a zip/directory containing a custom Dockerfile
```

If no Dockerfile is present and `language=go`, the runner generates a simple
multi-stage Dockerfile that builds the module and runs the resulting binary on
port `8080`.

The engine should implement:

```text
GET /health
WS  /ws
```

For the current Docker demo path, the runner starts the engine with:

```text
--addr :8080
--events /artifacts/engine_outputs.jsonl
```

If `engine_mode` is provided, it also passes:

```text
--mode <engine_mode>
```
