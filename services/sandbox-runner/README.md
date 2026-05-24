# Sandbox Runner

Go service boundary for building and running contestant engines.

This is intentionally local-first. The current implementation exposes the right
API shape, but its local runner starts the existing Go stub engine from
`examples/stub-engine`. Later implementations can replace this with Docker,
rootless BuildKit, and gVisor without changing the orchestrator contract.

## Run

```bash
make sandbox-runner
```

The API listens on `:9200` by default. Override with:

```bash
SANDBOX_RUNNER_ADDR=:9201 make sandbox-runner
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

## Local Contract

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
