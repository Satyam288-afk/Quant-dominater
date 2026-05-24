# Control Panel

Local Go API for operating the IICPC benchmark vertical slice.

It does not replace the existing benchmark core. It starts the current Go stub
engine, runs the Rust bot fleet against it, validates the resulting JSONL files,
and records an artifact folder for each run.

## Run

From this directory:

```bash
go run .
```

Or from anywhere:

```bash
REPO_ROOT=/Users/satyamkumar/iicpc go run /Users/satyamkumar/iicpc/services/control-panel
```

The API listens on `:9000` by default. Override it with:

```bash
CONTROL_PANEL_ADDR=:9001 go run .
```

## Endpoints

```text
POST   /api/runs
GET    /api/runs
GET    /api/runs/{run_id}
GET    /api/runs/{run_id}/logs
GET    /api/runs/{run_id}/artifacts
POST   /api/runs/{run_id}/cancel
```

Example:

```bash
curl -X POST http://localhost:9000/api/runs \
  -H "Content-Type: application/json" \
  -d '{
    "team_id": "team_1",
    "engine_mode": "normal",
    "bot_count": 10,
    "orders_per_sec": 5,
    "duration_sec": 5,
    "seed": 42
  }'
```

Each run writes:

```text
.runs/{run_id}/run_spec.json
.runs/{run_id}/engine_outputs.jsonl
.runs/{run_id}/events.jsonl
.runs/{run_id}/contestant_outputs.jsonl
.runs/{run_id}/metrics.json
.runs/{run_id}/validation.json
.runs/{run_id}/score.json
.runs/{run_id}/run.log
```
