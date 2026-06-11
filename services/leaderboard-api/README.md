# Leaderboard API

Go leaderboard service with live WebSocket fanout. Two interchangeable backends:

| Backend | Selected by | Source of truth |
|---|---|---|
| `redis` | `LEADERBOARD_BACKEND=redis` (or any `REDIS_URL`) | `leaderboard:global` ZSET + `team:{id}:scorecard` hashes written by the score-engine, plus `run:{id}:metrics` written by the telemetry-ingester |
| `file` | default | `.leaderboard/leaderboard.json` (dependency-free local slice + unit tests) |

In `redis` mode the service polls Redis on a short interval (`LEADERBOARD_POLL_MS`,
default 500ms), builds the ranked snapshot, and pushes it to every WebSocket
subscriber whenever it changes — this is the live data path the frontend consumes.

## Run

File backend (local slice):

```bash
cd services/leaderboard-api
REPO_ROOT=../.. go run .
```

Redis backend (live data plane):

```bash
cd services/leaderboard-api
LEADERBOARD_BACKEND=redis REDIS_URL=redis://localhost:56379/ go run .
```

The API listens on `:9500` by default. Override with `LEADERBOARD_API_ADDR=:9501`.

## Endpoints

```text
GET  /health
GET  /leaderboard          ranked entries with score breakdown + latency percentiles
POST /leaderboard/runs     upsert an entry (writes ZSET + scorecard in redis mode)
GET  /runs/{id}/live       in-flight run counters from the ingester (redis mode only)
GET  /ws                   WebSocket: initial snapshot + push on every change
```

Example:

```bash
curl -X POST http://localhost:9500/leaderboard/runs \
  -H "Content-Type: application/json" \
  -d '{"run_id":"run_1","team_id":"team_1","score":98.5,"valid":true,"tps":20,"p99_ms":4.2}'
```
