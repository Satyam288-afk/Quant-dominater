# Leaderboard API

Local-first Go leaderboard service with JSON persistence and WebSocket fanout.

Redis sorted sets/pubsub can replace the store later; this service keeps the API
surface runnable for the local control-plane slice.

## Run

```bash
cd services/leaderboard-api
REPO_ROOT=../.. go run .
```

The API listens on `:9500` by default. Override with:

```bash
LEADERBOARD_API_ADDR=:9501 go run .
```

## Endpoints

```text
GET  /health
GET  /leaderboard
POST /leaderboard/runs
GET  /ws
```

Example:

```bash
curl -X POST http://localhost:9500/leaderboard/runs \
  -H "Content-Type: application/json" \
  -d '{"run_id":"run_1","team_id":"team_1","score":98.5,"valid":true,"tps":20,"p99_ms":4.2}'
```
