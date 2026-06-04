# Score Engine

Local-first Go service for combining validation, latency, throughput, and
stability metrics into a correctness-gated final score.

## Run

```bash
cd services/score-engine
REPO_ROOT=../.. go run .
```

The API listens on `:9400` by default. Override with:

```bash
SCORE_ENGINE_ADDR=:9401 go run .
```

## Endpoints

```text
GET  /health
POST /score
POST /runs/{run_id}/score
GET  /runs/{run_id}/score
```

`POST /runs/{run_id}/score` reads:

```text
.runs/{run_id}/run_spec.json
.runs/{run_id}/metrics.json
.runs/{run_id}/validation.json
```

and writes:

```text
.runs/{run_id}/score.json
```
