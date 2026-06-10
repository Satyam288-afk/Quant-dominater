#!/usr/bin/env bash
# End-to-end live data plane demo:
#   bot-fleet --backend live ─▶ Redpanda ─▶ telemetry-ingester
#        ─▶ TimescaleDB (metrics_raw) + Redis (live run metrics)
#   validator ─▶ score-engine --backend live ─▶ scores table + Redis ZSET/scorecard
#   leaderboard-api (redis backend) ─▶ GET /leaderboard
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
LB_DIR="$ROOT_DIR/services/leaderboard-api"
RUN_DIR="$ROOT_DIR/.runs/live-demo"
COMPOSE_DIR="$ROOT_DIR/infra/docker-compose"

KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:19092}"
TIMESCALE_URL="${TIMESCALE_URL:-postgres://bench:bench@localhost:55432/bench}"
REDIS_URL="${REDIS_URL:-redis://localhost:56379/}"
TELEMETRY_TOPIC="${KAFKA_TELEMETRY_TOPIC:-telemetry.events.v1}"
# Unique per invocation: metrics_raw is append-only and the score-engine queries
# by run_id, so reusing a fixed id would let a previous run's rows (and their
# stale timestamps) pollute this run's duration/throughput. A fresh id isolates
# each benchmark cleanly — which is also how real runs are identified.
RUN_ID="${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
TEAM_ID="${TEAM_ID:-team_demo}"
BOTS="${BOTS:-50}"
ORDERS_PER_SEC="${ORDERS_PER_SEC:-5}"
DURATION_SEC="${DURATION_SEC:-5}"
EXPECTED_TPS="${EXPECTED_TPS:-$((BOTS * ORDERS_PER_SEC))}"
# Realistic order flow: shared symbols (real cross-bot trading), a multi-level
# price ladder (book depth + spread), variable sizes, and a slice of market
# orders. Defaults give a believable market while staying correctness-clean.
SYMBOLS="${SYMBOLS:-8}"
PRICE_LEVELS="${PRICE_LEVELS:-7}"
QTY_MAX="${QTY_MAX:-25}"
MARKET_PER_MILLE="${MARKET_PER_MILLE:-100}"
CANCEL_PER_MILLE="${CANCEL_PER_MILLE:-120}"
LB_ADDR="${LEADERBOARD_API_ADDR:-:9500}"

# CARGO_PROFILE=debug skips the release build for fast local iteration.
if [[ "${CARGO_PROFILE:-release}" == "debug" ]]; then
  PROFILE_FLAG=""
else
  PROFILE_FLAG="--release"
fi

mkdir -p "$RUN_DIR"
rm -f "$RUN_DIR"/{events.jsonl,contestant_outputs.jsonl,engine-events.jsonl,telemetry.jsonl,validation.json,telemetry-summary.json,score.json}

cleanup() {
  [[ -n "${ENGINE_PID:-}" ]] && kill "$ENGINE_PID" >/dev/null 2>&1 || true
  [[ -n "${INGESTER_PID:-}" ]] && kill "$INGESTER_PID" >/dev/null 2>&1 || true
  [[ -n "${LB_PID:-}" ]] && kill "$LB_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Preflight: fail fast with a clear message if the Docker daemon isn't up,
# rather than hanging on `compose up --wait`.
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker Desktop, then re-run." >&2
  echo "       (The dependency-free local slice — ./scripts/run-local-demo.sh — needs no Docker.)" >&2
  exit 1
fi

echo "[1/8] bringing up redpanda + timescale + redis"
# Only --wait the long-running services. redpanda-init is a one-shot topic
# creator that exits 0; recent Docker Compose treats a waited service exiting
# as a failure, which would abort the script under `set -e`. We run the init
# explicitly on the next line.
docker compose -f "$COMPOSE_DIR/docker-compose.yml" up -d --wait redpanda timescaledb redis
# Purge the telemetry topic so this run's ingester only ever sees this run's
# events. Redpanda retains the topic across runs; without this, a fresh consumer
# group reading from `earliest` would reprocess every prior run's events (stale
# timestamps, inflated counts). Delete + recreate gives each run a clean slate.
docker exec qd-redpanda rpk topic delete "$TELEMETRY_TOPIC" >/dev/null 2>&1 || true
docker compose -f "$COMPOSE_DIR/docker-compose.yml" run --rm redpanda-init >/dev/null 2>&1 || true

echo "[2/8] starting stub engine on :8080"
# Pre-flight: free :8080 so the fleet can't talk to a stale engine with
# accumulated cross-run book state (which would desync the validator).
if command -v lsof >/dev/null 2>&1; then
  lsof -ti tcp:8080 2>/dev/null | xargs kill -9 2>/dev/null || true
fi
(
  cd "$ENGINE_DIR"
  # disruptor engine: measured p99 2.00ms vs mutex 4.95ms at canonical load.
  go run . --addr :8080 --engine "${STUB_ENGINE:-disruptor}" --events "$RUN_DIR/engine-events.jsonl"
) &
ENGINE_PID=$!
for _ in {1..50}; do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

echo "[3/8] starting telemetry-ingester (kafka source → timescale + redis)"
(
  cd "$ROOT_DIR"
  cargo run -p telemetry-ingester --features live $PROFILE_FLAG --quiet -- \
    --source kafka \
    --kafka-brokers "$KAFKA_BROKERS" \
    --kafka-topic "$TELEMETRY_TOPIC" \
    --kafka-group-id "live-${RUN_ID}" \
    --run-id "$RUN_ID" \
    --timescale-url "$TIMESCALE_URL" \
    --redis-url "$REDIS_URL" \
    --summary-out "$RUN_DIR/telemetry-summary.json" \
    --flush-interval-ms 500 > "$RUN_DIR/ingester.log" 2>&1
) &
INGESTER_PID=$!
sleep 2

echo "[4/8] running bot fleet (live backend → kafka): bots=$BOTS rate=$ORDERS_PER_SEC dur=${DURATION_SEC}s"
(
  cd "$ROOT_DIR"
  cargo run -p bot-fleet --features kafka $PROFILE_FLAG --quiet -- \
    --target ws://localhost:8080/ws \
    --bots "$BOTS" \
    --orders-per-sec "$ORDERS_PER_SEC" \
    --duration-sec "$DURATION_SEC" \
    --seed 42 \
    --run-id "$RUN_ID" \
    --ws-connections 8 \
    --symbols "$SYMBOLS" \
    --price-levels "$PRICE_LEVELS" \
    --qty-max "$QTY_MAX" \
    --market-per-mille "$MARKET_PER_MILLE" \
    --cancel-per-mille "$CANCEL_PER_MILLE" \
    --backend live \
    --kafka-brokers "$KAFKA_BROKERS" \
    --kafka-topic "$TELEMETRY_TOPIC" \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl" \
    --telemetry-out "$RUN_DIR/telemetry.jsonl"
)

echo "[5/8] giving the ingester 3s to drain, then stopping it"
sleep 3
kill $INGESTER_PID 2>/dev/null || true
wait $INGESTER_PID 2>/dev/null || true

echo "[6/8] validating contestant fills against the reference orderbook"
(
  cd "$ROOT_DIR"
  cargo run -p validator $PROFILE_FLAG --quiet -- \
    --events "$RUN_DIR/events.jsonl" \
    --contestant-outputs "$RUN_DIR/contestant_outputs.jsonl"
) > "$RUN_DIR/validation.json" || true
echo "--- validation.json ---"
cat "$RUN_DIR/validation.json"

echo "[7/8] scoring (live backend → scores table + redis ZSET/scorecard)"
(
  cd "$ROOT_DIR"
  cargo run -p score-engine --features live $PROFILE_FLAG --quiet -- \
    --validation "$RUN_DIR/validation.json" \
    --run-id "$RUN_ID" \
    --backend live \
    --timescale-url "$TIMESCALE_URL" \
    --redis-url "$REDIS_URL" \
    --team-id "$TEAM_ID" \
    --expected-tps "$EXPECTED_TPS" \
    --out "$RUN_DIR/score.json"
)

echo "[8/8] verifying the data plane landed everywhere"
echo "--- redis: live run metrics (run:$RUN_ID:metrics) ---"
docker exec qd-redis redis-cli HGETALL "run:$RUN_ID:metrics" || true
echo "--- redis: global leaderboard ZSET (team → score) ---"
docker exec qd-redis redis-cli ZREVRANGE leaderboard:global 0 -1 WITHSCORES || true
echo "--- redis: scorecard (team:$TEAM_ID:scorecard) ---"
docker exec qd-redis redis-cli HGETALL "team:$TEAM_ID:scorecard" || true
echo "--- timescale: metrics_raw row count for $RUN_ID ---"
docker exec qd-timescale psql -U bench -d bench -t -c \
  "SELECT count(*) FROM metrics_raw WHERE run_id='$RUN_ID';"
echo "--- timescale: scores row for $RUN_ID ---"
docker exec qd-timescale psql -U bench -d bench -x -c \
  "SELECT run_id, team_id, valid, final_score, p99_ms, tps FROM scores WHERE run_id='$RUN_ID';"

echo "[+] starting leaderboard-api (redis backend) and querying it"
(
  cd "$LB_DIR"
  LEADERBOARD_BACKEND=redis REDIS_URL="$REDIS_URL" LEADERBOARD_API_ADDR="$LB_ADDR" \
    REPO_ROOT="$ROOT_DIR" go run .
) &
LB_PID=$!
LB_HOST="localhost${LB_ADDR}"
for _ in {1..50}; do
  if curl -fsS "http://$LB_HOST/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
echo "--- GET /leaderboard ---"
curl -fsS "http://$LB_HOST/leaderboard" || true
echo
echo "--- GET /runs/$RUN_ID/live ---"
curl -fsS "http://$LB_HOST/runs/$RUN_ID/live" || true
echo

echo "live demo artifacts: $RUN_DIR"
