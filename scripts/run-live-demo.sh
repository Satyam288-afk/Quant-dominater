#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
RUN_DIR="$ROOT_DIR/.runs/live-demo"
COMPOSE_DIR="$ROOT_DIR/infra/docker-compose"

KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:19092}"
TIMESCALE_URL="${TIMESCALE_URL:-postgres://bench:bench@localhost:55432/bench}"
REDIS_URL="${REDIS_URL:-redis://localhost:56379/}"
TELEMETRY_TOPIC="${KAFKA_TELEMETRY_TOPIC:-telemetry.events.v1}"

mkdir -p "$RUN_DIR"
rm -f "$RUN_DIR"/{events.jsonl,contestant_outputs.jsonl,engine-events.jsonl,telemetry.jsonl,validation.json,telemetry-summary.json,score.json}

cleanup() {
  [[ -n "${ENGINE_PID:-}" ]] && kill "$ENGINE_PID" >/dev/null 2>&1 || true
  [[ -n "${INGESTER_PID:-}" ]] && kill "$INGESTER_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[1/6] bringing up redpanda + timescale + redis"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" up -d --wait
docker compose -f "$COMPOSE_DIR/docker-compose.yml" run --rm redpanda-init >/dev/null 2>&1 || true

echo "[2/6] starting stub engine on :8080"
(
  cd "$ENGINE_DIR"
  go run . --addr :8080 --events "$RUN_DIR/engine-events.jsonl"
) &
ENGINE_PID=$!
for _ in {1..50}; do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

echo "[3/6] starting telemetry-ingester (kafka source → timescale + redis)"
(
  cd "$ROOT_DIR"
  cargo run -p telemetry-ingester --features live --release --quiet -- \
    --source kafka \
    --kafka-brokers "$KAFKA_BROKERS" \
    --kafka-topic "$TELEMETRY_TOPIC" \
    --kafka-group-id "live-demo" \
    --timescale-url "$TIMESCALE_URL" \
    --redis-url "$REDIS_URL" \
    --summary-out "$RUN_DIR/telemetry-summary.json" \
    --flush-interval-ms 500 > "$RUN_DIR/ingester.log" 2>&1
) &
INGESTER_PID=$!
sleep 2

echo "[4/6] running bot fleet (live backend → kafka)"
(
  cd "$ROOT_DIR"
  cargo run -p bot-fleet --features kafka --release --quiet -- \
    --target ws://localhost:8080/ws \
    --bots 50 \
    --orders-per-sec 5 \
    --duration-sec 5 \
    --seed 42 \
    --ws-connections 8 \
    --backend live \
    --kafka-brokers "$KAFKA_BROKERS" \
    --kafka-topic "$TELEMETRY_TOPIC" \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl" \
    --telemetry-out "$RUN_DIR/telemetry.jsonl"
)

echo "[5/6] giving the ingester 3s to drain"
sleep 3
kill $INGESTER_PID 2>/dev/null || true
wait $INGESTER_PID 2>/dev/null || true

echo "[6/6] checking redis + timescale"
echo "--- redis HGETALL run:run_local_001:metrics ---"
docker exec qd-redis redis-cli HGETALL run:run_local_001:metrics || true
echo "--- timescale row count ---"
docker exec qd-timescale psql -U bench -d bench -t -c \
  "SELECT count(*) FROM metrics_raw WHERE run_id='run_local_001';"

echo "live demo artifacts: $RUN_DIR"
