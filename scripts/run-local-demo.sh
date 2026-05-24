#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
RUN_DIR="$ROOT_DIR/.runs/local-demo"

mkdir -p "$RUN_DIR"
rm -f "$RUN_DIR/events.jsonl" "$RUN_DIR/contestant_outputs.jsonl" "$RUN_DIR/engine-events.jsonl"

cleanup() {
  if [[ -n "${ENGINE_PID:-}" ]]; then
    kill "$ENGINE_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "starting stub engine on :8080"
(
  cd "$ENGINE_DIR"
  go run . --addr :8080 --events "$RUN_DIR/engine-events.jsonl"
) &
ENGINE_PID=$!

for _ in {1..50}; do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

curl -fsS http://localhost:8080/health

echo "running bot fleet"
(
  cd "$ROOT_DIR"
  cargo run -p bot-fleet --bin bot-fleet -- \
    --target ws://localhost:8080/ws \
    --bots 10 \
    --orders-per-sec 2 \
    --duration-sec 5 \
    --seed 42 \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl"
)

echo "validating outputs"
(
  cd "$ROOT_DIR"
  cargo run -p validator -- \
    --events "$RUN_DIR/events.jsonl" \
    --contestant-outputs "$RUN_DIR/contestant_outputs.jsonl"
)

echo "local demo artifacts written to $RUN_DIR"
