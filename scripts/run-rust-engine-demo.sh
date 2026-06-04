#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.runs/rust-engine-demo"
PORT=18080

mkdir -p "$RUN_DIR"
find "$RUN_DIR" -maxdepth 1 -type f \( -name '*.jsonl' -o -name 'validation.json' \) -delete

ENGINE_PID=""
cleanup() {
  if [[ -n "$ENGINE_PID" ]]; then
    kill "$ENGINE_PID" >/dev/null 2>&1 || true
    wait "$ENGINE_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "starting Rust engine on :$PORT"
(
  cd "$ROOT_DIR"
  cargo run -q -p rust-engine -- --addr ":$PORT" --events "$RUN_DIR/engine-events.jsonl"
) &
ENGINE_PID=$!

for _ in {1..80}; do
  if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null

echo "running bot fleet"
(
  cd "$ROOT_DIR"
  cargo run -q -p bot-fleet --bin bot-fleet -- \
    --target "ws://127.0.0.1:$PORT/ws" \
    --bots 1 \
    --orders-per-sec 2 \
    --duration-sec 2 \
    --seed 42 \
    --run-id rust_engine_demo \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl"
)

echo "validating outputs"
(
  cd "$ROOT_DIR"
  cargo run -q -p validator -- \
    --events "$RUN_DIR/events.jsonl" \
    --contestant-outputs "$RUN_DIR/contestant_outputs.jsonl"
) | tee "$RUN_DIR/validation.json"

echo "Rust engine demo artifacts written to $RUN_DIR"
