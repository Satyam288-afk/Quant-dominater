#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
RUN_ROOT="$ROOT_DIR/.runs/price-time-proof"
PORT=8081
ENGINE_BIN="$RUN_ROOT/stub-engine"

mkdir -p "$RUN_ROOT"
(
  cd "$ENGINE_DIR"
  go build -o "$ENGINE_BIN" .
)

ENGINE_PID=""
cleanup() {
  if [[ -n "$ENGINE_PID" ]]; then
    kill "$ENGINE_PID" >/dev/null 2>&1 || true
    wait "$ENGINE_PID" >/dev/null 2>&1 || true
    ENGINE_PID=""
  fi
}
trap cleanup EXIT

wait_for_health() {
  for _ in {1..50}; do
    if curl -fsS "http://localhost:$PORT/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "engine did not become healthy" >&2
  return 1
}

run_case() {
  local mode="$1"
  local expected="$2"
  local run_dir="$RUN_ROOT/$mode"
  mkdir -p "$run_dir"
  rm -f "$run_dir/events.jsonl" "$run_dir/contestant_outputs.jsonl" "$run_dir/engine-events.jsonl" "$run_dir/validation.json"

  echo "starting stub engine mode=$mode"
  "$ENGINE_BIN" --addr ":$PORT" --mode "$mode" --events "$run_dir/engine-events.jsonl" &
  ENGINE_PID=$!
  wait_for_health

  echo "probing price-time priority mode=$mode"
  (
    cd "$ROOT_DIR"
    cargo run -p bot-fleet --bin price-time-probe -- \
      --target "ws://localhost:$PORT/ws" \
      --run-id "run_price_time_${mode//-/_}" \
      --events-out "$run_dir/events.jsonl" \
      --outputs-out "$run_dir/contestant_outputs.jsonl"
  )

  set +e
  (
    cd "$ROOT_DIR"
    cargo run -p validator -- \
      --events "$run_dir/events.jsonl" \
      --contestant-outputs "$run_dir/contestant_outputs.jsonl"
  ) > "$run_dir/validation.json"
  local status=$?
  set -e

  cleanup

  cat "$run_dir/validation.json"
  if [[ "$expected" == "valid" && "$status" -ne 0 ]]; then
    echo "expected validator to pass for mode=$mode" >&2
    return 1
  fi
  if [[ "$expected" == "invalid" && "$status" -eq 0 ]]; then
    echo "expected validator to fail for mode=$mode" >&2
    return 1
  fi
}

run_case normal valid
run_case broken-price-time-priority invalid

echo "price-time proof artifacts written to $RUN_ROOT"
