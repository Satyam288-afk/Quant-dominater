#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
RUN_DIR="$ROOT_DIR/.runs/local-demo"

mkdir -p "$RUN_DIR"
rm -f "$RUN_DIR/events.jsonl" "$RUN_DIR/contestant_outputs.jsonl" "$RUN_DIR/engine-events.jsonl" "$RUN_DIR/telemetry.jsonl"

# Pre-flight: free :8080. A stub engine left listening from a previous run keeps
# accumulating book state across runs; the bot fleet would silently talk to the
# stale engine (its book is non-empty) while the validator replays from an empty
# book — producing spurious correctness violations. Kill any straggler first.
if command -v lsof >/dev/null 2>&1; then
  lsof -ti tcp:8080 2>/dev/null | xargs kill -9 2>/dev/null || true
fi

cleanup() {
  if [[ -n "${ENGINE_PID:-}" ]]; then
    kill "$ENGINE_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "starting stub engine on :8080"
# Set STUB_PPROF=:6060 to expose net/http/pprof on the engine (see docs/PROFILING.md).
(
  cd "$ENGINE_DIR"
  go run . --addr :8080 --events "$RUN_DIR/engine-events.jsonl" ${STUB_PPROF:+--pprof "$STUB_PPROF"}
) &
ENGINE_PID=$!

for _ in {1..50}; do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

curl -fsS http://localhost:8080/health

echo "running bot fleet (24 bots over 4 ws conns, 4 shared symbols, 5-level price ladder, ~10% market + ~12% cancel orders)"
(
  cd "$ROOT_DIR"
  cargo run -p bot-fleet --bin bot-fleet -- \
    --target ws://localhost:8080/ws \
    --bots 24 \
    --orders-per-sec 5 \
    --duration-sec 5 \
    --seed 42 \
    --ws-connections 4 \
    --symbols 4 \
    --price-levels 5 \
    --qty-max 10 \
    --market-per-mille 100 \
    --cancel-per-mille 120 \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl" \
    --telemetry-out "$RUN_DIR/telemetry.jsonl"
)

echo "validating outputs"
(
  cd "$ROOT_DIR"
  cargo run -p validator -- \
    --events "$RUN_DIR/events.jsonl" \
    --contestant-outputs "$RUN_DIR/contestant_outputs.jsonl"
)

echo "local demo artifacts written to $RUN_DIR"
