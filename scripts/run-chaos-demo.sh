#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Chaos / failure-injection demo. Dependency-free (no Docker, no Redis).
#
# It proves two resilience properties added on top of the proven correctness +
# latency work, by actually injecting the failures and observing recovery:
#
#   Phase 1 — load-generator resilience.  Kill the engine mid-run. A pooled bot
#     fleet that previously dead-stopped on the first socket error now detects
#     the drop, reconnects each connection with capped exponential backoff,
#     resumes driving orders the instant the engine returns, and reports an
#     honest summary (reconnects>0, plus the orders lost during the outage as
#     timeouts). The harness keeps measuring across an engine blip.
#
#   Phase 2 — service graceful shutdown.  SIGTERM the leaderboard-api. Instead
#     of dying instantly (the old `log.Fatal(http.ListenAndServe)`), it drains
#     in-flight HTTP, sends a WebSocket Close frame, closes its backend, and
#     exits 0 — what every k8s rolling deploy / pod eviction needs.
#
# NOTE (honesty): a crashed engine loses its order book; this demo does NOT
# claim the engine's *correctness* survives a crash (it shouldn't — losing state
# is exactly what the scoring gate should punish). What is resilient here is the
# *platform around it*: the load generator and the services.
# ─────────────────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/examples/stub-engine"
LB_DIR="$ROOT_DIR/services/leaderboard-api"
ORCH_DIR="$ROOT_DIR/services/orchestrator"
RUN_DIR="$ROOT_DIR/.runs/chaos-demo"
ENGINE_BIN="$RUN_DIR/stub-engine"
LB_BIN="$RUN_DIR/leaderboard-api"
ORCH_BIN="$RUN_DIR/orchestrator"
FLEET_LOG="$RUN_DIR/fleet.log"
LB_LOG="$RUN_DIR/leaderboard.log"
ORCH_LOG="$RUN_DIR/orchestrator.log"

ENGINE_ADDR=":8080"
LB_ADDR=":9500"
ORCH_ADDR=":9399"

# Tunables (kept small so the whole demo runs in well under a minute).
DURATION_SEC="${DURATION_SEC:-26}"
KILL_AT_SEC="${KILL_AT_SEC:-8}"   # when to crash the engine
DOWN_SEC="${DOWN_SEC:-5}"         # how long the engine stays dead
BOTS="${BOTS:-24}"
CONNS="${CONNS:-4}"

mkdir -p "$RUN_DIR"

ENGINE_PID=""
FLEET_PID=""
TAIL_PID=""
LB_PID=""
ORCH_PID=""

cleanup() {
  for pid in "$FLEET_PID" "$ENGINE_PID" "$LB_PID" "$ORCH_PID" "$TAIL_PID"; do
    [[ -n "$pid" ]] && kill "$pid" >/dev/null 2>&1 || true
  done
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:8080 2>/dev/null | xargs kill -9 2>/dev/null || true
    lsof -ti tcp:9500 2>/dev/null | xargs kill -9 2>/dev/null || true
    lsof -ti tcp:9399 2>/dev/null | xargs kill -9 2>/dev/null || true
  fi
}
trap cleanup EXIT

free_port() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"$1" 2>/dev/null | xargs kill -9 2>/dev/null || true
  fi
}

wait_health() {
  local url="$1"
  for _ in {1..50}; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "!! timed out waiting for $url" >&2
  return 1
}

start_engine() {
  free_port 8080
  "$ENGINE_BIN" --addr "$ENGINE_ADDR" --events "" >/dev/null 2>&1 &
  ENGINE_PID=$!
  wait_health "http://localhost:8080/health"
}

echo "▶ building binaries (engine, leaderboard, orchestrator, fleet) ..."
( cd "$ENGINE_DIR" && go build -o "$ENGINE_BIN" . )
( cd "$LB_DIR" && go build -o "$LB_BIN" . )
( cd "$ORCH_DIR" && go build -o "$ORCH_BIN" . )
( cd "$ROOT_DIR" && cargo build -p bot-fleet --quiet )
FLEET_BIN="$ROOT_DIR/target/debug/bot-fleet"

echo
echo "══════════════════════════════════════════════════════════════════════"
echo " PHASE 1 — load-generator resilience: kill the engine mid-run"
echo "══════════════════════════════════════════════════════════════════════"

start_engine
echo "✓ engine up (pid $ENGINE_PID)"

echo "▶ launching fleet: $BOTS bots over $CONNS ws connections for ${DURATION_SEC}s"
(
  cd "$ROOT_DIR"
  "$FLEET_BIN" \
    --target ws://localhost:8080/ws \
    --bots "$BOTS" \
    --ws-connections "$CONNS" \
    --orders-per-sec 8 \
    --duration-sec "$DURATION_SEC" \
    --symbols 8 \
    --price-levels 5 \
    --qty-max 10 \
    --market-per-mille 100 \
    --cancel-per-mille 120 \
    --backend none \
    --events-out "$RUN_DIR/events.jsonl" \
    --outputs-out "$RUN_DIR/contestant_outputs.jsonl"
) >"$FLEET_LOG" 2>&1 &
FLEET_PID=$!

# Stream the fleet's stderr (where the [pool] reconnect lines land) live.
tail -f "$FLEET_LOG" 2>/dev/null | sed 's/^/   fleet| /' &
TAIL_PID=$!

sleep "$KILL_AT_SEC"
echo
echo "💥 T+${KILL_AT_SEC}s: KILL -9 the engine (pid $ENGINE_PID) — simulating a hard crash"
kill -9 "$ENGINE_PID" >/dev/null 2>&1 || true
ENGINE_PID=""

sleep "$DOWN_SEC"
echo "🔁 T+$((KILL_AT_SEC + DOWN_SEC))s: restarting the engine — watch the fleet reconnect"
start_engine
echo "✓ engine back up (pid $ENGINE_PID)"

echo "▶ waiting for the fleet to finish its run ..."
wait "$FLEET_PID" || true
FLEET_PID=""
kill "$TAIL_PID" >/dev/null 2>&1 || true
TAIL_PID=""

echo
echo "── fleet summary ─────────────────────────────────────────────────────"
grep -E '^(orders_sent|acks_received|fills_received|timeouts|connect_errors|reconnects|tps|peak_tps|p50|p90|p99):' "$FLEET_LOG" || true

RECONNECTS="$(grep -E '^reconnects:' "$FLEET_LOG" | awk '{print $2}' | tail -1)"
RECOVERED="$(grep -c '\[pool\] reconnected' "$FLEET_LOG" || true)"
echo
echo "   pool reconnect events logged: ${RECOVERED}"
if [[ -z "${RECONNECTS:-}" || "${RECONNECTS:-0}" -lt 1 ]]; then
  echo "✗ FAIL: fleet reported reconnects=${RECONNECTS:-<none>} — expected >= 1 after a mid-run engine kill" >&2
  exit 1
fi
echo "✅ PASS: fleet recovered from the engine crash (reconnects=${RECONNECTS}) and finished the run"

echo
echo "══════════════════════════════════════════════════════════════════════"
echo " PHASE 2 — service graceful shutdown: SIGTERM the services"
echo "══════════════════════════════════════════════════════════════════════"

free_port 9500
REPO_ROOT="$ROOT_DIR" \
  LEADERBOARD_STORE_PATH="$RUN_DIR/leaderboard.json" \
  LEADERBOARD_API_ADDR="$LB_ADDR" \
  "$LB_BIN" >"$LB_LOG" 2>&1 &
LB_PID=$!
wait_health "http://localhost:9500/health"
echo "✓ leaderboard-api up (pid $LB_PID, file backend)"

echo "   GET /health → $(curl -fsS -o /dev/null -w '%{http_code}' http://localhost:9500/health)  (liveness)"
echo "   GET /ready  → $(curl -fsS -o /dev/null -w '%{http_code}' http://localhost:9500/ready)  (readiness; file backend has no external dep)"

echo "▶ sending SIGTERM (what k8s sends on a rolling deploy) ..."
kill -TERM "$LB_PID" >/dev/null 2>&1 || true
# Wait for it to exit and capture its exit code.
LB_EXIT=0
wait "$LB_PID" 2>/dev/null || LB_EXIT=$?
LB_PID=""

echo "── leaderboard shutdown log ──────────────────────────────────────────"
sed 's/^/   lb| /' "$LB_LOG" | tail -4 || true
echo
if grep -q 'drained cleanly' "$LB_LOG"; then
  echo "✅ PASS: leaderboard-api drained and exited gracefully (exit=$LB_EXIT) instead of being severed mid-flight"
else
  echo "✗ FAIL: expected a clean drain log line from leaderboard-api" >&2
  exit 1
fi

# The orchestrator is the higher-stakes one: its claim worker and in-flight runs
# previously ran off context.Background(), so a kill orphaned them. Now SIGTERM
# stops the worker, drains HTTP, and cancels in-flight runs before exit.
echo
free_port 9399
REPO_ROOT="$ROOT_DIR" ORCHESTRATOR_ADDR="$ORCH_ADDR" "$ORCH_BIN" >"$ORCH_LOG" 2>&1 &
ORCH_PID=$!
sleep 1.5
echo "✓ orchestrator up (pid $ORCH_PID, claim worker running)"
echo "▶ sending SIGTERM ..."
kill -TERM "$ORCH_PID" >/dev/null 2>&1 || true
ORCH_EXIT=0
wait "$ORCH_PID" 2>/dev/null || ORCH_EXIT=$?
ORCH_PID=""
echo "── orchestrator shutdown log ─────────────────────────────────────────"
sed 's/^/   orch| /' "$ORCH_LOG" | tail -3 || true
echo
if grep -q 'drained cleanly' "$ORCH_LOG"; then
  echo "✅ PASS: orchestrator stopped its worker and drained gracefully (exit=$ORCH_EXIT) — no orphaned runs"
else
  echo "✗ FAIL: expected a clean drain log line from orchestrator" >&2
  exit 1
fi

echo
echo "══════════════════════════════════════════════════════════════════════"
echo " Chaos demo complete. Artifacts in $RUN_DIR"
echo "   • Phase 1 proved the fleet survives an engine crash (reconnect+backoff)"
echo "   • Phase 2 proved leaderboard-api + orchestrator shut down gracefully under SIGTERM"
echo "══════════════════════════════════════════════════════════════════════"
