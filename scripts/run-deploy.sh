#!/usr/bin/env bash
# Single-container deploy entrypoint (Render / Railway / Fly). Starts the 4
# backend Go services on container-localhost and the console-api gateway on the
# public $PORT. The evaluator runs in LOCAL mode (the uploaded engine is built +
# run as a host subprocess in this container) — explicitly opted in via
# SANDBOX_ALLOW_UNSAFE_LOCAL. Safe because the operator hosts known engines here;
# do NOT use this image to host arbitrary untrusted submissions.
set -euo pipefail

ROOT_DIR="${REPO_ROOT:-/app}"
PORT="${PORT:-8080}"
BIN="${ROOT_DIR}/bin"
STATE_DIR="${STATE_DIR:-/data}"

# --- auth: every backend is fail-closed (REQUIRE_AUTH=1); generate a token if
# the platform did not inject one. console-api has no inbound auth (it is the
# browser entry) but forwards this token to the backends.
: "${SERVICE_AUTH_TOKEN:=$(head -c 32 /dev/urandom | xxd -p -c 64 2>/dev/null || openssl rand -hex 32)}"
export SERVICE_AUTH_TOKEN
export REQUIRE_AUTH="${REQUIRE_AUTH:-1}"
export SANDBOX_ALLOW_UNSAFE_LOCAL="${SANDBOX_ALLOW_UNSAFE_LOCAL:-1}"
export BOT_FLEET_BIN="${BOT_FLEET_BIN:-$BIN/bot-fleet}"
export VALIDATOR_BIN="${VALIDATOR_BIN:-$BIN/validator}"

mkdir -p "$STATE_DIR/submissions"
SUBMISSION_INDEX="$STATE_DIR/submissions/index.json"
LEADERBOARD_STORE="$STATE_DIR/leaderboard.json"

# Internal services bind container-localhost; only console-api is public.
SUB=127.0.0.1:9100;  SUB_URL="http://$SUB"
SBX=127.0.0.1:9200;  SBX_URL="http://$SBX"
ORCH=127.0.0.1:9300; ORCH_URL="http://$ORCH"
LB=127.0.0.1:9500;   LB_URL="http://$LB"

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

wait_health() {
  local url="$1" name="$2"
  for _ in $(seq 1 120); do
    curl -fsS "$url/health" >/dev/null 2>&1 && { echo "$name healthy"; return 0; }
    sleep 0.5
  done
  echo "ERROR: $name not healthy at $url" >&2
  return 1
}

cd "$ROOT_DIR"
echo "deploy: token set (len ${#SERVICE_AUTH_TOKEN}), sandbox=local(unsafe-opt-in), public port=$PORT"

REPO_ROOT="$ROOT_DIR" SUBMISSION_API_ADDR="$SUB" \
  SUBMISSION_ARTIFACT_ROOT="$STATE_DIR/submissions" SUBMISSION_INDEX_PATH="$SUBMISSION_INDEX" \
  "$BIN/submission-api" & PIDS+=($!)

REPO_ROOT="$ROOT_DIR" SANDBOX_RUNNER_ADDR="$SBX" SANDBOX_RUNNER_MODE=local \
  SUBMISSION_ARTIFACT_ROOT="$STATE_DIR/submissions" \
  "$BIN/sandbox-runner" & PIDS+=($!)

REPO_ROOT="$ROOT_DIR" LEADERBOARD_API_ADDR="$LB" LEADERBOARD_STORE_PATH="$LEADERBOARD_STORE" \
  "$BIN/leaderboard-api" & PIDS+=($!)

REPO_ROOT="$ROOT_DIR" ORCHESTRATOR_ADDR="$ORCH" ORCHESTRATOR_AUTO_START=false \
  ORCHESTRATOR_STORE_PATH="$SUBMISSION_INDEX" SANDBOX_RUNNER_URL="$SBX_URL" LEADERBOARD_URL="$LB_URL" \
  "$BIN/orchestrator" & PIDS+=($!)

wait_health "$SUB_URL" submission-api
wait_health "$SBX_URL" sandbox-runner
wait_health "$LB_URL" leaderboard-api
wait_health "$ORCH_URL" orchestrator

echo "deploy: backends up; starting console-api on 0.0.0.0:$PORT"
# Public gateway in the foreground (PID 1's child) so the container lifecycle
# tracks it. CONSOLE_API_ADDR override binds public; sameOrigin allows the
# deployed same-origin browser requests.
REPO_ROOT="$ROOT_DIR" CONSOLE_API_ADDR="0.0.0.0:$PORT" \
  SUBMISSION_API_URL="$SUB_URL" ORCHESTRATOR_URL="$ORCH_URL" LEADERBOARD_URL="$LB_URL" \
  exec "$BIN/console-api"
