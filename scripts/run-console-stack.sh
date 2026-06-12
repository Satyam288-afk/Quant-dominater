#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_DIR="$ROOT_DIR/.runs/console-stack"

SUBMISSION_ADDR="${SUBMISSION_API_ADDR:-:9110}"
SANDBOX_ADDR="${SANDBOX_RUNNER_ADDR:-:9210}"
ORCH_ADDR="${ORCHESTRATOR_ADDR:-:9310}"
LEADERBOARD_ADDR="${LEADERBOARD_API_ADDR:-:9510}"
CONSOLE_ADDR="${CONSOLE_API_ADDR:-:9700}"
SANDBOX_MODE="${SANDBOX_RUNNER_MODE:-docker}"
RUN_TIMEOUT="${ORCHESTRATOR_RUN_TIMEOUT:-10m}"

addr_port() {
  local addr="$1"
  local port="${addr##*:}"
  if [[ "$port" == "$addr" ]]; then
    echo "$addr"
  else
    echo "$port"
  fi
}

SUBMISSION_URL="http://127.0.0.1:$(addr_port "$SUBMISSION_ADDR")"
SANDBOX_URL="http://127.0.0.1:$(addr_port "$SANDBOX_ADDR")"
ORCH_URL="http://127.0.0.1:$(addr_port "$ORCH_ADDR")"
LEADERBOARD_URL="http://127.0.0.1:$(addr_port "$LEADERBOARD_ADDR")"
CONSOLE_URL="http://127.0.0.1:$(addr_port "$CONSOLE_ADDR")"

SUBMISSION_ROOT="$STACK_DIR/submissions"
SUBMISSION_INDEX="$SUBMISSION_ROOT/index.json"
LEADERBOARD_STORE="$STACK_DIR/leaderboard.json"

mkdir -p "$STACK_DIR"
rm -f "$STACK_DIR"/*.log "$STACK_DIR"/stub-engine.zip
rm -rf "$SUBMISSION_ROOT"
rm -f "$LEADERBOARD_STORE"

PIDS=()
cleanup() {
  if (( ${#PIDS[@]} == 0 )); then
    return 0
  fi
  for pid in "${PIDS[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${PIDS[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

require_port_free() {
  local name="$1"
  local addr="$2"
  local port
  port="$(addr_port "$addr")"
  if [[ -z "$port" ]] || ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "$name port $port is already in use; override the matching *_ADDR variable or stop the existing service" >&2
    return 1
  fi
}

wait_health() {
  local name="$1"
  local url="$2"
  for _ in {1..120}; do
    if curl -fsS "$url/health" >/dev/null 2>&1; then
      echo "$name healthy"
      return 0
    fi
    sleep 0.2
  done
  echo "$name did not become healthy at $url" >&2
  return 1
}

start_service() {
  local name="$1"
  local dir="$2"
  local log="$STACK_DIR/$name.log"
  shift 2
  echo "starting $name"
  (
    cd "$ROOT_DIR/$dir"
    REPO_ROOT="$ROOT_DIR" "$@" >"$log" 2>&1
  ) &
  PIDS+=("$!")
}

require_sandbox_mode() {
  case "$SANDBOX_MODE" in
    docker)
      if ! command -v docker >/dev/null 2>&1; then
        echo "SANDBOX_RUNNER_MODE=docker requires the docker CLI; install Docker or run with SANDBOX_RUNNER_MODE=local" >&2
        return 1
      fi
      if ! docker info >/dev/null 2>&1; then
        echo "SANDBOX_RUNNER_MODE=docker requires a running Docker daemon; start Docker or run with SANDBOX_RUNNER_MODE=local" >&2
        return 1
      fi
      ;;
    local)
      echo "using SANDBOX_RUNNER_MODE=local (Go-only dev path; Docker mode is the containerized prototype path)"
      ;;
    *)
      echo "unsupported SANDBOX_RUNNER_MODE=$SANDBOX_MODE; expected docker or local" >&2
      return 1
      ;;
  esac
}

require_port_free submission-api "$SUBMISSION_ADDR"
require_port_free sandbox-runner "$SANDBOX_ADDR"
require_port_free orchestrator "$ORCH_ADDR"
require_port_free leaderboard-api "$LEADERBOARD_ADDR"
require_port_free console-api "$CONSOLE_ADDR"
require_sandbox_mode

echo "packaging example upload artifact"
(
  cd "$ROOT_DIR/examples/stub-engine"
  # Omit the sample Dockerfile so the console upload exercises the sandbox
  # runner's default Go packaging policy. Contestant-supplied Dockerfiles are
  # still supported, but they intentionally build with network disabled.
  zip -qr "$STACK_DIR/stub-engine.zip" . -x Dockerfile ./Dockerfile
)

start_service submission-api services/submission-api env SUBMISSION_API_ADDR="$SUBMISSION_ADDR" SUBMISSION_ARTIFACT_ROOT="$SUBMISSION_ROOT" SUBMISSION_INDEX_PATH="$SUBMISSION_INDEX" go run .
start_service sandbox-runner services/sandbox-runner env SANDBOX_RUNNER_ADDR="$SANDBOX_ADDR" SANDBOX_RUNNER_MODE="$SANDBOX_MODE" SUBMISSION_ARTIFACT_ROOT="$SUBMISSION_ROOT" go run .
start_service leaderboard-api services/leaderboard-api env LEADERBOARD_API_ADDR="$LEADERBOARD_ADDR" LEADERBOARD_STORE_PATH="$LEADERBOARD_STORE" go run .
start_service orchestrator services/orchestrator env ORCHESTRATOR_ADDR="$ORCH_ADDR" ORCHESTRATOR_AUTO_START=false ORCHESTRATOR_RUN_TIMEOUT="$RUN_TIMEOUT" ORCHESTRATOR_STORE_PATH="$SUBMISSION_INDEX" SANDBOX_RUNNER_URL="$SANDBOX_URL" LEADERBOARD_URL="$LEADERBOARD_URL" go run .
start_service console-api services/console-api env CONSOLE_API_ADDR="$CONSOLE_ADDR" SUBMISSION_API_URL="$SUBMISSION_URL" ORCHESTRATOR_URL="$ORCH_URL" LEADERBOARD_URL="$LEADERBOARD_URL" go run .

wait_health submission-api "$SUBMISSION_URL"
wait_health sandbox-runner "$SANDBOX_URL"
wait_health leaderboard-api "$LEADERBOARD_URL"
wait_health orchestrator "$ORCH_URL"
wait_health console-api "$CONSOLE_URL"

echo
echo "console UI: $CONSOLE_URL/"
echo "example ZIP: $STACK_DIR/stub-engine.zip"
echo "sandbox mode: $SANDBOX_MODE"
echo "run timeout: $RUN_TIMEOUT"
echo "logs: $STACK_DIR/*.log"
echo "press Ctrl+C to stop services"

while true; do
  sleep 3600
done
