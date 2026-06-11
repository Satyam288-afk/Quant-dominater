#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO_DIR="$ROOT_DIR/.runs/platform-demo"
DEMO_ID="${DEMO_ID:-$$}"
DEMO_SUBMISSION_ROOT="$DEMO_DIR/submissions"
DEMO_SUBMISSION_INDEX="$DEMO_SUBMISSION_ROOT/index.json"

addr_port() {
  local addr="$1"
  local port="${addr##*:}"
  if [[ "$port" == "$addr" ]]; then
    echo "$addr"
  else
    echo "$port"
  fi
}

SUBMISSION_ADDR="${SUBMISSION_API_ADDR:-:9100}"
SANDBOX_ADDR="${SANDBOX_RUNNER_ADDR:-:9200}"
ORCH_ADDR="${ORCHESTRATOR_ADDR:-:9300}"
LEADERBOARD_ADDR="${LEADERBOARD_API_ADDR:-:9500}"
SUBMISSION_URL="http://127.0.0.1:$(addr_port "$SUBMISSION_ADDR")"
SANDBOX_URL="http://127.0.0.1:$(addr_port "$SANDBOX_ADDR")"
ORCH_URL="http://127.0.0.1:$(addr_port "$ORCH_ADDR")"
LEADERBOARD_URL="http://127.0.0.1:$(addr_port "$LEADERBOARD_ADDR")"
DEMO_LEADERBOARD_STORE="${LEADERBOARD_STORE_PATH:-$DEMO_DIR/leaderboard-store-$DEMO_ID.json}"
SUBMISSION_AUTH_TOKEN="${SUBMISSION_API_AUTH_TOKEN:-${SERVICE_AUTH_TOKEN:-}}"
ORCHESTRATOR_AUTH_TOKEN="${ORCHESTRATOR_AUTH_TOKEN:-${SERVICE_AUTH_TOKEN:-}}"
LEADERBOARD_AUTH_TOKEN="${LEADERBOARD_AUTH_TOKEN:-${SERVICE_AUTH_TOKEN:-}}"

auth_curl() {
  local token="$1"
  shift
  if [[ -n "$token" ]]; then
    curl -fsS -H "Authorization: Bearer $token" "$@"
  else
    curl -fsS "$@"
  fi
}

submission_curl() {
  auth_curl "$SUBMISSION_AUTH_TOKEN" "$@"
}

orchestrator_curl() {
  auth_curl "$ORCHESTRATOR_AUTH_TOKEN" "$@"
}

leaderboard_curl() {
  auth_curl "$LEADERBOARD_AUTH_TOKEN" "$@"
}

mkdir -p "$DEMO_DIR"
rm -f "$DEMO_DIR"/*.log "$DEMO_DIR"/stub-engine.zip "$DEMO_DIR"/submission.json "$DEMO_DIR"/run-created.json "$DEMO_DIR"/run-final.json "$DEMO_DIR"/leaderboard.json "$DEMO_DIR"/leaderboard-store*.json "$DEMO_DIR"/artifacts.json
rm -rf "$DEMO_SUBMISSION_ROOT"

PIDS=()
cleanup() {
  if [[ "${KEEP_SERVICES:-0}" == "1" ]]; then
    echo "services left running:"
    echo "  submission-api  $SUBMISSION_URL"
    echo "  sandbox-runner   $SANDBOX_URL"
    echo "  orchestrator     $ORCH_URL"
    echo "  leaderboard      $LEADERBOARD_URL"
    return 0
  fi
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
    echo "$name port $port is already in use; stop the existing service or override the *_ADDR variables" >&2
    return 1
  fi
}

json_get() {
  python3 - "$1" "$2" <<'PY'
import json, sys
path, key = sys.argv[1], sys.argv[2]
with open(path) as f:
    obj = json.load(f)
cur = obj
for part in key.split("."):
    cur = cur[part]
print(cur)
PY
}

wait_health() {
  local name="$1"
  local url="$2"
  for _ in {1..100}; do
    if curl -fsS "$url/health" >/dev/null 2>&1; then
      echo "$name healthy"
      return 0
    fi
    sleep 0.1
  done
  echo "$name did not become healthy at $url" >&2
  return 1
}

wait_leaderboard_entry() {
  local run_id="$1"
  for _ in {1..50}; do
    leaderboard_curl "$LEADERBOARD_URL/leaderboard" > "$DEMO_DIR/leaderboard.json"
    if python3 - "$DEMO_DIR/leaderboard.json" "$run_id" <<'PY'
import json, sys
path, run_id = sys.argv[1], sys.argv[2]
entries = json.load(open(path))
if any(entry.get("run_id") == run_id for entry in entries):
    sys.exit(0)
sys.exit(1)
PY
    then
      return 0
    fi
    sleep 0.2
  done
  echo "leaderboard did not publish run $run_id" >&2
  return 1
}

start_service() {
  local name="$1"
  local dir="$2"
  local log="$DEMO_DIR/$name.log"
  shift 2
  echo "starting $name"
  (
    cd "$ROOT_DIR/$dir"
    REPO_ROOT="$ROOT_DIR" "$@" >"$log" 2>&1
  ) &
  PIDS+=("$!")
}

echo "[1/7] packaging example engine"
require_port_free submission-api "$SUBMISSION_ADDR"
require_port_free sandbox-runner "$SANDBOX_ADDR"
require_port_free orchestrator "$ORCH_ADDR"
require_port_free leaderboard-api "$LEADERBOARD_ADDR"
(
  cd "$ROOT_DIR/examples/stub-engine"
  zip -qr "$DEMO_DIR/stub-engine.zip" .
)

echo "[2/7] starting services"
start_service submission-api services/submission-api env SUBMISSION_API_ADDR="$SUBMISSION_ADDR" SUBMISSION_ARTIFACT_ROOT="$DEMO_SUBMISSION_ROOT" SUBMISSION_INDEX_PATH="$DEMO_SUBMISSION_INDEX" go run .
start_service sandbox-runner services/sandbox-runner env SANDBOX_RUNNER_ADDR="$SANDBOX_ADDR" SANDBOX_RUNNER_MODE=local SUBMISSION_ARTIFACT_ROOT="$DEMO_SUBMISSION_ROOT" go run .
start_service leaderboard-api services/leaderboard-api env LEADERBOARD_API_ADDR="$LEADERBOARD_ADDR" LEADERBOARD_STORE_PATH="$DEMO_LEADERBOARD_STORE" go run .
start_service orchestrator services/orchestrator env ORCHESTRATOR_ADDR="$ORCH_ADDR" ORCHESTRATOR_AUTO_START=false ORCHESTRATOR_STORE_PATH="$DEMO_SUBMISSION_INDEX" SANDBOX_RUNNER_URL="$SANDBOX_URL" LEADERBOARD_URL="$LEADERBOARD_URL" go run .

wait_health submission-api "$SUBMISSION_URL"
wait_health sandbox-runner "$SANDBOX_URL"
wait_health leaderboard-api "$LEADERBOARD_URL"
wait_health orchestrator "$ORCH_URL"

echo "[3/7] submitting engine artifact"
submission_curl -X POST "$SUBMISSION_URL/submissions" \
  -F team_id=demo_team \
  -F language=go \
  -F protocol=ws-json \
  -F "artifact=@$DEMO_DIR/stub-engine.zip;type=application/zip" \
  > "$DEMO_DIR/submission.json"
SUBMISSION_ID="$(json_get "$DEMO_DIR/submission.json" submission_id)"
echo "submission_id=$SUBMISSION_ID"

echo "[4/7] creating benchmark run"
submission_curl -X POST "$SUBMISSION_URL/submissions/$SUBMISSION_ID/runs" \
  -H "Content-Type: application/json" \
  -d '{"benchmark_seed":42,"sandbox":{"cpu_limit":"1","memory_limit":"512Mi","network_egress":false},"config":{"bot_count":10,"rate_per_bot":2,"duration_sec":5,"warmup_sec":0}}' \
  > "$DEMO_DIR/run-created.json"
RUN_ID="$(json_get "$DEMO_DIR/run-created.json" run_id)"
echo "run_id=$RUN_ID"
orchestrator_curl -X POST "$ORCH_URL/runs/$RUN_ID/start" >/dev/null

echo "[5/7] waiting for orchestrator to finish"
for _ in {1..120}; do
  orchestrator_curl "$ORCH_URL/runs/$RUN_ID" > "$DEMO_DIR/run-final.json"
  STATUS="$(json_get "$DEMO_DIR/run-final.json" status)"
  echo "status=$STATUS"
  case "$STATUS" in
    FINISHED|FAILED|CANCELLED|TIMED_OUT)
      break
      ;;
  esac
  sleep 1
done

STATUS="$(json_get "$DEMO_DIR/run-final.json" status)"
if [[ "$STATUS" != "FINISHED" ]]; then
  echo "run did not finish successfully; see $DEMO_DIR/*.log" >&2
  cat "$DEMO_DIR/run-final.json"
  exit 1
fi

echo "[6/7] fetching leaderboard and artifacts"
wait_leaderboard_entry "$RUN_ID"
ARTIFACT_DIR="$(json_get "$DEMO_DIR/run-final.json" artifact_dir)"
python3 - "$ARTIFACT_DIR" > "$DEMO_DIR/artifacts.json" <<'PY'
import json, os, sys
root = sys.argv[1]
items = []
for name in sorted(os.listdir(root)):
    path = os.path.join(root, name)
    if os.path.isfile(path):
        items.append({"name": name, "size_bytes": os.path.getsize(path), "path": path})
print(json.dumps(items, indent=2))
PY

echo "[7/7] result"
python3 - "$DEMO_DIR/run-final.json" "$DEMO_DIR/leaderboard.json" "$DEMO_DIR/artifacts.json" <<'PY'
import json, sys
from pathlib import Path
run = json.load(open(sys.argv[1]))
leaderboard = json.load(open(sys.argv[2]))
artifacts = json.load(open(sys.argv[3]))
artifact_dir = Path(run["artifact_dir"])
metrics = json.load(open(artifact_dir / "metrics.json"))
validation = json.load(open(artifact_dir / "validation.json"))
score = json.load(open(artifact_dir / "score.json"))
summary = {
    "run_id": run["run_id"],
    "status": run["status"],
    "valid": run.get("valid"),
    "score": run.get("score"),
    "artifact_dir": run.get("artifact_dir"),
    "metrics": {
        "bots": metrics.get("bots"),
        "orders_sent": metrics.get("orders_sent"),
        "acks_received": metrics.get("acks_received"),
        "fills_received": metrics.get("fills_received"),
        "timeouts": metrics.get("timeouts"),
        "tps": metrics.get("tps"),
        "p50_ms": metrics.get("p50_ms"),
        "p90_ms": metrics.get("p90_ms"),
        "p99_ms": metrics.get("p99_ms"),
    },
    "validation": {
        "valid": validation.get("valid"),
        "reason": validation.get("reason"),
        "fills_checked": validation.get("fills_checked"),
    },
    "score_detail": score,
    "leaderboard_entries": len(leaderboard),
    "artifacts": [a["name"] for a in artifacts],
}
print(json.dumps(summary, indent=2))
PY

echo "platform demo files: $DEMO_DIR"
if [[ "${KEEP_SERVICES:-0}" == "1" ]]; then
  echo "leaderboard UI: $LEADERBOARD_URL/"
fi
