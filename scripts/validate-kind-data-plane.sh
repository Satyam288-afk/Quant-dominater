#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-iicpc-benchmark}"
OUT_DIR="${KIND_VALIDATION_OUT:-$ROOT_DIR/.runs/kind-validation}"

mkdir -p "$OUT_DIR"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    return 1
  fi
}

require docker
require kind
require kubectl

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not running" >&2
  exit 1
fi

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME"
fi

docker build \
  -f "$ROOT_DIR/infra/dockerfiles/leaderboard-api.Dockerfile" \
  -t ghcr.io/iicpc/leaderboard-api:latest \
  "$ROOT_DIR"

docker build \
  -f "$ROOT_DIR/infra/dockerfiles/telemetry-ingester.Dockerfile" \
  -t ghcr.io/iicpc/telemetry-ingester:latest \
  "$ROOT_DIR"

kind load docker-image --name "$CLUSTER_NAME" ghcr.io/iicpc/leaderboard-api:latest
kind load docker-image --name "$CLUSTER_NAME" ghcr.io/iicpc/telemetry-ingester:latest

kubectl apply -k "$ROOT_DIR/infra/k8s"

kubectl -n iicpc rollout status statefulset/redpanda --timeout=180s
kubectl -n iicpc rollout status statefulset/timescaledb --timeout=240s
kubectl -n iicpc rollout status deployment/redis --timeout=120s
kubectl -n iicpc rollout status deployment/leaderboard-api --timeout=120s
kubectl -n iicpc rollout status deployment/telemetry-ingester --timeout=180s
kubectl -n iicpc wait --for=condition=complete job/redpanda-topic-init --timeout=180s

kubectl get nodes -o wide > "$OUT_DIR/nodes.txt"
kubectl get pods -A -o wide > "$OUT_DIR/pods-all.txt"
kubectl -n iicpc get pods,svc,hpa,pvc,jobs -o wide > "$OUT_DIR/iicpc-resources.txt"
kubectl -n iicpc get events --sort-by=.lastTimestamp > "$OUT_DIR/iicpc-events.txt"
kubectl -n iicpc logs job/redpanda-topic-init > "$OUT_DIR/redpanda-topic-init.log" 2>&1 || true

echo "kind validation evidence written to $OUT_DIR"
cat "$OUT_DIR/iicpc-resources.txt"
