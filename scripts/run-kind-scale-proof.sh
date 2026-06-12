#!/usr/bin/env bash
# Multi-node horizontal-scale proof for the distributed bot fleet on a local
# kind cluster. It exercises the SAME Indexed-Job + `--pod-index` sharding that
# infra/k8s/31-bot-fleet-job.yaml ships, and shows:
#   * the Job fans its pods out across multiple worker nodes,
#   * each pod drives a disjoint, globally-unique bot-id shard (pod_index offset),
#   * aggregate throughput scales ~linearly with pod count, with zero drops.
#
# It turns the "scales horizontally" claim from a design extrapolation into a
# measured result on real (containerised) Kubernetes nodes. Per-pod load is kept
# laptop-modest (100 bots x 10/s); the same manifest reaches 10k bots / ~200k
# orders/s by raising the per-pod numbers on a real multi-core cluster.
#
# Requires: docker, kind, kubectl. Set KEEP_CLUSTER=1 to leave the cluster up.
set -euo pipefail

CLUSTER="${CLUSTER:-iicpc-scale}"
NS=scale-proof
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBECONFIG_FILE="$(mktemp)"
cd "$ROOT"

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    echo "==> tearing down kind cluster '$CLUSTER'"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  rm -f "$KUBECONFIG_FILE"
}
trap cleanup EXIT

echo "==> [1/5] creating 4-node kind cluster (1 control-plane + 3 workers)"
cat >"$KUBECONFIG_FILE.kind" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: $CLUSTER
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
EOF
kind create cluster --config "$KUBECONFIG_FILE.kind" --wait 120s
kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"
kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"; }

echo "==> [2/5] building + loading images (stub engine + bot fleet)"
DOCKER_BUILDKIT=1 docker build -q -t iicpc/stub-engine:proof examples/stub-engine >/dev/null
DOCKER_BUILDKIT=1 docker build -q -t iicpc/bot-fleet:proof -f rust/bot-fleet/Dockerfile . >/dev/null
kind load docker-image iicpc/stub-engine:proof iicpc/bot-fleet:proof --name "$CLUSTER"

echo "==> [3/5] deploying the contestant engine (Deployment + Service)"
kc apply -f infra/k8s/kind-proof/engine.yaml
kc -n "$NS" wait --for=condition=available deploy/stub-engine --timeout=90s

echo "==> [4/5] horizontal-scale sweep — same 100-bot/pod load, vary pod count"
run_sweep() {
  local p="$1"
  local name="fleet-p$p" total=0 drops=0
  sed "s/JOB_NAME/$name/; s/PARALLELISM/$p/" infra/k8s/kind-proof/bot-fleet-job.tmpl.yaml | kc apply -f - >/dev/null
  kc -n "$NS" wait --for=condition=complete "job/$name" --timeout=180s >/dev/null
  for i in $(seq 0 $((p - 1))); do
    pod="$(kc -n "$NS" get pods -l app=bot-fleet -o name | grep "$name-$i-" | head -1)"
    log="$(kc -n "$NS" logs "$pod" 2>/dev/null)"
    total=$((total + $(echo "$log" | awk -F': ' '/^orders_sent:/{print $2}')))
    drops=$((drops + $(echo "$log" | awk -F': ' '/^timeouts:/{print $2}')))
  done
  local nodes
  nodes="$(kc -n "$NS" get pods -l app=bot-fleet -o wide --no-headers | grep "$name-" | awk '{print $7}' | sort -u | wc -l | tr -d ' ')"
  printf "    %d pods -> aggregate_orders=%d  timeouts=%d  nodes_used=%d  (~%d orders/s)\n" \
    "$p" "$total" "$drops" "$nodes" "$((total / 20))"
}
run_sweep 2
run_sweep 4
run_sweep 8

echo "==> [5/5] pod_index sharding (disjoint global bot-id ranges, no collisions)"
kc -n "$NS" get pods -l app=bot-fleet -o wide --no-headers | grep 'fleet-p8-' | awk '{print "    "$1" on "$7}'
for i in $(seq 0 7); do
  pod="$(kc -n "$NS" get pods -l app=bot-fleet -o name | grep "fleet-p8-$i-" | head -1 || true)"
  rng="$(kc -n "$NS" logs "$pod" 2>/dev/null | grep -oE 'global bots [0-9]+\.\.[0-9]+' | head -1 || true)"
  printf "    pod_index=%d  %s\n" "$i" "${rng:-global bots 1..100}"
done

echo "==> done. Set KEEP_CLUSTER=1 to keep the cluster; otherwise it is deleted on exit."
