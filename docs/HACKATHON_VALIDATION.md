# Hackathon Validation Checklist

Use this as the final submission evidence page. It focuses on what the IICPC
PDF asks for: a working infrastructure prototype, architecture blueprint, and
IaC proof that the platform can be spun up and scaled horizontally.

## 1. End-To-End Prototype

```bash
./scripts/run-platform-demo.sh
```

Expected evidence:

- submission ZIP uploaded
- sandbox started
- bot fleet sends traffic
- validator checks fills against the reference orderbook
- score is published to the leaderboard
- artifacts are written under `.runs/{run_id}/`

Recent local result:

```text
status=FINISHED
valid=true
score=100
orders_sent=110
acks_received=110
timeouts=0
p99_ms≈1.8-2.8
artifacts=17
```

## 2. Browser Upload Console

```bash
./scripts/run-console-stack.sh
```

Open:

```text
http://127.0.0.1:9700/
```

Upload the generated example artifact:

```text
.runs/console-stack/stub-engine.zip
```

This demonstrates the interactive flow:

```text
upload ZIP -> create submission -> start run -> sandbox -> benchmark -> validate -> score -> artifacts
```

## 3. Kubernetes Horizontal-Scale Proof

Prerequisites:

```bash
docker version
kind version
kubectl version --client
```

Run:

```bash
make kind-scale-proof
```

The script:

1. creates or reuses a `kind` cluster,
2. builds and loads the real `stub-engine` and `bot-fleet` images,
3. deploys the contestant engine as a Kubernetes `Deployment` + `Service`,
4. runs the same Indexed Job + `--pod-index` sharding pattern shipped in
   `infra/k8s/31-bot-fleet-job.yaml`,
5. sweeps 2, 4, and 8 bot-fleet pods across a 4-node cluster,
6. prints aggregate throughput, drops, nodes used, and disjoint bot-id ranges.

Expected evidence:

```text
2 pods -> aggregate_orders≈40k   timeouts=0   nodes_used>=1
4 pods -> aggregate_orders≈80k   timeouts=0   nodes_used>=2
8 pods -> aggregate_orders≈160k  timeouts=0   nodes_used>=3
pod_index=0..7 -> disjoint global bot ranges
```

Submission wording:

> Kubernetes proof demonstrates the horizontal load-generation pattern: an
> Indexed Job fans bot-fleet pods across nodes, pod-index sharding prevents
> duplicate bot IDs, and aggregate throughput scales linearly with zero drops.
> The full 10k-bot ceiling and multi-node ingester throughput remain documented
> residuals for a larger production cluster.

## 4. Static Validation

```bash
make test-go
make test-rust
cd web && npm ci && npm audit --omit=dev && npm run build
bash -n scripts/*.sh
kubectl kustomize infra/k8s
make k8s-validate
make tf-validate
```

If `terraform`/`tofu` or `kubeconform` is missing locally, run those commands in
CI or on a machine with the tools installed and paste the output here.

## Final Positioning

Accurate claim:

> Production-oriented distributed benchmarking prototype with verified
> local/Docker upload-to-score execution, real validation/scoring, live
> leaderboard, and Kubernetes/Terraform IaC for the shared data-plane pattern.

Do not claim:

> Fully production-ready multi-tenant cloud service.
