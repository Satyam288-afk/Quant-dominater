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

## 3. Kubernetes Data-Plane Proof

Prerequisites:

```bash
docker version
kind version
kubectl version --client
```

Run:

```bash
./scripts/validate-kind-data-plane.sh
```

The script:

1. creates or reuses a `kind` cluster,
2. builds the real `leaderboard-api` and `telemetry-ingester` images,
3. loads those images into the cluster,
4. applies `infra/k8s`,
5. waits for Redpanda, TimescaleDB, Redis, leaderboard-api, telemetry-ingester,
   and the Redpanda topic-init job,
6. writes evidence under `.runs/kind-validation/`.

Evidence files:

```text
.runs/kind-validation/nodes.txt
.runs/kind-validation/pods-all.txt
.runs/kind-validation/iicpc-resources.txt
.runs/kind-validation/iicpc-events.txt
.runs/kind-validation/redpanda-topic-init.log
```

Submission wording:

> Kubernetes proof covers the shared data plane and live leaderboard read path.
> Upload-driven Kubernetes sandbox orchestration is deliberately documented as
> future production work because the current verified sandbox runners are local
> and Docker based.

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
