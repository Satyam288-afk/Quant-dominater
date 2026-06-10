# Kubernetes — Benchmark Cell

A self-contained "benchmark cell": one `kubectl apply -k .` brings up the data
plane, control plane, load generator, and the network-fenced sandbox namespace.

```bash
kubectl apply -k infra/k8s
# or render/inspect first:
kubectl kustomize infra/k8s | less
```

Validate without a cluster:

```bash
kubectl kustomize infra/k8s | kubeconform -strict -summary -kubernetes-version 1.30.0
```

## Layout

| File | Resources | Role |
|---|---|---|
| `00-namespaces.yaml` | `iicpc`, `iicpc-sandbox` | trust boundary; sandbox ns is PSS `restricted` |
| `01-config.yaml` | ConfigMap + Secret | service discovery + DB creds |
| `02-rbac.yaml` | SA + Role + RoleBinding | sandbox-runner may manage pods **only** in `iicpc-sandbox` |
| `10-redpanda.yaml` | StatefulSet + Svc + init Job | telemetry bus (Kafka API), 4-partition topic |
| `11-timescaledb.yaml` | StatefulSet + Svc | durable metrics + scores (schema via ConfigMap) |
| `12-redis.yaml` | Deployment + Svc | live leaderboard state |
| `20..22` | Deployments + Svcs | submission-api, sandbox-runner, orchestrator |
| `23-leaderboard-api.yaml` | Deployment + Svc + **LB** + **HPA** | live WS API, externally exposed |
| `30-telemetry-ingester.yaml` | Deployment + **HPA** | Kafka consumer group, scales to partition count |
| `31-bot-fleet-job.yaml` | Job (template) | distributed load generator (per-run) |
| `40-sandbox-pod-template.yaml` | Pod + Svc (template) | contestant engine, full `securityContext` |
| `41-network-policies.yaml` | NetworkPolicy ×2 | default-deny + bot-fleet→engine only |

## How it scales horizontally

- **Load generator** — `31-bot-fleet-job.yaml` is an Indexed Job:
  `parallelism × BOTS_PER_POD` bots across nodes (default 8 × 1250 = **10,000**).
  Raise `parallelism` and node count for more.
- **Ingestion** — `telemetry-ingester` is a Kafka consumer group; its HPA scales
  replicas up to the topic partition count (4). More partitions → more throughput.
- **Live API** — `leaderboard-api` is stateless (reads Redis); HPA 2→10 on CPU.

## Per-run lifecycle (orchestrator-driven)

The orchestrator does not pre-create contestant pods. Per benchmark run it:

1. asks `sandbox-runner` to build + apply a `40-sandbox-pod-template` instance
   (`RUN_ID`/`REGISTRY` substituted) into `iicpc-sandbox`;
2. waits for `/health`, then applies a `31-bot-fleet-job` instance pointed at the
   engine Service;
3. the fleet streams telemetry → Redpanda → ingester → Timescale/Redis;
4. `score-engine` writes the scorecard; `leaderboard-api` streams it live.

`41-network-policies.yaml` guarantees a contestant pod can be reached **only** by
the bot fleet on `:8080` and has **no** egress (internet or cross-contestant).

## Production notes

- Data stores shown as single instances are dev-grade. In production use the
  Redpanda Operator, a managed Postgres/Timescale, and a Redis with replicas
  (or managed ElastiCache) — provisioned by `infra/terraform`.
- CPU pinning for sandboxes requires the kubelet `static` CPU manager policy on
  the sandbox node group (configured in Terraform) + Guaranteed QoS (already set).
- Retag images centrally via `kustomize edit set image` after Terraform creates
  the registry.
