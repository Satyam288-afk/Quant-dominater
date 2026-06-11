# Kubernetes IaC

The base kustomization deploys the shared cloud data plane and live leaderboard
read path:

- Redpanda telemetry bus
- TimescaleDB metrics store
- Redis live leaderboard store
- leaderboard-api
- telemetry-ingester
- sandbox namespace and network-policy boundary

It intentionally does **not** deploy upload-driven sandbox orchestration. The
current Go sandbox-runner implements local and Docker runners only; a production
Kubernetes runner still needs durable artifact storage plus image build/push and
Pod/Service lifecycle code. The disabled control-plane manifests remain in this
directory as templates, not as a claimed working runtime.

## Render

```bash
kubectl kustomize infra/k8s
```

## Strict Validation

```bash
kubectl kustomize infra/k8s | kubeconform -strict -summary -kubernetes-version 1.30.0
kubeconform -strict -summary -kubernetes-version 1.30.0 \
  infra/k8s/20-submission-api.yaml \
  infra/k8s/21-sandbox-runner.yaml \
  infra/k8s/22-orchestrator.yaml \
  infra/k8s/31-bot-fleet-job.yaml \
  infra/k8s/40-sandbox-pod-template.yaml
```

## Active Base Resources

| File | Resources | Role |
|---|---|---|
| `00-namespaces.yaml` | `iicpc`, `iicpc-sandbox` | trust boundary; sandbox namespace is PSS `restricted` |
| `01-config.yaml` | ConfigMap + Secret | service discovery + demo DB credentials |
| `02-rbac.yaml` | SA + Role + RoleBinding | scoped RBAC for future sandbox orchestration |
| `10-redpanda.yaml` | StatefulSet + Svc + init Job | telemetry bus, Kafka API, 4-partition topic |
| `11-timescaledb.yaml` | StatefulSet + Svc | metrics and score storage for the live data path |
| `12-redis.yaml` | Deployment + Svc | live leaderboard state |
| `23-leaderboard-api.yaml` | Deployment + Svc + LoadBalancer + HPA | externally exposed live API |
| `30-telemetry-ingester.yaml` | Deployment + HPA | Kafka consumer group, scales to partition count |
| `41-network-policies.yaml` | NetworkPolicy | default-deny sandbox namespace and bot-fleet ingress template |

## Disabled Templates

| File | Why disabled |
|---|---|
| `20-submission-api.yaml` | Uses pod-local artifact storage; production needs S3/MinIO and a DB-backed control-plane store. |
| `21-sandbox-runner.yaml` | The binary does not implement `kubernetes` mode yet; scaling this above zero would not provide a cloud sandbox runtime. |
| `22-orchestrator.yaml` | Current executor runs local bot-fleet/validator binaries; production needs a Kubernetes bot-fleet executor. |
| `31-bot-fleet-job.yaml` | Per-run template; should be created by the future Kubernetes executor. |
| `40-sandbox-pod-template.yaml` | Per-run contestant sandbox template; should be created by the future Kubernetes runner. |

## Production Hardening Needed

- Use managed Redpanda/MSK, managed Postgres/Timescale, and managed Redis or
  operator-managed clusters for production.
- Add S3/MinIO artifact storage and a Postgres/Timescale control-plane store.
- Implement Kubernetes runner code that builds/pushes contestant images and
  creates per-run Pod/Service resources through Kubernetes APIs.
- Configure sandbox node groups with kubelet static CPU manager policy before
  claiming exclusive CPU pinning.
- Retag service images centrally via `kustomize edit set image` after Terraform
  creates the registry.
