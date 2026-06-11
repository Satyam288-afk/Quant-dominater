# Terraform — Cloud Provisioning (AWS EKS)

Provisions the cloud substrate the benchmark platform runs on:

- **VPC** across 3 AZs (public + private subnets, single NAT) with the subnet
  tags the AWS Load Balancer Controller and autoscaler expect.
- **EKS** cluster (`1.30`) with two managed node groups:
  - `platform` — general pool for control plane, data plane, bot fleet, ingester
    (scales 2→10).
  - `sandbox` — **tainted, isolated** pool for untrusted contestant engines, with
    the kubelet **static CPU manager** enabled for exclusive core pinning.
- **ECR** repositories (one per service) with scan-on-push + a keep-last-20
  lifecycle policy.

It works hand-in-glove with [`infra/k8s`](../k8s): Terraform builds the cluster +
registry, then `kubectl apply -k infra/k8s` deploys the benchmark cell onto it.

> Compatible with both Terraform (`terraform`) and OpenTofu (`tofu`).

## Usage

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # optional; defaults are fine

tofu init            # or: terraform init
tofu plan
tofu apply

# point kubectl at the cluster, then deploy the platform
$(tofu output -raw configure_kubectl)
kubectl apply -k ../k8s
```

## Validate without credentials

```bash
tofu fmt -check
tofu init -backend=false
tofu validate
```

## Push images

```bash
eval "$(tofu output -json ecr_repository_urls | jq -r 'to_entries[] | "echo \(.key)=\(.value)"')"
# docker build + push each service to its repo, then retag the manifests:
#   cd ../k8s && kustomize edit set image ghcr.io/iicpc/leaderboard-api=<repo-url>:v1
```

## Production hardening (next)

- Managed data stores instead of in-cluster dev instances: **MSK** (Kafka),
  **RDS/Aurora Postgres + Timescale** or Timescale Cloud, **ElastiCache** (Redis).
- Private-only API endpoint + bastion/SSM; restrict `cluster_endpoint_public_access`.
- IRSA / Pod Identity for service → AWS access; secrets via Secrets Manager +
  External Secrets Operator.
- Remote state backend (S3 + DynamoDB lock) — see `versions.tf`.
