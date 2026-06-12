.PHONY: proto-go test-go test-rust bot-fleet validator stub-engine rust-engine validate-fixture control-panel submission-api sandbox-runner orchestrator score-engine leaderboard-api console-api web web-build k8s-validate tf-validate iac-validate live-demo chaos-demo platform-demo console-stack reset-demo-state images demo kind-scale-proof

PROTOC_GEN_GO ?= $(shell go env GOPATH)/bin/protoc-gen-go

# Container registry the k8s manifests pull from (see infra/k8s/2*.yaml,30,31).
# Override REGISTRY/IMAGE_TAG to push elsewhere, e.g.
#   make images REGISTRY=<acct>.dkr.ecr.<region>.amazonaws.com/iicpc IMAGE_TAG=v1
REGISTRY ?= ghcr.io/iicpc
IMAGE_TAG ?= latest

proto-go:
	mkdir -p shared/go
	protoc --plugin=protoc-gen-go=$(PROTOC_GEN_GO) \
		--go_out=shared/go \
		--go_opt=module=github.com/iicpc/benchmark-platform/shared/go \
		proto/benchmark.proto

test-go:
	cd shared/go && go test ./...
	cd examples/stub-engine && go test ./...
	cd services/control-panel && go test ./...
	cd services/submission-api && go test ./...
	cd services/sandbox-runner && go test ./...
	cd services/orchestrator && go test ./...
	cd services/score-engine && go test ./...
	cd services/leaderboard-api && go test ./...
	cd services/console-api && go test ./...

test-rust:
	cargo test --workspace

bot-fleet:
	cargo run --release -p bot-fleet --bin bot-fleet -- --target ws://localhost:8080/ws --bots 100 --orders-per-sec 5 --duration-sec 60 --seed 42

validator:
	cargo run -p validator -- --events events.jsonl --contestant-outputs contestant_outputs.jsonl

validate-fixture:
	cargo run -p validator -- --events fixtures/events.valid.jsonl --contestant-outputs fixtures/contestant_outputs.valid.jsonl

stub-engine:
	cd examples/stub-engine && go run . --addr :8080 --engine mutex --events engine-events.jsonl

rust-engine:
	cargo run -p rust-engine -- --addr :8080 --events engine-events.jsonl

control-panel:
	cd services/control-panel && REPO_ROOT=$(CURDIR) go run .

submission-api:
	cd services/submission-api && REPO_ROOT=$(CURDIR) go run .

sandbox-runner:
	cd services/sandbox-runner && REPO_ROOT=$(CURDIR) go run .

orchestrator:
	cd services/orchestrator && REPO_ROOT=$(CURDIR) go run .

score-engine:
	cd services/score-engine && REPO_ROOT=$(CURDIR) go run .

leaderboard-api:
	cd services/leaderboard-api && REPO_ROOT=$(CURDIR) go run .

# Frontend
web:
	cd web && npm install && npm run dev

web-build:
	cd web && npm install && npm run build

# Full live data-plane demo (needs Docker for redpanda/timescale/redis)
live-demo:
	./scripts/run-live-demo.sh

# Failure-injection demo (no Docker): engine crash → fleet reconnect, SIGTERM → graceful drain
chaos-demo:
	./scripts/run-chaos-demo.sh

# IaC validation (no cloud creds / cluster required)
k8s-validate:
	kubectl kustomize infra/k8s | kubeconform -strict -summary -kubernetes-version 1.30.0
	kubeconform -strict -summary -kubernetes-version 1.30.0 infra/k8s/40-sandbox-pod-template.yaml infra/k8s/31-bot-fleet-job.yaml

tf-validate:
	cd infra/terraform && (command -v tofu >/dev/null && tofu fmt -check -recursive && tofu init -backend=false -input=false >/dev/null && tofu validate) || \
		(terraform fmt -check -recursive && terraform init -backend=false -input=false >/dev/null && terraform validate)

iac-validate: k8s-validate tf-validate

# Build + tag every platform service image the k8s manifests pull. Each builds
# with the repo root as context (Rust crates resolve ../bench-core and the
# proto/ tree; submission-api/orchestrator resolve their ../../shared/go replace)
# and its own -f Dockerfile. Tags match infra/k8s exactly (REGISTRY/<svc>:TAG).
# The Rust images build --features kafka/live to match the manifest comments.
images:
	docker build -f services/submission-api/Dockerfile     -t $(REGISTRY)/submission-api:$(IMAGE_TAG)     .
	docker build -f services/sandbox-runner/Dockerfile      -t $(REGISTRY)/sandbox-runner:$(IMAGE_TAG)      .
	docker build -f services/orchestrator/Dockerfile        -t $(REGISTRY)/orchestrator:$(IMAGE_TAG)        .
	docker build -f services/leaderboard-api/Dockerfile     -t $(REGISTRY)/leaderboard-api:$(IMAGE_TAG)     .
	docker build -f services/score-engine/Dockerfile        -t $(REGISTRY)/score-engine:$(IMAGE_TAG)        .
	docker build -f services/console-api/Dockerfile         -t $(REGISTRY)/console-api:$(IMAGE_TAG)         .
	docker build -f services/control-panel/Dockerfile       -t $(REGISTRY)/control-panel:$(IMAGE_TAG)       .
	docker build -f rust/bot-fleet/Dockerfile               -t $(REGISTRY)/bot-fleet:$(IMAGE_TAG)           .
	docker build -f rust/telemetry-ingester/Dockerfile      -t $(REGISTRY)/telemetry-ingester:$(IMAGE_TAG)  .

# One-command platform walkthrough for the judges (maintained in scripts/).
demo:
	bash scripts/run-platform-demo.sh

# Multi-node horizontal-scale proof on a local kind cluster (requires kind).
kind-scale-proof:
	bash scripts/run-kind-scale-proof.sh

# Console (colleague's local benchmark console)
console-api:
	cd services/console-api && REPO_ROOT=$(CURDIR) go run .

platform-demo:
	./scripts/run-platform-demo.sh

console-stack:
	./scripts/run-console-stack.sh

reset-demo-state:
	rm -f .leaderboard/leaderboard.json
	rm -f .runs/platform-demo/leaderboard-store*.json
	rm -f .runs/platform-demo/*.json .runs/platform-demo/*.log .runs/platform-demo/*.zip
