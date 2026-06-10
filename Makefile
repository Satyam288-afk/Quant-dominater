.PHONY: proto-go test-go test-rust bot-fleet validator stub-engine rust-engine validate-fixture control-panel submission-api sandbox-runner orchestrator score-engine leaderboard-api console-api platform-demo console-stack reset-demo-state

PROTOC_GEN_GO ?= $(shell go env GOPATH)/bin/protoc-gen-go

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
	cargo run -p bot-fleet --bin bot-fleet -- --target ws://localhost:8080/ws --bots 100 --orders-per-sec 5 --duration-sec 60 --seed 42

validator:
	cargo run -p validator -- --events events.jsonl --contestant-outputs contestant_outputs.jsonl

validate-fixture:
	cargo run -p validator -- --events fixtures/events.valid.jsonl --contestant-outputs fixtures/contestant_outputs.valid.jsonl

stub-engine:
	cd examples/stub-engine && go run . --addr :8080 --events engine-events.jsonl

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
