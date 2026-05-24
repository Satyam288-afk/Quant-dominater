.PHONY: test-rust bot-fleet validator stub-engine validate-fixture control-panel submission-api sandbox-runner orchestrator

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

control-panel:
	cd services/control-panel && REPO_ROOT=$(CURDIR) go run .

submission-api:
	cd services/submission-api && REPO_ROOT=$(CURDIR) go run .

sandbox-runner:
	cd services/sandbox-runner && REPO_ROOT=$(CURDIR) go run .

orchestrator:
	cd services/orchestrator && REPO_ROOT=$(CURDIR) go run .
