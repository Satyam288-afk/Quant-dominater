# Rust Engine

Contestant-style Rust WebSocket engine stub.

```bash
cargo run -p rust-engine -- --addr :8080 --events engine-events.jsonl
```

It implements:

```text
GET /health
WS  /ws
```

The engine uses deterministic price-time priority and emits `ack` and `fill`
messages matching the benchmark JSON contract.
