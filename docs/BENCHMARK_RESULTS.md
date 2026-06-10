# Measured Results

Real numbers from running the platform end-to-end, not projections. They show
the platform driving a high-velocity, mixed order flow and measuring the
latency / throughput / correctness of the engine under test.

## Method

- **Host:** single Apple-Silicon macOS laptop. **Build:** `--release` for the
  Rust fleet + validator; the engine under test is the repo's
  [`examples/stub-engine`](../examples/stub-engine) — a deliberately simple
  single-`sync.Mutex` Go matcher (the *contestant*, not our infrastructure).
- **Load:** one `bot-fleet` process multiplexing N virtual bots over a small WS
  connection pool, sending a realistic mix: limit orders across a 9-level price
  ladder, ~10 % market orders, ~12 % cancels, sizes 1..30, bots sharing symbols
  so they trade against each other.
- **Correctness:** every run replayed through the deterministic
  `reference-orderbook` in the engine's accepted arrival order (ack
  `engine_seq`) and diffed fill-for-fill.
- Reproduce with `target/release/bot-fleet … --market-per-mille 100 --cancel-per-mille 120`.

## Latency vs throughput (the "max TPS before failure" curve)

| Regime | Bots × rate | Orders (5 s) | Sustained TPS | Peak TPS¹ | p50 | p90 | p99 | Timeouts | Correctness² |
|---|---|---|---|---|---|---|---|---|---|
| **Healthy** | 200 × 20 | 20,200 | 4,040 | 4,000 | 6.8 ms | 10.8 ms | 12.4 ms | 0 | ✅ valid |
| **Saturation** | 500 × 100 | 250,467 | 50,093 | 43,406 | 443 ms | 904 ms | 1,080 ms | 0 | ✅ valid |

¹ Peak TPS = the busiest aligned 1-second window (max acks/s), distinct from the
average. ² Correctness validated over **191,173 fills** at the saturation point.

**What this shows.** The single-process Rust fleet pushed **250k orders in 5 s
(~50k orders/s)** and the engine acknowledged every one (zero timeouts), so the
*platform* is not the bottleneck. As the single-mutex engine saturates, latency
degrades ~80× (p99 12 ms → 1.08 s) while throughput plateaus — exactly the
"maximum TPS a submission sustains before its latency falls over" the brief asks
us to surface. Price-time-priority correctness held at both regimes, including
across all 191k saturation-point fills, with the full limit/market/cancel mix.

## Correctness under concurrency

The same load is replayed in the engine's authoritative arrival order, so a
*correct* engine validates clean even with thousands of bots racing across
shared symbols — and a deliberately broken one is caught. Proven repeatably by
[`scripts/run-price-time-proof.sh`](../scripts/run-price-time-proof.sh): normal
mode passes, `broken-price-time-priority` mode fails with
`PRICE_TIME_PRIORITY_VIOLATION`.

## Honest scope

These characterize the platform on one host against a toy engine; they are not a
production-exchange benchmark. The fleet scales horizontally as a Kubernetes
Indexed Job (`pod_index`-offset global IDs → 10k bots across 8 pods,
[`infra/k8s/31-bot-fleet-job.yaml`](../infra/k8s/31-bot-fleet-job.yaml)) and the
ingester scales with Kafka partitions, but those multi-node figures were not
measured here.
