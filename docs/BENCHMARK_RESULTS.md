# Measured Results

Real numbers from running the platform end-to-end, not projections. They show
the platform driving a high-velocity, mixed order flow and measuring the
latency / throughput / correctness of the engine under test.

> **History note.** An earlier revision of this table reported a saturation p99
> of **1.08 s**. That number was real but wrong about the *engine*: it was
> measured before the round-2 harness fixes (the fleet's telemetry sink and
> latency stamps sat inside the measured window — see
> [PROFILING.md](PROFILING.md), "Round 2"). With the instrument fixed and the
> engine binary identical, the same load measures a p99 of ~5.5 ms. We keep
> this note instead of silently rewriting the table: finding and fixing your
> own instrument error is the result.

## Method

- **Host:** single Apple-Silicon macOS laptop (12 cores). **Build:**
  `--release` for the Rust fleet + validator; the engine under test is the
  repo's [`examples/stub-engine`](../examples/stub-engine) — a deliberately
  simple Go matcher with per-symbol sharded locks, or its lock-free disruptor
  variant (`--engine disruptor`); it is the *contestant*, not our
  infrastructure.
- **Load:** one `bot-fleet` process multiplexing N virtual bots over a small WS
  connection pool, sending a realistic mix: limit orders across a 9-level price
  ladder, ~10 % market orders, ~12 % cancels, sizes 1..30, bots sharing symbols
  so they trade against each other.
- **Correctness:** every run replayed through the deterministic
  `reference-orderbook` in the engine's accepted arrival order (ack
  `engine_seq`) and diffed fill-for-fill.
- Reproduce with `target/release/bot-fleet … --market-per-mille 100 --cancel-per-mille 120`.

## Latency vs throughput (the "max TPS before failure" curve)

Sharded-mutex engine, wire-adjacent stamps, telemetry decoupled (HEAD):

| Regime | Bots × rate | Orders (5 s) | Sustained TPS | p50 | p90 | p99 | Timeouts | Correctness² |
|---|---|---|---|---|---|---|---|---|
| **Healthy** | 200 × 20 | 20,200 | 4,040 | ~1.1 ms | ~3.1 ms | ~3.8 ms | 0 | ✅ valid |
| **Saturation** | 500 × 100 | 250,500 | 50,100 | 2.2 ms | 3.8 ms | 5.5 ms | 0 | ✅ valid |

² Correctness validated over **191,137 fills** at the saturation point.
Healthy-row percentiles are the consistent center of 4 repeated runs;
saturation is a single representative run (repeats stayed single-digit-ms).

**What this shows.** The single-process Rust fleet pushed **250k orders in 5 s
(~50k orders/s)** and the engine acknowledged every one — zero timeouts — while
p99 stayed **single-digit milliseconds**. The measured ceiling on this host is
~250k orders/s (mutex engine), and at that wall the matcher itself is ~0.3 % of
engine CPU samples: the binding constraint is JSON encoding + per-message
socket writes plus total host CPU shared with the fleet, i.e. the *transport*,
not the matching logic. Price-time-priority correctness held at every regime,
with the full limit/market/cancel mix.

**Canonical demo numbers** (`scripts/run-local-demo.sh`, disruptor engine,
24 bots × 5/s, n=624 orders/run): p50 0.26 ms, p90 0.36–0.62 ms, p99
0.49–1.63 ms across repeated runs (at n=624 the p99 is the 7th-worst sample —
quote it with the spread, not from a single run).

## Correctness under concurrency

The same load is replayed in the engine's authoritative arrival order, so a
*correct* engine validates clean even with thousands of bots racing across
shared symbols — and a deliberately broken one is caught. Proven repeatably by
[`scripts/run-price-time-proof.sh`](../scripts/run-price-time-proof.sh): normal
mode passes, `broken-price-time-priority` mode fails with
`PRICE_TIME_PRIORITY_VIOLATION`.

A structural note for book-depth claims: resting depth has no steady state
under this load mix (~4 % of per-symbol flow rests forever), so depth grows
with run duration — 16/side at 5 s vs ~106/side at 60 s on the canonical
config. Any depth figure is only meaningful paired with a run length.

## Honest scope

These characterize the platform on one host against a toy engine; they are not a
production-exchange benchmark. The fleet scales horizontally as a Kubernetes
Indexed Job (`pod_index`-offset global IDs → 10k bots across 8 pods,
[`infra/k8s/31-bot-fleet-job.yaml`](../infra/k8s/31-bot-fleet-job.yaml)) and the
ingester scales with Kafka partitions, but those multi-node figures were not
measured here.
