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
**release-built fleet**, 24 bots × 5/s, n=624 orders/run): p50 0.24–0.29 ms,
p90 0.34–0.40 ms, p99 0.42–0.47 ms across repeated runs. (An earlier revision of
this line quoted p99 0.49–1.63 ms — that wide spread was the demo's *debug-built*
load generator bunching its own tail; building the fleet `--release` like the
saturation table above tightened it, with the engine unchanged. At n=624 the p99
is still the 7th-worst sample, so quote it as a range, not from a single run.)

## Latency floor (closed-loop probe)

The open-loop numbers above measure latency under a realistic arrival process —
which, at 5 orders/s per bot, is dominated by the *fleet's own* parked-task
wake-ups (~90 % of the round trip; decomposition in
[PROFILING.md](PROFILING.md), Round 5), not by the engine or the wire. The
fleet's `--closed-loop` mode removes that term by sending each next order the
moment the previous ack lands, measuring the WS+JSON transport floor itself:

| Mode | Bots | Orders (5 s) | Sustained TPS | p50 | p90 | p99 | Timeouts | Correctness |
|---|---|---|---|---|---|---|---|---|
| Closed-loop probe | 1 | 138,319 | 27,664 | 0.03 ms | 0.04 ms | 0.05 ms | 0 | ✅ valid |
| Closed-loop probe | 4 | 287,775 | 57,555 | 0.06 ms | 0.10 ms | 0.14 ms | 0 | — |

Same engine, same order mix (limit/market/cancel), same wire-adjacent stamps,
same validator replay — only the send trigger differs. The probe is **not** the
scored mode: a closed loop self-clocks, so it under-reports queueing under
overload (the coordinated-omission caveat); it answers "what can this platform
resolve?" (~30 µs round trips, ~1.5 µs/order of fleet overhead at 28 k/s on one
connection), while the open-loop tables above answer "how does an engine behave
under load it does not control?". Reproduce: add `--closed-loop --bots 1` to
the fleet invocation against a **fresh** engine (a reused engine still holds the
previous run's resting orders, which the new run's validator rightly knows
nothing about and will flag).

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

## Multi-node horizontal scale (measured on kind)

The fleet's horizontal-scale claim is no longer an extrapolation. The same
Indexed-Job + `--pod-index` sharding from
[`infra/k8s/31-bot-fleet-job.yaml`](../infra/k8s/31-bot-fleet-job.yaml) was run
on a **4-node `kind` cluster** (1 control-plane + 3 workers) via
[`scripts/run-kind-scale-proof.sh`](../scripts/run-kind-scale-proof.sh), at a
laptop-modest 100 bots/pod × 10 orders/s:

| Pods (Indexed Job) | Aggregate orders (20 s) | Sustained | Timeouts |
|---|---|---|---|
| 2 | 40,200 | ~2,010 /s | 0 |
| 4 | 80,397 | ~4,019 /s | 0 |
| 8 | 160,794 | ~8,039 /s | 0 |

Aggregate throughput scales **linearly** with pod count (a clean 2× per
doubling) with **zero drops**, the Job's pods **fan out across the worker
nodes**, and `--pod-index` gives each pod a **disjoint, globally-unique bot-id
range** (pod 0 → bots 1..100, pod 1 → 101..200, … pod 7 → 701..800; no
collisions). This is the horizontal-scale design the IaC realizes, demonstrated
on real (containerised) Kubernetes nodes rather than asserted. The same manifest
reaches the 10k-bot / ~200k-orders/s regime by raising the per-pod numbers and
node count on a multi-core cluster.

## Honest scope

These characterize the platform on one host (and a small local cluster) against
a toy engine; they are not a production-exchange benchmark. The kind sweep above
proves the fleet's *load-generation* scales linearly across pods/nodes; the
data-plane (Kafka-partitioned ingester, Timescale/Redis) is designed to scale
with it but was sized, not load-tested, at the 10k-bot regime — see
[RESIDUALS.md](RESIDUALS.md).
