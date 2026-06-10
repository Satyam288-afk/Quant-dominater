# Scoring

Correctness is a hard gate.

```text
if correctness_failed:
    score = 0
else:
    score =
        0.40 * latency_score +
        0.30 * throughput_score +
        0.20 * stability_score +
        0.10 * resource_efficiency_score
```

## Resource efficiency (the 10% term) — sampled, not a stub

The resource term rewards engines that pass the correctness gate while using
*less* CPU and memory. It is computed from **real samples** of the contestant
engine taken by the sandbox during the run, written to `resource.json` in the
run's artifact dir and folded into the metrics before scoring:

- **Docker mode** (`docker_runner.go`): cgroup-accurate CPU% + memory via the
  Docker `ContainerStats` API (CPU% from total-vs-system-time deltas across
  ticks, scaled by online CPUs — the same math as `docker stats`).
- **Local mode** (`local_runner.go`): the engine process's `%CPU`/`RSS` via
  `ps` (cross-platform). A background sampler tracks the peak and keeps
  `resource.json` current.

The curve (identical in the Go and Rust scorers — `score-engine` /
`orchestrator` / `bench-core`, pinned by `TestResourceScoreMatchesRustCurve`):

```text
cpu = min(cpu_pct_peak, 100)          # a busy single core caps CPU penalty
score = 100
      - max(cpu - 50, 0) * 1.5        # soft penalty past 50% CPU
      - max(mem_mb_peak - 512, 0)*0.05 # soft penalty past 512 MB
score = clamp(score, 0, 100)
```

`cpu_pct_peak <= 0` means **not measured** (no sandbox, or sampling failed) and
yields a neutral `100` — a sampling miss never unfairly penalises an engine.

**Measured live** (this engine, `ps`-sampled under load): at 200 bots it used
**32.8% CPU / 21 MB → resource_score = 100** (genuinely efficient, *earned* not
assumed); at 600 bots on one symbol it used **163% CPU (~1.6 cores) → 25** (CPU
capped at 100% → `(100-50)*1.5 = 75` penalty). The term demonstrably moves with
real load.

## Local Slice Metrics

The first milestone computes:

| Metric | Meaning |
|---|---|
| `orders_sent` | total orders written by bots |
| `acks_received` | accepted/rejected/cancel acks received |
| `timeouts` | orders without ack before drain timeout |
| `tps` | acks per benchmark second |
| `p50` | median ack latency |
| `p90` | 90th percentile ack latency |
| `p99` | 99th percentile ack latency |
| `valid` | validator pass/fail |

## Correctness Fail Reasons

```text
MISSING_FILL
UNEXPECTED_FILL
FILL_MISMATCH
PRICE_TIME_PRIORITY_VIOLATION
INVALID_EVENT
```

## Later Metrics

Production scoring adds:

- p99.9 latency
- timeout rate
- reject rate
- disconnect count
- burst stability
- cancel-storm stability
- ✅ CPU efficiency — *implemented* (sampled, see above)
- ✅ memory efficiency — *implemented* (sampled, see above)

