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
- CPU efficiency
- memory efficiency

