# Resilience & Failure Modes

This document describes how the platform behaves under failure, grounded in the
actual code paths. Where a mechanism is best-effort or stubbed, it is called out
as such — we would rather under-claim than overclaim.

> Companion docs: [BLUEPRINT.md](BLUEPRINT.md) (system design),
> [SECURITY_SANDBOX.md](SECURITY_SANDBOX.md) (isolation controls),
> [SCORING.md](SCORING.md) (the correctness gate).

## 1. Failure-mode table

| Failure | Detected by | System response | Blast radius |
|---|---|---|---|
| Contestant engine crash mid-run | bot-fleet read loop returns `None`/`Err` on the WS; orchestrator run `context` deadline (`runTimeout`, default **3 min**, `manager.go`) | The bot task breaks out of its `select!` loop, records remaining `pending` orders as `timeouts` (`main.rs`); a hard crash surfaces as a `BENCHMARKING`-stage error → run `Status=FAILED`, `FailureStage="BENCHMARKING"` (`manager.go fail()`). `defer handle.Stop()` removes the container. | One run. Other concurrent runs and the leaderboard are untouched; the bad engine simply produces no/partial fills and gets scored on what it delivered (likely a correctness fail → 0). |
| Build failure | `sandbox-runner` Docker build stream emits an `error`/`errorDetail` line; `writeDockerBuildLog` returns it (`docker_runner.go`); 10-min build context timeout otherwise | `engine.Build` returns error → orchestrator `fail(ctx, run, "BUILDING", err)` → `Status=FAILED`, `FailureStage="BUILDING"`. Full build log captured at `docker-build.log`. | One run / one submission. The image is never started; nothing reaches the data plane. |
| Healthcheck failure | `waitForHealth` polls `GET /health` every **100 ms** with a **500 ms** per-request timeout until a **10 s** deadline (`docker_runner.go` / `local_runner.go`) | On timeout the container is force-stopped (`dockerStop`) and `Start` returns an error → `fail(..., "SANDBOX_STARTING", err)` (the `HEALTHCHECKING` transition itself only checks `ctx`). Run ends `FAILED`. | One run. The container is reaped (`AutoRemove` + force-remove). |
| bot-fleet connection drop mid-run (engine blip / pooled socket dies) | The per-connection **supervisor task** (`pool.rs`) sees `write.send()` error or the reader's `read.next()` return `Err`/`None` | The supervisor **reconnects with capped exponential backoff** (10 ms → 2 s) and resumes on the *same* bot channels, so bots are oblivious — they experience backpressure while the socket is down (their bounded 4096-deep sender buffers, then awaits) and resume the instant it reconnects. Because the buffer rides through a short outage, **no orders are lost** for blips shorter than the buffer depth; the recovery count is surfaced as `reconnects: N` in the fleet summary, and the orders that *did* outlive the outage show up honestly as elevated tail latency / `timeouts`. The initial connect stays strict (engine unreachable at startup still fails the run). Verified live by [`scripts/run-chaos-demo.sh`](../scripts/run-chaos-demo.sh) (`kill -9` the engine mid-run → `reconnects=4`, run completes). | The bots on the affected connection, for the backoff window only. Surviving connections are untouched; the recovery is visible (`reconnects`) not hidden. |
| Kafka/Redpanda broker unavailable (live telemetry) | `KafkaSink::emit` send future errors after `message.timeout.ms=5000` / the 5 s `delivery_timeout`; producer queue bounded at `queue.buffering.max.messages=200000` (`sink_kafka.rs`) | At the bot-fleet call site the emit result is **discarded** (`let _ = sink.emit(...)`), so a broker outage degrades telemetry silently rather than killing the load test — the WS order path and the local `events.jsonl`/`contestant_outputs.jsonl` (the validator's inputs) are unaffected. When the 200k-message buffer fills, librdkafka applies backpressure / drops per its queue policy. | Live metrics/leaderboard freshness only. Correctness and the file-based scoring path still work, because the validator reads the JSONL artifacts, not Kafka. |
| Ingester lag / backpressure | Bounded `mpsc::channel(16_384)` between source and aggregator (`telemetry-ingester/main.rs`); consumer group lag observable on the broker | The Kafka source `tx.send(evt).await` **awaits** when the channel is full, which stops `consumer.recv()` and lets consumer-group lag build instead of dropping events (backpressure, not loss). Adding ingester replicas up to the partition count drains lag. | Live aggregate latency only. No data loss in the channel; events wait in Kafka. |
| Redis unavailable | `sink::redis::spawn` returns `Err` at startup → logged and the sink is set to `None` (`telemetry-ingester/main.rs`); live score publish wrapped in best-effort `if let Err(...)` (`score-engine`); the leaderboard-api poller's `lastRefresh` stops advancing (`redisboard.go`) | The ingester continues with Redis disabled; the score-engine logs `redis leaderboard publish failed` and **does not fail the run** (local `score.json` is the source of truth). The leaderboard-api keeps **serving the last good snapshot** but flips its dependency-aware `/ready` probe to **503** once the data is older than 3 poll intervals (while `/health`, liveness, stays 200) and stamps `X-Leaderboard-Age-Ms` on `/leaderboard` — so k8s pulls the pod from the LB and clients can see the staleness instead of trusting frozen data as live. Redis live state is rebuildable from Timescale ([BLUEPRINT.md](BLUEPRINT.md) §4). | Live leaderboard/scorecard freshness only — and now *observable*, not silent. Runs still complete, score, and persist. |
| Malicious / runaway submission | Container resource caps + correctness gate (`docker_runner.go`, validator) | Contained at the cgroup boundary (mem cap + swap off, `PidsLimit=512`, `nofile=4096`, `CapDrop: ALL`, `no-new-privileges`, read-only rootfs, default-deny egress — see §4). A submission that is fast but wrong is caught by the validator and scored **0** (§5). CPU/wallclock are bounded by the engine's `NanoCPUs` quota and the orchestrator `runTimeout`. | The submission's own container. No host descriptor/process exhaustion, no egress to other contestants or the internet. |

The orchestrator FSM is `QUEUED → BUILDING → SANDBOX_STARTING → HEALTHCHECKING →
BENCHMARKING → VALIDATING → SCORING → FINISHED` (`model.go`). **Note (honesty):**
there are no distinct `BUILD_FAILED` / `HEALTHCHECK_FAILED` / `SANDBOX_CRASHED`
states — every failure collapses to a single terminal `RunStatus` (`FAILED`,
`TIMED_OUT`, or `CANCELLED`) plus a `FailureStage` string recording where it
broke (`manager.go fail()` / `finishDirectFailure()`). `ctx.Err()`
distinguishes a deadline (`TIMED_OUT`) from a cancel (`CANCELLED`).

## 2. Delivery & idempotency

Telemetry is **at-least-once**. The consumer runs with
`enable.auto.commit=true` and `auto.offset.reset=earliest`
(`telemetry-ingester/source/kafka.rs`), so on an ingester crash between an
auto-commit and processing, already-handled events can be re-delivered. The
producer keys every record on `{run_id}:{bot_id}` so per-bot ordering survives
partitioning (`sink_kafka.rs`). To shrink that re-delivery window, on `SIGTERM`
the consumer **synchronously commits its offset** (`commit_consumer_state`,
`CommitMode::Sync`) before exiting, so a clean restart resumes from the last
processed position instead of re-reading the ~5 s auto-commit window.

**Idempotency — two independent layers.** (1) For *live telemetry*, the
**aggregator now dedups**: each `RunState` keeps a `HashSet<u64>` of every
event's identity (a hash of all fields), so a re-delivered `TelemetryEvent` is
dropped instead of double-counting `peak_tps` or inflating the percentile sample
set (`telemetry-ingester/aggregator.rs`). The count of dropped duplicates is
surfaced as `duplicates_dropped` in the run summary, so re-delivery is
*observable*, not silent. The identity hashes every field, so the multiple
partial fills of one order — which share a `client_order_id` but arrive at
distinct `recv_ts_ns` — are correctly kept, while a byte-identical re-delivery
is dropped. Proven on real data: feeding the same telemetry stream twice yields
*identical* `peak_tps`/`acks_received` and `duplicates_dropped` equal to the
re-delivered count. (2) For *scoring*, the **validator** independently dedups
fills on `engine_seq` (a `HashSet<u64>`; falls back to a `buy|sell|price|qty`
key when `engine_seq` is absent) in `read_actual_fills`
(`rust/validator/src/main.rs`). A single trade is *legitimately* reported to
both counterparties (and, through the fleet's connection pool, by every
connection that carries one of them), so the same `engine_seq` appearing
multiple times is correct two-sided execution reporting, not a fault — those
identical copies are deduped, never penalised. The validator only raises an
`INCONSISTENT_FILL` violation when two reports share an `engine_seq` but
*disagree* on the trade (`edge_cases.rs`). So duplicate delivery now perturbs
**neither** live telemetry **nor** the correctness verdict or final score.

## 3. Backpressure

- **Producer side** — the Kafka producer batches with `linger.ms=5` and bounds
  its in-flight buffer at `queue.buffering.max.messages=200000`; once full,
  librdkafka backpressures/drops per its queue policy. The bot-fleet treats
  telemetry emit as fire-and-forget (`let _ = sink.emit(...)`), so telemetry
  pressure never stalls the order path.
- **Pool side** — each pooled WS writer drains a bounded `mpsc::channel(4096)`;
  a bot's `sender.send(text).await` (`pool.rs`/`main.rs`) awaits when that
  buffer is full, throttling order issuance to what the socket can flush rather
  than growing memory unboundedly.
- **Ingester side** — the source→aggregator `mpsc::channel(16_384)` is bounded
  and the source `await`s on `send`, converting overload into consumer-group lag
  (recoverable) instead of dropped events. HPA on the ingester scales replicas
  to the partition count to drain that lag ([BLUEPRINT.md](BLUEPRINT.md) §8).

## 4. Isolation as blast-radius containment

Every contestant container is created with (`docker_runner.go`, matching
[SECURITY_SANDBOX.md](SECURITY_SANDBOX.md) §Hardening):

- `CapDrop: ["ALL"]` + `SecurityOpt: no-new-privileges:true` — no Linux
  capabilities, no privilege escalation.
- `ReadonlyRootfs: true` + a single locked `/tmp` tmpfs
  (`rw,noexec,nosuid,nodev,size=64m`); writes go only to the mounted artifacts dir.
- `Memory` cap with **swap disabled** (`MemorySwap == Memory`,
  `MemorySwappiness=0`) so memory pressure can't be masked by paging;
  `PidsLimit=512` and `nofile` ulimit `4096` cap fork/descriptor exhaustion.
- `NanoCPUs` quota + optional `CpusetCpus` pinning (fairness + reproducible
  latency), and a pluggable OCI runtime via `SANDBOX_DOCKER_RUNTIME` (e.g.
  gVisor `runsc`).
- Network **default-deny**: the port is published bound to `127.0.0.1` only, and
  with `network_egress=false` DNS is black-holed (`DNS: ["127.0.0.1"]`). In
  Kubernetes this is enforced by a per-cell `NetworkPolicy` (`infra/k8s`) so the
  pod is reachable **only** by the bot fleet on `:8080` with no internet or
  cross-contestant egress.
- `AutoRemove: true` + force-remove on stop, with bounded build (10 min) and
  start (30 s) context timeouts so a stuck container is always reaped.

The blast radius of any one submission is therefore its own cgroup/netns: it
cannot starve the host, reach the network, or touch another contestant.

## 5. Correctness gate — fast-but-wrong scores 0

Correctness is a hard gate, not a weighted term. The validator replays the
canonical input through the deterministic price-time-priority
`reference-orderbook`, reconstructing the engine's true arrival order from the
ack `engine_seq` (`order_by_engine_seq`, `rust/validator/src/replay.rs`), then
diffs expected vs actual fills (`compare.rs`). Any mismatch —
`PRICE_TIME_PRIORITY_VIOLATION`, `MISSING_FILL`, `UNEXPECTED_FILL`,
`INCONSISTENT_FILL`, `PARTIAL_FILL_OVER_QTY`, `OUT_OF_ORDER_SEQ` — sets
`valid=false`.

A `false` verdict short-circuits scoring **before** any latency/throughput math:
`score-engine` returns `CorrectnessGate="failed"` with `Score=0`
(`services/score-engine/internal/scoring/scoring.go`; the Rust path is identical
via `compose`, `rust/bench-core/src/score/formula.rs`). The engine's measured
output is never trusted — only the reference orderbook decides what a correct set
of fills is. The weighted formula
(`0.40·latency + 0.30·throughput + 0.20·stability + 0.10·resource`) applies
**only** to engines that pass the gate. The `resource_score` is now **real**:
the sandbox samples the contestant engine's CPU/memory (cgroup-accurate via
Docker `ContainerStats`, or `ps` locally), writes `resource.json`, and the
scorer folds the peak into the resource term — measured live at 100 (efficient)
down to 25 (heavy load). Unmeasured falls back to a neutral 100. See
[SCORING.md](SCORING.md).

## 6. Graceful shutdown (SIGTERM)

Every k8s rolling deploy, scale-down, and pod eviction starts with a `SIGTERM`,
then a grace period, then `SIGKILL`. A service that ignores `SIGTERM` is severed
mid-flight: in-flight requests cut, WebSocket clients dropped without a Close
frame (they hang and retry-storm), buffered work and connection pools leaked.

The **leaderboard-api** handles it (`services/leaderboard-api/main.go`):
`signal.NotifyContext(SIGTERM, SIGINT)` drives an explicit `*http.Server`; on the
signal it `srv.Shutdown(ctx)` (10 s grace) to drain in-flight HTTP, the
WebSocket fan-out loop selects on the same shutdown context and sends a proper
`CloseGoingAway` frame so clients exit cleanly, and only then does the deferred
`rb.Close()` release the Redis pool — cleanup that the previous
`log.Fatal(http.ListenAndServe(...))` (which skips all defers) could never run.
A second signal restores default handling so an impatient operator can still
force-quit. This is the user-facing half of the chaos demo (the live board
survives a deploy).

The **telemetry-ingester** handles `SIGTERM` too (`telemetry-ingester/main.rs`):
a signal flips a `watch` channel the Kafka source observes, so it commits its
offset and closes; the main loop then **drains the in-flight `mpsc(16384)`**
buffer into the aggregator (bounded by a 5 s grace so a wedged source can't
hang shutdown) and writes the final summary — instead of dropping up to 16 k
buffered events and re-reading the auto-commit window on restart.

The **orchestrator** is the higher-stakes one (`services/orchestrator`): its
claim worker and every in-flight run previously ran off `context.Background()`,
so a `kill` left the run goroutine — and its child engine process — running
*detached* past process exit, with the run wedged mid-`building` on disk. Now
`signal.NotifyContext` cancels a `serverCtx` that (1) stops the claim worker from
picking up new runs, (2) drives `srv.Shutdown`, and (3) calls `Manager.Shutdown`,
which **cancels every in-flight run** (iterating the `cancels` map) so each
goroutine unwinds and persists a terminal `CANCELLED` state, then waits —
bounded — for them to drain. Unit-tested (`manager_shutdown_test.go`, race-clean)
for both the drain and the grace-deadline paths. The **submission-api**,
**sandbox-runner**, and **control-panel** get the same `signal.NotifyContext` +
`srv.Shutdown` HTTP drain (so in-flight requests finish and `defer`s run).

> Honest scope: every long-running service now installs a `SIGTERM`/`SIGINT`
> handler and drains. The remaining hardening the audit flagged is finer-grained:
> `fsync` durability on the submission-api artifact write (deliberately deferred —
> it sits on the measured path) and explicit child-process reaping in the
> sandbox-runner beyond HTTP-handler drain. Listed here rather than hidden.

## 7. Chaos demo — inject the failure, watch the recovery

[`scripts/run-chaos-demo.sh`](../scripts/run-chaos-demo.sh) is a dependency-free
(no Docker, no Redis) failure-injection harness that *proves* the two properties
above by actually causing the failures:

1. **Load-generator resilience.** It `kill -9`s the stub engine 8 s into a live
   fleet run, leaves it dead for 5 s, then restarts it. The fleet detects the
   drop, reconnects each pooled connection with backoff, and resumes — a typical
   result is `reconnects: 4`, `timeouts: 0` (the 4096-deep per-connection buffer
   rode through the 5 s gap), with `p50` unchanged but `p90/p99` spiking into
   the seconds (the buffered orders that waited out the outage — honest tail).
   The script *fails* (non-zero exit) if `reconnects < 1`, so it's a test, not a
   slideshow.
2. **Graceful shutdown.** It then `SIGTERM`s the leaderboard-api *and* the
   orchestrator and asserts each logs `drained cleanly` and exits `0` — proving
   the orchestrator stops its worker and cancels in-flight runs rather than
   orphaning them.

A crashed engine loses its order book, so this demo deliberately does **not**
claim the *engine's* correctness survives a crash — losing state is exactly what
the scoring gate should punish. What is resilient is the platform *around* the
engine: the load generator keeps measuring across the blip, and the services
shut down cleanly.
