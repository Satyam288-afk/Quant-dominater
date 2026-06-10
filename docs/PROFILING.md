# Profiling & Latency Optimization

Yes — profilers are part of how this platform is tuned. The benchmark *measures*
latency; profilers tell you *where it comes from* so you can cut it. This doc is
the toolbox plus a worked example where one profiler bought a **12× p99 win**.

## Tool map (which tool for what)

| Tool | Scope | When to reach for it | How |
|---|---|---|---|
| **Go pprof** | the engine under test (Go) | "why is *this* engine's tail latency high?" — CPU, heap, **mutex**, block, goroutine | `--pprof :6060`, then `go tool pprof http://localhost:6060/debug/pprof/mutex` |
| **Tracy** (`tracing-tracy`) | Rust hot paths (fleet/validator) | real-time, ns-resolution zone timeline; see per-op cost live | `cargo run -p bot-fleet --features profiling -- …` + Tracy server |
| **tokio-console** | Rust async runtime | task stalls, long polls, starvation in the fleet/ingester | add `console-subscriber`, run `tokio-console` |
| **samply** | any native binary (macOS-friendly) | quick sampling flamegraph, zero code change | `samply record ./target/release/bot-fleet …` |
| **cargo flamegraph** | Rust release binary | CPU flamegraph from `perf`/dtrace | `cargo flamegraph -p bot-fleet -- …` |
| **criterion** | a single Rust hot function | stable per-op micro-benchmark to optimize against | `cargo bench -p reference-orderbook --bench matching` |

## Worked example — pprof found a 12× p99 win

**Question:** under saturation the stub engine showed p99 ≈ 90 ms at ~40k TPS.
Where does the latency go — the matching logic? the order mutex?

**Profile** (engine run with `--pprof :6060`, mutex profiling on, under a
500-bot / 80-ord/s / 16-symbol load):

```
go tool pprof -top http://localhost:6060/debug/pprof/mutex
  cum%   function
  97.2%  main.(*JSONLLogger).Write      ← the audit log, not the matcher
  57.8%  main.(*Server).sendToClient
   1.2%  main.(*Engine).ProcessNew      ← actual matching: ~1%
   1.6%  main.(*Engine).ProcessCancel
```

The bottleneck was **not** the matching engine — it was the *synchronous audit
log* (`mutex` + `json.Encode` + file write on every in/out message) serializing
every WS send/recv. Non-obvious, and exactly the kind of thing a profiler
surfaces that guesswork misses.

**Fix:** make the audit log asynchronous — the hot path does a non-blocking
channel send; a single background goroutine owns the encoder + file; under
extreme load it drops (and counts) rather than stall the order path. The
authoritative record is the bot fleet's own `events.jsonl`, so dropping engine
audit lines is safe (correctness still validated `valid: true` over 187k fills).

**Result — same 40k TPS:**

| | p50 | p90 | p99 |
|---|---|---|---|
| before (sync log) | — | — | **90.35 ms** |
| after (async log) | 2.70 ms | 5.22 ms | **7.31 ms** |

p99 **90 ms → 7.3 ms (~12×)**, no throughput or correctness change.

**Then the profiler pointed at the next target.** With logging gone, the mutex
profile showed contention had moved to `ProcessNew`/`ProcessCancel` (62% / 37%) —
the matcher's single global lock. So we **sharded the lock per symbol**: each
order book has its own mutex (different symbols match concurrently), the
`engineSeq` is a global atomic assigned *under the target book's lock* (so within
a symbol engine_seq order still equals processing order — what the validator
relies on), and a global `orderID→symbol` index routes cancels. Correctness is
preserved because the validator replays per symbol in engine_seq order and diffs
fills as a multiset.

**The full picture, same 40k TPS:**

| stage | p50 | p90 | p99 | guard |
|---|---|---|---|---|
| original (sync log, 1 global mutex) | — | — | **90.35 ms** | |
| + async audit log | 2.70 ms | 5.22 ms | **7.31 ms** | 12× |
| + per-symbol sharded locks | 1.98 ms | 3.34 ms | **4.24 ms** | **~21× total** |

Both steps verified: **zero data races** (`go build -race` under a 12-symbol
cancel+market load), `valid: true` over 187k fills, and the price-time-priority
proof still passes. Two profiles, two safe optimizations, a 21× tail-latency cut.

### Third profile — knowing when you've hit the floor

A *CPU* profile at this point (not mutex) tells the real story:

```
go tool pprof -top http://localhost:6060/debug/pprof/profile
  71%  syscall.rawsyscalln     ← WebSocket read/write — the network
  ~15% runtime (scheduler, GC)
   ~5% encoding/json
   ~2% sortBook / matcher       ← the matcher is now noise
```

The engine is now **network-bound**: ~71% of CPU is the WebSocket syscalls, and
the matcher is in the noise. The allocation profile points at the remaining
compute cost — a redundant two-step JSON decode and the per-message audit-log
maps — so we cut those too (single decode + dispatch; audit log skippable via
`--events ""` for a perf run; both verified `valid: true` + race-clean). But the
honest read is that these *raise the throughput ceiling* (less CPU per message →
more TPS before saturation) rather than move p99 at a fixed load, because the
tail is now I/O queueing, not compute.

**"Network-bound" is a clue, not a dead end.** When the profile says the cost is
the network, the next move isn't more *compute* tuning — it's a *network-layer*
look. And there it found the classic trading-path latency bug: **Nagle's
algorithm**. tokio leaves `TCP_NODELAY` *off* by default, so the Rust load
generator was letting the kernel coalesce small order frames before sending —
adding pure buffering delay to every measured round-trip. Setting
`set_nodelay(true)` on each fleet connection (`pool.rs`, `main.rs`) is a one-line
fix with a clean A/B (200 bots / 8 conns / 5k TPS, machine cooled):

| | p50 | p90 | p99 |
|---|---|---|---|
| Nagle ON (tokio default) | 2.56 ms | 4.82 ms | **11.34 ms** |
| `TCP_NODELAY` | 2.44 ms | 4.35 ms | **5.64 ms** |

**p99 halved (11.3 → 5.6 ms)** — Nagle's signature: it leaves p50/p90 alone and
inflates the tail. (Go sets `NoDelay=true` by default, so the engine side was
already clean; the bug was the generator.) Correctness unchanged (`valid: true`).

**The engineering conclusion:** *now* stop the latency chase. Across the three
profiles we cut p99 by mutex (sync→async logging, 12×), lock sharding (→21×), and
a network-layer Nagle fix (another 2× on the generator). What's left — object
pools, a `sonic`-class JSON codec — trades real complexity and risk on the
contestant for gains the profiles say are small. Profilers are as useful for
telling you *when to stop* as for what to fix; here they pointed at compute,
then mutex, then the network, and we took each win that was real and safe.

## Bonus: a lock-free Disruptor engine (`--engine disruptor`)

"Could a different *architecture* beat the locks entirely?" is a fair question,
so the engine ships a second, opt-in matching core
([`examples/stub-engine/disruptor.go`](../examples/stub-engine/disruptor.go)) in
the LMAX-Disruptor style — the default stays the proven sharded-mutex one:

- **Lock-free MPSC ring buffer** per shard (LMAX claim/publish with per-slot
  atomic sequences): many WS goroutines publish, one matcher consumes.
- **One matcher goroutine per shard** ⇒ matching touches no locks (single
  consumer). Symbols hash to a fixed shard, so a symbol's orders stay FIFO —
  engine_seq order == processing order, what the validator needs.
- **Output stage**: results fan out to a pool of writers, decoupling the network
  write from matching.

A/B at ~7.7k TPS, same load:

| symbols | mutex p99 | disruptor p99 |
|---|---|---|
| 1 (max contention) | 8.00 ms | **4.04 ms** |
| 2 | 8.06 ms | **3.78 ms** |
| 16 (realistic) | 7.33 ms | **3.91 ms** |

**~2× lower p99 across the board** — and it wins even at 16 symbols where lock
contention is already low. The honest read: most of the gain isn't "no locks"
(those cost little) — it's **pipelining**. The mutex engine's WS goroutine does
read→match→send synchronously; the Disruptor's reader just publishes and reads
the next message, with matching and sending on separate goroutines, so the
per-message critical path is shorter and I/O can't stall matching. Verified:
**zero data races** (`-race` under cancels+market), `valid: true` over 18k fills,
and the price-time-priority proof passes on the disruptor engine too (normal
valid, broken `PRICE_TIME_PRIORITY_VIOLATION`). The LMAX pattern, with numbers.

### Wait strategy — a hotspot that was *not* a bug

Profiling the Disruptor itself: the block profile showed the output writers and
logger mostly idle (spare capacity, not the bottleneck), but the CPU profile
showed **~30% in `runtime.usleep`** — the matchers' idle wait. Tempting to "fix".
We tried: a channel-**park** (LMAX BlockingWaitStrategy) and a time-budgeted
hybrid. Both cut idle CPU but **raised p99** (disruptor fell back to ~mutex
levels), because parking adds wakeup latency to *active* matchers too. So we
reverted to busy-spin. The lesson: that 30% `usleep` was idle shards *correctly*
yielding their cores via short sleeps, while active shards stayed hot — which is
*why* the latency was 2× better. Not every profiler hotspot is waste; here it was
the mechanism delivering the win. (In a real deployment you'd also size shards to
cores instead of running 32 on ~10; the busy-spin/blocking choice is the same
trade-off LMAX exposes as a configurable WaitStrategy.)

## A full-tool sweep — and the discipline of the floor

A last pass pointed *every* tool at the codebase looking for remaining margin:
`cargo clippy --workspace --all-targets`, Go escape analysis
(`go build -gcflags=-m`), Go `pprof` alloc attribution, a Go micro-benchmark
(`testing.AllocsPerRun` + `-benchmem`), and a build-profile audit — with a
multi-agent analysis that proposed candidates and then **adversarially verified
each against the code**. Of ~32 candidates, exactly **one** survived as real,
safe, and measurable. The rest were rejected with reasons, which is the point:

- **`[profile.release]` (LTO / `codegen-units=1` / `panic=abort`)** — real for a
  hot serving binary, but the Rust here is the *load generator* (itself
  network-bound) and two batch tools; the verified impact on the judged workload
  was negligible, so it wasn't worth the build-time cost.
- **Sorted-insert instead of `sortBook`'s full re-sort per insert** — rejected in
  this round on the "books stay shallow" assumption. *Round 2 (below) refuted the
  rejection*: controlled-depth benchmarks plus an e2e A/B at 60k orders/s showed
  the re-sort collapsing the tail (p99.9 up to 43ms), and a differential test
  proved the binary insert order-identical in both engine modes. Now done — an
  honest record that the discipline cuts both ways: rejections are hypotheses
  too, and a deeper measurement can overturn them.
- **Wire protocol (binary framing / `simd-json` / write-batching)** — batching
  trades p99 for throughput, directly against the `TCP_NODELAY` latency fix, and
  pushes complexity onto the contestant. Not worth it.

**The one win — `uniqueClients` fast-path (`examples/stub-engine`).** Computing a
fill's recipients ran a `map[*Client]bool` dedup on *every* fill in both engines.
Replacing it with direct pointer comparison for the only shape that occurs (the
two trade owners) is **~2.4× faster per call** — and the benchmark is where it
gets honest:

```
BenchmarkUniqueClients-12        11.5 ns/op   16 B/op   1 allocs/op   ← new
BenchmarkUniqueClientsOld-12     28.2 ns/op   16 B/op   1 allocs/op   ← old
```

The escape-analysis *hypothesis* was that the map was a per-fill **heap
allocation**. The benchmark **refuted it**: both versions show `1 allocs/op` —
Go stack-allocates the small non-escaping map, so the only heap object is the
result slice the `FillDelivery` must own. The real cost the map carried was
**CPU** (map init + pointer hashing + lookups), not allocation. Measure, don't
assume. Verified `valid: true` on both the mutex and disruptor engines and the
price-time-proof still passes; pinned by `uniqueclients_test.go`.

**The floor, stated plainly.** This is a network-bound system and the headline
moves are already made (21× p99, lock-free Disruptor, the resilience story). The
`uniqueClients` change is worth shipping because it is real, zero-risk, and
*measured* — but its honest effect is a small rise in the **throughput ceiling**
(less CPU per fill → marginally more TPS before the socket saturates), **not**
p99 at fixed load, because the tail is the network round-trip, not engine CPU.
Past this point, further engine micro-optimization stops paying off on the judged
workload: the books are shallow, the matcher is off the critical path, and any
nanoseconds saved in the engine are swamped by socket + serialization time on the
wire. If more margin were wanted it lives in *transport*, not the matcher — and
knowing that, and stopping, is itself the result.

## Round 2 — profile the instrument, not just the engine

A second sweep ran six profilers in parallel — live `sample(1)` on a real run,
Go CPU/alloc pprof with **controlled book depth**, Rust microbenches of the
fleet's send/recv path, a wire-path audit on both sides, ingester throughput,
GC A/B — then **re-measured every candidate sequentially and adversarially**
(an unloaded box; a finding survives only if an independent skeptic reproduces
its numbers AND it can plausibly move end-to-end p99). The headline:

**At every load tested, the largest contributors to the *reported* p99/TPS were
in the benchmark harness, not the matching engine.** The baseline sample showed
the engine >99% idle at canonical load — the tail lived in the measuring
instrument:

1. **FileSink convoy (`bench-core/telemetry/sink_file.rs`)** — every bot
   `await`ed a global `Mutex<tokio::fs::File>` + unbuffered write *inside the
   latency-measuring loop*, 2+ emits per order. Measured ~7.9µs per emit →
   135ns after decoupling to a writer task + 64KiB `BufWriter`. E2E: healthy
   p99 11.3→5.9ms (lands at the `backend=none` floor); at saturation **26.7k →
   50.1k TPS, ~50k timeouts → 0, p99 3.5s → 5-6ms**. Telemetry completeness
   verified line-for-line.
2. **KafkaSink delivery-await (`sink_kafka.rs`)** — the live backend awaited
   each rdkafka delivery report inline with `linger.ms=5`: a measured ~5.9ms
   hard floor per emit, capping a bot at ~84 orders/s. Now enqueues via
   `send_result` and consumes reports out-of-band in batches; `flush()` stays
   the durability barrier.
3. **Stamp placement (`bot-fleet/pool.rs`)** — pooled mode stamped *send*
   before the outbound channel hop and *recv* after parse+clone+inbox hop, so
   the fleet billed its own plumbing to the engine. Stamps now sit at the wire
   (supervisor after `write.send`, reader right after `read.next()`): reported
   p50 −0.55ms, p99 −0.65 to −1.17ms, reaching direct-socket parity. Pooled
   numbers before/after this change are not comparable — by design.
4. **Engine: `sortBook` → binary-search insert (both engines)** — the full
   both-sides stable re-sort per resting insert was 46-48% of matching CPU.
   Upper-bound binary insert + one memmove is order-identical (differential
   test over 20k ops × both modes, pinned in `insertsorted_test.go`) and ~8.5×
   faster per resting order; at 60k/s it is the difference between tail
   collapse (0.3-22ms, p99.9 43ms) and a tight 105-137µs. Cancels moved from a
   linear ID scan (`runtime.memequal` ~26% of profile) to O(log n) positional
   removal with pointer-identity verification.
5. **Demo default: `--engine disruptor`** — at the exact canonical load the
   lock-free engine measured p50/p99 0.48/2.00ms vs mutex 1.52/4.95ms. The
   demos now default to it (`STUB_ENGINE=mutex` flips back); cost is ~55% of
   one core of idle backoff-timer churn, fine on a bench host.
6. **Ingester dedup 2.7×** — the idempotency gate paid SipHash twice (identity
   hash + the `HashSet`'s own). ahash identity + a pass-through hasher on the
   already-uniform u64 keys: 10.6 → 29.0 Mevents/s, full-field identity
   semantics unchanged. (Either half alone yields only ~1.4× — measured.)
7. **Audit log honesty** — the engine's `--events` JSONL silently lost *all*
   buffered entries on SIGTERM (0-line files while the writer actively
   encoded). The engine now drains and flushes on shutdown, flushes every
   500ms, and the WS loop guards the per-message boxing behind the nil-logger
   check.

Equally important, **what the verifiers killed** (each with a reproduced
measurement): GOGC/GOMEMLIMIT tuning (GC STW touches ~0.1% of ops — between
p99.9 and p99.99), marshal-once fill fanout (212ns saved vs a 72µs RTT, e2e
flat), gorilla `PreparedMessage` (~5.8µs of setup per message at fanout=2),
`simd-json` (loses to `serde_json` on 213B flat objects on Apple Silicon),
widening the WS pool (pool64 measured *worse* than pool8), wire-key compaction
(every message already fits one TCP segment with NODELAY), and parking the
disruptor's idle matchers (the known regression — the idle cost is timer
churn, not the spin).

The Round-1 floor statement said the remaining margin lived "in transport, not
the matcher". Round 2 sharpened it: most of that margin lived **in the
harness's own bookkeeping on the measurement path** — and a benchmarking
platform that finds and fixes its own instrument error is worth more than one
that optimizes its engine into the noise.

## Reproduce it

```bash
# 1) engine with the pprof endpoint
go run ./examples/stub-engine --addr :8080 --pprof :6060

# 2) saturate it
cargo run --release -p bot-fleet -- --target ws://localhost:8080/ws \
  --bots 500 --orders-per-sec 80 --duration-sec 6 --ws-connections 16 \
  --symbols 16 --market-per-mille 100 --cancel-per-mille 100 --backend none

# 3) profile the contention (also: /profile for CPU, /heap, /block, /goroutine)
go tool pprof -top -unit=ms http://localhost:6060/debug/pprof/mutex
go tool pprof -http=: http://localhost:6060/debug/pprof/profile   # CPU flamegraph in the browser
```

## Tracy on the Rust fleet

The fleet's hot functions are wrapped in `zone!(...)` macros that compile to
nothing by default and to Tracy zones under `--features profiling`:

```bash
# launch the Tracy server (GUI), then:
cargo run -p bot-fleet --features profiling -- --target ws://localhost:8080/ws --bots 1000 …
```

Zones stream live (`make_order`, `build_send`, …) with per-call timing, so you
can see order-generation cost as you scale the fleet. (Async handlers are left
to `#[tracing::instrument]` rather than scope guards, which can't cross `.await`
in a `Send` future.)

## Principle

Don't optimize by guessing. Measure end-to-end latency (the leaderboard's
p50/p90/p99 + per-second [time-series](BLUEPRINT.md)), then attach a profiler to
the component that owns the tail and cut the top contributor — repeat. The 12×
win above came from one mutex profile, not from rewriting the matcher.
