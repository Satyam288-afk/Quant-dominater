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
- **Sorted-insert instead of `sortBook`'s full re-sort per insert** — genuine
  O(n·log n)→O(n) algebra, but the realized payoff is conditioned on *deep* books,
  and the realistic 16-symbol + heavy-cancel workload keeps books shallow, so the
  sort is already cheap. Worse, it touches matching-order correctness — the
  price-time-proof and the `broken-price-time-priority` test vector — for a gain
  the network ceiling swallows. **Deliberately not done.**
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
