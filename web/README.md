# Web — Consoles & Live Leaderboard UI

There are two complementary front-ends in this repo — a React/Vite app and a
pair of dependency-free static HTML consoles. They serve different needs and
coexist.

## React app (Vite) — real-time leaderboard

Real-time leaderboard for the IICPC benchmark platform. React + TypeScript + Vite.
It opens a WebSocket to the `leaderboard-api`, receives a full ranked snapshot on
connect and on every change, and renders teams ranked by composite score with
latency percentiles (p50/p90/p99), TPS, correctness, and a live score breakdown
(latency / throughput / stability / resource). Auto-reconnects with backoff.

### Develop

```bash
cd web
npm install
# start the API first (file or redis backend):
#   cd ../services/leaderboard-api && LEADERBOARD_BACKEND=redis REDIS_URL=redis://localhost:56379/ go run .
npm run dev   # http://localhost:5173, proxies /leaderboard, /runs, /ws to :9500
```

Point at an API on another host:

```bash
LEADERBOARD_API_URL=http://my-host:9500 npm run dev
# or, for a prod build talking to an absolute WS endpoint:
VITE_LEADERBOARD_WS=wss://leaderboard.example.com/ws npm run build
```

### Build

```bash
npm run build     # tsc type-check + vite build -> dist/
npm run preview   # serve the production build
```

### Data contract

`GET /leaderboard` and the `/ws` stream both return `LeaderboardEntry[]`
(see `src/types.ts`), sorted by `score` descending. In the platform these are
written by the score-engine into Redis (`leaderboard:global` + per-team
scorecards) and served by the leaderboard-api in `redis` backend mode.

## Static HTML consoles (served by the Go services)

`web/console-ui/index.html` is the browser-facing control console served by
`services/console-api` at `/`. It drives the whole pipeline through one gateway
API: engine upload, run configuration, run start, lifecycle polling, leaderboard
display, and artifact links.

`web/leaderboard-ui/index.html` is a local benchmark console served by
`services/leaderboard-api` at `/`. It shows live leaderboard rows, aggregate
benchmark KPIs, pipeline evidence, score breakdowns, and top-run details from
the leaderboard API/WebSocket stream.

Run the full interactive console stack:

```bash
make console-stack
```

Open:

```text
http://localhost:9700/
```

Run the leaderboard-only view:

```bash
make leaderboard-api
```

Open:

```text
http://localhost:9500/
```

## Submitting an engine — upload SOURCE, not a compiled binary

The control console expects a **ZIP of your engine's source code**, not a
precompiled binary. The platform compiles the upload itself inside the sandbox
and then runs it — that is the point of the evaluator, and it keeps the
submission honest (the judges build and run exactly what you shipped).

For the bundled Go reference engine (`examples/stub-engine`), a valid source ZIP
contains just:

```text
go.mod
go.sum
main.go
disruptor.go
Dockerfile
```

Two consequences worth knowing:

- **Do not zip the build output.** `examples/stub-engine/` also holds a
  `stub-engine` binary (~9.5 MB) left over from local builds. Zipping the whole
  folder produces a multi-MB archive; a source-only ZIP is ~16 KB. The big
  archive is also slow to upload through the gateway and can hit the upload
  timeout over a remote connection — so always ship source only.
- **The sandbox builds it.** In the deploy image the evaluator runs in local
  mode (`SANDBOX_ALLOW_UNSAFE_LOCAL=1`) with a Go toolchain and a pre-warmed
  module cache, so the run-time `go build` of your engine resolves offline and
  fast. In production this same step runs inside a network-isolated Docker
  sandbox.

## Deploy (single container)

`infra/deploy/` packages the whole console stack into one image: the four
backend services bind to container-localhost and `console-api` is exposed on
`$PORT` (default `8080`). The evaluator builds + runs the uploaded engine in
local mode. See `infra/deploy/Dockerfile` plus the `render.yaml` / `fly.toml`
blueprints. Auth is fail-closed (`REQUIRE_AUTH=1`); a `SERVICE_AUTH_TOKEN` is
generated at start if one is not supplied.
