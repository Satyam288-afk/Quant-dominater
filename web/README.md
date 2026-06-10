# Web — Live Leaderboard UI

Real-time leaderboard for the IICPC benchmark platform. React + TypeScript + Vite.
It opens a WebSocket to the `leaderboard-api`, receives a full ranked snapshot on
connect and on every change, and renders teams ranked by composite score with
latency percentiles (p50/p90/p99), TPS, correctness, and a live score breakdown
(latency / throughput / stability / resource). Auto-reconnects with backoff.

## Develop

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

## Build

```bash
npm run build     # tsc type-check + vite build -> dist/
npm run preview   # serve the production build
```

## Data contract

`GET /leaderboard` and the `/ws` stream both return `LeaderboardEntry[]`
(see `src/types.ts`), sorted by `score` descending. In the platform these are
written by the score-engine into Redis (`leaderboard:global` + per-team
scorecards) and served by the leaderboard-api in `redis` backend mode.
