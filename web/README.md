# Web

`web/console-ui/index.html` is the browser-facing control console served by
`services/console-api` at `/`. It supports ZIP upload, run configuration, run
startup, lifecycle polling, leaderboard display, and artifact links through one
gateway API.

`web/leaderboard-ui/index.html` is a local benchmark console served by
`services/leaderboard-api` at `/`. It shows live leaderboard rows, aggregate
benchmark KPIs, pipeline evidence, score breakdowns, and top-run details from
the leaderboard API/WebSocket stream.

Run the full interactive console:

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
