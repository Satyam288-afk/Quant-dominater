import { useMemo } from "react";
import { useLeaderboard } from "./useLeaderboard";
import { useTimeseries } from "./useTimeseries";
import type { LeaderboardEntry, TimeseriesPoint } from "./types";

const fmt = (v: number | undefined, digits = 2, suffix = "") =>
  v === undefined || Number.isNaN(v) ? "—" : `${v.toFixed(digits)}${suffix}`;

// Score components are already on a 0..100 scale (see bench-core score formula).
const pct = (v: number | undefined) =>
  v === undefined ? 0 : Math.max(0, Math.min(100, v));

function StatusPill({ state }: { state: string }) {
  return (
    <span className={`pill pill-${state}`}>
      <span className="dot" />
      {state === "open" ? "LIVE" : state === "connecting" ? "CONNECTING" : "OFFLINE"}
    </span>
  );
}

function Breakdown({ entry }: { entry: LeaderboardEntry }) {
  const parts = [
    { key: "lat", label: "Latency", v: entry.latency_score },
    { key: "tps", label: "Throughput", v: entry.throughput_score },
    { key: "stab", label: "Stability", v: entry.stability_score },
    { key: "res", label: "Resource", v: entry.resource_score },
  ];
  return (
    <div className="breakdown">
      {parts.map((p) => (
        <div className="bar-row" key={p.key} title={`${p.label}: ${fmt(p.v, 2)}`}>
          <span className="bar-label">{p.label}</span>
          <span className="bar-track">
            <span className={`bar-fill bar-${p.key}`} style={{ width: `${pct(p.v)}%` }} />
          </span>
        </div>
      ))}
    </div>
  );
}

// Compact SVG sparkline of p99 latency over the run, so you can *see* latency
// move (and degrade) second-by-second rather than reading a single number.
function Sparkline({ points }: { points?: TimeseriesPoint[] }) {
  if (!points || points.length < 2) return <span className="peak">—</span>;
  const w = 96;
  const h = 26;
  const pad = 3;
  const xs = points.map((p) => p.t);
  const ys = points.map((p) => p.p99_ms);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const maxY = Math.max(...ys, 1);
  const sx = (x: number) =>
    pad + ((x - minX) / Math.max(1, maxX - minX)) * (w - 2 * pad);
  const sy = (y: number) => h - pad - (y / maxY) * (h - 2 * pad);
  const d = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${sx(p.t).toFixed(1)},${sy(p.p99_ms).toFixed(1)}`)
    .join(" ");
  const last = ys[ys.length - 1];
  return (
    <span className="spark" title={`p99 latency per second · peak ${maxY.toFixed(1)}ms`}>
      <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`}>
        <path d={d} fill="none" stroke="currentColor" strokeWidth="1.5" />
      </svg>
      <span className="spark-val">{last.toFixed(0)}ms</span>
    </span>
  );
}

function Row({
  entry,
  rank,
  points,
}: {
  entry: LeaderboardEntry;
  rank: number;
  points?: TimeseriesPoint[];
}) {
  return (
    <tr className={rank <= 3 ? `top top-${rank}` : ""}>
      <td className="rank">{rank}</td>
      <td className="team">
        <div className="team-name">{entry.team_id}</div>
        <div className="run-id">{entry.run_id || "—"}</div>
      </td>
      <td className="score">{fmt(entry.score, 1)}</td>
      <td>
        {entry.valid ? (
          <span className="badge ok">VALID</span>
        ) : (
          <span className="badge bad" title={entry.failure_reason}>
            {entry.failure_reason || "INVALID"}
          </span>
        )}
      </td>
      <td className="num">{fmt(entry.p50_ms, 2, "ms")}</td>
      <td className="num">{fmt(entry.p90_ms, 2, "ms")}</td>
      <td className="num">{fmt(entry.p99_ms, 2, "ms")}</td>
      <td className="num" title="average / peak (max acks in any 1s window)">
        {fmt(entry.tps, 0)}
        {entry.peak_tps ? <span className="peak"> / {fmt(entry.peak_tps, 0)}</span> : null}
      </td>
      <td className="spark-cell">
        <Sparkline points={points} />
      </td>
      <td className="bd-cell">
        <Breakdown entry={entry} />
      </td>
    </tr>
  );
}

export function App() {
  const { entries, state, lastUpdate } = useLeaderboard();

  const { ranked, validCount, peakTps } = useMemo(() => {
    const ranked = [...entries].sort((a, b) => b.score - a.score);
    return {
      ranked,
      validCount: entries.filter((e) => e.valid).length,
      // Real peak (max acks in any 1s window), falling back to average TPS for
      // entries scored before peak telemetry was available.
      peakTps: entries.reduce((m, e) => Math.max(m, e.peak_tps ?? e.tps ?? 0), 0),
    };
  }, [entries]);

  // Per-run latency/throughput series for the sparklines.
  const series = useTimeseries(ranked.map((e) => e.run_id));

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">◆</span>
          <div>
            <h1>IICPC Benchmark</h1>
            <p>Distributed trading-engine stress test · live leaderboard</p>
          </div>
        </div>
        <div className="meta">
          <StatusPill state={state} />
          <div className="stat">
            <span className="stat-val">{entries.length}</span>
            <span className="stat-lbl">teams</span>
          </div>
          <div className="stat">
            <span className="stat-val">{validCount}</span>
            <span className="stat-lbl">valid</span>
          </div>
          <div className="stat">
            <span className="stat-val">{peakTps ? peakTps.toFixed(0) : "—"}</span>
            <span className="stat-lbl">peak tps</span>
          </div>
        </div>
      </header>

      <main>
        {ranked.length === 0 ? (
          <div className="empty">
            {state === "open"
              ? "Waiting for the first scored run…"
              : "Connecting to the live data plane…"}
          </div>
        ) : (
          <table className="board">
            <thead>
              <tr>
                <th>#</th>
                <th>Team</th>
                <th>Score</th>
                <th>Correctness</th>
                <th>p50</th>
                <th>p90</th>
                <th>p99</th>
                <th>TPS avg/peak</th>
                <th>p99 trend</th>
                <th>Score breakdown</th>
              </tr>
            </thead>
            <tbody>
              {ranked.map((e, i) => (
                <Row
                  entry={e}
                  rank={i + 1}
                  points={series[e.run_id]}
                  key={e.team_id}
                />
              ))}
            </tbody>
          </table>
        )}
      </main>

      <footer>
        <span>
          {lastUpdate
            ? `updated ${new Date(lastUpdate).toLocaleTimeString()}`
            : "no updates yet"}
        </span>
        <span>price-time-priority validated · composite = 0.40·latency + 0.30·throughput + 0.20·stability + 0.10·resource, gated by correctness</span>
      </footer>
    </div>
  );
}
