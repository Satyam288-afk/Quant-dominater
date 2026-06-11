import { useEffect, useState } from "react";
import type { TimeseriesPoint } from "./types";

// Fetches and caches the per-second latency/throughput series for each run id
// (GET /runs/{id}/timeseries). Refetches whenever the set of run ids changes —
// e.g. when a new run is scored — so the leaderboard's latency sparklines stay
// in sync with the live board.
export function useTimeseries(
  runIds: string[],
): Record<string, TimeseriesPoint[]> {
  const [series, setSeries] = useState<Record<string, TimeseriesPoint[]>>({});
  const key = runIds.filter(Boolean).sort().join(",");

  useEffect(() => {
    if (!key) return;
    let cancelled = false;
    const ids = key.split(",");
    Promise.all(
      ids.map((id) =>
        fetch(`/runs/${encodeURIComponent(id)}/timeseries`)
          .then((r) => (r.ok ? r.json() : { points: [] }))
          .then(
            (d) => [id, (d.points ?? []) as TimeseriesPoint[]] as const,
          )
          .catch(() => [id, [] as TimeseriesPoint[]] as const),
      ),
    ).then((pairs) => {
      if (!cancelled) setSeries(Object.fromEntries(pairs));
    });
    return () => {
      cancelled = true;
    };
  }, [key]);

  return series;
}
