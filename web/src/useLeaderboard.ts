import { useEffect, useRef, useState } from "react";
import type { ConnectionState, LeaderboardEntry } from "./types";

function resolveWsUrl(): string {
  // Explicit override wins (e.g. VITE_LEADERBOARD_WS=ws://host:9500/ws).
  const explicit = import.meta.env.VITE_LEADERBOARD_WS as string | undefined;
  if (explicit) return explicit;
  // Otherwise same-origin /ws (works behind the Vite dev proxy and in prod
  // when the UI is served next to the API / through an ingress).
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws`;
}

/**
 * Subscribes to the leaderboard-api WebSocket. The server pushes a full ranked
 * snapshot on connect and on every change, so we just replace state. Reconnects
 * automatically with capped backoff.
 */
export function useLeaderboard() {
  const [entries, setEntries] = useState<LeaderboardEntry[]>([]);
  const [state, setState] = useState<ConnectionState>("connecting");
  const [lastUpdate, setLastUpdate] = useState<number | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const attemptRef = useRef(0);

  useEffect(() => {
    let stopped = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    const connect = () => {
      if (stopped) return;
      setState("connecting");
      const ws = new WebSocket(resolveWsUrl());
      wsRef.current = ws;

      ws.onopen = () => {
        attemptRef.current = 0;
        setState("open");
      };
      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data) as LeaderboardEntry[];
          if (Array.isArray(data)) {
            setEntries(data);
            setLastUpdate(Date.now());
          }
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onclose = () => {
        setState("closed");
        if (stopped) return;
        const delay = Math.min(1000 * 2 ** attemptRef.current, 10000);
        attemptRef.current += 1;
        reconnectTimer = setTimeout(connect, delay);
      };
      ws.onerror = () => ws.close();
    };

    connect();
    return () => {
      stopped = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      wsRef.current?.close();
    };
  }, []);

  return { entries, state, lastUpdate };
}
