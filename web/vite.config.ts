import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, proxy the live API + WebSocket to the leaderboard-api so the app can
// use same-origin relative paths (/leaderboard, /ws). Override the upstream
// with LEADERBOARD_API_URL when the API runs elsewhere.
const apiTarget = process.env.LEADERBOARD_API_URL ?? "http://localhost:9500";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/leaderboard": { target: apiTarget, changeOrigin: true },
      "/runs": { target: apiTarget, changeOrigin: true },
      "/ws": { target: apiTarget, changeOrigin: true, ws: true },
    },
  },
});
