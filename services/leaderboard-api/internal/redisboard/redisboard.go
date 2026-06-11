// Package redisboard serves the live leaderboard straight from Redis.
//
// The data plane writes two things the score-engine owns:
//   - ZSET  leaderboard:global        member=team_id  score=final_score
//   - HASH  team:{team_id}:scorecard  full ScoreJson (percentiles + breakdown)
//
// This board polls those keys on a short interval, builds a ranked snapshot,
// and pushes it to WebSocket subscribers whenever it changes. It exposes the
// same surface (List / Subscribe / Upsert) as the file-backed board so the
// HTTP handler is backend-agnostic.
package redisboard

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"leaderboard-api/internal/board"
)

const globalZSet = "leaderboard:global"

// Board is a Redis-backed, poll-driven leaderboard.
type Board struct {
	rdb      *redis.Client
	interval time.Duration

	mu          sync.Mutex
	snapshot    []board.Entry
	lastPayload []byte
	subscribers map[chan []byte]struct{}
	// lastRefresh is the wall-clock time of the last SUCCESSFUL read from
	// Redis. It is the readiness signal: if the poller can't reach Redis the
	// snapshot freezes but lastRefresh stops advancing, so /ready can report
	// the data as stale (liveness stays green; readiness flips) instead of
	// serving frozen data as if it were live.
	lastRefresh time.Time

	cancel context.CancelFunc
}

// New connects to Redis and starts the background poller. The caller owns the
// returned Board and should Close it on shutdown.
func New(url string, interval time.Duration) (*Board, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	b := &Board{
		rdb:         rdb,
		interval:    interval,
		subscribers: make(map[chan []byte]struct{}),
	}

	pollCtx, pollCancel := context.WithCancel(context.Background())
	b.cancel = pollCancel
	// Prime once synchronously so the first List/Subscribe is non-empty.
	b.refresh(pollCtx)
	go b.loop(pollCtx)
	return b, nil
}

func (b *Board) loop(ctx context.Context) {
	t := time.NewTicker(b.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.refresh(ctx)
		}
	}
}

// refresh reads the current ranking from Redis and broadcasts if it changed.
func (b *Board) refresh(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	ranked, err := b.rdb.ZRevRangeWithScores(rctx, globalZSet, 0, -1).Result()
	if err != nil {
		return
	}

	entries := make([]board.Entry, 0, len(ranked))
	for _, z := range ranked {
		team, _ := z.Member.(string)
		entry := board.Entry{TeamID: team, Score: z.Score}
		if card, err := b.rdb.HGetAll(rctx, scorecardKey(team)).Result(); err == nil && len(card) > 0 {
			applyScorecard(&entry, card)
		}
		if entry.UpdatedAt.IsZero() {
			entry.UpdatedAt = time.Now()
		}
		entries = append(entries, entry)
	}

	payload, err := json.Marshal(entries)
	if err != nil {
		return
	}

	b.mu.Lock()
	changed := !bytesEqual(payload, b.lastPayload)
	b.snapshot = entries
	b.lastPayload = payload
	b.lastRefresh = time.Now() // only reached on a successful Redis read
	var subs []chan []byte
	if changed {
		subs = make([]chan []byte, 0, len(b.subscribers))
		for ch := range b.subscribers {
			subs = append(subs, ch)
		}
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- payload:
		default:
		}
	}
}

// List returns the latest ranked snapshot.
func (b *Board) List() []board.Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]board.Entry, len(b.snapshot))
	copy(out, b.snapshot)
	return out
}

// Subscribe registers a WebSocket subscriber and immediately seeds it with the
// current snapshot.
func (b *Board) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 8)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	payload := b.lastPayload
	if payload == nil {
		payload, _ = json.Marshal([]board.Entry{})
	}
	b.mu.Unlock()

	ch <- payload
	// cancel only unregisters; it must NOT close(ch). refresh() snapshots the
	// subscriber list under the lock but sends after unlock, so a close here
	// races that send (send on closed channel would kill the background
	// refresh goroutine). The unregistered channel is garbage collected.
	cancel := func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}
	return ch, cancel
}

// Upsert writes an entry into Redis (ZSET + scorecard) so manual POSTs and the
// score-engine share one source of truth, then refreshes the snapshot.
func (b *Board) Upsert(entry board.Entry) ([]board.Entry, error) {
	if entry.TeamID == "" {
		entry.TeamID = entry.RunID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pipe := b.rdb.TxPipeline()
	pipe.ZAdd(ctx, globalZSet, redis.Z{Member: entry.TeamID, Score: entry.Score})
	pipe.HSet(ctx, scorecardKey(entry.TeamID),
		"run_id", entry.RunID,
		"team_id", entry.TeamID,
		"valid", boolToInt(entry.Valid),
		"score", entry.Score,
		"p50_ms", entry.P50MS,
		"p90_ms", entry.P90MS,
		"p99_ms", entry.P99MS,
		"p999_ms", entry.P999MS,
		"tps", entry.TPS,
		"peak_tps", entry.PeakTPS,
		"latency_score", entry.LatencyScore,
		"throughput_score", entry.ThroughputScore,
		"stability_score", entry.StabilityScore,
		"resource_score", entry.ResourceScore,
		"orders_sent", entry.OrdersSent,
		"acks_received", entry.AcksReceived,
		"timeouts", entry.Timeouts,
		"failure_reason", entry.FailureReason,
	)
	if _, err := pipe.Exec(ctx); err != nil {
		return b.List(), err
	}
	b.refresh(ctx)
	return b.List(), nil
}

// LiveRunMetrics returns the ingester's in-flight counters for a run, used by
// the /runs/{id}/live endpoint to show progress before a run is scored.
func (b *Board) LiveRunMetrics(runID string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return b.rdb.HGetAll(ctx, "run:"+runID+":metrics").Result()
}

// RunTimeseries returns the per-second latency/throughput series for a run as a
// JSON array — `[{"t":0,"tps":1500,"p50_ms":6.6,"p99_ms":31.3}, ...]`. The
// score-engine computes it from the authoritative Timescale rows and caches it
// in `run:{id}:latency_series`, so the UI can chart how latency and TPS moved
// (and degraded) over the run. Returns `[]` if the run hasn't been scored yet.
func (b *Board) RunTimeseries(runID string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := b.rdb.Get(ctx, "run:"+runID+":latency_series").Result()
	if err == redis.Nil {
		return json.RawMessage("[]"), nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(s), nil
}

// Ready reports whether the live data is fresh enough to be trusted. It returns
// false (and a detail map) when the background poller hasn't completed a
// successful Redis read within 3 poll intervals — i.e. Redis is down or the
// poller is wedged — so a k8s readinessProbe can pull this pod from the load
// balancer while /health (liveness) stays green. The detail is also useful for
// a human or a UI staleness badge.
func (b *Board) Ready() (bool, map[string]any) {
	b.mu.Lock()
	last := b.lastRefresh
	b.mu.Unlock()

	maxStale := 3 * b.interval
	if last.IsZero() {
		return false, map[string]any{
			"ready":  false,
			"reason": "no successful redis refresh yet",
		}
	}
	age := time.Since(last)
	ready := age <= maxStale
	detail := map[string]any{
		"ready":        ready,
		"last_refresh": last.UTC().Format(time.RFC3339Nano),
		"age_ms":       age.Milliseconds(),
		"max_stale_ms": maxStale.Milliseconds(),
	}
	if !ready {
		detail["reason"] = "redis unreachable: serving stale snapshot"
	}
	return ready, detail
}

// StaleAgeMS returns how long ago (ms) the live snapshot was last refreshed
// from Redis, or -1 if it has never refreshed. Used for a non-breaking
// freshness header on /leaderboard.
func (b *Board) StaleAgeMS() int64 {
	b.mu.Lock()
	last := b.lastRefresh
	b.mu.Unlock()
	if last.IsZero() {
		return -1
	}
	return time.Since(last).Milliseconds()
}

// Close stops the poller and releases the Redis client.
func (b *Board) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return b.rdb.Close()
}

func scorecardKey(team string) string {
	return "team:" + team + ":scorecard"
}

func applyScorecard(e *board.Entry, card map[string]string) {
	e.RunID = card["run_id"]
	if t := card["team_id"]; t != "" {
		e.TeamID = t
	}
	e.Valid = card["valid"] == "1"
	e.FailureReason = card["failure_reason"]
	e.Score = atof(card["score"], e.Score)
	e.LatencyScore = atof(card["latency_score"], 0)
	e.ThroughputScore = atof(card["throughput_score"], 0)
	e.StabilityScore = atof(card["stability_score"], 0)
	e.ResourceScore = atof(card["resource_score"], 0)
	e.P50MS = atof(card["p50_ms"], 0)
	e.P90MS = atof(card["p90_ms"], 0)
	e.P99MS = atof(card["p99_ms"], 0)
	e.P999MS = atof(card["p999_ms"], 0)
	e.TPS = atof(card["tps"], 0)
	e.PeakTPS = atof(card["peak_tps"], 0)
	e.OrdersSent = atoi(card["orders_sent"])
	e.AcksReceived = atoi(card["acks_received"])
	e.Timeouts = atoi(card["timeouts"])
}

func atof(s string, def float64) float64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func atoi(s string) int {
	v, _ := strconv.ParseInt(s, 10, 64)
	return int(v)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
