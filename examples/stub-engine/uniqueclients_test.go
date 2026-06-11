package main

import "testing"

// The return contract uniqueClients must preserve exactly: distinct, non-nil
// recipients in active-then-resting order. The fill-delivery consumer ranges
// over this and is order/duplicate-indifferent, and the validator matches fills
// as a multiset — but we pin the contract anyway so a future edit can't drift.
func TestUniqueClientsContract(t *testing.T) {
	a, b := &Client{}, &Client{}

	if got := uniqueClients(a, b); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("two distinct: want [a b], got %v", got)
	}
	if got := uniqueClients(a, a); len(got) != 1 || got[0] != a {
		t.Fatalf("self-trade: want [a], got %v", got)
	}
	if got := uniqueClients(a, nil); len(got) != 1 || got[0] != a {
		t.Fatalf("nil resting (counterparty on another pod): want [a], got %v", got)
	}
	if got := uniqueClients(nil, b); len(got) != 1 || got[0] != b {
		t.Fatalf("nil active: want [b], got %v", got)
	}
	if got := uniqueClients(nil, nil); len(got) != 0 {
		t.Fatalf("both nil: want empty, got %v", got)
	}
	// Fallback (>2) path still dedups correctly, even though the per-fill call
	// never reaches it.
	if got := uniqueClients(a, b, a, nil, b); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("fallback dedup: want [a b], got %v", got)
	}
}

// The per-fill hot-path call allocates only the single result slice the
// FillDelivery must own (1/op). The old map-based dedup also reported 1/op —
// Go stack-allocates the small non-escaping map — so the win is CPU, not allocs
// (see BenchmarkUniqueClients vs ...Old). This test pins that we never regress
// to >1 heap allocation on the hot path.
func TestUniqueClientsAllocations(t *testing.T) {
	a, b := &Client{}, &Client{}
	if n := testing.AllocsPerRun(1000, func() { sinkClients = uniqueClients(a, b) }); n > 1 {
		t.Fatalf("uniqueClients(a,b) allocs/op = %.0f, want <= 1", n)
	}
	if n := testing.AllocsPerRun(1000, func() { sinkClients = uniqueClients(a, a) }); n > 1 {
		t.Fatalf("uniqueClients(a,a) allocs/op = %.0f, want <= 1", n)
	}
}

var sinkClients []*Client

// Reports allocs/op for the report (go test -bench=UniqueClients -benchmem).
func BenchmarkUniqueClients(b *testing.B) {
	x, y := &Client{}, &Client{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkClients = uniqueClients(x, y)
	}
}

// uniqueClientsOld is the pre-optimization implementation, kept ONLY so the
// benchmark can show the per-fill allocation it cost (map + slice) vs the new
// fast path (slice only). Not used by the engine.
func uniqueClientsOld(clients ...*Client) []*Client {
	seen := map[*Client]bool{}
	out := make([]*Client, 0, len(clients))
	for _, client := range clients {
		if client == nil || seen[client] {
			continue
		}
		seen[client] = true
		out = append(out, client)
	}
	return out
}

func BenchmarkUniqueClientsOld(b *testing.B) {
	x, y := &Client{}, &Client{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkClients = uniqueClientsOld(x, y)
	}
}
