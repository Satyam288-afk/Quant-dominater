package main

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// refSortSide is the pre-optimization algorithm (the old sortBook comparator,
// applied with sort.SliceStable) kept here as the executable spec: the
// differential test holds insertSorted/removeOrder to element-identical book
// state against it, in both engine modes, after every single operation.
func refSortSide(s []*Order, buySide, broken bool) {
	sort.SliceStable(s, func(i, j int) bool {
		a, b := s[i], s[j]
		if a.Price != b.Price {
			if buySide {
				return a.Price > b.Price
			}
			return a.Price < b.Price
		}
		if a.TsNs != b.TsNs {
			if broken {
				return a.TsNs > b.TsNs
			}
			return a.TsNs < b.TsNs
		}
		return a.InsertSeq < b.InsertSeq
	})
}

// refRemove is the old removeResting linear scan, side-local.
func refRemove(s []*Order, id string) ([]*Order, bool) {
	for i, o := range s {
		if o.ID == id {
			return append(s[:i], s[i+1:]...), true
		}
	}
	return s, false
}

func sameOrders(t *testing.T, what string, op int, got, want []*Order) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("op %d %s: len %d != %d", op, what, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("op %d %s[%d]: got %+v want %+v", op, what, i, *got[i], *want[i])
		}
	}
}

// TestInsertSortedEquivalence drives the optimized book (insertSorted +
// removeOrder + head pops) and the reference book (append + stable sort +
// linear remove) through the same randomized stream with heavy tie pressure
// (few prices, few timestamps, so every comparator level is exercised) and
// requires identical order placement after EVERY operation, in normal and
// broken-price-time-priority mode.
func TestInsertSortedEquivalence(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken=%v", broken), func(t *testing.T) {
			rng := rand.New(rand.NewSource(42))
			book := &Book{}
			var refBuys, refSells []*Order
			var resting []*Order
			var seq uint64

			for op := 0; op < 20000; op++ {
				r := rng.Intn(10)
				switch {
				case r < 6 || len(resting) == 0: // insert a resting order
					seq++
					buySide := rng.Intn(2) == 0
					o := &Order{
						ID:        fmt.Sprintf("o%d", seq),
						Symbol:    "T",
						Price:     int64(100 + rng.Intn(5)),
						Qty:       1,
						TsNs:      uint64(1000 + rng.Intn(3)),
						InsertSeq: seq,
					}
					if buySide {
						o.Side = "BUY"
						insertSorted(&book.buys, o, true, broken)
						refBuys = append(refBuys, o)
						refSortSide(refBuys, true, broken)
					} else {
						o.Side = "SELL"
						insertSorted(&book.sells, o, false, broken)
						refSells = append(refSells, o)
						refSortSide(refSells, false, broken)
					}
					resting = append(resting, o)
				case r < 8: // cancel a random resting order
					k := rng.Intn(len(resting))
					ord := resting[k]
					got := removeOrder(book, ord, broken)
					var want bool
					if ord.Side == "BUY" {
						refBuys, want = refRemove(refBuys, ord.ID)
					} else {
						refSells, want = refRemove(refSells, ord.ID)
					}
					if got != want {
						t.Fatalf("op %d cancel %s: got %v want %v", op, ord.ID, got, want)
					}
					resting = append(resting[:k], resting[k+1:]...)
				default: // pop the head, as a full fill does
					if rng.Intn(2) == 0 && len(book.buys) > 0 {
						head := book.buys[0]
						book.buys = book.buys[1:]
						refBuys = refBuys[1:]
						dropResting(&resting, head)
					} else if len(book.sells) > 0 {
						head := book.sells[0]
						book.sells = book.sells[1:]
						refSells = refSells[1:]
						dropResting(&resting, head)
					}
				}
				sameOrders(t, "buys", op, book.buys, refBuys)
				sameOrders(t, "sells", op, book.sells, refSells)
			}

			// A second remove of the same order must be a clean not-found.
			if len(book.buys) > 0 {
				ord := book.buys[0]
				if !removeOrder(book, ord, broken) {
					t.Fatal("first removeOrder of a resting order returned false")
				}
				if removeOrder(book, ord, broken) {
					t.Fatal("second removeOrder of the same order returned true")
				}
			}
		})
	}
}

func dropResting(resting *[]*Order, ord *Order) {
	s := *resting
	for i, o := range s {
		if o == ord {
			*resting = append(s[:i], s[i+1:]...)
			return
		}
	}
}

// Honest A/B of the change at a steady depth of 256 resting orders per side:
// one resting insert + its cancel per iteration, optimized path vs the old
// append+stable-sort insert and linear-scan remove.
func BenchmarkBookInsertCancel(b *testing.B) {
	book, orders := benchBook(256)
	seq := uint64(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq++
		o := &Order{ID: "bench", Symbol: "T", Side: "BUY", Price: 102, Qty: 1, TsNs: 999, InsertSeq: seq}
		insertSorted(&book.buys, o, true, false)
		removeOrder(book, o, false)
	}
	_ = orders
}

func BenchmarkBookInsertCancelOld(b *testing.B) {
	book, orders := benchBook(256)
	seq := uint64(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq++
		o := &Order{ID: "bench", Symbol: "T", Side: "BUY", Price: 102, Qty: 1, TsNs: 999, InsertSeq: seq}
		book.buys = append(book.buys, o)
		refSortSide(book.buys, true, false)
		book.buys, _ = refRemove(book.buys, o.ID)
	}
	_ = orders
}

func benchBook(depth int) (*Book, []*Order) {
	book := &Book{}
	orders := make([]*Order, 0, depth)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < depth; i++ {
		o := &Order{
			ID:        fmt.Sprintf("seed%d", i),
			Symbol:    "T",
			Side:      "BUY",
			Price:     int64(90 + rng.Intn(20)),
			Qty:       1,
			TsNs:      uint64(1000 + rng.Intn(5)),
			InsertSeq: uint64(i + 1),
		}
		insertSorted(&book.buys, o, true, false)
		orders = append(orders, o)
	}
	return book, orders
}
