package main

// Disruptor-mode matching engine (opt-in via --engine disruptor). An LMAX-style
// pipeline: many WS goroutines (producers) publish orders into a per-shard
// LOCK-FREE MPSC ring buffer; a single matcher goroutine per shard consumes and
// matches with NO locks (single consumer ⇒ no contention on the book); results
// fan out to a small pool of output writers. Symbols hash to a fixed shard, so
// a symbol's orders are always handled by one matcher in FIFO order — which is
// exactly what correctness needs (engine_seq order == processing order per
// symbol; the validator replays per symbol in that order). The default engine
// stays the proven sharded-mutex one; this is a parallel implementation for an
// A/B and the architecture credential.

import (
	"hash/fnv"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	disruptorShards  = 32
	disruptorRingLog = 14 // 16384 slots per shard
	disruptorOutputs = 4
)

type reqKind uint8

const (
	reqNew reqKind = iota
	reqCancel
)

type orderReq struct {
	kind   reqKind
	order  NewOrder
	cancel CancelOrder
	symbol string // resolved symbol (cancels carry it so the matcher needn't scan)
	client *Client
}

// ring is a bounded lock-free multi-producer / single-consumer queue. Each slot
// carries a sequence number; a producer claims a slot by atomically advancing
// the producer cursor, waits until the slot is free for its round, writes, then
// publishes by bumping the slot sequence. The single consumer waits for the
// published sequence, reads, then frees the slot one buffer-length ahead.
type ring struct {
	mask  uint64
	slots []ringSlot
	prod  atomic.Uint64
	_pad  [56]byte // keep the producer cursor off the consumer's cache line
	cons  uint64   // single consumer: plain field, ordering via slot.seq
}

type ringSlot struct {
	seq  atomic.Uint64
	data orderReq
}

func newRing(log uint64) *ring {
	size := uint64(1) << log
	r := &ring{mask: size - 1, slots: make([]ringSlot, size)}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	return r
}

func (r *ring) publish(item orderReq) {
	pos := r.prod.Add(1) - 1
	s := &r.slots[pos&r.mask]
	for spins := 0; s.seq.Load() != pos; spins++ {
		backoff(spins)
	}
	s.data = item
	s.seq.Store(pos + 1) // publish (release): consumer may now read pos
}

func (r *ring) consume() orderReq {
	pos := r.cons
	s := &r.slots[pos&r.mask]
	for spins := 0; s.seq.Load() != pos+1; spins++ {
		backoff(spins)
	}
	item := s.data
	s.data = orderReq{} // drop the *Client reference so it can be GC'd
	r.cons = pos + 1
	s.seq.Store(pos + r.mask + 1) // free for the producer one buffer ahead
	return item
}

// backoff is a busy-spin wait strategy: an active shard's matcher gets its next
// order during the hot-spin/yield tiers and stays on-core for lowest latency;
// only a genuinely idle shard reaches the sleep tier and yields its core. (We
// tried a channel-park "blocking" strategy — it cut idle CPU but added wakeup
// latency to active matchers, the classic LMAX BusySpin-vs-Blocking trade-off,
// so we kept busy-spin since latency is the goal here.)
func backoff(spins int) {
	switch {
	case spins < 64:
		// hot spin
	case spins < 1024:
		runtime.Gosched()
	default:
		time.Sleep(20 * time.Microsecond)
	}
}

type dShard struct {
	idx   uint64
	ring  *ring
	books map[string]*Book
}

type DisruptorEngine struct {
	shards    []*dShard
	engineSeq atomic.Uint64
	index     sync.Map // orderID(string) -> symbol(string), for cancel routing
	mode      string
	out       chan outMsg
}

type outMsg struct {
	client  *Client
	payload any
}

func NewDisruptorEngine(mode string) *DisruptorEngine {
	de := &DisruptorEngine{
		mode: mode,
		out:  make(chan outMsg, 1<<16),
	}
	de.shards = make([]*dShard, disruptorShards)
	for i := range de.shards {
		de.shards[i] = &dShard{
			idx:   uint64(i),
			ring:  newRing(disruptorRingLog),
			books: make(map[string]*Book),
		}
	}
	return de
}

// Start launches the per-shard matcher goroutines and the output writers.
func (de *DisruptorEngine) Start() {
	for _, sh := range de.shards {
		go de.matchLoop(sh)
	}
	for i := 0; i < disruptorOutputs; i++ {
		go de.outputLoop()
	}
}

func (de *DisruptorEngine) nextSeq() uint64 { return de.engineSeq.Add(1) }

func (de *DisruptorEngine) shardFor(symbol string) *dShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(symbol))
	return de.shards[uint64(h.Sum32())%disruptorShards]
}

// SubmitNew / SubmitCancel are called by producer (WS) goroutines.
func (de *DisruptorEngine) SubmitNew(o NewOrder, client *Client) {
	symbol := o.Symbol
	if symbol == "" {
		symbol = "DEFAULT"
	}
	de.shardFor(symbol).ring.publish(orderReq{kind: reqNew, order: o, symbol: symbol, client: client})
}

func (de *DisruptorEngine) SubmitCancel(c CancelOrder, client *Client) {
	if v, ok := de.index.Load(c.OrigClientOrderID); ok {
		symbol := v.(string)
		de.shardFor(symbol).ring.publish(orderReq{kind: reqCancel, cancel: c, symbol: symbol, client: client})
		return
	}
	// Unknown/already-gone order: a no-op cancel. Sequence it and reply directly
	// — its position can't affect any book, so it needn't go through a matcher.
	de.out <- outMsg{client, Ack{
		Type: "ack", ClientOrderID: c.ClientOrderID, Status: "not_found",
		EngineSeq: de.nextSeq(), TsNs: nowNs(),
	}}
}

// Reject sequences and emits a rejection (bad JSON / unknown type).
func (de *DisruptorEngine) Reject(client *Client, reason string) {
	de.out <- outMsg{client, Ack{
		Type: "ack", Status: "rejected", Reason: reason,
		EngineSeq: de.nextSeq(), TsNs: nowNs(),
	}}
}

func (de *DisruptorEngine) outputLoop() {
	for msg := range de.out {
		if msg.client == nil {
			continue
		}
		if err := msg.client.SendJSON(msg.payload); err != nil {
			// connection gone — drop; the run's correctness comes from the
			// validator over the bot fleet's own logs.
			_ = err
		}
	}
}

func (de *DisruptorEngine) matchLoop(sh *dShard) {
	for {
		req := sh.ring.consume()
		switch req.kind {
		case reqNew:
			ack, deliveries := de.processNew(sh, req.order, req.client)
			de.out <- outMsg{req.client, ack}
			for _, d := range deliveries {
				for _, c := range d.Recipients {
					if c != nil {
						de.out <- outMsg{c, d.Fill}
					}
				}
			}
		case reqCancel:
			de.out <- outMsg{req.client, de.processCancel(sh, req.symbol, req.cancel)}
		}
	}
}

// processNew/processCancel run ONLY on the shard's single matcher goroutine, so
// they need no locks. The logic mirrors the mutex engine exactly.
func (de *DisruptorEngine) processNew(sh *dShard, in NewOrder, owner *Client) (Ack, []FillDelivery) {
	side := strings.ToUpper(in.Side)
	orderType := strings.ToUpper(in.OrderType)
	if orderType == "" {
		orderType = "LIMIT"
	}
	if in.ClientOrderID == "" || in.Qty <= 0 || (side != "BUY" && side != "SELL") || (orderType == "LIMIT" && in.Price <= 0) {
		return Ack{Type: "ack", ClientOrderID: in.ClientOrderID, Status: "rejected", Reason: "invalid_order", EngineSeq: de.nextSeq(), TsNs: nowNs()}, nil
	}

	ack := Ack{Type: "ack", ClientOrderID: in.ClientOrderID, Status: "accepted", EngineSeq: de.nextSeq(), TsNs: nowNs()}

	symbol := in.Symbol
	if symbol == "" {
		symbol = "DEFAULT"
	}
	book := sh.books[symbol]
	if book == nil {
		book = &Book{}
		sh.books[symbol] = book
	}
	book.insertSeq++
	active := &Order{ID: in.ClientOrderID, Symbol: symbol, Side: side, Price: in.Price, Qty: in.Qty, TsNs: in.TsNs, InsertSeq: book.insertSeq, Owner: owner}

	var deliveries []FillDelivery
	if side == "BUY" {
		deliveries = de.matchBuy(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.buys = append(book.buys, active)
			de.sortBook(book)
			de.index.Store(active.ID, symbol)
		}
	} else {
		deliveries = de.matchSell(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.sells = append(book.sells, active)
			de.sortBook(book)
			de.index.Store(active.ID, symbol)
		}
	}
	return ack, deliveries
}

func (de *DisruptorEngine) processCancel(sh *dShard, symbol string, in CancelOrder) Ack {
	ack := Ack{Type: "ack", ClientOrderID: in.ClientOrderID, Status: "not_found", EngineSeq: de.nextSeq(), TsNs: nowNs()}
	if book := sh.books[symbol]; book != nil && removeResting(book, in.OrigClientOrderID) {
		ack.Status = "canceled"
		de.index.Delete(in.OrigClientOrderID)
	}
	return ack
}

func (de *DisruptorEngine) matchBuy(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	activeOwner := active.Owner // hoist out of the loop (immutable within the call)
	for active.Qty > 0 && len(book.sells) > 0 {
		resting := book.sells[0]
		if !market && active.Price < resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		deliveries = append(deliveries, FillDelivery{
			Fill:       Fill{Symbol: resting.Symbol, Type: "fill", BuyOrderID: active.ID, SellOrderID: resting.ID, Price: resting.Price, Qty: qty, EngineSeq: de.nextSeq()},
			Recipients: uniqueClients(activeOwner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			de.index.Delete(resting.ID)
			book.sells = book.sells[1:]
		}
	}
	return deliveries
}

func (de *DisruptorEngine) matchSell(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	activeOwner := active.Owner // hoist out of the loop (immutable within the call)
	for active.Qty > 0 && len(book.buys) > 0 {
		resting := book.buys[0]
		if !market && active.Price > resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		deliveries = append(deliveries, FillDelivery{
			Fill:       Fill{Symbol: resting.Symbol, Type: "fill", BuyOrderID: resting.ID, SellOrderID: active.ID, Price: resting.Price, Qty: qty, EngineSeq: de.nextSeq()},
			Recipients: uniqueClients(activeOwner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			de.index.Delete(resting.ID)
			book.buys = book.buys[1:]
		}
	}
	return deliveries
}

func (de *DisruptorEngine) sortBook(book *Book) {
	broken := de.mode == "broken-price-time-priority"
	sort.SliceStable(book.buys, func(i, j int) bool {
		a, b := book.buys[i], book.buys[j]
		if a.Price != b.Price {
			return a.Price > b.Price
		}
		if a.TsNs != b.TsNs {
			if broken {
				return a.TsNs > b.TsNs
			}
			return a.TsNs < b.TsNs
		}
		return a.InsertSeq < b.InsertSeq
	})
	sort.SliceStable(book.sells, func(i, j int) bool {
		a, b := book.sells[i], book.sells[j]
		if a.Price != b.Price {
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
