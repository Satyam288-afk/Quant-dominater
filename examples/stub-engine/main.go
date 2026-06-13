package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Hardening limits. A single misbehaving (or hostile) client must not be able
// to pin engine memory or drive a quadratic insert.
const (
	// maxRestingPerSymbol hard-caps resting orders per side per symbol. The
	// front-insert in insertSorted is O(n) under the book lock, so an unbounded
	// book is both an O(n^2) CPU sink and a memory pin; the cap bounds both.
	// Far above any legitimate book depth (the price-time proof rests at most 2
	// per side) so it never changes matching/priority behavior in practice.
	maxRestingPerSymbol = 100_000
	// maxOrderFieldLen bounds attacker-controlled string fields so a single
	// huge accepted order cannot rest and pin memory.
	maxOrderFieldLen = 256
	// maxRESTBody caps the REST /orders request body (mirrors the WS read limit)
	// so a giant POST cannot balloon RSS during decode.
	maxRESTBody = 64 << 10
	// wsReadLimit caps a single inbound WebSocket message.
	wsReadLimit = 64 << 10
)

type BaseMessage struct {
	Type string `json:"type"`
}

type NewOrder struct {
	Type          string `json:"type"`
	RunID         string `json:"run_id"`
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol,omitempty"`
	Side          string `json:"side"`
	Price         int64  `json:"price"`
	Qty           int64  `json:"qty"`
	TsNs          uint64 `json:"ts_ns"`
	OrderType     string `json:"order_type,omitempty"`
}

type CancelOrder struct {
	Type              string `json:"type"`
	RunID             string `json:"run_id"`
	ClientOrderID     string `json:"client_order_id"`
	OrigClientOrderID string `json:"orig_client_order_id"`
	TsNs              uint64 `json:"ts_ns"`
}

// Inbound is the union of the inbound message types, decoded in a single pass.
// pprof showed the old two-step decode (type sniff, then a full re-parse of the
// same bytes) doubled the JSON work on the hot path; one decode + a dispatch on
// Type is exact and halves inbound parse cost.
type Inbound struct {
	Type              string `json:"type"`
	RunID             string `json:"run_id"`
	ClientOrderID     string `json:"client_order_id"`
	Symbol            string `json:"symbol,omitempty"`
	Side              string `json:"side,omitempty"`
	Price             int64  `json:"price,omitempty"`
	Qty               int64  `json:"qty,omitempty"`
	TsNs              uint64 `json:"ts_ns"`
	OrderType         string `json:"order_type,omitempty"`
	OrigClientOrderID string `json:"orig_client_order_id,omitempty"`
}

func (in *Inbound) asNewOrder() NewOrder {
	return NewOrder{
		Type: in.Type, RunID: in.RunID, ClientOrderID: in.ClientOrderID,
		Symbol: in.Symbol, Side: in.Side, Price: in.Price, Qty: in.Qty,
		TsNs: in.TsNs, OrderType: in.OrderType,
	}
}

func (in *Inbound) asCancel() CancelOrder {
	return CancelOrder{
		Type: in.Type, RunID: in.RunID, ClientOrderID: in.ClientOrderID,
		OrigClientOrderID: in.OrigClientOrderID, TsNs: in.TsNs,
	}
}

type Ack struct {
	Type          string `json:"type"`
	ClientOrderID string `json:"client_order_id"`
	Status        string `json:"status"`
	EngineSeq     uint64 `json:"engine_seq"`
	TsNs          uint64 `json:"ts_ns"`
	Reason        string `json:"reason,omitempty"`
}

type Fill struct {
	Symbol      string `json:"symbol,omitempty"`
	Type        string `json:"type"`
	BuyOrderID  string `json:"buy_order_id"`
	SellOrderID string `json:"sell_order_id"`
	Price       int64  `json:"price"`
	Qty         int64  `json:"qty"`
	EngineSeq   uint64 `json:"engine_seq"`
}

type Order struct {
	ID        string
	Symbol    string
	Side      string
	Price     int64
	Qty       int64
	TsNs      uint64
	InsertSeq uint64
	Owner     *Client
}

type Book struct {
	mu        sync.Mutex // per-symbol lock: different symbols match concurrently
	insertSeq uint64     // per-book tiebreak (matches reference-orderbook semantics)
	buys      []*Order
	sells     []*Order
}

type FillDelivery struct {
	Fill       Fill
	Recipients []*Client
}

// Engine matches per symbol under independent book locks so different symbols
// run concurrently — pprof showed the old single global mutex became the
// bottleneck once synchronous logging was removed. Correctness is preserved by
// assigning each order's monotonic `engineSeq` *under the target book's lock*,
// so within a symbol the engine_seq order equals the processing order (the
// validator replays per symbol in engine_seq order). A global `index`
// (orderID→*Order) routes cancels to the right book without a global lock.
type Engine struct {
	engineSeq atomic.Uint64
	booksMu   sync.RWMutex // guards the books map (creation), not the matching
	books     map[string]*Book
	index     sync.Map // orderID(string) -> *Order (resting); Symbol routes the cancel
	mode      string
}

func (e *Engine) nextSeq() uint64 {
	return e.engineSeq.Add(1)
}

func (e *Engine) RejectAck(reason string) Ack {
	return Ack{
		Type:      "ack",
		Status:    "rejected",
		EngineSeq: e.nextSeq(),
		TsNs:      nowNs(),
		Reason:    reason,
	}
}

func (e *Engine) ProcessNew(in NewOrder, owner *Client) (Ack, []FillDelivery) {
	side := strings.ToUpper(in.Side)
	orderType := strings.ToUpper(in.OrderType)
	if orderType == "" {
		orderType = "LIMIT"
	}
	if in.ClientOrderID == "" || in.Qty <= 0 || (side != "BUY" && side != "SELL") || (orderType == "LIMIT" && in.Price <= 0) {
		// Rejected orders never touch a book; their seq position is irrelevant.
		return Ack{
			Type:          "ack",
			ClientOrderID: in.ClientOrderID,
			Status:        "rejected",
			Reason:        "invalid_order",
			EngineSeq:     e.nextSeq(),
			TsNs:          nowNs(),
		}, nil
	}

	// Reject oversized attacker-controlled string fields before they can rest
	// and pin memory. (WS/REST already bound the whole message, but a single
	// accepted order with a 256KB symbol could still rest indefinitely.)
	if len(in.ClientOrderID) > maxOrderFieldLen || len(in.Symbol) > maxOrderFieldLen {
		return Ack{
			Type:          "ack",
			ClientOrderID: in.ClientOrderID,
			Status:        "rejected",
			Reason:        "field_too_long",
			EngineSeq:     e.nextSeq(),
			TsNs:          nowNs(),
		}, nil
	}

	symbol := in.Symbol
	if symbol == "" {
		symbol = "DEFAULT"
	}
	book := e.bookFor(symbol)

	book.mu.Lock()
	defer book.mu.Unlock()

	// Assign the ack seq under the book lock: within this symbol, engine_seq
	// order == processing order, which is what the validator relies on.
	ack := Ack{
		Type:          "ack",
		ClientOrderID: in.ClientOrderID,
		Status:        "accepted",
		EngineSeq:     e.nextSeq(),
		TsNs:          nowNs(),
	}

	book.insertSeq++
	active := &Order{
		ID:        in.ClientOrderID,
		Symbol:    symbol,
		Side:      side,
		Price:     in.Price,
		Qty:       in.Qty,
		TsNs:      in.TsNs,
		InsertSeq: book.insertSeq,
		Owner:     owner,
	}

	var deliveries []FillDelivery
	broken := e.mode == "broken-price-time-priority"
	if side == "BUY" {
		deliveries = e.matchBuy(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			// Cap resting depth: an unbounded book makes the front-insert
			// quadratic and pins memory. The residual is silently dropped (it
			// matched as far as it could) and the ack stays "accepted" — the
			// cap is far above any legitimate depth, so this never fires in the
			// proof or normal play; only an abusive flood reaches it.
			if len(book.buys) < maxRestingPerSymbol {
				insertSorted(&book.buys, active, true, broken)
				e.index.Store(active.ID, active)
			}
		}
	} else {
		deliveries = e.matchSell(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			if len(book.sells) < maxRestingPerSymbol {
				insertSorted(&book.sells, active, false, broken)
				e.index.Store(active.ID, active)
			}
		}
	}

	return ack, deliveries
}

func (e *Engine) ProcessCancel(in CancelOrder) Ack {
	v, ok := e.index.Load(in.OrigClientOrderID)
	if !ok {
		// Not resting (never rested, or already filled/cancelled): a no-op
		// cancel, sequenced by the atomic counter — its position can't affect
		// any book, exactly as the reference replays a not-found cancel.
		return Ack{
			Type:          "ack",
			ClientOrderID: in.ClientOrderID,
			Status:        "not_found",
			EngineSeq:     e.nextSeq(),
			TsNs:          nowNs(),
		}
	}
	ord := v.(*Order)

	book := e.bookFor(ord.Symbol)
	book.mu.Lock()
	defer book.mu.Unlock()

	// Seq under the book lock so the cancel is ordered consistently with this
	// symbol's matches. Re-check membership under the lock: the order may have
	// filled (lower seq) between the Load above and acquiring the lock — the
	// pointer-identity check at the binary-searched position handles that.
	ack := Ack{
		Type:          "ack",
		ClientOrderID: in.ClientOrderID,
		Status:        "not_found",
		EngineSeq:     e.nextSeq(),
		TsNs:          nowNs(),
	}
	if removeOrder(book, ord, e.mode == "broken-price-time-priority") {
		ack.Status = "canceled"
		e.index.Delete(in.OrigClientOrderID)
	}
	return ack
}

// orderBefore reports whether a sorts strictly before b on the given side.
// (Price, TsNs, InsertSeq) is a strict total order because InsertSeq is unique
// per book — semantics identical to the old sortBook comparators, including
// the deliberate broken-price-time-priority inversion the validator must be
// able to catch.
func orderBefore(a, b *Order, buySide, broken bool) bool {
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
}

// insertSorted replaces the old append + sort.SliceStable of BOTH sides per
// resting insert (profiled at ~47% of matching CPU). The book is always
// sorted, and a stable sort of "sorted slice + one appended element" is
// exactly an upper-bound insert: O(log n) compares + one memmove, zero
// allocations. Equivalence is enforced by TestInsertSortedEquivalence.
func insertSorted(side *[]*Order, o *Order, buySide, broken bool) {
	s := *side
	i := sort.Search(len(s), func(i int) bool {
		return orderBefore(o, s[i], buySide, broken)
	})
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = o
	*side = s
}

// removeOrder binary-searches the order's exact ladder position (the
// comparator fields Price/TsNs/InsertSeq are immutable after insert), verifies
// by pointer identity, and removes with one memmove. Replaces a linear ID scan
// of both sides (runtime.memequal was 26% of the cancel-path profile). A
// pointer mismatch at the computed position means the order is no longer
// resting (filled or already cancelled) — exactly the not_found semantics.
func removeOrder(book *Book, ord *Order, broken bool) bool {
	buySide := ord.Side == "BUY"
	s := book.sells
	if buySide {
		s = book.buys
	}
	i := sort.Search(len(s), func(i int) bool {
		return !orderBefore(s[i], ord, buySide, broken)
	})
	if i >= len(s) || s[i] != ord {
		return false
	}
	s = append(s[:i], s[i+1:]...)
	if buySide {
		book.buys = s
	} else {
		book.sells = s
	}
	return true
}

func (e *Engine) bookFor(symbol string) *Book {
	if symbol == "" {
		symbol = "DEFAULT"
	}
	e.booksMu.RLock()
	book := e.books[symbol]
	e.booksMu.RUnlock()
	if book != nil {
		return book
	}
	e.booksMu.Lock()
	defer e.booksMu.Unlock()
	if book = e.books[symbol]; book == nil {
		book = &Book{}
		e.books[symbol] = book
	}
	return book
}

func (e *Engine) matchBuy(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	activeOwner := active.Owner // hoist out of the loop (immutable within the call)
	for active.Qty > 0 && len(book.sells) > 0 {
		resting := book.sells[0]
		if !market && active.Price < resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		fill := Fill{
			Symbol:      resting.Symbol,
			Type:        "fill",
			BuyOrderID:  active.ID,
			SellOrderID: resting.ID,
			Price:       resting.Price,
			Qty:         qty,
			EngineSeq:   e.nextSeq(),
		}
		deliveries = append(deliveries, FillDelivery{
			Fill:       fill,
			Recipients: uniqueClients(activeOwner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			e.index.Delete(resting.ID)
			book.sells = book.sells[1:]
		}
	}
	return deliveries
}

func (e *Engine) matchSell(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	activeOwner := active.Owner // hoist out of the loop (immutable within the call)
	for active.Qty > 0 && len(book.buys) > 0 {
		resting := book.buys[0]
		if !market && active.Price > resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		fill := Fill{
			Symbol:      resting.Symbol,
			Type:        "fill",
			BuyOrderID:  resting.ID,
			SellOrderID: active.ID,
			Price:       resting.Price,
			Qty:         qty,
			EngineSeq:   e.nextSeq(),
		}
		deliveries = append(deliveries, FillDelivery{
			Fill:       fill,
			Recipients: uniqueClients(activeOwner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			e.index.Delete(resting.ID)
			book.buys = book.buys[1:]
		}
	}
	return deliveries
}

type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *Client) SendJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

// JSONLLogger is an asynchronous audit log. pprof showed the old synchronous
// version (mutex + json.Encode + file write on every in/out message) was the
// dominant source of mutex contention under load — ~97% of lock wait time,
// dwarfing the matching engine itself. So the hot path now only does a
// non-blocking channel send; a single background goroutine owns the encoder and
// file. Under extreme load the buffer may fill, in which case we drop (and
// count) rather than stall the order path — the audit log is best-effort; the
// authoritative record is the bot fleet's own events.jsonl.
type JSONLLogger struct {
	ch      chan any
	quit    chan struct{}
	done    chan struct{}
	dropped atomic.Uint64
}

func NewJSONLLogger(path string) (*JSONLLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &JSONLLogger{
		ch:   make(chan any, 1<<16),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(l.done)
		w := bufio.NewWriterSize(f, 1<<20)
		enc := json.NewEncoder(w)
		// Periodic flush bounds the loss window on an unclean exit to ~500ms:
		// before this, the 1MB buffer flushed only when full or on graceful
		// close, so a SIGTERM-ed run left a 0-line audit file.
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case v := <-l.ch:
				_ = enc.Encode(v)
			case <-ticker.C:
				_ = w.Flush()
			case <-l.quit:
				// Drain whatever is already queued, then flush and close.
				for {
					select {
					case v := <-l.ch:
						_ = enc.Encode(v)
					default:
						_ = w.Flush()
						_ = f.Close()
						return
					}
				}
			}
		}
	}()
	return l, nil
}

func (l *JSONLLogger) Write(v any) {
	if l == nil {
		return
	}
	select {
	case l.ch <- v:
	default:
		l.dropped.Add(1)
	}
}

// Close drains queued entries, flushes the buffer and closes the file. Safe on
// a nil logger and safe against concurrent Write (the entry channel is never
// closed; late writes are simply never drained).
func (l *JSONLLogger) Close() {
	if l == nil {
		return
	}
	close(l.quit)
	<-l.done
}

type Server struct {
	engine    *Engine
	disruptor *DisruptorEngine // non-nil only in --engine disruptor mode
	logger    *JSONLLogger
	upgrader  websocket.Upgrader
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) orders(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// The REST path drives the mutex engine synchronously; in disruptor mode
	// s.engine is nil and the disruptor's async pipeline has no request/reply
	// slot for HTTP. Refuse cleanly instead of dereferencing nil.
	if s.engine == nil {
		http.Error(w, "REST /orders is only served in --engine mutex mode; use the WebSocket API", http.StatusNotImplemented)
		return
	}

	// Cap the request body so a giant POST cannot balloon RSS during decode.
	r.Body = http.MaxBytesReader(w, r.Body, maxRESTBody)

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var base BaseMessage
	if err := json.Unmarshal(raw, &base); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.logMsg("in", "rest", raw)

	switch base.Type {
	case "new_order":
		var order NewOrder
		if err := json.Unmarshal(raw, &order); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ack, deliveries := s.engine.ProcessNew(order, nil)
		fills := make([]Fill, 0, len(deliveries))
		for _, delivery := range deliveries {
			fills = append(fills, delivery.Fill)
			s.sendFill(delivery)
		}
		s.logMsg("out", "rest", ack)
		_ = json.NewEncoder(w).Encode(map[string]any{"ack": ack, "fills": fills})
	case "cancel_order":
		var cancel CancelOrder
		if err := json.Unmarshal(raw, &cancel); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ack := s.engine.ProcessCancel(cancel)
		s.logMsg("out", "rest", ack)
		_ = json.NewEncoder(w).Encode(ack)
	default:
		http.Error(w, "unknown message type", http.StatusBadRequest)
	}
}

// checkOrigin guards the WebSocket upgrade against cross-site hijacking while
// keeping the demo and the bot-fleet working. A missing/empty Origin (non-
// browser clients such as the bot-fleet send none) is allowed; a present Origin
// must parse and resolve to a loopback host. This replaces the previous
// always-true CheckOrigin, which let any website open a socket to a locally
// running engine.
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Non-browser clients (the bot-fleet, curl, wscat) send no Origin.
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]"
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	// Cap a single inbound frame so a huge message cannot balloon RSS.
	conn.SetReadLimit(wsReadLimit)
	client := &Client{conn: conn}
	defer conn.Close()

	// Idle/dead-connection reaping: a peer that opens the socket and then goes
	// silent (slowloris) must not hold a goroutine forever. We refresh a read
	// deadline on every received frame and on every pong; a background ticker
	// sends pings so a live-but-quiet peer keeps the connection open. If neither
	// data nor a pong arrives within the deadline, ReadMessage errors and the
	// loop returns, freeing the goroutine.
	const (
		wsPongWait   = 60 * time.Second
		wsPingPeriod = (wsPongWait * 9) / 10
	)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		ticker := time.NewTicker(wsPingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				client.mu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := conn.WriteMessage(websocket.PingMessage, nil)
				client.mu.Unlock()
				if err != nil {
					return
				}
			case <-pingDone:
				return
			}
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		// A data frame proves liveness; extend the read deadline.
		_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
		// Guard BEFORE boxing: json.RawMessage(raw) into `any` is an
		// allocation per inbound message that --events "" runs must not pay.
		if s.logger != nil {
			s.logMsg("in", "ws", json.RawMessage(raw))
		}

		// Single decode + dispatch on Type (no second full re-parse).
		var in Inbound
		if err := json.Unmarshal(raw, &in); err != nil {
			s.reject(client, "invalid_json")
			continue
		}

		switch in.Type {
		case "new_order":
			if s.disruptor != nil {
				s.disruptor.SubmitNew(in.asNewOrder(), client) // async: output stage replies
			} else {
				ack, deliveries := s.engine.ProcessNew(in.asNewOrder(), client)
				s.sendToClient(client, ack)
				for _, delivery := range deliveries {
					s.sendFill(delivery)
				}
			}
		case "cancel_order":
			if s.disruptor != nil {
				s.disruptor.SubmitCancel(in.asCancel(), client)
			} else {
				ack := s.engine.ProcessCancel(in.asCancel())
				s.sendToClient(client, ack)
			}
		default:
			s.reject(client, "unknown_type")
		}
	}
}

// reject sequences and emits a rejection ack, routing through whichever engine
// owns the engine_seq counter (the disruptor and the mutex engine must never
// share a counter, or engine_seq would stop being monotonic per symbol).
func (s *Server) reject(client *Client, reason string) {
	if s.disruptor != nil {
		s.disruptor.Reject(client, reason)
		return
	}
	s.sendToClient(client, s.engine.RejectAck(reason))
}

func (s *Server) sendFill(delivery FillDelivery) {
	for _, client := range delivery.Recipients {
		if client == nil {
			continue
		}
		s.sendToClient(client, delivery.Fill)
	}
}

// auditEntry is the audit-log record. A typed struct instead of the old
// map[string]any: the map cost an allocation + four hashed inserts per logged
// message on the hot path (A/B with audit on: p99 2.9us -> 6.1us was largely
// this); the struct is one boxed value into the logger channel.
type auditEntry struct {
	Direction string `json:"direction"`
	Transport string `json:"transport"`
	TsNs      uint64 `json:"ts_ns"`
	Message   any    `json:"message"`
}

// logMsg records an audit entry only when audit logging is enabled. The nil
// check lets a perf run (`--events ""`) skip the entry construction entirely.
func (s *Server) logMsg(direction, transport string, msg any) {
	if s.logger == nil {
		return
	}
	s.logger.Write(auditEntry{Direction: direction, Transport: transport, TsNs: nowNs(), Message: msg})
}

func (s *Server) sendToClient(client *Client, msg any) {
	s.logMsg("out", "ws", msg)
	if err := client.SendJSON(msg); err != nil {
		log.Printf("websocket write failed: %v", err)
	}
}

// uniqueClients returns the distinct, non-nil recipients of a fill in
// active-then-resting order. It is called once per fill on the matching hot
// path (always with the two trade owners), so the common shapes are handled
// with direct pointer comparison instead of the general map[*Client]bool dedup.
// Measured win (BenchmarkUniqueClients): ~2.4x faster per call (11.5 ns vs
// 28.2 ns) — the cost the map carried was CPU (map init + pointer hashing +
// lookups), NOT a heap allocation. (Go stack-allocates the small non-escaping
// map, so allocs/op is unchanged at 1: the single returned slice the
// FillDelivery must own. The benchmark refuted the escape-analysis hypothesis
// that the map was a per-fill heap allocation — measure, don't assume.) A
// self-trade or one-sided (counterparty on another pod) fill collapses to one
// recipient. The map path remains a correct fallback, never reached by the
// 2-owner call.
func uniqueClients(clients ...*Client) []*Client {
	switch len(clients) {
	case 0:
		return nil
	case 1:
		if clients[0] == nil {
			return nil
		}
		return []*Client{clients[0]}
	case 2:
		a, b := clients[0], clients[1]
		switch {
		case a == nil && b == nil:
			return nil
		case a == nil:
			return []*Client{b}
		case b == nil:
			return []*Client{a}
		case a == b: // self-trade: a single recipient hears both sides
			return []*Client{a}
		default:
			return []*Client{a, b}
		}
	default:
		seen := make(map[*Client]bool, len(clients))
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
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func nowNs() uint64 {
	return uint64(time.Now().UnixNano())
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	eventsPath := flag.String("events", "engine-events.jsonl", "engine-side JSONL audit log")
	mode := flag.String("mode", "normal", "engine mode: normal or broken-price-time-priority")
	engineKind := flag.String("engine", "mutex", "matching engine: mutex (sharded per-symbol locks) or disruptor (lock-free MPSC ring + single matcher goroutine per shard)")
	pprofAddr := flag.String("pprof", "", "if set (e.g. :6060), serve net/http/pprof + enable mutex/block profiling")
	flag.Parse()

	if *mode != "normal" && *mode != "broken-price-time-priority" {
		log.Fatalf("unsupported mode %q", *mode)
	}

	// Profiling endpoint. Lets `go tool pprof` attach to a running engine and
	// pull CPU / heap / mutex / block / goroutine profiles under live load —
	// the fastest way to find what actually drives an engine's tail latency
	// (for this single-lock engine: mutex contention on the matching path).
	if *pprofAddr != "" {
		runtime.SetMutexProfileFraction(1) // sample every mutex contention event
		runtime.SetBlockProfileRate(1)     // sample every blocking event (ns)
		go func() {
			log.Printf("pprof listening on %s (try: go tool pprof http://localhost%s/debug/pprof/mutex)", *pprofAddr, *pprofAddr)
			log.Println(http.ListenAndServe(*pprofAddr, nil))
		}()
	}

	// An empty --events path disables the audit log entirely (no file, no
	// per-message allocation) — the lever a contestant flips for a perf run.
	var logger *JSONLLogger
	if *eventsPath != "" {
		var err error
		logger, err = NewJSONLLogger(*eventsPath)
		if err != nil {
			log.Fatalf("open event log: %v", err)
		}
	}

	server := &Server{
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: checkOrigin,
		},
	}
	switch *engineKind {
	case "disruptor":
		de := NewDisruptorEngine(*mode)
		if logger != nil {
			// Outbound audit parity with the mutex path: without this hook the
			// disruptor's audit log silently recorded inbound traffic only.
			de.audit = func(v any) { server.logMsg("out", "ws", v) }
		}
		de.Start()
		server.disruptor = de
	case "mutex", "":
		server.engine = &Engine{mode: *mode, books: make(map[string]*Book)}
	default:
		log.Fatalf("unknown --engine %q (want mutex or disruptor)", *engineKind)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.health)
	mux.HandleFunc("/orders", server.orders)
	mux.HandleFunc("/ws", server.ws)

	// Timeouts bound slow/idle connections so a slowloris flood cannot pin one
	// goroutine per stalled connection. WriteTimeout is intentionally left zero:
	// the /ws handler hijacks the connection and manages its own read/write
	// deadlines (see ws()), and a server-wide WriteTimeout would kill long-lived
	// WebSocket sessions.
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("stub engine listening on %s mode=%s engine=%s", *addr, *mode, *engineKind)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown: stop the listener, then flush the audit log. Without
	// this, a SIGTERM-ed run silently lost the entire buffered JSONL audit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Printf("stub engine: shutdown signal; draining")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	logger.Close()
	log.Printf("stub engine: audit log flushed (dropped=%d); bye", func() uint64 {
		if logger == nil {
			return 0
		}
		return logger.dropped.Load()
	}())
}
