package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
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
// (orderID→symbol) routes cancels to the right book without a global lock.
type Engine struct {
	engineSeq atomic.Uint64
	booksMu   sync.RWMutex // guards the books map (creation), not the matching
	books     map[string]*Book
	index     sync.Map // orderID(string) -> symbol(string), for resting orders
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
	if side == "BUY" {
		deliveries = e.matchBuy(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.buys = append(book.buys, active)
			e.sortBook(book)
			e.index.Store(active.ID, symbol)
		}
	} else {
		deliveries = e.matchSell(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.sells = append(book.sells, active)
			e.sortBook(book)
			e.index.Store(active.ID, symbol)
		}
	}

	return ack, deliveries
}

func (e *Engine) ProcessCancel(in CancelOrder) Ack {
	symVal, ok := e.index.Load(in.OrigClientOrderID)
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

	book := e.bookFor(symVal.(string))
	book.mu.Lock()
	defer book.mu.Unlock()

	// Seq under the book lock so the cancel is ordered consistently with this
	// symbol's matches. Re-check membership under the lock: the order may have
	// filled (lower seq) between the Load above and acquiring the lock.
	ack := Ack{
		Type:          "ack",
		ClientOrderID: in.ClientOrderID,
		Status:        "not_found",
		EngineSeq:     e.nextSeq(),
		TsNs:          nowNs(),
	}
	if removeResting(book, in.OrigClientOrderID) {
		ack.Status = "canceled"
		e.index.Delete(in.OrigClientOrderID)
	}
	return ack
}

func removeResting(book *Book, id string) bool {
	for i, order := range book.buys {
		if order.ID == id {
			book.buys = append(book.buys[:i], book.buys[i+1:]...)
			return true
		}
	}
	for i, order := range book.sells {
		if order.ID == id {
			book.sells = append(book.sells[:i], book.sells[i+1:]...)
			return true
		}
	}
	return false
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

func (e *Engine) sortBook(book *Book) {
	sort.SliceStable(book.buys, func(i, j int) bool {
		a, b := book.buys[i], book.buys[j]
		if a.Price != b.Price {
			return a.Price > b.Price
		}
		if a.TsNs != b.TsNs {
			if e.mode == "broken-price-time-priority" {
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
			if e.mode == "broken-price-time-priority" {
				return a.TsNs > b.TsNs
			}
			return a.TsNs < b.TsNs
		}
		return a.InsertSeq < b.InsertSeq
	})
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
	dropped atomic.Uint64
}

func NewJSONLLogger(path string) (*JSONLLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &JSONLLogger{ch: make(chan any, 1<<16)}
	go func() {
		w := bufio.NewWriterSize(f, 1<<20)
		enc := json.NewEncoder(w)
		for v := range l.ch {
			_ = enc.Encode(v)
		}
		_ = w.Flush()
		_ = f.Close()
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

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	client := &Client{conn: conn}
	defer conn.Close()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		s.logMsg("in", "ws", json.RawMessage(raw))

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

// logMsg records an audit entry only when audit logging is enabled. Building the
// map is itself an allocation on the hot path, so the nil check lets a perf run
// (`--events ""`) skip it entirely.
func (s *Server) logMsg(direction, transport string, msg any) {
	if s.logger == nil {
		return
	}
	s.logger.Write(map[string]any{"direction": direction, "transport": transport, "ts_ns": nowNs(), "message": msg})
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
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
	}
	switch *engineKind {
	case "disruptor":
		de := NewDisruptorEngine(*mode)
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

	log.Printf("stub engine listening on %s mode=%s engine=%s", *addr, *mode, *engineKind)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
