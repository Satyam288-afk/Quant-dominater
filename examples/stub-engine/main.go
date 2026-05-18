package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
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

type Ack struct {
	Type          string `json:"type"`
	ClientOrderID string `json:"client_order_id"`
	Status        string `json:"status"`
	EngineSeq     uint64 `json:"engine_seq"`
	TsNs          uint64 `json:"ts_ns"`
	Reason        string `json:"reason,omitempty"`
}

type Fill struct {
	Type        string `json:"type"`
	BuyOrderID  string `json:"buy_order_id"`
	SellOrderID string `json:"sell_order_id"`
	Price       int64  `json:"price"`
	Qty         int64  `json:"qty"`
	EngineSeq   uint64 `json:"engine_seq"`
}

type Order struct {
	ID        string
	Side      string
	Price     int64
	Qty       int64
	TsNs      uint64
	InsertSeq uint64
	Owner     *Client
}

type Book struct {
	buys  []*Order
	sells []*Order
}

type FillDelivery struct {
	Fill       Fill
	Recipients []*Client
}

type Engine struct {
	mu        sync.Mutex
	engineSeq uint64
	insertSeq uint64
	books     map[string]*Book
}

func (e *Engine) nextSeq() uint64 {
	e.engineSeq++
	return e.engineSeq
}

func (e *Engine) RejectAck(reason string) Ack {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Ack{
		Type:      "ack",
		Status:    "rejected",
		EngineSeq: e.nextSeq(),
		TsNs:      nowNs(),
		Reason:    reason,
	}
}

func (e *Engine) ProcessNew(in NewOrder, owner *Client) (Ack, []FillDelivery) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ack := Ack{
		Type:          "ack",
		ClientOrderID: in.ClientOrderID,
		Status:        "accepted",
		EngineSeq:     e.nextSeq(),
		TsNs:          nowNs(),
	}

	side := strings.ToUpper(in.Side)
	orderType := strings.ToUpper(in.OrderType)
	if orderType == "" {
		orderType = "LIMIT"
	}
	if in.ClientOrderID == "" || in.Qty <= 0 || (side != "BUY" && side != "SELL") || (orderType == "LIMIT" && in.Price <= 0) {
		ack.Status = "rejected"
		ack.Reason = "invalid_order"
		return ack, nil
	}

	e.insertSeq++
	book := e.bookFor(in.Symbol)
	active := &Order{
		ID:        in.ClientOrderID,
		Side:      side,
		Price:     in.Price,
		Qty:       in.Qty,
		TsNs:      in.TsNs,
		InsertSeq: e.insertSeq,
		Owner:     owner,
	}

	var deliveries []FillDelivery
	if side == "BUY" {
		deliveries = e.matchBuy(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.buys = append(book.buys, active)
			sortBook(book)
		}
	} else {
		deliveries = e.matchSell(book, active, orderType == "MARKET")
		if active.Qty > 0 && orderType != "MARKET" {
			book.sells = append(book.sells, active)
			sortBook(book)
		}
	}

	return ack, deliveries
}

func (e *Engine) ProcessCancel(in CancelOrder) Ack {
	e.mu.Lock()
	defer e.mu.Unlock()

	ack := Ack{
		Type:          "ack",
		ClientOrderID: in.ClientOrderID,
		Status:        "not_found",
		EngineSeq:     e.nextSeq(),
		TsNs:          nowNs(),
	}

	for _, book := range e.books {
		for i, order := range book.buys {
			if order.ID == in.OrigClientOrderID {
				book.buys = append(book.buys[:i], book.buys[i+1:]...)
				ack.Status = "canceled"
				return ack
			}
		}
		for i, order := range book.sells {
			if order.ID == in.OrigClientOrderID {
				book.sells = append(book.sells[:i], book.sells[i+1:]...)
				ack.Status = "canceled"
				return ack
			}
		}
	}

	return ack
}

func (e *Engine) bookFor(symbol string) *Book {
	if symbol == "" {
		symbol = "DEFAULT"
	}
	if e.books == nil {
		e.books = make(map[string]*Book)
	}
	book := e.books[symbol]
	if book == nil {
		book = &Book{}
		e.books[symbol] = book
	}
	return book
}

func (e *Engine) matchBuy(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	for active.Qty > 0 && len(book.sells) > 0 {
		resting := book.sells[0]
		if !market && active.Price < resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		fill := Fill{
			Type:        "fill",
			BuyOrderID:  active.ID,
			SellOrderID: resting.ID,
			Price:       resting.Price,
			Qty:         qty,
			EngineSeq:   e.nextSeq(),
		}
		deliveries = append(deliveries, FillDelivery{
			Fill:       fill,
			Recipients: uniqueClients(active.Owner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			book.sells = book.sells[1:]
		}
	}
	return deliveries
}

func (e *Engine) matchSell(book *Book, active *Order, market bool) []FillDelivery {
	var deliveries []FillDelivery
	for active.Qty > 0 && len(book.buys) > 0 {
		resting := book.buys[0]
		if !market && active.Price > resting.Price {
			break
		}
		qty := minInt64(active.Qty, resting.Qty)
		fill := Fill{
			Type:        "fill",
			BuyOrderID:  resting.ID,
			SellOrderID: active.ID,
			Price:       resting.Price,
			Qty:         qty,
			EngineSeq:   e.nextSeq(),
		}
		deliveries = append(deliveries, FillDelivery{
			Fill:       fill,
			Recipients: uniqueClients(active.Owner, resting.Owner),
		})
		active.Qty -= qty
		resting.Qty -= qty
		if resting.Qty == 0 {
			book.buys = book.buys[1:]
		}
	}
	return deliveries
}

func sortBook(book *Book) {
	sort.SliceStable(book.buys, func(i, j int) bool {
		a, b := book.buys[i], book.buys[j]
		if a.Price != b.Price {
			return a.Price > b.Price
		}
		if a.TsNs != b.TsNs {
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

type JSONLLogger struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewJSONLLogger(path string) (*JSONLLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &JSONLLogger{enc: json.NewEncoder(f)}, nil
}

func (l *JSONLLogger) Write(v any) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(v); err != nil {
		log.Printf("jsonl write failed: %v", err)
	}
}

type Server struct {
	engine   *Engine
	logger   *JSONLLogger
	upgrader websocket.Upgrader
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("OK\n"))
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

	s.logger.Write(map[string]any{"direction": "in", "transport": "rest", "ts_ns": nowNs(), "message": raw})

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
		s.logger.Write(map[string]any{"direction": "out", "transport": "rest", "ts_ns": nowNs(), "message": ack})
		_ = json.NewEncoder(w).Encode(map[string]any{"ack": ack, "fills": fills})
	case "cancel_order":
		var cancel CancelOrder
		if err := json.Unmarshal(raw, &cancel); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ack := s.engine.ProcessCancel(cancel)
		s.logger.Write(map[string]any{"direction": "out", "transport": "rest", "ts_ns": nowNs(), "message": ack})
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
		s.logger.Write(map[string]any{"direction": "in", "transport": "ws", "ts_ns": nowNs(), "message": json.RawMessage(raw)})

		var base BaseMessage
		if err := json.Unmarshal(raw, &base); err != nil {
			s.sendToClient(client, s.engine.RejectAck("invalid_json"))
			continue
		}

		switch base.Type {
		case "new_order":
			var order NewOrder
			if err := json.Unmarshal(raw, &order); err != nil {
				s.sendToClient(client, s.engine.RejectAck("invalid_order"))
				continue
			}
			ack, deliveries := s.engine.ProcessNew(order, client)
			s.sendToClient(client, ack)
			for _, delivery := range deliveries {
				s.sendFill(delivery)
			}
		case "cancel_order":
			var cancel CancelOrder
			if err := json.Unmarshal(raw, &cancel); err != nil {
				s.sendToClient(client, s.engine.RejectAck("invalid_cancel"))
				continue
			}
			ack := s.engine.ProcessCancel(cancel)
			s.sendToClient(client, ack)
		default:
			s.sendToClient(client, s.engine.RejectAck("unknown_type"))
		}
	}
}

func (s *Server) sendFill(delivery FillDelivery) {
	for _, client := range delivery.Recipients {
		if client == nil {
			continue
		}
		s.sendToClient(client, delivery.Fill)
	}
}

func (s *Server) sendToClient(client *Client, msg any) {
	s.logger.Write(map[string]any{"direction": "out", "transport": "ws", "ts_ns": nowNs(), "message": msg})
	if err := client.SendJSON(msg); err != nil {
		log.Printf("websocket write failed: %v", err)
	}
}

func uniqueClients(clients ...*Client) []*Client {
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
	flag.Parse()

	logger, err := NewJSONLLogger(*eventsPath)
	if err != nil {
		log.Fatalf("open event log: %v", err)
	}

	server := &Server{
		engine: &Engine{},
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.health)
	mux.HandleFunc("/orders", server.orders)
	mux.HandleFunc("/ws", server.ws)

	log.Printf("stub engine listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
