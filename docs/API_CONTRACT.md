# API Contract

All contestants implement the same trading protocol. WebSocket is the primary benchmark path; REST is an optional compatibility fallback.

## Endpoints

```text
GET  /health
WS   /ws
POST /orders
```

`GET /health` returns:

```text
OK
```

## Message Rules

- Every message is JSON.
- `type` selects the message kind.
- `run_id` identifies the benchmark run.
- `client_order_id` is globally unique within a run.
- `symbol` is optional; if omitted, engines should use `DEFAULT`.
- Prices and quantities are integers.
- `ts_ns` is nanoseconds and is used for deterministic replay.
- Engines must emit monotonically increasing `engine_seq` values for outputs.

## New Limit Order

```json
{
  "type": "new_order",
  "run_id": "run_001",
  "client_order_id": "bot_1_0001",
  "side": "BUY",
  "symbol": "SYM_1",
  "price": 10025,
  "qty": 10,
  "ts_ns": 1770000000000000000
}
```

Optional fields:

```json
{
  "order_type": "LIMIT"
}
```

## Cancel Order

```json
{
  "type": "cancel_order",
  "run_id": "run_001",
  "client_order_id": "bot_1_cancel_0001",
  "orig_client_order_id": "bot_1_0001",
  "ts_ns": 1770000000000100000
}
```

## Ack

```json
{
  "type": "ack",
  "client_order_id": "bot_1_0001",
  "status": "accepted",
  "engine_seq": 1,
  "ts_ns": 1770000000000001000
}
```

Allowed statuses:

```text
accepted
rejected
canceled
not_found
```

## Fill

```json
{
  "type": "fill",
  "buy_order_id": "bot_1_0001",
  "sell_order_id": "bot_2_0007",
  "price": 10025,
  "qty": 5,
  "engine_seq": 2
}
```

## Bot Event JSONL

The bot fleet writes canonical input orders to `events.jsonl`:

```json
{"event_type":"order_sent","run_id":"run_local_001","bot_id":"bot_1","seq_no":1,"send_ts_ns":1770000000000000001,"order":{"type":"new_order","run_id":"run_local_001","client_order_id":"bot_1_000001","symbol":"SYM_1","side":"BUY","price":10025,"qty":10,"ts_ns":1770000000000000001}}
```

The bot fleet writes contestant outputs to `contestant_outputs.jsonl`:

```json
{"event_type":"ack_received","run_id":"run_local_001","client_order_id":"bot_1_000001","recv_ts_ns":1770000000000001000,"latency_ns":1000,"message":{"type":"ack","client_order_id":"bot_1_000001","status":"accepted","engine_seq":1,"ts_ns":1770000000000001000}}
```

```json
{"event_type":"fill_received","run_id":"run_local_001","engine_seq":2,"message":{"type":"fill","buy_order_id":"bot_1_000001","sell_order_id":"bot_2_000007","price":10025,"qty":5,"engine_seq":2}}
```

## Correctness Semantics

- BUY book priority: highest price first, then earliest `ts_ns`, then insertion order.
- SELL book priority: lowest price first, then earliest `ts_ns`, then insertion order.
- Trade price is the resting order price.
- Limit orders with remaining quantity rest on the book.
- Market orders never rest.
- Cancel succeeds only for a resting order with remaining quantity.
