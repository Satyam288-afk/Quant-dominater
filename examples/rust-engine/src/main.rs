use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Debug, Parser)]
#[command(about = "IICPC contestant-style Rust engine stub")]
struct Args {
    #[arg(long, default_value = ":8080")]
    addr: String,

    #[arg(long, default_value = "engine-events.jsonl")]
    events: PathBuf,

    #[arg(long, default_value = "normal")]
    mode: String,
}

#[derive(Debug, Deserialize)]
struct Incoming {
    #[serde(rename = "type")]
    message_type: String,
    client_order_id: Option<String>,
    orig_client_order_id: Option<String>,
    symbol: Option<String>,
    side: Option<String>,
    price: Option<i64>,
    qty: Option<i64>,
    ts_ns: Option<u64>,
}

#[derive(Clone, Debug)]
struct Order {
    id: String,
    symbol: String,
    side: Side,
    price: i64,
    qty: i64,
    ts_ns: u64,
    insert_seq: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Default)]
struct Book {
    buys: Vec<Order>,
    sells: Vec<Order>,
}

#[derive(Serialize)]
struct Ack {
    #[serde(rename = "type")]
    message_type: &'static str,
    client_order_id: String,
    status: &'static str,
    engine_seq: u64,
    ts_ns: u64,
}

#[derive(Serialize)]
struct Fill {
    #[serde(rename = "type")]
    message_type: &'static str,
    symbol: String,
    buy_order_id: String,
    sell_order_id: String,
    price: i64,
    qty: i64,
    engine_seq: u64,
}

struct Engine {
    books: HashMap<String, Book>,
    engine_seq: u64,
    insert_seq: u64,
    events: BufWriter<File>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.mode != "normal" {
        anyhow::bail!("unsupported mode {:?}", args.mode);
    }

    let file =
        File::create(&args.events).with_context(|| format!("create {}", args.events.display()))?;
    let engine = Arc::new(Mutex::new(Engine {
        books: HashMap::new(),
        engine_seq: 0,
        insert_seq: 0,
        events: BufWriter::new(file),
    }));

    let addr = normalize_addr(&args.addr);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    eprintln!("rust engine listening on {addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, engine).await {
                eprintln!("connection failed: {err:#}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, engine: Arc<Mutex<Engine>>) -> Result<()> {
    let mut peek = [0_u8; 1024];
    let n = stream.peek(&mut peek).await?;
    let request = String::from_utf8_lossy(&peek[..n]);
    if request.starts_with("GET /health ") {
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"status\":\"ok\"}")
            .await?;
        return Ok(());
    }

    let ws = accept_async(stream).await?;
    let (mut write, mut read) = ws.split();
    while let Some(message) = read.next().await {
        let message = message?;
        let text = match message {
            Message::Text(text) => text,
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Message::Close(_) => break,
            _ => continue,
        };

        let outputs = {
            let mut engine = engine.lock().await;
            engine.process_text(&text)?
        };
        for output in outputs {
            write.send(Message::Text(output)).await?;
        }
    }
    Ok(())
}

impl Engine {
    fn process_text(&mut self, text: &str) -> Result<Vec<String>> {
        let raw: Value = serde_json::from_str(text)?;
        let incoming: Incoming = serde_json::from_value(raw.clone())?;
        self.write_event(json!({
            "event_type": "input",
            "ts_ns": now_ns(),
            "message": raw,
        }))?;

        match incoming.message_type.as_str() {
            "new_order" => self.new_order(incoming),
            "cancel_order" => self.cancel_order(incoming),
            _ => Ok(Vec::new()),
        }
    }

    fn new_order(&mut self, incoming: Incoming) -> Result<Vec<String>> {
        let client_order_id = incoming.client_order_id.unwrap_or_default();
        let symbol = incoming.symbol.unwrap_or_else(|| "DEFAULT".to_string());
        let side = match incoming.side.as_deref() {
            Some("BUY") => Side::Buy,
            Some("SELL") => Side::Sell,
            _ => {
                return self.ack(client_order_id, "rejected");
            }
        };
        let qty = incoming.qty.unwrap_or_default();
        let price = incoming.price.unwrap_or_default();
        if client_order_id.is_empty() || qty <= 0 || price <= 0 {
            return self.ack(client_order_id, "rejected");
        }

        self.insert_seq += 1;
        let mut order = Order {
            id: client_order_id.clone(),
            symbol: symbol.clone(),
            side,
            price,
            qty,
            ts_ns: incoming.ts_ns.unwrap_or_else(now_ns),
            insert_seq: self.insert_seq,
        };

        let mut outputs = self.ack(client_order_id, "accepted")?;
        let mut fills = self.match_order(&mut order)?;
        outputs.append(&mut fills);

        if order.qty > 0 {
            let book = self.books.entry(symbol).or_default();
            match order.side {
                Side::Buy => book.buys.push(order),
                Side::Sell => book.sells.push(order),
            }
        }
        Ok(outputs)
    }

    fn cancel_order(&mut self, incoming: Incoming) -> Result<Vec<String>> {
        let orig = incoming.orig_client_order_id.unwrap_or_default();
        let mut found = false;
        for book in self.books.values_mut() {
            let before = book.buys.len();
            book.buys.retain(|order| order.id != orig);
            found |= book.buys.len() != before;

            let before = book.sells.len();
            book.sells.retain(|order| order.id != orig);
            found |= book.sells.len() != before;
        }
        self.ack(
            incoming.client_order_id.unwrap_or_default(),
            if found { "canceled" } else { "not_found" },
        )
    }

    fn match_order(&mut self, order: &mut Order) -> Result<Vec<String>> {
        let mut outputs = Vec::new();
        loop {
            let resting = {
                let book = self.books.entry(order.symbol.clone()).or_default();
                match order.side {
                    Side::Buy => {
                        book.sells.sort_by(compare_sell_priority);
                        if book
                            .sells
                            .first()
                            .is_some_and(|resting| resting.price <= order.price)
                        {
                            Some(book.sells.remove(0))
                        } else {
                            None
                        }
                    }
                    Side::Sell => {
                        book.buys.sort_by(compare_buy_priority);
                        if book
                            .buys
                            .first()
                            .is_some_and(|resting| resting.price >= order.price)
                        {
                            Some(book.buys.remove(0))
                        } else {
                            None
                        }
                    }
                }
            };

            let Some(mut resting) = resting else {
                break;
            };
            let qty = order.qty.min(resting.qty);
            order.qty -= qty;
            resting.qty -= qty;

            self.engine_seq += 1;
            let fill = match order.side {
                Side::Buy => Fill {
                    message_type: "fill",
                    symbol: order.symbol.clone(),
                    buy_order_id: order.id.clone(),
                    sell_order_id: resting.id.clone(),
                    price: resting.price,
                    qty,
                    engine_seq: self.engine_seq,
                },
                Side::Sell => Fill {
                    message_type: "fill",
                    symbol: order.symbol.clone(),
                    buy_order_id: resting.id.clone(),
                    sell_order_id: order.id.clone(),
                    price: resting.price,
                    qty,
                    engine_seq: self.engine_seq,
                },
            };
            outputs.push(self.write_output(&fill)?);

            if resting.qty > 0 {
                let book = self.books.entry(resting.symbol.clone()).or_default();
                match resting.side {
                    Side::Buy => book.buys.push(resting),
                    Side::Sell => book.sells.push(resting),
                }
            }
            if order.qty == 0 {
                break;
            }
        }
        Ok(outputs)
    }

    fn ack(&mut self, client_order_id: String, status: &'static str) -> Result<Vec<String>> {
        self.engine_seq += 1;
        let ack = Ack {
            message_type: "ack",
            client_order_id,
            status,
            engine_seq: self.engine_seq,
            ts_ns: now_ns(),
        };
        Ok(vec![self.write_output(&ack)?])
    }

    fn write_output<T: Serialize>(&mut self, message: &T) -> Result<String> {
        let text = serde_json::to_string(message)?;
        let value: Value = serde_json::from_str(&text)?;
        self.write_event(json!({
            "event_type": "output",
            "ts_ns": now_ns(),
            "message": value,
        }))?;
        Ok(text)
    }

    fn write_event(&mut self, event: Value) -> Result<()> {
        serde_json::to_writer(&mut self.events, &event)?;
        self.events.write_all(b"\n")?;
        self.events.flush()?;
        Ok(())
    }
}

fn compare_buy_priority(a: &Order, b: &Order) -> Ordering {
    b.price
        .cmp(&a.price)
        .then_with(|| a.ts_ns.cmp(&b.ts_ns))
        .then_with(|| a.insert_seq.cmp(&b.insert_seq))
}

fn compare_sell_priority(a: &Order, b: &Order) -> Ordering {
    a.price
        .cmp(&b.price)
        .then_with(|| a.ts_ns.cmp(&b.ts_ns))
        .then_with(|| a.insert_seq.cmp(&b.insert_seq))
}

fn normalize_addr(addr: &str) -> String {
    if addr.starts_with(':') {
        format!("0.0.0.0{addr}")
    } else {
        addr.to_string()
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
