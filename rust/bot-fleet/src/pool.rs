use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// One outgoing WebSocket per pool connection. `bots/N` virtual bots share
/// each socket. Routing inbound messages back to a specific bot is done by
/// parsing `client_order_id` (which encodes the bot index).
pub struct ConnectionPool {
    senders: Vec<mpsc::Sender<String>>,
    bot_inboxes: Vec<Option<mpsc::UnboundedReceiver<Value>>>,
}

impl ConnectionPool {
    /// Open `n_conns` WebSocket connections to `target`, spin up read+write
    /// driver tasks for each, and pre-allocate one inbox per virtual bot.
    pub async fn connect(target: &str, n_conns: usize, n_bots: usize) -> Result<Self> {
        assert!(n_conns >= 1 && n_bots >= 1, "pool needs at least 1 conn and 1 bot");

        let mut inboxes_tx: Vec<mpsc::UnboundedSender<Value>> = Vec::with_capacity(n_bots);
        let mut inboxes_rx: Vec<Option<mpsc::UnboundedReceiver<Value>>> = Vec::with_capacity(n_bots);
        for _ in 0..n_bots {
            let (tx, rx) = mpsc::unbounded_channel::<Value>();
            inboxes_tx.push(tx);
            inboxes_rx.push(Some(rx));
        }
        let inboxes_tx = Arc::new(inboxes_tx);

        let mut senders = Vec::with_capacity(n_conns);
        for _ in 0..n_conns {
            let (ws_stream, _) = connect_async(target)
                .await
                .with_context(|| format!("pool connect {}", target))?;
            let (mut write, mut read) = ws_stream.split();

            // Writer task: drain its mpsc and push text frames over the WS.
            let (tx_out, mut rx_out) = mpsc::channel::<String>(4096);
            tokio::spawn(async move {
                while let Some(text) = rx_out.recv().await {
                    if write.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                let _ = write.close().await;
            });
            senders.push(tx_out);

            // Reader task: parse messages, demux by client_order_id.
            let inboxes_tx_clone = Arc::clone(&inboxes_tx);
            tokio::spawn(async move {
                while let Some(msg) = read.next().await {
                    let text = match msg {
                        Ok(Message::Text(t)) => t,
                        Ok(Message::Binary(b)) => String::from_utf8_lossy(&b).to_string(),
                        Ok(_) => continue,
                        Err(_) => break,
                    };
                    let value: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let targets = recipient_bots(&value);
                    if targets.is_empty() {
                        continue;
                    }
                    // Most messages route to a single bot; fills go to both
                    // sides. Cloning once per recipient is cheap relative to
                    // a WS roundtrip.
                    for (idx, bot) in targets.iter().enumerate() {
                        if *bot >= inboxes_tx_clone.len() {
                            continue;
                        }
                        let payload = if idx + 1 == targets.len() {
                            // Last recipient — move the value instead of cloning.
                            value.clone()
                        } else {
                            value.clone()
                        };
                        let _ = inboxes_tx_clone[*bot].send(payload);
                    }
                }
            });
        }

        Ok(Self {
            senders,
            bot_inboxes: inboxes_rx,
        })
    }

    pub fn sender_for(&self, bot_index: usize) -> mpsc::Sender<String> {
        let n = self.senders.len();
        self.senders[bot_index % n].clone()
    }

    /// Hand the inbox for a given bot to its task. Each inbox is consumed
    /// exactly once.
    pub fn take_inbox(&mut self, bot_index: usize) -> mpsc::UnboundedReceiver<Value> {
        self.bot_inboxes[bot_index]
            .take()
            .expect("inbox taken twice for the same bot")
    }
}

/// Decide which bot inboxes a single inbound engine message should go to.
/// `ack` is routed by `client_order_id`. `fill` lacks one, so we route by
/// both `buy_order_id` and `sell_order_id`. Other types are dropped.
fn recipient_bots(value: &Value) -> Vec<usize> {
    let msg_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    match msg_type {
        "ack" => value
            .get("client_order_id")
            .and_then(Value::as_str)
            .and_then(parse_bot_index)
            .map(|i| vec![i])
            .unwrap_or_default(),
        "fill" => {
            let mut out = Vec::with_capacity(2);
            let buy = value
                .get("buy_order_id")
                .and_then(Value::as_str)
                .and_then(parse_bot_index);
            let sell = value
                .get("sell_order_id")
                .and_then(Value::as_str)
                .and_then(parse_bot_index);
            if let Some(b) = buy {
                out.push(b);
            }
            if let Some(s) = sell {
                if !out.contains(&s) {
                    out.push(s);
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Parse "bot_{N}_{seq}" -> N - 1 (we use 1-indexed externally, 0-indexed
/// internally). Returns None for any other shape.
fn parse_bot_index(client_order_id: &str) -> Option<usize> {
    let rest = client_order_id.strip_prefix("bot_")?;
    let underscore = rest.find('_')?;
    let n: usize = rest[..underscore].parse().ok()?;
    if n == 0 {
        return None;
    }
    Some(n - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_bot_index() {
        assert_eq!(parse_bot_index("bot_1_000001"), Some(0));
        assert_eq!(parse_bot_index("bot_42_000999"), Some(41));
        assert_eq!(parse_bot_index("bot_0_000001"), None);
        assert_eq!(parse_bot_index("garbage"), None);
        assert_eq!(parse_bot_index("bot_x_000001"), None);
    }

    #[test]
    fn ack_routes_to_one_bot() {
        let v = json!({ "type": "ack", "client_order_id": "bot_5_000001" });
        assert_eq!(recipient_bots(&v), vec![4]);
    }

    #[test]
    fn fill_routes_to_both_sides() {
        let v = json!({
            "type": "fill",
            "buy_order_id": "bot_3_000001",
            "sell_order_id": "bot_7_000002",
        });
        let r = recipient_bots(&v);
        assert!(r.contains(&2));
        assert!(r.contains(&6));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn fill_dedupes_self_trade_recipients() {
        let v = json!({
            "type": "fill",
            "buy_order_id": "bot_3_000001",
            "sell_order_id": "bot_3_000002",
        });
        assert_eq!(recipient_bots(&v), vec![2]);
    }

    #[test]
    fn unknown_message_dropped() {
        let v = json!({ "type": "weird" });
        assert!(recipient_bots(&v).is_empty());
    }
}
