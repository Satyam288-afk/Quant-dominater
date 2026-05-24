use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Limit,
    Market,
}

impl Default for OrderType {
    fn default() -> Self {
        Self::Limit
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NewOrder {
    #[serde(default, rename = "type")]
    pub message_type: Option<String>,
    pub run_id: String,
    pub client_order_id: String,
    #[serde(default)]
    pub symbol: Option<String>,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub ts_ns: u64,
    #[serde(default)]
    pub order_type: OrderType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order {
    pub order_id: String,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub ts_ns: u64,
    pub insert_seq: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Fill {
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub price: i64,
    pub qty: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_seq: Option<u64>,
}

impl Fill {
    pub fn without_engine_seq(&self) -> Self {
        Self {
            buy_order_id: self.buy_order_id.clone(),
            sell_order_id: self.sell_order_id.clone(),
            price: self.price,
            qty: self.qty,
            engine_seq: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum OrderBookError {
    #[error("missing client_order_id")]
    MissingOrderId,
    #[error("quantity must be positive")]
    NonPositiveQuantity,
    #[error("limit order price must be positive")]
    NonPositiveLimitPrice,
}

#[derive(Debug, Default)]
pub struct OrderBook {
    buys: Vec<Order>,
    sells: Vec<Order>,
    insert_seq: u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process_new_order(&mut self, input: NewOrder) -> Result<Vec<Fill>, OrderBookError> {
        if input.client_order_id.is_empty() {
            return Err(OrderBookError::MissingOrderId);
        }
        if input.qty <= 0 {
            return Err(OrderBookError::NonPositiveQuantity);
        }
        if input.order_type == OrderType::Limit && input.price <= 0 {
            return Err(OrderBookError::NonPositiveLimitPrice);
        }

        self.insert_seq += 1;
        let mut active = Order {
            order_id: input.client_order_id,
            side: input.side,
            price: input.price,
            qty: input.qty,
            ts_ns: input.ts_ns,
            insert_seq: self.insert_seq,
        };

        let is_market = input.order_type == OrderType::Market;
        let fills = match active.side {
            Side::Buy => {
                let fills = self.match_buy(&mut active, is_market);
                if active.qty > 0 && !is_market {
                    self.buys.push(active);
                    self.sort_books();
                }
                fills
            }
            Side::Sell => {
                let fills = self.match_sell(&mut active, is_market);
                if active.qty > 0 && !is_market {
                    self.sells.push(active);
                    self.sort_books();
                }
                fills
            }
        };

        Ok(fills)
    }

    pub fn cancel(&mut self, order_id: &str) -> bool {
        if let Some(idx) = self
            .buys
            .iter()
            .position(|order| order.order_id == order_id)
        {
            self.buys.remove(idx);
            return true;
        }
        if let Some(idx) = self
            .sells
            .iter()
            .position(|order| order.order_id == order_id)
        {
            self.sells.remove(idx);
            return true;
        }
        false
    }

    pub fn buy_depth(&self) -> usize {
        self.buys.len()
    }

    pub fn sell_depth(&self) -> usize {
        self.sells.len()
    }

    fn match_buy(&mut self, active: &mut Order, market: bool) -> Vec<Fill> {
        let mut fills = Vec::new();
        while active.qty > 0 && !self.sells.is_empty() {
            if !market && active.price < self.sells[0].price {
                break;
            }
            let qty = active.qty.min(self.sells[0].qty);
            let fill = Fill {
                buy_order_id: active.order_id.clone(),
                sell_order_id: self.sells[0].order_id.clone(),
                price: self.sells[0].price,
                qty,
                engine_seq: None,
            };
            active.qty -= qty;
            self.sells[0].qty -= qty;
            if self.sells[0].qty == 0 {
                self.sells.remove(0);
            }
            fills.push(fill);
        }
        fills
    }

    fn match_sell(&mut self, active: &mut Order, market: bool) -> Vec<Fill> {
        let mut fills = Vec::new();
        while active.qty > 0 && !self.buys.is_empty() {
            if !market && active.price > self.buys[0].price {
                break;
            }
            let qty = active.qty.min(self.buys[0].qty);
            let fill = Fill {
                buy_order_id: self.buys[0].order_id.clone(),
                sell_order_id: active.order_id.clone(),
                price: self.buys[0].price,
                qty,
                engine_seq: None,
            };
            active.qty -= qty;
            self.buys[0].qty -= qty;
            if self.buys[0].qty == 0 {
                self.buys.remove(0);
            }
            fills.push(fill);
        }
        fills
    }

    fn sort_books(&mut self) {
        self.buys.sort_by(|a, b| {
            b.price
                .cmp(&a.price)
                .then_with(|| a.ts_ns.cmp(&b.ts_ns))
                .then_with(|| a.insert_seq.cmp(&b.insert_seq))
        });
        self.sells.sort_by(|a, b| {
            a.price
                .cmp(&b.price)
                .then_with(|| a.ts_ns.cmp(&b.ts_ns))
                .then_with(|| a.insert_seq.cmp(&b.insert_seq))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(id: &str, side: Side, price: i64, qty: i64, ts_ns: u64) -> NewOrder {
        NewOrder {
            message_type: Some("new_order".to_string()),
            run_id: "run_test".to_string(),
            client_order_id: id.to_string(),
            symbol: None,
            side,
            price,
            qty,
            ts_ns,
            order_type: OrderType::Limit,
        }
    }

    #[test]
    fn earlier_timestamp_at_same_price_fills_first() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("buy_late", Side::Buy, 10025, 5, 2))
            .unwrap();
        book.process_new_order(limit("buy_early", Side::Buy, 10025, 5, 1))
            .unwrap();

        let fills = book
            .process_new_order(limit("sell_1", Side::Sell, 10025, 5, 3))
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].buy_order_id, "buy_early");
        assert_eq!(fills[0].sell_order_id, "sell_1");
    }

    #[test]
    fn higher_buy_price_has_priority_before_time() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("buy_low_early", Side::Buy, 10020, 5, 1))
            .unwrap();
        book.process_new_order(limit("buy_high_late", Side::Buy, 10025, 5, 2))
            .unwrap();

        let fills = book
            .process_new_order(limit("sell_1", Side::Sell, 10000, 5, 3))
            .unwrap();

        assert_eq!(fills[0].buy_order_id, "buy_high_late");
        assert_eq!(fills[0].price, 10025);
    }

    #[test]
    fn partial_fill_leaves_remainder_on_book() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("buy_1", Side::Buy, 10025, 10, 1))
            .unwrap();

        let fills = book
            .process_new_order(limit("sell_1", Side::Sell, 10025, 4, 2))
            .unwrap();

        assert_eq!(fills[0].qty, 4);
        assert_eq!(book.buy_depth(), 1);
    }

    #[test]
    fn cancel_removes_resting_order() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("buy_1", Side::Buy, 10025, 10, 1))
            .unwrap();

        assert!(book.cancel("buy_1"));
        assert_eq!(book.buy_depth(), 0);
        assert!(!book.cancel("buy_1"));
    }
}
