use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::types::{Fill, NewOrder, Order, OrderBookError, OrderType, Side};

#[derive(Debug, Default)]
pub struct OrderBook {
    buys: BTreeMap<i64, VecDeque<Order>>,
    sells: BTreeMap<i64, VecDeque<Order>>,
    index: HashMap<String, (Side, i64)>,
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
                    self.rest_order(active);
                }
                fills
            }
            Side::Sell => {
                let fills = self.match_sell(&mut active, is_market);
                if active.qty > 0 && !is_market {
                    self.rest_order(active);
                }
                fills
            }
        };

        Ok(fills)
    }

    pub fn cancel(&mut self, order_id: &str) -> bool {
        let Some((side, price)) = self.index.remove(order_id) else {
            return false;
        };
        let book = match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        };
        let level_empty = match book.get_mut(&price) {
            None => return false,
            Some(level) => {
                if let Some(pos) = level.iter().position(|o| o.order_id == order_id) {
                    level.remove(pos);
                }
                level.is_empty()
            }
        };
        if level_empty {
            book.remove(&price);
        }
        true
    }

    pub fn buy_depth(&self) -> usize {
        self.buys.values().map(|level| level.len()).sum()
    }

    pub fn sell_depth(&self) -> usize {
        self.sells.values().map(|level| level.len()).sum()
    }

    fn rest_order(&mut self, order: Order) {
        let book = match order.side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        };
        self.index
            .insert(order.order_id.clone(), (order.side, order.price));
        let level = book.entry(order.price).or_default();
        // Maintain (ts_ns, insert_seq) ascending order so the front of the
        // queue is always the next to be filled.
        let pos = level
            .iter()
            .rposition(|existing| {
                (existing.ts_ns, existing.insert_seq) <= (order.ts_ns, order.insert_seq)
            })
            .map(|p| p + 1)
            .unwrap_or(0);
        level.insert(pos, order);
    }

    fn match_buy(&mut self, active: &mut Order, market: bool) -> Vec<Fill> {
        let mut fills = Vec::new();
        while active.qty > 0 {
            let Some((&best_price, _)) = self.sells.iter().next() else {
                break;
            };
            if !market && active.price < best_price {
                break;
            }
            let level_empty;
            let mut filled_id: Option<String> = None;
            {
                let level = self.sells.get_mut(&best_price).expect("level exists");
                let resting = level.front_mut().expect("level non-empty");
                let qty = active.qty.min(resting.qty);
                let fill = Fill {
                    buy_order_id: active.order_id.clone(),
                    sell_order_id: resting.order_id.clone(),
                    price: resting.price,
                    qty,
                    engine_seq: None,
                };
                active.qty -= qty;
                resting.qty -= qty;
                if resting.qty == 0 {
                    filled_id = Some(resting.order_id.clone());
                    level.pop_front();
                }
                level_empty = level.is_empty();
                fills.push(fill);
            }
            if let Some(id) = filled_id {
                self.index.remove(&id);
            }
            if level_empty {
                self.sells.remove(&best_price);
            }
        }
        fills
    }

    fn match_sell(&mut self, active: &mut Order, market: bool) -> Vec<Fill> {
        let mut fills = Vec::new();
        while active.qty > 0 {
            let Some((&best_price, _)) = self.buys.iter().next_back() else {
                break;
            };
            if !market && active.price > best_price {
                break;
            }
            let level_empty;
            let mut filled_id: Option<String> = None;
            {
                let level = self.buys.get_mut(&best_price).expect("level exists");
                let resting = level.front_mut().expect("level non-empty");
                let qty = active.qty.min(resting.qty);
                let fill = Fill {
                    buy_order_id: resting.order_id.clone(),
                    sell_order_id: active.order_id.clone(),
                    price: resting.price,
                    qty,
                    engine_seq: None,
                };
                active.qty -= qty;
                resting.qty -= qty;
                if resting.qty == 0 {
                    filled_id = Some(resting.order_id.clone());
                    level.pop_front();
                }
                level_empty = level.is_empty();
                fills.push(fill);
            }
            if let Some(id) = filled_id {
                self.index.remove(&id);
            }
            if level_empty {
                self.buys.remove(&best_price);
            }
        }
        fills
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

    #[test]
    fn lowest_ask_matched_first_across_levels() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("sell_high", Side::Sell, 10050, 5, 1))
            .unwrap();
        book.process_new_order(limit("sell_low", Side::Sell, 10030, 5, 2))
            .unwrap();

        let fills = book
            .process_new_order(limit("buy_marketable", Side::Buy, 10100, 7, 3))
            .unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].sell_order_id, "sell_low");
        assert_eq!(fills[0].price, 10030);
        assert_eq!(fills[1].sell_order_id, "sell_high");
        assert_eq!(fills[1].price, 10050);
    }

    #[test]
    fn cancel_then_reinsert_works() {
        let mut book = OrderBook::new();
        book.process_new_order(limit("a", Side::Buy, 100, 5, 1))
            .unwrap();
        assert!(book.cancel("a"));
        // Reinsert same id at a different price.
        book.process_new_order(limit("a", Side::Buy, 110, 5, 2))
            .unwrap();
        let fills = book
            .process_new_order(limit("s", Side::Sell, 100, 5, 3))
            .unwrap();
        assert_eq!(fills[0].buy_order_id, "a");
        assert_eq!(fills[0].price, 110);
    }
}
