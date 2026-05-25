// Naive Vec-based reference implementation kept solely as a property-test
// oracle against `OrderBook`. It mirrors the previous shipping algorithm:
// sort the entire book after every insert, scan from the front for matches.

use crate::types::{Fill, NewOrder, Order, OrderBookError, OrderType, Side};

#[derive(Debug, Default)]
pub struct NaiveBook {
    buys: Vec<Order>,
    sells: Vec<Order>,
    insert_seq: u64,
}

impl NaiveBook {
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
                    self.sort();
                }
                fills
            }
            Side::Sell => {
                let fills = self.match_sell(&mut active, is_market);
                if active.qty > 0 && !is_market {
                    self.sells.push(active);
                    self.sort();
                }
                fills
            }
        };
        Ok(fills)
    }

    pub fn cancel(&mut self, order_id: &str) -> bool {
        if let Some(idx) = self.buys.iter().position(|o| o.order_id == order_id) {
            self.buys.remove(idx);
            return true;
        }
        if let Some(idx) = self.sells.iter().position(|o| o.order_id == order_id) {
            self.sells.remove(idx);
            return true;
        }
        false
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

    fn sort(&mut self) {
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
