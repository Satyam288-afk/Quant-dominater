use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
