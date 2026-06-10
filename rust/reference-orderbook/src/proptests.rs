// Property tests: random sequences of new_order / cancel must produce
// identical fills from the BTreeMap-backed `OrderBook` and the Vec-backed
// `NaiveBook` oracle. This protects the perf rewrite against semantic drift.

use proptest::prelude::*;

use crate::oracle::NaiveBook;
use crate::types::{NewOrder, OrderType, Side};
use crate::OrderBook;

#[derive(Clone, Debug)]
enum Action {
    Place {
        id: u32,
        side: Side,
        price: i64,
        qty: i64,
        ts_ns: u64,
    },
    Cancel {
        id: u32,
    },
}

fn arb_action(max_id: u32) -> impl Strategy<Value = Action> {
    prop_oneof![
        (
            0u32..max_id,
            prop::bool::ANY.prop_map(|b| if b { Side::Buy } else { Side::Sell }),
            90i64..=110,
            1i64..=10,
            0u64..1_000_000,
        )
            .prop_map(|(id, side, price, qty, ts_ns)| Action::Place {
                id,
                side,
                price,
                qty,
                ts_ns,
            }),
        (0u32..max_id).prop_map(|id| Action::Cancel { id }),
    ]
}

fn run_against<B>(actions: &[Action], book: &mut B) -> Vec<crate::Fill>
where
    B: BookLike,
{
    // Track which ids are live so we never replay a place with an id that's
    // still on the book. Duplicate ids are undefined in the domain (bot-fleet
    // never generates them) and the two implementations diverge on them.
    use std::collections::HashSet;
    let mut live: HashSet<u32> = HashSet::new();
    let mut all_fills = Vec::new();
    for action in actions {
        match action {
            Action::Place {
                id,
                side,
                price,
                qty,
                ts_ns,
            } => {
                if live.contains(id) {
                    continue;
                }
                live.insert(*id);
                let order = NewOrder {
                    message_type: Some("new_order".to_string()),
                    run_id: "p".to_string(),
                    client_order_id: format!("o{}", id),
                    symbol: None,
                    side: *side,
                    price: *price,
                    qty: *qty,
                    ts_ns: *ts_ns,
                    order_type: OrderType::Limit,
                };
                match book.process_new_order(order) {
                    Ok(fills) => {
                        // If fully matched, the id never rests, so it can be
                        // reused later.
                        let remaining_on_book = fills.iter().map(|f| f.qty).sum::<i64>() < *qty;
                        if !remaining_on_book {
                            live.remove(id);
                        }
                        all_fills.extend(fills);
                    }
                    Err(_) => {
                        live.remove(id);
                    }
                }
            }
            Action::Cancel { id } => {
                if book.cancel(&format!("o{}", id)) {
                    live.remove(id);
                }
            }
        }
    }
    all_fills
}

trait BookLike {
    fn process_new_order(
        &mut self,
        input: NewOrder,
    ) -> Result<Vec<crate::Fill>, crate::OrderBookError>;
    fn cancel(&mut self, id: &str) -> bool;
}

impl BookLike for OrderBook {
    fn process_new_order(
        &mut self,
        input: NewOrder,
    ) -> Result<Vec<crate::Fill>, crate::OrderBookError> {
        OrderBook::process_new_order(self, input)
    }
    fn cancel(&mut self, id: &str) -> bool {
        OrderBook::cancel(self, id)
    }
}

impl BookLike for NaiveBook {
    fn process_new_order(
        &mut self,
        input: NewOrder,
    ) -> Result<Vec<crate::Fill>, crate::OrderBookError> {
        NaiveBook::process_new_order(self, input)
    }
    fn cancel(&mut self, id: &str) -> bool {
        NaiveBook::cancel(self, id)
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn new_book_matches_naive_oracle(actions in prop::collection::vec(arb_action(40), 1..200)) {
        let mut new_book = OrderBook::new();
        let mut naive = NaiveBook::new();
        let a = run_against(&actions, &mut new_book);
        let b = run_against(&actions, &mut naive);
        prop_assert_eq!(a, b);
    }
}
