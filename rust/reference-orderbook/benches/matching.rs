// Micro-benchmark for the reference orderbook's hot path. The validator replays
// every contestant order through this matcher, so its throughput sets the
// ceiling on how fast we can verify a run. `cargo bench -p reference-orderbook`
// gives a stable per-operation number to optimize against (Criterion isolates
// it from noise far better than a wall-clock loop).
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use reference_orderbook::{NewOrder, OrderBook, OrderType, Side};

fn order(id: &str, side: Side, price: i64, qty: i64, ts: u64) -> NewOrder {
    NewOrder {
        message_type: None,
        run_id: "bench".to_string(),
        client_order_id: id.to_string(),
        symbol: None,
        side,
        price,
        qty,
        ts_ns: ts,
        order_type: OrderType::Limit,
    }
}

/// A book with depth on both sides, mirroring a loaded run.
fn loaded_book() -> OrderBook {
    let mut book = OrderBook::new();
    for i in 0..1000u64 {
        let _ = book.process_new_order(order(
            &format!("b{i}"),
            Side::Buy,
            100 - (i as i64 % 5),
            5,
            i,
        ));
        let _ = book.process_new_order(order(
            &format!("s{i}"),
            Side::Sell,
            101 + (i as i64 % 5),
            5,
            i,
        ));
    }
    book
}

fn bench_matching(c: &mut Criterion) {
    // Resting insert (no cross): the common case — most orders rest.
    c.bench_function("rest_no_cross", |b| {
        b.iter_batched(
            loaded_book,
            |mut book| {
                let _ =
                    black_box(book.process_new_order(order("rest", Side::Buy, 90, 5, 1_000_000)));
            },
            BatchSize::SmallInput,
        )
    });

    // Aggressive sweep across several price levels: the expensive case.
    c.bench_function("sweep_multi_level", |b| {
        b.iter_batched(
            loaded_book,
            |mut book| {
                let _ =
                    black_box(book.process_new_order(order("agg", Side::Sell, 90, 60, 1_000_000)));
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_matching);
criterion_main!(benches);
