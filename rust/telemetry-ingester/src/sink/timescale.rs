use anyhow::{Context, Result};
use bench_core::telemetry::{EventKind, TelemetryEvent};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};

/// Spawn the Timescale sink. It owns a connection pool and batches incoming
/// telemetry events into the metrics_raw hypertable. The summary aggregates
/// produced by the in-process aggregator are written separately at finalize.
pub async fn spawn(
    url: String,
    flush_ms: u64,
    batch_size: usize,
) -> Result<mpsc::Sender<TelemetryEvent>> {
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .with_context(|| format!("connecting timescale {url}"))?;
    let (tx, mut rx) = mpsc::channel::<TelemetryEvent>(16_384);
    tokio::spawn(async move {
        let mut buf: Vec<TelemetryEvent> = Vec::with_capacity(batch_size);
        let mut ticker = interval(Duration::from_millis(flush_ms.max(50)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(e) => {
                            buf.push(e);
                            if buf.len() >= batch_size {
                                if let Err(err) = flush(&pool, &mut buf).await {
                                    tracing::warn!(error=%err, "timescale flush error");
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        if let Err(err) = flush(&pool, &mut buf).await {
                            tracing::warn!(error=%err, "timescale flush error");
                        }
                    }
                }
            }
        }
        if !buf.is_empty() {
            let _ = flush(&pool, &mut buf).await;
        }
    });
    Ok(tx)
}

async fn flush(pool: &PgPool, buf: &mut Vec<TelemetryEvent>) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let mut query = String::from(
        "INSERT INTO metrics_raw \
         (time, run_id, bot_id, event_type, client_order_id, seq_no, latency_ns, send_ts_ns, recv_ts_ns) VALUES "
    );
    for i in 0..buf.len() {
        if i > 0 {
            query.push(',');
        }
        let base = i * 9;
        query.push_str(&format!(
            "(to_timestamp(${}::double precision / 1e9), ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
            base + 6,
            base + 7,
            base + 8,
            base + 9
        ));
    }
    let mut q = sqlx::query(&query);
    for e in buf.iter() {
        let ts_for_time = if e.recv_ts_ns > 0 {
            e.recv_ts_ns
        } else {
            e.send_ts_ns
        };
        q = q
            .bind(ts_for_time as f64)
            .bind(&e.run_id)
            .bind(&e.bot_id)
            .bind(event_kind_str(e.event_type))
            .bind(&e.client_order_id)
            .bind(e.seq_no as i64)
            .bind(e.latency_ns as i64)
            .bind(e.send_ts_ns as i64)
            .bind(e.recv_ts_ns as i64);
    }
    q.execute(pool).await?;
    buf.clear();
    Ok(())
}

fn event_kind_str(k: EventKind) -> &'static str {
    match k {
        EventKind::OrderSent => "order_sent",
        EventKind::AckReceived => "ack_received",
        EventKind::FillReceived => "fill_received",
        EventKind::Timeout => "timeout",
        EventKind::Error => "error",
    }
}
