// Kafka/Redpanda telemetry sink. Built only when `kafka` feature is on
// because rdkafka pulls librdkafka (native build).

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio::sync::{mpsc, oneshot};

use super::event::TelemetryEvent;
use super::sink::TelemetrySink;

enum Cmd {
    Event(TelemetryEvent),
    Flush(oneshot::Sender<()>),
}

/// Kafka sink with a dedicated producer task. `emit()` is a non-blocking
/// channel send; the task enqueues records via `send_result` WITHOUT awaiting
/// the delivery report inline.
///
/// The previous implementation awaited each delivery future inside `emit()`:
/// with linger.ms=5 that is a measured ~5.9ms hard floor per emit, capping a
/// bot task at ~168 sequential emits/s (~84 orders/s at 2 emits per order) —
/// on the live backend it cost 66% of offered TPS in a bot-loop simulation.
/// Delivery reports are still consumed: the task drains them in batches of
/// 1024 (by which point the early ones have long resolved) and counts
/// failures, reported at flush/teardown instead of per-call.
pub struct KafkaSink {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: impl Into<String>) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .set("acks", "1")
            .set("queue.buffering.max.messages", "200000")
            .create()
            .context("building kafka producer")?;
        let topic: String = topic.into();
        let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();

        tokio::spawn(async move {
            let mut inflight = Vec::with_capacity(1024);
            let mut delivery_errors: u64 = 0;
            let mut enqueue_errors: u64 = 0;

            async fn drain(
                inflight: &mut Vec<rdkafka::producer::DeliveryFuture>,
                errors: &mut u64,
            ) {
                for fut in inflight.drain(..) {
                    match fut.await {
                        Ok(Ok(_)) => {}
                        _ => *errors += 1,
                    }
                }
            }

            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::Event(event) => {
                        let key = format!("{}:{}", event.run_id, event.bot_id);
                        let payload = match serde_json::to_vec(&event) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        let record = FutureRecord::to(&topic).key(&key).payload(&payload);
                        match producer.send_result(record) {
                            Ok(fut) => inflight.push(fut),
                            Err(_) => enqueue_errors += 1, // local queue full
                        }
                        if inflight.len() >= 1024 {
                            drain(&mut inflight, &mut delivery_errors).await;
                        }
                    }
                    Cmd::Flush(ack) => {
                        drain(&mut inflight, &mut delivery_errors).await;
                        let p = producer.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            let _ = p.flush(Duration::from_secs(10));
                        })
                        .await;
                        if delivery_errors > 0 || enqueue_errors > 0 {
                            eprintln!(
                                "[kafka-sink] delivery_errors={delivery_errors} enqueue_errors={enqueue_errors}"
                            );
                        }
                        let _ = ack.send(());
                    }
                }
            }
            // Sender dropped: settle whatever is still in flight.
            drain(&mut inflight, &mut delivery_errors).await;
            if delivery_errors > 0 || enqueue_errors > 0 {
                eprintln!(
                    "[kafka-sink] delivery_errors={delivery_errors} enqueue_errors={enqueue_errors}"
                );
            }
        });

        Ok(Self { tx })
    }
}

#[async_trait]
impl TelemetrySink for KafkaSink {
    async fn emit(&self, event: TelemetryEvent) -> Result<()> {
        self.tx
            .send(Cmd::Event(event))
            .map_err(|_| anyhow::anyhow!("kafka producer task gone"))?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(Cmd::Flush(ack_tx)).is_err() {
            return Ok(());
        }
        let _ = ack_rx.await;
        Ok(())
    }
}
