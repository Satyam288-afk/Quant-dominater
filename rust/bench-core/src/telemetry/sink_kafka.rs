// Kafka/Redpanda telemetry sink. Built only when `kafka` feature is on
// because rdkafka pulls librdkafka (native build).

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use super::event::TelemetryEvent;
use super::sink::TelemetrySink;

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    delivery_timeout: Duration,
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
        Ok(Self {
            producer,
            topic: topic.into(),
            delivery_timeout: Duration::from_secs(5),
        })
    }
}

#[async_trait]
impl TelemetrySink for KafkaSink {
    async fn emit(&self, event: TelemetryEvent) -> Result<()> {
        // Key on {run_id}:{bot_id} so per-bot ordering survives partitioning.
        let key = format!("{}:{}", event.run_id, event.bot_id);
        let payload = serde_json::to_vec(&event)?;
        let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
        // Fire-and-forget at this layer; rdkafka batches under the hood.
        if let Err((err, _)) = self.producer.send(record, self.delivery_timeout).await {
            return Err(err.into());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // FutureProducer::flush is blocking — run it on a blocking task.
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || {
            let _ = producer.flush(Duration::from_secs(10));
        })
        .await
        .ok();
        Ok(())
    }
}
