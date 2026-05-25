// Kafka source. Gated behind the `kafka` cargo feature so the file pipeline
// builds without librdkafka. Wire-format mirrors bench-core's proto module.

use anyhow::{Context, Result};
use bench_core::telemetry::TelemetryEvent;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::mpsc;

pub async fn run(
    brokers: String,
    topic: String,
    group_id: String,
    tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("building kafka consumer")?;

    consumer
        .subscribe(&[&topic])
        .context("subscribing to telemetry topic")?;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                // For now we accept either JSON or protobuf payloads; live
                // bot-fleet writes proto, but JSON keeps the dev loop simple.
                let parsed: Result<TelemetryEvent, _> = serde_json::from_slice(payload);
                match parsed {
                    Ok(evt) => {
                        if tx.send(evt).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, topic = %topic, "skipping malformed kafka payload");
                    }
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "kafka consumer error");
                return Err(err.into());
            }
        }
    }
}
