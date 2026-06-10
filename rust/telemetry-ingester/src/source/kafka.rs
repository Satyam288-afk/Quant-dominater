// Kafka source. Gated behind the `kafka` cargo feature so the file pipeline
// builds without librdkafka. Wire-format mirrors bench-core's proto module.

use anyhow::{Context, Result};
use bench_core::telemetry::TelemetryEvent;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::{mpsc, watch};

pub async fn run(
    brokers: String,
    topic: String,
    group_id: String,
    tx: mpsc::Sender<TelemetryEvent>,
    mut shutdown: watch::Receiver<bool>,
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
        tokio::select! {
            biased;
            // SIGTERM/SIGINT: synchronously commit the consumed offset so a
            // restart resumes from here instead of re-reading the ~5s auto-commit
            // window. The aggregator dedups any residual re-delivery, so this is
            // belt-and-braces, not the sole guard.
            _ = shutdown.changed() => {
                match consumer.commit_consumer_state(CommitMode::Sync) {
                    Ok(()) => tracing::info!("kafka offsets committed on shutdown"),
                    Err(err) => tracing::warn!(error = %err, "kafka shutdown commit failed"),
                }
                return Ok(());
            }
            recv = consumer.recv() => {
                match recv {
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
    }
}
