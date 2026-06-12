// Kafka source. Gated behind the `kafka` cargo feature so the file pipeline
// builds without librdkafka. Wire-format mirrors bench-core's proto module.

use anyhow::{Context, Result};
use bench_core::telemetry::TelemetryEvent;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::statistics::Statistics;
use tokio::sync::{mpsc, watch};

/// Consumer context that surfaces per-partition consumer lag from librdkafka's
/// periodic statistics. Without this the only ingest-health signal is CPU, and
/// the HPA gates on CPU — but a sink-await stall (the ingester blocked on the
/// Timescale/Redis write) burns ~0 CPU while lag climbs, so it never scales.
/// Emitting `consumer_lag` lets the HPA (or an alert) gate on lag instead. This
/// is observability only: librdkafka calls `stats` on its own thread every
/// `statistics.interval.ms`; it never touches the consume hot path.
struct LagLoggingContext;

impl ClientContext for LagLoggingContext {
    fn stats(&self, stats: Statistics) {
        // `consumer_lag` is the broker high-watermark minus the committed
        // offset, i.e. the backlog. -1 means "not yet known" (no fetch since
        // assignment); skip those so we don't log noise on startup.
        for (topic_name, topic) in &stats.topics {
            for (partition_id, partition) in &topic.partitions {
                if partition.consumer_lag < 0 {
                    continue;
                }
                tracing::info!(
                    topic = %topic_name,
                    partition = *partition_id,
                    consumer_lag = partition.consumer_lag,
                    "kafka consumer lag"
                );
            }
        }
    }
}

impl ConsumerContext for LagLoggingContext {}

pub async fn run(
    brokers: String,
    topic: String,
    group_id: String,
    tx: mpsc::Sender<TelemetryEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let consumer: StreamConsumer<LagLoggingContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        // Emit consumer statistics (consumed via the context above) every 5s so
        // per-partition lag is observable for HPA/alerting.
        .set("statistics.interval.ms", "5000")
        .create_with_context(LagLoggingContext)
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
