use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tokio::sync::mpsc;

mod aggregator;
mod sink;
mod source;

use aggregator::{Aggregator, RunSummary};
use bench_core::telemetry::TelemetryEvent;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SourceKind {
    File,
    #[cfg(feature = "kafka")]
    Kafka,
}

#[derive(Debug, Parser)]
#[command(about = "Aggregate bot-fleet telemetry into rolling percentiles and run summaries")]
struct Args {
    /// Where telemetry events come from. `file` reads a JSONL stream
    /// (compatible with bot-fleet's FileSink output). `kafka` consumes a
    /// Redpanda/Kafka topic (requires --features kafka at build time).
    #[arg(long, value_enum, default_value_t = SourceKind::File)]
    source: SourceKind,

    /// File source: path to telemetry.jsonl.
    #[arg(long, env = "TELEMETRY_INPUT", default_value = "telemetry.jsonl")]
    input: PathBuf,

    /// Where to write the aggregated run summary as JSON.
    #[arg(
        long,
        env = "TELEMETRY_SUMMARY_OUT",
        default_value = "telemetry-summary.json"
    )]
    summary_out: PathBuf,

    /// Restrict aggregation to a single run_id. Empty = all runs.
    #[arg(long, default_value = "")]
    run_id: String,

    /// Kafka brokers (used only when --source kafka).
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    kafka_brokers: String,

    /// Kafka topic (used only when --source kafka).
    #[arg(
        long,
        env = "KAFKA_TELEMETRY_TOPIC",
        default_value = "telemetry.events.v1"
    )]
    kafka_topic: String,

    /// Kafka consumer group id.
    #[arg(long, env = "KAFKA_GROUP_ID", default_value = "telemetry-ingester")]
    kafka_group_id: String,

    /// Optional Timescale connection string. When set, aggregates are also
    /// written to the `metrics_1s` hypertable. Requires --features timescale.
    #[arg(long, env = "TIMESCALE_URL", default_value = "")]
    timescale_url: String,

    /// Optional Redis connection string. When set, the live leaderboard
    /// hash + streams are populated. Requires --features redis-backend.
    #[arg(long, env = "REDIS_URL", default_value = "")]
    redis_url: String,

    /// How often to flush aggregates to sinks (milliseconds).
    #[arg(long, default_value_t = 1000)]
    flush_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let (tx, rx) = mpsc::channel::<TelemetryEvent>(16_384);

    // Shutdown plumbing. A SIGTERM/SIGINT (every k8s rolling deploy / eviction)
    // flips this watch; the Kafka source observes it, synchronously commits its
    // offset, and exits — so a restart resumes from the committed position
    // instead of re-reading the auto-commit window, and the in-flight channel
    // is drained rather than dropped.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        wait_for_shutdown().await;
        let _ = shutdown_tx.send(true);
    });

    // Spawn source. It pushes events into the channel and exits when done
    // (file mode: EOF; kafka mode: shutdown-signal driven, committing offsets).
    let source_handle = match args.source {
        SourceKind::File => {
            let path = args.input.clone();
            tokio::spawn(async move { source::file::run(path, tx).await })
        }
        #[cfg(feature = "kafka")]
        SourceKind::Kafka => {
            let brokers = args.kafka_brokers.clone();
            let topic = args.kafka_topic.clone();
            let group = args.kafka_group_id.clone();
            let shutdown = shutdown_rx.clone();
            tokio::spawn(
                async move { source::kafka::run(brokers, topic, group, tx, shutdown).await },
            )
        }
    };

    let run_filter = if args.run_id.is_empty() {
        None
    } else {
        Some(args.run_id.clone())
    };
    let mut aggregator = Aggregator::new(run_filter);

    // Optional live sinks. Built only when the corresponding cargo feature
    // is enabled AND the user passed a non-empty URL. Each sink runs as its
    // own task with its own mpsc; we fan out each incoming event.
    #[cfg(feature = "timescale")]
    let timescale_tx = if !args.timescale_url.is_empty() {
        match sink::timescale::spawn(args.timescale_url.clone(), args.flush_interval_ms, 1000).await
        {
            Ok(tx) => Some(tx),
            Err(err) => {
                tracing::error!(error = %err, "timescale sink disabled");
                None
            }
        }
    } else {
        None
    };
    #[cfg(feature = "redis-backend")]
    let redis_tx = if !args.redis_url.is_empty() {
        match sink::redis::spawn(args.redis_url.clone(), args.flush_interval_ms).await {
            Ok(tx) => Some(tx),
            Err(err) => {
                tracing::error!(error = %err, "redis sink disabled");
                None
            }
        }
    } else {
        None
    };

    // Drain the channel. We could also fan out to sinks here, but for now
    // we accumulate and flush at the end (and optionally on a ticker).
    let mut flush_ticker =
        tokio::time::interval(Duration::from_millis(args.flush_interval_ms.max(100)));
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut rx = rx;
    let mut events_seen: u64 = 0;
    // Bounded shutdown grace: on signal we keep draining buffered events (the
    // Kafka source commits and closes its sender, so rx reaches `None` quickly),
    // but cap the wait so a wedged source can't block shutdown forever.
    let mut main_shutdown = shutdown_rx.clone();
    let mut shutting_down = false;
    let grace = tokio::time::sleep(Duration::from_secs(86_400));
    tokio::pin!(grace);
    loop {
        tokio::select! {
            biased;
            _ = main_shutdown.changed(), if !shutting_down => {
                shutting_down = true;
                tracing::info!("shutdown signal received; draining buffered events (grace 5s)");
                grace.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
            }
            _ = &mut grace, if shutting_down => {
                tracing::warn!("shutdown grace elapsed; finalizing with what was drained");
                break;
            }
            evt = rx.recv() => {
                match evt {
                    Some(e) => {
                        aggregator.record(&e);
                        events_seen += 1;
                        #[cfg(feature = "timescale")]
                        if let Some(tx) = &timescale_tx {
                            let _ = tx.send(e.clone()).await;
                        }
                        #[cfg(feature = "redis-backend")]
                        if let Some(tx) = &redis_tx {
                            let _ = tx.send(e).await;
                        }
                    }
                    None => break,
                }
            }
            _ = flush_ticker.tick() => {
                // Ticker is kept so sinks can drain on idle. No-op here;
                // each sink owns its own flush cadence.
            }
        }
    }

    let _ = source_handle.await; // surface task panic if any (best-effort)

    let summary = aggregator.finalize();
    write_summary(&args.summary_out, &summary).await?;

    tracing::info!(
        events = events_seen,
        runs = summary.runs.len(),
        output = %args.summary_out.display(),
        "ingester complete"
    );
    println!(
        "{}",
        serde_json::to_string(&IngesterReport {
            events_processed: events_seen,
            runs: summary.runs.len(),
            summary_out: args.summary_out.display().to_string(),
        })?
    );

    Ok(())
}

/// Resolves when the process is asked to stop — SIGTERM or SIGINT on Unix
/// (what k8s sends on a rolling deploy / eviction), Ctrl-C elsewhere.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn write_summary(path: &PathBuf, summary: &RunSummary) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating parent dir for {:?}", path))?;
        }
    }
    let bytes = serde_json::to_vec_pretty(summary)?;
    tokio::fs::write(path, bytes)
        .await
        .with_context(|| format!("writing {:?}", path))?;
    Ok(())
}

#[derive(serde::Serialize)]
struct IngesterReport {
    events_processed: u64,
    runs: usize,
    summary_out: String,
}
