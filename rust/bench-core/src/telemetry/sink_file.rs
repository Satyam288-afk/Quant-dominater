use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot};

use super::event::TelemetryEvent;
use super::sink::TelemetrySink;

enum Cmd {
    Event(TelemetryEvent),
    Flush(oneshot::Sender<()>),
}

/// Default bound on the in-flight telemetry queue. Large enough to absorb the
/// bursts a healthy writer drains between, small enough that a stalled writer
/// can't let the queue grow without bound and OOM the process under saturation.
const DEFAULT_CAPACITY: usize = 1 << 16;

/// File sink with a dedicated writer task. `emit()` is a non-blocking channel
/// send; the spawned task owns a 64 KiB `BufWriter` over the file.
///
/// The previous implementation (`Mutex<File>` + unbuffered `write_all` per
/// event) ran INSIDE every bot's latency-measuring loop — 2+ emits per order,
/// all bots convoying on one lock. Measured: ~7.9us per emit uncontended vs
/// ~135ns for the channel send; end-to-end the convoy inflated healthy-load
/// p99 ~1.9x and at saturation collapsed TPS ~2x with mass timeouts. A
/// benchmarking platform must not let its own telemetry distort the numbers
/// it reports.
///
/// The queue is BOUNDED: an unbounded channel let RAM grow without limit when
/// the writer lagged under saturation (a slow/stalled disk would back the whole
/// run's telemetry up in memory). On a full queue `emit()` DROPS the event and
/// bumps a counter ([`dropped`]) rather than blocking the latency-measuring bot
/// loop — telemetry is best-effort and must never apply backpressure to the
/// measurement path. `flush()` is delivered reliably (it blocks briefly if the
/// queue is momentarily full), so "flush returned ⇒ every accepted prior emit
/// is written" still holds for run teardown.
pub struct FileSink {
    tx: mpsc::Sender<Cmd>,
    dropped: Arc<AtomicU64>,
}

impl FileSink {
    pub async fn create(path: PathBuf) -> Result<Self> {
        Self::create_with_capacity(path, DEFAULT_CAPACITY).await
    }

    /// Like [`create`], but with an explicit bound on the in-flight queue.
    pub async fn create_with_capacity(path: PathBuf, capacity: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating parent dir for {:?}", path))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .with_context(|| format!("opening telemetry file {:?}", path))?;
        let (tx, mut rx) = mpsc::channel::<Cmd>(capacity.max(1));
        tokio::spawn(async move {
            let mut writer = BufWriter::with_capacity(1 << 16, file);
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::Event(event) => {
                        if let Ok(mut line) = serde_json::to_vec(&event) {
                            line.push(b'\n');
                            let _ = writer.write_all(&line).await;
                        }
                    }
                    Cmd::Flush(ack) => {
                        let _ = writer.flush().await;
                        let _ = ack.send(());
                    }
                }
            }
            // Sender dropped (sink owner gone): flush whatever remains.
            let _ = writer.flush().await;
        });
        Ok(Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Number of telemetry events dropped because the bounded queue was full.
    /// Non-zero means the writer could not keep up with the emit rate; the
    /// dropped events are absent from the file but the measurement path was
    /// never blocked.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TelemetrySink for FileSink {
    async fn emit(&self, event: TelemetryEvent) -> Result<()> {
        // Non-blocking: never apply backpressure to the bot's measurement loop.
        // A full queue means the writer is lagging — drop and count instead of
        // letting the in-flight queue (and RAM) grow without bound.
        match self.tx.try_send(Cmd::Event(event)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("telemetry writer task gone"))
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        // Flush must be delivered, so block briefly if the queue is momentarily
        // full (it is rare — flush runs at teardown, not on the hot path).
        if self.tx.send(Cmd::Flush(ack_tx)).await.is_err() {
            // Writer already exited (and flushed on the way out).
            return Ok(());
        }
        let _ = ack_rx.await;
        Ok(())
    }
}
