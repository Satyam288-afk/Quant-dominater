use std::path::PathBuf;

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
/// `flush()` round-trips a oneshot through the writer task, so "flush returned
/// ⇒ every prior emit is written" still holds for run teardown.
pub struct FileSink {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl FileSink {
    pub async fn create(path: PathBuf) -> Result<Self> {
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
        let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();
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
        Ok(Self { tx })
    }
}

#[async_trait]
impl TelemetrySink for FileSink {
    async fn emit(&self, event: TelemetryEvent) -> Result<()> {
        self.tx
            .send(Cmd::Event(event))
            .map_err(|_| anyhow::anyhow!("telemetry writer task gone"))?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(Cmd::Flush(ack_tx)).is_err() {
            // Writer already exited (and flushed on the way out).
            return Ok(());
        }
        let _ = ack_rx.await;
        Ok(())
    }
}
