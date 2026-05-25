use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use super::event::TelemetryEvent;
use super::sink::TelemetrySink;

pub struct FileSink {
    writer: Arc<Mutex<File>>,
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
        Ok(Self {
            writer: Arc::new(Mutex::new(file)),
        })
    }
}

#[async_trait]
impl TelemetrySink for FileSink {
    async fn emit(&self, event: TelemetryEvent) -> Result<()> {
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        let mut guard = self.writer.lock().await;
        guard.write_all(&line).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.writer.lock().await;
        guard.flush().await?;
        Ok(())
    }
}
