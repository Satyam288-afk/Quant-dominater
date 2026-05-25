use anyhow::Result;
use async_trait::async_trait;

use super::event::TelemetryEvent;

#[async_trait]
pub trait TelemetrySink: Send + Sync {
    async fn emit(&self, event: TelemetryEvent) -> Result<()>;

    async fn emit_batch(&self, events: Vec<TelemetryEvent>) -> Result<()> {
        for e in events {
            self.emit(e).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
