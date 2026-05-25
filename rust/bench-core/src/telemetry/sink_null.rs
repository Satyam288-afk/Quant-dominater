use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use async_trait::async_trait;

use super::event::TelemetryEvent;
use super::sink::TelemetrySink;

#[derive(Default)]
pub struct NullSink {
    count: AtomicUsize,
}

impl NullSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emitted(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TelemetrySink for NullSink {
    async fn emit(&self, _event: TelemetryEvent) -> Result<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
