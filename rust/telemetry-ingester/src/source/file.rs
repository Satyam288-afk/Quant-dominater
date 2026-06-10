use std::path::PathBuf;

use anyhow::{Context, Result};
use bench_core::telemetry::TelemetryEvent;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

pub async fn run(path: PathBuf, tx: mpsc::Sender<TelemetryEvent>) -> Result<()> {
    let file = File::open(&path)
        .await
        .with_context(|| format!("opening {:?}", path))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await.context("reading telemetry line")? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<TelemetryEvent>(&line) {
            Ok(evt) => {
                if tx.send(evt).await.is_err() {
                    // Downstream gone — clean exit.
                    return Ok(());
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "skipping malformed telemetry line");
            }
        }
    }
    Ok(())
}
