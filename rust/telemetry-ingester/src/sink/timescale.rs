use std::fmt::Write as _;

use anyhow::{Context, Result};
use bench_core::telemetry::{EventKind, TelemetryEvent};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::postgres::PgPoolCopyExt;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};

/// Spawn the Timescale sink. It owns a connection pool and batches incoming
/// telemetry events into the metrics_raw hypertable. The summary aggregates
/// produced by the in-process aggregator are written separately at finalize.
pub async fn spawn(
    url: String,
    flush_ms: u64,
    batch_size: usize,
) -> Result<mpsc::Sender<TelemetryEvent>> {
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .with_context(|| format!("connecting timescale {url}"))?;
    let (tx, mut rx) = mpsc::channel::<TelemetryEvent>(16_384);
    tokio::spawn(async move {
        let mut buf: Vec<TelemetryEvent> = Vec::with_capacity(batch_size);
        let mut ticker = interval(Duration::from_millis(flush_ms.max(50)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(e) => {
                            buf.push(e);
                            if buf.len() >= batch_size {
                                if let Err(err) = flush(&pool, &mut buf).await {
                                    tracing::warn!(error=%err, "timescale flush error");
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        if let Err(err) = flush(&pool, &mut buf).await {
                            tracing::warn!(error=%err, "timescale flush error");
                        }
                    }
                }
            }
        }
        if !buf.is_empty() {
            let _ = flush(&pool, &mut buf).await;
        }
    });
    Ok(tx)
}

async fn flush(pool: &PgPool, buf: &mut Vec<TelemetryEvent>) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    // Bulk-load via COPY ... FROM STDIN instead of a string-built multi-row
    // INSERT. COPY skips the per-row parse/plan/bind overhead of INSERT, and
    // we pre-render the `time` column to an ISO-8601 literal client-side so
    // Postgres no longer evaluates `to_timestamp($n/1e9)` once per row — both
    // cut server-side CPU on the ingest hot path. Column order matches the
    // metrics_raw hypertable exactly. The batch is the COPY unit, mirroring
    // the old INSERT's flush cadence.
    let mut copy = pool
        .copy_in_raw(
            "COPY metrics_raw \
             (time, run_id, bot_id, event_type, client_order_id, seq_no, latency_ns, send_ts_ns, recv_ts_ns) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("starting metrics_raw COPY")?;

    let mut row = String::with_capacity(160);
    for e in buf.iter() {
        let ts_for_time = if e.recv_ts_ns > 0 {
            e.recv_ts_ns
        } else {
            e.send_ts_ns
        };
        row.clear();
        // Text COPY format: tab-separated columns, `\N` for NULL, terminated
        // by a newline. Text fields are escaped so tabs/newlines/backslashes in
        // an id can't corrupt the stream.
        push_iso8601_utc(&mut row, ts_for_time);
        row.push('\t');
        push_escaped(&mut row, &e.run_id);
        row.push('\t');
        push_escaped(&mut row, &e.bot_id);
        row.push('\t');
        push_escaped(&mut row, event_kind_str(e.event_type));
        row.push('\t');
        push_escaped(&mut row, &e.client_order_id);
        row.push('\t');
        let _ = write!(row, "{}", e.seq_no as i64);
        row.push('\t');
        let _ = write!(row, "{}", e.latency_ns as i64);
        row.push('\t');
        let _ = write!(row, "{}", e.send_ts_ns as i64);
        row.push('\t');
        let _ = write!(row, "{}", e.recv_ts_ns as i64);
        row.push('\n');
        copy.send(row.as_bytes())
            .await
            .context("streaming metrics_raw COPY row")?;
    }
    copy.finish().await.context("finishing metrics_raw COPY")?;
    buf.clear();
    Ok(())
}

/// Append `value` to `out` with COPY text-format escaping (backslash, tab,
/// newline, carriage return). Other characters pass through unchanged.
fn push_escaped(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
}

/// Render an epoch-nanosecond timestamp as an ISO-8601 UTC literal that
/// Postgres parses into TIMESTAMPTZ, e.g. `2024-01-02 03:04:05.123456+00`.
/// Done client-side so the server never runs `to_timestamp()` per row.
fn push_iso8601_utc(out: &mut String, ts_ns: u64) {
    let secs = (ts_ns / 1_000_000_000) as i64;
    let nanos = (ts_ns % 1_000_000_000) as u32;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    // Microsecond resolution: TIMESTAMPTZ stores microseconds, so emitting more
    // would be silently truncated by Postgres anyway. Matches the precision the
    // old `to_timestamp(ns/1e9)` path produced.
    let micros = nanos / 1_000;
    let _ = write!(
        out,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00",
        year, month, day, hour, minute, second, micros
    );
}

/// Convert a count of days since the Unix epoch (1970-01-01) into a civil
/// `(year, month, day)`. Howard Hinnant's `civil_from_days` algorithm, valid
/// across the full TIMESTAMPTZ range.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn event_kind_str(k: EventKind) -> &'static str {
    match k {
        EventKind::OrderSent => "order_sent",
        EventKind::AckReceived => "ack_received",
        EventKind::FillReceived => "fill_received",
        EventKind::Timeout => "timeout",
        EventKind::Error => "error",
    }
}
