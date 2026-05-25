pub mod event;
pub mod sink;
pub mod sink_file;
pub mod sink_null;

#[cfg(feature = "kafka")]
pub mod sink_kafka;

pub use event::{EventKind, TelemetryEvent};
pub use sink::TelemetrySink;
pub use sink_file::FileSink;
pub use sink_null::NullSink;

#[cfg(feature = "kafka")]
pub use sink_kafka::KafkaSink;
