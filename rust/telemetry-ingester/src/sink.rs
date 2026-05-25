// Optional live sinks. Each module is gated behind its feature flag so the
// default file-only build pulls no native deps (no librdkafka, no libpq).

#[cfg(feature = "timescale")]
pub mod timescale;

#[cfg(feature = "redis-backend")]
pub mod redis;
