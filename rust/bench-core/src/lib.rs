pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/benchmark.v1.rs"));
}

pub mod metrics;
pub mod score;
pub mod shard;
pub mod telemetry;
pub mod time;
