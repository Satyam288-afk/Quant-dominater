# telemetry-ingester — ghcr.io/iicpc/telemetry-ingester (built --features live, per 30-telemetry-ingester.yaml)
#
# Multi-stage: cargo release builder -> debian slim runtime. Build from the repo
# root: the crate path-depends on ../bench-core, whose build.rs compiles
# ../../proto/benchmark.proto, so the whole workspace + proto/ must be in the
# context:
#   docker build -f rust/telemetry-ingester/Dockerfile -t ghcr.io/iicpc/telemetry-ingester:latest .
#
# --features live = kafka + timescale + redis-backend. kafka pulls rdkafka
# (cmake-build) which compiles librdkafka from source — hence cmake + a C/C++
# toolchain in the builder. protobuf-compiler backs prost-build. sqlx (timescale)
# and redis are pure-Rust here. It's a Kafka consumer group, so no port to EXPOSE.
FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake make g++ pkg-config libssl-dev libsasl2-dev zlib1g-dev \
        libcurl4-openssl-dev protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src

# Whole workspace: cargo must resolve every member listed in the root manifest.
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY rust/ rust/
COPY examples/ examples/

RUN cargo build --release -p telemetry-ingester --bin telemetry-ingester --features live \
    && cp target/release/telemetry-ingester /out-telemetry-ingester

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 libsasl2-2 libcurl4 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --no-create-home iicpc
COPY --from=build /out-telemetry-ingester /usr/local/bin/telemetry-ingester
USER iicpc
ENTRYPOINT ["telemetry-ingester"]
