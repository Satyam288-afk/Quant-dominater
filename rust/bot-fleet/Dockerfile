# bot-fleet — ghcr.io/iicpc/bot-fleet (built --features kafka, per 31-bot-fleet-job.yaml)
#
# Multi-stage: cargo release builder -> debian slim runtime. Build from the repo
# root: bot-fleet is a workspace member that path-depends on ../bench-core, and
# bench-core's build.rs compiles ../../proto/benchmark.proto, so the whole
# workspace + proto/ must be in the context:
#   docker build -f rust/bot-fleet/Dockerfile -t ghcr.io/iicpc/bot-fleet:latest .
#
# --features kafka pulls rdkafka (cmake-build) which compiles librdkafka from
# source — hence cmake + a C/C++ toolchain in the builder. protobuf-compiler
# backs prost-build. The release profile (LTO, codegen-units=1) is inherited
# from the workspace Cargo.toml, so this is a slow but tail-optimised build.
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

RUN cargo build --release -p bot-fleet --bin bot-fleet --features kafka \
    && cp target/release/bot-fleet /out-bot-fleet

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 libsasl2-2 libcurl4 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --no-create-home iicpc
COPY --from=build /out-bot-fleet /usr/local/bin/bot-fleet
USER iicpc
ENTRYPOINT ["bot-fleet"]
