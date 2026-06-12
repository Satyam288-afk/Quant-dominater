FROM rust:1-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    g++ \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY rust ./rust
RUN cargo build --release -p telemetry-ingester --features live

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --uid 65532 --system --create-home appuser
COPY --from=build /src/target/release/telemetry-ingester /usr/local/bin/telemetry-ingester
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/telemetry-ingester"]
