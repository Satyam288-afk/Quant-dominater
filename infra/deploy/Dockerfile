# syntax=docker/dockerfile:1
# Single-container deploy of the console stack for a PaaS (Render / Railway / Fly).
# Exposes the browser console (console-api) on $PORT; the other 4 Go services run
# on container-localhost. The evaluator runs in LOCAL mode (the uploaded engine is
# built+run as a host subprocess) — gated by SANDBOX_ALLOW_UNSAFE_LOCAL=1, set in
# the entrypoint. The Go toolchain stays in the final image because the
# sandbox-runner compiles the uploaded engine at run time.

# ---- Rust builder: package the bot-fleet + validator release binaries ----
FROM rust:1-bookworm AS rust-build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY rust ./rust
COPY proto ./proto
COPY shared ./shared
# examples/rust-engine is a workspace member, so its manifest must exist for
# cargo to load the workspace (even though we only build bot-fleet + validator).
COPY examples ./examples
RUN cargo build --release -p bot-fleet -p validator

# ---- Go builder: prebuild the 5 console-stack service binaries ----
FROM golang:1.26-bookworm AS go-build
WORKDIR /src
COPY . .
RUN set -eux; mkdir -p /out; \
    for svc in submission-api sandbox-runner orchestrator leaderboard-api console-api; do \
      (cd "services/$svc" && go build -trimpath -o "/out/$svc" .); \
    done
# Pre-warm the module cache so the run-time local-mode build of an uploaded
# Go engine (gorilla/websocket etc.) resolves from cache and is fast/offline.
RUN cd examples/stub-engine && go mod download

# ---- Final runtime: Go toolchain (for run-time engine builds) + binaries + repo ----
FROM golang:1.26-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends bash curl ca-certificates git xxd \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
COPY --from=rust-build /src/target/release/bot-fleet /app/bin/bot-fleet
COPY --from=rust-build /src/target/release/validator  /app/bin/validator
COPY --from=go-build   /out/                          /app/bin/
# Carry the pre-warmed Go module cache for fast/offline run-time engine builds.
COPY --from=go-build   /go/pkg/mod                     /go/pkg/mod
ENV REPO_ROOT=/app \
    BOT_FLEET_BIN=/app/bin/bot-fleet \
    VALIDATOR_BIN=/app/bin/validator \
    SANDBOX_ALLOW_UNSAFE_LOCAL=1 \
    REQUIRE_AUTH=1 \
    STATE_DIR=/data \
    PORT=8080
EXPOSE 8080
CMD ["bash", "scripts/run-deploy.sh"]
