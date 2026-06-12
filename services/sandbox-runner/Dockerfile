# sandbox-runner — ghcr.io/iicpc/sandbox-runner
#
# Multi-stage: Go builder -> distroless static. Self-contained module (no
# shared/go replace), but built from the repo root for a uniform `make images`
# context:
#   docker build -f services/sandbox-runner/Dockerfile -t ghcr.io/iicpc/sandbox-runner:latest .
FROM golang:1.25-alpine AS build
WORKDIR /src

COPY services/sandbox-runner/go.mod services/sandbox-runner/go.sum services/sandbox-runner/
RUN cd services/sandbox-runner && go mod download

COPY services/sandbox-runner/ services/sandbox-runner/
RUN cd services/sandbox-runner && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sandbox-runner .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sandbox-runner /usr/local/bin/sandbox-runner
EXPOSE 9200
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/sandbox-runner"]
