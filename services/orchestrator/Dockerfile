# orchestrator — ghcr.io/iicpc/orchestrator
#
# Multi-stage: Go builder -> distroless static. Build from the repo root so the
# `replace github.com/iicpc/benchmark-platform/shared/go => ../../shared/go`
# directive in go.mod resolves:
#   docker build -f services/orchestrator/Dockerfile -t ghcr.io/iicpc/orchestrator:latest .
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY shared/go/ shared/go/
COPY services/orchestrator/go.mod services/orchestrator/go.sum services/orchestrator/
RUN cd services/orchestrator && go mod download

COPY services/orchestrator/ services/orchestrator/
RUN cd services/orchestrator && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orchestrator .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/orchestrator /usr/local/bin/orchestrator
EXPOSE 9300
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/orchestrator"]
