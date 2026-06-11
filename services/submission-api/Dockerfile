# submission-api — ghcr.io/iicpc/submission-api
#
# Multi-stage: Go builder -> distroless static. Build from the repo root so the
# `replace github.com/iicpc/benchmark-platform/shared/go => ../../shared/go`
# directive in go.mod resolves:
#   docker build -f services/submission-api/Dockerfile -t ghcr.io/iicpc/submission-api:latest .
FROM golang:1.22-alpine AS build
WORKDIR /src

# Module graph first for a cached dependency layer.
COPY shared/go/ shared/go/
COPY services/submission-api/go.mod services/submission-api/go.sum services/submission-api/
RUN cd services/submission-api && go mod download

COPY services/submission-api/ services/submission-api/
RUN cd services/submission-api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/submission-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/submission-api /usr/local/bin/submission-api
EXPOSE 9100
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/submission-api"]
