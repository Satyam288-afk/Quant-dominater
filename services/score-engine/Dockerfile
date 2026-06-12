# score-engine — ghcr.io/iicpc/score-engine
#
# Multi-stage: Go builder -> distroless static. Stdlib-only module (no external
# deps, so no go.sum / go mod download step). Built from the repo root for a
# uniform `make images` context:
#   docker build -f services/score-engine/Dockerfile -t ghcr.io/iicpc/score-engine:latest .
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY services/score-engine/ services/score-engine/
RUN cd services/score-engine && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/score-engine .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/score-engine /usr/local/bin/score-engine
EXPOSE 9400
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/score-engine"]
