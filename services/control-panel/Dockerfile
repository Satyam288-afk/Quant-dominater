# control-panel — ghcr.io/iicpc/control-panel
#
# Multi-stage: Go builder -> distroless static. Stdlib-only module (no external
# deps, so no go.sum / go mod download step). Built from the repo root for a
# uniform `make images` context:
#   docker build -f services/control-panel/Dockerfile -t ghcr.io/iicpc/control-panel:latest .
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY services/control-panel/ services/control-panel/
RUN cd services/control-panel && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/control-panel .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/control-panel /usr/local/bin/control-panel
EXPOSE 9000
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/control-panel"]
