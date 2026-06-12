# console-api — ghcr.io/iicpc/console-api
#
# Multi-stage: Go builder -> distroless static. Stdlib-only module (no external
# deps, so no go.sum / go mod download step). Built from the repo root for a
# uniform `make images` context:
#   docker build -f services/console-api/Dockerfile -t ghcr.io/iicpc/console-api:latest .
#
# The committed ./console-api host binary lands in the builder stage but never
# the final image, which copies only the freshly compiled /out/console-api.
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY services/console-api/ services/console-api/
RUN cd services/console-api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/console-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/console-api /usr/local/bin/console-api
EXPOSE 9700
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/console-api"]
