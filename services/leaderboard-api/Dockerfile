# leaderboard-api — ghcr.io/iicpc/leaderboard-api
#
# Multi-stage: Go builder -> distroless static. Build from the repo root for a
# uniform `make images` context:
#   docker build -f services/leaderboard-api/Dockerfile -t ghcr.io/iicpc/leaderboard-api:latest .
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY services/leaderboard-api/go.mod services/leaderboard-api/go.sum services/leaderboard-api/
RUN cd services/leaderboard-api && go mod download

COPY services/leaderboard-api/ services/leaderboard-api/
RUN cd services/leaderboard-api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/leaderboard-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/leaderboard-api /usr/local/bin/leaderboard-api
EXPOSE 9500
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/leaderboard-api"]
