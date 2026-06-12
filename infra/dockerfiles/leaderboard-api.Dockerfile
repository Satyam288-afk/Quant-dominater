FROM golang:1.25-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /src/services/leaderboard-api
COPY services/leaderboard-api/go.mod services/leaderboard-api/go.sum ./
RUN go mod download
COPY services/leaderboard-api/ ./
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/leaderboard-api .

FROM alpine:3.20
RUN adduser -D -u 65532 appuser
WORKDIR /app
COPY --from=build /out/leaderboard-api /usr/local/bin/leaderboard-api
COPY web/leaderboard-ui /app/web/leaderboard-ui
COPY Cargo.toml /app/Cargo.toml
COPY proto /app/proto
USER 65532:65532
ENV REPO_ROOT=/app
ENV LEADERBOARD_UI_DIR=/app/web/leaderboard-ui
EXPOSE 9500
ENTRYPOINT ["/usr/local/bin/leaderboard-api"]
