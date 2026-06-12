# Docker Compose

Local data-plane infrastructure for the full live demo: Redpanda (Kafka API),
TimescaleDB, and Redis. Object storage uses the `local://` artifact store today;
MinIO/S3 is a cloud-mode swap (see [docs/PRODUCTION_GAP_ANALYSIS.md](../../docs/PRODUCTION_GAP_ANALYSIS.md)),
not a service this compose file defines.

```bash
docker compose up -d --wait redpanda timescaledb redis
```

