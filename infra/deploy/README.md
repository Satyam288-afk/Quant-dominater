# One-click PaaS deploy (live demo URL)

A single Docker image that runs the whole **console stack** — submission-api,
sandbox-runner, orchestrator, leaderboard-api, and the **console-api** browser
gateway — so you get a **public URL** where you can upload an engine, run a
benchmark, and watch the leaderboard. Much lighter than the AWS EKS path; meant
for a live demo, not production.

- **Public port:** `console-api` on `$PORT` (the platform injects it; defaults 8080).
- **Evaluator:** **LOCAL mode** — the uploaded engine is compiled (`go build`) and
  run as a subprocess *inside this container*. Enabled via
  `SANDBOX_ALLOW_UNSAFE_LOCAL=1`. Fine for engines you control; **do not** host
  arbitrary untrusted submissions this way (no container isolation).
- **Auth:** every backend is fail-closed (`REQUIRE_AUTH=1`); a `SERVICE_AUTH_TOKEN`
  is auto-generated in the container if the platform doesn't inject one. The
  browser hits `console-api`, which forwards the token to the backends.
- **State:** submissions + leaderboard live under `/data` (ephemeral unless you
  attach a volume — fine for a demo; resets on redeploy).

## Build/run locally first (verify)
```bash
docker build -f infra/deploy/Dockerfile -t quant-demo .
docker run --rm -p 8080:8080 quant-demo
# open http://localhost:8080
```

## Deploy — pick one

### Railway (simplest, web UI)
1. railway.app > New Project > Deploy from GitHub repo.
2. Settings > set the Dockerfile path to `infra/deploy/Dockerfile` (or add a
   `railway.json` with `"dockerfilePath"`). Railway injects `$PORT` automatically.
3. (Optional) Variables > add `SERVICE_AUTH_TOKEN` = a random hex; otherwise it's
   auto-generated in-container. Give the service ~1GB.
4. Deploy → open the generated URL.

### Render (web UI, Blueprint)
1. render.com > New + > **Blueprint** > pick this repo. It reads
   `infra/deploy/render.yaml`.
2. It builds the Docker image and generates `SERVICE_AUTH_TOKEN`. Plan is
   `standard` (2GB) in the blueprint — the engine build can OOM on 512MB.
3. Deploy → open the `*.onrender.com` URL.

### Fly.io (CLI, Docker-native)
```bash
# edit infra/deploy/fly.toml: set a unique `app` name
fly apps create <your-app-name>
fly deploy -c infra/deploy/fly.toml
# optional pinned token: fly secrets set SERVICE_AUTH_TOKEN=$(openssl rand -hex 32)
fly open
```

## Caveats
- **Memory:** the in-container `go build` of the uploaded engine spikes RAM. Use
  **≥ 512MB, ideally 1GB**. Free 512MB tiers may OOM mid-build.
- **One public UI:** the upload/run/leaderboard **console** (`console-api`). The
  separate polished board SPA (`/board/`, `web/dist`) is not bundled (it's a
  local-only artifact); the console already shows the leaderboard.
- **Cold start:** first request after idle-spin-down can take a few seconds.
- This is the lightweight live-demo path. The scalable cloud deliverable is the
  Terraform + Kubernetes IaC under `infra/terraform` and `infra/k8s` (and the
  free local proof `make kind-scale-proof`).
