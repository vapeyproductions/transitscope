# Cloud deployment

TransitScope can be deployed without Docker Desktop on the development laptop.
The first cloud target is Railway because it can build Dockerfiles from GitHub,
run persistent services, and attach a durable volume.

## First deployment shape

The initial deployment uses two Railway services:

1. **transitscope-api-worker** uses `Dockerfile.api-worker` and has a persistent
   volume mounted at `/app/data`. It runs the FastAPI read layer and the gated
   Python worker in one service so both processes share the same DuckDB state.
2. **transitscope-web** uses `Dockerfile.web` and serves the Next.js dashboard.

This combined API/worker service is intentional. A Railway volume is attached
to a service, and the current local implementation relies on one DuckDB file
with a single writer. Splitting the processes before migrating to Postgres
would give the API and worker different database files.

## Safe launch state

The worker is paused by default:

```text
RUN_LIVE_PIPELINE=false
```

In this state the service writes a heartbeat but does not download MAST data,
run models, populate the 500-target Stage 1 pool, or advance the queue. Do
not change this variable until the API, volume, provider limits, and dashboard
have been checked in the cloud.

## Railway setup

1. Create a private GitHub repository and push this project. Do not commit
   `.env`, provider credentials, or runtime files under `data/`.
2. Create a Railway project and connect the GitHub repository.
3. Create the `transitscope-api-worker` service:
   - Dockerfile path: `Dockerfile.api-worker`
   - Public domain: generate one so the web service can reach the API
   - Healthcheck path: `/api/health`
   - Add a volume mounted at `/app/data`
4. Set these API/worker variables:

```text
APP_ENV=production
DATA_DIR=/app/data
WORKER_MODE=live
RUN_LIVE_PIPELINE=false
CORS_ALLOWED_ORIGINS=https://<web-domain>
OPS_CONTROL_TOKEN=<long-random-server-token>
```

The remaining queue and capacity variables already have safe defaults in the
Dockerfile and can be adjusted later from Railway variables.

5. Create the `transitscope-web` service from the same repository:
   - Dockerfile path: `Dockerfile.web`
   - `NEXT_PUBLIC_API_BASE_URL=` (empty, so the browser uses same-origin `/api`)
   - `API_BACKEND_URL=https://<api-domain>`
   - `OPS_PASSWORD=<private-monitor-password>`
   - `OPS_SESSION_SECRET=<long-random-session-secret>`
   - `OPS_CONTROL_TOKEN=<same-long-random-server-token-as-api-worker>`
   - Generate a public domain for the web service
6. Replace `<web-domain>` in the API service's `CORS_ALLOWED_ORIGINS`, then
   redeploy both services.
7. Verify:

```text
https://<api-domain>/api/health
https://<api-domain>/api/monitoring/overview
https://<web-domain>/
```

The ML Ops view requires the configured password. Its Pause worker button writes
a durable operator gate that the worker checks between cycles; its Resume worker
button only works when `RUN_LIVE_PIPELINE=true` and `PIPELINE_PAUSED=false`.
No MAST download starts while either deployment gate or the operator gate is
paused. A pause does not interrupt an in-flight cycle; it prevents the next one.

## When to enable processing

The first live run should be enabled as a controlled deployment change:

```text
RUN_LIVE_PIPELINE=true
```

Use the bounded acceleration profile of `STAGE1_BATCH_SIZE=32`,
`STAGE1_MAX_WORKERS=8`, `LIVE_POLL_SECONDS=300`, and a 500-target Stage 1 pool.
Watch the ML Ops dashboard, provider health, data volume, and worker heartbeat
before increasing concurrency. The initial pool should fill over hours, not
finish in a request or a single build.

## Follow-up scaling work

The combined service is appropriate for the first live cloud demonstration but
not the final high-availability architecture. Before scaling workers or adding
replicas, migrate durable relational state to managed Postgres and raw FITS /
normalized artifacts to object storage. Then the API and worker can become
independent services without sharing a local filesystem.

