# Live operations plan

TransitScope is designed to become a continuously updating assessment service, but the execution gate is currently closed.

## Current safe state

`RUN_LIVE_PIPELINE=false` is the default in `docker-compose.yml`. When the worker is started in `WORKER_MODE=live` with that value, it only writes a `paused_by_config` heartbeat. It does not call MAST, download a FITS product, run a model, mutate DuckDB, or advance the Stage 1 cursor.

The dashboard and `GET /api/monitoring/overview` are read-only. Refreshing them reads the latest published state and never triggers processing.

## What the operations view monitors

- **Universe:** configured 126,000-target universe, manifest coverage, processed count, remaining count, and the initial 500-target Stage 1 pool.
- **Stage pressure:** occupied slots and capacity for Stage 1 (500), Stage 2 (25), and Stage 3 (6), plus counts for Promising, Needs More Data, Inconclusive, and Known Match.
- **Models:** model version, evaluation count, evidence score, Stage 3 holdout F1, model lift over the baseline when available, classification counts, and data sufficiency.
- **Data plane:** provider runs, successes/failures, products downloaded/failed, coverage products/windows, recent batches, and recent pipeline events.
- **Sampling:** checkpoint cursor and policy, selection-bucket outcomes, and coordinate cells summarized into evidence-bearing or known-dense regions. The UI labels these signals as observed guidance until adaptive selection is enabled.

## Launch sequence

1. Deploy the dashboard/API with the execution gate still closed.
2. Move durable state from local DuckDB to a managed Postgres-compatible store and raw/normalized artifacts to durable object storage. The worker can continue to use Python and the existing model adapters.
3. Add a scheduler/queue with bounded concurrency, provider rate limits, retries, and idempotency keys. A multi-hour run is expected for the first 500-target pool; the dashboard should show queued, running, succeeded, deferred, failed, and paused work separately.
4. Seed and validate the Stage 1 manifest for the first 500 targets without dispatching jobs. Confirm that replacements are drawn only when a slot is released and that only Promising candidates can advance.
5. Enable execution deliberately by changing the worker environment to `RUN_LIVE_PIPELINE=true`. Keep the API read-only; the control-plane change should happen through the worker deployment or an authenticated operator workflow, not a public dashboard button.
6. Watch the ML Ops view for the first several cycles before increasing concurrency or enabling adaptive sampling. The current bounded acceleration profile is 16 targets per batch, four workers, and a 300-second poll interval; increase these only after provider health and CPU pressure are stable.

## Why this is not a Vercel-only workload

The dashboard is request/response work and can run on Vercel. The worker is different: it must survive requests ending, deployments, and process restarts while preserving progress. Serverless functions are stateless and time-limited, so a persistent container worker plus durable state is the safer fit for the long-running MAST/model pipeline. Vercel may still serve the frontend or read API, but it should not be treated as the database or the always-on worker.
