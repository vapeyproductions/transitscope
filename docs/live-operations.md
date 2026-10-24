# Live operations plan

TransitScope is a continuously updating assessment service. The worker owns execution and publishes durable state; the authenticated ML Ops control endpoint can update only the durable operator pause gate.

## Current safe state

`RUN_LIVE_PIPELINE=false` is the default in `docker-compose.yml`. When the worker is started in `WORKER_MODE=live` with that value, it only writes a `paused_by_config` heartbeat. It does not call MAST, download a FITS product, run a model, mutate DuckDB, or advance the Stage 1 cursor.

Refreshing the dashboard reads the latest published state and never triggers processing. The monitoring overview and pause/resume endpoint require the authenticated ML Ops session; pause/resume only updates the durable operator gate and never runs a model directly.

## What the operations view monitors

- **Universe:** configured 126,000-target universe, manifest coverage, processed count, remaining count, and the initial 500-target Stage 1 pool.
- **Stage prioritization:** the Stage 1 top-100 priority window and Stage 2 top-100 reassessment window, plus counts for Promising, Needs More Data, Inconclusive, and Known Match. These are ranking windows, not caps on how many candidates a stage can retain. Stage 2 has no promotion cutoff: when Stage 3 calls for a candidate, it takes the highest-ranked eligible Promising signal. Stage 3 has no cutoff: it works its Needs More Data queue in rank order and is the only dispatcher for downstream movement.
- **Models:** model version, evaluation count, evidence score, Stage 3 holdout F1, model lift over the baseline when available, classification counts, and data sufficiency.
- **Data plane:** provider runs, successes/failures, products downloaded/failed, coverage products/windows, recent batches, and recent pipeline events.
- **Catalog audit:** after Stage 3 dispatches, the worker checks up to 100 previously unchecked Stage 2 candidates against the cached NASA catalog, records each check durably, and reports the remaining audit queue. This comparison does not rerun scientific models.
- **Sampling:** checkpoint cursor and policy, selection-bucket outcomes, and coordinate cells summarized into evidence-bearing or known-dense regions. The UI labels these signals as observed guidance until adaptive selection is enabled.

## Launch sequence

1. Deploy the dashboard/API with the execution gate still closed.
2. Move durable state from local DuckDB to a managed Postgres-compatible store and raw/normalized artifacts to durable object storage. The worker can continue to use Python and the existing model adapters.
3. Add a scheduler/queue with bounded concurrency, provider rate limits, retries, and idempotency keys. A multi-hour run is expected for the first 500-target pool; the dashboard should show queued, running, succeeded, deferred, failed, and paused work separately.
4. Seed and validate the Stage 1 manifest for the first 500 targets without dispatching jobs. Confirm that the continuous intake is bounded and checkpointed, that waterfall replacements are still drawn when downstream work is released, and that only Promising candidates can advance.
5. Enable execution deliberately by changing the worker environment to `RUN_LIVE_PIPELINE=true`. The authenticated ML Ops page can then pause or resume the operator gate without redeploying; deployment gates still take precedence. After the initial Stage 1 baseline, each worker cycle evaluates the small `STAGE1_CONTINUOUS_INTAKE_PER_CYCLE` batch from the unprocessed manifest. Stage 3 independently reassesses unresolved candidates and its durable resolution ledger calls the highest-ranked eligible Promising Stage 2 candidate. That Stage 2 handoff can cascade to a Stage 1 replacement; downstream stages still do not advance from an independent timer.
6. Watch the ML Ops view for the first several cycles before increasing concurrency or enabling adaptive sampling. The current worker uses bounded Stage 3 reassessment/dispatch budgets and a 300-second poll interval; reduce those budgets if provider health or CPU pressure becomes unstable.

## Why this is not a Vercel-only workload

The dashboard is request/response work and can run on Vercel. The worker is different: it must survive requests ending, deployments, and process restarts while preserving progress. Serverless functions are stateless and time-limited, so a persistent container worker plus durable state is the safer fit for the long-running MAST/model pipeline. Vercel may still serve the frontend or read API, but it should not be treated as the database or the always-on worker.
