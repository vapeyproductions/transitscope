# TransitScope

TransitScope is a Dockerized astronomy observatory for assessing potential exoplanet candidates from public stellar observations.

## Local development

```bash
docker compose up --build
```

- Web dashboard: http://localhost:3000
- API health: http://localhost:8000/api/health
- API status: http://localhost:8000/api/status
- ML operations view: open the `ML Ops` tab, or query http://localhost:8000/api/monitoring/overview

The `data/` directory is a durable local volume for provider cache, normalized data, DuckDB state, and replay history. The current stack includes the official MAST/TESS probe, bounded/checkpointed Stage 1 processing, Stage 2 historical repeatability, Stage 3 chronological holdout prediction, NASA Exoplanet Archive matching, Overall/stage ranking snapshots, replay APIs, and the candidate investigation modal. The 126,000-row universe is represented as a durable queue; the local demo downloads a bounded real seed set so it can run official data without pretending that 126,000 FITS files are already local.

The compose worker is intentionally paused by default with `RUN_LIVE_PIPELINE=false`. This is a safety gate: opening the site, refreshing the dashboard, or inspecting ML Ops never starts MAST downloads or model jobs. The live worker also honors `PIPELINE_PAUSED`; it defaults to `true` so a deployment cannot resume ingestion accidentally. Set `PIPELINE_PAUSED=false` only after the public deployment, durable state, provider limits, and monitoring have been reviewed.

## ML operations and eventual deployment

The `ML Ops` tab is a read-only control-plane view for the system that will run the long-lived search. It reports the configured 500-target Stage 1 pool, progress through the 126,000-target manifest, occupied Stage 1/2/3 slots, status counts, model versions and aggregate metrics, MAST/catalog provider health, archive coverage watermarks, recent batches/events, selection buckets, and 10-degree sky-cell outcome signals. Sampling signals are descriptive until adaptive replacement sampling is explicitly enabled.

For a public always-updating launch, keep the responsibilities separate even if they are deployed from one repository:

- The web/API layer serves the dashboard and read-only monitoring endpoints.
- A durable database stores manifests, candidates, evaluations, ranking snapshots, events, checkpoints, and health metrics.
- A persistent Python worker performs rate-limited MAST ingestion and CPU model jobs. A queue or scheduler dispatches bounded jobs; it is not a request handler.
- Raw FITS and normalized artifacts live in durable object storage or a mounted volume, not an ephemeral serverless filesystem.

Vercel can host the Next.js dashboard, but the current DuckDB-plus-worker arrangement is a better fit for an always-on container host. The Next.js app supports an `API_BACKEND_URL` rewrite so the same web build can point at a separately deployed API; the compose setup uses `http://api:8000`. This keeps the deployment choice reversible while the pipeline remains paused.

The first cloud deployment shape is documented in [`docs/cloud-deployment.md`](docs/cloud-deployment.md). It uses Railway with a combined API/worker service and a persistent `/app/data` volume, plus a separate Next.js web service. This keeps the current single-writer DuckDB design safe while the worker is migrated to managed Postgres/object storage for later scale.

## MAST/TESS probe

Run the bounded official-data probe from the repository root:

```bash
python scripts/probe_mast.py
```

The default configuration queries one public Sector 1 TESS target, selects its SPOC light curve, caches the FITS artifact under `data/raw/`, and writes a checksum/provenance manifest under `data/manifests/`. Runtime data is ignored by Git.

## Stage 1 batching

Populate a durable queue from the NASA Exoplanet Archive TOI table and official MAST/TESS SPOC light curves. This downloads a bounded real seed set and keeps the remaining 126,000-target queue explicit as deferred work:

```bash
docker compose run --rm worker python populate_real_targets.py \
  --target-count 8 \
  --universe-size 126000 \
  --catalog-rows 500 \
  --force-refresh
```

For a synthetic/metadata-first queue, create a durable manifest directly. A probe manifest can seed one real MAST/TESS target; remaining queue slots stay metadata-only until the provider adapter attaches products:

```bash
docker compose run --rm worker python prepare_target_universe.py \
  --universe-size 126000 \
  --probe-manifest /app/data/manifests/<successful-probe-manifest>.json
```

Run a bounded batch and resume it later from its DuckDB checkpoint:

```bash
docker compose run --rm worker python run_stage1_batch.py \
  --batch-size 16 \
  --max-workers 4
```

The live Docker worker uses a 500-target Stage 1 processing pool, bounded CPU parallelism, checkpointing, and an unprocessed-manifest replacement queue. It displays the top 500 rows per ranking through 100-row pages. Stage 2 and Stage 3 use independent promotion windows of 25 and 6 by default. Only `Promising` candidates advance; `Inconclusive` candidates remain visible in history but release their processing slot; `Needs More Data` holds its slot until an unexamined archive/live window is available; and known matches are removed from active views while remaining in replay history. Each published change saves ranking snapshots.

The accelerated live profile evaluates up to 16 fresh Stage 1 targets per batch with four CPU workers and polls every five minutes. The live cycle can perform two Stage 1 fills when the pool has room, so the initial fill can reach roughly 32 fresh targets per five-minute cycle before provider latency, failures, and refresh work are included.

Stage 1 now performs an iterative multi-signal search. It records the strongest periodic dimming signal, approximately removes that signal from the normalized light curve, and searches the residual for up to three additional signals when the preceding signal has sufficient SNR. Each child is identified by the parent target plus a durable signal fingerprint derived from period, phase, and duration; the extraction ordinal is metadata only. Existing legacy single-signal rows are matched by target/period/phase tolerance when possible, so their history can continue under the same candidate identity. Every child carries its signal family into Steps 2 and 3, where sibling transit windows are removed before repeatability and holdout evaluation.

The paused migration command backfills explored targets without starting the live heartbeat:

```bash
docker compose run --rm worker python migrate_multisignal.py --max-targets 25
```

It preserves children already in Stage 2/3, creates independent children for newly detected signals, and leaves the normal worker gate unchanged. Run it only while `RUN_LIVE_PIPELINE=false` and review the published ranking before enabling live processing.

## Stage 2 historical validation

Stage 2 reuses the Stage 1 period and timing instead of starting a new search. It can evaluate cached products, a history manifest, or a bounded list of MAST sectors:

```bash
docker compose run --rm worker python run_stage2.py \
  --candidate-id <candidate-id> \
  --fetch-sectors 1,2,3
```

The live orchestrator evaluates the current operational top-X from Stage 1; it does not auto-promote a candidate solely because it is labeled `Promising`. Results with insufficient historical coverage remain `Needs More Data`, and the processor can backfill older archive history when a recent two-year window is too sparse.

## Stage 3 chronological holdout prediction

Stage 3 sorts the available observations, trains a CPU-capable phase-feature logistic model on the first two chronological windows, and evaluates dimming predictions on the third. It records precision, recall, F1, event recovery/accuracy, timing error, depth error, model versions, and the window boundaries. A transparent phase-window baseline is evaluated on the same holdout for comparison:

```bash
docker compose run --rm worker python run_stage3.py \
  --candidate-id <candidate-id> \
  --history-manifest /app/data/manifests/<history-manifest>.json
```

The live orchestrator evaluates the current operational top-X from Stage 2 using the same capacity rule. Stage 3 results are assessments of candidate evidence and never scientific confirmation. Published evaluations and ranking snapshots are stored in DuckDB so replay can read them without rerunning the model.

## Known-object matching and Overall ranking

The catalog step queries a narrow, cached projection from the NASA Exoplanet Archive TAP service. Matching requires a compatible target identifier or a coordinate match with compatible period evidence; tolerances are recorded with each mapping. A match is a catalog mapping event, not TransitScope scientific confirmation:

```bash
docker compose run --rm worker python run_catalog_match.py \
  --candidate-id <candidate-id> \
  --force-refresh \
  --dry-run
```

Remove `--dry-run` to publish a catalog mapping. The candidate becomes an inactive `Known Match`, remains available in history, and is removed from Overall and all stage ranking snapshots. The remaining active candidates are re-ranked. In replay, the removal event is returned separately so the UI can briefly show an `Already Known` exit row before animating it out of the ranking.

## Project status

The application assesses potential planets and does not claim scientific confirmation. The production path currently uses an explainable Stage 1 Box Least Squares evidence score, Stage 2 repeatability features, and a CPU-capable Stage 3 logistic model adapter with a transparent baseline. A neural-network experiment remains optional and is not claimed as part of this draft. See `docs/hackathon-build/` for the scope, product requirements, technical specification, build checklist, and item-by-item verification notes.

## Replay and candidate investigation

Replay is a read-only view over persisted ranking snapshots and pipeline events:

```text
GET /api/replay/{view}/meta
GET /api/replay/{view}/snapshot?sequence={n}
GET /api/replay/{view}/events?start_sequence={n}&end_sequence={n}
```

The dashboard keeps an independent replay playhead for Overall, Stage 1, Stage 2, and Stage 3. It supports empty-start state, play/pause, approximate dragging, and playback speed. Opening a ranking row loads its normalized light curve, stage-specific metrics, current activity, next action, and lifecycle history in a modal. Replay reads saved state only; it does not call MAST, the NASA Exoplanet Archive, or a model.

Live ranking pages use the same read-only contract, for example:

```text
GET /api/rankings?view=stage1&limit=100&page=1
```

The response reports the 500-row display cap, total ranked candidates, and available pages.

## Demo walkthrough

Use a 1–3 minute recording with this sequence:

1. Show the Overall dashboard and explain that ranks are provisional evidence, not confirmation.
2. Switch through Stage 1, Stage 2, and Stage 3; point out status-gated movement and the Stage 3 `Prime Candidate` label.
3. Open one candidate and show its light curve, metrics, current activity, and pipeline history.
4. Start the active tab's replay, pause it at a rank change, and show that the replay reads saved snapshots without rerunning models.
5. Show an `Already Known` removal and the Identified tab when the final demo dataset contains a catalog match.

If the live archive is quiet, use the persisted replay timeline or a documented local fixture. Do not imply that a candidate is scientifically confirmed.

## Verification

Build the containers and run the API replay test:

```bash
docker compose build api web worker
docker compose run --rm api python -m unittest discover -s tests -v
docker compose up -d api web worker
```

The worker tests can be run in the worker image with:

```bash
docker compose run --rm worker python -m unittest discover -s tests -p 'test_*.py' -v
```

For a real-data demonstration, run the MAST probe, prepare or resume a bounded target batch, open `http://localhost:3000`, and use the Overall/Stage tabs, Replay control, and candidate row. Runtime data and provider caches are intentionally ignored by Git.

