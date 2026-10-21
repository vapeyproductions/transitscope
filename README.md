# TransitScope

TransitScope is a Dockerized astronomy observatory for assessing potential exoplanet candidates from public stellar observations.

## Local development

```bash
docker compose up --build
```

- Web dashboard: http://localhost:3000
- API health: http://localhost:8000/api/health
- API status: http://localhost:8000/api/status

The `data/` directory is a durable local volume for provider cache, normalized data, DuckDB state, and replay history. The current stack includes the official MAST/TESS probe, bounded/checkpointed Stage 1 processing, Stage 2 historical repeatability, Stage 3 chronological holdout prediction, NASA Exoplanet Archive matching, Overall/stage ranking snapshots, replay APIs, and the candidate investigation modal. The checked-in pipeline is broader than the small local runtime dataset used for development; the current sample feed contains one real TESS candidate and honest empty states for stages without published candidates.

## MAST/TESS probe

Run the bounded official-data probe from the repository root:

```bash
python scripts/probe_mast.py
```

The default configuration queries one public Sector 1 TESS target, selects its SPOC light curve, caches the FITS artifact under `data/raw/`, and writes a checksum/provenance manifest under `data/manifests/`. Runtime data is ignored by Git.

## Stage 1 batching

Create a durable queue manifest. A probe manifest can seed one real MAST/TESS target; remaining queue slots stay metadata-only until the provider adapter attaches products:

```bash
docker compose run --rm worker python prepare_target_universe.py \
  --universe-size 126000 \
  --probe-manifest /app/data/manifests/<successful-probe-manifest>.json
```

Run a bounded batch and resume it later from its DuckDB checkpoint:

```bash
docker compose run --rm worker python run_stage1_batch.py \
  --batch-size 4 \
  --max-workers 1
```

The baseline `random-stratified-v1` selector records its policy, processes only the next queue slice, persists ranking snapshots, and leaves adaptive sampling disabled until outcome statistics are available. Use `--retire-candidate <candidate-id>` when a later stage releases a slot so the next unprocessed targets fill it.

## Stage 2 historical validation

Stage 2 reuses the Stage 1 period and timing instead of starting a new search. It can evaluate cached products, a history manifest, or a bounded list of MAST sectors:

```bash
docker compose run --rm worker python run_stage2.py \
  --candidate-id <candidate-id> \
  --fetch-sectors 1,2,3
```

Only `Promising` Stage 1 candidates are eligible by default. Use `--dry-run --allow-nonpromising` only for a diagnostic; results with insufficient historical coverage remain `Needs More Data`.

## Stage 3 chronological holdout prediction

Stage 3 sorts the available observations, trains a CPU-capable phase-feature logistic model on the first two chronological windows, and evaluates dimming predictions on the third. It records precision, recall, F1, event recovery/accuracy, timing error, depth error, model versions, and the window boundaries. A transparent phase-window baseline is evaluated on the same holdout for comparison:

```bash
docker compose run --rm worker python run_stage3.py \
  --candidate-id <candidate-id> \
  --history-manifest /app/data/manifests/<history-manifest>.json
```

Only `Promising` Stage 2 candidates are eligible by default. Use `--dry-run --allow-nonpromising` for a diagnostic; Stage 3 results are assessments of candidate evidence and never scientific confirmation. Published evaluations and ranking snapshots are stored in DuckDB so replay can read them without rerunning the model.

## Known-object matching and Overall ranking

The catalog step queries a narrow, cached projection from the NASA Exoplanet Archive TAP service. Matching requires a compatible target identifier or a coordinate match with compatible period evidence; tolerances are recorded with each mapping. A match is a catalog mapping event, not TransitScope scientific confirmation:

```bash
docker compose run --rm worker python run_catalog_match.py \
  --candidate-id <candidate-id> \
  --force-refresh \
  --dry-run
```

Remove `--dry-run` to publish a confirmed catalog mapping. The candidate becomes an inactive `Known Match`, remains available in history, and is removed from Overall and all stage ranking snapshots. The remaining active candidates are re-ranked. Promotion selection is capacity-driven but status-gated: only `Promising` candidates inside the configured top-X can be selected; `Needs More Data` does not advance from rank alone.

## Project status

The application assesses potential planets and does not claim scientific confirmation. The production path currently uses an explainable Stage 1 Box Least Squares evidence score and a CPU-capable Stage 3 logistic model adapter with a transparent baseline; a neural-network experiment remains optional and is not claimed as part of this draft. See `docs/hackathon-build/` for the scope, product requirements, technical specification, build checklist, and item-by-item verification notes.

## Replay and candidate investigation

Replay is a read-only view over persisted ranking snapshots and pipeline events:

```text
GET /api/replay/{view}/meta
GET /api/replay/{view}/snapshot?sequence={n}
GET /api/replay/{view}/events?start_sequence={n}&end_sequence={n}
```

The dashboard keeps an independent replay playhead for Overall, Stage 1, Stage 2, and Stage 3. It supports empty-start state, play/pause, approximate dragging, and playback speed. Opening a ranking row loads its normalized light curve, stage-specific metrics, current activity, next action, and lifecycle history in a modal. Replay reads saved state only; it does not call MAST, the NASA Exoplanet Archive, or a model.

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

