# TransitScope

TransitScope is a Dockerized astronomy observatory for assessing potential exoplanet candidates from public stellar observations.

## Local development

```bash
docker compose up --build
```

- Web dashboard: http://localhost:3000
- API health: http://localhost:8000/api/health
- API status: http://localhost:8000/api/status

The `data/` directory is a durable local volume for provider cache, normalized data, DuckDB state, and replay history. The current stack includes the official MAST/TESS probe, one-target Stage 1 processing, and a bounded/checkpointed Stage 1 batch runner.

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

## Project status

The application assesses potential planets and does not claim scientific confirmation. See `docs/hackathon-build/` for the scope, product requirements, technical specification, and build checklist.

