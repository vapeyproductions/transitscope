# TransitScope

TransitScope is a Dockerized astronomy observatory for assessing potential exoplanet candidates from public stellar observations.

## Local development

```bash
docker compose up --build
```

- Web dashboard: http://localhost:3000
- API health: http://localhost:8000/api/health
- API status: http://localhost:8000/api/status

The `data/` directory is a durable local volume for provider cache, normalized data, DuckDB state, and replay history. The ingestion and processing pipeline is being built incrementally; current services only provide the foundation health checks.

## MAST/TESS probe

Run the bounded official-data probe from the repository root:

```bash
python scripts/probe_mast.py
```

The default configuration queries one public Sector 1 TESS target, selects its SPOC light curve, caches the FITS artifact under `data/raw/`, and writes a checksum/provenance manifest under `data/manifests/`. Runtime data is ignored by Git.

## Project status

The application assesses potential planets and does not claim scientific confirmation. See `docs/hackathon-build/` for the scope, product requirements, technical specification, and build checklist.

