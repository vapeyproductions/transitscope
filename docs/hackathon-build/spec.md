# Technical Specification: TransitScope

## 1. Product contract

TransitScope is a public, no-login web observatory for assessing potential exoplanet candidates from stellar brightness data. It presents a live Overall ranking plus independent Stage 1, Stage 2, and Stage 3 rankings. The product reports accumulated evidence and pipeline state; it does not claim scientific confirmation or an absolute probability that a planet exists.

The core technical promise is reproducibility:

- real MAST/TESS observations are ingested and provenance is retained;
- a fast, explainable screen covers the broad target universe;
- more expensive modeling is reserved for a threshold-driven priority queue;
- every published ranking change is persisted;
- replay reads persisted state and never reruns expensive models.

The first development run will validate the complete path on one manageable TESS sector/product family. This is an integration probe, not a replacement for the intended approximately 126,000-star target universe. The universe size, active-stage capacities, and batch sizes are configuration values rather than hard-coded limits.

## 2. Technology decisions

| Area | Decision | Rationale |
| --- | --- | --- |
| Web UI | Next.js + TypeScript + custom CSS/components | Gives direct control over the observatory layout, modal, charts, replay controls, and accessible states. Tableau is intentionally out of scope. |
| API | Python + FastAPI | Keeps API models close to ingestion and scientific-processing code. |
| Processing | Python, NumPy, SciPy, Astropy, scikit-learn, optional Lightkurve/astroquery helpers | Supports astronomy-aware time-series processing, CPU parallelism, and an explainable ML baseline without requiring a GPU. |
| Broad search | Astropy Box Least Squares plus engineered signal-quality features | A transparent periodic-dip detector is measurable and easier to explain than an opaque first-pass neural network. |
| Complex shortlist model | A versioned scikit-learn model behind a stable interface; neural-network implementation remains an optional experiment | The application can demonstrate ML without making a neural network a prerequisite for a credible candidate screen. |
| Raw/processed data | Filesystem cache with FITS/JSON metadata and Parquet feature tables | Keeps downloaded provider artifacts inspectable and supports columnar batch processing. |
| Query/state store | DuckDB | Suitable for local analytical queries and replay state without introducing a database server. The worker is the sole writer; the API is read-only. |
| Communication | JSON HTTP endpoints; manual refresh in the UI | Avoids unnecessary WebSocket/SSE complexity while preserving user control over when the visible dashboard changes. |
| Packaging | Docker Compose with `web`, `api`, and `worker` services sharing a durable data volume | Demonstrates reproducible local setup and keeps public deployment possible on a host that supports persistent storage and long-running workers. |
| Authentication | None | The dashboard is intentionally public and read-only from the user's perspective. |

## 3. External data roles and provenance

### 3.1 MAST/TESS observation adapter

The `MASTProductAdapter` is the primary observation source. It queries MAST observation/product metadata first, selects a configured light-curve product family, and downloads only the products needed for the current batch. MAST documents programmatic observation queries, product lists, and retrieval workflows through its API and `astroquery.mast` integration: [MAST API Advanced Search](https://outerspace.stsci.edu/spaces/MASTDOCS/pages/153686876/API%2BAdvanced%2BSearch) and [TESS mission data](https://archive.stsci.edu/missions-and-data/tess).

The adapter must not assume that the archive is a continuous real-time sensor. “Live mode” means the worker periodically checks for newly available or newly selected archive products, processes them, and publishes the newest completed state. The dashboard displays the last successful ingestion time and the source status.

The first probe uses one configurable TESS product family, preferably an official light-curve product available through MAST. Product selection remains an adapter setting so the implementation can compare a systematics-corrected product against an alternate MAST-hosted product if a quality test justifies it. No second provider is added merely to increase the provider count.

Every downloaded or derived artifact records:

- provider name and product family;
- MAST observation/product identifiers;
- source URL or query identifier;
- retrieval timestamp;
- checksum when a file is cached;
- target identifier, sector, cadence, and time coverage;
- processing code/model version;
- quality-filter and normalization settings.

### 3.2 Known-object catalog adapter

The `ExoplanetArchiveAdapter` queries the NASA Exoplanet Archive TAP service for known planets and candidate/TOI records used for cross-reference. TAP supports programmatic table queries and asynchronous/synchronous access; the adapter should use a narrow projection and cache the result rather than repeatedly downloading an entire catalog. See [NASA Exoplanet Archive TAP](https://exoplanetarchive.ipac.caltech.edu/docs/TAP/usingTAP.html).

The adapter supplies catalog coordinates, target identifiers where available, orbital period, object name, and catalog status. A match is treated as a catalog mapping event, not as independent scientific confirmation by TransitScope.

### 3.3 Provider interface

All providers implement a small interface so the pipeline is testable with recorded fixtures:

```text
list_targets(query_config) -> TargetManifest rows
list_products(target_ids, time_window) -> ProductManifest rows
download_product(product_id, cache_dir) -> CachedArtifact
health() -> ProviderHealth
```

The worker never passes provider-specific response objects into ranking code. It converts them into normalized records first.

## 4. System architecture

```text
MAST/TESS ─┐
           ├─> ingestion adapters ─> raw cache + manifest ─> batch worker
Exoplanet ─┘                                           │
Archive TAP                                             ├─> features/evaluations
                                                        ├─> candidate state
                                                        ├─> ranking snapshots/events
                                                        └─> DuckDB publish boundary

Next.js dashboard <─ HTTP JSON ─ FastAPI API <─ read-only DuckDB queries
       │
       ├─ live rankings and candidate modal
       └─ independent replay playheads per ranking tab
```

### 4.1 Services

#### `web`

Next.js application that renders the dashboard. It owns UI state only: selected tab, modal candidate, replay position, replay speed, pause/play state, and the last loaded published version. It does not run models, download FITS files, or decide candidate status.

#### `api`

FastAPI service that reads the latest published DuckDB state. It exposes current rankings, candidate detail, light-curve series, pipeline history, replay snapshots, ingestion health, and a manual-refresh/read-latest operation. It must never mutate candidate state during a normal read.

#### `worker`

Long-running Python process responsible for polling configured archive queries, downloading/cache-validating products, running batches, updating the candidate state, and publishing ranking changes. It uses a process pool for CPU-bound target-level work and limits provider requests with a configurable concurrency cap.

#### Shared data volume

The shared volume contains raw provider cache, normalized Parquet data, DuckDB, worker logs, and a small manifest of successful runs. The volume must be durable in public deployment because it contains the replay history and cached MAST artifacts.

## 5. Normalized data contracts

The implementation may use Python dataclasses/Pydantic models at service boundaries and DuckDB tables internally. IDs are stable strings; timestamps are UTC ISO-8601 values; numeric scores are evidence scores, not calibrated probabilities unless explicitly labeled.

### 5.1 `target_manifest`

One row per target in the configured processing universe.

```text
target_id              stable target identifier, preferably TIC when available
provider_target_id     original provider identifier
ra_deg, dec_deg        sky position
sector                 TESS sector or equivalent observation grouping
cadence                cadence label
source_product_family  selected MAST product family
selection_bucket       random/stratified/adaptive sampling bucket
manifest_status        queued, processing, processed, unavailable
created_at
```

The manifest records the intended total universe, the number processed, and the current batch cursor so the UI can distinguish “not yet scanned” from “no candidate found.”

### 5.2 `light_curve_points`

Stored in Parquet and queried into batches rather than loaded into the browser.

```text
target_id
time_bjd
flux
flux_err
quality_flags
cadence
sector
source_product_id
retrieved_at
```

Raw FITS products remain in the cache; normalized points are derived artifacts and can be rebuilt from the raw source using the recorded processing version.

### 5.3 `candidates`

```text
candidate_id
target_id
current_stage       stage1, stage2, stage3
status              Promising, Needs More Data, Inconclusive, Known Match
overall_score
stage_score
rank_overall
rank_stage
activity
rank_delta
movement_kind       rising, falling, unchanged, sustained_top, rapid_rise
last_evaluated_at
known_match_id
active              boolean; false for tombstoned known matches
```

### 5.4 `evaluations`

One row per completed model evaluation, never overwritten.

```text
evaluation_id
candidate_id
stage
model_version
data_window_start, data_window_end
input_product_ids
feature_json
metric_json
classification
data_sufficiency
created_at
```

`feature_json` and `metric_json` are intentionally retained for the candidate modal and for later model comparison. The UI receives a curated projection rather than all raw feature values by default.

### 5.5 `pipeline_events`

Append-only event log for candidate history and replay narration.

```text
event_id
sequence_number       monotonically increasing publish sequence
candidate_id
stage
event_type            discovered, evaluated, promoted, backfilled,
                      status_changed, known_match, rank_changed,
                      data_delayed, provider_error
payload_json
occurred_at
model_run_id
```

### 5.6 `ranking_snapshots`

Snapshots are written after each published ranking change for each view: `overall`, `stage1`, `stage2`, and `stage3`.

```text
snapshot_id
view
sequence_number
occurred_at
candidate_order_json  ordered top-N candidate IDs with display scores/status
active_candidate_count
processed_target_count
```

The first version may store compact top-100 snapshots rather than every candidate. Candidate history and event rows retain the broader audit trail. Snapshot writes are idempotent on `(view, sequence_number)`.

### 5.7 `ingest_runs`

```text
run_id
provider
query_config_json
started_at, finished_at
status
targets_seen
products_downloaded
products_failed
error_json
```

The API uses this table to report `last_successful_ingest`, provider health, and stale-data notices.

## 6. Processing pipeline

### 6.1 Bootstrap and batching

1. Query the selected MAST product family and create/update the target manifest.
2. Preserve the configured universe target of approximately 126,000 stars. Do not require the browser or a single worker process to hold all products in memory.
3. Process a small real-data probe first. The probe must exercise metadata query, product selection, download, FITS parsing, normalization, Stage 1 scoring, candidate persistence, API response, and replay snapshot creation.
4. Expand to larger batches after throughput, error rates, and storage growth are measured.
5. Use a bounded process pool for independent target-level feature extraction. Use a provider request limiter so parallelism does not become an uncontrolled archive request fan-out.
6. Commit each batch as a transaction. Publish the new sequence only after candidate rows, evaluations, events, and affected ranking snapshots are consistent.

### 6.2 Light-curve cleaning and normalization

For each target:

1. Remove non-finite samples and samples rejected by provider quality flags.
2. Normalize flux relative to a robust baseline.
3. Remove obvious long-timescale trends with a documented configurable detrending method.
4. Preserve coverage, missingness, and quality statistics as features; do not silently turn missing data into a negative detection.
5. Produce a normalized series plus provenance metadata.

Cleaning failures produce a data-quality event and usually `Needs More Data` or `Inconclusive` according to whether more observations could plausibly resolve the issue.

### 6.3 Stage 1: broad periodic-dip screening

Stage 1 is designed for coverage and throughput. It uses a Box Least Squares search over a configurable period range and extracts interpretable features such as:

- best period and period uncertainty;
- transit-like depth and duration;
- BLS power or equivalent signal strength;
- signal-to-noise estimate;
- number of observed transit-like events;
- odd/even depth consistency;
- out-of-event scatter;
- usable-data fraction and quality-flag rate;
- sector/cadence coverage;
- contamination or neighboring-source metadata when available.

The Stage 1 score is a deterministic weighted ranking score over normalized features. It is labeled `evidence score`, not `planet probability`. The feature weights and model version are stored with every evaluation.

Stage 1 maintains a comparative baseline and continues exploring the unprocessed manifest. When a source star is promoted to Stage 2, becomes a known match, or is finalized as inconclusive, the worker selects replacement targets from the unprocessed manifest. Initial selection is random or stratified enough to preserve broad coverage; adaptive sampling is a later policy with an exploration floor.

### 6.4 Stage 2: longer-baseline repeatability

For a Stage 1 candidate inside the current top-X priority threshold:

1. Request a longer chronological history, targeting approximately two years where MAST coverage permits.
2. If the target does not have enough historical coverage, persist `Needs More Data` and record the missing window.
3. Fold the available history on the Stage 1 period and measure repeatability, timing consistency, depth consistency, shape consistency, and false-alarm indicators.
4. Compare the new evidence with the Stage 1 pattern rather than starting a new unrelated search.
5. Apply the Stage 2 ranking model and update the candidate's score, status, and next-step activity.

The Stage 2 implementation starts with engineered features and a versioned scikit-learn model. A more complex model can be evaluated behind the same interface if it improves held-out recovery or ranking metrics. The system must keep the baseline metrics for comparison.

### 6.5 Stage 3: chronological holdout prediction

For candidates eligible for Stage 3:

1. Split the available chronological observations into three windows, preserving time order.
2. Train on the first two windows using phase-folded or event-level features.
3. Predict the presence, timing, and approximate depth of dimming events in the third window.
4. Report holdout metrics such as precision, recall, F1, event-level accuracy, phase/timing error, and depth error where the data supports them.
5. Use the holdout result as evidence in Stage 3 ranking; do not describe it as confirmation.

The initial implementation uses a CPU-capable scikit-learn estimator with a stable `fit/predict/evaluate` adapter. A 1D neural network is an optional model experiment only if it can be evaluated against the transparent baseline on the same candidates and windows. Model choice must not change the replay contract.

### 6.6 Known-object cross-reference

After each stage evaluation, compare candidate target coordinates, target identifiers, and period evidence against cached NASA Exoplanet Archive records. The exact tolerances are configuration values and are recorded in the evaluation. A successful mapping emits a `known_match` event, stores the catalog identifier, changes status to `Known Match`, sets `active=false`, and removes the candidate from all active ranking snapshots after the mapping sequence.

The candidate remains queryable in history and replay. A later stage must not process a tombstoned candidate unless an explicit data correction workflow is added in a future version.

### 6.7 Adaptive sampling experiment

Adaptive sampling is not allowed to distort the baseline result silently. The worker first maintains coverage-preserving random/stratified selection. After outcome statistics exist, it may prioritize batches using sector/sky cell, quality, product availability, and observed rates of known matches, inconclusive results, or promising repeatable signals.

Every selection records its policy and weights. The policy has an exploration floor so unvisited portions of the universe remain eligible. Nearby stars are used for contamination/systematics analysis, not as a direct prior that a planet should be present.

## 7. Candidate lifecycle and ranking policy

### 7.1 Status rules

| Status | Meaning | Processing behavior |
| --- | --- | --- |
| Promising | Current evidence is strong enough for the candidate's stage | Highest operational priority; Stage 3 calls the highest-ranked eligible Stage 2 signal whenever it needs a replacement |
| Needs More Data | Evidence is insufficient or delayed | Remains active, ranks above `Inconclusive`, and triggers targeted historical backfill inside the Stage 1/2 reassessment windows or through the ranked Stage 3 reassessment queue; it does not advance on rank alone |
| Inconclusive | Current evidence is sufficient but unresolved or not competitive | Remains in active comparative rankings and history, but is lower priority than `Needs More Data` and is not rerun unless new products change the input signature |
| Known Match | Evidence maps to a known catalog object | Tombstoned from active rankings; mapping remains visible in replay and candidate history |

### 7.2 Top-X progression

Stage 1 has a configurable priority window, while Stage 2 has a separate reassessment threshold such as `stage2_reassessment_threshold`. The Stage 2 threshold is a ranking window for re-evaluating `Needs More Data`, not a promotion cutoff or a stage capacity. Stage 3 intentionally has no top-X threshold: it maintains a ranked reassessment queue of `Needs More Data` candidates and always selects the highest-ranked unresolved candidate first. A candidate may advance when:

- it is `Promising`;
- it is selected as the highest-ranked eligible `Promising` candidate when Stage 3 calls for a replacement, or is selected by the Stage 3 Needs More Data queue;
- the previous stage has published a completed evaluation;
- the next-stage worker calls for another candidate.

If `Needs More Data` enters a Stage 1 or Stage 2 top-X threshold, the worker backfills the most useful missing historical windows and re-evaluates it only when that work is requested by the Stage 3 waterfall. Stage 3 does this systematically for every unresolved candidate in rank order, without a top-X cutoff. Status is part of the ranking policy, not an automatic promotion bypass: `Promising` ranks above `Needs More Data`, which ranks above `Inconclusive`, and evidence score breaks ties within each status. When Stage 3 calls for a new candidate, it selects the highest-ranked eligible `Promising` signal from all of Stage 2; the signal does not need to be within the reassessment window. If no Promising signal is available, the same request can reassess Stage 2 or ask Stage 1 for a replacement. When a Stage 3 candidate resolves as a Prime Candidate, Inconclusive, or Known Match, a durable replacement-debt event records the downstream call. Known matches leave active rankings, allowing lower-ranked candidates to move upward and become eligible; unresolved candidates remain visible until later evidence changes their status. After the initial Stage 1 baseline, no stage advances from an independent timer.

### 7.3 Overall ranking

The Overall view combines active candidates across stages. It uses the latest stage evidence, evidence completeness, and a bounded stage-progression component so that candidates with stronger completed tests are surfaced without allowing “being advanced” to overwhelm poor evidence. The score formula and weights are versioned configuration, not hidden UI logic.

The UI shows the score as an evidence/ranking value and avoids a percent sign unless a metric is explicitly a calibrated evaluation metric.

### 7.4 Movement indicators

Movement is computed by comparing the current published snapshot with the immediately preceding snapshot and a short history window:

- `rising`, `falling`, or `unchanged` for ordinary movement;
- `sustained_top` when a candidate remains in the configured top band for the configured number of snapshots;
- `rapid_rise` when rank improves by the configured delta within the configured window.

The numeric rank remains primary. Icons are supplementary and must have text or accessible labels.

## 8. Replay design

Replay is a view over persisted state, not a second execution path for the models.

1. Each ranking view has its own ordered snapshot stream.
2. The earliest snapshot represents the empty or nearly empty starting state.
3. The UI loads snapshot metadata once, then requests snapshots by sequence or normalized playhead position.
4. Play/pause, speed, and dragging are client-side controls over the saved sequence. No provider request or model call is made by replay.
5. The UI interpolates only presentation timing; it does not invent rankings between snapshots.
6. Candidate detail can show the candidate's event history up to the selected replay sequence. If the candidate is not yet discovered at that sequence, the modal explains that it is not available at that point in the replay.
7. When a known-match event removes a candidate, the replay response includes a separate removal transition with the prior rank and catalog identifier. The UI may show that candidate briefly as `Already Known` before animating it out of the active list.
7. A replay state is deterministic for a given `view` and `sequence_number`.

Recommended first API shape:

```text
GET /api/replay/{view}/meta
GET /api/replay/{view}/snapshot?sequence=42
GET /api/replay/{view}/events?from=35&to=42
```

The API returns `data_version`, `sequence_number`, `occurred_at`, processed/total counts, and the top-N rows needed by the dashboard.

## 9. API contract

All endpoints return JSON and include a `data_version` or `sequence_number` so the UI can detect stale responses. Errors use a stable `code`, human-readable `message`, and optional `retryable` flag.

```text
GET  /api/health
GET  /api/status
GET  /api/rankings?view=overall|stage1|stage2|stage3
GET  /api/candidates/{candidate_id}
GET  /api/candidates/{candidate_id}/light-curve
GET  /api/candidates/{candidate_id}/history
GET  /api/replay/{view}/meta
GET  /api/replay/{view}/snapshot?sequence={n}
GET  /api/replay/{view}/events?from={n}&to={n}
POST /api/refresh
```

`POST /api/refresh` does not rerun a model. It returns the newest published data version and ingestion status so the UI can apply available work intentionally. The worker may continue processing independently.

The candidate detail response should contain:

```json
{
  "candidate_id": "tic-123",
  "status": "Promising",
  "activity": "Running Stage 3 prediction",
  "stage": "stage3",
  "evidence_score": 0.82,
  "rank": 3,
  "rank_movement": "rapid_rise",
  "light_curve": {"series_url": "/api/candidates/tic-123/light-curve"},
  "evaluations": [],
  "next_step": "Awaiting holdout prediction publication",
  "last_updated_at": "..."
}
```

## 10. Frontend specification

### 10.1 Dashboard shell

- Dark space-themed background with a restrained scientific palette.
- Header with product name, honest product promise, live/replay indicator, last data update, provider health, and Refresh control.
- Tabs: `Overall`, `Stage 1`, `Stage 2`, `Stage 3`.
- Each tab has a title, short stage description, Replay button, ranking table, and processing summary.
- Ranking target is approximately 50–100 rows; the server controls the returned top-N.

### 10.2 Ranking row

Each row includes:

- numeric rank;
- candidate/target identity;
- evidence score or stage score;
- current status;
- current activity text;
- movement indicator with accessible label;
- last update time.

The row is clickable and opens the candidate modal. A row never implies that “No current activity” means “finished forever”; it means no active operation is currently assigned.

### 10.3 Candidate modal

The modal blurs the dashboard background and can be closed. It shows one candidate only:

- identity and status;
- current activity and last update;
- light curve;
- completed evaluation metrics and evidence summary;
- pipeline history timeline;
- next required evidence or action.

The precise visual ordering can be adjusted after the first dashboard prototype, but the API must expose all required data from the start.

### 10.4 Replay controls

- Replay starts at the earliest snapshot for the selected tab.
- Empty initial state is brief but visible.
- Controls: play/pause, draggable approximate timeline, speed selector.
- Each tab stores its own replay state in component state or URL-safe client state.
- Replay does not auto-refresh live data or mutate the worker.

### 10.5 Accessible and honest states

The UI must distinguish loading, stale-but-usable, provider unavailable, no eligible candidates, `Needs More Data`, and `Inconclusive`. Icons require text alternatives. Color is not the only status signal. The learning pop-up layer is deferred; the first interface may use concise helper text only.

## 11. Repository structure

```text
.
├── api/
│   ├── app/main.py
│   ├── app/routes/
│   └── app/schemas.py
├── worker/
│   ├── main.py
│   ├── adapters/
│   │   ├── mast.py
│   │   └── exoplanet_archive.py
│   ├── pipeline/
│   │   ├── cleaning.py
│   │   ├── stage1.py
│   │   ├── stage2.py
│   │   ├── stage3.py
│   │   ├── matching.py
│   │   ├── ranking.py
│   │   └── replay.py
│   └── storage/
│       ├── duckdb_store.py
│       └── parquet_store.py
├── web/
│   ├── app/
│   ├── components/
│   └── lib/api.ts
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── scripts/
│   ├── probe_mast.py
│   ├── run_batch.py
│   └── seed_replay_from_run.py
├── data/
│   ├── raw/
│   ├── normalized/
│   ├── manifests/
│   └── .gitkeep
├── docs/hackathon-build/
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.worker
├── Dockerfile.web
└── README.md
```

Provider credentials, if ever required, live in environment variables and are never committed. The default MAST and archive endpoints should work without user accounts where the provider permits public access.

## 12. Docker and deployment

Local development must run with:

```text
docker compose up --build
```

The compose setup provides:

- `web` on the public dashboard port;
- `api` on an internal API port, optionally exposed for debugging;
- `worker` with the same `/app/data` durable volume;
- health checks for API and worker liveness;
- environment variables for product family, batch size, stage capacities, polling interval, and top-N display size.

The first public deployment should use the same container boundaries on a host with persistent disk and a long-running worker. A stateless frontend-only deployment is insufficient because the application promises saved replay history and cached source data. If a later hosting platform separates the web and worker, the API must retain the same JSON contract and the data volume must move to durable object storage or a managed database without changing replay semantics.

## 13. Verification plan

### Unit tests

- quality-flag filtering and robust normalization;
- BLS feature extraction from a small recorded light curve;
- Stage 1, Stage 2, and Stage 3 score calculations;
- status transitions, including `Needs More Data` persistence;
- known-match tombstoning;
- top-X promotion and replacement sampling;
- rank movement indicators;
- idempotent event and snapshot writes.

### Integration tests

- MAST adapter against a recorded metadata/product fixture or an explicitly enabled live probe;
- FITS-to-Parquet normalization;
- Exoplanet Archive query parsing against a recorded response;
- worker batch transaction and publish boundary;
- API ranking/candidate/replay responses;
- deterministic replay from a saved DuckDB state.

### End-to-end verification

The acceptance path is:

1. Start the Docker stack.
2. Run the official-data probe.
3. Confirm a target manifest, cached provenance, evaluation rows, candidate rows, and ranking snapshots exist.
4. Open the dashboard on Overall.
5. Refresh and observe a new published version or a clear no-new-data state.
6. Open a candidate modal and verify light curve, activity, metrics, and history.
7. Replay Overall and each stage independently.
8. Confirm replay does not increase model-run count or provider-download count.
9. Force or record a known catalog match and verify removal from active rankings plus historical visibility.
10. Stop/restart containers and confirm the durable replay history remains available.

### Model evaluation requirements

Before claiming the complex model improves the pipeline, compare it with the transparent baseline using the same held-out candidates or injected transit signals. Record recovery rate, false-positive behavior, ranking quality at the displayed top-N, runtime, and resource use. If the neural-network variant does not show a meaningful improvement, keep the CPU baseline as the production path and document the experiment.

## 14. Configuration and operational defaults

The exact values are tuned during the probe, but the code must expose these settings:

```text
TARGET_UNIVERSE_SIZE=126000
DISPLAY_TOP_N=100
STAGE1_PROMOTION_THRESHOLD=100
STAGE2_REASSESSMENT_THRESHOLD=100
STAGE3_REASSESSMENTS_PER_CYCLE=4
STAGE3_DISPATCHES_PER_CYCLE=4
CATALOG_AUDIT_BATCH_SIZE=100
BATCH_SIZE=<measured>
MAST_PRODUCT_FAMILY=<probe-selected product family>
STAGE2_LOOKBACK_DAYS=730
WORKER_POLL_SECONDS=<configured>
REPLAY_SNAPSHOT_LIMIT=<configured>
MODEL_MODE=baseline|complex
ADAPTIVE_SAMPLING=false|true
```

The application must display processed/total counts and last-ingest time so users can see whether the current view is a complete run, a partial run, or a stale cached state.

## 15. Risks and explicit boundaries

- MAST is an archive, so newly available data may not arrive at a dramatic live cadence. The UI must show this honestly and make replay the reliable demonstration path.
- A two-year historical window may not exist for every target. Missing coverage is evidence for `Needs More Data`, not automatic evidence against a candidate.
- A top-ranked transit-like dip can be caused by eclipsing binaries, contamination, instrumental systematics, or catalog matches. Stage scores are screening evidence, not confirmation.
- Processing the full target universe may exceed a laptop's convenient runtime. The architecture therefore supports batches, checkpoints, bounded parallelism, and a public worker with more resources later without changing the UI contract.
- Raw products can consume substantial disk space. The manifest, checksums, and cache policy must support retention of only the products needed for current evaluation plus provenance for discarded artifacts.
- Replay correctness depends on the publish boundary. No ranking snapshot is visible to the API until its referenced candidate/evaluation/event rows are committed.

## 16. Definition of done for the first working draft

The first draft is complete when:

- Docker starts the web, API, and worker services;
- an official MAST/TESS probe completes and its provenance is visible in API state;
- the worker processes real targets in batches and writes Stage 1 rankings;
- Stage 2 and Stage 3 can run on a configurable shortlist, with metrics and status transitions persisted;
- NASA Exoplanet Archive cross-reference can create a Known Match removal event;
- Overall and all three stage tabs render current rankings;
- candidate modal and light curve work for one selected candidate;
- replay for every tab reconstructs saved states without model/provider calls;
- live refresh applies a newer published state without silently rerunning old work;
- tests cover state transitions, replay determinism, and the Docker/API happy path;
- the README explains setup, data provenance, limitations, and the non-confirmatory product claim.
