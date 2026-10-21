# Build Notes

## Onboarding

- Guided build onboarding started for Build With AI: Basics.
- Participant name from Devpost profile: Marie Rosario.

## Onboarding Decisions

- Audience: students and curious people who want to explore astronomy data without needing specialist knowledge.
- Initial concept: a virtual astronomy lab that streams a candidate-discovery process, ranks anomalies, and lets users inspect and compare candidates.
- Product feel: dark space theme paired with a clear scientific dashboard; approachable, educational, and not overloaded with repeated explanations.
- Learning layer: show what data is being examined, where it came from, and what it is being compared against. Use optional informational pop-ups for users who want more context.
- Portfolio angle: use machine learning for anomaly scoring and investigate parallel processing for efficient searching, while keeping the first proof of concept honest about what it can and cannot claim.
- Active shaping: participant prioritized what is most feasible while still novel and impressive, and clarified that the app should provide a live-feeling process even if it does not discover a new astronomical body during the demo.
- Next planning question: choose the smallest credible detection target, public data source, and end-to-end interaction for an afternoon-sized build.

## Scope Decisions

- Time budget: several days.
- Direction: exoplanet candidates from periodic brightness dips.
- Product promise: “A live assessment of potential planets, updated for likelihood regularly using incoming data and increasingly rigorous testing.” No claim of scientific confirmation or guaranteed discovery.
- Ambition retained: process a candidate universe of approximately 126,000 stars, use a complex second-tier model if validation supports it, and use multiple data providers when each provider has a clear quality or verification role.
- Processing strategy: two-tier model. A parallel, explainable first pass scans the broad universe; more expensive modeling runs on a capacity-driven top-X shortlist. Neural networks remain an implementation option, not a requirement before a baseline is measured.
- Pipeline stages: broad periodic-dip screening; longer-baseline repeatability analysis; train-on-two-windows/predict-the-third evaluation; known-object cross-reference.
- Candidate lifecycle: Promising, Needs More Data, Inconclusive, Known Match. Needs More Data triggers targeted historical backfill rather than automatic promotion. Inconclusive remains visible but is not repeatedly reprocessed. Known Matches are tombstoned from active rankings but remain visible in replay.
- Replay architecture: persist ranking changes, pipeline events, and snapshots/deltas as the model runs. Replay reconstructs the timeline and never reruns expensive models.
- Sampling: begin with coverage-preserving random or stratified batches, then experiment with adaptive sampling based on sector/sky position, data quality, and observed false-positive or inconclusive rates. Adaptive sampling can deprioritize systematic regions but must not permanently omit the remaining universe.
- Learning layer: deferred until the technical pipeline is working; keep only concise status labels and evidence summaries in the first build.
- Deepening rounds completed: 1.
- Active shaping: participant explicitly rejected replacing the 126,000-star ambition with a toy dataset, while accepting staged computation, replay from stored state, and removal of nonessential product features.

## PRD Decisions

- Default experience: live Overall dashboard, with Overall, Stage 1, Stage 2, and Stage 3 tabs.
- Candidate rows combine ranking, status, current activity, rank movement, and last-update context.
- Activity labels are explicit text; the final icon-versus-text treatment will be chosen after the dashboard is visible.
- Rank movement includes rising/falling/unchanged indicators, a sustained-top flame, and a rapid-rise zigzag indicator.
- Live updates are user-controlled through Refresh; refreshing closes an open candidate modal. Feed failures preserve the last successful state and show a stale-data notice.
- Replay is independent per tab, starts at the earliest saved state, briefly shows the empty beginning, and supports pause, dragging, and playback speed.
- Candidate investigation is a single-candidate modal with a blurred background. Side-by-side comparison is deferred.
- `Needs More Data` persists until evidence changes the classification. It is not treated as an automatic promotion state.
- Educational pop-ups and the larger learning layer are deferred until the technical pipeline is working.
- PRD deepening rounds completed: 2.

## Technical Research Note

Nearby-star information is relevant as a contamination and systematics signal, not as a direct planet-discovery prior. TESS light curves can be affected by blended nearby sources, and field/instrument systematics can create clusters of similar false alarms. The scope therefore uses spatial/sector clustering for quality control and adaptive sampling rather than assuming physically neighboring stars share planets.

## Spec Decisions

- Architecture locked: custom Next.js/TypeScript dashboard, Python/FastAPI API, and a long-running Python worker.
- Tableau is intentionally skipped so the ranking table, candidate modal, replay controls, activity labels, and evidence views can be designed as one coherent product.
- MAST/TESS is the primary observation source. The NASA Exoplanet Archive TAP service is the known-object cross-reference. Additional providers require a demonstrated quality or verification benefit.
- Raw provider artifacts are cached on disk; normalized features are stored in Parquet; candidate state, events, evaluations, and ranking snapshots are stored in DuckDB.
- The worker is the sole writer and the API is read-only. A publish boundary prevents the dashboard from observing partial batches.
- The target universe remains approximately 126,000 stars. A smaller real MAST sector/product-family probe is the first integration test before scaling batch throughput.
- The first model path is a transparent Box Least Squares/feature baseline followed by a versioned CPU-capable shortlist model. Neural networks are optional experiments, not a requirement without measured improvement.
- Replay is a persisted view over ranking snapshots and pipeline events. Replay must not make provider requests or rerun models.
- Public deployment is allowed after the Docker Compose stack is stable, but the deployment must provide durable storage for cached data and replay history.
- Technical specification written to `docs/hackathon-build/spec.md`.

## Checklist Planning Decisions

- Checklist is co-designed with the participant rather than handed off.
- Build mode: step-by-step implementation with a verification pause after every item.
- Comprehension preference: after each item, explain what was implemented, why it works, and surface only material decisions or questions.
- Git cadence: commit after every checklist item.
- The first implementation checkpoint is a thin Stage 1 dashboard plus one real MAST/TESS light curve. It is not the final project scope.
- The full-project wow moment is an independent replay showing candidates enter from an empty ranking, advance through all three assessment stages, change rank based on evidence, and potentially become known matches while other candidates move up.
- Sampling clarification: create a lightweight manifest for the approximately 126,000-star universe, process bounded random/stratified batches, persist results, and use replacement sampling. Do not process every light curve at once.
- Dashboard order decision: build a thin shell first, then the MAST probe immediately, so the UI has a real target without being designed around invented astronomy results.
- Checklist drafted with 12 sequenced items, ending in the Devpost handoff.

## Build Phase

- Entered the step-by-step build with item 1: establish the Dockerized project foundation.
- Created the initial `web`, `api`, and `worker` service structure, shared data directory, health/status endpoints, Dockerfiles, Compose configuration, and README.
- `docker compose config` passed.
- Runtime verification is currently blocked because Docker Desktop's daemon is not running. The checklist item remains unchecked; start Docker Desktop and retry item 1 before continuing.

### Item 1: Dockerized project foundation — complete

- Docker Desktop was started and the foundation was retried successfully.
- `docker compose config` passed.
- `docker compose up --build -d` built and started `api`, `worker`, and `web`.
- API health returned `status: ok`; API status reported the worker `idle` with a live heartbeat; the web service returned HTML.
- The web dependency audit initially exposed a vulnerable older Next.js line. The scaffold was upgraded to Next.js 16.4.2 and React/React DOM 19.3.1; the final image build reported zero vulnerabilities.
- The first foundation commit should be created before beginning item 2.

### Item 2: DuckDB schemas, Parquet paths, and publish boundary — complete

- Added `DataPaths` for raw, normalized, manifests, and the DuckDB database under the shared data directory.
- Added DuckDB schemas for manifests, ingest runs, candidates, evaluations, pipeline events, ranking snapshots, and publish versions.
- Added a transactional single-writer `publish_batch` operation with idempotent inserts and a published-version boundary.
- Added three storage tests covering directory creation, idempotent publishing, and rollback of a failed batch.
- Rebuilt the worker with DuckDB 1.4.3; `python -m unittest discover -s tests -p 'test_*.py'` passed with 3 tests.
- Inspected the running database: all seven expected tables exist and no version is published before a batch runs.
- The database and heartbeat remain runtime data ignored by Git.

### Item 3: Thin Stage 1 dashboard shell — complete

- Replaced the starter page with a responsive dark scientific dashboard for the Stage 1 ranking view.
- Added stable UI locations for target counts, processing state, candidate status, current activity, movement, and last update so later API data can fill the same contract.
- Added disabled placeholders for future Overall, Stage 2, Stage 3, replay, and refresh interactions without implying those features are already live.
- Kept the initial state honest: the ranking contains no fabricated astronomy results and explicitly says it is waiting for the MAST/TESS ingestion probe.
- Rebuilt the web image successfully with Next.js 16.4.2, confirmed the API remained healthy, and verified the rendered HTML includes the observatory title, empty-state message, and MAST/TESS provenance.
- TypeScript verification passed with `npm run typecheck`.

### Item 4: Real MAST/TESS ingestion probe — complete

- Added a bounded `MastProductAdapter` using MAST's documented metadata request and direct file-download endpoints.
- Added `config/mast_probe.json` for a reproducible Sector 1 TESS/SPOC light-curve probe, with CLI overrides for sector, target, and data directory.
- The probe selected observation `60835920` / target `TIC 277691220`, selected the public SPOC LC product, and cached the 2,039,040-byte FITS artifact under `data/raw/mast/tess/sector-0001/`.
- Wrote a JSON manifest under `data/manifests/` containing the query, MAST observation/product identifiers, source URI, retrieval headers, target-manifest fields, cache path, and SHA-256 `bf71e43b097651cb93f61cd871097582bcc1fad047910760489780d8db678889`.
- Downloads use a temporary `.part` file and atomic rename; a rerun reused the cache with the same checksum. Raw data and runtime manifests remain ignored by Git.
- Added adapter tests and verified six tests pass. The explicit no-match path returned the clear provider error `MAST returned no TESS timeseries observations` with exit code 2.
- MAST references used for the implementation: [MAST API documentation](https://mast.stsci.edu/api/v0/index.html), [MAST services](https://mast.stsci.edu/api/v0/_services.html), and [TESS archive query guidance](https://outerspace.stsci.edu/spaces/TESS/pages/30278757/7.0%2B-%2BTips%2Band%2BTricks%2Bto%2BGetting%2BTESS%2BData%2BAt%2BMAST).

### Item 5: One-target Stage 1 pipeline — complete

- Added the Astropy FITS reader and quality-aware cleaning path. The processor prefers `PDCSAP_FLUX`, removes non-finite/quality-flagged samples, robustly normalizes flux, and removes a linear long-timescale trend.
- Added a bounded Astropy Box Least Squares search over the configured 0.5-day to 14-day period range and interpretable features for period, duration, depth, BLS SNR/power, transit count, odd/even consistency, scatter, cadence, coverage, missingness, and quality flags.
- Added deterministic `stage1-bls-v1` evidence scoring. The value is explicitly stored as an evidence score, not a planet probability; this real target scored 56.112 and was classified `Inconclusive` because its BLS SNR was below the promising threshold.
- Added normalized-series persistence as a compressed `.npz` artifact, plus transactional persistence of the target manifest, ingest run, candidate, evaluation, pipeline event, and first Stage 1 ranking snapshot.
- Added `worker/run_stage1.py`; the real cached TESS light curve published version 1 with candidate `stage1-tic-277691220`. DuckDB inspection confirmed one published version, one candidate, one evaluation, and one `stage1` snapshot.
- The first worker dependency build exposed an Astropy/NumPy 2.4 incompatibility; pinning `numpy<2.4` resolved it. The complete worker suite passes with 8 tests.
- Astropy references used for FITS and BLS: [FITS file handling](https://docs.astropy.org/en/stable/io/fits/) and [Box Least Squares](https://docs.astropy.org/en/stable/api/astropy.timeseries.BoxLeastSquares.html).

### Item 6: API + Stage 1 dashboard vertical slice — complete

- Added a read-only DuckDB API projection for published worker state: status, rankings, candidate detail, normalized light-curve points, and pipeline history.
- Added a refresh contract that reads the newest published version and explicitly does not rerun the model. The worker remains the only process that writes pipeline state.
- Connected the dashboard to the API with loading, error, empty, real-data, refresh, and candidate-modal states. The modal shows the actual TESS target's score, BLS metrics, normalized light curve, target metadata, and Stage 1 history.
- Added local-development CORS for the separate web and API containers and pinned `pytz` so DuckDB timezone-aware timestamps serialize correctly in the API image.
- Rebuilt the API and web containers. Endpoint checks confirmed version 1, one processed target out of 126,000, one Stage 1 ranking row, candidate detail, 1,200 plotted light-curve points, history, and no-rerun refresh behavior.
- Browser smoke check confirmed the rendered dashboard shows `TIC 277691220`, `Inconclusive`, evidence score `56.112`, published version 1, and the candidate investigation modal.

### Item 7: Bounded Stage 1 batches and replacement sampling — complete

- Added a durable JSONL target-universe generator. It can preserve a configured 126,000-target queue, seed real provider-backed rows from a successful MAST probe, and keep metadata-only queue slots explicit until product lookup attaches observations.
- Added `random-stratified-v1` queue selection with a streaming cursor, DuckDB `processing_checkpoints`, and `batch_runs` records. The worker processes only the next batch slice and resumes from the last committed cursor.
- Added bounded `ProcessPoolExecutor` target processing, per-target failure capture, and transactional publishing of candidates, evaluations, events, ranking snapshots, batch metadata, and checkpoints. A partial batch leaves completed state intact.
- Added candidate-release support. Passing `--retire-candidate` marks a candidate inactive, persists a release event, and increases the next selection request so unprocessed targets fill the opened slot. Adaptive sampling is explicitly rejected until outcome statistics exist.
- Configured Docker defaults for a 126,000-target universe, batch size 4, one worker on the laptop, stage capacity 100, and top-N display 100. The process pool remains available through `--max-workers` for measured CPU-heavy batches.
- Worker tests pass with 13 tests. A real local batch consumed the seeded MAST target plus two unavailable queue slots and published version 2; API status reported 2 processed targets out of 126,000 while preserving the real candidate ranking.
- Restart verification returned “manifest exhausted” without creating another publish version or duplicate events. On eight metadata-only queue slots, the measured serial run took about 0.83s and the two-worker run about 1.00s, so the default remains serial for tiny batches because process startup outweighed trivial work.

### Item 8: Stage 2 historical repeatability validation — complete

- Added `Stage2Processor`, which reuses the Stage 1 period and transit timing, combines available cleaned products chronologically, and measures coverage, event count, timing consistency, depth consistency, shape consistency, and an explainable repeatability evidence score.
- Added explicit `Needs More Data` behavior for missing history or insufficient coverage, plus `Promising`/`Inconclusive` classification only after the configured historical evidence is sufficient. Stage 2 does not promote a candidate from rank alone.
- Added bounded `MastHistoryAdapter` support for querying selected sectors and caching the available official MAST/TESS light curves with existing provenance behavior.
- Added Stage 2 persistence that moves an explicitly eligible candidate into `stage2`, records the model/ranking versions and input products, writes the Stage 2 and Overall snapshots, and preserves the prior normalized light curve when a backfill is still missing.
- Added `run_stage2.py` with cached-product, history-manifest, bounded-sector-fetch, dry-run, and explicit diagnostic override paths. The normal path accepts only `Promising` Stage 1 candidates.
- Worker tests pass with 16 tests, including insufficient 30-day history, a 365-day repeatable fixture, Stage 2/Overall snapshot persistence, and the no-rank-only promotion rule.
- The real cached MAST/TESS target was evaluated non-mutatingly: one sector yielded 27.88 days, 3 observed events, and 702.12 missing days in the requested 730-day lookback, so the honest Stage 2 result is `Needs More Data` with evidence score 52.0226.

### Item 9: Stage 3 chronological holdout prediction — complete

- Added `Stage3Processor` with a replaceable model adapter boundary. The first implementation uses a CPU-capable scikit-learn logistic model over transit-phase features; a transparent phase-window baseline is evaluated against the same holdout.
- Stage 3 sorts observations chronologically, trains on the first two windows, predicts dimming in the third, and persists the window boundaries, model versions, precision, recall, F1, accuracy, false-positive rate, event recovery/accuracy, timing error, and depth error.
- Added explicit `Needs More Data` behavior for missing/short histories and positive-event support, with `Promising`/`Inconclusive` classification only after a usable holdout. The UI-facing result remains an assessment and never uses scientific-confirmation language.
- Added Stage 3 persistence that moves an explicitly eligible Stage 2 candidate into `stage3`, updates Stage 3 and Overall rankings, records a `stage3_evaluated` event, and writes replay snapshots. Reading those snapshots does not invoke the model again.
- Added `run_stage3.py` for cached FITS, history manifests, bounded MAST sector fetches, normalized Stage 2 fallback data, dry runs, and explicit diagnostic overrides. Normal execution accepts only `Promising` Stage 2 candidates.
- Pinned `scikit-learn==1.6.1` in the worker image. Docker worker tests pass with 19 tests, including a deterministic 90-day holdout fixture, baseline comparison, persistence, and replay read-without-rerun verification.

### Item 10: Catalog matching, candidate lifecycle, and Overall ranking — complete

- Added `ExoplanetArchiveAdapter` with narrow `pscomppars`/`toi` TAP projections, bounded row counts, cache TTL, atomic cache writes, recorded queries, partial-provider errors, and configurable coordinate/period tolerances.
- Added conservative matching rules: target ID plus compatible period, or coordinate plus compatible period. A target-ID match with an incompatible period is not treated as a known match, preventing a known planet on the same star from automatically hiding a different-period candidate.
- Added `publish_known_matches`, which records a catalog evaluation and `known_match` event, changes the candidate to inactive `Known Match`, preserves its history, removes it from Overall and all stage snapshots, and re-ranks surviving candidates.
- Added `promotable_candidates` so capacity-driven promotion remains explicitly status-gated: only `Promising` candidates within top-X are selectable; `Needs More Data` never advances from rank alone.
- Extended Stage 1 publication to maintain Overall snapshots and rank existing candidates across stages. Movement tokens now include `rapid_rise`, `rapid_fall`, and `sustained_top` in the form expected by the dashboard.
- Added `run_catalog_match.py` with cached projection, optional refresh, candidate filtering, dry-run, and publish paths. Docker worker tests pass with 22 tests, including archive parsing/cache behavior, conservative matching, known-match tombstoning/re-ranking across four views, and top-X promotion gating.
- Verified the live official TAP path with the current candidate: both configured projections cached successfully, the candidate produced no match, and the dry run published no state change. The `pscomppars` projection intentionally uses only columns confirmed by the current archive schema.
