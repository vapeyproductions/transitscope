# Build Checklist

## Build Preferences

- **Build mode:** Step-by-step
- **Comprehension checks:** Yes. After each item, explain what was implemented, why it works, and surface only decisions that materially affect the project.
- **Git:** Commit after every checklist item.
- **Verification:** Yes. Stop after every item for tests/manual inspection before continuing.
- **Check-in cadence:** Every item.

## Build Strategy

The first vertical slice is intentionally narrow: one real MAST/TESS light curve enters the Stage 1 pipeline and appears on the dashboard. It is a verification checkpoint, not the final MVP. The checklist then expands the same contracts to batched Stage 1 processing, Stage 2 historical validation, Stage 3 holdout prediction, catalog matching, overall ranking, replay, and the candidate modal.

The target universe remains approximately 126,000 stars. The implementation will create a manifest for that universe, but it will download and process bounded batches rather than loading every light curve at once. Batch size, active-stage capacities, and parallelism are tuned from measurements.

## Checklist

- [x] **1. Establish the Dockerized project foundation**
  Spec ref: `spec.md > 4. System architecture > 4.1 Services` and `spec.md > 12. Docker and deployment`
  What to build: Create the `web`, `api`, and `worker` service structure, shared data volume, environment configuration, health endpoints, and a minimal README with local startup instructions. Keep the service boundaries compatible with the final architecture.
  Acceptance: Docker starts the services; the API reports healthy; the worker reports its configured mode; no authentication is required; configuration values are read from environment variables.
  Verify: Run `docker compose config`, `docker compose up --build`, then check the API and worker health endpoints. Commit the foundation.

- [x] **2. Create DuckDB schemas, Parquet paths, and the publish boundary**
  Spec ref: `spec.md > 5. Normalized data contracts` and `spec.md > 4. System architecture > 4.1 Services`
  What to build: Implement the storage layer for target manifests, ingest runs, candidates, evaluations, pipeline events, ranking snapshots, and published data versions. Add filesystem paths for raw and normalized data. Make writes idempotent and ensure the API cannot observe a partially committed batch.
  Acceptance: A test transaction can insert a target, evaluation, event, and ranking snapshot; the published version appears only after all referenced rows exist; re-running the same write does not duplicate it.
  Verify: Run storage unit tests and inspect the DuckDB tables with a small command-line query. Commit the storage layer.

- [x] **3. Build the thin Stage 1 dashboard shell**
  Spec ref: `spec.md > 10. Frontend specification > 10.1 Dashboard shell` and `spec.md > 10. Frontend specification > 10.5 Accessible and honest states`
  What to build: Create the dark scientific dashboard frame with the Stage 1 tab, ranking table layout, loading/empty/error/stale states, refresh control placeholder, and candidate-row visual contract. Use no invented astronomy results; the initial table should clearly communicate that data has not arrived.
  Acceptance: The app loads without an account, presents a readable scientific dashboard, shows an empty Stage 1 state, and gives status/activity fields a stable visual location. Color is not the only status signal.
  Verify: Run the web lint/typecheck command and manually inspect loading, empty, and error states in the browser. Commit the shell.

- [x] **4. Implement the real MAST/TESS ingestion probe**
  Spec ref: `spec.md > 3. External data roles and provenance > 3.1 MAST/TESS observation adapter` and `spec.md > 6. Processing pipeline > 6.1 Bootstrap and batching`
  What to build: Implement the MAST adapter and probe configuration for one manageable sector/product family. Query metadata, select one real product, download it into the cache, record provenance/checksum, and expose a clear provider error when the archive request fails. Keep the target-manifest interface ready for the full universe.
  Acceptance: The probe can retrieve an official MAST/TESS artifact or reports a reproducible provider failure; the cached artifact has source identifiers and retrieval metadata; no raw file is committed to the repository.
  Verify: Run `python scripts/probe_mast.py` with the probe configuration and inspect the resulting manifest/cache metadata. Commit the adapter and probe.

- [x] **5. Process one target through Stage 1**
  Spec ref: `spec.md > 6. Processing pipeline > 6.2 Light-curve cleaning and normalization` and `spec.md > 6.3 Stage 1: broad periodic-dip screening`
  What to build: Parse the real product, apply quality filtering and robust normalization, run the transparent periodic-dip/BLS feature extraction, calculate a versioned evidence score, and persist the target, evaluation, candidate status, and first ranking event.
  Acceptance: One real target produces a normalized series, interpretable Stage 1 features, a score labeled as evidence rather than planet probability, and a valid candidate lifecycle state including `Needs More Data` when coverage is insufficient.
  Verify: Run the single-target pipeline command; inspect the stored features/evaluation and confirm the score is reproducible for the same model version. Commit the Stage 1 single-target path.

- [x] **6. Connect the API to the live Stage 1 dashboard**
  Spec ref: `spec.md > 9. API contract` and `spec.md > 10. Frontend specification > 10.2 Ranking row`
  What to build: Implement `/api/status`, `/api/rankings`, candidate detail, light-curve, and history endpoints, then replace the shell placeholder with the persisted Stage 1 result. Add manual refresh behavior that reads the newest published version without rerunning the model.
  Acceptance: The dashboard shows the real candidate identity, evidence score, status, activity, rank movement, and last update; a candidate row opens a detail response; refresh closes any open modal and applies only a newer published state.
  Verify: Run API tests, start the stack, run the probe/pipeline, open the dashboard, and manually verify the real candidate appears. Commit the first end-to-end vertical slice.

- [ ] **7. Expand Stage 1 to bounded parallel batches and replacement sampling**
  Spec ref: `spec.md > 6.1 Bootstrap and batching`, `spec.md > 6.3 Stage 1`, and `spec.md > 6.7 Adaptive sampling experiment`
  What to build: Generate/update the approximately 126,000-target manifest, process configurable batches with bounded CPU parallelism, checkpoint after each batch, maintain the top-N display, and select replacements when candidates leave Stage 1. Start with random/stratified coverage and record the selection policy; keep adaptive sampling behind a flag.
  Acceptance: The pipeline does not load the universe into browser memory; batch failures do not erase completed results; processed/total counts are visible; ranking changes create persisted snapshots; a candidate leaving the active pool makes room for an unprocessed target.
  Verify: Run a measured small batch, compare serial and parallel throughput, restart the worker, and confirm it resumes from the checkpoint without duplicating events. Commit the batch implementation and record measured defaults.

- [ ] **8. Add Stage 2 historical repeatability validation**
  Spec ref: `spec.md > 6.4 Stage 2: longer-baseline repeatability` and `spec.md > 7.1 Status rules`
  What to build: For eligible top-X Stage 1 candidates, fetch the available longer history, target a two-year lookback where possible, calculate repeatability/timing/depth/shape features, and update the Stage 2 ranking. Implement `Needs More Data` backfill behavior without automatic promotion from rank alone.
  Acceptance: Stage 2 candidates can be `Promising`, `Needs More Data`, or `Inconclusive`; missing history remains `Needs More Data`; completed evaluations retain their window and metrics; Stage 2 capacity is configurable.
  Verify: Run Stage 2 against a small shortlist with both sufficient and insufficient-history fixtures, inspect status transitions, and verify the Stage 2 ranking snapshot. Commit Stage 2.

- [ ] **9. Add Stage 3 chronological holdout prediction**
  Spec ref: `spec.md > 6.5 Stage 3: chronological holdout prediction` and `spec.md > 13. Verification plan > Model evaluation requirements`
  What to build: Split eligible candidate observations chronologically, train the CPU-capable shortlist model on two windows, predict dimming in the third, persist holdout metrics, and update Stage 3 evidence/ranking. Keep the model adapter replaceable so a neural-network experiment can be measured later without changing the replay contract.
  Acceptance: Stage 3 reports supported metrics such as precision, recall, F1, event accuracy, timing error, or depth error; model version and windows are persisted; the UI never labels the result as confirmation.
  Verify: Run Stage 3 with a recorded time-series fixture, compare against the transparent baseline, and confirm replay data contains the evaluation without rerunning it. Commit Stage 3.

- [ ] **10. Implement catalog matching, candidate lifecycle, and Overall ranking**
  Spec ref: `spec.md > 3.2 Known-object catalog adapter`, `spec.md > 6.6 Known-object cross-reference`, and `spec.md > 7. Candidate lifecycle and ranking policy`
  What to build: Add the NASA Exoplanet Archive TAP adapter/cache, configurable coordinate/period matching, Known Match tombstoning, finalized Inconclusive behavior, top-X promotion rules, rank movement indicators, and the Overall ranking combination across stages.
  Acceptance: A catalog match emits a mapping event and removes the candidate from every active ranking while preserving history; lower-ranked candidates can move up; `Needs More Data` does not auto-promote; Overall and stage-specific scores are versioned and inspectable.
  Verify: Run catalog parsing/matching tests, simulate a known match and replacement candidate, and inspect Overall plus stage-specific rankings. Commit lifecycle and Overall ranking.

- [ ] **11. Add persisted replay and the candidate investigation modal**
  Spec ref: `spec.md > 8. Replay design` and `spec.md > 10.3 Candidate modal` / `10.4 Replay controls`
  What to build: Implement independent replay streams for Overall and each stage using saved snapshots/events, with empty-start state, pause, drag, and speed controls. Add the blurred-background candidate modal with light curve, evidence metrics, current activity, lifecycle history, and next step.
  Acceptance: Replay reconstructs candidates entering, changing rank, advancing, becoming inconclusive, and becoming known matches; replay makes no provider/model calls; each tab has an independent playhead; refresh closes the modal; one candidate can be fully inspected.
  Verify: Run replay determinism tests, count model/provider calls before and during replay, manually replay all four views, and verify modal history at multiple replay positions. Commit replay and investigation UI.

- [ ] **12. Prepare the Devpost handoff**
  Spec ref: `prd.md > Submission Proof Points` and `spec.md > 16. Definition of done for the first working draft`
  What to build: Finalize README setup/provenance/limitations, confirm the planning docs are present, capture the full-pipeline replay story, prepare demo steps, collect screenshots or recording targets, and document the repo/run instructions needed for `$prepare-submission`.
  Acceptance: The project can be started from the documented Docker commands; the demo can show real ingestion, all three stages, rank changes, known-match removal, candidate inspection, and replay; the write-up clearly avoids scientific-confirmation claims.
  Verify: Run the full Docker/API/UI verification path, review the demo script and screenshots, and confirm the next command is `$prepare-submission`. Commit the handoff materials.

