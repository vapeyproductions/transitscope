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
