# Title

TransitScope: Exoplanet Evidence Observatory

## One-line Summary

A replayable live observatory that screens public TESS light curves for transit-like signals and shows how candidates gain—or lose—evidence through increasingly rigorous tests.

## Problem

Public astronomy data is abundant, but the path from a suspicious brightness dip to a credible exoplanet candidate is difficult to follow. Many tools show a light curve or a catalog record without making the investigation legible: what was tested, what evidence changed the rank, why a candidate needs more data, or why it was removed.

TransitScope is designed for students and curious people who want to explore that process without needing to be professional astronomers.

## Solution

TransitScope presents a live/replay astronomy observatory for assessing potential planets. It starts with official MAST/TESS observations, screens targets for periodic transit-like dimming, ranks the strongest evidence, and carries eligible candidates through longer-history repeatability and chronological holdout prediction. A NASA Exoplanet Archive cross-reference can map known objects and remove them from active rankings while preserving the lifecycle event in history.

The product deliberately says “potential” and “assessment.” It does not claim scientific confirmation or promise that a new planet will be found.

## Why This Matters

The project turns a complicated scientific workflow into something observable and inspectable. The core experience is not a black-box “planet found” result; it is watching uncertainty change as data arrives, tests become stricter, candidates move up or down, and known matches leave the active board. That makes the product useful as a learning-oriented visualization and as a portfolio example of data ingestion, CPU machine learning, staged processing, durable state, and replayable results.

## How We Used AI

The application uses machine-learning and signal-processing methods rather than presenting an LLM chat interface as the product:

- Stage 1 uses an explainable Box Least Squares search and deterministic evidence score over transit period, depth, signal strength, event count, consistency, coverage, and data quality.
- Stage 2 reuses the Stage 1 period and checks repeatability across longer chronological history.
- Stage 3 uses a CPU-capable scikit-learn logistic model over transit-phase features, compares it with a transparent phase-window baseline, and evaluates predictions on a later chronological holdout.
- The worker keeps a replaceable model boundary so a neural-network experiment can be measured later against the same baseline and replay contract. The current draft does not claim a neural network or a calibrated planet probability.

AI-assisted development was used to explore the problem, challenge feasibility assumptions, turn the idea into a scoped plan, generate implementation drafts, inspect failures, and write tests. Official MAST/TESS and NASA Exoplanet Archive data remain the scientific data sources; they are not replaced by generated data.

## How We Used Codex

We used the Devpost Learn Skill Pack in Codex as a staged planning and build workflow:

1. Codex interviewed the project idea and narrowed an initially broad “space anomaly” concept into a focused exoplanet transit-assessment product.
2. The generated `scope.md`, `prd.md`, and `spec.md` documents captured the target user, lifecycle rules, honest status language, data-provider roles, replay contract, and operational boundaries before implementation.
3. The build checklist was executed one item at a time, with a verification pause and a Git commit after each item.
4. Codex used live official MAST/TESS and NASA Exoplanet Archive probes, Docker builds, unit/integration tests, API smoke checks, and browser checks to validate the implementation.
5. When the archive schema rejected an assumed column projection, the query was corrected to the confirmed current schema and the live probe was rerun. When the UI needed replay, the implementation was kept read-only so replay could not silently rerun a model.

The planning artifacts are included in `docs/hackathon-build/`.

## Key Features

### Working in the current draft

- Dockerized web, API, and worker services with a shared durable data volume.
- Official MAST/TESS ingestion probe with cached FITS provenance and checksums.
- Bounded Stage 1 batching, checkpointing, replacement sampling, and CPU parallelism controls.
- Stage 1 Box Least Squares screening with interpretable evidence features.
- Stage 2 repeatability and Stage 3 chronological holdout processing code with persisted metrics and model versions.
- NASA Exoplanet Archive TAP matching with conservative coordinate/period rules.
- Promising, Needs More Data, Inconclusive, and Known Match lifecycle states.
- Overall, Stage 1, Stage 2, and Stage 3 ranking APIs and dashboard tabs.
- Persisted replay metadata, snapshots, and events with independent tab playheads.
- Candidate modal with light curve, stage-specific metrics, current activity, next action, and pipeline history.
- Live queue policy with a 2,000-target Stage 1 processing pool, status-gated promotion, separate Stage 2/3 capacities, and same-cycle replacement sampling.
- Top-500 ranking display with 100-row pagination, coverage watermarks, unchanged-input deduplication, and Stage 3 `Prime Candidate` labeling.
- Honest loading, empty, unavailable, and provisional-evidence states.

### Not yet proven in the small local runtime dataset

- A populated multi-stage live feed with a real candidate promoted through Stage 2 and Stage 3.
- A recorded known-match removal in the current public demo dataset.
- Full-universe processing of all approximately 126,000 targets. The architecture batches that universe; the demo intentionally processes a small bounded sample.

## Architecture

```text
MAST/TESS + NASA Exoplanet Archive
                │
                ▼
      worker: adapters + staged models
                │
                ▼
 DuckDB + normalized series + snapshots/events
                │
                ▼
       read-only FastAPI projection
                │
                ▼
 Next.js/TypeScript observatory dashboard
```

The worker is the sole writer. The API reads only published DuckDB state. Ranking snapshots and pipeline events are committed before publication, which makes replay deterministic and prevents the UI from seeing a partial batch. Docker Compose provides the same service boundaries locally that the eventual public deployment would need, with persistent storage for replay history.

## Testing Instructions

From the repository root:

```bash
docker compose build api web worker
docker compose run --rm api python -m unittest discover -s tests -v
docker compose run --rm worker python -m unittest discover -s tests -p 'test_*.py' -v
docker compose up -d api web worker
```

Open `http://localhost:3000`. The development database contains real MAST/TESS-derived state; depending on the current worker checkpoint, candidates may appear in later stages while an exhausted stage shows an intentional empty state. Click a candidate row to open the modal. Click Replay to inspect the persisted timeline; replay does not invoke a provider or model.

For a fresh official-data probe:

```bash
python scripts/probe_mast.py
```

For a bounded Stage 1 run:

```bash
docker compose run --rm worker python prepare_target_universe.py \
  --universe-size 126000 \
  --probe-manifest /app/data/manifests/<successful-probe-manifest>.json
docker compose run --rm worker python run_stage1_batch.py \
  --batch-size 4 \
  --max-workers 1
```

The API endpoints used by the dashboard include `/api/status`, `/api/rankings?view=stage1&limit=100&page=1`, `/api/candidates/{candidate_id}`, `/api/candidates/{candidate_id}/light-curve`, `/api/candidates/{candidate_id}/history`, and the three `/api/replay/{view}/...` endpoints. The complete recording sequence is in `docs/hackathon-build/demo-script.md`.

## Public Demo Link

TODO: Add the public deployed dashboard URL. Local fallback: `http://localhost:3000` after `docker compose up --build`.

## Public Repository Link

TODO: Create or select the public GitHub, GitLab, or Bitbucket repository and add its URL here.

## Demo Video

TODO: Record and add a public 1–3 minute video URL. The official requirement says reviewers will not clone or run the project, so the video must show the working dashboard.

## Screenshot Shot List

Capture 3–5 screenshots after the final demo dataset is ready:

1. Overall live dashboard showing the top candidates, activity labels, movement indicators, processed count, and honest provisional-evidence language.
2. Stage 1 or Stage 2 ranking with a real candidate row and its evidence score.
3. Candidate investigation modal showing the normalized light curve, evidence metrics, next action, and pipeline history.
4. Replay at the empty-start position and again after candidates populate, with the saved snapshot timestamp/playhead visible.
5. A known-match removal or multi-stage progression view, if the final demo dataset contains one.

## Submission Readiness Notes

Official Devpost status checked on 2026-10-21: `Build With AI: Basics` is open for submissions and the participant is registered. Submissions close October 26, 2026 at 21:00 UTC. The official judging criteria are Design, Innovation/Idea, and Presentation. The official deliverables require a public repository, a public 1–3 minute video, and a short description; a public website is not required.

Current readiness: **drafting, not ready to submit**.

Strengths:

- The project has a coherent product story and a distinctive replayable scientific-investigation interface.
- The repository contains the required `scope.md`, `prd.md`, and `spec.md` planning documents.
- The current build has real MAST/TESS provenance, Docker setup, tests, and a browser-verifiable Stage 1 dashboard/modal/replay path.

Remaining blockers and proof gaps:

- The repository is not public yet. The MIT `LICENSE` is present locally; verify its public URL after publishing.
- There is no public deployed demo URL or video URL yet.
- The current local runtime has two active Stage 3 inconclusive candidates from the seeded official data, but it does not yet provide the complete visual showcase. Populate or fixture a convincing Stage 1 → Stage 2 → Stage 3 progression and known-match removal before recording the final demo.
- The official form still needs participant-provided answers: individual/team selection, country of residence, resources actually used, AI-cost impact, confidence score, open-source license URL, value created, technology-requirement explanation, and eligibility confirmations.
- Do not describe the evidence score as a planet probability or scientific confirmation.

## Known Limitations

- MAST is an archive, so “live” means new available published data plus intentional refresh; replay is the reliable way to show activity when the feed is quiet.
- The full 126,000-target universe is represented by a durable queue but is not processed on a laptop in one run.
- Stage 2 needs approximately two years of history where available; missing coverage remains `Needs More Data` rather than being treated as negative evidence.
- The current complex-model path is a CPU logistic baseline with a transparent comparator; no neural network is claimed in this draft.
- The current dashboard defers the optional learning pop-up layer until the technical pipeline is more fully populated.
- Stage-specific replay is deterministic only for snapshots that have actually been published; an empty stage is an honest absence of saved history, not an invented result.

## TODO Official Form Fields

Use the live Devpost submission form as the source of truth. Do not fill these with guesses:

- Field 28614: Individual or Team — TODO participant choice.
- Field 28615: Country of residence — TODO participant answer.
- Field 28618: Resources actually used — TODO participant answer; options include Intro video, Live build session, Cup of Joe office hours, Discord, or None.
- Field 28619: AI-cost impact — TODO participant answer.
- Field 28620: Confidence building a maintainable project with a coding agent — TODO participant answer from 1–5.
- Field 28998: Public URL to the open-source license file — `LICENSE` is now present locally; add the public repository URL after publishing.
- Field 28624: Value created — TODO participant answer.
- Field 28916: Explain use of the Devpost Learn Skill Pack — draft answer: “I used the Learn Skill Pack to interview and narrow the idea, then generated and followed scope, PRD, technical spec, checklist, and stepwise build documents. Each implementation item was verified before moving on, and the repository includes those planning documents.”
- Field 28917: Age of majority — participant confirmation required.
- Field 28617: Eligibility confirmation — participant confirmation required.

The project is not being sent to Devpost by this draft step.
