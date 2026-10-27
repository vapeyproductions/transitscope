# Title

TransitScope: Exoplanet Evidence Observatory

## One-line Summary

A replayable live observatory that screens public TESS light curves for transit-like signals and shows how candidates gain—or lose—evidence through increasingly rigorous tests.

## Project Story

## Inspiration

Earlier in October, reports showed that AI-assisted analysis had helped an amateur astronomer surface a possible hidden exoplanet. Those stories made the idea feel both exciting and approachable: astronomy discoveries do not have to begin inside a polished institutional instrument; they can begin with a careful question asked of public data. The reporting that sparked this project includes [Scientific American's coverage](https://www.scientificamerican.com/article/amateur-astronomer-harnesses-ai-to-find-a-possible-hidden-exoplanet/) and [Gizmodo's coverage](https://gizmodo.com/did-claude-help-an-amateur-astronomer-discover-a-new-planet-2000824393).

The project began as a much broader idea about finding unusual behavior in space. The Learn Skill Pack helped narrow that idea into one concrete, demonstrable workflow: screen stellar brightness measurements for transit-like signals, then make the evidence-building process visible enough for a student or curious observer to understand.

## What it does

TransitScope is a live and replayable assessment observatory for potential exoplanet signals. It samples public TESS observations, looks for periodic brightness dips that could be caused by an orbiting object, and ranks candidates by the strength and consistency of the available evidence.

Candidates move through three increasingly demanding stages:

1. **Stage 1** screens light curves for periodic, transit-like patterns.
2. **Stage 2** checks whether the pattern repeats across a longer chronological history.
3. **Stage 3** tests whether a model trained on earlier observations can predict a later observation.

The dashboard shows the overall and stage-specific rankings, candidate statuses, activity, metrics, light curves, pipeline history, and saved replay events. It is intentionally framed as an assessment of potential planets—not scientific confirmation. A candidate can remain promising, need more data, become inconclusive, or be identified as a known object and removed from the active rankings.

## How we built it

The data path starts with official MAST/TESS observations and uses the NASA Exoplanet Archive for conservative known-object cross-reference. A Dockerized worker owns ingestion and model execution. It writes normalized observations, candidate assessments, ranking snapshots, and pipeline events to DuckDB. A read-only FastAPI layer publishes consistent state to a Next.js/TypeScript dashboard.

The model strategy favors evidence that can be explained. Stage 1 uses a Box Least Squares search and combines period, depth, signal strength, event count, consistency, coverage, and data quality into an interpretable evidence score. Stage 2 reuses the detected signal while checking longer-history repeatability. Stage 3 uses a CPU-capable scikit-learn logistic model over transit-phase features, compares it with a transparent phase-window baseline, and evaluates on a later chronological holdout. This gives the interface concrete measurements to show instead of presenting an unexplained “planet probability.”

The observatory also persists its published rankings and events. Replay reads those saved states rather than rerunning expensive models, so a visitor can inspect how candidates entered, moved through, or left the pipeline. Codex and the Devpost Learn Skill Pack were used to interview the idea, create `scope.md`, `prd.md`, and `spec.md` before implementation, execute a verification-based checklist, and keep the build grounded in tests and official data probes.

## Challenges we ran into

The first challenge was scope. A request to search 126,000 targets sounds simple until data availability, archive latency, long historical windows, CPU limits, and staged compute are made explicit. We had to distinguish a real public-data workflow from an unrealistic promise of instant discovery.

The second challenge was designing the pipeline as a fair investigation rather than a one-shot leaderboard. A high Stage 1 score is only a reason to investigate. Candidates need repeatability, chronological holdouts, and enough coverage before their status can change. Missing data must remain “Needs More Data,” not be silently treated as negative evidence.

The third challenge was making live behavior inspectable. Worker publication, queue transitions, ranking changes, and replay all need durable state. During development, archive schema differences and DuckDB publication/read timing exposed how easily a dashboard can show stale or partial information. We added bounded reads, publication checkpoints, cached last-published state, and explicit loading, unavailable, and empty states so the interface is honest about what has and has not happened.

Finally, we had to communicate uncertainty without making the product feel empty. The answer was to show the evidence, the next test, the candidate history, and the changes in rank—not to overstate a provisional signal as a confirmed planet.

## Accomplishments that we're proud of

- We turned a broad space-anomaly idea into a focused end-to-end observatory with a clear scientific question.
- The current build uses real MAST/TESS-derived observations, provenance, checksums, Docker services, tests, and a public dashboard deployment.
- The three-stage workflow is visible in the product instead of hidden behind a single score.
- Replay is deterministic and read-only: it shows saved ranking and pipeline transitions without rerunning the models.
- Candidate details combine a light curve, interpretable metrics, current activity, next evidence, and pipeline history in one investigation view.
- The repository contains the planning artifacts that shaped the build, including the required `scope.md`, `prd.md`, and `spec.md` files.
- We built the product to be useful even when no new planet is found. The changing quality and rarity of evidence is the experience.

## What we learned

We learned that AI-assisted development becomes much more useful when the agent has a durable plan, explicit constraints, and verification checkpoints. The most important improvements did not come from asking for more code; they came from clarifying what a candidate represents, what each stage is allowed to conclude, and which state must be saved for replay.

We also learned that an evidence score is not a probability of a planet. It is a prioritization signal whose meaning depends on coverage, repeatability, model evaluation, and comparison with other candidates. Archive data is not the same thing as a live stream, and a “live” product must explain refresh and publication timing. Finally, observability is part of the model: queue health, data sufficiency, model metrics, and publication state are necessary to understand whether the pipeline is making progress.

## What's next for TransitScope

The next step is to make the production pipeline more scalable and scientifically useful: move durable state to managed storage and queues, improve throughput across the target universe, expand catalog cross-reference coverage for final Prime Candidates, and calibrate the stage metrics against labeled transit and false-positive examples.

We also want to deepen the educational layer. The existing explainer can show how a passing object dims a star and how that pattern becomes evidence; future versions can add clearer comparisons between real transit signals, instrumental artifacts, and known objects. The long-term goal is a public observatory where visitors can follow a candidate from first suspicious dip to a well-supported research lead—or watch the evidence responsibly rule it out.

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
- Live queue policy with a 500-target Stage 1 processing pool, status-gated promotion, separate Stage 2/3 capacities, and same-cycle replacement sampling.
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
