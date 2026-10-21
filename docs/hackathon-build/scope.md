# Project Scope

## Project Name Candidates

- TransitScope
- Starlight Lab
- TESScope
- Candidate Observatory

## One-Line Summary

A live assessment of potential planets that processes public stellar observations, ranks transit-like candidates by accumulating evidence, and updates their likelihood as new data and increasingly rigorous tests arrive.

## Target User

Students and curious people who want to explore how astronomers evaluate possible exoplanets without needing to understand the full scientific pipeline first.

## Problem

Public astronomy data is rich but difficult to interpret. A learner can see that a star's brightness changes, but usually cannot follow how a possible transit becomes a candidate, gets tested against more history, is compared with known objects, or is ruled inconclusive. Existing tools often show final charts or catalog entries without making the evolving investigation legible.

## Core Workflow

1. The user selects live mode or a historical replay window. Live mode ingests newly available observations as they arrive; replay mode starts at the beginning of the selected window and advances through the saved pipeline history.
2. The system processes a large candidate universe of approximately 126,000 stars in batches. The first stage performs broad, parallel screening for periodic transit-like dimming and records the evidence and rank changes.
3. The dashboard shows the overall active ranking plus separate Stage 1, Stage 2, and Stage 3 rankings. The UI surfaces a compact top list (targeting roughly 50–100 entries) rather than attempting to display every processed star.
4. The strongest available candidates are promoted into the next stage according to a capacity-driven top-X policy. X can change with available compute and the number of candidates; it is not a fixed scientific threshold.
5. Stage 2 pulls a longer historical window for promoted candidates and checks whether the observed dip pattern repeats with consistent timing and shape.
6. Stage 3 uses earlier observation windows to predict dimming in a later window, reports predictive accuracy, and updates the candidate's evidence score.
7. Candidates can be labeled `Promising`, `Needs More Data`, `Inconclusive`, or `Known Match`. Active rankings use the operational order `Promising` > `Needs More Data` > `Inconclusive`, with evidence score breaking ties. The top-X capacity gate controls stage advancement; no label alone bypasses it. `Needs More Data` can trigger targeted backfill, `Inconclusive` remains visible unless later evidence changes it, and `Known Match` candidates are removed from active rankings while their mapping event remains visible in replay.
8. Incoming data can change rankings at any stage. The overall dashboard reflects all active candidates, with advanced candidates generally rising because they have accumulated more evidence, while stage-specific views show the evidence appropriate to each stage.

## What We Are Building

- A provider-adapter layer selected for data quality and efficiency. The likely roles are a raw/systematics-corrected light-curve source, a known-exoplanet catalog, and only additional metadata sources that improve vetting or explainability.
- A batch-oriented processing system that can cover the 126,000-star universe without loading all data into the browser at once.
- A two-tier detection strategy: a fast, explainable first-pass screen across the broad universe, followed by a more complex model on shortlisted candidates. The complex model may use neural networks if validation shows they help; the product promise is evidence quality, not a particular model family.
- Parallel processing for the broad scan and later-stage work where it improves throughput. The first implementation should be CPU-capable, with optional GPU or hosted execution considered only if measurements show it is necessary.
- A timestamped pipeline history containing rank changes, promotions, test results, new-data events, status changes, and known-match removals. Replay uses this history and saved ranking snapshots or deltas; it never reruns expensive models just because a user presses Replay.
- A live/replay dashboard with a processing timeline, current rankings, stage-specific views, candidate detail pages, and a candidate history that explains how each result changed over time.
- Adaptive sampling as an experiment layered on top of baseline coverage. Initial batches should be random or stratified enough to maintain coverage; later sampling may use sector, sky position, data quality, and observed false-positive/inconclusive rates to prioritize productive regions and deprioritize systematic problem areas. Adaptive sampling must not permanently hide the rest of the 126,000-star universe.
- Minimal status explanations in the core interface. The larger learning layer—informational pop-ups, repeated concepts, and guided lessons—will wait until the technical pipeline is working.

## What We Are Not Building

- A scientific confirmation system or a claim that the app has discovered a planet. The honest product label is: “A live assessment of potential planets, updated for likelihood regularly using incoming data and increasingly rigorous testing.”
- A guarantee that a new planet will be found during the demo.
- User accounts, social features, collaboration tools, or personalization.
- 3D orbital visualization.
- An asteroid or general-purpose space-anomaly detector in the first version.
- A full-sky, all-provider, all-model production platform. Multiple providers are allowed only when each has a clear role and demonstrably improves data quality, verification, or context.
- A large custom neural network trained without a measured baseline. A transparent signal-processing baseline must exist so model improvements can be evaluated.
- A replay implementation that recomputes the pipeline on demand.

## Inspiration And References

- Planet Hunters TESS: citizen-facing inspection of transit-like light curves and human-readable candidate discovery.
- NASA Eyes on Exoplanets: approachable visual exploration of exoplanet information.
- MAST/TESS data products and the NASA Exoplanet Archive: candidate sources for observations, systematics-corrected light curves, metadata, and known-object cross-reference. Exact providers and access paths belong in the technical specification.
- Real exoplanet vetting workflows: inexpensive screening first, followed by progressively more resource-intensive tests that remove common false positives before deeper investigation.

Nearby-star and sky-region information is useful primarily for detecting contamination and shared systematics. A cluster of similar anomalies should be treated as a possible field/instrument artifact or blended-light issue before it is treated as evidence of multiple planets. This relationship will inform data-quality features and adaptive sampling, not create a direct “nearby stars imply planets” rule.

## Demo Path

1. Open the observatory dashboard and choose a live feed or a historical replay window.
2. Start replay from the beginning. The Stage 1 queue processes stars in batches, the ranking grows, and the top candidates change as evidence arrives.
3. Select a candidate to see its light curve, evidence breakdown, current status, and the next test.
4. Watch strong candidates move into Stage 2, receive longer-history analysis, and change rank. Show a candidate becoming `Known Match` and disappearing from active rankings while remaining in the historical timeline.
5. Continue into Stage 3, where candidates are evaluated by holdout prediction accuracy and the ranking changes again.
6. Switch to the overall ranking and compare the active leaders with the stage-specific views. End on the strongest currently unresolved candidate and show why it is promising without calling it confirmed.

## Submission Story

This project demonstrates a working, research-inspired end-to-end application built with the Devpost Learn Skill Pack. Its central value is not a generic AI chat interface; it makes a complex astronomy investigation observable, replayable, and honest about uncertainty. The repository will include the planning documents, source code, data-provider decisions, model evaluation notes, and a short demo showing the live/replay pipeline, candidate promotion, ranking changes, and known-match removal.
