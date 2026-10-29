# TransitScope portfolio brief

## One-line project description

Built a Dockerized, CPU-capable ML pipeline that ingests public MAST/TESS observations, extracts transit-like signals, evaluates them through three evidence stages, and exposes replayable rankings plus read-only ML Ops telemetry.

## What this demonstrates

- **ML engineering:** explainable Stage 1 Box Least Squares screening, Stage 2 historical repeatability, and Stage 3 chronological holdout prediction with a transparent baseline.
- **Data engineering:** provider adapters, durable target manifests, light-curve normalization, data sufficiency gates, checkpoints, and provenance for every evaluation.
- **Distributed-systems thinking:** bounded parallel worker jobs, explicit queues, downstream replacement debt, retries, rate limits, and a single-writer persistence model.
- **MLOps:** model/version metadata, status balance, dwell time, queue depth, throughput, job-call distribution, provider health, cumulative processed history, and persisted event streams.
- **Reproducibility:** ranking snapshots and pipeline events are saved as state transitions, so replay can reconstruct the system without rerunning models or refetching data.
- **Responsible scientific UX:** the interface distinguishes evidence, insufficient data, inconclusive results, catalog matches, and scientific confirmation.

## Metric definitions

The public ML Ops page intentionally reports measured values rather than hard-coded success claims:

| Metric | Meaning |
| --- | --- |
| Flow rate | Candidates evaluated per minute over the latest saved operating window. |
| Average stage time | Mean time from a stage job starting to its persisted result. |
| Needs-more-data time | Time spent waiting for or processing additional coverage before reassessment. |
| Handoffs | Candidates released from one stage and called by the downstream stage. |
| Top-100 status balance | Status percentages among the current leaderboard window. |
| Status dwell | Time a candidate remains represented in a given leaderboard status. |
| Model lift | Holdout metric minus the transparent baseline metric, when both are available. |
| Coverage health | Persisted products, candidates, and observation windows available to the pipeline. |
| Queue distribution | Stage-specific jobs called in each saved worker cycle; it is not retained-candidate capacity. |

If a metric cannot be computed because a holdout or coverage requirement is missing, the dashboard displays `Not computed` or `No data yet` rather than estimating it.

## Portfolio walkthrough

1. Open the [public ML Ops dashboard](https://desirable-luck-production-ad18.up.railway.app/ops) and start with the architecture brief.
2. Use **Pipeline motion** to inspect the saved worker-cycle timeline and queue events.
3. Compare **Where jobs are being called** with **Top-100 status balance and dwell** to identify bottlenecks.
4. Open a candidate from the observatory to show its light curve, model evidence, and chronological assessment trail.
5. Use replay to show that a ranking transition is read from persisted snapshots rather than recomputed during playback.

## Limitations to state explicitly

TransitScope is an investigation and prioritization system. It does not claim that a Prime Candidate is a scientifically confirmed planet. Catalog matches are cross-references, and model metrics are only meaningful when the underlying observation coverage and holdout are sufficient. The current Stage 3 adapter is CPU-capable and explainable; a neural network is not claimed as part of this deployment.
