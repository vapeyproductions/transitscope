# TransitScope demo script

Target length: 1–3 minutes.

## Story

TransitScope is a live assessment of potential planets. It screens public MAST/TESS observations, keeps the evidence ranked, and subjects only eligible candidates to increasingly demanding tests. A high rank is an investigation priority, not scientific confirmation.

## Recording sequence

1. **0:00–0:20 — Set the question.** Show Overall and say: “We are looking for transit-like dimming in public stellar observations. The board ranks evidence and shows what the pipeline is doing now.”
2. **0:20–0:45 — Show the funnel.** Visit Stage 1, Stage 2, and Stage 3. Explain that only Promising candidates advance, Needs More Data holds its slot, and Inconclusive candidates remain visible while releasing capacity. Point out Prime Candidate in Stage 3.
3. **0:45–1:10 — Investigate one candidate.** Open a row and show the normalized light curve, evidence score, stage metrics, next action, current activity, and history.
4. **1:10–1:35 — Replay.** Start the active tab's replay, pause after a candidate rises or changes stage, and drag to a saved snapshot. Say that snapshots are persisted, so replay makes no provider or model calls.
5. **1:35–1:55 — Show removal.** If the demo data contains a catalog match, show the highlighted `Already Known` removal and then the Identified tab with its match evidence and prior stage performance.
6. **1:55–2:15 — Close honestly.** Show the data source and provisional-evidence language. Explain that archive availability controls the live cadence and that the product assesses candidates rather than claiming discovery.

## Evidence to capture

- Overall or Stage 1 ranking with processed count, activity labels, and movement indicators.
- Stage 2/3 view showing the stricter metrics and status labels.
- Candidate modal with the light curve and pipeline history.
- Replay at the empty start and a later populated/rank-change snapshot.
- Identified tab with an `Already Known` candidate, if available in the final dataset.

## Recording notes

- Use the seeded official MAST/TESS data or a clearly labeled local fixture; do not invent a new planet.
- Keep the browser at a readable zoom and pause long enough for status labels to be legible.
- Say “candidate,” “assessment,” and “evidence” throughout; avoid “confirmed,” “discovered,” or “planet probability.”
