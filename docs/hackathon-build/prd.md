# Product Requirements Document

## Product Summary

The product is a live astronomy observatory for assessing potential exoplanets. It opens on an Overall leaderboard and shows how candidates move through three increasingly rigorous assessment stages. Users can watch current activity, refresh when they want new data, replay each ranking from its beginning, and open one candidate at a time to inspect its evidence and history.

The product must be honest about uncertainty. It assesses potential planets; it does not claim scientific confirmation or guarantee that a new planet will be found. The main “wow” moment is seeing live data and completed pipeline steps change the rankings over time.

## Target User

Students and curious people who want to understand what happens between a suspicious brightness pattern and a more credible exoplanet candidate. They should be able to explore without prior astronomy expertise, while still seeing enough evidence to understand why a candidate moves or is removed.

## Core User Journey

1. The user opens the app and lands on the live Overall ranking without signing in.
2. The user sees the current top candidates, their pipeline status, current activity, rank movement, and last update information.
3. The user switches between Overall, Stage 1, Stage 2, and Stage 3 tabs to understand how each view differs. Each tab represents its own ranking and has its own Replay control.
4. The user watches the system change over time, or selects Refresh to apply available updates. If the user is reading a candidate modal, Refresh closes the modal so the updated leaderboard is visible.
5. The user clicks one candidate. A modal opens over a blurred dashboard background and shows the candidate’s current assessment, light curve, evaluation performance, current activity, and pipeline history. Candidate comparison is not required in the first version.
6. The user clicks Replay on a ranking. The view resets to its earliest state, briefly shows an empty or nearly empty ranking, and gradually reconstructs the ranking as candidates are discovered, promoted, weakened, or removed.
7. The user pauses, drags the replay bar to a different point, or changes playback speed. The user can observe how the same candidate’s rank and status evolved without entering exact timestamps.
8. The user ends on a high-ranking unresolved candidate and can see why it is promising without mistaking the result for confirmation.

## Epics And User Stories

### Epic 1: Live Overall Observatory

- As a curious student, I want the app to open on a live Overall ranking so that I can immediately see what the observatory is investigating.

Acceptance criteria:

- The app opens on the Overall tab in live mode.
- No account or setup is required to view the dashboard.
- The ranking displays a compact top list, targeting approximately 50–100 candidates rather than every processed star.
- Each row includes the candidate identity, rank, current status, current activity, rank movement, and last relevant update.
- A candidate with no active processing displays `No current activity` rather than appearing broken or stale.
- The visual treatment is space-themed but the hierarchy reads as a clear scientific dashboard.

### Epic 2: Stage-Specific Rankings

- As a user, I want separate Stage 1, Stage 2, and Stage 3 tabs so that I can understand where candidates are in the assessment process.

Acceptance criteria:

- The user can move between Overall, Stage 1, Stage 2, and Stage 3 without losing the application state.
- Each stage shows its own ranking, activity states, statuses, and rank movement indicators.
- The Overall view combines active candidates across stages; it does not replace the stage-specific views.
- Candidates that have been mapped to known objects are removed from active rankings but remain represented in historical replay.

### Epic 3: Activity And Rank Movement

- As a user, I want to see what is happening to each candidate so that a changing rank has an understandable reason.

Acceptance criteria:

- Activity labels can include `No current activity`, `Fetching new data`, `Fetching historical data`, `Running Stage 1 screening`, `Running Stage 2 validation`, `Running Stage 3 prediction`, and `Comparing with known objects`.
- Activity is displayed for every candidate row across every ranking view.
- Rank movement includes rising, falling, and unchanged indicators.
- A sustained top performer can display a flame indicator.
- A rapidly rising candidate can display a left-to-right zigzag-up indicator.
- The interface provides enough visual distinction that movement indicators do not replace the candidate’s numeric rank or status.

### Epic 4: Intentional Live Refresh

- As a user, I want to decide when to apply new data so that the dashboard does not change while I am reading it.

Acceptance criteria:

- A subtle refresh control is available in live mode.
- Refresh applies available ranking, activity, and status updates.
- Refresh closes an open candidate modal and returns the user to the updated dashboard.
- If the feed is unavailable, the app preserves the last successful rankings and shows a small notice such as `Data source unavailable — showing last update`.
- The user can continue browsing the last successful state while the feed is unavailable.

### Epic 5: Ranking Replay

- As a user, I want to replay any ranking from its beginning so that I can watch the assessment process unfold.

Acceptance criteria:

- Each ranking tab has its own Replay control near the ranking title.
- Replay begins at the earliest available state for that ranking and does not require manual date entry.
- The initial empty or nearly empty state is shown briefly rather than being skipped entirely or held for an unnecessarily long time.
- Candidates progressively appear, change rank, advance, become inconclusive, need more data, or become known matches as the replay advances.
- The replay control supports pause, dragging through the timeline, and a selectable playback speed.
- The timeline is intentionally approximate; users do not need to select exact timestamps.
- Each tab has an independent replay position. Replaying Stage 2 does not silently change the replay position of Overall, Stage 1, or Stage 3.
- Replay uses the saved historical outcome and does not visibly restart expensive analysis.

### Epic 6: Candidate Investigation Modal

- As a user, I want to inspect one candidate without leaving the leaderboard so that I can understand its evidence and history in context.

Acceptance criteria:

- Clicking a candidate opens a modal over the current dashboard.
- The dashboard background is visibly blurred while the modal is open.
- The modal includes the candidate’s light curve, current status, current activity, performance across completed evaluations, and pipeline history.
- The modal communicates what the candidate is waiting for or what test comes next when applicable.
- The user can close the modal and return to the same ranking view.
- Candidate comparison is out of scope for the first version.
- The precise information order and visual arrangement can be finalized after the first dashboard prototype is visible, provided all required evidence is available.

### Epic 7: Candidate Status And Lifecycle

- As a user, I want candidate statuses to persist until evidence changes them so that the rankings do not imply unjustified certainty.

Acceptance criteria:

- `Promising` candidates can advance when they enter the capacity-driven top-X group for their current stage.
- `Needs More Data` candidates remain visible and do not advance automatically based only on rank.
- Additional historical or live data can cause a `Needs More Data` candidate to become `Promising`, `Inconclusive`, or `Known Match`.
- `Inconclusive` candidates remain visible in the historical record and comparative ranking history but are not repeatedly sent through expensive investigation.
- `Known Match` candidates generate a visible mapping event and are removed from all active rankings.
- When a candidate is removed, lower-ranked active candidates can move up and become eligible for the next stage.
- A replay shows the status transition and removal at the point it occurred.

### Epic 8: Honest Empty And Delayed States

- As a user, I want the app to explain empty or delayed states so that I do not mistake missing data for a failed product or a negative scientific result.

Acceptance criteria:

- Early replay can display an empty ranking with a brief scanning message.
- A live candidate with insufficient or delayed evidence remains visible as `Needs More Data`.
- A candidate is not labeled `Inconclusive` merely because new data has not arrived yet.
- If a stage has no currently eligible candidates, the view explains whether it is still scanning, waiting for data, or has no candidates that meet the current progression policy.
- If a provider error prevents an update, the last successful state remains inspectable.

## Edge Cases

- A candidate rises rapidly while the user is viewing another tab; the change appears after the user refreshes, rather than interrupting the current view.
- A candidate is promoted, becomes a known match, and is removed before the user opens its detail modal; the historical replay still exposes the candidate and its removal event.
- Several candidates change status at once; rank movement should remain understandable without requiring the user to watch every event.
- No candidate reaches the current stage’s top-X capacity; the stage explains that it is waiting for stronger evidence.
- The feed is delayed for a long period; the dashboard continues to show last-update time and preserves the last known status.
- A replay is paused, the user changes speed, or drags backward; the view should remain deterministic and show the state represented by that replay position.
- A user switches tabs while a replay is active; each tab preserves its own independent replay state.
- A candidate has enough data for one test but not the next; it remains in the current stage with an explicit next-step explanation.

## What We Are Building

- Live Overall, Stage 1, Stage 2, and Stage 3 ranking views.
- Per-candidate activity labels, status labels, rank movement indicators, and last-update context.
- Manual live refresh with an unavailable-feed state.
- Independent replay for every ranking, with pause, drag, and speed controls.
- A blurred-background candidate investigation modal.
- Candidate lifecycle behavior for Promising, Needs More Data, Inconclusive, and Known Match.
- A clear distinction between active rankings and historical replay.

## What We Would Add With More Time

- The full optional learning layer with informational pop-ups and guided explanations.
- Candidate comparison views.
- Richer filtering, search, and historical investigation bookmarks.
- More detailed visualizations of sky regions and provider agreement.
- More polished animation and storytelling around rank changes.
- Additional astronomy data sources and deeper model diagnostics after the first pipeline is reliable.

## Submission Proof Points

- The demo opens on the Overall live dashboard and shows active candidate processing.
- A candidate’s rank, activity label, and movement indicators change after a refresh.
- The viewer opens a candidate modal and sees its evidence and pipeline history.
- Replay begins with an empty state and reconstructs a ranking through candidate discovery, promotion, weakening, and known-match removal.
- Independent stage replays demonstrate that the product is a working investigation tool rather than a static chart.
- The final description clearly states that the project assesses potential planets and does not claim scientific confirmation.
