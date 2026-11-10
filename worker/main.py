import json
import os
import re
import signal
import time
import atexit
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread

from pipeline.live import run_live_cycle
from pipeline.lifecycle import rebuild_overall_ranking, repair_legacy_history
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


running = True
status_write_lock = Lock()
heartbeat_stop_event: Event | None = None
heartbeat_thread: Thread | None = None


def stop_worker(_signum: int, _frame: object) -> None:
    global running
    running = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_flag(name: str, *, default: bool = False) -> bool:
    """Read a boolean environment setting without making truthiness surprising."""

    return os.getenv(name, "true" if default else "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_status_payload(status_path: Path) -> dict:
    try:
        loaded = json.loads(status_path.read_text())
        return loaded if isinstance(loaded, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _write_status_payload(status_path: Path, payload: dict) -> None:
    temporary_path = status_path.with_suffix(".json.tmp")
    temporary_path.write_text(json.dumps(payload, indent=2) + "\n")
    temporary_path.replace(status_path)


def write_status(
    data_dir: Path,
    state: str,
    details: dict | None = None,
    *,
    progress: bool = True,
) -> None:
    with status_write_lock:
        data_dir.mkdir(parents=True, exist_ok=True)
        status_path = data_dir / "worker-status.json"
        previous = _read_status_payload(status_path)
        now = utc_now()
        payload = {
            "service": "worker",
            "state": state,
            "mode": os.getenv("WORKER_MODE", "bootstrap"),
            "last_heartbeat": now,
        }
        # Preserve the last completed cycle and its compact telemetry while a new
        # cycle is running. This lets the API show the last known progress during
        # a long publication lock instead of falling back to an empty dashboard.
        for key in (
            "last_cycle",
            "last_cycle_metrics",
            "live_cycle_metrics",
            "telemetry",
            "catalog_health",
            "last_progress_at",
            "last_progress_state",
            "cycle_id",
            "cycle_started_at",
            "last_cycle_finished_at",
            "cycle_elapsed_seconds",
            "progress",
            "heartbeat_count",
            "last_stage1_publish_at",
            "last_stage1_published_count",
            "last_stage1_batch_id",
            "last_published_at",
        ):
            if key in previous:
                payload[key] = previous[key]
        if progress or "last_progress_at" not in payload:
            payload["last_progress_at"] = now
            payload["last_progress_state"] = state
        if details:
            payload.update(details)
        _write_status_payload(status_path, payload)


def touch_heartbeat(data_dir: Path) -> None:
    """Refresh liveness without pretending the pipeline made progress.

    The live cycle can spend minutes inside provider I/O or catalog matching.
    A separate heartbeat must continue during that work, while
    ``last_progress_at`` remains unchanged so the supervisor can distinguish a
    healthy long-running job from a stuck one.
    """

    with status_write_lock:
        data_dir.mkdir(parents=True, exist_ok=True)
        status_path = data_dir / "worker-status.json"
        payload = _read_status_payload(status_path)
        payload.setdefault("service", "worker")
        payload.setdefault("state", "starting")
        payload.setdefault("mode", os.getenv("WORKER_MODE", "bootstrap"))
        payload["last_heartbeat"] = utc_now()
        payload["heartbeat_count"] = int(payload.get("heartbeat_count") or 0) + 1
        _write_status_payload(status_path, payload)


def _heartbeat_loop(data_dir: Path, interval_seconds: float, stop_event: Event) -> None:
    interval = max(1.0, float(interval_seconds))
    while not stop_event.wait(interval):
        try:
            touch_heartbeat(data_dir)
        except OSError:
            # A transient volume write failure must not kill the worker. The
            # supervisor will use the stale heartbeat as the failure signal.
            continue


def stop_heartbeat() -> None:
    global heartbeat_stop_event, heartbeat_thread
    if heartbeat_stop_event is not None:
        heartbeat_stop_event.set()
    if heartbeat_thread is not None and heartbeat_thread.is_alive():
        heartbeat_thread.join(timeout=2.0)
    heartbeat_stop_event = None
    heartbeat_thread = None


def read_pipeline_control(data_dir: Path) -> dict:
    """Read the operator gate written by the authenticated ML Ops page."""

    control_path = data_dir / "pipeline-control.json"
    if not control_path.exists():
        return {"pipeline_paused": False}
    try:
        payload = json.loads(control_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"pipeline_paused": False}
    return payload if isinstance(payload, dict) else {"pipeline_paused": False}


def _manual_migration_dir(data_dir: Path) -> Path:
    return data_dir / "manual-migrations"


def _write_manual_migration(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def claim_manual_migrations(data_dir: Path) -> list[dict]:
    """Claim pending operator requests exactly once for the next cycle."""

    directory = _manual_migration_dir(data_dir)
    if not directory.exists():
        return []
    claimed: list[dict] = []
    for path in sorted(directory.glob("*.json"), key=lambda item: item.stat().st_mtime):
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict) or payload.get("status") != "pending":
            continue
        payload["status"] = "processing"
        payload["updated_at"] = utc_now()
        payload["message"] = "Claimed by the worker; evaluating the requested eligible candidates."
        try:
            _write_manual_migration(path, payload)
        except OSError:
            continue
        claimed.append(payload)
    return claimed


def update_manual_migration(data_dir: Path, request_id: str, **updates: object) -> None:
    path = _manual_migration_dir(data_dir) / f"{request_id}.json"
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        payload = {"request_id": request_id}
    payload.update(updates)
    payload["updated_at"] = utc_now()
    try:
        _write_manual_migration(path, payload)
    except OSError:
        pass


def queue_snapshot(store: DuckDBStore) -> dict[str, dict[str, int]]:
    """Capture the active stage/status queues at a cycle boundary."""

    snapshot = {stage: {} for stage in ("stage1", "stage2", "stage3")}
    with store.read_connect() as connection:
        rows = connection.execute(
            """
            SELECT current_stage, status, COUNT(*)
            FROM candidates
            WHERE active = true
            GROUP BY current_stage, status
            """
        ).fetchall()
    for stage, status, count in rows:
        if stage in snapshot:
            snapshot[stage][str(status)] = int(count or 0)
    return snapshot


def ranked_queue_samples(store: DuckDBStore, limit: int = 100) -> dict[str, list[dict[str, object]]]:
    """Capture rank/status telemetry for the ML Ops view.

    The motion view still renders a smaller representative subset, but the
    persisted cycle sample keeps the top 100 available for status composition
    and dwell-time metrics without querying the full ranking on every refresh.
    """

    samples: dict[str, list[dict[str, object]]] = {}
    with store.read_connect() as connection:
        for stage in ("stage1", "stage2", "stage3"):
            rows = connection.execute(
                """
                SELECT candidate_id, status, rank_stage, stage_score,
                       COALESCE(
                           (
                               SELECT MAX(event.occurred_at)
                               FROM pipeline_events AS event
                               WHERE event.candidate_id = c.candidate_id
                                 AND event.event_type IN (
                                     'stage1_evaluated',
                                     'stage2_evaluated',
                                     'stage3_evaluated',
                                     'known_match',
                                     'signal_alias_held',
                                     'signal_alias_released',
                                     'signal_independence_assessed'
                                 )
                           ),
                           c.last_evaluated_at
                       ) AS status_since
                FROM candidates AS c
                WHERE active = true AND current_stage = ?
                ORDER BY rank_stage NULLS LAST, stage_score DESC NULLS LAST, candidate_id ASC
                LIMIT ?
                """,
                [stage, limit],
            ).fetchall()
            samples[stage] = [
                {
                    "candidate_id": str(row[0]),
                    "status": str(row[1] or "unknown"),
                    "rank": int(row[2]) if row[2] is not None else index + 1,
                    "status_since": row[4].isoformat() if row[4] is not None else None,
                }
                for index, row in enumerate(rows)
            ]
    return samples


def _new_live_cycle_metrics(cycle_id: str, started_at: str) -> dict[str, object]:
    """Create the small status-side telemetry used while a cycle is running."""

    return {
        "cycle_id": cycle_id,
        "started_at": started_at,
        "handoff_metric_version": 2,
        "updated_at": started_at,
        "stage1": {"jobs": 0, "assessed": 0, "handoffs": 0},
        "stage2": {"jobs": 0, "assessed": 0, "handoffs": 0},
        "stage3": {"jobs": 0, "assessed": 0, "handoffs": 0},
    }


def _record_live_cycle_event(
    live_metrics: dict[str, object],
    state: str,
    details: dict[str, object] | None,
) -> None:
    """Fold dispatch/completion callbacks into worker-status telemetry.

    This is deliberately compact. The durable cycle payload remains the
    detailed source for completed cycles; these counters only keep the health
    endpoint truthful during a long-running cycle.
    """

    details = details or {}
    stage_match = re.search(r"stage[123]", state, re.IGNORECASE)
    if not stage_match:
        return
    stage_name = stage_match.group(0).lower()
    stage = live_metrics.get(stage_name)
    if not isinstance(stage, dict):
        return

    reason = str(details.get("reason") or "")
    if stage_name == "stage1" and state == "processing_stage1" and reason == "target_completed":
        # Stage 1 reports one completion callback per target. Count this live
        # signal immediately; the later batch-published callback is a durable
        # boundary, not a second job.
        stage["jobs"] = int(stage.get("jobs") or 0) + 1
        stage["assessed"] = int(stage.get("assessed") or 0) + 1
        live_metrics["updated_at"] = utc_now()
        return

    if state == "stage1_batch_published":
        stage["jobs"] = int(stage.get("jobs") or 0) + int(details.get("selected_count") or 0)
        stage["assessed"] = int(stage.get("assessed") or 0) + int(details.get("evaluated_count") or 0)
        live_metrics["updated_at"] = utc_now()
        return

    candidate_ids = details.get("candidate_ids")
    dispatched = 0
    if isinstance(candidate_ids, list):
        if reason in {
            "manual_stage2_migration",
            "stage2_top_window_reassessment_batch",
            "ranked_stage3_reassessment_batch",
        }:
            dispatched = len(candidate_ids)
    elif details.get("candidate_id") and reason in {
        "stage2_waterfall_request",
        "stage2_waterfall_gate_bootstrap",
        "stage3_initial_intake",
        "stage3_called_stage2",
    }:
        dispatched = 1

    if dispatched:
        stage["jobs"] = int(stage.get("jobs") or 0) + dispatched
        if stage_name == "stage2" and reason in {
            "manual_stage2_migration",
            "stage2_waterfall_request",
            "stage2_waterfall_gate_bootstrap",
        }:
            # The job is entering Stage 2, so it is a Stage 1 release.
            stage1 = live_metrics.get("stage1")
            if isinstance(stage1, dict):
                stage1["handoffs"] = int(stage1.get("handoffs") or 0) + dispatched
        elif stage_name == "stage3" and reason in {
            "stage3_initial_intake",
            "stage3_called_stage2",
        }:
            # The same dispatch is both a Stage 2 release and a Stage 3
            # replacement arrival, so both lanes expose the transition from
            # their own perspective.
            for routed_stage in ("stage2", "stage3"):
                routed = live_metrics.get(routed_stage)
                if isinstance(routed, dict):
                    routed["handoffs"] = int(routed.get("handoffs") or 0) + dispatched

    completed = details.get("job_completed")
    if completed:
        try:
            completed_count = max(0, int(completed))
        except (TypeError, ValueError):
            completed_count = 0
        stage["assessed"] = int(stage.get("assessed") or 0) + completed_count

    live_metrics["updated_at"] = utc_now()


def result_summary(results: list[dict] | None) -> dict[str, object]:
    items = results or []
    counts: dict[str, int] = {}
    queue_status_counts: dict[str, int] = {}
    for result in items:
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        queue_status = result.get("queue_status")
        if queue_status:
            queue_status_name = str(queue_status)
            queue_status_counts[queue_status_name] = queue_status_counts.get(queue_status_name, 0) + 1
    return {
        "attempted": len(items),
        "promising": counts.get("Promising", 0),
        "needs_more_data": counts.get("Needs More Data", 0),
        "queued_needs_more_data": queue_status_counts.get("Needs More Data", 0),
        "inconclusive": counts.get("Inconclusive", 0),
        "known_match": counts.get("Known Match", 0),
        "errors": counts.get("error", 0),
        "unchanged": counts.get("unchanged", 0),
        "statuses": counts,
        "queue_statuses": queue_status_counts,
    }


def candidate_status_summary(store: DuckDBStore, candidate_ids: list[object]) -> dict[str, object]:
    """Summarize the persisted statuses produced by a Stage 1 publish."""

    normalized_ids = [str(candidate_id) for candidate_id in candidate_ids if candidate_id]
    if not normalized_ids:
        return result_summary([])
    placeholders = ", ".join("?" for _ in normalized_ids)
    with store.read_connect() as connection:
        rows = connection.execute(
            f"SELECT status FROM candidates WHERE candidate_id IN ({placeholders})",
            normalized_ids,
        ).fetchall()
    return result_summary([{"status": row[0]} for row in rows])


def candidate_cycle_events(store: DuckDBStore, candidate_ids: list[object]) -> list[dict[str, object]]:
    """Return one event per Stage 1 candidate evaluated in this cycle.

    Cycle telemetry is intentionally compact, but the ML Ops view benefits from
    seeing the individual work items as dots. The candidate state is read after
    the publish boundary, so the event reflects the status that was produced
    by the Stage 1 evaluation.
    """

    normalized_ids = list(dict.fromkeys(str(candidate_id) for candidate_id in candidate_ids if candidate_id))
    if not normalized_ids:
        return []
    placeholders = ", ".join("?" for _ in normalized_ids)
    with store.read_connect() as connection:
        rows = connection.execute(
            f"SELECT candidate_id, status FROM candidates WHERE candidate_id IN ({placeholders})",
            normalized_ids,
        ).fetchall()
    statuses = {str(row[0]): str(row[1] or "unknown") for row in rows}
    return [
        {
            "candidate_id": candidate_id,
            "stage": "stage1",
            "status": statuses.get(candidate_id, "unknown"),
            "kind": "evaluation",
        }
        for candidate_id in normalized_ids
    ]


def result_cycle_events(stage: str, results: object) -> list[dict[str, object]]:
    """Normalize per-candidate stage results for the ML Ops dot timeline."""

    if not isinstance(results, list):
        return []
    events: list[dict[str, object]] = []
    for result in results:
        if not isinstance(result, dict) or not result.get("candidate_id"):
            continue
        event: dict[str, object] = {
            "candidate_id": str(result["candidate_id"]),
            "stage": stage,
            "status": str(result.get("status") or "unknown"),
            "kind": "evaluation",
        }
        if result.get("dispatch_source_candidate_id"):
            event["kind"] = "handoff"
            event["source_candidate_id"] = str(result["dispatch_source_candidate_id"])
        elif stage == "stage3":
            event["kind"] = "reassessment"
        events.append(event)
    return events


def queue_cycle_events(result: dict, store: DuckDBStore) -> list[dict[str, object]]:
    """Describe jobs that entered a stage queue during this worker cycle.

    Queue telemetry is intentionally distinct from ranking telemetry. A stage
    status is not a queued job: an Inconclusive candidate remains in its
    stage's ranked population but does not appear here unless it is later
    submitted for a new reassessment job.
    """

    events: list[dict[str, object]] = []

    def add(
        candidate_id: object,
        stage: str,
        kind: str,
        queue_status: str,
        reason: str,
        source_candidate_id: object | None = None,
    ) -> None:
        if not candidate_id:
            return
        event: dict[str, object] = {
            "candidate_id": str(candidate_id),
            "stage": stage,
            "kind": kind,
            "queue_status": queue_status,
            "reason": reason,
        }
        if source_candidate_id:
            event["source_candidate_id"] = str(source_candidate_id)
        events.append(event)

    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    for candidate_id in batch.get("candidate_ids") or []:
        add(
            candidate_id,
            "stage1",
            "intake",
            "New Target",
            "candidate_universe_intake",
        )

    refresh = result.get("stage1_refresh") if isinstance(result.get("stage1_refresh"), dict) else {}
    for candidate_id in refresh.get("candidate_ids") or []:
        add(
            candidate_id,
            "stage1",
            "reassessment",
            "Needs More Data",
            "stage1_needs_more_data_reassessment",
        )

    for request in result.get("stage1_requests") or []:
        if not isinstance(request, dict):
            continue
        is_refresh = bool(request.get("refresh_count"))
        for candidate_id in request.get("candidate_ids") or []:
            add(
                candidate_id,
                "stage1",
                "reassessment" if is_refresh else "intake",
                "Needs More Data" if is_refresh else "New Target",
                str(
                    request.get("intake_reason")
                    or request.get("replacement_reason")
                    or "stage3_waterfall_replacement"
                ),
            )

    continuous = result.get("continuous_stage1_intake")
    if isinstance(continuous, dict):
        for candidate_id in continuous.get("candidate_ids") or []:
            add(
                candidate_id,
                "stage1",
                "intake",
                "New Target",
                str(continuous.get("intake_reason") or "continuous_stage1_intake"),
            )

    for item in result.get("stage2") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("queue_kind") or "handoff")
        queue_status = str(item.get("queue_status") or ("Needs More Data" if kind == "reassessment" else "Promising"))
        add(
            item.get("candidate_id"),
            "stage2",
            kind,
            queue_status,
            str(item.get("queue_reason") or "stage3_downstream_request"),
            item.get("source_candidate_id"),
        )

    for item in result.get("stage3") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("queue_kind") or ("handoff" if item.get("dispatch_source_candidate_id") else "reassessment"))
        queue_status = str(item.get("queue_status") or ("Promising" if kind == "handoff" else "Needs More Data"))
        add(
            item.get("candidate_id"),
            "stage3",
            kind,
            queue_status,
            str(item.get("queue_reason") or "stage3_ranked_reassessment"),
            item.get("dispatch_source_candidate_id"),
        )

    return events


def cycle_payload(result: dict, timings: dict[str, float], store: DuckDBStore) -> dict:
    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    refresh = result.get("stage1_refresh") if isinstance(result.get("stage1_refresh"), dict) else {}
    continuous = result.get("continuous_stage1_intake") if isinstance(result.get("continuous_stage1_intake"), dict) else {}
    request_ids = [
        candidate_id
        for request in result.get("stage1_requests") or []
        if isinstance(request, dict)
        for candidate_id in request.get("candidate_ids") or []
    ]
    stage1_candidate_ids = (
        list(batch.get("candidate_ids") or [])
        + list(refresh.get("candidate_ids") or [])
        + request_ids
        + list(continuous.get("candidate_ids") or [])
    )
    stage1_summary = candidate_status_summary(store, stage1_candidate_ids)
    stage1_summary.update({
        "selected": (
            int(batch.get("selected_count") or 0)
            + int(refresh.get("refresh_count") or 0)
            + sum(
                int(request.get("selected_count") or 0)
                for request in result.get("stage1_requests") or []
                if isinstance(request, dict)
            )
            + int(continuous.get("selected_count") or 0)
        ),
        "evaluated": len(stage1_candidate_ids),
        "failed": (
            int(batch.get("failed_count") or 0)
            + int(refresh.get("failed_count") or 0)
            + sum(
                int(request.get("failed_count") or 0)
                for request in result.get("stage1_requests") or []
                if isinstance(request, dict)
            )
            + int(continuous.get("failed_count") or 0)
        ),
        "released": int(batch.get("replacement_slots") or 0),
        "timing_seconds": round(float(timings.get("stage1_seconds") or 0.0), 3),
    })
    if stage1_summary.get("needs_more_data"):
        # Stage 1 evaluates a batch concurrently, so this is the wall-clock
        # batch interval rather than a sum of per-candidate CPU time.
        stage1_summary["needs_more_data_seconds"] = stage1_summary["timing_seconds"]
    queue_events = queue_cycle_events(result, store)
    stage1_to_stage2 = sum(
        1
        for event in queue_events
        if event.get("stage") == "stage2"
        and event.get("kind") in {"handoff", "manual_handoff", "gate_bootstrap"}
    )
    stage2_to_stage3 = sum(
        1
        for event in queue_events
        if event.get("stage") == "stage3"
        and event.get("kind") in {"handoff", "manual_handoff", "gate_bootstrap"}
    )
    handoffs_by_stage = {
        "stage1": stage1_to_stage2,
        "stage2": stage2_to_stage3,
        "stage3": stage2_to_stage3,
    }
    stage2_summary = result_summary(result.get("stage2"))
    stage2_reassessment = result.get("stage2_reassessment")
    if isinstance(stage2_reassessment, dict):
        stage2_summary.update(
            {
                "priority_queue_count": int(stage2_reassessment.get("initial_queue_count") or 0),
                "priority_queue_remaining_count": int(stage2_reassessment.get("remaining_queue_count") or 0),
                "priority_queue_called": int(stage2_reassessment.get("evaluated_count") or 0),
                "priority_queue_unchanged": len(stage2_reassessment.get("unchanged_candidate_ids") or []),
            }
        )
    # A reassessment that discovers no new archive product is still a Stage 2
    # Needs More Data job. Count it in the operational throughput view even
    # though no new model evaluation was published.
    stage2_summary["needs_more_data"] = max(
        int(stage2_summary.get("needs_more_data") or 0),
        int(stage2_summary.get("queued_needs_more_data") or 0),
    )
    stage2_summary.update({
        "handoffs": handoffs_by_stage["stage2"],
        "timing_seconds": round(float(timings.get("stage2_seconds") or 0.0), 3),
        "needs_more_data_seconds": round(float(timings.get("stage2_needs_more_data_seconds") or 0.0), 3),
    })
    stage3_results = result.get("stage3") if isinstance(result.get("stage3"), list) else []
    stage3_summary = result_summary(stage3_results)
    stage3_summary.update({
        "handoffs": handoffs_by_stage["stage3"],
        "timing_seconds": round(float(timings.get("stage3_seconds") or 0.0), 3),
        "needs_more_data_seconds": round(float(timings.get("stage3_needs_more_data_seconds") or 0.0), 3),
        "dispatch_seconds": round(float(timings.get("stage3_dispatch_seconds") or 0.0), 3),
    })
    events = candidate_cycle_events(store, stage1_candidate_ids)
    events.extend(result_cycle_events("stage2", result.get("stage2")))
    events.extend(result_cycle_events("stage3", stage3_results))
    return {
        "handoff_metric_version": 2,
        "stage1": stage1_summary,
        "stage2": stage2_summary,
        "stage3": stage3_summary,
        "timings": {key: round(float(value), 3) for key, value in timings.items()},
        "queue": queue_snapshot(store),
        "ranked_samples": ranked_queue_samples(store),
        "queue_events": queue_events,
        "events": events,
        "stage2_intake": result.get("stage2_intake", {}),
        "stage2_reassessment": result.get("stage2_reassessment", {}),
        "stage2_waterfall_gate": result.get("stage2_waterfall_gate", {}),
        "stage3_reassessment": result.get("stage3_reassessment", {}),
        "stage3_intake": result.get("stage3_intake", {}),
        "continuous_stage1_intake": result.get("continuous_stage1_intake", {}),
    }


def auto_populate_manifest(data_dir: Path, manifest_path: Path) -> bool:
    enabled = os.getenv("AUTO_POPULATE_MANIFEST", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if manifest_path.exists() or not enabled:
        return manifest_path.exists()
    write_status(
        data_dir,
        "populating_manifest",
        {
            "pipeline_paused": True,
            "reason": "Building a metadata-only MAST target universe before model jobs begin.",
            "manifest": str(manifest_path),
        },
    )
    try:
        from populate_mast_universe import main as populate_main

        populate_main()
    except Exception as exc:  # keep the worker inspectable and retryable
        write_status(data_dir, "population_error", {"error": repr(exc)})
        return False
    return manifest_path.exists()


def main() -> None:
    global heartbeat_stop_event, heartbeat_thread
    signal.signal(signal.SIGTERM, stop_worker)
    signal.signal(signal.SIGINT, stop_worker)

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_seconds = float(os.getenv("WORKER_HEARTBEAT_SECONDS", "5"))
    paths = DataPaths(data_dir)
    paths.ensure()
    write_status(
        data_dir,
        "starting",
        {"pipeline_paused": True, "reason": "Opening the persistent worker database."},
    )
    heartbeat_stop_event = Event()
    heartbeat_thread = Thread(
        target=_heartbeat_loop,
        args=(data_dir, heartbeat_seconds, heartbeat_stop_event),
        name="worker-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()
    atexit.register(stop_heartbeat)
    store = DuckDBStore(paths.database)
    store.initialize()
    # These repairs are intentionally opt-in. They are durable, idempotent
    # maintenance operations, but running them inside the normal startup path
    # can hold DuckDB's write transaction long enough to make the API report
    # ``publication_busy`` and hide otherwise healthy persisted rankings. A
    # live worker must be able to resume from its last published state first.
    history_repair: dict[str, int | bool] = {"skipped": True}
    overall_ranking_rebuild: dict[str, int | bool] = {"skipped": True}
    if env_flag("RUN_STARTUP_MAINTENANCE", default=False):
        write_status(
            data_dir,
            "starting",
            {"pipeline_paused": True, "reason": "Repairing persisted pipeline history."},
        )
        history_repair = repair_legacy_history(store)
        write_status(
            data_dir,
            "starting",
            {"pipeline_paused": True, "reason": "Rebuilding the overall ranking index."},
        )
        overall_ranking_rebuild = rebuild_overall_ranking(store)
    else:
        write_status(
            data_dir,
            "starting",
            {
                "pipeline_paused": True,
                "reason": "Resuming from the last published state; startup maintenance is deferred.",
            },
        )

    mode = os.getenv("WORKER_MODE", "bootstrap").lower()
    if mode == "live":
        autostart = env_flag("RUN_LIVE_PIPELINE")
        if not autostart:
            while running:
                write_status(
                    data_dir,
                    "paused_by_config",
                    {
                        "pipeline_paused": True,
                        "reason": "RUN_LIVE_PIPELINE is false; no MAST downloads or model jobs will start.",
                    },
                )
                time.sleep(heartbeat_seconds)
            write_status(data_dir, "stopped", {"pipeline_paused": True})
            stop_heartbeat()
            return

        manifest_path = Path(
            os.getenv(
                "STAGE1_MANIFEST",
                str(data_dir / "manifests" / "stage1-real-target-universe.jsonl"),
            )
        )
        while running:
            operator_control = read_pipeline_control(data_dir)
            operator_paused = bool(operator_control.get("pipeline_paused"))
            if env_flag("PIPELINE_PAUSED", default=True) or operator_paused:
                if operator_paused:
                    state = "paused_by_operator"
                    reason = "Paused from ML Ops; no new MAST downloads or model jobs will start until resumed."
                else:
                    state = "paused_by_config"
                    reason = "PIPELINE_PAUSED is true; no MAST downloads or model jobs will start."
                write_status(
                    data_dir,
                    state,
                    {
                        "pipeline_paused": True,
                        "reason": reason,
                        "operator_control_updated_at": operator_control.get("updated_at"),
                    },
                )
                time.sleep(heartbeat_seconds)
                continue
            if not manifest_path.exists():
                auto_populate_manifest(data_dir, manifest_path)
                if not manifest_path.exists():
                    write_status(data_dir, "waiting_for_population", {"manifest": str(manifest_path)})
                    time.sleep(heartbeat_seconds)
                    continue
            cycle_started_at = datetime.now(timezone.utc)
            cycle_started_monotonic = time.perf_counter()
            cycle_id = f"worker-cycle-{cycle_started_at.strftime('%Y%m%dT%H%M%S%fZ')}"
            live_cycle_metrics = _new_live_cycle_metrics(cycle_id, cycle_started_at.isoformat())
            manual_migrations = claim_manual_migrations(data_dir)
            write_status(
                data_dir,
                "starting_cycle",
                {
                    "pipeline_paused": False,
                    "cycle_id": cycle_id,
                    "cycle_started_at": cycle_started_at.isoformat(),
                    "cycle_elapsed_seconds": 0.0,
                    "live_cycle_metrics": live_cycle_metrics,
                    "progress": {
                        "state": "starting_cycle",
                        "reason": "Beginning the next live pipeline cycle.",
                    },
                },
            )

            def cycle_status_callback(state: str, details: dict) -> None:
                _record_live_cycle_event(live_cycle_metrics, state, details)
                status_details = {
                    "pipeline_paused": False,
                    "cycle_id": cycle_id,
                    "cycle_started_at": cycle_started_at.isoformat(),
                    "cycle_elapsed_seconds": round(
                        max(0.0, time.perf_counter() - cycle_started_monotonic),
                        2,
                    ),
                    "live_cycle_metrics": live_cycle_metrics,
                    "progress": {
                        "state": state,
                        "updated_at": utc_now(),
                        **(details or {}),
                    },
                }
                if state == "stage1_batch_published":
                    status_details.update(
                        {
                            "last_stage1_publish_at": utc_now(),
                            "last_stage1_published_count": details.get(
                                "processed_target_count"
                            ),
                            "last_stage1_batch_id": details.get("batch_id"),
                        }
                    )
                write_status(
                    data_dir,
                    state,
                    status_details,
                )

            try:
                result = run_live_cycle(
                    data_dir=data_dir,
                    manifest_path=manifest_path,
                    batch_size=int(os.getenv("STAGE1_BATCH_SIZE", "32")),
                    max_workers=int(os.getenv("STAGE1_MAX_WORKERS", "8")),
                    top_n=int(os.getenv("TOP_N_DISPLAY", "500")),
                    stage1_threshold=int(os.getenv("STAGE1_PROMOTION_THRESHOLD", "100")),
                    stage2_reassessment_threshold=int(os.getenv("STAGE2_REASSESSMENT_THRESHOLD", "100")),
                    stage2_reassessment_concurrency=int(
                        os.getenv("STAGE2_REASSESSMENT_CONCURRENCY", "4")
                    ),
                    max_history_sectors=int(os.getenv("MAX_HISTORY_SECTORS", "8")),
                    stage1_pool_size=int(os.getenv("STAGE1_POOL_SIZE", "500")),
                    stage1_continuous_intake_per_cycle=int(
                        os.getenv("STAGE1_CONTINUOUS_INTAKE_PER_CYCLE", "2")
                    ),
                    stage3_initial_intake_size=int(os.getenv("STAGE3_INITIAL_INTAKE_SIZE", "100")),
                    stage3_reassessment_concurrency=int(
                        os.getenv("STAGE3_REASSESSMENT_CONCURRENCY", "4")
                    ),
                    manual_migrations=manual_migrations,
                    status_callback=cycle_status_callback,
                )
                cycle_finished_at = datetime.now(timezone.utc)
                cycle_metrics = cycle_payload(result, result.get("timings", {}), store)
                store.record_pipeline_cycle(
                    {
                        "cycle_id": f"worker-cycle-{cycle_started_at.strftime('%Y%m%dT%H%M%S%fZ')}",
                        "started_at": cycle_started_at,
                        "finished_at": cycle_finished_at,
                        "status": "complete",
                        "duration_seconds": time.perf_counter() - cycle_started_monotonic,
                        "payload": cycle_metrics,
                    }
                )
                for migration in result.get("manual_migrations", []):
                    request_id = str(migration.get("request_id") or "")
                    if not request_id:
                        continue
                    completed = int(migration.get("completed_count") or 0)
                    selected = int(migration.get("selected_count") or 0)
                    status = "completed" if migration.get("status") == "completed" else "failed"
                    update_manual_migration(
                        data_dir,
                        request_id,
                        status=status,
                        selected_count=selected,
                        completed_count=completed,
                        candidate_ids=migration.get("candidate_ids", []),
                        message=migration.get("message") or ("Migration completed." if status == "completed" else "Migration failed."),
                        error=migration.get("error"),
                    )
                poll_seconds = float(os.getenv("LIVE_POLL_SECONDS", "60"))
                catalog = result.get("catalog") if isinstance(result, dict) else None
                catalog_health = {
                    "status": catalog.get("status") if isinstance(catalog, dict) else "unknown",
                    "provider_status": catalog.get("catalog_status") if isinstance(catalog, dict) else None,
                    "matches": int(catalog.get("matches") or 0) if isinstance(catalog, dict) else 0,
                    "record_count": int(catalog.get("catalog_record_count") or 0) if isinstance(catalog, dict) else 0,
                    "fetched_at": catalog.get("catalog_fetched_at") if isinstance(catalog, dict) else None,
                    "complete_projection": bool(catalog.get("catalog_complete_projection")) if isinstance(catalog, dict) else False,
                    "stages": catalog.get("stages", []) if isinstance(catalog, dict) else [],
                }
                active_candidate_rows = active_candidates(store)
                write_status(
                    data_dir,
                    "idle",
                    {
                        "last_cycle": result,
                        "last_cycle_metrics": cycle_metrics,
                        "last_cycle_finished_at": cycle_finished_at.isoformat(),
                        "last_published_at": cycle_finished_at.isoformat(),
                        "telemetry": {
                            "processed_target_count": int(processed_target_count(store)),
                            "active_candidate_count": len(active_candidate_rows),
                            "active_star_count": len({
                                str(candidate.get("target_id"))
                                for candidate in active_candidate_rows
                                if candidate.get("target_id") is not None
                            }),
                            "data_version": int(store.latest_published_version() or 0),
                        },
                        "catalog_health": catalog_health,
                        "cycle_id": cycle_id,
                        "cycle_started_at": cycle_started_at.isoformat(),
                        "cycle_elapsed_seconds": round(
                            max(0.0, time.perf_counter() - cycle_started_monotonic),
                            2,
                        ),
                        "progress": {
                            "state": "idle",
                            "reason": "Cycle published successfully; waiting for the next poll.",
                        },
                        "history_repair": history_repair,
                        "overall_ranking_rebuild": overall_ranking_rebuild,
                        "next_update_at": (datetime.now(timezone.utc) + timedelta(seconds=poll_seconds)).isoformat(),
                    },
                )
            except Exception as exc:  # keep the worker alive and make the error inspectable
                for migration in manual_migrations:
                    update_manual_migration(
                        data_dir,
                        str(migration.get("request_id") or ""),
                        status="failed",
                        message="The worker cycle failed before the manual migration completed.",
                        error=repr(exc),
                    )
                cycle_finished_at = datetime.now(timezone.utc)
                try:
                    store.record_pipeline_cycle(
                        {
                            "cycle_id": f"worker-cycle-{cycle_started_at.strftime('%Y%m%dT%H%M%S%fZ')}",
                            "started_at": cycle_started_at,
                            "finished_at": cycle_finished_at,
                            "status": "error",
                            "duration_seconds": time.perf_counter() - cycle_started_monotonic,
                            "payload": {"error": repr(exc), "queue": queue_snapshot(store)},
                        }
                    )
                except Exception:
                    pass
                write_status(data_dir, "error", {"error": repr(exc)})
                time.sleep(float(os.getenv("WORKER_ERROR_RETRY_SECONDS", "30")))
                continue
            time.sleep(float(os.getenv("LIVE_POLL_SECONDS", "60")))
        write_status(data_dir, "stopped")
        stop_heartbeat()
        return

    while running:
        write_status(data_dir, "idle")
        time.sleep(heartbeat_seconds)

    write_status(data_dir, "stopped")
    stop_heartbeat()


if __name__ == "__main__":
    main()

