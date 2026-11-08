import json
import os
import re
import secrets
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from .store import PublicationBusyError, ReadOnlyStore

DEFAULT_TARGET_UNIVERSE_SIZE = 100_000


app = FastAPI(
    title="TransitScope API",
    version="0.1.0",
    description="TransitScope observatory API with authenticated ML Ops control.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3000").split(",")
        if origin.strip()
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


_RAILWAY_STATUS_CACHE: tuple[float, dict] | None = None
_RAILWAY_STATUS_CACHE_TTL_SECONDS = 30.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def worker_status() -> dict:
    status_path = Path(os.getenv("DATA_DIR", "/app/data")) / "worker-status.json"
    if not status_path.exists():
        return {"state": "starting", "last_heartbeat": None}

    try:
        payload = json.loads(status_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"state": "unknown", "last_heartbeat": None}
    if not isinstance(payload, dict):
        return {"state": "unknown", "last_heartbeat": None}

    now = datetime.now(timezone.utc)

    def age_seconds(value: object) -> float | None:
        if not value:
            return None
        try:
            timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return round(max(0.0, (now - timestamp).total_seconds()), 2)

    heartbeat_age = age_seconds(payload.get("last_heartbeat"))
    progress_age = age_seconds(payload.get("last_progress_at"))
    stage1_publish_age = age_seconds(payload.get("last_stage1_publish_at"))
    payload["heartbeat_age_seconds"] = heartbeat_age
    payload["progress_age_seconds"] = progress_age
    payload["stage1_publish_age_seconds"] = stage1_publish_age
    stall_timeout = max(30.0, float(os.getenv("WORKER_STALL_TIMEOUT_SECONDS", "600")))
    state = str(payload.get("state") or "unknown")
    active_states = {
        "starting_cycle",
        "processing_stage1",
        "processing_stage2",
        "processing_stage3",
        "matching_catalog",
        "populating_manifest",
    }
    stage1_publish_timeout = max(
        300.0,
        float(os.getenv("STAGE1_PUBLISH_STALL_TIMEOUT_SECONDS", "1800")),
    )
    stage1_published_count = int(payload.get("last_stage1_published_count") or 0)
    target_universe = int(os.getenv("TARGET_UNIVERSE_SIZE", str(DEFAULT_TARGET_UNIVERSE_SIZE)))
    stage1_publish_stalled = (
        state in active_states
        and stage1_publish_age is not None
        and stage1_publish_age > stage1_publish_timeout
        and stage1_published_count < target_universe
    )
    if state in active_states and progress_age is not None and progress_age > stall_timeout:
        payload["health"] = "stalled"
        payload["health_message"] = (
            "The worker heartbeat is alive, but no pipeline progress has been recorded "
            f"for {round(progress_age / 60, 1)} minutes."
        )
    elif stage1_publish_stalled:
        payload["health"] = "stage1_publish_stalled"
        payload["health_message"] = (
            "The worker is active, but no new Stage 1 batch has been published "
            f"for {round(stage1_publish_age / 60, 1)} minutes."
        )
    elif heartbeat_age is not None and heartbeat_age > max(30.0, float(os.getenv("WORKER_HEARTBEAT_SECONDS", "15")) * 3):
        payload["health"] = "heartbeat_stale"
        payload["health_message"] = "The worker has not refreshed its heartbeat recently."
    elif state.startswith("paused") or payload.get("pipeline_paused"):
        payload["health"] = "paused"
        payload["health_message"] = "The worker is paused by configuration or operator control."
    else:
        payload["health"] = "healthy"
        payload["health_message"] = "The worker is refreshing its heartbeat and progress telemetry."
    return payload


def _env_flag(name: str, *, default: bool = False) -> bool:
    return os.getenv(name, "true" if default else "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _ops_token_matches(request: Request) -> bool:
    expected = os.getenv("OPS_CONTROL_TOKEN") or os.getenv("OPS_PASSWORD") or "monitor"
    provided = request.headers.get("x-transitscope-ops-token", "")
    return bool(provided) and secrets.compare_digest(provided, expected)


def _require_ops_token(request: Request) -> None:
    if not _ops_token_matches(request):
        raise HTTPException(
            status_code=401,
            detail={"code": "ops_auth_required", "message": "ML Ops access requires an authenticated session."},
        )


def _health_check(
    check_id: str,
    label: str,
    status: str,
    summary: str,
    checked_at: str,
    *,
    details: list[str] | None = None,
    source: str,
    latency_ms: float | None = None,
) -> dict:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "summary": summary,
        "details": details or [],
        "source": source,
        "latency_ms": latency_ms,
        "checked_at": checked_at,
    }


def _railway_platform_check() -> dict:
    """Read Railway's public status API without making it a hard dependency.

    A status-page timeout is reported as unknown, never healthy. The short
    process-local cache keeps a slow third-party status page from blocking the
    health tab on every 15-second poll.
    """

    global _RAILWAY_STATUS_CACHE
    now = time.monotonic()
    if _RAILWAY_STATUS_CACHE and now - _RAILWAY_STATUS_CACHE[0] < _RAILWAY_STATUS_CACHE_TTL_SECONDS:
        cached = dict(_RAILWAY_STATUS_CACHE[1])
        cached["cached"] = True
        return cached

    url = os.getenv("RAILWAY_STATUS_URL", "https://status.railway.com/api/v2/status.json")
    started = time.perf_counter()
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "TransitScope-health-probe/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=2.5) as response:
            payload = json.loads(response.read(100_000).decode("utf-8"))
        status_payload = payload.get("status", {}) if isinstance(payload, dict) else {}
        indicator = str(status_payload.get("indicator") or "unknown").lower()
        description = str(status_payload.get("description") or "No status description returned.")
        state = {
            "none": "healthy",
            "minor": "warning",
            "major": "critical",
            "critical": "critical",
        }.get(indicator, "unknown")
        result = {
            "status": state,
            "indicator": indicator,
            "summary": description,
            "source": url,
            "checked_at": utc_now(),
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "cached": False,
        }
    except (OSError, ValueError, urllib.error.URLError) as exc:
        result = {
            "status": "unknown",
            "indicator": "unknown",
            "summary": "Railway status page could not be reached from the API service.",
            "source": url,
            "checked_at": utc_now(),
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "cached": False,
            "error": str(exc)[:240],
        }

    _RAILWAY_STATUS_CACHE = (now, result)
    return result


def _worker_compact_status(worker: dict) -> dict:
    """Build a non-blocking status projection from worker-status.json.

    The API and worker may briefly contend on DuckDB during a publish. The
    worker sidecar is the only source that can be read without waiting on that
    database, so health/status callers use its monotonic publish counter while
    a live cycle is active.
    """

    telemetry = worker.get("telemetry") if isinstance(worker.get("telemetry"), dict) else {}
    processed_values: list[int] = []
    for value in (
        telemetry.get("processed_target_count"),
        worker.get("last_stage1_published_count"),
    ):
        try:
            processed_values.append(int(value))
        except (TypeError, ValueError):
            continue
    processed_count = max(processed_values, default=0)
    configured_count = int(os.getenv("TARGET_UNIVERSE_SIZE", str(DEFAULT_TARGET_UNIVERSE_SIZE)))
    return {
        "data_version": telemetry.get("data_version"),
        "sequence_number": telemetry.get("data_version"),
        "processed_target_count": processed_count,
        "total_target_count": configured_count,
        "active_candidate_count": int(telemetry.get("active_candidate_count") or 0),
        "active_star_count": int(telemetry.get("active_star_count") or 0),
        "last_ingest_at": worker.get("last_ingest_at") or telemetry.get("last_ingest_at"),
        "processed_history": [],
        "read_state": "worker_snapshot",
        "served_from_cache": True,
        "worker": worker,
    }


def _fast_monitoring_projection(status: dict, worker: dict) -> dict:
    """Project the latest durable worker snapshot without the full monitor query.

    The regular monitoring view intentionally computes model aggregates and
    long histories. The source-of-truth tab must remain responsive while that
    projection is locked or expensive, so it uses the worker's persisted
    last-cycle payload plus the compact status read instead.
    """

    last_metrics = worker.get("last_cycle_metrics") if isinstance(worker.get("last_cycle_metrics"), dict) else {}
    live_metrics = worker.get("live_cycle_metrics") if isinstance(worker.get("live_cycle_metrics"), dict) else {}
    saved_queue = last_metrics.get("queue") if isinstance(last_metrics.get("queue"), dict) else {}
    has_cycle_snapshot = bool(last_metrics) and isinstance(last_metrics.get("queue"), dict)
    has_live_snapshot = bool(live_metrics) and any(
        isinstance(live_metrics.get(stage), dict) for stage in ("stage1", "stage2", "stage3")
    )
    metrics_source = last_metrics if has_cycle_snapshot else live_metrics
    stages: dict[str, dict] = {}
    throughput: dict[str, dict] = {}
    current_queue: dict[str, dict] = {}
    worker_state = str(worker.get("state") or "")
    active_stage_match = re.search(r"stage[123]", worker_state, re.IGNORECASE)
    active_stage = active_stage_match.group(0).lower() if active_stage_match else None
    for stage in ("stage1", "stage2", "stage3"):
        has_stage_snapshot = has_cycle_snapshot and stage in saved_queue and isinstance(saved_queue.get(stage), dict)
        has_live_stage = has_live_snapshot and isinstance(live_metrics.get(stage), dict)
        has_observed_metrics = has_stage_snapshot or has_live_stage
        counts_raw = saved_queue.get(stage) if has_stage_snapshot else {}
        counts = {str(key): int(value or 0) for key, value in counts_raw.items() if isinstance(value, (int, float))}
        stage_metrics = metrics_source.get(stage) if isinstance(metrics_source.get(stage), dict) else {}
        total_candidates = sum(counts.values())
        last_activity_at = (
            worker.get("last_cycle_finished_at")
            or last_metrics.get("finished_at")
            or last_metrics.get("occurred_at")
        ) if has_stage_snapshot else (
            worker.get("last_progress_at") if active_stage == stage else live_metrics.get("updated_at")
        )
        if last_activity_at is None and stage == "stage1":
            last_activity_at = worker.get("last_stage1_publish_at")
        stages[stage] = {
            # An absent snapshot is not an empty pipeline. Keep it explicit so
            # the health panel cannot turn a publication lock into false zeros.
            "available": has_observed_metrics,
            "total_candidates": total_candidates if has_stage_snapshot else None,
            "priority_candidates": (
                int(
                    stage_metrics.get("priority_queue_count")
                    or (counts.get("Needs More Data", 0) if stage == "stage3" else stage_metrics.get("selected", 0))
                    or 0
                )
                if has_stage_snapshot
                else None
            ),
            "counts": counts,
            "last_evaluated_at": last_activity_at,
            "last_activity_at": last_activity_at,
        }
        if has_stage_snapshot:
            jobs = int(stage_metrics.get("selected") or stage_metrics.get("attempted") or 0)
            evaluated = int(stage_metrics.get("evaluated") or stage_metrics.get("attempted") or 0)
            handoffs = int(stage_metrics.get("handoffs") or 0)
            throughput[stage] = {
                key: int(stage_metrics.get(key) or 0)
                for key in ("attempted", "evaluated", "promising", "needs_more_data", "inconclusive", "known_match", "errors", "handoffs")
            }
            throughput[stage]["attempted"] = jobs
            throughput[stage]["evaluated"] = evaluated
            throughput[stage]["handoffs"] = handoffs
        elif has_live_stage:
            jobs = int(stage_metrics.get("jobs") or 0)
            evaluated = int(stage_metrics.get("assessed") or 0)
            handoffs = int(stage_metrics.get("handoffs") or 0)
            throughput[stage] = {
                "attempted": jobs,
                "evaluated": evaluated,
                "handoffs": handoffs,
            }
        else:
            jobs = evaluated = handoffs = 0
            throughput[stage] = {}
        if has_observed_metrics:
            throughput[stage]["per_hour"] = throughput[stage]["evaluated"]
            throughput[stage]["per_minute"] = round(throughput[stage]["evaluated"] / 60, 2)
        current_queue[stage] = {"jobs": jobs} if has_observed_metrics else {}

    finished_at = worker.get("last_heartbeat") or status.get("checked_at") or utc_now()
    latest_cycle = {
        "cycle_id": worker.get("cycle_id") or "last-completed-cycle",
        "finished_at": finished_at,
        "stage2_reassessment": metrics_source.get("stage2_reassessment", {}),
    }
    timings = last_metrics.get("timings") if isinstance(last_metrics.get("timings"), dict) else {}
    return {
        "data_version": status.get("data_version"),
        "read_state": status.get("read_state", "published"),
        "served_from_cache": bool(status.get("served_from_cache")),
        "last_ingest_at": status.get("last_ingest_at"),
        "universe": {
            "configured_count": int(status.get("total_target_count") or 0),
            "processed_count": int(status.get("processed_target_count") or 0),
            "remaining_count": max(0, int(status.get("total_target_count") or 0) - int(status.get("processed_target_count") or 0)),
        },
        "stages": stages,
        "cycle_activity": {
            "throughput": throughput,
            "queue": {"current": current_queue},
            "runs": [latest_cycle] if metrics_source else [],
        },
        "data_health": {"providers": []},
        "timings": timings,
        "control": _source_of_truth_control(worker),
    }


def _source_of_truth_control(worker: dict) -> dict:
    """Derive the effective control state without trusting missing fields.

    The full monitoring projection gets these values from deployment
    configuration. The bounded health endpoint may be served while DuckDB is
    locked, so it must use the worker's live state as the authority for what
    is actually happening. In particular, a running worker must never be
    rendered as ``autostart disabled`` merely because the compact projection
    has no control object.
    """

    operator_control = _read_pipeline_control()
    operator_paused = bool(operator_control.get("pipeline_paused"))
    worker_state = str(worker.get("state") or "unknown")
    worker_paused = bool(worker.get("pipeline_paused")) or worker_state.startswith("paused")
    active_states = {
        "starting_cycle",
        "processing_stage1",
        "processing_stage2",
        "processing_stage3",
        "matching_catalog",
        "populating_manifest",
    }
    worker_active = worker_state in active_states and not worker_paused
    configured_autostart = _env_flag("RUN_LIVE_PIPELINE")
    configured_gate = configured_autostart and not _env_flag("PIPELINE_PAUSED", default=True)
    # The worker can be in a live cycle even while the API process is serving
    # a lock-bound response. Treat that observed activity as authoritative.
    autostart_enabled = worker_active or configured_autostart
    deployment_gate = worker_active or configured_gate
    pipeline_paused = operator_paused or worker_paused
    if pipeline_paused:
        reason = str(worker.get("reason") or "The worker is paused by configuration or operator control.")
    elif worker_active:
        reason = f"The worker is actively processing {worker_state.replace('_', ' ')}."
    elif not deployment_gate:
        reason = "The deployment gate is closed; no new worker cycle will start."
    else:
        reason = f"Worker state: {worker_state}."
    return {
        "pipeline_paused": pipeline_paused,
        "autostart_enabled": autostart_enabled,
        "deployment_gate": deployment_gate,
        "operator_control": operator_paused,
        "updated_at": operator_control.get("updated_at"),
        "reason": reason,
    }


def _monitoring_system_health(worker: dict) -> dict:
    """Build the operator source-of-truth projection from live checks.

    This deliberately returns a useful response even if the published DuckDB
    read is temporarily busy. The worker status file and control gate remain
    independently visible, and the affected check is marked accordingly.
    """

    checked_at = utc_now()
    request_started = time.perf_counter()
    monitoring: dict = {}
    # Do not touch DuckDB here. This endpoint is deliberately bounded and
    # must remain responsive while the worker is opening, publishing, or
    # recovering the database. The full ops projection still owns the detailed
    # provider/catalog/history reads.
    compact_status = _worker_compact_status(worker)
    monitoring = _fast_monitoring_projection(compact_status, worker)

    control = monitoring.get("control") if isinstance(monitoring.get("control"), dict) else _source_of_truth_control(worker)
    worker_health = str(worker.get("health") or "unknown")
    worker_status = {
        "stalled": "critical",
        "heartbeat_stale": "critical",
        "stage1_publish_stalled": "critical",
        "paused": "paused",
        "healthy": "healthy",
    }.get(worker_health, "unknown")
    checks: list[dict] = [
        _health_check(
            "api-server",
            "API server",
            "healthy",
            "Health endpoint is responding.",
            checked_at,
            source="This request",
            latency_ms=round((time.perf_counter() - request_started) * 1000, 1),
        ),
        _health_check(
            "worker-heartbeat",
            "Worker heartbeat",
            worker_status,
            str(worker.get("health_message") or f"Worker state: {worker.get('state') or 'unknown'}"),
            checked_at,
            source="worker-status.json",
            details=[
                f"State: {worker.get('state') or 'unknown'}",
                f"Last heartbeat: {worker.get('last_heartbeat') or 'not recorded'}",
                f"Last progress: {worker.get('last_progress_at') or 'not recorded'}",
            ],
        ),
    ]

    control_status = "paused" if control.get("pipeline_paused") else "healthy"
    checks.append(
        _health_check(
            "pipeline-control",
            "Pipeline control gate",
            control_status,
            str(control.get("reason") or "The worker is armed."),
            checked_at,
            source="pipeline-control.json + deployment environment",
            details=[
                f"Autostart: {'enabled' if control.get('autostart_enabled') else 'disabled'}",
                f"Deployment gate: {'open' if control.get('deployment_gate') else 'closed'}",
                f"Operator pause: {'on' if control.get('operator_control') else 'off'}",
            ],
        )
    )

    read_state = str(monitoring.get("read_state") or "published")
    served_from_cache = bool(monitoring.get("served_from_cache"))
    publication_status = "warning" if read_state in {"publication_busy", "worker_progress_ahead"} or served_from_cache else "healthy"
    if read_state == "worker_progress_ahead":
        publication_summary = "The worker has published progress newer than the read replica; the monotonic worker count is being shown."
    elif publication_status == "warning":
        publication_summary = "Publication lock is active; awaiting the next durable snapshot."
    else:
        publication_summary = "The latest durable data projection was read successfully."
    checks.append(
        _health_check(
            "published-state",
            "Published data state",
            publication_status,
            publication_summary,
            checked_at,
            source="published DuckDB projection",
            details=[
                f"Data version: {monitoring.get('data_version') or 'not recorded'}",
                f"Read state: {read_state}",
                f"Processed targets: {monitoring.get('universe', {}).get('processed_count', 0):,}",
            ],
        )
    )
    checks[-1]["details"].append("This bounded check uses worker-status.json; full DuckDB reads remain in the detailed ops projection.")

    data_health = monitoring.get("data_health") if isinstance(monitoring.get("data_health"), dict) else {}
    providers = data_health.get("providers") if isinstance(data_health.get("providers"), list) else []
    mast = next((item for item in providers if "mast" in str(item.get("provider", "")).lower()), None)
    if mast is None:
        last_ingest_at = monitoring.get("last_ingest_at")
        if read_state in {"publication_busy", "worker_progress_ahead", "worker_snapshot"} or served_from_cache:
            provider_status = "warning"
            provider_summary = "MAST run history is not included in this bounded worker check."
            provider_details = [
                f"Last ingest timestamp from the compact status snapshot: {last_ingest_at or 'not recorded'}",
                "This is not evidence of an empty or failed MAST run; the detailed projection owns provider history.",
            ]
        elif last_ingest_at:
            provider_status = "unknown"
            provider_summary = "A MAST ingest timestamp exists, but provider run details are not available in this response."
            provider_details = [f"Last ingest timestamp: {last_ingest_at}"]
        else:
            provider_status = "unknown"
            provider_summary = "No MAST ingest run is recorded in the published data."
            provider_details = ["A missing run is different from a successful empty result."]
    elif mast.get("failed_runs", 0) and not mast.get("successful_runs", 0):
        provider_status = "critical"
        provider_summary = "All recorded MAST ingest runs failed."
        provider_details = [f"{mast.get('failed_runs', 0)} failed runs · {mast.get('products_failed', 0)} failed products"]
    elif mast.get("failed_runs", 0):
        provider_status = "warning"
        provider_summary = "MAST has recorded failures, but successful ingest runs also exist."
        provider_details = [f"{mast.get('successful_runs', 0)} successful · {mast.get('failed_runs', 0)} failed runs"]
    else:
        provider_status = "healthy"
        provider_summary = "MAST ingest runs are succeeding."
        provider_details = [f"{mast.get('successful_runs', 0)} successful runs · {mast.get('products_downloaded', 0)} products downloaded"]
    checks.append(
        _health_check(
            "mast-ingest",
            "MAST / TESS ingest",
            provider_status,
            provider_summary,
            checked_at,
            source="ingest_runs in published DuckDB projection",
            details=provider_details,
        )
    )

    catalog = worker.get("catalog_health") if isinstance(worker.get("catalog_health"), dict) else {}
    catalog_state = str(catalog.get("status") or "")
    if worker.get("state") == "matching_catalog":
        catalog_status = "warning"
        catalog_summary = "Catalog cross-reference is the active worker lane."
    elif catalog_state in {"error", "failed"}:
        catalog_status = "critical"
        catalog_summary = "The latest catalog cross-reference failed."
    elif catalog_state:
        catalog_status = "healthy"
        catalog_summary = f"Catalog projection status: {catalog_state}."
    else:
        catalog_status = "unknown"
        catalog_summary = "No completed catalog cross-reference is recorded."
    checks.append(
        _health_check(
            "catalog-cross-reference",
            "Known-object cross-reference",
            catalog_status,
            catalog_summary,
            checked_at,
            source="worker catalog telemetry",
            details=[
                f"Matches: {catalog.get('matches', 0)}",
                f"Catalog rows: {catalog.get('record_count', 0)}",
                f"Last fetched: {catalog.get('fetched_at') or 'not recorded'}",
            ],
        )
    )

    stages = monitoring.get("stages") if isinstance(monitoring.get("stages"), dict) else {}
    activity = monitoring.get("cycle_activity") if isinstance(monitoring.get("cycle_activity"), dict) else {}
    throughput = activity.get("throughput") if isinstance(activity.get("throughput"), dict) else {}
    queue = activity.get("queue") if isinstance(activity.get("queue"), dict) else {}
    current_queue = queue.get("current") if isinstance(queue.get("current"), dict) else {}
    latest_cycle = (activity.get("runs") or [])[-1] if activity.get("runs") else {}
    if not isinstance(latest_cycle, dict):
        latest_cycle = {}
    stage2_reassessment = latest_cycle.get("stage2_reassessment") if isinstance(latest_cycle.get("stage2_reassessment"), dict) else {}
    stage_summaries = []
    for stage in ("stage1", "stage2", "stage3"):
        stage_data = stages.get(stage) if isinstance(stages.get(stage), dict) else {}
        stage_available = bool(stage_data.get("available"))
        counts = stage_data.get("counts") if isinstance(stage_data.get("counts"), dict) else {}
        stage_flow = throughput.get(stage) if isinstance(throughput.get(stage), dict) else {}
        stage_queue = current_queue.get(stage) if isinstance(current_queue.get(stage), dict) else {}
        jobs = sum(int(value or 0) for value in stage_queue.values() if isinstance(value, (int, float)))
        needs_more_data = int(counts.get("Needs More Data", 0) or 0)
        stage_activity_at = (
            stage_data.get("last_evaluated_at")
            or (worker.get("last_progress_at") if str(worker.get("state") or "").replace("processing_", "") == stage else None)
            or (worker.get("last_stage1_publish_at") if stage == "stage1" else None)
        )
        if stage_available:
            stage_status = "paused" if control.get("pipeline_paused") else "healthy"
            retained = stage_data.get("total_candidates")
            summary = (
                f"{int(retained):,} retained · {jobs:,} jobs in the latest queue."
                if retained is not None
                else f"Live cycle telemetry · {jobs:,} jobs in the latest queue; retained counts come from the published status projection."
            )
            details = [
                f"Promising: {int(counts.get('Promising', 0) or 0):,}",
                f"Needs More Data: {needs_more_data:,}",
                f"Inconclusive: {int(counts.get('Inconclusive', 0) or 0):,}",
                f"Last stage activity: {stage_activity_at or 'no completed assessment in this snapshot'}",
            ]
        else:
            worker_health = str(worker.get("health") or "unknown")
            current_stage = str(worker.get("state") or "").replace("processing_", "")
            stage_status = "paused" if control.get("pipeline_paused") else "unknown"
            if worker_health in {"stalled", "stage1_publish_stalled"} and current_stage == stage:
                stage_status = "critical"
            summary = "No durable stage snapshot is available in this response."
            if read_state in {"publication_busy", "worker_snapshot"}:
                summary = "Stage counts are not included in this bounded worker snapshot."
            details = [
                f"Worker state: {worker.get('state') or 'not recorded'}",
                "The monitor will not display unavailable data as zero.",
            ]
            jobs = None
        if stage == "stage2" and int(stage2_reassessment.get("remaining_queue_count", 0) or 0) > 0:
            stage_status = "warning" if not control.get("pipeline_paused") else "paused"
            summary += f" {int(stage2_reassessment.get('remaining_queue_count', 0)):,} reassessments waiting."
        if stage == "stage3" and needs_more_data:
            details.append("Stage 3 works Needs More Data in ranked order.")
        stage_summaries.append(
            {
                "stage": stage,
                "status": stage_status,
                "summary": summary,
                "details": details,
                "available": stage_available,
                "retained_candidates": (
                    int(stage_data.get("total_candidates"))
                    if stage_available and stage_data.get("total_candidates") is not None
                    else None
                ),
                "priority_candidates": (
                    int(stage_data.get("priority_candidates"))
                    if stage_available and stage_data.get("priority_candidates") is not None
                    else None
                ),
                "jobs_in_latest_queue": jobs,
                "evaluated_in_window": int(stage_flow.get("evaluated", 0) or 0) if stage_available else None,
                "handoffs_in_window": int(stage_flow.get("handoffs", 0) or 0) if stage_available else None,
                "last_evaluated_at": stage_data.get("last_evaluated_at") if stage_available else None,
                "last_activity_at": stage_activity_at,
            }
        )

    checks.extend(
        _health_check(
            f"{item['stage']}-pipeline",
            f"{item['stage'].replace('stage', 'Stage ')} pipeline",
            item["status"],
            item["summary"],
            checked_at,
            details=item["details"],
            source="published stage counts + cycle queue telemetry",
        )
        for item in stage_summaries
    )

    railway = _railway_platform_check()
    checks.append(
        _health_check(
            "railway-platform",
            "Railway platform",
            railway.get("status", "unknown"),
            railway.get("summary", "Railway status is unknown."),
            checked_at,
            source=railway.get("source", "Railway status API"),
            details=[f"Status-page indicator: {railway.get('indicator', 'unknown')}"],
            latency_ms=railway.get("latency_ms"),
        )
    )

    # A known pause should be more informative than an unrecorded optional
    # check, while a provider/platform warning or hard failure must dominate
    # the headline state.
    severity = {"healthy": 0, "unknown": 1, "paused": 2, "warning": 3, "critical": 4}
    overall_status = max(checks, key=lambda item: severity.get(item["status"], 2))["status"]
    alerts = [check for check in checks if check["status"] != "healthy"]
    next_check = datetime.fromisoformat(checked_at) + timedelta(seconds=15)
    return {
        "checked_at": checked_at,
        "next_check_at": next_check.isoformat(),
        "refresh_interval_seconds": 15,
        "stale_after_seconds": 45,
        "overall_status": overall_status,
        "alerts": alerts,
        "checks": checks,
        "pipeline": {
            "control": control,
            "worker": worker,
            "data_version": monitoring.get("data_version"),
            "universe": monitoring.get("universe", {}),
            "stages": stage_summaries,
            "latest_cycle_id": latest_cycle.get("cycle_id"),
        },
        "read_state": monitoring.get("read_state", "published"),
        "served_from_cache": bool(monitoring.get("served_from_cache")),
        "source_of_truth": [
            "worker-status.json for heartbeat, progress, and active lane",
            "pipeline-control.json and deployment gates for pause state",
            "published DuckDB projection for counts, queues, stages, and provider runs",
            "Railway public status API for platform incidents",
        ],
    }


def _pipeline_control_path() -> Path:
    return Path(os.getenv("DATA_DIR", "/app/data")) / "pipeline-control.json"


def _read_pipeline_control() -> dict:
    path = _pipeline_control_path()
    if not path.exists():
        return {"pipeline_paused": False}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"pipeline_paused": False}
    return payload if isinstance(payload, dict) else {"pipeline_paused": False}


def _write_pipeline_control(paused: bool) -> dict:
    path = _pipeline_control_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pipeline_paused": paused,
        "updated_at": utc_now(),
        "updated_by": "ops-dashboard",
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)
    return payload


def _manual_migration_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "/app/data")) / "manual-migrations"


def _write_json_atomically(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def _manual_migration_requests(limit: int = 20) -> list[dict]:
    directory = _manual_migration_dir()
    if not directory.exists():
        return []
    requests: list[dict] = []
    for path in sorted(directory.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            requests.append(payload)
        if len(requests) >= limit:
            break
    return requests


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "service": "api", "version": app.version}


@app.get("/api/status")
def status() -> dict:
    current_worker = worker_status()
    # Keep this public heartbeat endpoint strictly non-blocking. Detailed
    # DuckDB projections are available to the authenticated monitoring routes;
    # a database publication lock must never make the public status panel hang.
    response = ReadOnlyStore().fast_status(current_worker)
    response["checked_at"] = utc_now()
    return response


@app.get("/api/monitoring/overview")
def monitoring_overview(request: Request) -> dict:
    """Return the authenticated ML operations/control-plane projection.

    This endpoint never starts a job. It reports the worker's explicit pause
    gate, durable pipeline counts, model evidence, provider health, and the
    sampling signals that will guide future replacement-target selection.
    """

    _require_ops_token(request)
    response = ReadOnlyStore().monitoring(worker_status())
    response["manual_migrations"] = _manual_migration_requests()
    return response


@app.get("/api/monitoring/system-health")
def monitoring_system_health(request: Request) -> dict:
    """Return the independently computed operator source-of-truth checks."""

    _require_ops_token(request)
    return _monitoring_system_health(worker_status())


@app.post("/api/monitoring/control")
def monitoring_control(request: Request, payload: dict) -> dict:
    """Set the durable operator pause gate read by the live worker."""

    _require_ops_token(request)
    action = payload.get("action") if isinstance(payload, dict) else None
    if action not in {"pause", "resume"}:
        raise HTTPException(status_code=400, detail={"code": "invalid_action", "message": "Action must be pause or resume."})

    if action == "resume":
        if not _env_flag("RUN_LIVE_PIPELINE"):
            raise HTTPException(status_code=409, detail={"code": "deployment_gate_closed", "message": "RUN_LIVE_PIPELINE is false; enable the deployment gate before resuming the worker."})
        if _env_flag("PIPELINE_PAUSED", default=True):
            raise HTTPException(status_code=409, detail={"code": "deployment_gate_closed", "message": "PIPELINE_PAUSED is true; clear the deployment pause before resuming the worker."})

    control = _write_pipeline_control(action == "pause")
    # Do not run the full monitoring projection before acknowledging a control
    # action. A DuckDB publication lock can make that projection slow even
    # though the durable pause gate was written successfully.
    return {
        "action": action,
        "control": {
            "pipeline_paused": bool(control.get("pipeline_paused")),
            "operator_control": bool(control.get("pipeline_paused")),
            "updated_at": control.get("updated_at"),
            "reason": (
                "Paused from ML Ops; no new jobs will start until resumed."
                if action == "pause"
                else "Resume requested; the worker will start on its next heartbeat."
            ),
        },
        "updated_at": control["updated_at"],
    }


@app.post("/api/monitoring/migrations")
def monitoring_migration(request: Request, payload: dict) -> dict:
    """Queue an authenticated, bounded manual stage handoff.

    The API never writes DuckDB directly. The live worker claims this durable
    request and uses the same evaluation/publish paths as the normal
    waterfall, which keeps replay history and ranking updates consistent.
    """

    _require_ops_token(request)
    from_stage = payload.get("from_stage") if isinstance(payload, dict) else None
    to_stage = payload.get("to_stage") if isinstance(payload, dict) else None
    requested_count = payload.get("count", 1) if isinstance(payload, dict) else 1
    if (from_stage, to_stage) not in {
        ("universe", "stage1"),
        ("stage1", "stage2"),
        ("stage2", "stage3"),
    }:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_migration", "message": "Only Candidate Universe → Stage 1, Stage 1 → Stage 2, and Stage 2 → Stage 3 migrations are supported."},
        )
    try:
        requested_count = int(requested_count)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"code": "invalid_count", "message": "Migration count must be an integer."}) from exc
    if not 1 <= requested_count <= 100:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_count", "message": "Manual migrations are limited to 1–100 candidates per request."},
        )

    requested_at = utc_now()
    request_id = f"manual-migration-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
    migration = {
        "request_id": request_id,
        "from_stage": from_stage,
        "to_stage": to_stage,
        "requested_count": requested_count,
        "status": "pending",
        "created_at": requested_at,
        "updated_at": requested_at,
        "requested_by": "ops-dashboard",
        "selected_count": 0,
        "completed_count": 0,
        "candidate_ids": [],
        "message": "Queued for the next worker cycle.",
    }
    _write_json_atomically(_manual_migration_dir() / f"{request_id}.json", migration)
    return migration


def not_found(code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"code": code, "message": message, "retryable": False},
    )


@app.get("/api/rankings")
def rankings(
    view: str = Query("overall"),
    limit: int = Query(100, ge=1, le=100),
    page: int = Query(1, ge=1, le=5),
    stage_filter: str | None = Query(None),
    status_filter: str | None = Query(None),
    sort: str = Query("rank"),
) -> dict:
    try:
        return ReadOnlyStore().rankings(
            view,
            limit,
            page,
            stage_filter=stage_filter,
            status_filter=status_filter,
            sort=sort,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_view", "message": str(exc), "retryable": False},
        ) from exc


@app.get("/api/identified")
def identified(limit: int = Query(100, ge=1, le=500)) -> dict:
    # The store already coordinates reads with the worker's publication lock
    # and falls back to the last persisted response if a publish is in flight.
    # Do not short-circuit merely because the worker is active: that made a
    # never-before-loaded Identified tab appear empty even when durable match
    # records were available in DuckDB.
    return ReadOnlyStore().identified(limit)


@app.get("/api/sky")
def sky(limit: int = Query(12000, ge=1, le=20000)) -> dict:
    """Return the explored-target sky projection for the public observatory map."""

    return ReadOnlyStore().sky(limit)


@app.get("/api/replay/{view}/meta")
def replay_meta(view: str) -> dict:
    try:
        return ReadOnlyStore().replay_meta(view)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_view", "message": str(exc), "retryable": False},
        ) from exc


@app.get("/api/replay/{view}/snapshot")
def replay_snapshot(
    view: str,
    sequence: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    page: int = Query(1, ge=1, le=5),
) -> dict:
    try:
        return ReadOnlyStore().replay_snapshot(view, sequence, limit, page)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_view", "message": str(exc), "retryable": False},
        ) from exc


@app.get("/api/replay/{view}/events")
def replay_events(
    view: str,
    start_sequence: int = Query(0, ge=0),
    end_sequence: int = Query(0, ge=0),
) -> dict:
    try:
        return ReadOnlyStore().replay_events(view, start_sequence, end_sequence)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_view", "message": str(exc), "retryable": False},
        ) from exc


@app.get("/api/candidates/{candidate_id}")
def candidate(candidate_id: str) -> dict:
    store = ReadOnlyStore()
    try:
        result = store.candidate(candidate_id)
    except PublicationBusyError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "publication_busy",
                "message": "The worker is publishing the latest candidate snapshot; retrying will not rerun a model.",
                "retryable": True,
            },
        ) from exc
    if result is None:
        raise not_found("candidate_not_found", f"Candidate {candidate_id} was not found.")
    return {"data_version": store._version(), "candidate": result}


@app.get("/api/candidates/{candidate_id}/light-curve")
def light_curve(candidate_id: str) -> dict:
    store = ReadOnlyStore()
    try:
        result = store.light_curve(candidate_id)
    except PublicationBusyError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "publication_busy",
                "message": "The worker is publishing the latest light curve snapshot; retrying will not rerun a model.",
                "retryable": True,
            },
        ) from exc
    if result is None:
        raise not_found("light_curve_unavailable", f"No light curve is available for {candidate_id}.")
    return result


@app.get("/api/candidates/{candidate_id}/history")
def history(candidate_id: str) -> dict:
    try:
        result = ReadOnlyStore().history(candidate_id)
    except PublicationBusyError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "publication_busy",
                "message": "The worker is publishing the latest candidate history; retrying will not rerun a model.",
                "retryable": True,
            },
        ) from exc
    if result is None:
        raise not_found("candidate_not_found", f"Candidate {candidate_id} was not found.")
    return result


@app.post("/api/refresh")
def refresh(known_version: int | None = Query(None, ge=0)) -> dict:
    store = ReadOnlyStore()
    current = store._version()
    return {
        "data_version": current,
        "known_version": known_version,
        "has_new_data": current is not None and current != known_version,
        "worker": worker_status(),
        "message": "Refresh reads the newest published state; it does not rerun a model.",
    }

