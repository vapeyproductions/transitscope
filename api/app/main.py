import json
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from .store import ReadOnlyStore


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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def worker_status() -> dict:
    status_path = Path(os.getenv("DATA_DIR", "/app/data")) / "worker-status.json"
    if not status_path.exists():
        return {"state": "starting", "last_heartbeat": None}

    try:
        return json.loads(status_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"state": "unknown", "last_heartbeat": None}


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


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "service": "api", "version": app.version}


@app.get("/api/status")
def status() -> dict:
    response = ReadOnlyStore().status(worker_status())
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
    return ReadOnlyStore().monitoring(worker_status())


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
    return {"action": action, "control": ReadOnlyStore().monitoring(worker_status())["control"], "updated_at": control["updated_at"]}


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
    return ReadOnlyStore().identified(limit)


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
    result = ReadOnlyStore().candidate(candidate_id)
    if result is None:
        raise not_found("candidate_not_found", f"Candidate {candidate_id} was not found.")
    return {"data_version": ReadOnlyStore()._version(), "candidate": result}


@app.get("/api/candidates/{candidate_id}/light-curve")
def light_curve(candidate_id: str) -> dict:
    store = ReadOnlyStore()
    result = store.light_curve(candidate_id)
    if result is None:
        raise not_found("light_curve_unavailable", f"No light curve is available for {candidate_id}.")
    return result


@app.get("/api/candidates/{candidate_id}/history")
def history(candidate_id: str) -> dict:
    result = ReadOnlyStore().history(candidate_id)
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

