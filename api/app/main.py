import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .store import ReadOnlyStore


app = FastAPI(
    title="TransitScope API",
    version="0.1.0",
    description="Read-only API foundation for the TransitScope observatory.",
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


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "service": "api", "version": app.version}


@app.get("/api/status")
def status() -> dict:
    response = ReadOnlyStore().status(worker_status())
    response["checked_at"] = utc_now()
    return response


@app.get("/api/monitoring/overview")
def monitoring_overview() -> dict:
    """Return the read-only ML operations/control-plane projection.

    This endpoint never starts a job. It reports the worker's explicit pause
    gate, durable pipeline counts, model evidence, provider health, and the
    sampling signals that will guide future replacement-target selection.
    """

    return ReadOnlyStore().monitoring(worker_status())


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

