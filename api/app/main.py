import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI


app = FastAPI(
    title="TransitScope API",
    version="0.1.0",
    description="Read-only API foundation for the TransitScope observatory.",
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
    return {
        "service": "api",
        "environment": os.getenv("APP_ENV", "unknown"),
        "data_version": None,
        "worker": worker_status(),
        "checked_at": utc_now(),
    }

