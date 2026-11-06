"""Run the API and a self-healing live worker in one persistent service.

Railway volumes are attached to one service, so the API and worker share the
same mounted DATA_DIR while the project still uses DuckDB. The supervisor
keeps the API available while restarting a worker that exits or stops making
pipeline progress.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


API_PROCESS: subprocess.Popen[bytes] | None = None
WORKER_PROCESS: subprocess.Popen[bytes] | None = None
STOP_REQUESTED = False


def stop_children(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    for child in (API_PROCESS, WORKER_PROCESS):
        if child is not None and child.poll() is None:
            child.terminate()


def _env_flag(environment: dict[str, str], name: str, default: bool = False) -> bool:
    return environment.get(name, "true" if default else "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _parse_timestamp(value: object) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.timestamp()
    except (TypeError, ValueError):
        return None


def _read_worker_status(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _worker_is_stalled(
    status_path: Path,
    environment: dict[str, str],
    *,
    started_monotonic: float,
) -> tuple[bool, dict]:
    """Detect a live worker that has stopped making progress.

    A fresh heartbeat alone is not sufficient: a blocked worker can still
    heartbeat forever. The supervisor therefore checks ``last_progress_at``
    while allowing paused, idle, and startup states to wait normally.
    """

    if not _env_flag(environment, "RUN_LIVE_PIPELINE"):
        return False, {}
    if _env_flag(environment, "PIPELINE_PAUSED", default=True):
        return False, {}
    if time.monotonic() - started_monotonic < float(
        environment.get("WORKER_STARTUP_GRACE_SECONDS", "180")
    ):
        return False, {}

    status = _read_worker_status(status_path)
    state = str(status.get("state") or "")
    active_states = {
        "starting_cycle",
        "processing_stage1",
        "processing_stage2",
        "processing_stage3",
        "matching_catalog",
        "populating_manifest",
    }
    if state not in active_states:
        return False, status
    last_progress = _parse_timestamp(status.get("last_progress_at"))
    if last_progress is None:
        return False, status
    age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - last_progress)
    timeout_seconds = max(
        30.0,
        float(environment.get("WORKER_STALL_TIMEOUT_SECONDS", "600")),
    )
    if age_seconds <= timeout_seconds:
        # A stale Stage 1 publish is an operational alert, not proof that the
        # current MAST/history job is frozen. Restarting here used to kill
        # long-running but still-progressing work, then repeat the same cycle
        # from the beginning. The API health projection reports the publish
        # lag separately; only the general no-progress watchdog may restart
        # the worker.
        return False, status
    return True, {
        **status,
        "progress_age_seconds": round(age_seconds, 2),
        "stall_timeout_seconds": timeout_seconds,
    }


def _terminate_process(child: subprocess.Popen[bytes] | None) -> None:
    if child is None or child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def _start_worker(environment: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "/app/worker/main.py"],
        cwd="/app/worker",
        env=environment,
    )


def main() -> int:
    global API_PROCESS, WORKER_PROCESS
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment.setdefault("PYTHONPATH", "/app/api:/app/worker")
    port = environment.get("PORT", "8000")

    api = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            port,
        ],
        cwd="/app/api",
        env=environment,
    )
    worker = _start_worker(environment)
    API_PROCESS = api
    WORKER_PROCESS = worker

    signal.signal(signal.SIGTERM, stop_children)
    signal.signal(signal.SIGINT, stop_children)

    status_path = data_dir / "worker-status.json"
    restart_delay = max(
        1.0,
        float(environment.get("WORKER_RESTART_BACKOFF_SECONDS", "10")),
    )
    max_restart_delay = max(
        restart_delay,
        float(environment.get("WORKER_MAX_RESTART_BACKOFF_SECONDS", "120")),
    )
    worker_started_monotonic = time.monotonic()
    try:
        while True:
            api_exit_code = api.poll()
            if api_exit_code is not None:
                stop_children(signal.SIGTERM, None)
                return api_exit_code if api_exit_code != 0 else 1

            worker_exit_code = worker.poll()
            if worker_exit_code is not None:
                if STOP_REQUESTED:
                    return worker_exit_code if worker_exit_code != 0 else 0
                print(
                    f"worker exited with code {worker_exit_code}; restarting in {restart_delay:.0f}s",
                    flush=True,
                )
                time.sleep(restart_delay)
                if STOP_REQUESTED:
                    return 0
                worker = _start_worker(environment)
                WORKER_PROCESS = worker
                worker_started_monotonic = time.monotonic()
                restart_delay = min(max_restart_delay, restart_delay * 2)
                continue

            stalled, status = _worker_is_stalled(
                status_path,
                environment,
                started_monotonic=worker_started_monotonic,
            )
            if stalled:
                print(
                    "worker progress is stale; restarting worker "
                    f"state={status.get('state')} "
                    f"reason={status.get('stall_reason', 'pipeline_progress')} "
                    f"age={status.get('progress_age_seconds')}s",
                    flush=True,
                )
                _terminate_process(worker)
                if STOP_REQUESTED:
                    return 0
                time.sleep(restart_delay)
                worker = _start_worker(environment)
                WORKER_PROCESS = worker
                worker_started_monotonic = time.monotonic()
                restart_delay = min(max_restart_delay, restart_delay * 2)
                continue

            # A healthy worker gets the short backoff again. This keeps a
            # transient crash from causing a permanently slow restart loop.
            restart_delay = max(
                1.0,
                float(environment.get("WORKER_RESTART_BACKOFF_SECONDS", "10")),
            )
            time.sleep(1)
    finally:
        stop_children(signal.SIGTERM, None)
        _terminate_process(api)
        _terminate_process(worker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
