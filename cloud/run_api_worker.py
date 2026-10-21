"""Run the read-only API and the gated worker in one persistent service.

This is the first cloud deployment shape for TransitScope. Railway volumes are
attached to a service, so the API and worker intentionally share one process
container and one mounted DATA_DIR while the project still uses DuckDB. The
worker remains paused unless RUN_LIVE_PIPELINE is explicitly enabled.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


CHILDREN: list[subprocess.Popen[bytes]] = []


def stop_children(_signum: int, _frame: object) -> None:
    for child in CHILDREN:
        if child.poll() is None:
            child.terminate()


def main() -> int:
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
    worker = subprocess.Popen(
        [sys.executable, "/app/worker/main.py"],
        cwd="/app/worker",
        env=environment,
    )
    CHILDREN.extend((api, worker))

    signal.signal(signal.SIGTERM, stop_children)
    signal.signal(signal.SIGINT, stop_children)

    try:
        while True:
            for child in CHILDREN:
                exit_code = child.poll()
                if exit_code is not None:
                    stop_children(signal.SIGTERM, None)
                    return exit_code if exit_code != 0 else 1
            time.sleep(1)
    finally:
        stop_children(signal.SIGTERM, None)
        for child in CHILDREN:
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

