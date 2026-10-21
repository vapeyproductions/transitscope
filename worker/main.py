import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


running = True


def stop_worker(_signum: int, _frame: object) -> None:
    global running
    running = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_status(data_dir: Path, state: str) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    status_path = data_dir / "worker-status.json"
    payload = {
        "service": "worker",
        "state": state,
        "mode": os.getenv("WORKER_MODE", "bootstrap"),
        "last_heartbeat": utc_now(),
    }
    status_path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> None:
    signal.signal(signal.SIGTERM, stop_worker)
    signal.signal(signal.SIGINT, stop_worker)

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_seconds = float(os.getenv("WORKER_HEARTBEAT_SECONDS", "5"))
    paths = DataPaths(data_dir)
    paths.ensure()
    DuckDBStore(paths.database).initialize()

    while running:
        write_status(data_dir, "idle")
        time.sleep(heartbeat_seconds)

    write_status(data_dir, "stopped")


if __name__ == "__main__":
    main()

