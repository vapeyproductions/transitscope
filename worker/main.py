import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline.live import run_live_cycle
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


running = True


def stop_worker(_signum: int, _frame: object) -> None:
    global running
    running = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_status(data_dir: Path, state: str, details: dict | None = None) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    status_path = data_dir / "worker-status.json"
    payload = {
        "service": "worker",
        "state": state,
        "mode": os.getenv("WORKER_MODE", "bootstrap"),
        "last_heartbeat": utc_now(),
    }
    if details:
        payload.update(details)
    status_path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> None:
    signal.signal(signal.SIGTERM, stop_worker)
    signal.signal(signal.SIGINT, stop_worker)

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_seconds = float(os.getenv("WORKER_HEARTBEAT_SECONDS", "5"))
    paths = DataPaths(data_dir)
    paths.ensure()
    DuckDBStore(paths.database).initialize()

    mode = os.getenv("WORKER_MODE", "bootstrap").lower()
    if mode == "live":
        autostart = os.getenv("RUN_LIVE_PIPELINE", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
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
            return

        manifest_path = Path(
            os.getenv(
                "STAGE1_MANIFEST",
                str(data_dir / "manifests" / "stage1-real-target-universe.jsonl"),
            )
        )
        while running:
            if not manifest_path.exists():
                write_status(data_dir, "waiting_for_population", {"manifest": str(manifest_path)})
                time.sleep(heartbeat_seconds)
                continue
            try:
                result = run_live_cycle(
                    data_dir=data_dir,
                    manifest_path=manifest_path,
                    batch_size=int(os.getenv("STAGE1_BATCH_SIZE", "4")),
                    max_workers=int(os.getenv("STAGE1_MAX_WORKERS", "1")),
                    top_n=int(os.getenv("TOP_N_DISPLAY", "500")),
                    stage_capacity=int(os.getenv("STAGE2_CAPACITY", "100")),
                    max_history_sectors=int(os.getenv("MAX_HISTORY_SECTORS", "8")),
                    stage1_pool_size=int(os.getenv("STAGE1_POOL_SIZE", "2000")),
                    stage2_capacity=int(os.getenv("STAGE2_CAPACITY", "100")),
                    stage3_capacity=int(os.getenv("STAGE3_CAPACITY", "25")),
                    status_callback=lambda state, details: write_status(data_dir, state, details),
                )
                write_status(data_dir, "idle", {"last_cycle": result})
            except Exception as exc:  # keep the worker alive and make the error inspectable
                write_status(data_dir, "error", {"error": repr(exc)})
            time.sleep(float(os.getenv("LIVE_POLL_SECONDS", str(heartbeat_seconds))))
        write_status(data_dir, "stopped")
        return

    while running:
        write_status(data_dir, "idle")
        time.sleep(heartbeat_seconds)

    write_status(data_dir, "stopped")


if __name__ == "__main__":
    main()

