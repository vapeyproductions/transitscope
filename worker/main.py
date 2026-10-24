import json
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipeline.live import run_live_cycle
from pipeline.lifecycle import repair_legacy_history
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


running = True


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


def queue_snapshot(store: DuckDBStore) -> dict[str, dict[str, int]]:
    """Capture the active stage/status queues at a cycle boundary."""

    snapshot = {stage: {} for stage in ("stage1", "stage2", "stage3")}
    with store.connect() as connection:
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


def result_summary(results: list[dict] | None) -> dict[str, object]:
    items = results or []
    counts: dict[str, int] = {}
    for result in items:
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return {
        "attempted": len(items),
        "promising": counts.get("Promising", 0),
        "needs_more_data": counts.get("Needs More Data", 0),
        "inconclusive": counts.get("Inconclusive", 0),
        "known_match": counts.get("Known Match", 0),
        "errors": counts.get("error", 0),
        "unchanged": counts.get("unchanged", 0),
        "statuses": counts,
    }


def candidate_status_summary(store: DuckDBStore, candidate_ids: list[object]) -> dict[str, object]:
    """Summarize the persisted statuses produced by a Stage 1 publish."""

    normalized_ids = [str(candidate_id) for candidate_id in candidate_ids if candidate_id]
    if not normalized_ids:
        return result_summary([])
    placeholders = ", ".join("?" for _ in normalized_ids)
    with store.connect() as connection:
        rows = connection.execute(
            f"SELECT status FROM candidates WHERE candidate_id IN ({placeholders})",
            normalized_ids,
        ).fetchall()
    return result_summary([{"status": row[0]} for row in rows])


def cycle_payload(result: dict, timings: dict[str, float], store: DuckDBStore) -> dict:
    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    refresh = result.get("stage1_refresh") if isinstance(result.get("stage1_refresh"), dict) else {}
    stage1_candidate_ids = list(batch.get("candidate_ids") or []) + list(refresh.get("candidate_ids") or [])
    stage1_summary = candidate_status_summary(store, stage1_candidate_ids)
    stage1_summary.update({
        "selected": int(batch.get("selected_count") or 0) + int(refresh.get("refresh_count") or 0),
        "evaluated": len(stage1_candidate_ids),
        "failed": int(batch.get("failed_count") or 0) + int(refresh.get("failed_count") or 0),
        "released": int(batch.get("replacement_slots") or 0),
        "timing_seconds": round(float(timings.get("stage1_seconds") or 0.0), 3),
    })
    if stage1_summary.get("needs_more_data"):
        # Stage 1 evaluates a batch concurrently, so this is the wall-clock
        # batch interval rather than a sum of per-candidate CPU time.
        stage1_summary["needs_more_data_seconds"] = stage1_summary["timing_seconds"]
    stage2_summary = result_summary(result.get("stage2"))
    stage2_summary.update({
        "timing_seconds": round(float(timings.get("stage2_seconds") or 0.0), 3),
        "needs_more_data_seconds": round(float(timings.get("stage2_needs_more_data_seconds") or 0.0), 3),
    })
    stage3_results = result.get("stage3") if isinstance(result.get("stage3"), list) else []
    stage3_summary = result_summary(stage3_results)
    stage3_summary.update({
        "handoffs": sum(1 for item in stage3_results if isinstance(item, dict) and item.get("dispatch_source_candidate_id")),
        "timing_seconds": round(float(timings.get("stage3_seconds") or 0.0), 3),
        "needs_more_data_seconds": round(float(timings.get("stage3_needs_more_data_seconds") or 0.0), 3),
        "dispatch_seconds": round(float(timings.get("stage3_dispatch_seconds") or 0.0), 3),
    })
    return {
        "stage1": stage1_summary,
        "stage2": stage2_summary,
        "stage3": stage3_summary,
        "timings": {key: round(float(value), 3) for key, value in timings.items()},
        "queue": queue_snapshot(store),
        "stage3_reassessment": result.get("stage3_reassessment", {}),
        "stage3_intake": result.get("stage3_intake", {}),
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
    signal.signal(signal.SIGTERM, stop_worker)
    signal.signal(signal.SIGINT, stop_worker)

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_seconds = float(os.getenv("WORKER_HEARTBEAT_SECONDS", "5"))
    paths = DataPaths(data_dir)
    paths.ensure()
    store = DuckDBStore(paths.database)
    store.initialize()
    history_repair = repair_legacy_history(store)

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
            try:
                result = run_live_cycle(
                    data_dir=data_dir,
                    manifest_path=manifest_path,
                    batch_size=int(os.getenv("STAGE1_BATCH_SIZE", "32")),
                    max_workers=int(os.getenv("STAGE1_MAX_WORKERS", "8")),
                    top_n=int(os.getenv("TOP_N_DISPLAY", "500")),
                    stage1_threshold=int(os.getenv("STAGE1_PROMOTION_THRESHOLD", "100")),
                    stage2_reassessment_threshold=int(os.getenv("STAGE2_REASSESSMENT_THRESHOLD", "100")),
                    max_history_sectors=int(os.getenv("MAX_HISTORY_SECTORS", "8")),
                    stage1_pool_size=int(os.getenv("STAGE1_POOL_SIZE", "500")),
                    stage3_initial_intake_size=int(os.getenv("STAGE3_INITIAL_INTAKE_SIZE", "100")),
                    stage3_reassessment_concurrency=int(
                        os.getenv("STAGE3_REASSESSMENT_CONCURRENCY", "4")
                    ),
                    status_callback=lambda state, details: write_status(data_dir, state, details),
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
                poll_seconds = float(os.getenv("LIVE_POLL_SECONDS", "300"))
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
                write_status(
                    data_dir,
                    "idle",
                    {
                        "last_cycle": result,
                        "catalog_health": catalog_health,
                        "history_repair": history_repair,
                        "next_update_at": (datetime.now(timezone.utc) + timedelta(seconds=poll_seconds)).isoformat(),
                    },
                )
            except Exception as exc:  # keep the worker alive and make the error inspectable
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
            time.sleep(float(os.getenv("LIVE_POLL_SECONDS", "300")))
        write_status(data_dir, "stopped")
        return

    while running:
        write_status(data_dir, "idle")
        time.sleep(heartbeat_seconds)

    write_status(data_dir, "stopped")


if __name__ == "__main__":
    main()

