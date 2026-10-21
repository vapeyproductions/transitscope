"""Orchestrate the archive-backed TransitScope live/replay pipeline."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Callable

from ingestion.exoplanet_archive import ExoplanetArchiveAdapter, ExoplanetArchiveConfig
from ingestion.mast_adapter import MastProbeConfig
from ingestion.mast_history import MastHistoryAdapter
from pipeline.lifecycle import (
    active_candidates,
    candidate_match_inputs,
    promotable_candidates,
    publish_known_matches,
)
from pipeline.stage1 import resolve_artifact_path
from pipeline.stage2 import (
    MODEL_VERSION as STAGE2_MODEL_VERSION,
    Stage2Config,
    Stage2Processor,
    load_stage1_input,
    publish_stage2_result,
)
from pipeline.stage3 import (
    MODEL_VERSION as STAGE3_MODEL_VERSION,
    Stage3Config,
    Stage3Processor,
    load_stage2_input,
    publish_stage3_result,
)
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value)


def _history_paths(payload: dict[str, Any], data_dir: Path) -> list[Path]:
    paths: list[Path] = []
    for record in payload.get("records", []):
        cache_path = record.get("artifact", {}).get("cache_path")
        if cache_path:
            paths.append(resolve_artifact_path(str(cache_path), data_dir=data_dir))
    return sorted({path.resolve() for path in paths if path.exists()})


def _latest_evaluation_signature(
    store: DuckDBStore,
    candidate_id: str,
    stage: str,
) -> tuple[str, tuple[str, ...]] | None:
    store.initialize()
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT model_version, input_product_ids
            FROM evaluations
            WHERE candidate_id = ? AND stage = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [candidate_id, stage],
        ).fetchone()
    if not row:
        return None
    try:
        return str(row[0]), tuple(sorted(str(value) for value in json.loads(row[1])))
    except (TypeError, ValueError, json.JSONDecodeError):
        return "", ()


def _history_for_candidate(
    *,
    target_id: str,
    data_dir: Path,
    max_sectors: int,
) -> tuple[list[Path], dict[str, Any]]:
    target_digits = _digits(target_id)
    config = MastProbeConfig(
        data_dir=data_dir,
        sector=1,
        target_name=target_digits or None,
        max_observations=5,
        request_timeout_seconds=int(os.getenv("MAST_REQUEST_TIMEOUT_SECONDS", "60")),
    )
    discovery = MastHistoryAdapter(config)
    sectors = discovery.discover_sectors()
    if max_sectors > 0 and len(sectors) > max_sectors:
        # Keep the oldest and newest observations so a bounded fetch still
        # maximizes chronological coverage for Stage 2/3.
        if max_sectors == 1:
            sectors = sectors[:1]
        else:
            chosen = {sectors[0], sectors[-1]}
            for sector in sectors:
                if len(chosen) >= max_sectors:
                    break
                chosen.add(sector)
            sectors = sorted(chosen)
    history = MastHistoryAdapter(config, sectors).fetch()
    return _history_paths(history, data_dir), history


def _evaluate_stage2(
    store: DuckDBStore,
    candidate: dict[str, Any],
    *,
    data_dir: Path,
    max_history_sectors: int,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    stage1_input = load_stage1_input(store, candidate_id)
    if stage1_input is None:
        return {"candidate_id": candidate_id, "status": "missing_stage1_input"}
    paths, history = _history_for_candidate(
        target_id=stage1_input["target_id"],
        data_dir=data_dir,
        max_sectors=max_history_sectors,
    )
    product_ids = tuple(str(path) for path in paths)
    if _latest_evaluation_signature(store, candidate_id, "stage2") == (
        STAGE2_MODEL_VERSION,
        product_ids,
    ):
        return {
            "candidate_id": candidate_id,
            "status": "unchanged",
            "history_products": len(paths),
            "discovered_sectors": history.get("sectors", []),
        }
    processor = Stage2Processor(
        Stage2Config(
            target_lookback_days=float(os.getenv("STAGE2_TARGET_LOOKBACK_DAYS", "730")),
            minimum_coverage_days=float(os.getenv("STAGE2_MINIMUM_COVERAGE_DAYS", "180")),
            minimum_events=int(os.getenv("STAGE2_MINIMUM_EVENTS", "2")),
        )
    )
    if paths:
        result = processor.evaluate(
            paths,
            candidate_id=candidate_id,
            target_id=stage1_input["target_id"],
            stage1_features=stage1_input["features"],
            normalized_dir=data_dir / "normalized",
        )
    else:
        result = processor.missing_history_result(
            candidate_id=candidate_id,
            target_id=stage1_input["target_id"],
            stage1_features=stage1_input["features"],
        )
    version = publish_stage2_result(
        store,
        result,
        stage1_score=stage1_input["stage1_score"],
        input_product_ids=list(product_ids),
    )
    return {
        "candidate_id": candidate_id,
        "status": result["status"],
        "publish_version": version,
        "history_products": len(paths),
        "discovered_sectors": history.get("sectors", []),
    }


def _evaluate_stage3(
    store: DuckDBStore,
    candidate: dict[str, Any],
    *,
    data_dir: Path,
    max_history_sectors: int,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    stage2_input = load_stage2_input(store, candidate_id)
    if stage2_input is None:
        return {"candidate_id": candidate_id, "status": "missing_stage2_input"}
    paths, history = _history_for_candidate(
        target_id=stage2_input["target_id"],
        data_dir=data_dir,
        max_sectors=max_history_sectors,
    )
    product_ids = tuple(str(path) for path in paths)
    if _latest_evaluation_signature(store, candidate_id, "stage3") == (
        STAGE3_MODEL_VERSION,
        product_ids,
    ):
        return {
            "candidate_id": candidate_id,
            "status": "unchanged",
            "history_products": len(paths),
            "discovered_sectors": history.get("sectors", []),
        }
    processor = Stage3Processor(
        Stage3Config(
            minimum_total_samples=int(os.getenv("STAGE3_MINIMUM_TOTAL_SAMPLES", "90")),
            minimum_samples_per_window=int(os.getenv("STAGE3_MINIMUM_SAMPLES_PER_WINDOW", "30")),
        )
    )
    if paths:
        result = processor.evaluate(
            paths,
            candidate_id=candidate_id,
            target_id=stage2_input["target_id"],
            stage2_features=stage2_input["features"],
            normalized_dir=data_dir / "normalized",
        )
    else:
        result = processor.missing_history_result(
            candidate_id=candidate_id,
            target_id=stage2_input["target_id"],
            stage2_features=stage2_input["features"],
        )
    version = publish_stage3_result(
        store,
        result,
        stage1_score=stage2_input["stage1_score"],
        stage2_score=stage2_input["stage2_score"],
        input_product_ids=list(product_ids),
    )
    return {
        "candidate_id": candidate_id,
        "status": result["status"],
        "publish_version": version,
        "history_products": len(paths),
        "discovered_sectors": history.get("sectors", []),
    }


def _match_known_candidates(store: DuckDBStore, data_dir: Path) -> dict[str, Any]:
    candidates = candidate_match_inputs(store)
    if not candidates:
        return {"status": "no_active_candidates", "matches": 0}
    config = ExoplanetArchiveConfig(
        data_dir=data_dir,
        tables=("pscomppars", "toi"),
        max_rows=int(os.getenv("CATALOG_MAX_ROWS", "5000")),
        request_timeout_seconds=int(os.getenv("CATALOG_REQUEST_TIMEOUT_SECONDS", "60")),
        cache_ttl_hours=float(os.getenv("CATALOG_CACHE_TTL_HOURS", "168")),
    )
    adapter = ExoplanetArchiveAdapter(config)
    payload = adapter.fetch(force=False)
    records = [adapter._record_from_cache(record) for record in payload.get("records", [])]
    matches: list[dict[str, Any]] = []
    for candidate in candidates:
        match = adapter.match_candidate(
            target_id=candidate["target_id"],
            ra_deg=candidate["ra_deg"],
            dec_deg=candidate["dec_deg"],
            period_days=candidate["period_days"],
            records=records,
        )
        if match:
            matches.append({"candidate_id": candidate["candidate_id"], **match})
    if not matches:
        return {"status": "no_matches", "matches": 0, "catalog_status": payload.get("status")}
    version = publish_known_matches(
        store,
        matches,
        catalog_cache_path=str(payload.get("cache_path") or adapter.cache_path),
        top_n=int(os.getenv("TOP_N_DISPLAY", "100")),
    )
    return {
        "status": "matched",
        "matches": len(matches),
        "publish_version": version,
        "catalog_status": payload.get("status"),
    }


def run_live_cycle(
    *,
    data_dir: Path,
    manifest_path: Path,
    batch_size: int,
    max_workers: int,
    top_n: int,
    stage_capacity: int,
    max_history_sectors: int,
    status_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded archive poll and publish only changed pipeline state."""

    from batching import BatchConfig, run_batch

    paths = DataPaths(data_dir)
    store = DuckDBStore(paths.database)
    store.initialize()
    if status_callback:
        status_callback("processing_stage1", {})
    batch = run_batch(
        data_dir=data_dir,
        manifest_path=manifest_path,
        config=BatchConfig(
            batch_size=batch_size,
            max_workers=max_workers,
            top_n=top_n,
            stage_capacity=stage_capacity,
        ),
    )

    stage2_results: list[dict[str, Any]] = []
    stage1_candidates = promotable_candidates(store, stage="stage1", capacity=stage_capacity)
    stage2_retry_candidates = [
        candidate
        for candidate in active_candidates(store)
        if candidate["current_stage"] == "stage2"
        and candidate["status"] == "Needs More Data"
        and candidate["rank_stage"] is not None
        and int(candidate["rank_stage"]) <= stage_capacity
    ]
    for candidate in stage1_candidates + stage2_retry_candidates:
        if status_callback:
            status_callback("processing_stage2", {"candidate_id": candidate["candidate_id"]})
        stage2_results.append(
            _evaluate_stage2(
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
            )
        )

    stage3_results: list[dict[str, Any]] = []
    stage2_candidates = promotable_candidates(store, stage="stage2", capacity=stage_capacity)
    stage3_retry_candidates = [
        candidate
        for candidate in active_candidates(store)
        if candidate["current_stage"] == "stage3"
        and candidate["status"] == "Needs More Data"
        and candidate["rank_stage"] is not None
        and int(candidate["rank_stage"]) <= stage_capacity
    ]
    for candidate in stage2_candidates + stage3_retry_candidates:
        if status_callback:
            status_callback("processing_stage3", {"candidate_id": candidate["candidate_id"]})
        stage3_results.append(
            _evaluate_stage3(
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
            )
        )

    if status_callback:
        status_callback("matching_catalog", {})
    catalog = _match_known_candidates(store, data_dir)
    return {
        "batch": batch,
        "stage2": stage2_results,
        "stage3": stage3_results,
        "catalog": catalog,
    }


__all__ = ["run_live_cycle"]
