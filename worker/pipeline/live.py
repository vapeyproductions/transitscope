"""Orchestrate the archive-backed TransitScope live/replay pipeline."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Callable

from ingestion.exoplanet_archive import ExoplanetArchiveAdapter, ExoplanetArchiveConfig
from ingestion.mast_adapter import MastProbeConfig, MastProductAdapter, MastProbeError
from ingestion.mast_history import MastHistoryAdapter
from pipeline.lifecycle import (
    active_candidates,
    candidate_match_inputs,
    processed_target_count,
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


def _history_limit(max_history_sectors: int, previous_product_count: int) -> int:
    """Expand archive coverage only when the prior window has been consumed."""

    if max_history_sectors <= 0:
        return max_history_sectors
    initial = max(1, int(os.getenv("INITIAL_HISTORY_SECTORS", "2")))
    expansion = max(1, int(os.getenv("HISTORY_EXPANSION_SECTORS", "2")))
    if previous_product_count:
        return min(max_history_sectors, previous_product_count + expansion)
    return min(max_history_sectors, initial)


def _stage1_refresh_targets(
    store: DuckDBStore,
    *,
    data_dir: Path,
    max_targets: int,
) -> list[dict[str, Any]]:
    """Find a small ranked slice whose MAST product set has advanced."""

    if max_targets < 1:
        return []
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT c.candidate_id, c.target_id, c.rank_stage,
                   tm.provider_target_id, tm.sector, tm.ra_deg, tm.dec_deg,
                   tm.cadence, tm.source_product_family, tm.selection_bucket
            FROM candidates c
            LEFT JOIN target_manifest tm ON tm.target_id = c.target_id
            WHERE c.active = true AND c.current_stage = 'stage1'
            ORDER BY c.rank_stage NULLS LAST, c.last_evaluated_at ASC, c.candidate_id
            LIMIT ?
            """,
            [max_targets],
        ).fetchall()

    refreshes: list[dict[str, Any]] = []
    for row in rows:
        provider_target_id = str(row[3] or row[1]).removeprefix("TIC ").strip()
        if not provider_target_id or provider_target_id.startswith("queued-target-"):
            continue
        known_signature = _latest_evaluation_signature(store, str(row[0]), "stage1")
        known_products = set(known_signature[1] if known_signature else ())
        try:
            discovery_config = MastProbeConfig(
                data_dir=data_dir,
                sector=1,
                target_name=provider_target_id,
                max_observations=5,
                request_timeout_seconds=int(os.getenv("MAST_REQUEST_TIMEOUT_SECONDS", "60")),
            )
            discovery = MastProductAdapter(discovery_config)
            observations = discovery.query_all_observations(
                target_name=provider_target_id,
                max_records=int(os.getenv("STAGE1_REFRESH_MAX_OBSERVATIONS", "50")),
            )
            observations.sort(
                key=lambda item: int(item.get("sequence_number", 0) or 0),
                reverse=True,
            )
            for observation in observations:
                try:
                    sector = int(observation["sequence_number"])
                    adapter = MastProductAdapter(
                        MastProbeConfig(
                            data_dir=data_dir,
                            sector=sector,
                            target_name=provider_target_id,
                            max_observations=5,
                            request_timeout_seconds=discovery_config.request_timeout_seconds,
                        )
                    )
                    products = adapter.query_products(observation["obsid"])
                    product = adapter.select_light_curve_product(products)
                    product_id = str(product.get("dataURI", ""))
                    if not product_id or product_id in known_products:
                        continue
                    artifact = adapter.download_product(product)
                except (KeyError, MastProbeError, TypeError, ValueError, OSError):
                    continue
                refreshes.append(
                    {
                        "target_id": row[1],
                        "provider_target_id": provider_target_id,
                        "candidate_id": row[0],
                        "ra_deg": row[5],
                        "dec_deg": row[6],
                        "sector": sector,
                        "cadence": row[7],
                        "source_product_family": row[8],
                        "selection_bucket": row[9] or "stage1-refresh",
                        "source_observation_id": observation.get("obsid"),
                        "source_observation_name": observation.get("obs_id"),
                        "source_product_name": product.get("productFilename"),
                        "artifact_path": artifact["cache_path"],
                        "cache_path": artifact["cache_path"],
                        "product_id": artifact["data_uri"],
                        "product_checksum": artifact["sha256"],
                        "manifest_status": "queued",
                    }
                )
                break
        except (MastProbeError, OSError, ValueError):
            continue
    return refreshes


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
    previous_signature = _latest_evaluation_signature(store, candidate_id, "stage2")
    previous_product_count = len(previous_signature[1]) if previous_signature else 0
    paths, history = _history_for_candidate(
        target_id=stage1_input["target_id"],
        data_dir=data_dir,
        max_sectors=_history_limit(max_history_sectors, previous_product_count),
    )
    product_ids = tuple(str(path) for path in paths)
    if previous_signature == (
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
    previous_signature = _latest_evaluation_signature(store, candidate_id, "stage3")
    previous_product_count = len(previous_signature[1]) if previous_signature else 0
    paths, history = _history_for_candidate(
        target_id=stage2_input["target_id"],
        data_dir=data_dir,
        max_sectors=_history_limit(max_history_sectors, previous_product_count),
    )
    product_ids = tuple(str(path) for path in paths)
    if previous_signature == (
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
    stage_capacity: int | None = None,
    max_history_sectors: int,
    stage1_pool_size: int = 2000,
    stage2_capacity: int | None = None,
    stage3_capacity: int = 25,
    status_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded archive poll and publish only changed pipeline state."""

    from batching import BatchConfig, run_batch

    paths = DataPaths(data_dir)
    store = DuckDBStore(paths.database)
    store.initialize()
    promotion_capacity = stage2_capacity or stage_capacity or 100

    def fill_stage1() -> dict[str, Any]:
        stage1 = [
            candidate
            for candidate in active_candidates(store)
            if candidate["current_stage"] == "stage1"
            and candidate["processing_slot_occupied"]
        ]
        replacement_needed = max(0, stage1_pool_size - len(stage1))
        if not replacement_needed:
            return {
                "status": "idle",
                "message": "Stage 1 processing pool is full; waiting for a slot to open",
                "replacement_slots": 0,
                "publish_version": store.latest_published_version(),
            }
        if status_callback:
            status_callback("processing_stage1", {"replacement_slots": replacement_needed})
        return run_batch(
            data_dir=data_dir,
            manifest_path=manifest_path,
            config=BatchConfig(
                batch_size=min(batch_size, replacement_needed),
                max_workers=max_workers,
                top_n=top_n,
                stage_capacity=promotion_capacity,
                target_pool_size=stage1_pool_size,
            ),
        )

    refresh_targets = _stage1_refresh_targets(
        store,
        data_dir=data_dir,
        max_targets=int(os.getenv("STAGE1_REFRESH_TARGETS", "8")),
    )
    if refresh_targets:
        from batching import BatchConfig, run_stage1_refresh

        refresh_batch = run_stage1_refresh(
            data_dir=data_dir,
            manifest_path=manifest_path,
            targets=refresh_targets,
            config=BatchConfig(
                batch_size=len(refresh_targets),
                max_workers=max_workers,
                top_n=top_n,
                stage_capacity=promotion_capacity,
                target_pool_size=stage1_pool_size,
            ),
        )
    else:
        refresh_batch = {"status": "idle", "refresh_count": 0}

    batch = fill_stage1()

    # The first Stage 2 handoff is intentionally gated on a complete initial
    # Stage 1 exploration pool.  This prevents a handful of early Promising
    # results from draining the queue before the comparative baseline exists.
    initial_stage1_ready = processed_target_count(store) >= stage1_pool_size
    stage2_results: list[dict[str, Any]] = []
    stage1_candidates = (
        promotable_candidates(store, stage="stage1", capacity=promotion_capacity)
        if initial_stage1_ready
        else []
    )
    stage2_retry_candidates = (
        [
            candidate
            for candidate in active_candidates(store)
            if candidate["current_stage"] == "stage2"
            and candidate["status"] == "Needs More Data"
            and candidate["rank_stage"] is not None
            and int(candidate["rank_stage"]) <= promotion_capacity
        ]
        if initial_stage1_ready
        else []
    )
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

    # Promotion and an inconclusive result both release a Stage 1 slot. Fill
    # those openings in the same live cycle so the next poll starts with a
    # fresh exploration frontier rather than waiting for another heartbeat.
    replacement_batch = fill_stage1()

    stage3_results: list[dict[str, Any]] = []
    stage2_queue_ready = initial_stage1_ready and sum(
        1
        for candidate in active_candidates(store)
        if candidate["current_stage"] == "stage2"
    ) >= stage3_capacity
    stage2_candidates = (
        promotable_candidates(store, stage="stage2", capacity=stage3_capacity)
        if stage2_queue_ready
        else []
    )
    stage3_retry_candidates = (
        [
            candidate
            for candidate in active_candidates(store)
            if candidate["current_stage"] == "stage3"
            and candidate["status"] == "Needs More Data"
            and candidate["rank_stage"] is not None
            and int(candidate["rank_stage"]) <= stage3_capacity
        ]
        if stage2_queue_ready
        else []
    )
    stage3_prime_candidates = (
        [
            candidate
            for candidate in active_candidates(store)
            if candidate["current_stage"] == "stage3"
            and candidate["status"] == "Promising"
            and candidate["rank_stage"] is not None
            and int(candidate["rank_stage"]) <= stage3_capacity
        ]
        if stage2_queue_ready
        else []
    )
    # Existing Stage 3 records still need maintenance even before the initial
    # Stage 2 queue reaches its promotion threshold. This is intentionally
    # separate from new Stage 2 -> Stage 3 promotion: it lets an older
    # proof-of-life record pick up improved persistence/coverage behavior
    # without opening the gate for new candidates prematurely.
    stage3_existing_candidates = [
        candidate
        for candidate in active_candidates(store)
        if candidate["current_stage"] == "stage3"
    ]
    stage3_work: list[dict[str, Any]] = []
    seen_stage3: set[str] = set()
    for candidate in (
        stage2_candidates
        + stage3_retry_candidates
        + stage3_prime_candidates
        + stage3_existing_candidates
    ):
        if candidate["candidate_id"] not in seen_stage3:
            stage3_work.append(candidate)
            seen_stage3.add(candidate["candidate_id"])
    for candidate in stage3_work:
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
        "stage1_refresh": refresh_batch,
        "replacement_batch": replacement_batch,
        "stage2": stage2_results,
        "stage3": stage3_results,
        "catalog": catalog,
    }


__all__ = ["run_live_cycle"]
