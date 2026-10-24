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
    priority_threshold: int = 100,
) -> list[dict[str, Any]]:
    """Find a small ranked slice whose MAST product set has advanced.

    The threshold keeps refresh work focused on the priority window. In
    particular, a Stage 1 ``Needs More Data`` candidate inside the window is
    eligible for another evaluation; it is not silently left behind because
    only Promising candidates are promotable.
    """

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
              AND c.rank_stage IS NOT NULL AND c.rank_stage <= ?
            ORDER BY c.rank_stage NULLS LAST, c.last_evaluated_at ASC, c.candidate_id
            LIMIT ?
            """,
            [priority_threshold, max_targets],
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


def _load_catalog_context(data_dir: Path) -> dict[str, Any]:
    """Load one catalog snapshot for all stage-local checks in a cycle."""

    config = ExoplanetArchiveConfig(
        data_dir=data_dir,
        tables=("pscomppars", "toi"),
        max_rows=int(os.getenv("CATALOG_MAX_ROWS", "10000")),
        request_timeout_seconds=int(os.getenv("CATALOG_REQUEST_TIMEOUT_SECONDS", "60")),
        cache_ttl_hours=float(os.getenv("CATALOG_CACHE_TTL_HOURS", "168")),
    )
    adapter = ExoplanetArchiveAdapter(config)
    payload = adapter.fetch(force=False)
    return {
        "adapter": adapter,
        "payload": payload,
        "records": [adapter._record_from_cache(record) for record in payload.get("records", [])],
    }


def _match_known_candidates(
    store: DuckDBStore,
    data_dir: Path,
    *,
    stage: str | None = None,
    catalog_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Cross-reference only the candidates currently waiting in one stage.

    A stage-local check is intentionally performed before promotion. A match in
    Stage 1 is therefore shown as a Stage 1 removal in replay; unresolved
    candidates are the only ones allowed to consume the next stage's capacity.
    """

    candidates = candidate_match_inputs(store)
    if stage is not None:
        candidates = [candidate for candidate in candidates if candidate["current_stage"] == stage]
    if not candidates:
        return {"status": "no_active_candidates", "matches": 0, "stage": stage}
    context = catalog_context or _load_catalog_context(data_dir)
    adapter = context["adapter"]
    payload = context["payload"]
    records = context["records"]
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
        return {
            "status": "no_matches",
            "matches": 0,
            "stage": stage,
            "catalog_status": payload.get("status"),
        }
    version = publish_known_matches(
        store,
        matches,
        catalog_cache_path=str(payload.get("cache_path") or adapter.cache_path),
        top_n=int(os.getenv("TOP_N_DISPLAY", "100")),
    )
    return {
        "status": "matched",
        "matches": len(matches),
        "stage": stage,
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
    stage1_threshold: int = 100,
    stage2_threshold: int = 25,
    stage3_threshold: int = 6,
    # Legacy keyword compatibility. These names used to describe stage
    # capacity, but now map only to the corresponding priority threshold.
    stage_capacity: int | None = None,
    max_history_sectors: int,
    stage1_pool_size: int = 500,
    stage2_capacity: int | None = None,
    stage3_capacity: int | None = None,
    status_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded archive poll and publish only changed pipeline state.

    Stage thresholds are ranking windows. They decide which candidates are
    eligible for prioritization or reassessment; they never cap the number of
    candidates retained in a stage.
    """

    from batching import BatchConfig, run_batch, run_stage1_refresh

    paths = DataPaths(data_dir)
    store = DuckDBStore(paths.database)
    store.initialize()
    if stage_capacity is not None:
        stage1_threshold = stage_capacity
    if stage2_capacity is not None:
        stage2_threshold = stage2_capacity
    if stage3_capacity is not None:
        stage3_threshold = stage3_capacity
    if min(stage1_threshold, stage2_threshold, stage3_threshold) < 1:
        raise ValueError("stage priority thresholds must be positive")

    def occupied_stage1_target_ids() -> set[str]:
        """Return source stars with active Stage 1 work.

        A source star can produce several signal candidates. Treating those
        rows as separate pool capacity is incorrect, and treating the display
        cap as operational capacity is worse: it silently stops the universe
        scan once the first page of the ranking is full. This helper is only
        used to detect source stars that actually leave the active Stage 1
        queue and therefore need replacement sampling.
        """

        return {
            str(candidate["target_id"])
            for candidate in active_candidates(store)
            if candidate["current_stage"] == "stage1"
            and candidate["processing_slot_occupied"]
        }

    def fill_stage1(replacement_slots: int | None = None) -> dict[str, Any]:
        """Seed the initial baseline or replace released Stage 1 work.

        ``stage1_pool_size`` is used only to establish the initial comparative
        baseline. It is not a cap on the number of stars that may be explored
        and it is not the ranking display limit. After the baseline exists,
        ``replacement_slots`` is driven exclusively by source stars that left
        active Stage 1 work.
        """

        if replacement_slots is None:
            replacement_needed = max(0, stage1_pool_size - processed_target_count(store))
            replacement_reason = "initial_baseline"
        else:
            replacement_needed = max(0, replacement_slots)
            replacement_reason = "released_stage1_work"
        if not replacement_needed:
            return {
                "status": "idle",
                "message": (
                    "Initial Stage 1 baseline is complete"
                    if replacement_slots is None
                    else "No Stage 1 source stars were released"
                ),
                "replacement_slots": 0,
                "replacement_reason": replacement_reason,
                "processed_target_count": processed_target_count(store),
                "publish_version": store.latest_published_version(),
            }
        if status_callback:
            status_callback(
                "processing_stage1",
                {
                    "replacement_slots": replacement_needed,
                    "replacement_reason": replacement_reason,
                },
            )
        return run_batch(
            data_dir=data_dir,
            manifest_path=manifest_path,
            config=BatchConfig(
                # A released source-star slot should be replenished in the
                # same cycle. The worker pool still bounds parallelism via
                # max_workers; batch_size here controls how many replacements
                # are requested, not how many candidates are shown.
                batch_size=replacement_needed,
                max_workers=max_workers,
                top_n=top_n,
                stage_capacity=stage1_threshold,
                target_pool_size=stage1_pool_size,
            ),
        )

    stage1_targets_before_refresh = occupied_stage1_target_ids()
    refresh_targets = _stage1_refresh_targets(
        store,
        data_dir=data_dir,
        max_targets=int(os.getenv("STAGE1_REFRESH_TARGETS", "8")),
        priority_threshold=stage1_threshold,
    )
    if refresh_targets:
        refresh_batch = run_stage1_refresh(
            data_dir=data_dir,
            manifest_path=manifest_path,
            targets=refresh_targets,
            config=BatchConfig(
                batch_size=len(refresh_targets),
                max_workers=max_workers,
                top_n=top_n,
                stage_capacity=stage1_threshold,
                target_pool_size=stage1_pool_size,
            ),
        )
    else:
        refresh_batch = {"status": "idle", "refresh_count": 0}

    if processed_target_count(store) < stage1_pool_size:
        batch = fill_stage1()
    else:
        batch = fill_stage1(
            len(stage1_targets_before_refresh - occupied_stage1_target_ids())
        )

    # Keep one immutable catalog snapshot for the entire cycle. Each gate
    # below sees only the candidates in that stage, so a known match is
    # recorded at the earliest stage that had enough evidence to identify it.
    # The snapshot is loaded lazily so an empty stage does not make an
    # unnecessary external catalog request.
    catalog_context: dict[str, Any] | None = None
    catalog_checks: list[dict[str, Any]] = []

    def check_catalog(stage: str) -> dict[str, Any]:
        nonlocal catalog_context
        if status_callback:
            status_callback("matching_catalog", {"stage": stage})
        if catalog_context is None and any(
            candidate["current_stage"] == stage for candidate in active_candidates(store)
        ):
            catalog_context = _load_catalog_context(data_dir)
        result = _match_known_candidates(
            store,
            data_dir,
            stage=stage,
            catalog_context=catalog_context,
        )
        catalog_checks.append(result)
        return result

    # Stage 1 known matches must be removed before the first Stage 2 queue is
    # selected. Replenish only source stars that actually left active Stage 1
    # work; the number shown in the UI is irrelevant to this decision.
    stage1_targets_before_catalog = occupied_stage1_target_ids()
    check_catalog("stage1")
    stage1_catalog_replacement = fill_stage1(
        len(stage1_targets_before_catalog - occupied_stage1_target_ids())
        if processed_target_count(store) >= stage1_pool_size
        else None
    )
    if stage1_catalog_replacement.get("status") != "idle":
        check_catalog("stage1")

    # The first Stage 2 handoff is intentionally gated on a complete initial
    # Stage 1 exploration pool. This prevents a handful of early Promising
    # results from draining the queue before the comparative baseline exists.
    # Once the baseline exists, the Stage 1 top-X value is only a priority
    # threshold; it is not a cap on the retained ranking.
    initial_stage1_ready = processed_target_count(store) >= stage1_pool_size
    stage1_targets_before_stage2 = occupied_stage1_target_ids()
    stage2_results: list[dict[str, Any]] = []
    stage1_candidates = (
        promotable_candidates(store, stage="stage1", threshold=stage1_threshold)
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
            and int(candidate["rank_stage"]) <= stage2_threshold
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

    # A Stage 2 evaluation can reveal a catalog match before Stage 3 work is
    # selected. Remove those matches now so they never enter the Stage 3
    # priority queue or appear as a Stage 3 known match in replay.
    check_catalog("stage2")

    # Promotion and an inconclusive result release Stage 1 source-star work.
    # Fill those openings in the same live cycle. This is deliberately based
    # on the before/after active target set, never on the presentational top-N
    # display cap.
    replacement_batch = fill_stage1(
        len(stage1_targets_before_stage2 - occupied_stage1_target_ids())
        if initial_stage1_ready
        else None
    )
    if replacement_batch.get("status") != "idle":
        check_catalog("stage1")

    # Keep exploring the unprocessed manifest after the initial baseline is
    # complete. Replacement sampling protects the active Stage 1 queue, but
    # it must not become the only way the 126,000-target universe advances:
    # otherwise throughput is limited by however many candidates happen to
    # leave Stage 1 in one cycle. The configured batch size is the total fresh
    # target budget for this cycle, so replacements use part of that budget
    # and the exploration batch consumes the remainder.
    exploration_batch: dict[str, Any] = {
        "status": "idle",
        "message": (
            "Waiting for the initial Stage 1 baseline"
            if not initial_stage1_ready
            else "No fresh exploration budget available"
        ),
        "selected_count": 0,
    }
    if initial_stage1_ready:
        replacement_count = int(replacement_batch.get("selected_count") or 0)
        exploration_count = max(0, int(batch_size) - replacement_count)
        if exploration_count:
            if status_callback:
                status_callback(
                    "processing_stage1",
                    {
                        "replacement_slots": 0,
                        "replacement_reason": "continuous_exploration",
                        "requested_count": exploration_count,
                    },
                )
            exploration_batch = run_batch(
                data_dir=data_dir,
                manifest_path=manifest_path,
                config=BatchConfig(
                    batch_size=exploration_count,
                    max_workers=max_workers,
                    top_n=top_n,
                    stage_capacity=stage1_threshold,
                    target_pool_size=stage1_pool_size,
                ),
            )
            if exploration_batch.get("status") != "idle":
                check_catalog("stage1")

    stage3_results: list[dict[str, Any]] = []
    # Stage 3 does not wait for a fictitious queue capacity to fill. When it
    # calls for another candidate, it takes the highest-ranked Promising
    # Stage 2 candidate that is available. The Stage 2 top-X value only
    # defines which Promising candidates are eligible to be called.
    stage2_queue_ready = initial_stage1_ready
    stage2_candidates = (
        promotable_candidates(store, stage="stage2", threshold=stage2_threshold)[:1]
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
            and int(candidate["rank_stage"]) <= stage3_threshold
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
            and int(candidate["rank_stage"]) <= stage3_threshold
        ]
        if stage2_queue_ready
        else []
    )
    # Stage 3 candidates outside the reassessment threshold remain in the
    # ranking but are not rerun. This keeps the threshold meaningful and
    # prevents every historical Stage 3 row from consuming a new evaluation
    # on every live cycle.
    stage3_work: list[dict[str, Any]] = []
    seen_stage3: set[str] = set()
    for candidate in (
        stage2_candidates
        + stage3_retry_candidates
        + stage3_prime_candidates
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

    # Stage 3 is the final assessment stage, so a catalog match here is a
    # legitimate Stage 3 removal and is retained in the identified/replay
    # history at that stage.
    check_catalog("stage3")
    matched_count = sum(int(check.get("matches") or 0) for check in catalog_checks)
    catalog = {
        "status": "matched" if matched_count else "no_matches",
        "matches": matched_count,
        "catalog_status": next(
            (
                check.get("catalog_status")
                for check in reversed(catalog_checks)
                if check.get("catalog_status") is not None
            ),
            None,
        ),
        "stages": catalog_checks,
    }
    return {
        "batch": batch,
        "stage1_refresh": refresh_batch,
        "stage1_catalog_replacement": stage1_catalog_replacement,
        "replacement_batch": replacement_batch,
        "exploration_batch": exploration_batch,
        "stage2": stage2_results,
        "stage3": stage3_results,
        "catalog": catalog,
    }


__all__ = ["run_live_cycle"]
