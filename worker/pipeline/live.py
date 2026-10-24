"""Orchestrate the archive-backed TransitScope live/replay pipeline."""

from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
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
    resolve_priority_aliases,
    stage3_replacement_debt,
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
    # A Needs More Data reassessment should materially expand coverage. Four
    # additional sectors usually moves the candidate from a short probe to a
    # useful multi-window comparison, while max_history_sectors remains the
    # hard upper bound for one candidate.
    expansion = max(1, int(os.getenv("HISTORY_EXPANSION_SECTORS", "4")))
    if previous_product_count:
        return min(max_history_sectors, previous_product_count + expansion)
    return min(max_history_sectors, initial)


def _stage3_needs_data_queue(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return unresolved Stage 3 candidates in current ranking order."""

    return sorted(
        (
            candidate
            for candidate in candidates
            if candidate["current_stage"] == "stage3"
            and candidate["active"]
            and candidate["status"] == "Needs More Data"
            and candidate["rank_stage"] is not None
        ),
        key=lambda candidate: (int(candidate["rank_stage"]), str(candidate["candidate_id"])),
    )


def _stage2_needs_data_queue(
    candidates: list[dict[str, Any]],
    *,
    threshold: int,
) -> list[dict[str, Any]]:
    """Return Stage 2 Needs More Data rows inside the rolling priority window.

    The window is a reassessment trigger, not a capacity or promotion gate.
    Candidates remain in the Stage 2 ranking while this helper orders the
    work queue by the current Stage 2 rank.
    """

    return sorted(
        (
            candidate
            for candidate in candidates
            if candidate["current_stage"] == "stage2"
            and candidate["active"]
            and candidate["status"] == "Needs More Data"
            and candidate["promotion_eligible"]
            and candidate["rank_stage"] is not None
            and int(candidate["rank_stage"]) <= threshold
        ),
        key=lambda candidate: (int(candidate["rank_stage"]), str(candidate["candidate_id"])),
    )


def _stage3_initial_intake_queue(
    store: DuckDBStore,
    *,
    size: int,
) -> dict[str, Any]:
    """Create once and read the durable initial Stage 3 intake queue.

    The queue is deliberately recorded as pipeline events instead of being
    held only in worker memory. A restart can therefore continue the same
    top-ranked Stage 2 intake without rerunning Stage 2 or silently selecting
    a different set of candidates.
    """

    if size < 1:
        return {
            "status": "disabled",
            "batch_id": None,
            "queued_count": 0,
            "pending": [],
        }
    store.initialize()
    with store.connect() as connection:
        started = connection.execute(
            """
            SELECT sequence_number, payload_json
            FROM pipeline_events
            WHERE event_type = 'stage3_intake_started'
            ORDER BY sequence_number ASC
            LIMIT 1
            """
        ).fetchone()

    if not started:
        selected = promotable_candidates(store, stage="stage2", threshold=size)
        if selected:
            version = (store.latest_published_version() or 0) + 1
            batch_id = f"stage3-initial-intake-{version}"
            occurred_at = datetime.now(timezone.utc)
            events: list[dict[str, Any]] = [
                {
                    "event_id": f"{batch_id}-started",
                    "sequence_number": version,
                    "candidate_id": None,
                    "stage": "stage3",
                    "event_type": "stage3_intake_started",
                    "payload_json": {
                        "batch_id": batch_id,
                        "requested_count": size,
                        "selection_policy": "top_stage2_promising_snapshot",
                    },
                    "occurred_at": occurred_at,
                    "model_run_id": batch_id,
                }
            ]
            for candidate in selected:
                candidate_id = str(candidate["candidate_id"])
                events.append(
                    {
                        "event_id": f"{batch_id}-queued-{candidate_id}",
                        "sequence_number": version,
                        "candidate_id": candidate_id,
                        "stage": "stage3",
                        "event_type": "stage3_intake_queued",
                        "payload_json": {
                            "batch_id": batch_id,
                            "rank_at_enqueue": int(candidate["rank_stage"]),
                            "reason": "initial_top_stage2_promising_intake",
                        },
                        "occurred_at": occurred_at,
                        "model_run_id": batch_id,
                    }
                )
            store.publish_batch(
                version=version,
                events=events,
                note=f"Queued top {len(selected)} eligible Stage 2 candidates for initial Stage 3 intake",
            )
            started = (version, json.dumps({"batch_id": batch_id}))

    if not started:
        return {
            "status": "waiting",
            "batch_id": None,
            "queued_count": 0,
            "pending": [],
        }

    try:
        started_payload = json.loads(started[1] or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        started_payload = {}
    batch_id = str(started_payload.get("batch_id") or "")
    started_sequence = int(started[0] or 0)
    with store.connect() as connection:
        queued_rows = connection.execute(
            """
            SELECT q.candidate_id, q.sequence_number, q.payload_json,
                   c.target_id, c.rank_stage
            FROM pipeline_events q
            JOIN candidates c ON c.candidate_id = q.candidate_id
            WHERE q.event_type = 'stage3_intake_queued'
              AND q.sequence_number >= ?
              AND c.current_stage = 'stage2'
              AND c.status = 'Promising'
              AND c.promotion_eligible = true
            """,
            [started_sequence],
        ).fetchall()
        queued_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_events
                WHERE event_type = 'stage3_intake_queued'
                  AND sequence_number >= ?
                """,
                [started_sequence],
            ).fetchone()[0]
        )
        completed = {
            str(row[0])
            for row in queued_rows
            if connection.execute(
                """
                SELECT 1
                FROM pipeline_events
                WHERE event_type = 'stage3_evaluated'
                  AND candidate_id = ?
                  AND sequence_number >= ?
                LIMIT 1
                """,
                [row[0], row[1]],
            ).fetchone()
        }

    pending: list[dict[str, Any]] = []
    for candidate_id, sequence_number, payload_json, target_id, rank_stage in queued_rows:
        candidate_key = str(candidate_id)
        if candidate_key in completed:
            continue
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if str(payload.get("batch_id") or "") != batch_id:
            continue
        pending.append(
            {
                "candidate_id": candidate_key,
                "target_id": str(target_id),
                "rank_at_enqueue": int(payload.get("rank_at_enqueue") or rank_stage or 0),
                "rank_stage": rank_stage,
            }
        )
    pending.sort(key=lambda item: (int(item["rank_at_enqueue"]), item["candidate_id"]))
    return {
        "status": "queued" if pending else "complete",
        "batch_id": batch_id,
        "queued_count": queued_count,
        "pending": pending,
    }


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
              AND c.status = 'Needs More Data'
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
    dispatch_source_candidate_id: str | None = None,
    publish_lock: Any | None = None,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    stage2_input = load_stage2_input(store, candidate_id)
    if stage2_input is None:
        return {"candidate_id": candidate_id, "status": "missing_stage2_input", "dispatch_source_candidate_id": dispatch_source_candidate_id}
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
            "dispatch_source_candidate_id": dispatch_source_candidate_id,
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
    def publish() -> int:
        return publish_stage3_result(
            store,
            result,
            stage1_score=stage2_input["stage1_score"],
            stage2_score=stage2_input["stage2_score"],
            input_product_ids=list(product_ids),
            dispatch_source_candidate_id=dispatch_source_candidate_id,
        )

    # Historical downloads and model fitting may run concurrently, but the
    # ranking/state write must be serialized. This protects publish versions,
    # snapshots, and replacement-debt events from racing one another.
    if publish_lock is None:
        version = publish()
    else:
        with publish_lock:
            version = publish()
    return {
        "candidate_id": candidate_id,
        "status": result["status"],
        "publish_version": version,
        "history_products": len(paths),
        "discovered_sectors": history.get("sectors", []),
        "dispatch_source_candidate_id": dispatch_source_candidate_id,
    }


def _run_stage3_reassessment_batch(
    store: DuckDBStore,
    candidates: list[dict[str, Any]],
    *,
    data_dir: Path,
    max_history_sectors: int,
    concurrency: int,
    status_callback: Callable[[str, dict[str, Any]], None] | None,
    publish_lock: Any,
) -> list[dict[str, Any]]:
    """Reassess a bounded Stage 3 batch concurrently.

    Candidate selection is made once, in ranking order, before work starts.
    Each task owns its MAST/history fetch and CPU model evaluation. The
    publish lock is passed into ``_evaluate_stage3`` so only the durable
    ranking/state commit is serialized. Failed candidates remain in their
    current state and are returned as inspectable per-candidate errors rather
    than stopping all other reassessments in the batch.
    """

    if not candidates:
        return []
    worker_count = max(1, min(int(concurrency), len(candidates)))
    candidate_ids = [str(candidate["candidate_id"]) for candidate in candidates]
    if status_callback:
        status_callback(
            "processing_stage3",
            {
                "reason": "ranked_needs_more_data_batch",
                "candidate_id": candidate_ids[0],
                "candidate_ids": candidate_ids,
                "batch_size": len(candidates),
                "concurrency": worker_count,
            },
        )

    results_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="stage3-reassessment",
    ) as executor:
        futures = {
            executor.submit(
                _evaluate_stage3,
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
                publish_lock=publish_lock,
            ): str(candidate["candidate_id"])
            for candidate in candidates
        }
        for future in as_completed(futures):
            candidate_id = futures[future]
            try:
                results_by_id[candidate_id] = future.result()
            except Exception as exc:  # keep other candidates moving
                results_by_id[candidate_id] = {
                    "candidate_id": candidate_id,
                    "status": "error",
                    "error": repr(exc),
                }
                if status_callback:
                    status_callback(
                        "processing_stage3",
                        {
                            "reason": "stage3_reassessment_error",
                            "candidate_id": candidate_id,
                            "error": repr(exc),
                        },
                    )

    # Preserve the rank order chosen at dispatch time for downstream waterfall
    # decisions and readable cycle output, regardless of completion timing.
    return [results_by_id[candidate_id] for candidate_id in candidate_ids]


def _load_catalog_context(data_dir: Path) -> dict[str, Any]:
    """Load one complete catalog snapshot for Stage 3 Prime Candidates."""

    config = ExoplanetArchiveConfig(
        data_dir=data_dir,
        tables=("pscomppars", "toi"),
        # Matching uses the complete current archive projections. Do not let
        # a bounded probe setting silently hide a known object in production.
        max_rows=0,
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
    candidate_ids: list[str] | None = None,
    catalog_context: dict[str, Any] | None = None,
    return_match_ids: bool = False,
) -> dict[str, Any]:
    """Cross-reference only Stage 3 Prime Candidates.

    Stage 1/2 candidates and sibling signals are intentionally excluded. A
    match is therefore shown as a Stage 3 removal in replay, exactly when the
    candidate has reached Prime Candidate status.
    """

    candidates = candidate_match_inputs(store, candidate_ids, prime_only=True)
    if stage is not None and stage != "stage3":
        candidates = []
    if not candidates:
        return {"status": "no_active_candidates", "matches": 0, "stage": stage}
    context = catalog_context or _load_catalog_context(data_dir)
    adapter = context["adapter"]
    payload = context["payload"]
    records = context["records"]
    catalog_metadata = {
        "catalog_status": payload.get("status"),
        "catalog_record_count": int(payload.get("record_count") or len(records)),
        "catalog_fetched_at": payload.get("fetched_at"),
        "catalog_complete_projection": bool(payload.get("complete_projection", False)),
    }
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
        result = {
            "status": "no_matches",
            "matches": 0,
            "stage": stage,
            **catalog_metadata,
        }
        if return_match_ids:
            result["matched_candidate_ids"] = []
        return result
    version = publish_known_matches(
        store,
        matches,
        catalog_cache_path=str(payload.get("cache_path") or adapter.cache_path),
        top_n=int(os.getenv("TOP_N_DISPLAY", "100")),
    )
    result = {
        "status": "matched",
        "matches": len(matches),
        "stage": stage,
        "publish_version": version,
        **catalog_metadata,
    }
    if return_match_ids:
        result["matched_candidate_ids"] = [str(match["candidate_id"]) for match in matches]
    return result


def run_live_cycle(
    *,
    data_dir: Path,
    manifest_path: Path,
    batch_size: int,
    max_workers: int,
    top_n: int,
    stage1_threshold: int = 100,
    stage2_reassessment_threshold: int = 100,
    # Legacy keyword compatibility. This now aliases the Stage 2
    # reassessment window; Stage 3 promotion is no longer threshold-gated.
    stage2_threshold: int | None = None,
    # Legacy keyword compatibility for Stage 1 and Stage 2 callers.
    stage_capacity: int | None = None,
    max_history_sectors: int,
    stage1_pool_size: int = 500,
    stage1_continuous_intake_per_cycle: int = 2,
    stage2_capacity: int | None = None,
    stage3_initial_intake_size: int = 100,
    stage3_reassessment_concurrency: int = 4,
    status_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded archive poll and publish only changed pipeline state.

    Stage 1 has a ranking window for its own reassessment/promotion policy.
    Stage 2 has a separate top-X reassessment window for ``Needs More Data``;
    it is not a promotion cutoff. After the initial Stage 1 baseline, Stage 3
    is the durable waterfall dispatcher: it keeps Stage 2's reassessment
    queue moving, performs one durable initial intake of the top eligible
    Promising Stage 2 candidates, then calls the highest-ranked eligible
    Promising Stage 2 candidate without a rank cutoff and asks Stage 2 to
    request Stage 1 replacements.
    """

    from batching import BatchConfig, run_batch, run_stage1_refresh

    paths = DataPaths(data_dir)
    store = DuckDBStore(paths.database)
    store.initialize()
    if stage_capacity is not None:
        stage1_threshold = stage_capacity
    if stage2_threshold is not None:
        stage2_reassessment_threshold = stage2_threshold
    if stage2_capacity is not None:
        stage2_reassessment_threshold = stage2_capacity
    if min(stage1_threshold, stage2_reassessment_threshold) < 1:
        raise ValueError("stage priority thresholds must be positive")
    if stage1_continuous_intake_per_cycle < 0:
        raise ValueError("stage1_continuous_intake_per_cycle cannot be negative")
    if stage3_reassessment_concurrency < 1:
        raise ValueError("stage3_reassessment_concurrency must be positive")

    timings: dict[str, float] = {
        "stage1_seconds": 0.0,
        "stage2_seconds": 0.0,
        "stage2_needs_more_data_seconds": 0.0,
        "stage3_seconds": 0.0,
        "stage3_needs_more_data_seconds": 0.0,
        "stage3_dispatch_seconds": 0.0,
        "catalog_seconds": 0.0,
    }

    def add_timing(key: str, started: float) -> float:
        elapsed = max(0.0, time.perf_counter() - started)
        timings[key] += elapsed
        return elapsed

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

    def fill_stage1(
        replacement_slots: int | None = None,
        *,
        intake_reason: str | None = None,
    ) -> dict[str, Any]:
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
            replacement_reason = intake_reason or "released_stage1_work"
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
                "intake_reason": replacement_reason,
            }
        if status_callback:
            status_callback(
                "processing_stage1",
                {
                    "replacement_slots": replacement_needed,
                    "replacement_reason": replacement_reason,
                },
            )
        stage1_started = time.perf_counter()
        try:
            result = run_batch(
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
            result["intake_reason"] = replacement_reason
            return result
        finally:
            add_timing("stage1_seconds", stage1_started)

    # Bootstrap the configured Stage 1 baseline before downstream work begins.
    # After the baseline is established, a separate small intake below keeps
    # the Stage 1 ranking growing even when Stage 3 has no replacement debt.
    baseline_ready_at_cycle_start = processed_target_count(store) >= stage1_pool_size
    refresh_batch: dict[str, Any] = {"status": "idle", "refresh_count": 0}
    batch = fill_stage1() if processed_target_count(store) < stage1_pool_size else {
        "status": "idle",
        "message": "Initial Stage 1 baseline is complete",
        "selected_count": 0,
    }

    # Keep one immutable catalog snapshot for the entire cycle. The only gate
    # is Stage 3 Prime Candidate status; no earlier-stage or sibling signal
    # can trigger a catalog lookup.
    # The snapshot is loaded lazily so an empty stage does not make an
    # unnecessary external catalog request.
    catalog_context: dict[str, Any] | None = None
    catalog_checks: list[dict[str, Any]] = []

    def check_catalog(stage: str) -> dict[str, Any]:
        nonlocal catalog_context
        catalog_started = time.perf_counter()
        if status_callback:
            status_callback("matching_catalog", {"stage": stage})
        if catalog_context is None and any(
            candidate["current_stage"] == "stage3"
            and candidate.get("status") == "Promising"
            for candidate in active_candidates(store)
        ):
            catalog_context = _load_catalog_context(data_dir)
        result = _match_known_candidates(
            store,
            data_dir,
            stage=stage,
            catalog_context=catalog_context,
        )
        catalog_checks.append(result)
        add_timing("catalog_seconds", catalog_started)
        return result

    # Stage 1 known matches are checked before any Stage 3 dispatch. The
    # replacement itself is deliberately deferred: Stage 3 owns the waterfall
    # and will request the replacement when it calls the next downstream job.
    stage1_catalog_replacement: dict[str, Any] = {
        "status": "idle",
        "message": "Catalog matching is reserved for Stage 3 Prime Candidates",
        "selected_count": 0,
    }
    signal_independence: dict[str, Any] = {
        "status": "idle",
        "message": "Sibling independence is requested by Stage 3 when it needs a Stage 2 handoff",
        "assessed": 0,
    }

    initial_stage1_ready = processed_target_count(store) >= stage1_pool_size
    stage2_results: list[dict[str, Any]] = []
    stage3_results: list[dict[str, Any]] = []
    stage1_requests: list[dict[str, Any]] = []
    stage3_publish_lock = Lock()
    attempted_stage1: set[str] = set()
    attempted_stage2: set[str] = set()
    attempted_stage2_reassessments: set[str] = set()
    def request_stage1_reassessment() -> dict[str, Any]:
        """Ask Stage 1 for more data only after Stage 3 requests a handoff."""

        nonlocal signal_independence
        signal_independence = resolve_priority_aliases(
            store,
            data_dir=data_dir,
            threshold=stage1_threshold,
            processed_target_count=processed_target_count(store),
        )
        targets = _stage1_refresh_targets(
            store,
            data_dir=data_dir,
            max_targets=1,
            priority_threshold=stage1_threshold,
        )
        if not targets:
            return {"status": "idle", "refresh_count": 0, "message": "No Stage 1 Needs More Data refresh is available"}
        if status_callback:
            status_callback("processing_stage1", {"reason": "stage3_waterfall_request"})
        stage1_started = time.perf_counter()
        try:
            result = run_stage1_refresh(
                data_dir=data_dir,
                manifest_path=manifest_path,
                targets=targets,
                config=BatchConfig(
                    batch_size=len(targets),
                    max_workers=max_workers,
                    top_n=top_n,
                    stage_capacity=stage1_threshold,
                    target_pool_size=stage1_pool_size,
                ),
            )
        finally:
            add_timing("stage1_seconds", stage1_started)
        result["intake_reason"] = "stage1_needs_more_data_reassessment"
        return result

    def request_stage2_replacement() -> dict[str, Any] | None:
        """Have a Stage 3 call cascade into one Stage 1 -> Stage 2 request."""

        before_targets = occupied_stage1_target_ids()
        candidates_now = [
            candidate
            for candidate in promotable_candidates(
                store,
                stage="stage1",
                threshold=stage1_threshold,
            )
            if candidate["candidate_id"] not in attempted_stage1
        ]
        if not candidates_now:
            refresh_result = request_stage1_reassessment()
            if refresh_result.get("status") != "idle":
                stage1_requests.append(refresh_result)
            candidates_now = [
                candidate
                for candidate in promotable_candidates(
                    store,
                    stage="stage1",
                    threshold=stage1_threshold,
                )
                if candidate["candidate_id"] not in attempted_stage1
            ]
        if not candidates_now:
            return None
        candidate = candidates_now[0]
        candidate_id = str(candidate["candidate_id"])
        attempted_stage1.add(candidate_id)
        if status_callback:
            status_callback(
                "processing_stage2",
                {"candidate_id": candidate_id, "reason": "stage3_waterfall_request"},
            )
        stage2_started = time.perf_counter()
        result = _evaluate_stage2(
            store,
            candidate,
            data_dir=data_dir,
            max_history_sectors=max_history_sectors,
        )
        result.update(
            {
                "queue_kind": "handoff",
                "queue_status": "Promising",
                "queue_reason": "stage3_called_stage1_promising_candidate",
            }
        )
        elapsed = add_timing("stage2_seconds", stage2_started)
        if result.get("status") == "Needs More Data":
            timings["stage2_needs_more_data_seconds"] += elapsed
        stage2_results.append(result)
        released = len(before_targets - occupied_stage1_target_ids())
        if released:
            replacement = fill_stage1(released)
            if replacement.get("status") != "idle":
                stage1_requests.append(replacement)
        return result

    stage2_reassessment_budget = max(
        1,
        int(
            os.getenv(
                "STAGE2_REASSESSMENTS_PER_CYCLE",
                str(stage2_reassessment_threshold),
            )
        ),
    )

    def run_stage2_reassessment_queue() -> int:
        """Process the current Stage 2 top-window queue in rank order.

        This sweep is intentionally independent of whether Stage 2 currently
        has Promising candidates. A new Stage 3 call or Stage 2 publish can
        change the ranking, so candidates are selected again after every
        evaluation. The per-cycle set prevents an unchanged candidate from
        being retried repeatedly in the same cycle, while the stored coverage
        signature prevents redundant model reruns across cycles.
        """

        processed = 0
        while processed < stage2_reassessment_budget:
            candidates_now = [
                candidate
                for candidate in _stage2_needs_data_queue(
                    active_candidates(store),
                    threshold=stage2_reassessment_threshold,
                )
                if str(candidate["candidate_id"]) not in attempted_stage2_reassessments
            ]
            if not candidates_now:
                break

            candidate = candidates_now[0]
            candidate_id = str(candidate["candidate_id"])
            attempted_stage2_reassessments.add(candidate_id)
            if status_callback:
                status_callback(
                    "processing_stage2",
                    {
                        "candidate_id": candidate_id,
                        "rank": int(candidate["rank_stage"]),
                        "reason": "stage2_top_window_reassessment",
                    },
                )
            stage2_started = time.perf_counter()
            result = _evaluate_stage2(
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
            )
            result.update(
                {
                    "queue_kind": "reassessment",
                    "queue_status": "Needs More Data",
                    "queue_reason": "stage2_top_window_reassessment",
                }
            )
            elapsed = add_timing("stage2_seconds", stage2_started)
            if result.get("status") == "Needs More Data":
                timings["stage2_needs_more_data_seconds"] += elapsed
            stage2_results.append(result)
            processed += 1

            # An inconclusive Stage 2 result releases its processing slot, so
            # the waterfall immediately asks Stage 1 for the replacement.
            # A Promising result stays in Stage 2 until Stage 3 calls for it.
            if result.get("status") == "Inconclusive":
                request_stage2_replacement()

        return processed

    def select_stage2_candidate() -> dict[str, Any] | None:
        """Return the highest-ranked Stage 2 Promising candidate for Stage 3.

        Stage 2's top-window reassessment sweep runs before every selection.
        The fallback replacement loop is only for the case where that sweep
        still leaves Stage 3 without an eligible Promising candidate.
        """

        for _ in range(3):
            run_stage2_reassessment_queue()
            candidates_now = [
                candidate
                for candidate in active_candidates(store)
                if candidate["current_stage"] == "stage2"
                and candidate["status"] == "Promising"
                and candidate["promotion_eligible"]
                and candidate["rank_stage"] is not None
                if candidate["candidate_id"] not in attempted_stage2
            ]
            candidates_now.sort(
                key=lambda item: (int(item["rank_stage"]), str(item["candidate_id"]))
            )
            if candidates_now:
                return candidates_now[0]

            if request_stage2_replacement() is None:
                return None
        return None

    # Stage 3 is the dispatcher. It first walks the ranked Needs More Data
    # queue without allowing one unchanged candidate to block the rest. Then
    # it spends durable replacement debt: every legacy/current Prime,
    # Inconclusive, or Stage 3 catalog match gets one downstream call.
    waterfall_ready = initial_stage1_ready
    stage3_intake: dict[str, Any] = {
        "status": "waiting",
        "batch_id": None,
        "queued_count": 0,
        "pending": [],
    }
    reassessment_budget = max(1, int(os.getenv("STAGE3_REASSESSMENTS_PER_CYCLE", "4")))
    attempted_stage3: set[str] = set()
    if waterfall_ready:
        # Stage 2 owns this rolling window even when Promising candidates are
        # available. This is the part that prevents a healthy-looking Stage 2
        # ranking from hiding a growing Needs More Data backlog.
        run_stage2_reassessment_queue()
        stage3_intake = _stage3_initial_intake_queue(
            store,
            size=stage3_initial_intake_size,
        )
        for queued_candidate in list(stage3_intake["pending"]):
            candidate_id = str(queued_candidate["candidate_id"])
            candidate = next(
                (
                    item
                    for item in active_candidates(store)
                    if str(item["candidate_id"]) == candidate_id
                    and item["current_stage"] == "stage2"
                    and item["status"] == "Promising"
                    and item["promotion_eligible"]
                ),
                None,
            )
            if candidate is None:
                continue
            if status_callback:
                status_callback(
                    "processing_stage3",
                    {
                        "candidate_id": candidate_id,
                        "reason": "stage3_initial_intake",
                        "intake_batch_id": stage3_intake["batch_id"],
                        "rank_at_enqueue": queued_candidate["rank_at_enqueue"],
                    },
                )
            stage3_started = time.perf_counter()
            result = _evaluate_stage3(
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
                publish_lock=stage3_publish_lock,
            )
            result.update(
                {
                    "queue_kind": "handoff",
                    "queue_status": "Promising",
                    "queue_reason": "stage3_initial_intake_from_stage2",
                }
            )
            add_timing("stage3_seconds", stage3_started)
            stage3_results.append(result)
            check_catalog("stage3")
            if result.get("publish_version") is not None:
                request_stage2_replacement()
                run_stage2_reassessment_queue()
        stage3_intake = _stage3_initial_intake_queue(
            store,
            size=stage3_initial_intake_size,
        )
        resolved_sources: list[str] = []
        reassessment_candidates = [
            candidate
            for candidate in _stage3_needs_data_queue(active_candidates(store))
            if candidate["candidate_id"] not in attempted_stage3
        ][:reassessment_budget]
        attempted_stage3.update(str(candidate["candidate_id"]) for candidate in reassessment_candidates)
        stage3_reassessment_started = time.perf_counter()
        reassessment_results = _run_stage3_reassessment_batch(
            store,
            reassessment_candidates,
            data_dir=data_dir,
            max_history_sectors=max_history_sectors,
            concurrency=stage3_reassessment_concurrency,
            status_callback=status_callback,
            publish_lock=stage3_publish_lock,
        )
        reassessment_elapsed = add_timing("stage3_seconds", stage3_reassessment_started)
        for result in reassessment_results:
            result.update(
                {
                    "queue_kind": "reassessment",
                    "queue_status": "Needs More Data",
                    "queue_reason": "stage3_ranked_needs_more_data_reassessment",
                }
            )
            stage3_results.append(result)
            if result.get("status") == "Needs More Data":
                timings["stage3_needs_more_data_seconds"] += reassessment_elapsed / max(1, len(reassessment_results))
            if result.get("status") != "error":
                check_catalog("stage3")
            if result.get("status") in {"Promising", "Inconclusive"}:
                resolved_sources.append(str(result["candidate_id"]))

        dispatch_sources: list[str] = []
        dispatch_seen: set[str] = set()

        def enqueue_sources(values: list[str]) -> None:
            for value in values:
                if value not in dispatch_seen and value not in dispatch_sources:
                    dispatch_sources.append(value)

        enqueue_sources([str(item["candidate_id"]) for item in stage3_replacement_debt(store)])
        enqueue_sources(resolved_sources)
        dispatch_budget = max(1, int(os.getenv("STAGE3_DISPATCHES_PER_CYCLE", "4")))
        for _ in range(dispatch_budget):
            enqueue_sources([str(item["candidate_id"]) for item in stage3_replacement_debt(store)])
            if not dispatch_sources:
                break
            source_candidate_id = dispatch_sources.pop(0)
            dispatch_seen.add(source_candidate_id)
            candidate = select_stage2_candidate()
            if candidate is None:
                break
            candidate_id = str(candidate["candidate_id"])
            attempted_stage2.add(candidate_id)
            if status_callback:
                status_callback(
                    "processing_stage3",
                    {
                        "candidate_id": candidate_id,
                        "reason": "stage3_called_stage2",
                        "source_candidate_id": source_candidate_id,
                    },
                )
            stage3_started = time.perf_counter()
            result = _evaluate_stage3(
                store,
                candidate,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
                dispatch_source_candidate_id=source_candidate_id,
                publish_lock=stage3_publish_lock,
            )
            result.update(
                {
                    "queue_kind": "handoff",
                    "queue_status": "Promising",
                    "queue_reason": "stage3_called_stage2_promising_candidate",
                }
            )
            dispatch_elapsed = add_timing("stage3_seconds", stage3_started)
            timings["stage3_dispatch_seconds"] += dispatch_elapsed
            if result.get("status") == "Needs More Data":
                timings["stage3_needs_more_data_seconds"] += dispatch_elapsed
            stage3_results.append(result)
            check_catalog("stage3")
            if result.get("publish_version") is not None:
                # Every actual Stage 2 -> Stage 3 handoff opens a Stage 2
                # request, regardless of whether the Stage 3 result later
                # resolves as Prime, Inconclusive, or Known Match.
                request_stage2_replacement()
                run_stage2_reassessment_queue()
            if result.get("status") in {"Promising", "Inconclusive"}:
                enqueue_sources([candidate_id])

    # Stage 3 is the final assessment stage, so a catalog match here is a
    # legitimate Stage 3 removal and is retained in the identified/replay
    # history at that stage.
    continuous_stage1_intake: dict[str, Any] = {
        "status": "idle",
        "message": (
            "Continuous Stage 1 intake is disabled"
            if stage1_continuous_intake_per_cycle == 0
            else "Waiting for the initial Stage 1 baseline"
        ),
        "selected_count": 0,
        "evaluated_count": 0,
        "intake_reason": "continuous_stage1_intake",
    }
    if baseline_ready_at_cycle_start and stage1_continuous_intake_per_cycle:
        if status_callback:
            status_callback(
                "processing_stage1",
                {
                    "reason": "continuous_stage1_intake",
                    "requested_count": stage1_continuous_intake_per_cycle,
                },
            )
        continuous_stage1_intake = fill_stage1(
            stage1_continuous_intake_per_cycle,
            intake_reason="continuous_stage1_intake",
        )

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
        "signal_independence": signal_independence,
        "replacement_batch": stage1_requests[-1] if stage1_requests else {"status": "idle", "selected_count": 0},
        "stage1_requests": stage1_requests,
        "continuous_stage1_intake": continuous_stage1_intake,
        "exploration_batch": continuous_stage1_intake,
        "stage2": stage2_results,
        "stage3": stage3_results,
        "stage3_intake": {
            "status": stage3_intake["status"],
            "batch_id": stage3_intake["batch_id"],
            "queued_count": stage3_intake["queued_count"],
            "pending_count": len(stage3_intake["pending"]),
            "selection_size": stage3_initial_intake_size,
        },
        "stage3_reassessment": {
            "concurrency": stage3_reassessment_concurrency,
            "requested_count": reassessment_budget if waterfall_ready else 0,
            "evaluated_count": sum(
                1
                for result in stage3_results
                if result.get("status") != "error"
                and result.get("candidate_id") in attempted_stage3
            ),
        },
        "stage2_reassessment": {
            "threshold": stage2_reassessment_threshold,
            "requested_count": stage2_reassessment_budget if waterfall_ready else 0,
            "evaluated_count": sum(
                1
                for result in stage2_results
                if result.get("queue_reason") == "stage2_top_window_reassessment"
            ),
        },
        "timings": {key: round(value, 3) for key, value in timings.items()},
        "catalog": catalog,
    }


__all__ = ["run_live_cycle"]
