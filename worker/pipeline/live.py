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


def _history_limit(
    max_history_sectors: int,
    previous_product_count: int,
    *,
    expansion_sectors: int | None = None,
) -> int:
    """Expand archive coverage only when the prior window has been consumed."""

    if max_history_sectors <= 0:
        return max_history_sectors
    initial = max(1, int(os.getenv("INITIAL_HISTORY_SECTORS", "2")))
    # A Needs More Data reassessment should materially expand coverage. Four
    # additional sectors usually moves the candidate from a short probe to a
    # useful multi-window comparison, while max_history_sectors remains the
    # hard upper bound for one candidate.
    expansion = max(
        1,
        int(
            expansion_sectors
            if expansion_sectors is not None
            else os.getenv("HISTORY_EXPANSION_SECTORS", "4")
        ),
    )
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


def _stage3_prime_rank_reassessment_queue(
    store: DuckDBStore,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return Prime Candidates whose Stage 3 rank moved since their last test.

    Rank movement is persisted as a pipeline event by Stage 3 publication. A
    later Stage 3 evaluation or a durable no-new-coverage attempt consumes the
    movement event, so a worker restart cannot lose the re-test and an
    unchanged archive window cannot cause an infinite retry loop.
    """

    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, stage, event_type, sequence_number, payload_json
            FROM pipeline_events
            WHERE candidate_id IS NOT NULL
              AND event_type IN (
                'stage3_prime_rank_moved',
                'stage3_evaluated',
                'known_match',
                'stage3_prime_reassessment_attempted'
              )
            ORDER BY sequence_number ASC
            """
        ).fetchall()

    movement_sequences: dict[str, int] = {}
    consumed_sequences: dict[str, int] = {}
    for candidate_id, stage, event_type, sequence_number, payload_json in rows:
        if str(stage or "") != "stage3":
            continue
        candidate_key = str(candidate_id)
        sequence = int(sequence_number or 0)
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if event_type == "stage3_prime_rank_moved":
            movement_sequences[candidate_key] = max(
                movement_sequences.get(candidate_key, 0),
                sequence,
            )
        elif event_type == "stage3_prime_reassessment_attempted":
            consumed_sequences[candidate_key] = max(
                consumed_sequences.get(candidate_key, 0),
                int(payload.get("movement_sequence") or sequence),
            )
        else:
            consumed_sequences[candidate_key] = max(
                consumed_sequences.get(candidate_key, 0),
                sequence,
            )

    pending_sequences = {
        candidate_id: movement_sequence
        for candidate_id, movement_sequence in movement_sequences.items()
        if movement_sequence > consumed_sequences.get(candidate_id, 0)
    }
    return sorted(
        [
            {
                **candidate,
                "_stage3_prime_rank_movement_sequence": pending_sequences[
                    str(candidate["candidate_id"])
                ],
            }
            for candidate in candidates
            if str(candidate["candidate_id"]) in pending_sequences
            and candidate["current_stage"] == "stage3"
            and candidate["active"]
            and candidate["status"] == "Promising"
            and candidate.get("rank_stage") is not None
        ],
        key=lambda candidate: (
            int(candidate["rank_stage"]),
            str(candidate["candidate_id"]),
        ),
    )


def _record_stage3_prime_reassessment_attempt(
    store: DuckDBStore,
    *,
    candidate_id: str,
    movement_sequence: int,
) -> int:
    """Durably consume a Prime rank-movement job when coverage is unchanged."""

    version = (store.latest_published_version() or 0) + 1
    occurred_at = datetime.now(timezone.utc)
    store.publish_batch(
        version=version,
        note=f"Deferred Stage 3 Prime re-test for {candidate_id}; no expanded coverage available",
        events=[
            {
                "event_id": f"stage3-prime-reassessment-attempted-{candidate_id}-{version}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": "stage3",
                "event_type": "stage3_prime_reassessment_attempted",
                "payload_json": {
                    "movement_sequence": movement_sequence,
                    "reason": "no_expanded_coverage_available",
                },
                "occurred_at": occurred_at,
                "model_run_id": f"stage3-prime-reassessment-{candidate_id}-{version}",
            }
        ],
    )
    return version


def _stage2_needs_data_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return all active, reassessable Stage 2 candidates in rank order."""

    return sorted(
        (
            candidate
            for candidate in candidates
            if candidate["current_stage"] == "stage2"
            and candidate["active"]
            and candidate["status"] == "Needs More Data"
            and candidate["promotion_eligible"]
            and candidate["rank_stage"] is not None
        ),
        key=lambda candidate: (int(candidate["rank_stage"]), str(candidate["candidate_id"])),
    )


def _stage2_needs_data_queue(
    candidates: list[dict[str, Any]],
    *,
    threshold: int,
) -> list[dict[str, Any]]:
    """Return the Stage 2 top-window work queue in rank order.

    The window is a reassessment trigger, not a capacity or promotion gate.
    Candidates remain in the Stage 2 ranking while this helper orders the
    preferred work queue by the current Stage 2 rank. Callers can use
    ``_stage2_needs_data_candidates`` for data-available fallback work beyond
    the window when a preferred candidate has no new coverage.
    """

    return [
        candidate
        for candidate in _stage2_needs_data_candidates(candidates)
        if int(candidate["rank_stage"]) <= threshold
    ]


def _stage2_waterfall_gate_snapshot(
    candidates: list[dict[str, Any]],
    *,
    threshold: int,
) -> dict[str, Any]:
    """Describe whether normal downstream work may resume.

    The gate is deliberately separate from Stage 2's reassessment window.
    The window decides which ``Needs More Data`` rows receive work; this gate
    decides whether Stage 1 intake and Stage 3 jobs may run at all.  Requiring
    a complete ranked cohort prevents a short or mixed Stage 2 list from
    making the downstream waterfall look healthy while it is still carrying
    unresolved work.
    """

    if threshold < 1:
        raise ValueError("Stage 2 waterfall gate threshold must be positive")
    ranked = sorted(
        (
            candidate
            for candidate in candidates
            if candidate.get("current_stage") == "stage2"
            and candidate.get("active", True)
            and candidate.get("rank_stage") is not None
        ),
        key=lambda candidate: (int(candidate["rank_stage"]), str(candidate["candidate_id"])),
    )
    top_window = ranked[:threshold]
    status_counts: dict[str, int] = {}
    for candidate in top_window:
        status = str(candidate.get("status") or "Unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    promising_count = status_counts.get("Promising", 0)
    complete_cohort = len(top_window) >= threshold
    gate_open = complete_cohort and promising_count == threshold
    if not complete_cohort:
        reason = "waiting_for_stage2_cohort"
        message = f"Stage 2 has {len(top_window)} ranked candidates; {threshold} are required before normal downstream work can resume."
    elif gate_open:
        reason = "top_stage2_window_is_promising"
        message = f"The top {threshold} Stage 2 candidates are Promising; normal Stage 1 and Stage 3 operations may run."
    else:
        reason = "top_stage2_window_needs_resolution"
        message = f"{threshold - promising_count} of the top {threshold} Stage 2 candidates are not Promising; Stage 1 and Stage 3 jobs remain paused while Stage 2 works the window."
    return {
        "open": gate_open,
        "threshold": threshold,
        "ranked_count": len(ranked),
        "top_count": len(top_window),
        "promising_count": promising_count,
        "non_promising_count": max(0, len(top_window) - promising_count),
        "status_counts": status_counts,
        "top_candidate_ids": [str(candidate["candidate_id"]) for candidate in top_window],
        "reason": reason,
        "message": message,
        "paused_stages": [] if gate_open else ["stage1", "stage3"],
    }


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
    publish_lock: Any | None = None,
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
    def publish() -> int:
        return publish_stage2_result(
            store,
            result,
            stage1_score=stage1_input["stage1_score"],
            input_product_ids=list(product_ids),
        )

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
    }


def _run_stage2_manual_batch(
    store: DuckDBStore,
    candidates: list[dict[str, Any]],
    *,
    data_dir: Path,
    max_history_sectors: int,
    concurrency: int,
    status_callback: Callable[[str, dict[str, Any]], None] | None,
    publish_lock: Any,
) -> list[dict[str, Any]]:
    """Evaluate an operator-selected Stage 1 → Stage 2 batch concurrently."""

    if not candidates:
        return []
    worker_count = max(1, min(int(concurrency), len(candidates)))
    candidate_ids = [str(candidate["candidate_id"]) for candidate in candidates]
    if status_callback:
        status_callback(
            "processing_stage2",
            {
                "reason": "manual_stage2_migration",
                "candidate_ids": candidate_ids,
                "batch_size": len(candidates),
                "concurrency": worker_count,
            },
        )
    results_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="manual-stage2-migration",
    ) as executor:
        futures = {
            executor.submit(
                _evaluate_stage2,
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
            except Exception as exc:
                results_by_id[candidate_id] = {
                    "candidate_id": candidate_id,
                    "status": "error",
                    "error": repr(exc),
                }
                if status_callback:
                    status_callback(
                        "processing_stage2",
                        {
                            "reason": "manual_stage2_migration_error",
                            "candidate_id": candidate_id,
                            "error": repr(exc),
                        },
                    )
    return [results_by_id[candidate_id] for candidate_id in candidate_ids]


def _run_stage2_reassessment_batch(
    store: DuckDBStore,
    candidates: list[dict[str, Any]],
    *,
    data_dir: Path,
    max_history_sectors: int,
    concurrency: int,
    status_callback: Callable[[str, dict[str, Any]], None] | None,
    publish_lock: Any,
) -> list[dict[str, Any]]:
    """Reassess a bounded Stage 2 priority batch concurrently.

    MAST discovery/download and the Stage 2 model run are independent for
    each candidate, so they can overlap. Only the durable ranking publication
    is serialized by ``publish_lock``. The batch is selected in current rank
    order before dispatch, preserving the priority-window policy while
    preventing one slow candidate from blocking every other reassessment.
    """

    if not candidates:
        return []
    worker_count = max(1, min(int(concurrency), len(candidates)))
    candidate_ids = [str(candidate["candidate_id"]) for candidate in candidates]
    if status_callback:
        status_callback(
            "processing_stage2",
            {
                "reason": "stage2_top_window_reassessment_batch",
                "candidate_id": candidate_ids[0],
                "candidate_ids": candidate_ids,
                "batch_size": len(candidates),
                "concurrency": worker_count,
            },
        )

    results_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="stage2-reassessment",
    ) as executor:
        futures = {
            executor.submit(
                _evaluate_stage2,
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
            except Exception as exc:  # keep other reassessments moving
                results_by_id[candidate_id] = {
                    "candidate_id": candidate_id,
                    "status": "error",
                    "error": repr(exc),
                }
                if status_callback:
                    status_callback(
                        "processing_stage2",
                        {
                            "reason": "stage2_reassessment_error",
                            "candidate_id": candidate_id,
                            "error": repr(exc),
                        },
                    )

    return [results_by_id[candidate_id] for candidate_id in candidate_ids]


def _evaluate_stage3(
    store: DuckDBStore,
    candidate: dict[str, Any],
    *,
    data_dir: Path,
    max_history_sectors: int,
    dispatch_source_candidate_id: str | None = None,
    force_expanded_history: bool = False,
    publish_lock: Any | None = None,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    stage2_input = load_stage2_input(store, candidate_id)
    if stage2_input is None:
        return {"candidate_id": candidate_id, "status": "missing_stage2_input", "dispatch_source_candidate_id": dispatch_source_candidate_id}
    previous_signature = _latest_evaluation_signature(store, candidate_id, "stage3")
    previous_product_count = len(previous_signature[1]) if previous_signature else 0
    history_expansion = None
    if force_expanded_history:
        history_expansion = int(
            os.getenv(
                "STAGE3_PRIME_HISTORY_EXPANSION_SECTORS",
                os.getenv("HISTORY_EXPANSION_SECTORS", "4"),
            )
        )
    paths, history = _history_for_candidate(
        target_id=stage2_input["target_id"],
        data_dir=data_dir,
        max_sectors=_history_limit(
            max_history_sectors,
            previous_product_count,
            expansion_sectors=history_expansion,
        ),
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
                "reason": "ranked_stage3_reassessment_batch",
                "candidate_id": candidate_ids[0],
                "candidate_ids": candidate_ids,
                "batch_size": len(candidates),
                "concurrency": worker_count,
                "prime_rank_movement_count": sum(
                    bool(candidate.get("_stage3_prime_rank_movement_sequence"))
                    for candidate in candidates
                ),
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
                force_expanded_history=bool(
                    candidate.get("_stage3_prime_rank_movement_sequence")
                ),
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
    stage2_reassessment_concurrency: int = 4,
    stage3_initial_intake_size: int = 100,
    stage3_reassessment_concurrency: int = 4,
    manual_migrations: list[dict[str, Any]] | None = None,
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
    request Stage 1 replacements. A separate Stage 2 waterfall gate pauses
    Stage 1 intake and all Stage 3 work until the ranked Stage 2 top window is
    complete and every slot in it is Promising.
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
    stage2_waterfall_gate_threshold = int(
        os.getenv("STAGE2_WATERFALL_GATE_THRESHOLD", "100")
    )
    if min(stage1_threshold, stage2_reassessment_threshold) < 1:
        raise ValueError("stage priority thresholds must be positive")
    if stage2_waterfall_gate_threshold < 1:
        raise ValueError("STAGE2_WATERFALL_GATE_THRESHOLD must be positive")
    if stage1_continuous_intake_per_cycle < 0:
        raise ValueError("stage1_continuous_intake_per_cycle cannot be negative")
    if stage3_reassessment_concurrency < 1:
        raise ValueError("stage3_reassessment_concurrency must be positive")
    if stage2_reassessment_concurrency < 1:
        raise ValueError("stage2_reassessment_concurrency must be positive")

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
    manual_migration_results: list[dict[str, Any]] = []
    attempted_stage1: set[str] = set()
    attempted_stage2: set[str] = set()
    attempted_stage2_reassessments: set[str] = set()
    attempted_stage3: set[str] = set()
    stage2_reassessments_used = 0
    # The gate starts closed for each cycle and is opened only after the
    # current Stage 2 top window has been re-ranked and every slot is
    # Promising. Stage 2 may continue to work while the other stages pause.
    stage2_gate_open = False
    stage2_gate_snapshot = _stage2_waterfall_gate_snapshot(
        active_candidates(store),
        threshold=stage2_waterfall_gate_threshold,
    )
    stage2_gate_bootstrap_used = 0

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
            # While the Stage 2 gate is closed, do not start a new Stage 1
            # refresh just to manufacture another handoff. Existing Stage 2
            # work can continue; normal Stage 1 intake resumes only after the
            # gate opens.
            if not stage2_gate_open:
                return None
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
        if released and stage2_gate_open:
            replacement = fill_stage1(released)
            if replacement.get("status") != "idle":
                stage1_requests.append(replacement)
        return result

    stage2_reassessment_budget = max(
        1,
        int(
            os.getenv(
                "STAGE2_REASSESSMENTS_PER_CYCLE",
                "8",
            )
        ),
    )
    stage2_reassessment_telemetry: dict[str, Any] = {
        "queue_order": "stage2_rank_ascending",
        "queue_threshold": stage2_reassessment_threshold,
        "initial_queue_count": 0,
        "initial_queue_candidate_ids": [],
        "last_queue_count": 0,
        "last_queue_candidate_ids": [],
        "remaining_queue_count": 0,
        "remaining_queue_candidate_ids": [],
        "dispatch_batches": [],
        "called_candidate_ids": [],
        "fallback_candidate_ids": [],
        "fallback_attempts": 0,
        "fallback_attempt_budget": max(1, stage2_reassessment_budget * 3),
        "published_candidate_ids": [],
        "unchanged_candidate_ids": [],
        "fallback_unchanged_candidate_ids": [],
        "inconclusive_candidate_ids": [],
        "error_candidate_ids": [],
    }

    def run_stage2_reassessment_queue() -> int:
        """Process the current Stage 2 top-window queue in rank order.

        This sweep is intentionally independent of whether Stage 2 currently
        has Promising candidates. A new Stage 3 call or Stage 2 publish can
        change the ranking, so candidates are selected again after every
        evaluation. The per-cycle set prevents an unchanged candidate from
        being retried repeatedly in the same cycle, while the stored coverage
        signature prevents redundant model reruns across cycles.
        """

        nonlocal stage2_reassessments_used
        processed = 0
        replacement_slots = 0
        remaining_budget = max(0, stage2_reassessment_budget - stage2_reassessments_used)
        while processed < remaining_budget:
            queue_now = _stage2_needs_data_queue(
                active_candidates(store),
                threshold=stage2_reassessment_threshold,
            )
            queue_ids = [str(candidate["candidate_id"]) for candidate in queue_now]
            if not stage2_reassessment_telemetry["initial_queue_candidate_ids"]:
                stage2_reassessment_telemetry["initial_queue_count"] = len(queue_now)
                stage2_reassessment_telemetry["initial_queue_candidate_ids"] = queue_ids[:stage2_reassessment_threshold]
            stage2_reassessment_telemetry["last_queue_count"] = len(queue_now)
            stage2_reassessment_telemetry["last_queue_candidate_ids"] = queue_ids[:stage2_reassessment_threshold]
            candidates_now = [
                candidate
                for candidate in queue_now
                if str(candidate["candidate_id"]) not in attempted_stage2_reassessments
            ]
            if not candidates_now:
                break

            candidates_batch = candidates_now[: min(
                stage2_reassessment_concurrency,
                remaining_budget - processed,
            )]
            stage2_reassessment_telemetry["dispatch_batches"].append(
                [str(candidate["candidate_id"]) for candidate in candidates_batch]
            )
            stage2_reassessment_telemetry["called_candidate_ids"].extend(
                str(candidate["candidate_id"]) for candidate in candidates_batch
            )
            attempted_stage2_reassessments.update(
                str(candidate["candidate_id"]) for candidate in candidates_batch
            )
            stage2_started = time.perf_counter()
            results = _run_stage2_reassessment_batch(
                store,
                candidates_batch,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
                concurrency=stage2_reassessment_concurrency,
                status_callback=status_callback,
                publish_lock=stage3_publish_lock,
            )
            elapsed = add_timing("stage2_seconds", stage2_started)
            if any(
                result.get("status") in {"Needs More Data", "unchanged"}
                for result in results
            ):
                timings["stage2_needs_more_data_seconds"] += elapsed
            for result in results:
                result.update(
                    {
                        "queue_kind": "reassessment",
                        "queue_status": "Needs More Data",
                        "queue_reason": "stage2_top_window_reassessment",
                    }
                )
                stage2_results.append(result)
                result_status = str(result.get("status") or "error")
                if result_status == "unchanged":
                    stage2_reassessment_telemetry["unchanged_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                    replacement_slots += 1
                elif result_status == "error":
                    stage2_reassessment_telemetry["error_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                elif result.get("publish_version") is not None:
                    stage2_reassessment_telemetry["published_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                if result_status == "Inconclusive":
                    stage2_reassessment_telemetry["inconclusive_candidate_ids"].append(
                        str(result["candidate_id"])
                    )

            processed += len(results)
            stage2_reassessments_used += len(results)

            # An inconclusive Stage 2 result releases its processing slot, so
            # the waterfall immediately asks Stage 1 for the replacement.
            # A Promising result stays in Stage 2 until Stage 3 calls for it.
            for result in results:
                if result.get("status") == "Inconclusive":
                    request_stage2_replacement()

        # A preferred top-window candidate can be called and still have no
        # additional MAST coverage. It remains in the ranking, but it should
        # not occupy a useful Stage 2 work slot for the rest of this cycle.
        # Replace that slot with the next ranked Needs More Data candidate
        # outside the window, continuing until the slot produces a new
        # evaluation or the bounded fallback attempt budget is exhausted.
        fallback_attempt_budget = max(
            1,
            int(
                os.getenv(
                    "STAGE2_FALLBACK_ATTEMPTS_PER_CYCLE",
                    str(stage2_reassessment_budget * 3),
                )
            ),
        )
        stage2_reassessment_telemetry["fallback_attempt_budget"] = fallback_attempt_budget
        fallback_attempts = 0
        while replacement_slots and fallback_attempts < fallback_attempt_budget:
            fallback_candidates = [
                candidate
                for candidate in _stage2_needs_data_candidates(active_candidates(store))
                if int(candidate["rank_stage"]) > stage2_reassessment_threshold
                and str(candidate["candidate_id"]) not in attempted_stage2_reassessments
            ]
            if not fallback_candidates:
                break
            candidates_batch = fallback_candidates[: min(
                stage2_reassessment_concurrency,
                replacement_slots,
                fallback_attempt_budget - fallback_attempts,
            )]
            if not candidates_batch:
                break
            candidate_ids = [str(candidate["candidate_id"]) for candidate in candidates_batch]
            stage2_reassessment_telemetry["dispatch_batches"].append(candidate_ids)
            stage2_reassessment_telemetry["fallback_candidate_ids"].extend(candidate_ids)
            attempted_stage2_reassessments.update(candidate_ids)
            stage2_started = time.perf_counter()
            results = _run_stage2_reassessment_batch(
                store,
                candidates_batch,
                data_dir=data_dir,
                max_history_sectors=max_history_sectors,
                concurrency=stage2_reassessment_concurrency,
                status_callback=status_callback,
                publish_lock=stage3_publish_lock,
            )
            elapsed = add_timing("stage2_seconds", stage2_started)
            if any(
                result.get("status") in {"Needs More Data", "unchanged"}
                for result in results
            ):
                timings["stage2_needs_more_data_seconds"] += elapsed
            unchanged_in_batch = 0
            for result in results:
                result.update(
                    {
                        "queue_kind": "reassessment",
                        "queue_status": "Needs More Data",
                        "queue_reason": "stage2_top_window_no_data_replacement",
                    }
                )
                stage2_results.append(result)
                result_status = str(result.get("status") or "error")
                if result_status == "unchanged":
                    unchanged_in_batch += 1
                    stage2_reassessment_telemetry["unchanged_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                    stage2_reassessment_telemetry["fallback_unchanged_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                elif result_status == "error":
                    stage2_reassessment_telemetry["error_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                elif result.get("publish_version") is not None:
                    stage2_reassessment_telemetry["published_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
                if result_status == "Inconclusive":
                    stage2_reassessment_telemetry["inconclusive_candidate_ids"].append(
                        str(result["candidate_id"])
                    )
            fallback_attempts += len(results)
            stage2_reassessment_telemetry["fallback_attempts"] = fallback_attempts
            stage2_reassessments_used += len(results)
            # A changed/evaluated candidate consumed its replacement slot. An
            # unchanged fallback keeps the slot open and causes the next
            # ranked candidate to be tried.
            replacement_slots = replacement_slots - len(results) + unchanged_in_batch
            for result in results:
                if result.get("status") == "Inconclusive":
                    request_stage2_replacement()

        remaining_queue = _stage2_needs_data_queue(
            active_candidates(store),
            threshold=stage2_reassessment_threshold,
        )
        remaining_ids = [str(candidate["candidate_id"]) for candidate in remaining_queue]
        stage2_reassessment_telemetry["remaining_queue_count"] = len(remaining_queue)
        stage2_reassessment_telemetry["remaining_queue_candidate_ids"] = remaining_ids[:stage2_reassessment_threshold]
        return processed

    def bootstrap_stage2_gate_cohort() -> int:
        """Fill a short Stage 2 cohort without starting new Stage 1 work.

        A strict gate must not deadlock when Stage 2 has fewer than 100 rows,
        and legacy Inconclusive rows need a downstream replacement opportunity
        before they can fall below the gate window. Existing eligible Stage 1
        Promising candidates can therefore be assessed into Stage 2 while the
        gate is closed. Their released Stage 1 slots intentionally remain
        empty until normal operations resume.
        """

        nonlocal stage2_gate_bootstrap_used
        budget = max(
            0,
            int(
                os.getenv(
                    "STAGE2_GATE_BOOTSTRAP_PER_CYCLE",
                    str(max(stage2_reassessment_concurrency, 4)),
                )
            ),
        )
        processed = 0
        while processed < budget:
            gate_now = _stage2_waterfall_gate_snapshot(
                active_candidates(store),
                threshold=stage2_waterfall_gate_threshold,
            )
            if gate_now["open"]:
                break
            candidates_now = [
                candidate
                for candidate in promotable_candidates(
                    store,
                    stage="stage1",
                    threshold=stage1_threshold,
                )
                if str(candidate["candidate_id"]) not in attempted_stage1
            ]
            if not candidates_now:
                break
            candidate = candidates_now[0]
            candidate_id = str(candidate["candidate_id"])
            attempted_stage1.add(candidate_id)
            if status_callback:
                status_callback(
                    "processing_stage2",
                    {
                        "candidate_id": candidate_id,
                        "reason": "stage2_waterfall_gate_bootstrap",
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
                    "queue_kind": "gate_bootstrap",
                    "queue_status": "Promising",
                    "queue_reason": "stage2_gate_cohort_bootstrap",
                }
            )
            elapsed = add_timing("stage2_seconds", stage2_started)
            if result.get("status") == "Needs More Data":
                timings["stage2_needs_more_data_seconds"] += elapsed
            stage2_results.append(result)
            processed += 1
            stage2_gate_bootstrap_used += 1
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

    def run_manual_migration(request: dict[str, Any]) -> dict[str, Any]:
        """Apply one bounded operator request through normal publish paths."""

        request_id = str(request.get("request_id") or "")
        from_stage = str(request.get("from_stage") or "")
        to_stage = str(request.get("to_stage") or "")
        requested_count = max(1, min(100, int(request.get("requested_count") or 1)))
        result: dict[str, Any] = {
            "request_id": request_id,
            "from_stage": from_stage,
            "to_stage": to_stage,
            "requested_count": requested_count,
            "selected_count": 0,
            "completed_count": 0,
            "candidate_ids": [],
            "status": "completed",
        }
        if (from_stage, to_stage) == ("universe", "stage1"):
            intake = fill_stage1(
                requested_count,
                intake_reason="manual_universe_stage1_migration",
            )
            completed_count = int(intake.get("evaluated_count") or intake.get("selected_count") or 0)
            result["selected_count"] = int(intake.get("selected_count") or completed_count)
            result["completed_count"] = completed_count
            if intake.get("status") == "error":
                result["status"] = "failed"
                result["message"] = intake.get("message") or "Stage 1 intake failed."
            else:
                result["message"] = f"Evaluated {completed_count} new target{'' if completed_count == 1 else 's'} in Stage 1."
            return result

        if (from_stage, to_stage) == ("stage1", "stage2"):
            selected = promotable_candidates(
                store,
                stage="stage1",
                threshold=max(stage1_threshold, requested_count),
            )[:requested_count]
            result["candidate_ids"] = [str(candidate["candidate_id"]) for candidate in selected]
            result["selected_count"] = len(selected)
            if selected:
                migrated = _run_stage2_manual_batch(
                    store,
                    selected,
                    data_dir=data_dir,
                    max_history_sectors=max_history_sectors,
                    concurrency=max_workers,
                    status_callback=status_callback,
                    publish_lock=stage3_publish_lock,
                )
                stage2_results.extend(
                    {
                        **item,
                        "queue_kind": "manual_handoff",
                        "queue_status": "Promising",
                        "queue_reason": "operator_requested_stage1_to_stage2_migration",
                    }
                    for item in migrated
                )
                completed = [item for item in migrated if item.get("publish_version") is not None]
                result["completed_count"] = len(completed)
                if completed:
                    replacement = fill_stage1(
                        len(completed),
                        intake_reason="manual_stage2_migration",
                    )
                    if replacement.get("status") != "idle":
                        stage1_requests.append(replacement)
                if len(completed) < len(selected):
                    result["status"] = "failed"
                    result["message"] = "Some selected candidates could not be evaluated; inspect the worker cycle for per-candidate errors."
                else:
                    result["message"] = f"Migrated {len(completed)} Stage 1 candidates to Stage 2."
            else:
                result["message"] = "No eligible Promising Stage 1 candidates were available."
            return result

        if (from_stage, to_stage) == ("stage2", "stage3"):
            selected = sorted(
                (
                    candidate
                    for candidate in active_candidates(store)
                    if candidate["current_stage"] == "stage2"
                    and candidate["status"] == "Promising"
                    and candidate["promotion_eligible"]
                    and candidate["rank_stage"] is not None
                ),
                key=lambda item: (int(item["rank_stage"]), str(item["candidate_id"])),
            )[:requested_count]
            result["candidate_ids"] = [str(candidate["candidate_id"]) for candidate in selected]
            result["selected_count"] = len(selected)
            if selected:
                migrated = _run_stage3_reassessment_batch(
                    store,
                    selected,
                    data_dir=data_dir,
                    max_history_sectors=max_history_sectors,
                    concurrency=stage3_reassessment_concurrency,
                    status_callback=status_callback,
                    publish_lock=stage3_publish_lock,
                )
                for item in migrated:
                    item.update(
                        {
                            "queue_kind": "manual_handoff",
                            "queue_status": "Promising",
                            "queue_reason": "operator_requested_stage2_to_stage3_migration",
                        }
                    )
                    stage3_results.append(item)
                    if item.get("publish_version") is not None:
                        check_catalog("stage3")
                completed = [item for item in migrated if item.get("publish_version") is not None]
                result["completed_count"] = len(completed)
                attempted_stage2.update(str(item["candidate_id"]) for item in completed)
                attempted_stage3.update(str(item["candidate_id"]) for item in completed)
                for _ in completed:
                    replacement = request_stage2_replacement()
                    if replacement is None:
                        break
                if len(completed) < len(selected):
                    result["status"] = "failed"
                    result["message"] = "Some selected candidates could not be evaluated; inspect the worker cycle for per-candidate errors."
                else:
                    result["message"] = f"Migrated {len(completed)} Stage 2 candidates to Stage 3."
            else:
                result["message"] = "No eligible Promising Stage 2 candidates were available."
            return result

        result["status"] = "failed"
        result["message"] = "Unsupported manual migration request."
        return result

    for migration in manual_migrations or []:
        try:
            manual_migration_results.append(run_manual_migration(migration))
        except Exception as exc:
            manual_migration_results.append(
                {
                    "request_id": migration.get("request_id"),
                    "from_stage": migration.get("from_stage"),
                    "to_stage": migration.get("to_stage"),
                    "requested_count": migration.get("requested_count"),
                    "selected_count": 0,
                    "completed_count": 0,
                    "candidate_ids": [],
                    "status": "failed",
                    "message": "Manual migration failed before completion.",
                    "error": repr(exc),
                }
            )

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
    stage3_steady_state_intakes = 0
    reassessment_budget = max(1, int(os.getenv("STAGE3_REASSESSMENTS_PER_CYCLE", "4")))
    reassessment_candidates: list[dict[str, Any]] = []
    # Stage 2 is the release valve for the entire waterfall. Reassess its
    # priority window first, then optionally bootstrap a missing cohort from
    # already-promising Stage 1 rows. Stage 1 intake and all Stage 3 work stay
    # paused until the ranked Stage 2 window is fully Promising.
    if waterfall_ready:
        run_stage2_reassessment_queue()
        stage2_gate_snapshot = _stage2_waterfall_gate_snapshot(
            active_candidates(store),
            threshold=stage2_waterfall_gate_threshold,
        )
        if (
            not stage2_gate_snapshot["open"]
            and stage2_gate_snapshot["top_count"] < stage2_waterfall_gate_threshold
        ):
            bootstrap_stage2_gate_cohort()
            stage2_gate_snapshot = _stage2_waterfall_gate_snapshot(
                active_candidates(store),
                threshold=stage2_waterfall_gate_threshold,
            )
        stage2_gate_open = bool(stage2_gate_snapshot["open"])
    else:
        stage2_gate_snapshot = {
            **stage2_gate_snapshot,
            "open": False,
            "reason": "waiting_for_stage1_baseline",
            "message": "Stage 2's release gate will be evaluated after the initial Stage 1 baseline is ready.",
            "paused_stages": ["stage1", "stage3"],
        }

    if waterfall_ready and not stage2_gate_open:
        stage3_intake = {
            "status": "paused_by_stage2_gate",
            "batch_id": None,
            "queued_count": 0,
            "pending": [],
        }

    if waterfall_ready and stage2_gate_open:
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
        current_stage3_candidates = active_candidates(store)
        needs_data_queue = _stage3_needs_data_queue(current_stage3_candidates)
        prime_rank_queue = _stage3_prime_rank_reassessment_queue(
            store,
            current_stage3_candidates,
        )
        reassessment_candidates: list[dict[str, Any]] = []
        reassessment_kinds: dict[str, str] = {}
        seen_reassessment_ids: set[str] = set()
        for candidate in sorted(
            [
                *needs_data_queue,
                *prime_rank_queue,
            ],
            key=lambda item: (
                int(item["rank_stage"]),
                0 if item.get("_stage3_prime_rank_movement_sequence") else 1,
                str(item["candidate_id"]),
            ),
        ):
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in attempted_stage3 or candidate_id in seen_reassessment_ids:
                continue
            seen_reassessment_ids.add(candidate_id)
            reassessment_candidates.append(candidate)
            reassessment_kinds[candidate_id] = (
                "prime_rank_movement"
                if candidate.get("_stage3_prime_rank_movement_sequence")
                else "needs_more_data"
            )
            if len(reassessment_candidates) >= reassessment_budget:
                break
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
            candidate_id = str(result["candidate_id"])
            reassessment_kind = reassessment_kinds.get(candidate_id, "needs_more_data")
            if (
                reassessment_kind == "prime_rank_movement"
                and result.get("status") == "unchanged"
            ):
                movement_sequence = next(
                    int(candidate["_stage3_prime_rank_movement_sequence"])
                    for candidate in reassessment_candidates
                    if str(candidate["candidate_id"]) == candidate_id
                )
                result["reassessment_attempted_version"] = _record_stage3_prime_reassessment_attempt(
                    store,
                    candidate_id=candidate_id,
                    movement_sequence=movement_sequence,
                )
            if reassessment_kind == "prime_rank_movement":
                queue_status = "Prime Candidate"
                queue_reason = "stage3_prime_rank_movement_reassessment"
            else:
                queue_status = "Needs More Data"
                queue_reason = "stage3_ranked_needs_more_data_reassessment"
            result.update(
                {
                    "queue_kind": "reassessment",
                    "queue_status": queue_status,
                    "queue_reason": queue_reason,
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
        steady_state_source = "__stage3_steady_state_intake__"
        steady_state_intake_budget = max(
            0,
            int(os.getenv("STAGE3_STEADY_INTAKE_PER_CYCLE", "1")),
        )
        steady_state_intakes_used = 0

        def enqueue_sources(values: list[str]) -> None:
            for value in values:
                if value not in dispatch_seen and value not in dispatch_sources:
                    dispatch_sources.append(value)

        enqueue_sources([str(item["candidate_id"]) for item in stage3_replacement_debt(store)])
        enqueue_sources(resolved_sources)
        dispatch_budget = max(1, int(os.getenv("STAGE3_DISPATCHES_PER_CYCLE", "4")))
        for _ in range(dispatch_budget):
            enqueue_sources([str(item["candidate_id"]) for item in stage3_replacement_debt(store)])
            # Initial intake is intentionally durable and one-time, but Stage 2
            # candidates can become Promising later as the waterfall runs. Do
            # not leave those candidates stranded just because Stage 3 has no
            # unresolved replacement-debt event at this exact moment. Admit a
            # small steady-state slice; replacement debt still has priority.
            if (
                not dispatch_sources
                and steady_state_intakes_used < steady_state_intake_budget
            ):
                candidate = select_stage2_candidate()
                if candidate is not None:
                    dispatch_sources.append(steady_state_source)
                    steady_state_intakes_used += 1
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
                dispatch_source_candidate_id=(
                    None
                    if source_candidate_id == steady_state_source
                    else source_candidate_id
                ),
                publish_lock=stage3_publish_lock,
            )
            result.update(
                {
                    "queue_kind": "handoff",
                    "queue_status": "Promising",
                    "queue_reason": (
                        "stage3_steady_state_intake"
                        if source_candidate_id == steady_state_source
                        else "stage3_called_stage2_promising_candidate"
                    ),
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
        stage3_steady_state_intakes = steady_state_intakes_used

    # Stage 3 is the final assessment stage, so a catalog match here is a
    # legitimate Stage 3 removal and is retained in the identified/replay
    # history at that stage.
    continuous_stage1_intake: dict[str, Any] = {
        "status": "idle",
        "message": (
            "Continuous Stage 1 intake is disabled"
            if stage1_continuous_intake_per_cycle == 0
            else "Paused until the top Stage 2 gate window is fully Promising"
            if not stage2_gate_open
            else "Waiting for the initial Stage 1 baseline"
        ),
        "selected_count": 0,
        "evaluated_count": 0,
        "intake_reason": "continuous_stage1_intake",
    }
    if (
        baseline_ready_at_cycle_start
        and stage1_continuous_intake_per_cycle
        and stage2_gate_open
    ):
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
        continuous_stage1_intake["scheduled_after"] = "stage2_top_window_reassessment_and_stage3_dispatch"
        continuous_stage1_intake["stage2_priority_calls_completed"] = stage2_reassessments_used

    if stage2_gate_open:
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
            "steady_state_intakes": stage3_steady_state_intakes,
        },
        "stage3_reassessment": {
            "concurrency": stage3_reassessment_concurrency,
            "requested_count": reassessment_budget if waterfall_ready and stage2_gate_open else 0,
            "prime_rank_movement_requested_count": sum(
                1
                for candidate in reassessment_candidates
                if candidate.get("_stage3_prime_rank_movement_sequence")
            ),
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
            "evaluated_count": stage2_reassessments_used if waterfall_ready else 0,
            "concurrency": stage2_reassessment_concurrency,
            "priority": "first_stage2_job_budget_in_cycle",
            "waterfall_gate": stage2_gate_snapshot,
            "waterfall_gate_open": stage2_gate_open,
            "gate_bootstrap_evaluated_count": stage2_gate_bootstrap_used,
            **stage2_reassessment_telemetry,
        },
        "stage2_waterfall_gate": stage2_gate_snapshot,
        "manual_migrations": manual_migration_results,
        "timings": {key: round(value, 3) for key, value in timings.items()},
        "catalog": catalog,
    }


__all__ = ["run_live_cycle"]
