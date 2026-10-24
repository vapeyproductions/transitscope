"""Candidate lifecycle, promotion policy, and known-match publication."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from storage.duckdb_store import DuckDBStore


CATALOG_MODEL_VERSION = "exoplanet-archive-match-v1"
OVERALL_RANKING_VERSION = "overall-ranking-v2-score-first"
RANKING_VIEWS = ("overall", "stage1", "stage2", "stage3")
STATUS_PRIORITY = {
    "Promising": 3,
    "Needs More Data": 2,
    "Inconclusive": 1,
    "Known Match": 0,
    "Duplicate Signal": 0,
}


def release_alias_siblings(
    candidates: Sequence[Mapping[str, Any]],
    *,
    primary_candidate_id: str,
    outcome: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Keep siblings pending until they pass an explicit independence test.

    This function name is retained for callers written before sibling
    independence existed. A primary becoming Inconclusive or Known Match is
    not evidence that a sibling is distinct, so it must never silently release
    the sibling into the next stage. The priority resolver performs the actual
    test when the sibling reaches the Stage 1 threshold.
    """

    del primary_candidate_id, outcome
    return [dict(candidate) for candidate in candidates], []


def active_candidates(store: DuckDBStore) -> list[dict[str, Any]]:
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, target_id, current_stage, status,
                   overall_score, stage_score, rank_overall, rank_stage,
                   activity, rank_delta, movement_kind, last_evaluated_at,
                   known_match_id, active, processing_slot_occupied,
                   signal_group_id, signal_resolution, alias_of_candidate_id,
                   promotion_eligible
            FROM candidates
            WHERE active = true
            """
        ).fetchall()
    return [_candidate_row(row) for row in rows]


def all_candidates(store: DuckDBStore) -> list[dict[str, Any]]:
    """Return active and historical candidate rows for lifecycle decisions."""

    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, target_id, current_stage, status,
                   overall_score, stage_score, rank_overall, rank_stage,
                   activity, rank_delta, movement_kind, last_evaluated_at,
                   known_match_id, active, processing_slot_occupied,
                   signal_group_id, signal_resolution, alias_of_candidate_id,
                   promotion_eligible
            FROM candidates
            """
        ).fetchall()
    return [_candidate_row(row) for row in rows]


def candidate_match_inputs(
    store: DuckDBStore,
    candidate_ids: Sequence[str] | None = None,
    *,
    primary_only: bool = True,
) -> list[dict[str, Any]]:
    """Return matchable candidates with coordinates and latest period evidence.

    Known-object matching is deliberately limited to the canonical primary
    signal for each star. Sibling signals are retained for independence
    analysis, but they must never independently trigger a catalog lookup.
    """

    store.initialize()
    with store.connect() as connection:
        params: list[Any] = []
        where = "WHERE c.active = true"
        if primary_only:
            where += " AND c.signal_resolution = 'primary'"
        if candidate_ids:
            placeholders = ", ".join("?" for _ in candidate_ids)
            where += f" AND c.candidate_id IN ({placeholders})"
            params.extend(candidate_ids)
        rows = connection.execute(
            f"""
            SELECT c.candidate_id, c.target_id, c.current_stage, c.status,
                   c.signal_resolution,
                   tm.ra_deg, tm.dec_deg
            FROM candidates c
            LEFT JOIN target_manifest tm ON tm.target_id = c.target_id
            {where}
            ORDER BY c.rank_overall NULLS LAST, c.candidate_id
            """,
            params,
        ).fetchall()
        inputs: list[dict[str, Any]] = []
        for row in rows:
            evaluation = connection.execute(
                """
                SELECT feature_json
                FROM evaluations
                WHERE candidate_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [row[0]],
            ).fetchone()
            features = json.loads(evaluation[0]) if evaluation else {}
            period = _first_float(
                features,
                "period_days",
                "stage1_period_days",
            )
            inputs.append(
                {
                    "candidate_id": row[0],
                    "target_id": row[1],
                    "current_stage": row[2],
                    "status": row[3],
                    "signal_resolution": row[4],
                    "ra_deg": row[5],
                    "dec_deg": row[6],
                    "period_days": period,
                    "features": features,
                }
            )
    return inputs


def stage3_replacement_debt(store: DuckDBStore) -> list[dict[str, Any]]:
    """Return Stage 3 resolutions that have not yet called the next candidate.

    This is the durable waterfall ledger.  It intentionally derives the
    backlog from pipeline events instead of an in-memory counter, so a worker
    restart cannot forget that a Prime Candidate, Inconclusive result, or
    Stage 3 catalog match still needs to trigger the next Stage 2 handoff.
    """

    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, stage, event_type, sequence_number, payload_json, occurred_at
            FROM pipeline_events
            WHERE event_type IN (
                'stage3_evaluated',
                'known_match',
                'stage3_replacement_called'
            )
            ORDER BY sequence_number ASC, occurred_at ASC
            """
        ).fetchall()

    resolutions: dict[str, dict[str, Any]] = {}
    dispatched_at: dict[str, int] = {}
    for candidate_id, stage, event_type, sequence_number, payload_json, occurred_at in rows:
        candidate_key = str(candidate_id or "")
        if not candidate_key:
            continue
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        sequence = int(sequence_number or 0)
        if event_type == "stage3_replacement_called":
            dispatched_at[candidate_key] = max(dispatched_at.get(candidate_key, 0), sequence)
            continue
        if event_type == "stage3_evaluated":
            status = str(payload.get("classification") or "")
            if status not in {"Promising", "Inconclusive"}:
                resolutions.pop(candidate_key, None)
                continue
            resolutions[candidate_key] = {
                "candidate_id": candidate_key,
                "status": status,
                "sequence_number": sequence,
                "occurred_at": occurred_at,
            }
        elif (
            event_type == "known_match"
            and str(stage or "") == "stage3"
            and str(payload.get("active", True)).lower() == "false"
        ):
            resolutions[candidate_key] = {
                "candidate_id": candidate_key,
                "status": "Known Match",
                "sequence_number": sequence,
                "occurred_at": occurred_at,
            }

    return sorted(
        (
            resolution
            for candidate_id, resolution in resolutions.items()
            if int(resolution["sequence_number"]) > dispatched_at.get(candidate_id, 0)
        ),
        key=lambda item: (int(item["sequence_number"]), str(item["candidate_id"])),
    )


def promotable_candidates(
    store: DuckDBStore,
    *,
    stage: str,
    threshold: int | None = None,
    capacity: int | None = None,
) -> list[dict[str, Any]]:
    """Select Promising candidates inside a stage's priority threshold.

    The threshold is a ranking window, not a stage capacity. It does not
    limit how many candidates a stage may retain. Only Promising candidates
    can leave a stage; Needs More Data and Inconclusive candidates remain
    visible in that stage's ranking. ``capacity`` is accepted as a backwards-
    compatible keyword for callers from the earlier capacity-based policy.
    """

    if stage not in {"stage1", "stage2"}:
        raise ValueError("promotion selection supports stage1 and stage2 only")
    if threshold is None:
        threshold = capacity
    if threshold is None or threshold < 1:
        raise ValueError("threshold must be positive")
    rows = [
        candidate
        for candidate in active_candidates(store)
        if candidate["current_stage"] == stage
        and candidate["status"] == "Promising"
        and candidate["processing_slot_occupied"]
        and candidate["promotion_eligible"]
        and candidate["rank_stage"] is not None
    ]
    # The threshold applies to eligible candidates, not sibling rows waiting
    # for their independent residual/holdout assessment. A pending sibling is
    # visible only until that assessment is triggered.
    return sorted(rows, key=lambda candidate: (candidate["rank_stage"], candidate["candidate_id"]))[:threshold]


def resolve_priority_aliases(
    store: DuckDBStore,
    *,
    data_dir: Path,
    threshold: int,
    processed_target_count: int,
) -> dict[str, Any]:
    """Assess top-priority sibling signals and publish their lifecycle result.

    Pending siblings remain visible only until they enter the Stage 1 priority
    window. At that point they leave the active ranking while the independent
    test runs. A passing sibling is reintroduced as an independent Stage 1
    candidate; a failed sibling is retained only as historical evidence.
    """

    from pipeline.independence import (
        MODEL_VERSION,
        assess_candidate_from_store,
        latest_independence_source,
        latest_stage1_features,
    )

    if threshold < 1:
        raise ValueError("threshold must be positive")

    candidates = all_candidates(store)
    candidate_by_id = {str(candidate["candidate_id"]): candidate for candidate in candidates}
    work: list[dict[str, Any]] = []
    for candidate in candidates:
        resolution = str(candidate.get("signal_resolution") or "")
        if candidate.get("current_stage") != "stage1":
            continue
        if resolution not in {"pending_alias_review", "pending_independence_review"}:
            continue
        if candidate.get("status") not in {"Promising", "Needs More Data"}:
            continue
        if resolution == "pending_alias_review":
            if not candidate.get("active") or candidate.get("rank_stage") is None:
                continue
            if int(candidate["rank_stage"]) > threshold:
                continue
        else:
            # An insufficient test is not rerun against the same normalized
            # product. A later Stage 1 refresh can change that path and make
            # the sibling eligible for another assessment.
            features = latest_stage1_features(store, str(candidate["candidate_id"]))
            source_path = str(features.get("normalized_path") or "")
            if source_path and source_path == (latest_independence_source(store, str(candidate["candidate_id"])) or ""):
                continue
        work.append(candidate)

    if not work:
        return {"status": "idle", "assessed": 0, "passed": 0, "failed": 0, "needs_more_data": 0}

    now = datetime.now(timezone.utc)
    assessments: list[dict[str, Any]] = []
    for candidate in work:
        candidate_id = str(candidate["candidate_id"])
        primary_id = str(candidate.get("alias_of_candidate_id") or "")
        if not primary_id or primary_id not in candidate_by_id:
            from pipeline.independence import IndependenceResult

            result = IndependenceResult(
                "Needs More Data",
                0.0,
                "insufficient",
                {},
                "The primary signal needed for residual comparison is not available yet.",
            )
            sibling_features = latest_stage1_features(store, candidate_id)
            primary_features: dict[str, Any] = {}
        else:
            result, sibling_features, primary_features = assess_candidate_from_store(
                store,
                data_dir=data_dir,
                sibling_candidate_id=candidate_id,
                primary_candidate_id=primary_id,
            )
        assessments.append(
            {
                "candidate": candidate,
                "primary_id": primary_id,
                "result": result,
                "sibling_features": sibling_features,
                "primary_features": primary_features,
                "evaluated_at": now,
            }
        )

    updated_by_id = {str(candidate["candidate_id"]): dict(candidate) for candidate in candidates}
    for assessment in assessments:
        candidate = updated_by_id[str(assessment["candidate"]["candidate_id"])]
        result = assessment["result"]
        candidate["last_evaluated_at"] = assessment["evaluated_at"]
        candidate["rank_delta"] = 0
        candidate["movement_kind"] = "unchanged"
        if result.status == "Promising":
            candidate.update(
                {
                    "active": True,
                    "status": "Promising",
                    "activity": "Released for independent review",
                    "signal_resolution": "released_for_independent_review",
                    "alias_of_candidate_id": None,
                    "promotion_eligible": True,
                    "processing_slot_occupied": True,
                }
            )
        elif result.status == "Duplicate Signal":
            candidate.update(
                {
                    "active": False,
                    "status": "Duplicate Signal",
                    "activity": "Excluded as duplicate signal",
                    "signal_resolution": "duplicate_signal",
                    "promotion_eligible": False,
                    "processing_slot_occupied": False,
                    "rank_overall": None,
                    "rank_stage": None,
                }
            )
        else:
            candidate.update(
                {
                    "active": False,
                    "status": "Needs More Data",
                    "activity": "Awaiting more data for independence test",
                    "signal_resolution": "pending_independence_review",
                    "promotion_eligible": False,
                    "processing_slot_occupied": False,
                    "rank_overall": None,
                    "rank_stage": None,
                }
            )

    updated_candidates = list(updated_by_id.values())
    active_after = [candidate for candidate in updated_candidates if candidate.get("active")]
    overall_pool = sorted(active_after, key=overall_sort_key)
    stage1_pool = _sort_pool(
        [candidate for candidate in active_after if candidate.get("current_stage") == "stage1"],
        score_key="stage_score",
    )
    overall_ranks = {candidate["candidate_id"]: index for index, candidate in enumerate(overall_pool, 1)}
    stage1_ranks = {candidate["candidate_id"]: index for index, candidate in enumerate(stage1_pool, 1)}
    previous_by_id = {str(candidate["candidate_id"]): candidate for candidate in candidates}
    candidate_updates: list[dict[str, Any]] = []
    for candidate in updated_candidates:
        candidate_id = str(candidate["candidate_id"])
        if candidate.get("active"):
            new_overall_rank = overall_ranks[candidate_id]
            new_stage_rank = stage1_ranks.get(candidate_id, candidate.get("rank_stage"))
            old_rank = previous_by_id[candidate_id].get("rank_overall")
            rank_delta, movement = movement_for_ranks(old_rank, new_overall_rank)
            candidate["rank_overall"] = new_overall_rank
            candidate["rank_stage"] = new_stage_rank
            candidate["rank_delta"] = rank_delta
            candidate["movement_kind"] = movement
        elif candidate_id in {str(item["candidate"]["candidate_id"]) for item in assessments}:
            candidate["rank_overall"] = None
            candidate["rank_stage"] = None
            candidate["rank_delta"] = -(previous_by_id[candidate_id].get("rank_overall") or 0)
            candidate["movement_kind"] = "removed"
        else:
            continue
        candidate_updates.append(
            {
                "candidate_id": candidate_id,
                "current_stage": candidate["current_stage"],
                "rank_overall": candidate.get("rank_overall"),
                "rank_stage": candidate.get("rank_stage"),
                "rank_delta": candidate.get("rank_delta", 0),
                "movement_kind": candidate.get("movement_kind", "unchanged"),
                "active": candidate.get("active", True),
                "activity": candidate.get("activity", "No current activity"),
                "status": candidate.get("status"),
                "overall_score": candidate.get("overall_score"),
                "stage_score": candidate.get("stage_score"),
                "last_evaluated_at": candidate.get("last_evaluated_at"),
                "known_match_id": candidate.get("known_match_id"),
                "processing_slot_occupied": candidate.get("processing_slot_occupied"),
                "signal_group_id": candidate.get("signal_group_id"),
                "signal_resolution": candidate.get("signal_resolution"),
                "alias_of_candidate_id": candidate.get("alias_of_candidate_id"),
                "promotion_eligible": candidate.get("promotion_eligible"),
            }
        )

    version = (store.latest_published_version() or 0) + 1
    evaluations: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for assessment in assessments:
        candidate = assessment["candidate"]
        candidate_id = str(candidate["candidate_id"])
        result = assessment["result"]
        suffix = re.sub(r"[^a-zA-Z0-9]+", "", candidate_id)[-12:]
        evaluations.append(
            {
                "evaluation_id": f"signal-independence-{candidate_id}-{version}",
                "candidate_id": candidate_id,
                "stage": "signal_independence",
                "model_version": MODEL_VERSION,
                "data_window_start": None,
                "data_window_end": None,
                "input_product_ids": [
                    value
                    for value in (
                        assessment["sibling_features"].get("product_id")
                        or assessment["sibling_features"].get("normalized_path"),
                        f"primary:{assessment['primary_id']}",
                    )
                    if value
                ],
                "feature_json": {
                    **result.metrics,
                    "source_normalized_path": result.metrics.get("source_normalized_path"),
                    "primary_candidate_id": assessment["primary_id"],
                    "reason": result.reason,
                },
                "metric_json": result.metrics,
                "classification": result.status,
                "data_sufficiency": result.data_sufficiency,
            }
        )
        events.append(
            {
                "event_id": f"signal-independence-{candidate_id}-{suffix}-{version}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": "stage1",
                "event_type": "signal_independence_assessed",
                "payload_json": {
                    "classification": result.status,
                    "score": result.score,
                    "reason": result.reason,
                    "primary_candidate_id": assessment["primary_id"],
                    "signal_resolution": updated_by_id[candidate_id]["signal_resolution"],
                    "metrics": result.metrics,
                },
                "occurred_at": assessment["evaluated_at"],
                "model_run_id": f"signal-independence-{version}",
            }
        )

    store.publish_batch(
        version=version,
        evaluations=evaluations,
        events=events,
        snapshots=[
            {
                "snapshot_id": f"stage1-independence-snapshot-{version}",
                "view": "stage1",
                "sequence_number": version,
                "occurred_at": now,
                "candidate_order_json": [candidate["candidate_id"] for candidate in stage1_pool],
                "active_candidate_count": len(stage1_pool),
                "processed_target_count": processed_target_count,
            },
            {
                "snapshot_id": f"overall-independence-snapshot-{version}",
                "view": "overall",
                "sequence_number": version,
                "occurred_at": now,
                "candidate_order_json": [candidate["candidate_id"] for candidate in overall_pool],
                "active_candidate_count": len(overall_pool),
                "processed_target_count": processed_target_count,
            },
        ],
        candidate_updates=candidate_updates,
        note="Independent-signal validation for Stage 1 siblings",
    )
    counts = {
        "passed": sum(assessment["result"].status == "Promising" for assessment in assessments),
        "failed": sum(assessment["result"].status == "Duplicate Signal" for assessment in assessments),
        "needs_more_data": sum(assessment["result"].status == "Needs More Data" for assessment in assessments),
    }
    return {"status": "complete", "assessed": len(assessments), **counts, "publish_version": version}


def movement_for_ranks(old_rank: int | None, new_rank: int) -> tuple[int, str]:
    """Return the user-facing movement token for an Overall rank change."""

    if old_rank is None:
        return 0, "new"
    delta = int(old_rank) - new_rank
    if delta >= 3:
        return delta, "rapid_rise"
    if delta > 0:
        return delta, "rising"
    if delta <= -3:
        return delta, "rapid_fall"
    if delta < 0:
        return delta, "falling"
    if new_rank <= 3 and int(old_rank) <= 3:
        return 0, "sustained_top"
    return 0, "unchanged"


def publish_known_matches(
    store: DuckDBStore,
    matches: Sequence[Mapping[str, Any]],
    *,
    catalog_cache_path: str | None,
    top_n: int = 100,
    ranking_version: str = OVERALL_RANKING_VERSION,
) -> int:
    """Tombstone matched candidates and publish all four updated ranking views."""

    if top_n < 1:
        raise ValueError("top_n must be positive")
    candidates = active_candidates(store)
    released_alias_ids: list[str] = []
    released_alias_primary_ids: dict[str, str] = {}
    for match in matches:
        primary_candidate_id = str(match.get("candidate_id"))
        primary_aliases = {
            str(candidate["candidate_id"]): str(candidate.get("alias_of_candidate_id"))
            for candidate in candidates
            if candidate.get("alias_of_candidate_id") == primary_candidate_id
        }
        candidates, released = release_alias_siblings(
            candidates,
            primary_candidate_id=primary_candidate_id,
            outcome="Known Match",
        )
        released_alias_ids.extend(released)
        released_alias_primary_ids.update(primary_aliases)
    by_id = {candidate["candidate_id"]: candidate for candidate in candidates}
    matched_by_id = {
        str(match["candidate_id"]): dict(match)
        for match in matches
        if str(match.get("candidate_id")) in by_id
    }
    if not matched_by_id:
        raise ValueError("no active candidate matches were provided")

    active_after = [candidate for candidate in candidates if candidate["candidate_id"] not in matched_by_id]
    overall_pool = _sort_pool(active_after, score_key="overall_score")
    stage_pools = {
        stage: _sort_pool(
            [candidate for candidate in active_after if candidate["current_stage"] == stage],
            score_key="stage_score",
        )
        for stage in ("stage1", "stage2", "stage3")
    }
    overall_ranks = {candidate["candidate_id"]: index for index, candidate in enumerate(overall_pool, 1)}
    stage_ranks = {
        stage: {candidate["candidate_id"]: index for index, candidate in enumerate(pool, 1)}
        for stage, pool in stage_pools.items()
    }
    previous_by_id = by_id
    updates: list[dict[str, Any]] = []
    for candidate in active_after:
        candidate_id = candidate["candidate_id"]
        new_rank = overall_ranks[candidate_id]
        old_rank = candidate["rank_overall"]
        delta, movement = movement_for_ranks(old_rank, new_rank)
        updates.append(
            {
                "candidate_id": candidate_id,
                "current_stage": candidate["current_stage"],
                "rank_overall": new_rank,
                "rank_stage": stage_ranks[candidate["current_stage"]].get(candidate_id),
                "rank_delta": delta,
                "movement_kind": movement,
                "active": True,
                "activity": candidate["activity"],
                "status": candidate["status"],
                "overall_score": candidate["overall_score"],
                "stage_score": candidate["stage_score"],
                "last_evaluated_at": candidate["last_evaluated_at"],
                "known_match_id": candidate["known_match_id"],
                "processing_slot_occupied": candidate["processing_slot_occupied"],
                "signal_group_id": candidate.get("signal_group_id"),
                "signal_resolution": candidate.get("signal_resolution"),
                "alias_of_candidate_id": candidate.get("alias_of_candidate_id"),
                "promotion_eligible": candidate.get("promotion_eligible"),
            }
        )
    for candidate_id, match in matched_by_id.items():
        candidate = previous_by_id[candidate_id]
        updates.append(
            {
                "candidate_id": candidate_id,
                "current_stage": candidate["current_stage"],
                "rank_overall": None,
                "rank_stage": None,
                "rank_delta": -(candidate["rank_overall"] or 0),
                "movement_kind": "removed",
                "active": False,
                "activity": "Compared with known objects",
                "status": "Known Match",
                "overall_score": candidate["overall_score"],
                "stage_score": candidate["stage_score"],
                # Catalog cross-referencing is a lifecycle decision, not a
                # model evaluation. Keep the timestamp tied to the latest
                # actual stage assessment.
                "last_evaluated_at": candidate["last_evaluated_at"],
                "known_match_id": str(match["catalog_id"]),
                "processing_slot_occupied": False,
                "signal_group_id": candidate.get("signal_group_id"),
                "signal_resolution": candidate.get("signal_resolution"),
                "alias_of_candidate_id": candidate.get("alias_of_candidate_id"),
                "promotion_eligible": False,
            }
        )

    version = (store.latest_published_version() or 0) + 1
    occurred_at = datetime.now(timezone.utc)
    evaluations = []
    events = []
    for candidate_id, match in matched_by_id.items():
        candidate = previous_by_id[candidate_id]
        suffix = re.sub(r"[^a-zA-Z0-9]+", "", candidate_id)[-12:]
        evaluations.append(
            {
                "evaluation_id": f"catalog-evaluation-{candidate_id}-{version}",
                "candidate_id": candidate_id,
                "stage": "catalog",
                "model_version": CATALOG_MODEL_VERSION,
                "data_window_start": None,
                "data_window_end": None,
                "input_product_ids": [catalog_cache_path] if catalog_cache_path else [],
                "feature_json": {
                    "current_stage": candidate["current_stage"],
                    "match_kind": match["match_kind"],
                    "catalog_record": match["record"],
                    "tolerances": match["tolerances"],
                    "coordinate_separation_deg": match["coordinate_separation_deg"],
                    "period_delta_fraction": match["period_delta_fraction"],
                },
                "metric_json": {
                    "match_score": match["match_score"],
                    "catalog_id": match["catalog_id"],
                    "classification": "Known Match",
                },
                "classification": "Known Match",
                "data_sufficiency": "sufficient",
            }
        )
        events.append(
            {
                "event_id": f"known-match-{candidate_id}-{version}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": candidate["current_stage"],
                "event_type": "known_match",
                "payload_json": {
                    "catalog_id": match["catalog_id"],
                    "object_name": match.get("object_name"),
                    "match_kind": match["match_kind"],
                    "match_score": match["match_score"],
                    "tolerances": match["tolerances"],
                    "active": False,
                },
                "occurred_at": occurred_at,
                "model_run_id": f"catalog-match-{suffix}-{version}",
            }
        )
    for released_id in released_alias_ids:
        events.append(
            {
                "event_id": f"signal-alias-released-{released_id}-{version}",
                "sequence_number": version,
                "candidate_id": released_id,
                "stage": previous_by_id[released_id]["current_stage"],
                "event_type": "signal_alias_released",
                "payload_json": {
                    "reason": "primary_signal_was_known_match",
                    "primary_candidate_id": released_alias_primary_ids.get(released_id),
                    "resolution": "released_for_independent_review",
                },
                "occurred_at": occurred_at,
                "model_run_id": f"catalog-match-alias-release-{version}",
            }
        )

    processed_count = processed_target_count(store)
    snapshots = []
    for view in RANKING_VIEWS:
        pool = overall_pool if view == "overall" else stage_pools[view]
        snapshots.append(
            {
                "snapshot_id": f"{view}-snapshot-{version}",
                "view": view,
                "sequence_number": version,
                "occurred_at": occurred_at,
                "candidate_order_json": [candidate["candidate_id"] for candidate in pool[:top_n]],
                "active_candidate_count": len(pool),
                "processed_target_count": processed_count,
            }
        )

    store.publish_batch(
        version=version,
        note=f"Known-object cross-reference using {len(matched_by_id)} catalog match(es)",
        ingest_runs=[
            {
                "run_id": f"catalog-ingest-{version}",
                "provider": "NASA Exoplanet Archive",
                "query_config_json": {
                    "stage": "catalog",
                    "cache_path": catalog_cache_path,
                    "ranking_model_version": ranking_version,
                },
                "started_at": occurred_at,
                "finished_at": occurred_at,
                "status": "complete",
                "targets_seen": len(matched_by_id),
                "products_downloaded": 0,
                "products_failed": 0,
                "error_json": None,
            }
        ],
        evaluations=evaluations,
        events=events,
        snapshots=snapshots,
        candidate_updates=updates,
    )
    return version


def repair_legacy_history(store: DuckDBStore) -> dict[str, int]:
    """Repair pre-stage-gating history without changing scientific results.

    Older live cycles sent Stage 1 refreshes through the replacement-slot
    path. That emitted ``stage1_released`` events even though the candidate
    remained in Stage 1. Remove only those explicitly marked refresh events,
    infer the one-time handoff events from the first downstream evaluation,
    and restore candidate timestamps from real evaluation events.

    The operation is idempotent and runs during worker startup so the durable
    live database is corrected once without rerunning any model.
    """

    store.initialize()
    with store.connect() as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            legacy_release_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM pipeline_events
                    WHERE event_type = 'stage1_released'
                      AND payload_json LIKE '%replacement_slot_opened%'
                    """
                ).fetchone()[0]
            )
            connection.execute(
                """
                DELETE FROM pipeline_events
                WHERE event_type = 'stage1_released'
                  AND payload_json LIKE '%replacement_slot_opened%'
                """
            )

            inferred_release_count = 0
            for source_event_type, released_stage, next_stage in (
                ("stage2_evaluated", "stage1", "stage2"),
                ("stage3_evaluated", "stage2", "stage3"),
            ):
                source_rows = connection.execute(
                    """
                    SELECT candidate_id, sequence_number, occurred_at, model_run_id
                    FROM pipeline_events
                    WHERE event_type = ? AND candidate_id IS NOT NULL
                    ORDER BY candidate_id, sequence_number, occurred_at
                    """,
                    [source_event_type],
                ).fetchall()
                first_by_candidate: dict[str, tuple[object, object, object]] = {}
                for candidate_id, sequence_number, occurred_at, model_run_id in source_rows:
                    first_by_candidate.setdefault(
                        str(candidate_id),
                        (sequence_number, occurred_at, model_run_id),
                    )
                for candidate_id, (sequence_number, occurred_at, model_run_id) in first_by_candidate.items():
                    already_exists = connection.execute(
                        """
                        SELECT 1
                        FROM pipeline_events
                        WHERE candidate_id = ? AND event_type = ?
                        LIMIT 1
                        """,
                        [candidate_id, f"{released_stage}_released"],
                    ).fetchone()
                    if already_exists:
                        continue
                    suffix = re.sub(r"[^a-zA-Z0-9]+", "", candidate_id)[-24:]
                    connection.execute(
                        """
                        INSERT INTO pipeline_events (
                            event_id, sequence_number, candidate_id, stage,
                            event_type, payload_json, occurred_at, model_run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            f"legacy-repair-{released_stage}-released-{suffix}-{sequence_number}",
                            sequence_number,
                            candidate_id,
                            released_stage,
                            f"{released_stage}_released",
                            json.dumps(
                                {
                                    "reason": f"advanced_to_{next_stage}",
                                    "next_stage": next_stage,
                                    "legacy_repaired": True,
                                    "inferred_from": source_event_type,
                                    "source_model_run_id": model_run_id,
                                },
                                sort_keys=True,
                            ),
                            occurred_at,
                            f"history-repair-{released_stage}-{suffix}-{sequence_number}",
                        ],
                    )
                    inferred_release_count += 1

            connection.execute(
                """
                UPDATE candidates AS candidate
                SET last_evaluated_at = COALESCE(
                    (
                        SELECT MAX(event.occurred_at)
                        FROM pipeline_events AS event
                        WHERE event.candidate_id = candidate.candidate_id
                          AND event.event_type IN (
                              'stage1_evaluated', 'stage2_evaluated', 'stage3_evaluated'
                          )
                    ),
                    (
                        SELECT MAX(evaluation.created_at)
                        FROM evaluations AS evaluation
                        WHERE evaluation.candidate_id = candidate.candidate_id
                          AND evaluation.stage IN ('stage1', 'stage2', 'stage3')
                    ),
                    candidate.last_evaluated_at
                )
                """
            )
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
            raise
    return {
        "legacy_release_events_removed": legacy_release_count,
        "handoff_events_inferred": inferred_release_count,
    }


def _candidate_row(row: Sequence[Any]) -> dict[str, Any]:
    return {
        "candidate_id": row[0],
        "target_id": row[1],
        "current_stage": row[2],
        "status": row[3],
        "overall_score": row[4],
        "stage_score": row[5],
        "rank_overall": row[6],
        "rank_stage": row[7],
        "activity": row[8],
        "rank_delta": row[9],
        "movement_kind": row[10],
        "last_evaluated_at": row[11],
        "known_match_id": row[12],
        "active": bool(row[13]),
        "processing_slot_occupied": bool(row[14]),
        "signal_group_id": row[15],
        "signal_resolution": row[16] or "independent",
        "alias_of_candidate_id": row[17],
        "promotion_eligible": bool(row[18]),
    }


def _sort_pool(candidates: Sequence[Mapping[str, Any]], *, score_key: str) -> list[dict[str, Any]]:
    if score_key == "overall_score":
        return sorted(
            (dict(candidate) for candidate in candidates),
            key=overall_sort_key,
        )
    return sorted(
        (dict(candidate) for candidate in candidates),
        key=lambda candidate: (
            -STATUS_PRIORITY.get(str(candidate.get("status")), 0),
            -float(candidate.get(score_key) or 0.0),
            str(candidate["candidate_id"]),
        ),
    )


def overall_sort_key(candidate: Mapping[str, Any]) -> tuple[float, int, str]:
    """Rank the overall board by accumulated evidence across all completed stages.

    Status remains a tie-breaker so an Inconclusive result is not preferred over
    an otherwise equal Promising result, but the latest stage label cannot erase
    strong evidence collected earlier in the pipeline.
    """

    return (
        -float(candidate.get("overall_score") or 0.0),
        -STATUS_PRIORITY.get(str(candidate.get("status")), 0),
        str(candidate["candidate_id"]),
    )


def _first_float(features: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        try:
            value = float(features.get(key))
        except (TypeError, ValueError):
            continue
        if value == value:
            return value
    return None


def processed_target_count(store: DuckDBStore) -> int:
    store.initialize()
    with store.connect() as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM target_manifest WHERE manifest_status = 'processed'"
            ).fetchone()[0]
        )


__all__ = [
    "CATALOG_MODEL_VERSION",
    "OVERALL_RANKING_VERSION",
    "STATUS_PRIORITY",
    "all_candidates",
    "active_candidates",
    "candidate_match_inputs",
    "stage3_replacement_debt",
    "movement_for_ranks",
    "overall_sort_key",
    "processed_target_count",
    "promotable_candidates",
    "publish_known_matches",
    "release_alias_siblings",
    "resolve_priority_aliases",
    "repair_legacy_history",
]
