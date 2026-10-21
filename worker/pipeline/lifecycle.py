"""Candidate lifecycle, promotion policy, and known-match publication."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from storage.duckdb_store import DuckDBStore


CATALOG_MODEL_VERSION = "exoplanet-archive-match-v1"
OVERALL_RANKING_VERSION = "overall-ranking-v1"
RANKING_VIEWS = ("overall", "stage1", "stage2", "stage3")
STATUS_PRIORITY = {
    "Promising": 3,
    "Needs More Data": 2,
    "Inconclusive": 1,
    "Known Match": 0,
}


def active_candidates(store: DuckDBStore) -> list[dict[str, Any]]:
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, target_id, current_stage, status,
                   overall_score, stage_score, rank_overall, rank_stage,
                   activity, rank_delta, movement_kind, last_evaluated_at,
                   known_match_id, active, processing_slot_occupied
            FROM candidates
            WHERE active = true
            """
        ).fetchall()
    return [_candidate_row(row) for row in rows]


def candidate_match_inputs(
    store: DuckDBStore,
    candidate_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Return active candidates with coordinates and latest period evidence."""

    store.initialize()
    with store.connect() as connection:
        params: list[Any] = []
        where = "WHERE c.active = true"
        if candidate_ids:
            placeholders = ", ".join("?" for _ in candidate_ids)
            where += f" AND c.candidate_id IN ({placeholders})"
            params.extend(candidate_ids)
        rows = connection.execute(
            f"""
            SELECT c.candidate_id, c.target_id, c.current_stage, c.status,
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
                    "ra_deg": row[4],
                    "dec_deg": row[5],
                    "period_days": period,
                    "features": features,
                }
            )
    return inputs


def promotable_candidates(
    store: DuckDBStore,
    *,
    stage: str,
    capacity: int,
) -> list[dict[str, Any]]:
    """Select the operational top-X for the next stage.

    Advancement is capacity/rank driven and status-gated. Only Promising
    candidates can leave a stage; Needs More Data and Inconclusive candidates
    remain visible in that stage's ranking.
    """

    if stage not in {"stage1", "stage2"}:
        raise ValueError("promotion selection supports stage1 and stage2 only")
    if capacity < 1:
        raise ValueError("capacity must be positive")
    rows = [
        candidate
        for candidate in active_candidates(store)
        if candidate["current_stage"] == stage
        and candidate["status"] == "Promising"
        and candidate["processing_slot_occupied"]
        and candidate["rank_stage"] is not None
        and int(candidate["rank_stage"]) <= capacity
    ]
    return sorted(rows, key=lambda candidate: (candidate["rank_stage"], candidate["candidate_id"]))


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
                "last_evaluated_at": datetime.now(timezone.utc),
                "known_match_id": str(match["catalog_id"]),
                "processing_slot_occupied": False,
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
    }


def _sort_pool(candidates: Sequence[Mapping[str, Any]], *, score_key: str) -> list[dict[str, Any]]:
    return sorted(
        (dict(candidate) for candidate in candidates),
        key=lambda candidate: (
            -STATUS_PRIORITY.get(str(candidate.get("status")), 0),
            -float(candidate.get(score_key) or 0.0),
            str(candidate["candidate_id"]),
        ),
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
    "active_candidates",
    "candidate_match_inputs",
    "movement_for_ranks",
    "processed_target_count",
    "promotable_candidates",
    "publish_known_matches",
]
