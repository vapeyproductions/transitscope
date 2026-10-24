from __future__ import annotations

import json
import os
import fcntl
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np


VIEW_STAGE = {"stage1": "stage1", "stage2": "stage2", "stage3": "stage3"}
RANKING_VIEWS = {"overall", "stage1", "stage2", "stage3"}
STATUS_PRIORITY = {
    "Promising": 3,
    "Needs More Data": 2,
    "Inconclusive": 1,
    "Known Match": 0,
}
RANKING_STATUS_FILTERS = set(STATUS_PRIORITY) | {"Duplicate Signal"}
RANKING_SORTS = {"rank", "last_assessed_desc", "last_assessed_asc"}


def isoformat(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def ranking_rationale(status: str, evidence_score: object) -> str:
    score = "unscored" if evidence_score is None else f"evidence score {float(evidence_score):.1f}"
    if status == "Promising":
        return f"Highest operational priority; {score} with sufficient supporting data."
    if status == "Needs More Data":
        return "Higher investigation priority than Inconclusive; the model needs more coverage before judging the signal."
    if status == "Inconclusive":
        return f"Sufficient data was examined, but the signal remains unresolved; {score}."
    if status == "Known Match":
        return "Mapped to a catalog object and removed from active rankings."
    return f"Ranked by the current pipeline policy; {score}."


class _LockedReadConnection:
    """DuckDB connection proxy that releases the shared sidecar lock on close."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, lock_file: Any) -> None:
        self._connection = connection
        self._lock_file = lock_file
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def __enter__(self) -> "_LockedReadConnection":
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self._connection.__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        finally:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()


class ReadOnlyStore:
    """Read-only projection of the worker's published DuckDB state."""

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir or Path(os.getenv("DATA_DIR", "/app/data"))
        self.database = self.data_dir / "transitscope.duckdb"

    def connect(self) -> Any | None:
        if not self.database.exists():
            return None
        # DuckDB uses a process-level file lock even for a read-only connection.
        # Coordinate with the worker through a sidecar lock so an API poll
        # cannot interrupt a publish transaction.
        lock_path = self.database.with_suffix(self.database.suffix + ".lock")
        lock_file = lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_SH)
            connection = duckdb.connect(str(self.database), read_only=True)
        except Exception:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            raise
        return _LockedReadConnection(connection, lock_file)

    def latest_version(self, connection: duckdb.DuckDBPyConnection) -> int | None:
        row = connection.execute(
            """
            SELECT version
            FROM publish_versions
            WHERE status = 'published'
            ORDER BY version DESC
            LIMIT 1
            """
        ).fetchone()
        return int(row[0]) if row else None

    @staticmethod
    def _signal_indices(
        connection: duckdb.DuckDBPyConnection,
        candidate_ids: list[str],
    ) -> dict[str, int]:
        """Return durable Stage 1 signal ordinals without breaking legacy stores.

        Older replay fixtures and databases may not have ``candidate_signals``
        yet. The UI can still use its candidate-id fallback in that case.
        """

        if not candidate_ids:
            return {}
        placeholders = ", ".join("?" for _ in candidate_ids)
        try:
            rows = connection.execute(
                f"""
                SELECT candidate_id, MAX(signal_index) AS signal_index
                FROM candidate_signals
                WHERE candidate_id IN ({placeholders})
                  AND signal_index IS NOT NULL
                GROUP BY candidate_id
                """,
                candidate_ids,
            ).fetchall()
        except Exception:
            return {}
        return {
            str(candidate_id): int(signal_index)
            for candidate_id, signal_index in rows
            if signal_index is not None
        }

    def status(self, worker: dict[str, Any]) -> dict[str, Any]:
        response = {
            "service": "api",
            "environment": os.getenv("APP_ENV", "unknown"),
            "data_version": None,
            "sequence_number": None,
            "processed_target_count": 0,
            "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
            "active_candidate_count": 0,
            "last_ingest_at": None,
            "worker": worker,
        }
        connection = self.connect()
        if connection is None:
            return response
        try:
            version = self.latest_version(connection)
            response["data_version"] = version
            response["sequence_number"] = version
            response["processed_target_count"] = int(
                connection.execute(
                    "SELECT COUNT(*) FROM target_manifest WHERE manifest_status = 'processed'"
                ).fetchone()[0]
            )
            response["active_candidate_count"] = int(
                connection.execute(
                    "SELECT COUNT(*) FROM candidates WHERE active = true"
                ).fetchone()[0]
            )
            ingest = connection.execute(
                """
                SELECT finished_at
                FROM ingest_runs
                ORDER BY finished_at DESC NULLS LAST
                LIMIT 1
                """
            ).fetchone()
            if ingest and ingest[0]:
                response["last_ingest_at"] = isoformat(ingest[0])
            return response
        finally:
            connection.close()

    def rankings(
        self,
        view: str,
        limit: int,
        page: int = 1,
        *,
        stage_filter: str | None = None,
        status_filter: str | None = None,
        sort: str = "rank",
    ) -> dict[str, Any]:
        if view not in RANKING_VIEWS:
            raise ValueError(f"unsupported ranking view: {view}")
        if stage_filter is not None and stage_filter not in VIEW_STAGE.values():
            raise ValueError(f"unsupported stage filter: {stage_filter}")
        if view != "overall" and stage_filter is not None:
            raise ValueError("stage filters are only available on the overall ranking")
        if status_filter is not None and status_filter not in RANKING_STATUS_FILTERS:
            raise ValueError(f"unsupported status filter: {status_filter}")
        if sort not in RANKING_SORTS:
            raise ValueError(f"unsupported ranking sort: {sort}")
        display_cap = 500
        page = max(1, int(page))
        offset = (page - 1) * limit
        if offset >= display_cap:
            offset = display_cap
        effective_limit = max(0, min(limit, display_cap - offset))
        connection = self.connect()
        empty = {
            "view": view,
            "data_version": None,
            "sequence_number": None,
            "occurred_at": None,
            "processed_target_count": 0,
            "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
            "page": page,
            "page_size": limit,
            "display_cap": display_cap,
            "total_ranked_count": 0,
            "total_star_count": 0,
            "page_count": 0,
            "rows": [],
            "removed": [],
        }
        if connection is None:
            return empty
        try:
            version = self.latest_version(connection)
            snapshot = None
            if version is not None:
                snapshot = connection.execute(
                    """
                    SELECT sequence_number, occurred_at, active_candidate_count,
                           processed_target_count, candidate_order_json
                    FROM ranking_snapshots
                    WHERE view = ?
                    ORDER BY sequence_number DESC
                    LIMIT 1
                    """,
                    [view],
                ).fetchone()
            view_stage = VIEW_STAGE.get(view)
            selected_stage = stage_filter or view_stage
            where_clauses = ["c.active = true"]
            where_params: list[Any] = []
            if selected_stage:
                where_clauses.append("c.current_stage = ?")
                where_params.append(selected_stage)
            if status_filter:
                where_clauses.append("c.status = ?")
                where_params.append(status_filter)
            where_sql = " AND ".join(where_clauses)

            if selected_stage:
                total_ranked_count = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM candidates AS c WHERE {where_sql}",
                        where_params,
                    ).fetchone()[0]
                )
            else:
                total_ranked_count = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM candidates AS c WHERE {where_sql}",
                        where_params,
                    ).fetchone()[0]
                )
            if selected_stage:
                total_star_count = int(
                    connection.execute(
                        f"SELECT COUNT(DISTINCT c.target_id) FROM candidates AS c WHERE {where_sql}",
                        where_params,
                    ).fetchone()[0]
                )
            else:
                total_star_count = int(
                    connection.execute(
                        f"SELECT COUNT(DISTINCT c.target_id) FROM candidates AS c WHERE {where_sql}",
                        where_params,
                    ).fetchone()[0]
                )

            if sort == "last_assessed_desc":
                order_sql = "last_evaluated_at DESC NULLS LAST"
            elif sort == "last_assessed_asc":
                order_sql = "last_evaluated_at ASC NULLS LAST"
            elif view_stage:
                order_sql = "c.rank_stage NULLS LAST, c.stage_score DESC NULLS LAST"
            else:
                order_sql = "c.rank_overall NULLS LAST, c.overall_score DESC NULLS LAST"
            rows = connection.execute(
                f"""
                    SELECT c.candidate_id, c.target_id, c.current_stage, c.status,
                           c.overall_score, c.stage_score, c.rank_overall, c.rank_stage,
                           c.activity, c.rank_delta, c.movement_kind,
                           COALESCE(
                               (
                                   SELECT MAX(event.occurred_at)
                                   FROM pipeline_events AS event
                                   WHERE event.candidate_id = c.candidate_id
                                     AND event.event_type IN ('stage1_evaluated', 'stage2_evaluated', 'stage3_evaluated')
                               ),
                               c.last_evaluated_at
                           ) AS last_evaluated_at,
                           c.signal_group_id, c.signal_resolution,
                           c.alias_of_candidate_id, c.promotion_eligible
                    FROM candidates AS c
                    WHERE {where_sql}
                    ORDER BY {order_sql}, c.candidate_id ASC
                    LIMIT ? OFFSET ?
                    """,
                [*where_params, effective_limit, offset],
            ).fetchall()

            signal_indices = self._signal_indices(
                connection,
                [str(row[0]) for row in rows],
            )
            projected_rows = []
            for index, row in enumerate(rows, start=1):
                rank = row[6] if view == "overall" else row[7]
                projected_rows.append(
                    {
                        "rank": int(rank or index),
                        "candidate_id": row[0],
                        "target_id": row[1],
                        "stage": row[2],
                        "status": row[3],
                        "evidence_score": row[5] if row[5] is not None else row[4],
                        "overall_score": row[4],
                        "ranking_rationale": ranking_rationale(
                            row[3], row[5] if row[5] is not None else row[4]
                        ),
                        "activity": row[8],
                        "rank_delta": int(row[9] or 0),
                        "movement": row[10] or "unchanged",
                        "last_evaluated_at": isoformat(row[11]),
                        "signal_group_id": row[12],
                        "signal_resolution": row[13] or "independent",
                        "alias_of_candidate_id": row[14],
                        "promotion_eligible": bool(row[15]),
                        "signal_index": signal_indices.get(str(row[0])),
                    }
                )

            processed_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM target_manifest WHERE manifest_status = 'processed'"
                ).fetchone()[0]
            )

            response = {
                "view": view,
                "data_version": version,
                "sequence_number": snapshot[0] if snapshot else version,
                "occurred_at": isoformat(snapshot[1]) if snapshot else None,
                "processed_target_count": processed_count,
                "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
                "page": page,
                "page_size": limit,
                "display_cap": display_cap,
                "total_ranked_count": total_ranked_count,
                "total_star_count": total_star_count,
                "page_count": (min(total_ranked_count, display_cap) + limit - 1) // limit,
                "stage_filter": stage_filter,
                "status_filter": status_filter,
                "sort": sort,
                "rows": projected_rows,
                "removed": [],
            }
            return response
        finally:
            connection.close()

    def identified(self, limit: int) -> dict[str, Any]:
        """Return known catalog matches with their persisted pipeline performance."""

        empty = {"data_version": None, "count": 0, "rows": []}
        connection = self.connect()
        if connection is None:
            return empty
        try:
            candidates = connection.execute(
                """
                SELECT candidate_id, target_id, current_stage, overall_score,
                       activity, last_evaluated_at, known_match_id
                FROM candidates
                WHERE status = 'Known Match'
                ORDER BY last_evaluated_at DESC NULLS LAST, candidate_id ASC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            signal_indices = self._signal_indices(
                connection,
                [str(candidate[0]) for candidate in candidates],
            )

            rows: list[dict[str, Any]] = []
            for candidate in candidates:
                evaluations = connection.execute(
                    """
                    SELECT stage, model_version, feature_json, metric_json,
                           classification, data_sufficiency, created_at
                    FROM evaluations
                    WHERE candidate_id = ?
                    ORDER BY created_at ASC
                    """,
                    [candidate[0]],
                ).fetchall()

                performance: dict[str, dict[str, Any]] = {}
                catalog_evaluation: dict[str, Any] | None = None
                for evaluation in evaluations:
                    try:
                        features = json.loads(evaluation[2]) if evaluation[2] else {}
                    except (TypeError, ValueError, json.JSONDecodeError):
                        features = {}
                    try:
                        metrics = json.loads(evaluation[3]) if evaluation[3] else {}
                    except (TypeError, ValueError, json.JSONDecodeError):
                        metrics = {}
                    if not isinstance(features, dict):
                        features = {}
                    if not isinstance(metrics, dict):
                        metrics = {}

                    if evaluation[0] == "catalog":
                        catalog_evaluation = {
                            "features": features,
                            "metrics": metrics,
                            "created_at": evaluation[6],
                        }
                        continue

                    holdout = features.get("holdout_metrics") or metrics.get("holdout_metrics")
                    if not isinstance(holdout, dict):
                        holdout = {}
                    evidence_score = metrics.get("evidence_score")
                    if evidence_score is None:
                        evidence_score = metrics.get("stage_score")
                    performance[evaluation[0]] = {
                        "status": evaluation[4],
                        "evidence_score": evidence_score,
                        "model_version": evaluation[1],
                        "data_sufficiency": evaluation[5],
                        "evaluated_at": isoformat(evaluation[6]),
                        "coverage_days": features.get("coverage_days"),
                        "observed_event_count": features.get(
                            "observed_event_count", features.get("observed_transit_count")
                        ),
                        "holdout_f1": holdout.get("f1"),
                        "model_lift": features.get("model_vs_baseline_f1_delta"),
                        "metrics": metrics,
                    }

                catalog_features = (catalog_evaluation or {}).get("features", {})
                catalog_metrics = (catalog_evaluation or {}).get("metrics", {})
                catalog_record = catalog_features.get("catalog_record")
                if not isinstance(catalog_record, dict):
                    catalog_record = {}
                catalog_id = (
                    catalog_metrics.get("catalog_id")
                    or catalog_record.get("record_id")
                    or candidate[6]
                )
                identified_stage = catalog_features.get("current_stage") or candidate[2]
                rows.append(
                    {
                        "candidate_id": candidate[0],
                        "target_id": candidate[1],
                        "signal_index": signal_indices.get(str(candidate[0])),
                        "identified_stage": identified_stage,
                        "identified_at": isoformat(
                            (catalog_evaluation or {}).get("created_at")
                            or candidate[5]
                        ),
                        "known_match_id": catalog_id,
                        "overall_score": candidate[3],
                        "activity": candidate[4],
                        "identified_as": {
                            "object_name": catalog_record.get("object_name") or catalog_id,
                            "catalog_id": catalog_id,
                            "table": catalog_record.get("table"),
                            "disposition": catalog_record.get("disposition"),
                            "match_kind": catalog_features.get("match_kind"),
                            "match_score": catalog_metrics.get("match_score"),
                            "period_days": catalog_record.get("period_days"),
                            "coordinate_separation_deg": catalog_features.get(
                                "coordinate_separation_deg"
                            ),
                            "period_delta_fraction": catalog_features.get(
                                "period_delta_fraction"
                            ),
                        },
                        "performance": performance,
                    }
                )

            return {
                "data_version": self.latest_version(connection),
                "count": len(rows),
                "rows": rows,
            }
        finally:
            connection.close()

    def replay_meta(self, view: str) -> dict[str, Any]:
        """Return the saved timeline bounds without touching providers or models."""

        if view not in RANKING_VIEWS:
            raise ValueError(f"unsupported ranking view: {view}")
        connection = self.connect()
        empty = {
            "view": view,
            "data_version": None,
            "initial_sequence": 0,
            "first_sequence": None,
            "last_sequence": 0,
            "snapshot_count": 0,
            "available_sequences": [],
            "first_occurred_at": None,
            "last_occurred_at": None,
        }
        if connection is None:
            return empty
        try:
            rows = connection.execute(
                """
                SELECT sequence_number, occurred_at
                FROM ranking_snapshots
                WHERE view = ?
                ORDER BY sequence_number ASC
                """,
                [view],
            ).fetchall()
            if not rows:
                return {**empty, "data_version": self.latest_version(connection)}
            return {
                "view": view,
                "data_version": self.latest_version(connection),
                "initial_sequence": 0,
                "first_sequence": int(rows[0][0]),
                "last_sequence": int(rows[-1][0]),
                "snapshot_count": len(rows),
                "available_sequences": [int(row[0]) for row in rows],
                "first_occurred_at": isoformat(rows[0][1]),
                "last_occurred_at": isoformat(rows[-1][1]),
            }
        finally:
            connection.close()

    def replay_snapshot(self, view: str, sequence: int, limit: int, page: int = 1) -> dict[str, Any]:
        """Project a previously persisted ranking snapshot at or before a playhead."""

        if view not in RANKING_VIEWS:
            raise ValueError(f"unsupported ranking view: {view}")
        connection = self.connect()
        empty = {
            "view": view,
            "data_version": None,
            "sequence_number": 0,
            "occurred_at": None,
            "processed_target_count": 0,
            "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
            "page": page,
            "page_size": limit,
            "display_cap": 500,
            "total_ranked_count": 0,
            "total_star_count": 0,
            "page_count": 0,
            "rows": [],
            "removed": [],
        }
        if connection is None or sequence <= 0:
            if connection is not None:
                connection.close()
            return empty
        try:
            snapshot = connection.execute(
                """
                SELECT sequence_number, occurred_at, active_candidate_count,
                       processed_target_count, candidate_order_json
                FROM ranking_snapshots
                WHERE view = ? AND sequence_number <= ?
                ORDER BY sequence_number DESC
                LIMIT 1
                """,
                [view, sequence],
            ).fetchone()
            if not snapshot:
                return {**empty, "data_version": self.latest_version(connection)}

            candidate_ids = json.loads(snapshot[4])
            if not isinstance(candidate_ids, list):
                candidate_ids = []
            total_star_count = 0
            if candidate_ids:
                placeholders = ", ".join("?" for _ in candidate_ids)
                total_star_count = int(
                    connection.execute(
                        f"SELECT COUNT(DISTINCT target_id) FROM candidates WHERE candidate_id IN ({placeholders})",
                        [str(candidate_id) for candidate_id in candidate_ids],
                    ).fetchone()[0]
                )
            display_cap = 500
            offset = max(0, (page - 1) * limit)
            rows = self._replay_rows(
                connection,
                view,
                int(snapshot[0]),
                candidate_ids,
                min(limit, max(0, display_cap - offset)),
                offset,
            )
            removed = self._replay_removals(connection, view, int(snapshot[0]))
            historical_processed = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(processed_target_count), 0)
                    FROM ranking_snapshots
                    WHERE view = 'stage1' AND sequence_number <= ?
                    """,
                    [int(snapshot[0])],
                ).fetchone()[0]
            )
            return {
                "view": view,
                "data_version": self.latest_version(connection),
                "sequence_number": int(snapshot[0]),
                "occurred_at": isoformat(snapshot[1]),
                "processed_target_count": historical_processed,
                "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
                "page": page,
                "page_size": limit,
                "display_cap": display_cap,
                "total_ranked_count": int(snapshot[2]),
                "total_star_count": total_star_count,
                "page_count": (min(int(snapshot[2]), display_cap) + limit - 1) // limit,
                "rows": rows,
                "removed": removed,
            }
        finally:
            connection.close()

    def replay_events(self, view: str, start_sequence: int, end_sequence: int) -> dict[str, Any]:
        """Return persisted lifecycle events for a view's independent replay stream."""

        if view not in RANKING_VIEWS:
            raise ValueError(f"unsupported ranking view: {view}")
        connection = self.connect()
        if connection is None or end_sequence < start_sequence:
            if connection is not None:
                connection.close()
            return {"view": view, "start_sequence": start_sequence, "end_sequence": end_sequence, "events": []}
        try:
            stage_filter = VIEW_STAGE.get(view)
            if stage_filter:
                rows = connection.execute(
                    """
                    SELECT sequence_number, candidate_id, stage, event_type,
                           payload_json, occurred_at, model_run_id
                    FROM pipeline_events
                    WHERE stage = ? AND sequence_number BETWEEN ? AND ?
                    ORDER BY sequence_number ASC, occurred_at ASC
                    """,
                    [stage_filter, start_sequence, end_sequence],
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT sequence_number, candidate_id, stage, event_type,
                           payload_json, occurred_at, model_run_id
                    FROM pipeline_events
                    WHERE sequence_number BETWEEN ? AND ?
                    ORDER BY sequence_number ASC, occurred_at ASC
                    """,
                    [start_sequence, end_sequence],
                ).fetchall()
            return {
                "view": view,
                "start_sequence": start_sequence,
                "end_sequence": end_sequence,
                "events": [
                    {
                        "sequence_number": int(row[0]),
                        "candidate_id": row[1],
                        "stage": row[2],
                        "event_type": row[3],
                        "payload": json.loads(row[4]),
                        "occurred_at": isoformat(row[5]),
                        "model_run_id": row[6],
                    }
                    for row in rows
                ],
            }
        finally:
            connection.close()

    def _replay_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        view: str,
        sequence: int,
        candidate_ids: list[object],
        limit: int,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        projected_rows: list[dict[str, Any]] = []
        signal_indices = self._signal_indices(
            connection,
            [str(candidate_id) for candidate_id in candidate_ids],
        )
        for index, raw_candidate_id in enumerate(candidate_ids[offset : offset + limit], start=offset + 1):
            candidate_id = str(raw_candidate_id)
            candidate = connection.execute(
                """
                SELECT candidate_id, target_id, current_stage, status,
                       overall_score, stage_score, rank_overall, rank_stage,
                       activity, rank_delta, movement_kind, last_evaluated_at,
                       signal_group_id, signal_resolution, alias_of_candidate_id,
                       promotion_eligible
                FROM candidates
                WHERE candidate_id = ?
                """,
                [candidate_id],
            ).fetchone()
            if not candidate:
                continue
            event = connection.execute(
                """
                SELECT stage, event_type, payload_json, occurred_at
                FROM pipeline_events
                WHERE candidate_id = ? AND sequence_number <= ?
                ORDER BY sequence_number DESC, occurred_at DESC
                LIMIT 1
                """,
                [candidate_id, sequence],
            ).fetchone()
            payload = json.loads(event[2]) if event else {}
            if not isinstance(payload, dict):
                payload = {}
            status = "Known Match" if event and event[1] == "known_match" else payload.get("classification", candidate[3])
            stage = VIEW_STAGE.get(view) or (event[0] if event and event[0] else candidate[2])
            event_features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
            signal_index = event_features.get("signal_index")
            if not isinstance(signal_index, int) or isinstance(signal_index, bool):
                signal_index = signal_indices.get(str(candidate_id))
            signal_resolution = event_features.get("signal_resolution", candidate[13])
            alias_of_candidate_id = event_features.get("alias_of_candidate_id", candidate[14])
            if event and event[1] == "signal_alias_released":
                signal_resolution = payload.get("resolution", "released_for_independent_review")
                alias_of_candidate_id = None
            evidence = payload.get("evidence_score")
            if evidence is None:
                evidence = candidate[5] if candidate[5] is not None else candidate[4]
            projected_rows.append(
                {
                    "rank": index,
                    "candidate_id": candidate[0],
                    "target_id": candidate[1],
                    "stage": stage,
                    "status": status,
                    "evidence_score": evidence,
                    "overall_score": candidate[4],
                    "ranking_rationale": ranking_rationale(status, evidence),
                    "activity": payload.get("activity", candidate[8]),
                    "rank_delta": int(candidate[9] or 0),
                    "movement": candidate[10] or "unchanged",
                    "last_evaluated_at": isoformat(event[3] if event else candidate[11]),
                    "signal_group_id": candidate[12],
                    "signal_resolution": signal_resolution or "independent",
                    "alias_of_candidate_id": alias_of_candidate_id,
                    "promotion_eligible": bool(candidate[15]),
                    "signal_index": signal_index,
                }
            )
        return projected_rows

    def _replay_removals(
        self,
        connection: duckdb.DuckDBPyConnection,
        view: str,
        sequence: int,
    ) -> list[dict[str, Any]]:
        """Project catalog removals that happened at this replay snapshot.

        A known match is deliberately absent from the active snapshot. Returning
        the persisted event separately lets the UI show the candidate's exit
        animation without pretending that it is still an active ranking row.
        """

        stage_filter = VIEW_STAGE.get(view)
        params: list[Any] = [sequence]
        stage_clause = ""
        if stage_filter:
            stage_clause = " AND event.stage = ?"
            params.append(stage_filter)
        events = connection.execute(
            f"""
            SELECT event.candidate_id, event.stage, event.payload_json,
                   event.occurred_at, candidate.target_id, candidate.known_match_id
            FROM pipeline_events AS event
            LEFT JOIN candidates AS candidate ON candidate.candidate_id = event.candidate_id
            WHERE event.event_type = 'known_match'
              AND event.sequence_number = ?{stage_clause}
            ORDER BY event.occurred_at ASC, event.candidate_id ASC
            """,
            params,
        ).fetchall()
        if not events:
            return []

        signal_indices = self._signal_indices(
            connection,
            [str(event[0]) for event in events],
        )

        previous_snapshot = connection.execute(
            """
            SELECT candidate_order_json
            FROM ranking_snapshots
            WHERE view = ? AND sequence_number < ?
            ORDER BY sequence_number DESC
            LIMIT 1
            """,
            [view, sequence],
        ).fetchone()
        previous_order: list[str] = []
        if previous_snapshot:
            try:
                decoded = json.loads(previous_snapshot[0])
                if isinstance(decoded, list):
                    previous_order = [str(item) for item in decoded]
            except (TypeError, ValueError, json.JSONDecodeError):
                previous_order = []

        removals: list[dict[str, Any]] = []
        for candidate_id, stage, payload_json, occurred_at, target_id, known_match_id in events:
            payload = json.loads(payload_json) if payload_json else {}
            if not isinstance(payload, dict):
                payload = {}
            features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
            signal_index = features.get("signal_index")
            if not isinstance(signal_index, int) or isinstance(signal_index, bool):
                signal_index = signal_indices.get(str(candidate_id))
            rank = previous_order.index(str(candidate_id)) + 1 if str(candidate_id) in previous_order else None
            catalog_id = payload.get("catalog_id") or known_match_id
            removals.append(
                {
                    "candidate_id": str(candidate_id),
                    "target_id": str(target_id or candidate_id),
                    "stage": stage,
                    "status": "Known Match",
                    "known_match_id": catalog_id,
                    "rank": rank,
                    "activity": "Mapped to known catalog object",
                    "removal_reason": "Already Known",
                    "ranking_rationale": ranking_rationale("Known Match", None),
                    "occurred_at": isoformat(occurred_at),
                    "signal_index": signal_index,
                }
            )
        return removals

    def candidate(self, candidate_id: str) -> dict[str, Any] | None:
        connection = self.connect()
        if connection is None:
            return None
        try:
            candidate = connection.execute(
                """
                SELECT c.candidate_id, c.target_id, c.current_stage, c.status,
                       c.overall_score, c.stage_score, c.rank_overall, c.rank_stage,
                       c.activity, c.rank_delta, c.movement_kind,
                       COALESCE(
                           (
                               SELECT MAX(event.occurred_at)
                               FROM pipeline_events AS event
                               WHERE event.candidate_id = c.candidate_id
                                 AND event.event_type IN ('stage1_evaluated', 'stage2_evaluated', 'stage3_evaluated')
                           ),
                           c.last_evaluated_at
                       ) AS last_evaluated_at,
                       c.known_match_id, c.active, c.signal_group_id,
                       c.signal_resolution, c.alias_of_candidate_id,
                       c.promotion_eligible
                FROM candidates AS c
                WHERE c.candidate_id = ?
                """,
                [candidate_id],
            ).fetchone()
            if not candidate:
                return None
            evaluation = connection.execute(
                """
                SELECT evaluation_id, model_version, input_product_ids,
                       feature_json, metric_json, classification,
                       data_sufficiency, created_at
                FROM evaluations
                WHERE candidate_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [candidate_id],
            ).fetchone()
            target = connection.execute(
                """
                SELECT provider_target_id, ra_deg, dec_deg, sector, cadence,
                       source_product_family, manifest_status
                FROM target_manifest
                WHERE target_id = ?
                """,
                [candidate[1]],
            ).fetchone()
            features = json.loads(evaluation[3]) if evaluation else {}
            metrics = json.loads(evaluation[4]) if evaluation else {}
            signal_index = features.get("signal_index") if isinstance(features, dict) else None
            if not isinstance(signal_index, int) or isinstance(signal_index, bool):
                signal_index = self._signal_indices(connection, [str(candidate[0])]).get(str(candidate[0]))
            return {
                "candidate_id": candidate[0],
                "target_id": candidate[1],
                "stage": candidate[2],
                "status": candidate[3],
                "evidence_score": candidate[5] if candidate[5] is not None else candidate[4],
                "ranking_rationale": ranking_rationale(
                    candidate[3], candidate[5] if candidate[5] is not None else candidate[4]
                ),
                "overall_score": candidate[4],
                "rank_overall": candidate[6],
                "rank_stage": candidate[7],
                "activity": candidate[8],
                "rank_delta": candidate[9] or 0,
                "movement": candidate[10] or "unchanged",
                "last_evaluated_at": isoformat(candidate[11]),
                "known_match_id": candidate[12],
                "active": bool(candidate[13]),
                "signal_group_id": candidate[14],
                "signal_resolution": candidate[15] or "independent",
                "alias_of_candidate_id": candidate[16],
                "promotion_eligible": bool(candidate[17]),
                "signal_index": signal_index,
                "target": {
                    "provider_target_id": target[0],
                    "ra_deg": target[1],
                    "dec_deg": target[2],
                    "sector": target[3],
                    "cadence": target[4],
                    "source_product_family": target[5],
                    "manifest_status": target[6],
                }
                if target
                else None,
                "evaluation": {
                    "evaluation_id": evaluation[0],
                    "model_version": evaluation[1],
                    "input_product_ids": json.loads(evaluation[2]),
                    "features": features,
                    "metrics": metrics,
                    "classification": evaluation[5],
                    "data_sufficiency": evaluation[6],
                    "created_at": isoformat(evaluation[7]),
                }
                if evaluation
                else None,
                "next_action": self._next_action(candidate[2], candidate[3]),
            }
        finally:
            connection.close()

    def light_curve(self, candidate_id: str, max_points: int = 1200) -> dict[str, Any] | None:
        detail = self.candidate(candidate_id)
        if detail is None or not detail.get("evaluation"):
            return None
        feature_path = detail["evaluation"]["features"].get("normalized_path")
        if not feature_path:
            return {"candidate_id": candidate_id, "points": [], "data_version": self._version()}
        path = Path(str(feature_path))
        if not path.exists():
            path = self.data_dir / "normalized" / path.name
        try:
            path = path.resolve()
            data_root = self.data_dir.resolve()
            if data_root not in path.parents:
                return None
            with np.load(path, allow_pickle=False) as arrays:
                time_values = np.asarray(arrays["time"], dtype=float)
                flux_values = np.asarray(arrays["normalized_flux"], dtype=float)
        except (OSError, KeyError, ValueError):
            return None
        indices = self._sample_indices(len(time_values), max_points)
        return {
            "candidate_id": candidate_id,
            "data_version": self._version(),
            "source": "normalized Stage 1 series",
            "points": [
                {"time": float(time_values[index]), "normalized_flux": float(flux_values[index])}
                for index in indices
            ],
        }

    def history(self, candidate_id: str) -> dict[str, Any] | None:
        if self.candidate(candidate_id) is None:
            return None
        connection = self.connect()
        if connection is None:
            return {"candidate_id": candidate_id, "data_version": None, "events": []}
        try:
            events = connection.execute(
                """
                SELECT sequence_number, stage, event_type, payload_json,
                       occurred_at, model_run_id
                FROM pipeline_events
                WHERE candidate_id = ?
                ORDER BY sequence_number ASC, occurred_at ASC,
                         CASE WHEN event_type IN ('stage1_released', 'stage2_released')
                              THEN 0 ELSE 1 END,
                         event_type ASC
                """,
                [candidate_id],
            ).fetchall()
            return {
                "candidate_id": candidate_id,
                "data_version": self.latest_version(connection),
                "events": [
                    {
                        "sequence_number": row[0],
                        "stage": row[1],
                        "event_type": row[2],
                        "payload": json.loads(row[3]),
                        "occurred_at": isoformat(row[4]),
                        "model_run_id": row[5],
                    }
                    for row in events
                ],
            }
        finally:
            connection.close()

    def monitoring(self, worker: dict[str, Any]) -> dict[str, Any]:
        """Build an operations view for the authenticated ML/pipeline dashboard.

        The worker owns execution. This projection only reads the published
        DuckDB state, so opening or refreshing the ML Ops tab cannot enqueue a
        provider request or rerun an evaluation.
        """

        autostart = os.getenv("RUN_LIVE_PIPELINE", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        deployment_paused = os.getenv("PIPELINE_PAUSED", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        control_path = Path(os.getenv("DATA_DIR", "/app/data")) / "pipeline-control.json"
        operator_control: dict[str, Any] = {"pipeline_paused": False}
        if control_path.exists():
            try:
                loaded_control = json.loads(control_path.read_text())
                if isinstance(loaded_control, dict):
                    operator_control = loaded_control
            except (OSError, json.JSONDecodeError):
                operator_control = {"pipeline_paused": False}
        operator_paused = bool(operator_control.get("pipeline_paused"))
        pipeline_paused = not autostart or deployment_paused or operator_paused
        if not autostart:
            control_reason = "RUN_LIVE_PIPELINE is false; no MAST downloads or model jobs will start."
        elif deployment_paused:
            control_reason = "PIPELINE_PAUSED is true; clear the deployment pause before resuming the worker."
        elif operator_paused:
            control_reason = "Paused from ML Ops; the worker will stop starting new jobs after its current cycle."
        else:
            control_reason = "The live worker is armed and will dispatch work on its next heartbeat."
        control_state = {
            "pipeline_paused": pipeline_paused,
            "autostart_enabled": autostart,
            "deployment_gate": not deployment_paused,
            "operator_control": operator_paused,
            "updated_at": operator_control.get("updated_at"),
            "reason": control_reason,
        }
        configured_universe = int(os.getenv("TARGET_UNIVERSE_SIZE", "126000"))
        initial_stage1_pool = int(os.getenv("STAGE1_POOL_SIZE", "500"))
        stage_thresholds: dict[str, int | None] = {
            # These are ranking windows, not runtime compute capacities. A
            # stage may retain and display candidates beyond its threshold.
            "stage1": int(os.getenv("STAGE1_PROMOTION_THRESHOLD", "100")),
            "stage2": int(os.getenv("STAGE2_REASSESSMENT_THRESHOLD", "100")),
            # Stage 3 is a ranked Needs More Data queue, not a top-X window.
            "stage3": None,
        }
        empty = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "data_version": None,
            "control": control_state,
            "worker": worker,
            "universe": {
                "configured_count": configured_universe,
                "manifest_count": 0,
                "processed_count": 0,
                "remaining_count": configured_universe,
                "progress_percent": 0.0,
                "initial_stage1_pool": initial_stage1_pool,
            },
            "stages": self._empty_monitoring_stages(stage_thresholds),
            "models": [],
            "sampling": self._empty_sampling(configured_universe),
            "cycle_activity": {
                "window_minutes": 60,
                "expected_runs": 12,
                "run_count": 0,
                "runs": [],
                "throughput": {},
                "timings": {},
                "queue": {"current": {}, "series": []},
                "note": "Cycle history will appear after the worker records its first completed cycle.",
            },
            "data_health": {
                "providers": [],
                "coverage": {"candidate_count": 0, "product_count": 0, "last_updated_at": None},
                "recent_batches": [],
                "recent_events": [],
            },
        }
        connection = self.connect()
        if connection is None:
            return empty

        try:
            version = self.latest_version(connection)
            manifest_count, processed_count, queued_count, unavailable_count = connection.execute(
                """
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE manifest_status = 'processed'),
                       COUNT(*) FILTER (WHERE manifest_status = 'queued'),
                       COUNT(*) FILTER (WHERE manifest_status IN ('failed', 'unavailable'))
                FROM target_manifest
                """
            ).fetchone()
            processed_count = int(processed_count or 0)
            remaining_count = max(0, configured_universe - processed_count)

            stage_rows = connection.execute(
                """
                SELECT current_stage, status, COUNT(*) AS count
                FROM candidates
                GROUP BY current_stage, status
                ORDER BY current_stage, status
                """
            ).fetchall()
            stage_data = self._empty_monitoring_stages(stage_thresholds)
            for stage, status, count in stage_rows:
                if stage not in stage_data:
                    continue
                stage_data[stage]["counts"][status] = int(count)
                stage_data[stage]["total_candidates"] += int(count)

            for stage in stage_data:
                threshold = stage_thresholds[stage]
                if threshold is None:
                    priority_count = connection.execute(
                        """
                        SELECT COUNT(*) FROM candidates
                        WHERE current_stage = ? AND active = true
                          AND status = 'Needs More Data'
                        """,
                        [stage],
                    ).fetchone()[0]
                    priority_label = "Needs More Data reassessment queue"
                else:
                    priority_count = connection.execute(
                        """
                        SELECT COUNT(*) FROM candidates
                        WHERE current_stage = ? AND active = true
                          AND rank_stage IS NOT NULL AND rank_stage <= ?
                        """,
                        [stage, threshold],
                    ).fetchone()[0]
                    priority_label = (
                        f"Top {threshold} reassessment window"
                        if stage == "stage2"
                        else f"Top {threshold} priority threshold"
                    )
                latest = connection.execute(
                    """
                    SELECT MAX(last_evaluated_at), COUNT(*)
                    FROM candidates
                    WHERE current_stage = ? AND last_evaluated_at IS NOT NULL
                    """,
                    [stage],
                ).fetchone()
                stage_data[stage]["priority_candidates"] = int(priority_count or 0)
                stage_data[stage]["threshold"] = threshold
                stage_data[stage]["priority_label"] = priority_label
                stage_data[stage]["last_evaluated_at"] = isoformat(latest[0])
                stage_data[stage]["evaluated_candidate_count"] = int(latest[1] or 0)

            model_rows = connection.execute(
                """
                SELECT stage, model_version, data_sufficiency, classification,
                       feature_json, metric_json, created_at
                FROM evaluations
                WHERE stage IN ('stage1', 'stage2', 'stage3')
                ORDER BY created_at ASC
                """
            ).fetchall()
            model_data: dict[tuple[str, str], dict[str, Any]] = {}
            for stage, model_version, sufficiency, evaluation_classification, feature_json, metric_json, created_at in model_rows:
                key = (str(stage), str(model_version))
                aggregate = model_data.setdefault(
                    key,
                    {
                        "stage": stage,
                        "model_version": model_version,
                        "evaluation_count": 0,
                        "status_counts": {},
                        "data_sufficiency_counts": {},
                        "evidence_scores": [],
                        "holdout_f1": [],
                        "model_lift": [],
                        "last_evaluated_at": None,
                    },
                )
                aggregate["evaluation_count"] += 1
                if sufficiency:
                    aggregate["data_sufficiency_counts"][str(sufficiency)] = aggregate[
                        "data_sufficiency_counts"
                    ].get(str(sufficiency), 0) + 1
                try:
                    features = json.loads(feature_json) if feature_json else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    features = {}
                try:
                    metrics = json.loads(metric_json) if metric_json else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    metrics = {}
                if not isinstance(features, dict):
                    features = {}
                if not isinstance(metrics, dict):
                    metrics = {}
                classification = evaluation_classification or metrics.get("classification") or features.get("classification")
                if classification:
                    aggregate["status_counts"][str(classification)] = aggregate["status_counts"].get(
                        str(classification), 0
                    ) + 1
                evidence = metrics.get("evidence_score", metrics.get("stage_score"))
                if isinstance(evidence, (int, float)):
                    aggregate["evidence_scores"].append(float(evidence))
                holdout = features.get("holdout_metrics") or metrics.get("holdout_metrics") or {}
                if isinstance(holdout, dict) and isinstance(holdout.get("f1"), (int, float)):
                    aggregate["holdout_f1"].append(float(holdout["f1"]))
                lift = features.get("model_vs_baseline_f1_delta")
                if isinstance(lift, (int, float)):
                    aggregate["model_lift"].append(float(lift))
                aggregate["last_evaluated_at"] = isoformat(created_at)

            models = []
            for aggregate in model_data.values():
                evidence_scores = aggregate.pop("evidence_scores")
                holdout_f1 = aggregate.pop("holdout_f1")
                model_lift = aggregate.pop("model_lift")
                models.append(
                    {
                        **aggregate,
                        "mean_evidence_score": self._mean(evidence_scores),
                        "mean_holdout_f1": self._mean(holdout_f1),
                        "mean_model_lift": self._mean(model_lift),
                    }
                )
            models.sort(key=lambda item: (item["stage"], item["model_version"]))

            sampling = self._monitoring_sampling(connection, configured_universe)
            data_health = self._monitoring_data_health(connection)
            cycle_activity = self._monitoring_cycle_activity(connection, stage_data)
            return {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "data_version": version,
                "control": control_state,
                "worker": worker,
                "universe": {
                    "configured_count": configured_universe,
                    "manifest_count": int(manifest_count or 0),
                    "processed_count": processed_count,
                    "queued_count": int(queued_count or 0),
                    "unavailable_count": int(unavailable_count or 0),
                    "remaining_count": remaining_count,
                    "progress_percent": round(
                        min(100.0, processed_count / configured_universe * 100)
                        if configured_universe
                        else 0.0,
                        2,
                    ),
                    "initial_stage1_pool": initial_stage1_pool,
                },
                "stages": stage_data,
                "models": models,
                "sampling": sampling,
                "cycle_activity": cycle_activity,
                "data_health": data_health,
            }
        finally:
            connection.close()

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 4) if values else None

    @staticmethod
    def _empty_monitoring_stages(
        thresholds: dict[str, int | None],
    ) -> dict[str, dict[str, Any]]:
        return {
            stage: {
                "stage": stage,
                "threshold": threshold,
                "priority_label": (
                    "Needs More Data reassessment queue"
                    if threshold is None
                    else (
                        f"Top {threshold} reassessment window"
                        if stage == "stage2"
                        else f"Top {threshold} priority threshold"
                    )
                ),
                "priority_candidates": 0,
                "total_candidates": 0,
                "evaluated_candidate_count": 0,
                "counts": {
                    "Promising": 0,
                    "Needs More Data": 0,
                    "Inconclusive": 0,
                    "Known Match": 0,
                },
                "last_evaluated_at": None,
            }
            for stage, threshold in thresholds.items()
        }

    @staticmethod
    def _empty_sampling(universe_count: int) -> dict[str, Any]:
        return {
            "policy": "stratified-adaptive replacement sampling",
            "adaptive_enabled": False,
            "cursor": 0,
            "universe_count": universe_count,
            "processed_count": 0,
            "remaining_count": universe_count,
            "buckets": [],
            "spatial_clusters": [],
            "explanation": "No manifest observations are available yet.",
        }

    def _monitoring_sampling(self, connection: duckdb.DuckDBPyConnection, universe_count: int) -> dict[str, Any]:
        checkpoint = connection.execute(
            """
            SELECT cursor, universe_count, processed_count, selection_policy,
                   adaptive_sampling_enabled, updated_at
            FROM processing_checkpoints
            WHERE stage = 'stage1'
            """
        ).fetchone()
        processed = int(checkpoint[2]) if checkpoint else 0
        configured_count = int(checkpoint[1]) if checkpoint else universe_count
        bucket_rows = connection.execute(
            """
            SELECT COALESCE(t.selection_bucket, 'unbucketed') AS bucket,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE t.manifest_status = 'processed') AS processed,
                   COUNT(*) FILTER (WHERE c.status = 'Promising') AS promising,
                   COUNT(*) FILTER (WHERE c.status = 'Needs More Data') AS needs_more_data,
                   COUNT(*) FILTER (WHERE c.status = 'Inconclusive') AS inconclusive,
                   COUNT(*) FILTER (WHERE c.status = 'Known Match') AS known_match,
                   COUNT(*) FILTER (WHERE c.candidate_id IS NOT NULL) AS observed
            FROM target_manifest AS t
            LEFT JOIN candidates AS c ON c.target_id = t.target_id
            GROUP BY COALESCE(t.selection_bucket, 'unbucketed')
            ORDER BY total DESC, bucket ASC
            """
        ).fetchall()
        buckets = []
        for bucket, total, processed_count, promising, needs_more_data, inconclusive, known_match, observed in bucket_rows:
            observed = int(observed or 0)
            known_rate = (int(known_match or 0) / observed) if observed else 0.0
            positive = int(promising or 0) + int(needs_more_data or 0)
            if observed < 3:
                recommendation = "Collecting evidence"
            elif known_rate >= 0.5:
                recommendation = "Deprioritize: many known matches"
            elif positive:
                recommendation = "Prioritize: evidence-bearing"
            else:
                recommendation = "Neutral exploration"
            buckets.append(
                {
                    "bucket": bucket,
                    "total": int(total or 0),
                    "processed": int(processed_count or 0),
                    "observed": observed,
                    "promising": int(promising or 0),
                    "needs_more_data": int(needs_more_data or 0),
                    "inconclusive": int(inconclusive or 0),
                    "known_match": int(known_match or 0),
                    "known_match_rate": round(known_rate, 3),
                    "recommendation": recommendation,
                }
            )

        cluster_rows = connection.execute(
            """
            SELECT FLOOR(t.ra_deg / 10) * 10 AS ra_cell,
                   FLOOR((t.dec_deg + 90) / 10) * 10 - 90 AS dec_cell,
                   COUNT(*) AS observed,
                   COUNT(*) FILTER (WHERE c.status = 'Promising') AS promising,
                   COUNT(*) FILTER (WHERE c.status = 'Needs More Data') AS needs_more_data,
                   COUNT(*) FILTER (WHERE c.status = 'Inconclusive') AS inconclusive,
                   COUNT(*) FILTER (WHERE c.status = 'Known Match') AS known_match
            FROM target_manifest AS t
            JOIN candidates AS c ON c.target_id = t.target_id
            WHERE t.ra_deg IS NOT NULL AND t.dec_deg IS NOT NULL
            GROUP BY ra_cell, dec_cell
            ORDER BY observed DESC
            LIMIT 24
            """
        ).fetchall()
        clusters = []
        for ra_cell, dec_cell, observed, promising, needs_more_data, inconclusive, known_match in cluster_rows:
            known_rate = (int(known_match or 0) / int(observed)) if observed else 0.0
            clusters.append(
                {
                    "ra_min_deg": float(ra_cell),
                    "dec_min_deg": float(dec_cell),
                    "observed": int(observed),
                    "promising": int(promising or 0),
                    "needs_more_data": int(needs_more_data or 0),
                    "inconclusive": int(inconclusive or 0),
                    "known_match": int(known_match or 0),
                    "recommendation": (
                        "Deprioritize known-dense region"
                        if known_rate >= 0.5
                        else "Prioritize evidence-bearing region"
                        if int(promising or 0) + int(needs_more_data or 0) > 0
                        else "Neutral exploration"
                    ),
                }
            )
        return {
            "policy": checkpoint[3] if checkpoint else "stratified-adaptive replacement sampling",
            "adaptive_enabled": bool(checkpoint[4]) if checkpoint else False,
            "cursor": int(checkpoint[0]) if checkpoint else 0,
            "universe_count": configured_count,
            "processed_count": processed,
            "remaining_count": max(0, configured_count - processed),
            "updated_at": isoformat(checkpoint[5]) if checkpoint else None,
            "buckets": buckets,
            "spatial_clusters": clusters,
            "explanation": "Bucket and sky-cell signals are descriptive until the live replacement sampler is enabled.",
        }

    def _monitoring_data_health(self, connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        providers = []
        for provider, runs, succeeded, failed, products, product_failures, last_finished in connection.execute(
            """
            SELECT provider, COUNT(*),
                   COUNT(*) FILTER (WHERE status IN ('success', 'complete')),
                   COUNT(*) FILTER (WHERE status IN ('failed', 'error')),
                   COALESCE(SUM(products_downloaded), 0),
                   COALESCE(SUM(products_failed), 0),
                   MAX(finished_at)
            FROM ingest_runs
            GROUP BY provider
            ORDER BY provider
            """
        ).fetchall():
            providers.append(
                {
                    "provider": provider,
                    "runs": int(runs),
                    "successful_runs": int(succeeded or 0),
                    "failed_runs": int(failed or 0),
                    "products_downloaded": int(products or 0),
                    "products_failed": int(product_failures or 0),
                    "last_finished_at": isoformat(last_finished),
                }
            )
        coverage = connection.execute(
            """
            SELECT COUNT(DISTINCT candidate_id),
                   COALESCE(SUM(product_count), 0),
                   COALESCE(SUM(window_count), 0),
                   MAX(updated_at)
            FROM coverage_watermarks
            """
        ).fetchone()
        batches = connection.execute(
            """
            SELECT batch_id, stage, selection_policy, requested_count,
                   selected_count, evaluated_count, failed_count, status,
                   started_at, finished_at
            FROM batch_runs
            ORDER BY started_at DESC
            LIMIT 8
            """
        ).fetchall()
        events = connection.execute(
            """
            SELECT sequence_number, stage, event_type, occurred_at, model_run_id
            FROM pipeline_events
            ORDER BY sequence_number DESC, occurred_at DESC
            LIMIT 12
            """
        ).fetchall()
        return {
            "providers": providers,
            "coverage": {
                "candidate_count": int(coverage[0] or 0),
                "product_count": int(coverage[1] or 0),
                "window_count": int(coverage[2] or 0),
                "last_updated_at": isoformat(coverage[3]),
            },
            "recent_batches": [
                {
                    "batch_id": row[0],
                    "stage": row[1],
                    "selection_policy": row[2],
                    "requested_count": int(row[3]),
                    "selected_count": int(row[4]),
                    "evaluated_count": int(row[5]),
                    "failed_count": int(row[6]),
                    "status": row[7],
                    "started_at": isoformat(row[8]),
                    "finished_at": isoformat(row[9]),
                }
                for row in batches
            ],
            "recent_events": [
                {
                    "sequence_number": int(row[0]),
                    "stage": row[1],
                    "event_type": row[2],
                    "occurred_at": isoformat(row[3]),
                    "model_run_id": row[4],
                }
                for row in events
            ],
        }

    def _monitoring_cycle_activity(
        self,
        connection: duckdb.DuckDBPyConnection,
        stage_data: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Return the recent worker-cycle ledger used by the live ML Ops view."""

        try:
            rows = connection.execute(
                """
                SELECT cycle_id, started_at, finished_at, status,
                       duration_seconds, payload_json
                FROM pipeline_cycles
                WHERE finished_at >= current_timestamp - INTERVAL '1 hour'
                ORDER BY finished_at DESC
                LIMIT 12
                """
            ).fetchall()
        except Exception:
            rows = []

        runs: list[dict[str, Any]] = []
        for row in reversed(rows):
            try:
                payload = json.loads(row[5] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            stages = payload.get("stage", payload.get("stages", {}))
            if not isinstance(stages, dict):
                stages = {}
            queue = payload.get("queue", {})
            if not isinstance(queue, dict):
                queue = {}
            events = payload.get("events", [])
            if not isinstance(events, list):
                events = []
            ranked_samples = payload.get("ranked_samples", {})
            if not isinstance(ranked_samples, dict):
                ranked_samples = {}
            queue_events = payload.get("queue_events", [])
            if not isinstance(queue_events, list):
                queue_events = []
            runs.append(
                {
                    "cycle_id": row[0],
                    "started_at": isoformat(row[1]),
                    "finished_at": isoformat(row[2]),
                    "status": row[3],
                    "duration_seconds": round(float(row[4] or 0.0), 2),
                    "stages": stages,
                    "queue": queue,
                    "ranked_samples": {
                        str(stage): [sample for sample in samples if isinstance(sample, dict)]
                        for stage, samples in ranked_samples.items()
                        if isinstance(samples, list)
                    },
                    "queue_events": [event for event in queue_events if isinstance(event, dict)],
                    "events": [event for event in events if isinstance(event, dict)],
                    "stage3_reassessment": payload.get("stage3_reassessment", {}),
                    "stage3_intake": payload.get("stage3_intake", {}),
                }
            )

        stage_names = ("stage1", "stage2", "stage3")
        throughput: dict[str, dict[str, Any]] = {}
        timings: dict[str, dict[str, Any]] = {}
        for stage in stage_names:
            attempted = 0
            evaluated = 0
            promising = 0
            needs_more_data = 0
            inconclusive = 0
            errors = 0
            timing_values: list[float] = []
            needs_timing_values: list[float] = []
            dispatch_timing_values: list[float] = []
            handoffs = 0
            for run in runs:
                summary = run["stages"].get(stage, {})
                if not isinstance(summary, dict):
                    continue
                attempted += int(summary.get("attempted") or summary.get("evaluated") or 0)
                evaluated += int(summary.get("evaluated") or summary.get("attempted") or 0)
                promising += int(summary.get("promising") or 0)
                needs_more_data += int(summary.get("needs_more_data") or 0)
                inconclusive += int(summary.get("inconclusive") or 0)
                errors += int(summary.get("errors") or summary.get("failed") or 0)
                if isinstance(summary.get("timing_seconds"), (int, float)):
                    timing_values.append(float(summary["timing_seconds"]))
                if isinstance(summary.get("needs_more_data_seconds"), (int, float)):
                    needs_timing_values.append(float(summary["needs_more_data_seconds"]))
                if isinstance(summary.get("dispatch_seconds"), (int, float)):
                    dispatch_timing_values.append(float(summary["dispatch_seconds"]))
                handoffs += int(summary.get("handoffs") or 0)
            throughput[stage] = {
                "attempted": attempted,
                "evaluated": evaluated,
                "promising": promising,
                "needs_more_data": needs_more_data,
                "inconclusive": inconclusive,
                "errors": errors,
                "handoffs": handoffs,
                "per_hour": evaluated,
                "per_minute": round(evaluated / 60, 2),
            }
            timings[stage] = {
                "average_cycle_seconds": round(sum(timing_values) / len(timing_values), 2) if timing_values else None,
                "needs_more_data_seconds": round(sum(needs_timing_values), 2) if needs_timing_values else 0.0,
                "average_needs_more_data_seconds": round(sum(needs_timing_values) / max(1, needs_more_data), 2) if needs_more_data else None,
                "average_dispatch_seconds": round(sum(dispatch_timing_values) / max(1, handoffs), 2) if handoffs else None,
                "handoffs": handoffs,
            }

        def queue_job_counts(run: dict[str, Any]) -> dict[str, dict[str, int]]:
            events = run.get("queue_events", [])
            if not isinstance(events, list) or not events:
                # Legacy cycles have no explicit queue ledger. Their event
                # stream still identifies work jobs, but their status totals
                # are ranking populations and must never be used as queues.
                events = run.get("events", [])
            counts = {stage: {"jobs": 0} for stage in stage_names}
            for event in events:
                if not isinstance(event, dict):
                    continue
                stage = str(event.get("stage") or "")
                if stage in counts and event.get("candidate_id"):
                    counts[stage]["jobs"] += 1
            return counts

        latest_queue = queue_job_counts(runs[-1]) if runs else {}
        queue_series = []
        for run in runs:
            queue = queue_job_counts(run)
            queue_series.append(
                {
                    "finished_at": run["finished_at"],
                    "stage1": sum(int(value or 0) for value in (queue.get("stage1", {}) or {}).values()),
                    "stage2": sum(int(value or 0) for value in (queue.get("stage2", {}) or {}).values()),
                    "stage3": sum(int(value or 0) for value in (queue.get("stage3", {}) or {}).values()),
                }
            )

        return {
            "window_minutes": 60,
            "expected_runs": 12,
            "run_count": len(runs),
            "runs": runs,
            "throughput": throughput,
            "timings": timings,
            "queue": {"current": latest_queue, "series": queue_series},
            "note": (
                "Cycle history is written at worker cycle boundaries. A cycle can contain several serialized publishes, so timing is wall-clock worker time, not dashboard refresh time."
            ),
        }

    def _version(self) -> int | None:
        connection = self.connect()
        if connection is None:
            return None
        try:
            return self.latest_version(connection)
        finally:
            connection.close()

    @staticmethod
    def _sample_indices(length: int, max_points: int) -> list[int]:
        if length <= 0:
            return []
        if length <= max_points:
            return list(range(length))
        return sorted(set(np.linspace(0, length - 1, max_points, dtype=int).tolist()))

    @staticmethod
    def _next_action(stage: str, status: str) -> str:
        if status == "Known Match":
            return "Mapped to a known catalog object; removed from active rankings while history is preserved."
        if status == "Promising":
            if stage == "stage1":
                return "Eligible for Stage 2 repeatability testing when the next Stage 2 call selects it."
            if stage == "stage2":
                return "Eligible for Stage 3 holdout prediction when the next Stage 3 call selects it."
            return "Continue reviewing the latest Stage 3 holdout evidence."
        if status == "Needs More Data":
            return "Wait for additional coverage before changing this assessment."
        if status == "Inconclusive":
            return "Remain in the historical assessment record; no expensive next-stage run is scheduled."
        if stage == "stage1":
            return "No higher-stage assessment is scheduled from this result yet."
        return "Continue collecting evidence at the current stage."
