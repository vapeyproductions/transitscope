from __future__ import annotations

import json
import os
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


class ReadOnlyStore:
    """Read-only projection of the worker's published DuckDB state."""

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir or Path(os.getenv("DATA_DIR", "/app/data"))
        self.database = self.data_dir / "transitscope.duckdb"

    def connect(self) -> duckdb.DuckDBPyConnection | None:
        if not self.database.exists():
            return None
        return duckdb.connect(str(self.database), read_only=True)

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

    def rankings(self, view: str, limit: int) -> dict[str, Any]:
        if view not in RANKING_VIEWS:
            raise ValueError(f"unsupported ranking view: {view}")
        connection = self.connect()
        empty = {
            "view": view,
            "data_version": None,
            "sequence_number": None,
            "occurred_at": None,
            "processed_target_count": 0,
            "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
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
            stage_filter = VIEW_STAGE.get(view)
            if stage_filter:
                rows = connection.execute(
                    """
                    SELECT candidate_id, target_id, current_stage, status,
                           overall_score, stage_score, rank_overall, rank_stage,
                           activity, rank_delta, movement_kind, last_evaluated_at
                    FROM candidates
                    WHERE active = true AND current_stage = ?
                    ORDER BY rank_stage NULLS LAST, stage_score DESC NULLS LAST
                    LIMIT ?
                    """,
                    [stage_filter, limit],
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT candidate_id, target_id, current_stage, status,
                           overall_score, stage_score, rank_overall, rank_stage,
                           activity, rank_delta, movement_kind, last_evaluated_at
                    FROM candidates
                    WHERE active = true
                    ORDER BY rank_overall NULLS LAST, overall_score DESC NULLS LAST
                    LIMIT ?
                    """,
                    [limit],
                ).fetchall()

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
                        "ranking_rationale": ranking_rationale(
                            row[3], row[5] if row[5] is not None else row[4]
                        ),
                        "activity": row[8],
                        "rank_delta": int(row[9] or 0),
                        "movement": row[10] or "unchanged",
                        "last_evaluated_at": isoformat(row[11]),
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

    def replay_snapshot(self, view: str, sequence: int, limit: int) -> dict[str, Any]:
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
            rows = self._replay_rows(connection, view, int(snapshot[0]), candidate_ids, limit)
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
    ) -> list[dict[str, Any]]:
        projected_rows: list[dict[str, Any]] = []
        for index, raw_candidate_id in enumerate(candidate_ids[:limit], start=1):
            candidate_id = str(raw_candidate_id)
            candidate = connection.execute(
                """
                SELECT candidate_id, target_id, current_stage, status,
                       overall_score, stage_score, rank_overall, rank_stage,
                       activity, rank_delta, movement_kind, last_evaluated_at
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
                    "ranking_rationale": ranking_rationale(status, evidence),
                    "activity": payload.get("activity", candidate[8]),
                    "rank_delta": int(candidate[9] or 0),
                    "movement": candidate[10] or "unchanged",
                    "last_evaluated_at": isoformat(event[3] if event else candidate[11]),
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
                SELECT candidate_id, target_id, current_stage, status,
                       overall_score, stage_score, rank_overall, rank_stage,
                       activity, rank_delta, movement_kind, last_evaluated_at,
                       known_match_id, active
                FROM candidates
                WHERE candidate_id = ?
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
                ORDER BY sequence_number ASC, occurred_at ASC
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
                return "Eligible for Stage 2 repeatability testing when capacity allows."
            if stage == "stage2":
                return "Eligible for Stage 3 holdout prediction when capacity allows."
            return "Continue reviewing the latest Stage 3 holdout evidence."
        if status == "Needs More Data":
            return "Wait for additional coverage before changing this assessment."
        if status == "Inconclusive":
            return "Remain in the historical assessment record; no expensive next-stage run is scheduled."
        if stage == "stage1":
            return "No higher-stage assessment is scheduled from this result yet."
        return "Continue collecting evidence at the current stage."
