from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np


VIEW_STAGE = {"stage1": "stage1", "stage2": "stage2", "stage3": "stage3"}


def isoformat(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


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
            if version is not None:
                row = connection.execute(
                    """
                    SELECT processed_target_count, active_candidate_count, occurred_at
                    FROM ranking_snapshots
                    WHERE view = 'stage1' AND sequence_number = ?
                    """,
                    [version],
                ).fetchone()
                if row:
                    response["processed_target_count"] = int(row[0])
                    response["active_candidate_count"] = int(row[1])
                    response["last_ingest_at"] = isoformat(row[2])
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
        if view not in {"overall", "stage1", "stage2", "stage3"}:
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
                        "activity": row[8],
                        "rank_delta": int(row[9] or 0),
                        "movement": row[10] or "unchanged",
                        "last_evaluated_at": isoformat(row[11]),
                    }
                )

            response = {
                "view": view,
                "data_version": version,
                "sequence_number": snapshot[0] if snapshot else version,
                "occurred_at": isoformat(snapshot[1]) if snapshot else None,
                "processed_target_count": int(snapshot[3]) if snapshot else 0,
                "total_target_count": int(os.getenv("TARGET_UNIVERSE_SIZE", "126000")),
                "rows": projected_rows,
            }
            return response
        finally:
            connection.close()

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
