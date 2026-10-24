"""Stage 2 historical repeatability evaluation and persistence."""

from __future__ import annotations

import json
import hashlib
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from pipeline.stage1 import Stage1Processor
from pipeline.lifecycle import STATUS_PRIORITY, movement_for_ranks, overall_sort_key, processed_target_count
from storage.duckdb_store import DuckDBStore


MODEL_VERSION = "stage2-repeatability-v2-backfill"
RANKING_VERSION = "stage2-overall-v2-score-first"


@dataclass(frozen=True)
class Stage2Config:
    target_lookback_days: float = 730.0
    minimum_coverage_days: float = 180.0
    minimum_events: int = 2
    promising_score: float = 60.0
    transit_duration_hours: float = 4.0


class Stage2ProcessingError(RuntimeError):
    """Raised when a Stage 2 history cannot be interpreted."""


def _safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


class Stage2Processor:
    """Compare later history with a Stage 1 period rather than searching anew."""

    def __init__(self, config: Stage2Config | None = None) -> None:
        self.config = config or Stage2Config()
        self.stage1_processor = Stage1Processor()

    def evaluate(
        self,
        fits_paths: Sequence[Path],
        *,
        candidate_id: str,
        target_id: str,
        stage1_features: Mapping[str, Any],
        normalized_dir: Path,
    ) -> dict[str, Any]:
        if not fits_paths:
            raise Stage2ProcessingError("no historical light-curve products were provided")

        series: list[tuple[np.ndarray, np.ndarray]] = []
        for path in fits_paths:
            light_curve = self.stage1_processor.read_fits(path)
            cleaned = self.stage1_processor.clean(light_curve)
            if len(cleaned["time"]):
                series.append((cleaned["time"], cleaned["flux"]))
        if not series:
            raise Stage2ProcessingError("historical products contained no usable samples")

        time_values = np.concatenate([item[0] for item in series])
        flux_values = np.concatenate([item[1] for item in series])
        order = np.argsort(time_values)
        time_values = time_values[order]
        flux_values = flux_values[order]
        unique_time, unique_indices = np.unique(time_values, return_index=True)
        time_values = unique_time
        flux_values = flux_values[unique_indices]

        normalized_dir.mkdir(parents=True, exist_ok=True)
        normalized_path = normalized_dir / f"stage2-{self._slug(candidate_id)}.npz"
        np.savez_compressed(normalized_path, time=time_values, normalized_flux=flux_values)
        return self.evaluate_series(
            time_values,
            flux_values,
            candidate_id=candidate_id,
            target_id=target_id,
            stage1_features=stage1_features,
            normalized_path=normalized_path,
            product_count=len(fits_paths),
        )

    def missing_history_result(
        self,
        *,
        candidate_id: str,
        target_id: str,
        stage1_features: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._result(
            candidate_id,
            target_id,
            "Needs More Data",
            "Awaiting historical light-curve products",
            0.0,
            "insufficient",
            {
                "model_version": MODEL_VERSION,
                "target_lookback_days": self.config.target_lookback_days,
                "minimum_coverage_days": self.config.minimum_coverage_days,
                "product_count": 0,
                "total_samples": 0,
                "window_start": None,
                "window_end": None,
                "coverage_days": 0.0,
                "missing_history_days": self.config.target_lookback_days,
                "stage1_period_days": self._finite_float(stage1_features.get("period_days")),
                "stage1_transit_time": self._finite_float(stage1_features.get("transit_time")),
                "observed_event_count": 0,
            },
            None,
        )

    def evaluate_series(
        self,
        time_values: np.ndarray,
        flux_values: np.ndarray,
        *,
        candidate_id: str,
        target_id: str,
        stage1_features: Mapping[str, Any],
        normalized_path: Path | None = None,
        product_count: int = 1,
    ) -> dict[str, Any]:
        if len(time_values) < 2:
            raise Stage2ProcessingError("historical series contains too few samples")
        period = self._finite_float(stage1_features.get("period_days"))
        transit_time = self._finite_float(stage1_features.get("transit_time"))
        duration_hours = self._finite_float(stage1_features.get("duration_hours"))
        duration_days = (duration_hours or self.config.transit_duration_hours) / 24.0
        coverage_days = float(time_values[-1] - time_values[0])
        lookback_start = float(time_values[-1] - self.config.target_lookback_days)
        in_window = time_values >= lookback_start
        window_time = time_values[in_window]
        window_flux = flux_values[in_window]
        if len(window_time) < 2:
            window_time = time_values
            window_flux = flux_values
        coverage_days = float(window_time[-1] - window_time[0])
        backfilled_beyond_lookback = False
        if (
            coverage_days < self.config.minimum_coverage_days
            and float(time_values[-1] - time_values[0]) > coverage_days
        ):
            # Archive coverage can be episodic: a target may have an old TESS
            # sector and a recent sector with a large gap between them.  When
            # the recent lookback cannot meet the operational minimum, use the
            # complete available history rather than silently discarding the
            # older observations.  This is a data-backfill decision, not a
            # second period search.
            window_time = time_values
            window_flux = flux_values
            coverage_days = float(window_time[-1] - window_time[0])
            backfilled_beyond_lookback = True

        features: dict[str, Any] = {
            "model_version": MODEL_VERSION,
            "target_lookback_days": self.config.target_lookback_days,
            "minimum_coverage_days": self.config.minimum_coverage_days,
            "product_count": product_count,
            "total_samples": int(len(window_time)),
            "window_start": float(window_time[0]),
            "window_end": float(window_time[-1]),
            "coverage_days": coverage_days,
            "missing_history_days": max(0.0, self.config.target_lookback_days - coverage_days),
            "backfilled_beyond_lookback": backfilled_beyond_lookback,
            "window_policy": "recent-window-with-full-history-backfill",
            "stage1_period_days": period,
            "stage1_transit_time": transit_time,
        }

        if period is None or transit_time is None:
            return self._result(
                candidate_id,
                target_id,
                "Needs More Data",
                "Awaiting a usable Stage 1 period and historical coverage",
                0.0,
                "insufficient",
                features,
                normalized_path,
            )

        event_metrics = self._event_metrics(
            window_time,
            window_flux,
            period=period,
            transit_time=transit_time,
            duration_days=duration_days,
        )
        features.update(event_metrics)
        score = self._score(features)
        enough_history = (
            coverage_days >= self.config.minimum_coverage_days
            and int(features["observed_event_count"]) >= self.config.minimum_events
        )
        if not enough_history:
            status = "Needs More Data"
            activity = "Awaiting longer historical coverage"
            sufficiency = "insufficient"
        elif score >= self.config.promising_score:
            status = "Promising"
            activity = "No current activity"
            sufficiency = "sufficient"
        else:
            status = "Inconclusive"
            activity = "No current activity"
            sufficiency = "sufficient"
        return self._result(
            candidate_id,
            target_id,
            status,
            activity,
            score,
            sufficiency,
            features,
            normalized_path,
        )

    def _result(
        self,
        candidate_id: str,
        target_id: str,
        status: str,
        activity: str,
        score: float,
        sufficiency: str,
        features: Mapping[str, Any],
        normalized_path: Path | None,
    ) -> dict[str, Any]:
        return {
            "candidate_id": candidate_id,
            "target_id": target_id,
            "stage": "stage2",
            "model_version": MODEL_VERSION,
            "status": status,
            "activity": activity,
            "evidence_score": round(float(score), 4),
            "data_sufficiency": sufficiency,
            "features": _safe(features),
            "normalized_path": str(normalized_path) if normalized_path else None,
            "evaluated_at": datetime.now(timezone.utc),
        }

    def _event_metrics(
        self,
        time_values: np.ndarray,
        flux_values: np.ndarray,
        *,
        period: float,
        transit_time: float,
        duration_days: float,
    ) -> dict[str, Any]:
        phase = ((time_values - transit_time + period / 2.0) % period) - period / 2.0
        transit_mask = np.abs(phase) <= duration_days / 2.0
        event_numbers = np.floor((time_values - transit_time) / period + 0.5).astype(int)
        event_times: list[float] = []
        depths: list[float] = []
        for event_number in np.unique(event_numbers[transit_mask]):
            event_values = flux_values[transit_mask & (event_numbers == event_number)]
            if len(event_values) < 2:
                continue
            event_times.append(float(np.nanmedian(time_values[transit_mask & (event_numbers == event_number)])))
            depths.append(max(0.0, 1.0 - float(np.nanmedian(event_values))))

        observed_count = len(event_times)
        if observed_count:
            expected_times = np.array(
                [transit_time + round((event_time - transit_time) / period) * period for event_time in event_times]
            )
            timing_scatter = float(np.std(np.array(event_times) - expected_times))
            median_depth = float(np.median(depths))
            depth_scatter = float(np.std(depths) / max(abs(median_depth), 1e-9))
            in_event = flux_values[transit_mask]
            out_event = flux_values[~transit_mask]
            shape_scatter = float(np.std(in_event) / max(np.std(out_event), 1e-9)) if len(out_event) else 1.0
        else:
            timing_scatter = math.nan
            median_depth = 0.0
            depth_scatter = math.nan
            shape_scatter = math.nan

        timing_consistency = float(np.clip(1.0 - timing_scatter / max(period * 0.25, 1e-9), 0.0, 1.0)) if np.isfinite(timing_scatter) else 0.0
        depth_consistency = float(np.clip(1.0 - depth_scatter, 0.0, 1.0)) if np.isfinite(depth_scatter) else 0.0
        shape_consistency = float(np.clip(1.0 - shape_scatter, 0.0, 1.0)) if np.isfinite(shape_scatter) else 0.0
        return {
            "observed_event_count": observed_count,
            "median_transit_depth": median_depth,
            "timing_scatter_days": timing_scatter,
            "depth_scatter_fraction": depth_scatter,
            "shape_scatter_ratio": shape_scatter,
            "timing_consistency": timing_consistency,
            "depth_consistency": depth_consistency,
            "shape_consistency": shape_consistency,
        }

    def _score(self, features: Mapping[str, Any]) -> float:
        coverage = float(
            np.clip(
                float(features.get("coverage_days", 0.0)) / self.config.target_lookback_days,
                0.0,
                1.0,
            )
        )
        event_support = float(np.clip(float(features.get("observed_event_count", 0)) / 5.0, 0.0, 1.0))
        return 100.0 * (
            0.30 * coverage
            + 0.20 * event_support
            + 0.20 * float(features.get("timing_consistency", 0.0))
            + 0.20 * float(features.get("depth_consistency", 0.0))
            + 0.10 * float(features.get("shape_consistency", 0.0))
        )

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if np.isfinite(number) else None

    @staticmethod
    def _slug(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _candidate_rows(store: DuckDBStore) -> list[dict[str, Any]]:
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, target_id, current_stage, status,
                   overall_score, stage_score, rank_overall, rank_stage,
                   activity, last_evaluated_at, active, processing_slot_occupied
            FROM candidates
            WHERE active = true
            """
        ).fetchall()
    return [
        {
            "candidate_id": row[0],
            "target_id": row[1],
            "current_stage": row[2],
            "status": row[3],
            "overall_score": row[4],
            "stage_score": row[5],
            "rank_overall": row[6],
            "rank_stage": row[7],
            "activity": row[8],
            "last_evaluated_at": row[9],
            "active": bool(row[10]),
            "processing_slot_occupied": bool(row[11]),
        }
        for row in rows
    ]


def publish_stage2_result(
    store: DuckDBStore,
    result: Mapping[str, Any],
    *,
    stage1_score: float,
    input_product_ids: Sequence[str],
    ranking_version: str = RANKING_VERSION,
) -> int:
    """Publish a Stage 2 evaluation and update Stage 2 plus Overall rankings."""

    candidates = _candidate_rows(store)
    candidate_id = str(result["candidate_id"])
    existing = next((item for item in candidates if item["candidate_id"] == candidate_id), None)
    if existing is None:
        raise Stage2ProcessingError(f"candidate does not exist: {candidate_id}")
    overall_score = round(0.45 * float(stage1_score) + 0.55 * float(result["evidence_score"]), 4)
    stage2_pool = [
        item for item in candidates if item["current_stage"] == "stage2" and item["candidate_id"] != candidate_id
    ]
    stage2_pool.append(
        {
            **existing,
            "current_stage": "stage2",
            "stage_score": result["evidence_score"],
            "overall_score": overall_score,
            "status": result["status"],
            "activity": result["activity"],
            "last_evaluated_at": result["evaluated_at"],
            "processing_slot_occupied": result["status"] != "Inconclusive",
        }
    )
    stage2_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage2_ranks = {item["candidate_id"]: index for index, item in enumerate(stage2_pool, 1)}

    stage1_pool = [
        item
        for item in candidates
        if item["current_stage"] == "stage1" and item["candidate_id"] != candidate_id
    ]
    stage1_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage1_ranks = {item["candidate_id"]: index for index, item in enumerate(stage1_pool, 1)}

    stage3_pool = [item for item in candidates if item["current_stage"] == "stage3"]
    stage3_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage3_ranks = {item["candidate_id"]: index for index, item in enumerate(stage3_pool, 1)}

    promoted = next(item for item in stage2_pool if item["candidate_id"] == candidate_id)
    overall_pool = [item for item in candidates if item["candidate_id"] != candidate_id]
    overall_pool.append(promoted)
    overall_pool.sort(key=overall_sort_key)
    overall_ranks = {item["candidate_id"]: index for index, item in enumerate(overall_pool, 1)}
    previous_by_id = {item["candidate_id"]: item for item in candidates}

    updates: list[dict[str, Any]] = []
    for item in overall_pool:
        old = previous_by_id[item["candidate_id"]]
        new_overall_rank = overall_ranks[item["candidate_id"]]
        old_overall_rank = old["rank_overall"]
        delta = (old_overall_rank - new_overall_rank) if old_overall_rank is not None else 0
        _, movement = movement_for_ranks(old_overall_rank, new_overall_rank)
        updates.append(
            {
                "candidate_id": item["candidate_id"],
                "current_stage": "stage2" if item["candidate_id"] == candidate_id else item["current_stage"],
                "rank_overall": new_overall_rank,
                "rank_stage": stage2_ranks.get(
                    item["candidate_id"],
                    stage1_ranks.get(
                        item["candidate_id"],
                        stage3_ranks.get(item["candidate_id"], old["rank_stage"]),
                    ),
                ),
                "rank_delta": delta,
                "movement_kind": movement,
                "active": True,
                "activity": item["activity"],
                "status": item["status"],
                "overall_score": item["overall_score"],
                "stage_score": item["stage_score"],
                "last_evaluated_at": item["last_evaluated_at"],
                "known_match_id": None,
                "processing_slot_occupied": item["processing_slot_occupied"],
            }
        )

    version = (store.latest_published_version() or 0) + 1
    evaluated_at = result["evaluated_at"]
    suffix = re.sub(r"[^a-zA-Z0-9]+", "", candidate_id)[-12:]
    store.publish_batch(
        version=version,
        note=f"Stage 2 repeatability evaluation for {result['target_id']}",
        ingest_runs=[
            {
                "run_id": f"stage2-ingest-{suffix}-{version}",
                "provider": "MAST",
                "query_config_json": {
                    "stage": "stage2",
                    "target_lookback_days": result["features"].get("target_lookback_days"),
                    "product_count": len(input_product_ids),
                },
                "started_at": evaluated_at,
                "finished_at": evaluated_at,
                "status": "complete",
                "targets_seen": 1,
                "products_downloaded": len(input_product_ids),
                "products_failed": 0,
                "error_json": None,
            }
        ],
        evaluations=[
            {
                "evaluation_id": f"stage2-evaluation-{candidate_id}-{version}",
                "candidate_id": candidate_id,
                "stage": "stage2",
                "model_version": result["model_version"],
                "data_window_start": None,
                "data_window_end": None,
                "input_product_ids": list(input_product_ids),
                "feature_json": {
                    **dict(result["features"]),
                    "normalized_path": result["normalized_path"],
                    "ranking_model_version": ranking_version,
                },
                "metric_json": {
                    "evidence_score": result["evidence_score"],
                    "classification": result["status"],
                    "data_sufficiency": result["data_sufficiency"],
                    "stage1_score": stage1_score,
                    "overall_score": overall_score,
                    "ranking_model_version": ranking_version,
                },
                "classification": result["status"],
                "data_sufficiency": result["data_sufficiency"],
            }
        ],
        coverage_watermarks=[
            {
                "candidate_id": candidate_id,
                "stage": "stage2",
                "coverage_signature": hashlib.sha256(
                    json.dumps(sorted(str(value) for value in input_product_ids)).encode("utf-8")
                ).hexdigest(),
                "coverage_start": result["features"].get("window_start"),
                "coverage_end": result["features"].get("window_end"),
                "product_count": len(input_product_ids),
                "window_count": 1,
                "updated_at": evaluated_at,
            }
        ],
        events=(
            [
                {
                    "event_id": f"stage1-released-{candidate_id}-{version}",
                    "sequence_number": version,
                    "candidate_id": candidate_id,
                    "stage": "stage1",
                    "event_type": "stage1_released",
                    "payload_json": {
                        "reason": "advanced_to_stage2",
                        "next_stage": "stage2",
                    },
                    "occurred_at": evaluated_at,
                    "model_run_id": f"stage2-run-{candidate_id}-{version}",
                }
            ]
            if stage1_input["current_stage"] == "stage1"
            else []
        )
        + [
            {
                "event_id": f"stage2-evaluated-{candidate_id}-{version}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": "stage2",
                "event_type": "stage2_evaluated",
                "payload_json": {
                    "classification": result["status"],
                    "evidence_score": result["evidence_score"],
                    "features": result["features"],
                    "model_version": result["model_version"],
                    "ranking_model_version": ranking_version,
                },
                "occurred_at": evaluated_at,
                "model_run_id": f"stage2-run-{candidate_id}-{version}",
            }
        ],
        snapshots=[
            {
                "snapshot_id": f"stage1-snapshot-{version}",
                "view": "stage1",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": [item["candidate_id"] for item in stage1_pool],
                "active_candidate_count": len(stage1_pool),
                "processed_target_count": processed_target_count(store),
            },
            {
                "snapshot_id": f"stage2-snapshot-{version}",
                "view": "stage2",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": [item["candidate_id"] for item in stage2_pool],
                "active_candidate_count": len(stage2_pool),
                "processed_target_count": processed_target_count(store),
            },
            {
                "snapshot_id": f"overall-snapshot-{version}",
                "view": "overall",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": [item["candidate_id"] for item in overall_pool],
                "active_candidate_count": len(overall_pool),
                "processed_target_count": processed_target_count(store),
            },
        ],
        candidate_updates=updates,
    )
    return version


def load_stage1_input(store: DuckDBStore, candidate_id: str) -> dict[str, Any] | None:
    store.initialize()
    with store.connect() as connection:
        candidate = connection.execute(
            """
            SELECT candidate_id, target_id, status, overall_score, current_stage
            FROM candidates WHERE candidate_id = ?
            """,
            [candidate_id],
        ).fetchone()
        evaluation = connection.execute(
            """
            SELECT feature_json, input_product_ids
            FROM evaluations
            WHERE candidate_id = ? AND stage = 'stage1'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [candidate_id],
        ).fetchone()
    if not candidate or not evaluation:
        return None
    return {
        "candidate_id": candidate[0],
        "target_id": candidate[1],
        "status": candidate[2],
        "stage1_score": float(candidate[3] or 0.0),
        "current_stage": candidate[4],
        "features": json.loads(evaluation[0]),
        "input_product_ids": json.loads(evaluation[1]),
    }


__all__ = [
    "MODEL_VERSION",
    "RANKING_VERSION",
    "Stage2Config",
    "Stage2ProcessingError",
    "Stage2Processor",
    "load_stage1_input",
    "publish_stage2_result",
]
