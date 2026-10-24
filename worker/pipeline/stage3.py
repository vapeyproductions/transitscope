"""Stage 3 chronological holdout prediction and persistence."""

from __future__ import annotations

import json
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from pipeline.stage1 import Stage1Processor
from pipeline.lifecycle import STATUS_PRIORITY, movement_for_ranks, overall_sort_key, processed_target_count
from pipeline.stage2 import _candidate_rows
from storage.duckdb_store import DuckDBStore


MODEL_VERSION = "stage3-logreg-phase-v1"
BASELINE_MODEL_VERSION = "stage3-phase-window-v1"
RANKING_VERSION = "stage3-overall-v2-score-first"


@dataclass(frozen=True)
class Stage3Config:
    minimum_total_samples: int = 90
    minimum_samples_per_window: int = 30
    promising_f1: float = 0.50
    promising_event_recovery: float = 0.50
    transit_duration_hours: float = 4.0
    max_iter: int = 200
    regularization_c: float = 1000.0


class Stage3ProcessingError(RuntimeError):
    """Raised when a Stage 3 time series cannot be evaluated reliably."""


class Stage3ModelAdapter(Protocol):
    """Stable interface for replacing the CPU model with a later experiment."""

    model_version: str

    def fit(self, features: np.ndarray, labels: np.ndarray) -> "Stage3ModelAdapter":
        ...

    def predict(self, features: np.ndarray) -> np.ndarray:
        ...


class LogisticPhaseModel:
    """CPU-capable model that learns transit-shaped phase features."""

    model_version = MODEL_VERSION

    def __init__(self, *, max_iter: int = 200, regularization_c: float = 1000.0) -> None:
        self.model = LogisticRegression(
            class_weight="balanced",
            C=regularization_c,
            max_iter=max_iter,
            random_state=0,
        )

    def fit(self, features: np.ndarray, labels: np.ndarray) -> "LogisticPhaseModel":
        self.model.fit(features, labels)
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        return np.asarray(self.model.predict(features), dtype=bool)


class PhaseWindowBaseline:
    """Transparent baseline: predict a transit whenever phase is in its window."""

    model_version = BASELINE_MODEL_VERSION

    def __init__(self, period_days: float, duration_days: float) -> None:
        self.period_days = period_days
        self.duration_days = duration_days

    def fit(self, features: np.ndarray, labels: np.ndarray) -> "PhaseWindowBaseline":
        del features, labels
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        # Feature 2 is the absolute phase fraction, so this remains readable and
        # independent of the fitted model's internal representation.
        return features[:, 2] <= (self.duration_days / self.period_days) / 2.0


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


def _phase_features(
    time_values: np.ndarray,
    *,
    period_days: float,
    transit_time: float,
) -> np.ndarray:
    phase = ((time_values - transit_time + period_days / 2.0) % period_days) - period_days / 2.0
    phase_fraction = phase / period_days
    angle = 2.0 * np.pi * phase_fraction
    return np.column_stack(
        (
            np.sin(angle),
            np.cos(angle),
            np.abs(phase_fraction),
            phase_fraction**2,
        )
    )


def _robust_scatter(values: np.ndarray) -> float:
    median = float(np.nanmedian(values))
    mad = float(np.nanmedian(np.abs(values - median)))
    scatter = 1.4826 * mad
    if not np.isfinite(scatter) or scatter <= 0:
        scatter = float(np.nanstd(values))
    return scatter if np.isfinite(scatter) and scatter > 0 else 1e-6


def _event_numbers(
    time_values: np.ndarray,
    *,
    period_days: float,
    transit_time: float,
) -> np.ndarray:
    return np.floor((time_values - transit_time) / period_days + 0.5).astype(int)


def _event_metrics(
    time_values: np.ndarray,
    true_labels: np.ndarray,
    predicted_labels: np.ndarray,
    *,
    period_days: float,
    transit_time: float,
) -> dict[str, float | int | None]:
    event_number_values = _event_numbers(
        time_values,
        period_days=period_days,
        transit_time=transit_time,
    )
    true_events = {
        int(number)
        for number in np.unique(event_number_values[true_labels])
        if int(np.count_nonzero(true_labels & (event_number_values == number))) >= 2
    }
    predicted_events = {
        int(number)
        for number in np.unique(event_number_values[predicted_labels])
        if int(np.count_nonzero(predicted_labels & (event_number_values == number))) >= 2
    }
    matched = true_events & predicted_events
    timing_errors: list[float] = []
    for event_number in matched:
        observed = time_values[predicted_labels & (event_number_values == event_number)]
        expected = transit_time + event_number * period_days
        if len(observed):
            timing_errors.append(abs(float(np.median(observed)) - expected))
    true_count = len(true_events)
    return {
        "true_event_count": true_count,
        "predicted_event_count": len(predicted_events),
        "matched_event_count": len(matched),
        "event_recovery_rate": len(matched) / true_count if true_count else None,
        "event_accuracy": len(matched) / max(true_count, len(predicted_events), 1),
        "timing_error_days": float(np.mean(timing_errors)) if timing_errors else None,
    }


def _classification_metrics(
    time_values: np.ndarray,
    true_labels: np.ndarray,
    predicted_labels: np.ndarray,
    *,
    period_days: float,
    transit_time: float,
    train_flux: np.ndarray,
    holdout_flux: np.ndarray,
) -> dict[str, Any]:
    true_labels = np.asarray(true_labels, dtype=bool)
    predicted_labels = np.asarray(predicted_labels, dtype=bool)
    event_metrics = _event_metrics(
        time_values,
        true_labels,
        predicted_labels,
        period_days=period_days,
        transit_time=transit_time,
    )
    true_positives = int(np.count_nonzero(true_labels & predicted_labels))
    false_positives = int(np.count_nonzero(~true_labels & predicted_labels))
    true_negatives = int(np.count_nonzero(~true_labels & ~predicted_labels))
    train_depth = max(0.0, 1.0 - float(np.median(train_flux)))
    holdout_depth = max(0.0, 1.0 - float(np.median(holdout_flux[true_labels]))) if true_labels.any() else 0.0
    return {
        "precision": float(precision_score(true_labels, predicted_labels, zero_division=0)),
        "recall": float(recall_score(true_labels, predicted_labels, zero_division=0)),
        "f1": float(f1_score(true_labels, predicted_labels, zero_division=0)),
        "accuracy": float(accuracy_score(true_labels, predicted_labels)),
        "false_positive_rate": false_positives / max(false_positives + true_negatives, 1),
        "true_positives": true_positives,
        "false_positives": false_positives,
        "true_negatives": true_negatives,
        "depth_error": abs(train_depth - holdout_depth),
        **event_metrics,
    }


class Stage3Processor:
    """Train on two chronological windows and evaluate on a third holdout."""

    def __init__(self, config: Stage3Config | None = None) -> None:
        self.config = config or Stage3Config()
        self.stage1_processor = Stage1Processor()

    def evaluate(
        self,
        fits_paths: Sequence[Path],
        *,
        candidate_id: str,
        target_id: str,
        stage2_features: Mapping[str, Any],
        normalized_dir: Path,
    ) -> dict[str, Any]:
        if not fits_paths:
            raise Stage3ProcessingError("no historical light-curve products were provided")
        series: list[tuple[np.ndarray, np.ndarray]] = []
        for path in fits_paths:
            cleaned = self.stage1_processor.clean(self.stage1_processor.read_fits(path))
            if len(cleaned["time"]):
                series.append((cleaned["time"], cleaned["flux"]))
        if not series:
            raise Stage3ProcessingError("historical products contained no usable samples")
        time_values = np.concatenate([item[0] for item in series])
        flux_values = np.concatenate([item[1] for item in series])
        return self.evaluate_series(
            time_values,
            flux_values,
            candidate_id=candidate_id,
            target_id=target_id,
            stage2_features=stage2_features,
            normalized_dir=normalized_dir,
            product_count=len(fits_paths),
        )

    def missing_history_result(
        self,
        *,
        candidate_id: str,
        target_id: str,
        stage2_features: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._result(
            candidate_id,
            target_id,
            "Needs More Data",
            "Awaiting three chronological holdout windows",
            0.0,
            "insufficient",
            {
                "model_version": MODEL_VERSION,
                "baseline_model_version": BASELINE_MODEL_VERSION,
                "total_samples": 0,
                "window_1": None,
                "window_2": None,
                "holdout_window": None,
                "holdout_metrics": None,
                "baseline_metrics": None,
                "period_days": self._finite_float(
                    stage2_features.get("stage1_period_days", stage2_features.get("period_days"))
                ),
                "transit_time": self._finite_float(
                    stage2_features.get("stage1_transit_time", stage2_features.get("transit_time"))
                ),
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
        stage2_features: Mapping[str, Any],
        normalized_dir: Path | None = None,
        product_count: int = 1,
    ) -> dict[str, Any]:
        time_values, flux_values = self._clean_arrays(time_values, flux_values)
        period = self._finite_float(
            stage2_features.get("stage1_period_days", stage2_features.get("period_days"))
        )
        transit_time = self._finite_float(
            stage2_features.get("stage1_transit_time", stage2_features.get("transit_time"))
        )
        duration_hours = self._finite_float(
            stage2_features.get("stage1_duration_hours", stage2_features.get("duration_hours"))
        )
        duration_days = (duration_hours or self.config.transit_duration_hours) / 24.0
        base_features: dict[str, Any] = {
            "model_version": MODEL_VERSION,
            "baseline_model_version": BASELINE_MODEL_VERSION,
            "product_count": product_count,
            "total_samples": int(len(time_values)),
            "series_start": float(time_values[0]) if len(time_values) else None,
            "series_end": float(time_values[-1]) if len(time_values) else None,
            "period_days": period,
            "transit_time": transit_time,
            "duration_days": duration_days,
        }
        if period is None or transit_time is None or len(time_values) < self.config.minimum_total_samples:
            return self._result(
                candidate_id,
                target_id,
                "Needs More Data",
                "Awaiting enough observations for three holdout windows",
                0.0,
                "insufficient",
                base_features,
                None,
            )

        windows = self._windows(time_values)
        if any(len(window) < self.config.minimum_samples_per_window for window in windows):
            base_features["windows"] = [self._window_metadata(time_values, window) for window in windows]
            return self._result(
                candidate_id,
                target_id,
                "Needs More Data",
                "Awaiting enough observations in each chronological window",
                0.0,
                "insufficient",
                base_features,
                None,
            )

        train_indices = np.concatenate((windows[0], windows[1]))
        holdout_indices = windows[2]
        train_time = time_values[train_indices]
        train_flux = flux_values[train_indices]
        holdout_time = time_values[holdout_indices]
        holdout_flux = flux_values[holdout_indices]
        scatter = _robust_scatter(train_flux)
        threshold = float(np.median(train_flux) - 3.0 * scatter)
        train_labels = train_flux <= threshold
        holdout_labels = holdout_flux <= threshold
        if not train_labels.any() or not holdout_labels.any() or len(np.unique(train_labels)) < 2:
            base_features.update(
                {
                    "threshold": threshold,
                    "windows": [self._window_metadata(time_values, window) for window in windows],
                    "train_positive_samples": int(np.count_nonzero(train_labels)),
                    "holdout_positive_samples": int(np.count_nonzero(holdout_labels)),
                }
            )
            return self._result(
                candidate_id,
                target_id,
                "Needs More Data",
                "Awaiting a recoverable dimming event in each holdout setup",
                0.0,
                "insufficient",
                base_features,
                None,
            )

        train_features = _phase_features(train_time, period_days=period, transit_time=transit_time)
        holdout_features = _phase_features(holdout_time, period_days=period, transit_time=transit_time)
        model = LogisticPhaseModel(
            max_iter=self.config.max_iter,
            regularization_c=self.config.regularization_c,
        ).fit(train_features, train_labels)
        baseline = PhaseWindowBaseline(period, duration_days).fit(train_features, train_labels)
        model_predictions = model.predict(holdout_features)
        baseline_predictions = baseline.predict(holdout_features)
        holdout_metrics = _classification_metrics(
            holdout_time,
            holdout_labels,
            model_predictions,
            period_days=period,
            transit_time=transit_time,
            train_flux=train_flux,
            holdout_flux=holdout_flux,
        )
        baseline_metrics = _classification_metrics(
            holdout_time,
            holdout_labels,
            baseline_predictions,
            period_days=period,
            transit_time=transit_time,
            train_flux=train_flux,
            holdout_flux=holdout_flux,
        )
        score = self._evidence_score(holdout_metrics)
        status = (
            "Promising"
            if float(holdout_metrics["f1"]) >= self.config.promising_f1
            and float(holdout_metrics["event_recovery_rate"] or 0.0) >= self.config.promising_event_recovery
            else "Inconclusive"
        )
        features = {
            **base_features,
            "threshold": threshold,
            "train_positive_samples": int(np.count_nonzero(train_labels)),
            "holdout_positive_samples": int(np.count_nonzero(holdout_labels)),
            "windows": [self._window_metadata(time_values, window) for window in windows],
            "training_windows": [self._window_metadata(time_values, window) for window in windows[:2]],
            "holdout_window": self._window_metadata(time_values, windows[2]),
            "holdout_metrics": holdout_metrics,
            "baseline_metrics": baseline_metrics,
            "model_vs_baseline_f1_delta": float(holdout_metrics["f1"])
            - float(baseline_metrics["f1"]),
        }
        normalized_path: Path | None = None
        if normalized_dir is not None:
            normalized_dir.mkdir(parents=True, exist_ok=True)
            normalized_path = normalized_dir / f"stage3-{self._slug(candidate_id)}.npz"
            np.savez_compressed(normalized_path, time=time_values, normalized_flux=flux_values)
        return self._result(
            candidate_id,
            target_id,
            status,
            "No current activity",
            score,
            "sufficient",
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
            "stage": "stage3",
            "model_version": MODEL_VERSION,
            "status": status,
            "activity": activity,
            "evidence_score": round(float(score), 4),
            "data_sufficiency": sufficiency,
            "features": _safe(features),
            "normalized_path": str(normalized_path) if normalized_path else None,
            "evaluated_at": datetime.now(timezone.utc),
        }

    def _evidence_score(self, metrics: Mapping[str, Any]) -> float:
        precision = float(metrics.get("precision", 0.0))
        recall = float(metrics.get("recall", 0.0))
        f1 = float(metrics.get("f1", 0.0))
        recovery = float(metrics.get("event_recovery_rate") or 0.0)
        false_positive_rate = float(metrics.get("false_positive_rate", 1.0))
        return 100.0 * float(
            np.clip(
                0.35 * f1
                + 0.20 * precision
                + 0.20 * recall
                + 0.20 * recovery
                + 0.05 * (1.0 - false_positive_rate),
                0.0,
                1.0,
            )
        )

    @staticmethod
    def _clean_arrays(
        time_values: np.ndarray, flux_values: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        time_values = np.asarray(time_values, dtype=float)
        flux_values = np.asarray(flux_values, dtype=float)
        finite = np.isfinite(time_values) & np.isfinite(flux_values)
        time_values = time_values[finite]
        flux_values = flux_values[finite]
        order = np.argsort(time_values)
        time_values = time_values[order]
        flux_values = flux_values[order]
        unique_time, indices = np.unique(time_values, return_index=True)
        if len(unique_time) < 2:
            raise Stage3ProcessingError("time series contains too few unique observations")
        return unique_time, flux_values[indices]

    @staticmethod
    def _windows(time_values: np.ndarray) -> list[np.ndarray]:
        return [np.asarray(window, dtype=int) for window in np.array_split(np.arange(len(time_values)), 3)]

    @staticmethod
    def _window_metadata(time_values: np.ndarray, indices: np.ndarray) -> dict[str, Any]:
        return {
            "start": float(time_values[indices[0]]) if len(indices) else None,
            "end": float(time_values[indices[-1]]) if len(indices) else None,
            "samples": int(len(indices)),
        }

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


def publish_stage3_result(
    store: DuckDBStore,
    result: Mapping[str, Any],
    *,
    stage1_score: float,
    stage2_score: float,
    input_product_ids: Sequence[str],
    ranking_version: str = RANKING_VERSION,
) -> int:
    """Publish Stage 3 evidence and update Stage 3 plus Overall snapshots."""

    candidates = _candidate_rows(store)
    candidate_id = str(result["candidate_id"])
    existing = next((item for item in candidates if item["candidate_id"] == candidate_id), None)
    if existing is None:
        raise Stage3ProcessingError(f"candidate does not exist: {candidate_id}")
    overall_score = round(
        0.30 * float(stage1_score)
        + 0.30 * float(stage2_score)
        + 0.40 * float(result["evidence_score"]),
        4,
    )
    stage3_pool = [
        item for item in candidates if item["current_stage"] == "stage3" and item["candidate_id"] != candidate_id
    ]
    stage3_pool.append(
        {
            **existing,
            "current_stage": "stage3",
            "stage_score": result["evidence_score"],
            "overall_score": overall_score,
            "status": result["status"],
            "activity": result["activity"],
            "last_evaluated_at": result["evaluated_at"],
            "processing_slot_occupied": result["status"] != "Inconclusive",
        }
    )
    stage3_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage3_ranks = {item["candidate_id"]: index for index, item in enumerate(stage3_pool, 1)}

    stage1_pool = [item for item in candidates if item["current_stage"] == "stage1"]
    stage1_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage1_ranks = {item["candidate_id"]: index for index, item in enumerate(stage1_pool, 1)}

    stage2_pool = [
        item
        for item in candidates
        if item["current_stage"] == "stage2" and item["candidate_id"] != candidate_id
    ]
    stage2_pool.sort(
        key=lambda item: (
            -STATUS_PRIORITY.get(str(item.get("status")), 0),
            -float(item["stage_score"] or 0.0),
            item["candidate_id"],
        )
    )
    stage2_ranks = {item["candidate_id"]: index for index, item in enumerate(stage2_pool, 1)}

    overall_pool = [item for item in candidates if item["candidate_id"] != candidate_id]
    overall_pool.append(next(item for item in stage3_pool if item["candidate_id"] == candidate_id))
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
                "current_stage": "stage3" if item["candidate_id"] == candidate_id else item["current_stage"],
                "rank_overall": new_overall_rank,
                "rank_stage": stage3_ranks.get(
                    item["candidate_id"],
                    stage2_ranks.get(
                        item["candidate_id"],
                        stage1_ranks.get(item["candidate_id"], old["rank_stage"]),
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
        note=f"Stage 3 chronological holdout evaluation for {result['target_id']}",
        ingest_runs=[
            {
                "run_id": f"stage3-ingest-{suffix}-{version}",
                "provider": "MAST",
                "query_config_json": {
                    "stage": "stage3",
                    "product_count": len(input_product_ids),
                    "holdout_strategy": "chronological-three-window",
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
                "evaluation_id": f"stage3-evaluation-{candidate_id}-{version}",
                "candidate_id": candidate_id,
                "stage": "stage3",
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
                    "stage2_score": stage2_score,
                    "overall_score": overall_score,
                    "model_version": result["model_version"],
                    "baseline_model_version": BASELINE_MODEL_VERSION,
                    "holdout_metrics": result["features"].get("holdout_metrics"),
                    "baseline_metrics": result["features"].get("baseline_metrics"),
                    "ranking_model_version": ranking_version,
                },
                "classification": result["status"],
                "data_sufficiency": result["data_sufficiency"],
            }
        ],
        coverage_watermarks=[
            {
                "candidate_id": candidate_id,
                "stage": "stage3",
                "coverage_signature": hashlib.sha256(
                    json.dumps(sorted(str(value) for value in input_product_ids)).encode("utf-8")
                ).hexdigest(),
                "coverage_start": result["features"].get("series_start"),
                "coverage_end": result["features"].get("series_end"),
                "product_count": len(input_product_ids),
                "window_count": len(result["features"].get("windows", [])),
                "updated_at": evaluated_at,
            }
        ],
        events=[
            {
                "event_id": f"stage3-evaluated-{candidate_id}-{version}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": "stage3",
                "event_type": "stage3_evaluated",
                "payload_json": {
                    "classification": result["status"],
                    "evidence_score": result["evidence_score"],
                    "features": result["features"],
                    "model_version": result["model_version"],
                    "baseline_model_version": BASELINE_MODEL_VERSION,
                    "ranking_model_version": ranking_version,
                },
                "occurred_at": evaluated_at,
                "model_run_id": f"stage3-run-{candidate_id}-{version}",
            }
        ],
        snapshots=[
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
                "snapshot_id": f"stage3-snapshot-{version}",
                "view": "stage3",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": [item["candidate_id"] for item in stage3_pool],
                "active_candidate_count": len(stage3_pool),
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


def load_stage2_input(store: DuckDBStore, candidate_id: str) -> dict[str, Any] | None:
    store.initialize()
    with store.connect() as connection:
        candidate = connection.execute(
            """
            SELECT candidate_id, target_id, status, overall_score, stage_score, current_stage
            FROM candidates WHERE candidate_id = ?
            """,
            [candidate_id],
        ).fetchone()
        evaluation = connection.execute(
            """
            SELECT feature_json, metric_json, input_product_ids
            FROM evaluations
            WHERE candidate_id = ? AND stage = 'stage2'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [candidate_id],
        ).fetchone()
    if not candidate or not evaluation:
        return None
    metric_json = json.loads(evaluation[1])
    return {
        "candidate_id": candidate[0],
        "target_id": candidate[1],
        "status": candidate[2],
        "overall_score": float(candidate[3] or 0.0),
        "stage2_score": float(candidate[4] or 0.0),
        "current_stage": candidate[5],
        "stage1_score": float(metric_json.get("stage1_score", 0.0)),
        "features": json.loads(evaluation[0]),
        "input_product_ids": json.loads(evaluation[2]),
    }


__all__ = [
    "BASELINE_MODEL_VERSION",
    "MODEL_VERSION",
    "RANKING_VERSION",
    "Stage3Config",
    "Stage3ProcessingError",
    "Stage3Processor",
    "load_stage2_input",
    "publish_stage3_result",
]
