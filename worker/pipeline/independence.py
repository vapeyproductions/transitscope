"""Independent-signal validation for same-star Stage 1 siblings.

Stage 1 deliberately keeps closely related signals together long enough to
avoid paying for duplicate Stage 2/3 work.  This module is the decision point
that was previously missing: remove the primary signal from the normalized
light curve, then ask whether the sibling still has a repeatable, significant
transit pattern in both chronological halves of the data.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from storage.duckdb_store import DuckDBStore


MODEL_VERSION = "stage1-signal-independence-v1"


@dataclass(frozen=True)
class IndependenceResult:
    status: str
    score: float
    data_sufficiency: str
    metrics: dict[str, Any]
    reason: str


def latest_stage1_features(store: DuckDBStore, candidate_id: str) -> dict[str, Any]:
    """Return the latest persisted Stage 1 feature payload for a candidate."""

    store.initialize()
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT feature_json
            FROM evaluations
            WHERE candidate_id = ? AND stage = 'stage1'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [candidate_id],
        ).fetchone()
    if not row or not row[0]:
        return {}
    try:
        payload = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def latest_independence_source(store: DuckDBStore, candidate_id: str) -> str | None:
    """Return the Stage 1 normalized path used by the last independence run."""

    store.initialize()
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT feature_json
            FROM evaluations
            WHERE candidate_id = ? AND stage = 'signal_independence'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [candidate_id],
        ).fetchone()
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return str(payload.get("source_normalized_path")) if payload.get("source_normalized_path") else None


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _resolve_path(raw_path: Any, data_dir: Path) -> Path | None:
    if not raw_path:
        return None
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_dir.name:
        return data_dir.parent.joinpath(*path.parts)
    return data_dir / path


def _remove_signal(time: np.ndarray, flux: np.ndarray, features: Mapping[str, Any]) -> np.ndarray:
    """Flatten one transit train while leaving all other observations intact."""

    period = _finite(features.get("period_days"))
    duration_hours = _finite(features.get("duration_hours"))
    transit_time = _finite(features.get("transit_time"))
    if period is None or period <= 0 or duration_hours is None or duration_hours <= 0 or transit_time is None:
        return flux.copy()
    duration = duration_hours / 24.0
    phase = ((time - transit_time + 0.5 * period) % period) - 0.5 * period
    mask = np.abs(phase) <= 0.5 * duration
    if not np.any(mask):
        return flux.copy()
    outside = flux[~mask] if np.any(~mask) else flux
    outside_level = float(np.nanmedian(outside))
    inside_level = float(np.nanmedian(flux[mask]))
    if not math.isfinite(outside_level) or not math.isfinite(inside_level) or outside_level == 0:
        return flux.copy()
    relative_depth = 1.0 - inside_level / outside_level
    if not math.isfinite(relative_depth) or relative_depth <= 0:
        return flux.copy()
    correction = max(1.0 - min(relative_depth, 0.95), 0.05)
    residual = flux.copy()
    residual[mask] = residual[mask] / correction
    return residual


def _robust_scatter(values: np.ndarray) -> float:
    if len(values) == 0:
        return math.nan
    median = float(np.nanmedian(values))
    return float(1.4826 * np.nanmedian(np.abs(values - median)))


def _segment_metrics(
    time: np.ndarray,
    flux: np.ndarray,
    *,
    period: float,
    transit_time: float,
    duration_days: float,
) -> dict[str, Any]:
    phase = ((time - transit_time + 0.5 * period) % period) - 0.5 * period
    transit_mask = np.abs(phase) <= duration_days / 2.0
    event_numbers = np.floor((time - transit_time) / period + 0.5).astype(int)
    event_depths: list[float] = []
    event_counts: list[int] = []
    for event_number in np.unique(event_numbers[transit_mask]):
        event_mask = transit_mask & (event_numbers == event_number)
        values = flux[event_mask]
        if len(values) < 2:
            continue
        baseline_values = flux[~event_mask]
        baseline = float(np.nanmedian(baseline_values)) if len(baseline_values) else 1.0
        depth = baseline - float(np.nanmedian(values))
        if math.isfinite(depth):
            event_depths.append(max(0.0, depth))
            event_counts.append(len(values))
    out_of_event = flux[~transit_mask] if np.any(~transit_mask) else flux
    scatter = _robust_scatter(out_of_event)
    median_depth = float(np.median(event_depths)) if event_depths else 0.0
    depth_scatter = float(np.std(event_depths) / max(abs(median_depth), 1e-9)) if len(event_depths) > 1 else math.nan
    depth_snr = median_depth / max(scatter, 1e-6) if math.isfinite(scatter) else 0.0
    return {
        "event_count": len(event_depths),
        "event_depths": event_depths,
        "median_depth": median_depth,
        "depth_scatter_fraction": depth_scatter,
        "out_of_event_scatter": scatter,
        "depth_snr": depth_snr,
        "in_event_samples": int(sum(event_counts)),
    }


def assess_signal_independence(
    time: np.ndarray,
    flux: np.ndarray,
    *,
    sibling_features: Mapping[str, Any],
    primary_features: Mapping[str, Any],
) -> IndependenceResult:
    """Assess whether the sibling remains significant after primary removal."""

    period = _finite(sibling_features.get("period_days"))
    transit_time = _finite(sibling_features.get("transit_time"))
    duration_hours = _finite(sibling_features.get("duration_hours"))
    min_samples = int(os.getenv("STAGE1_INDEPENDENCE_MIN_SAMPLES", "500"))
    min_events = int(os.getenv("STAGE1_INDEPENDENCE_MIN_EVENTS", "4"))
    min_holdout_events = int(os.getenv("STAGE1_INDEPENDENCE_MIN_HOLDOUT_EVENTS", "1"))
    min_snr = float(os.getenv("STAGE1_INDEPENDENCE_MIN_SNR", "3.0"))
    min_consistency = float(os.getenv("STAGE1_INDEPENDENCE_MIN_CONSISTENCY", "0.45"))

    if len(time) < min_samples or period is None or period <= 0 or transit_time is None or duration_hours is None:
        return IndependenceResult(
            "Needs More Data",
            0.0,
            "insufficient",
            {"sample_count": len(time), "required_samples": min_samples},
            "The normalized observation does not contain enough usable coverage for an independence test.",
        )

    order = np.argsort(time)
    time = np.asarray(time[order], dtype=float)
    flux = np.asarray(flux[order], dtype=float)
    residual = _remove_signal(time, flux, primary_features)
    duration_days = duration_hours / 24.0
    full = _segment_metrics(
        time,
        residual,
        period=period,
        transit_time=transit_time,
        duration_days=duration_days,
    )
    midpoint = float(np.median(time))
    first = time <= midpoint
    second = time > midpoint
    first_metrics = _segment_metrics(
        time[first],
        residual[first],
        period=period,
        transit_time=transit_time,
        duration_days=duration_days,
    )
    second_metrics = _segment_metrics(
        time[second],
        residual[second],
        period=period,
        transit_time=transit_time,
        duration_days=duration_days,
    )
    first_depth = float(first_metrics["median_depth"])
    second_depth = float(second_metrics["median_depth"])
    consistency = min(first_depth, second_depth) / max(first_depth, second_depth, 1e-9)
    metrics = {
        "model_version": MODEL_VERSION,
        "sample_count": len(time),
        "primary_removed": True,
        "full_event_count": int(full["event_count"]),
        "full_median_depth": float(full["median_depth"]),
        "full_depth_snr": float(full["depth_snr"]),
        "full_depth_scatter_fraction": full["depth_scatter_fraction"],
        "first_half_event_count": int(first_metrics["event_count"]),
        "second_half_event_count": int(second_metrics["event_count"]),
        "first_half_depth": first_depth,
        "second_half_depth": second_depth,
        "holdout_consistency": float(consistency),
        "primary_period_days": _finite(primary_features.get("period_days")),
        "sibling_period_days": period,
    }

    if (
        full["event_count"] < min_events
        or first_metrics["event_count"] < min_holdout_events
        or second_metrics["event_count"] < min_holdout_events
    ):
        return IndependenceResult(
            "Needs More Data",
            0.0,
            "insufficient",
            metrics,
            "The sibling pattern is not covered by enough independent transit events yet.",
        )

    score = 100.0 * (
        0.55 * min(float(full["depth_snr"]) / max(min_snr * 2.0, 1e-9), 1.0)
        + 0.45 * float(np.clip(consistency, 0.0, 1.0))
    )
    passes = float(full["depth_snr"]) >= min_snr and consistency >= min_consistency
    if passes:
        return IndependenceResult(
            "Promising",
            round(score, 4),
            "sufficient",
            metrics,
            "The sibling remains significant after the primary signal is removed and repeats in both data halves.",
        )
    return IndependenceResult(
        "Duplicate Signal",
        round(score, 4),
        "sufficient",
        metrics,
        "The sibling does not retain a sufficiently significant, repeatable residual after primary removal.",
    )


def assess_candidate_from_store(
    store: DuckDBStore,
    *,
    data_dir: Path,
    sibling_candidate_id: str,
    primary_candidate_id: str,
) -> tuple[IndependenceResult, dict[str, Any], dict[str, Any]]:
    """Load persisted Stage 1 artifacts and assess one sibling."""

    sibling_features = latest_stage1_features(store, sibling_candidate_id)
    primary_features = latest_stage1_features(store, primary_candidate_id)
    source_path = _resolve_path(sibling_features.get("normalized_path"), data_dir)
    if source_path is None or not source_path.exists():
        return (
            IndependenceResult(
                "Needs More Data",
                0.0,
                "insufficient",
                {"source_normalized_path": str(source_path) if source_path else None},
                "The normalized Stage 1 observation is not available for the independence test.",
            ),
            sibling_features,
            primary_features,
        )
    try:
        with np.load(source_path) as payload:
            time = np.asarray(payload["time"], dtype=float)
            flux = np.asarray(payload["normalized_flux"], dtype=float)
    except (OSError, KeyError, ValueError):
        return (
            IndependenceResult(
                "Needs More Data",
                0.0,
                "insufficient",
                {"source_normalized_path": str(source_path)},
                "The normalized Stage 1 observation could not be read for the independence test.",
            ),
            sibling_features,
            primary_features,
        )
    result = assess_signal_independence(
        time,
        flux,
        sibling_features=sibling_features,
        primary_features=primary_features,
    )
    metrics = {**result.metrics, "source_normalized_path": str(source_path)}
    return IndependenceResult(result.status, result.score, result.data_sufficiency, metrics, result.reason), sibling_features, primary_features


__all__ = [
    "MODEL_VERSION",
    "IndependenceResult",
    "assess_candidate_from_store",
    "assess_signal_independence",
    "latest_independence_source",
    "latest_stage1_features",
]
