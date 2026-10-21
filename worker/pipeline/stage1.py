from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
from astropy.io import fits
from astropy.timeseries import BoxLeastSquares

from storage.duckdb_store import DuckDBStore


MODEL_VERSION = "stage1-bls-v1"


class Stage1ProcessingError(RuntimeError):
    """Raised when a light curve cannot be evaluated reliably."""


@dataclass(frozen=True)
class Stage1Config:
    model_version: str = MODEL_VERSION
    minimum_samples: int = 500
    minimum_coverage_days: float = 5.0
    period_min_days: float = 0.5
    period_max_days: float = 14.0
    period_grid_size: int = 2400
    promising_score: float = 45.0
    promising_snr: float = 7.0


@dataclass(frozen=True)
class LightCurve:
    time: np.ndarray
    flux: np.ndarray
    quality: np.ndarray
    source_flux_column: str
    header_metadata: dict[str, Any]


class Stage1Processor:
    """Parse, clean, normalize, and screen one TESS light curve."""

    def __init__(self, config: Stage1Config | None = None) -> None:
        self.config = config or Stage1Config()

    def process(
        self,
        fits_path: Path,
        *,
        target_manifest: Mapping[str, Any],
        product_id: str,
        normalized_dir: Path,
    ) -> dict[str, Any]:
        light_curve = self.read_fits(fits_path)
        cleaned = self.clean(light_curve)
        candidate_id = self._candidate_id(str(target_manifest["target_id"]))
        normalized_path = normalized_dir / f"{candidate_id}.npz"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            normalized_path,
            time=cleaned["time"],
            normalized_flux=cleaned["flux"],
        )

        features = dict(cleaned["features"])
        if cleaned["sufficient"]:
            features.update(self._bls_features(cleaned["time"], cleaned["flux"]))
            score = self._evidence_score(features)
            classification = self._classification(features, score)
            data_sufficiency = "sufficient"
            activity = "No current activity"
        else:
            score = 0.0
            classification = "Needs More Data"
            data_sufficiency = "insufficient"
            activity = "Awaiting more data"

        now = datetime.now(timezone.utc)
        result = {
            "candidate_id": candidate_id,
            "target_id": str(target_manifest["target_id"]),
            "stage": "stage1",
            "model_version": self.config.model_version,
            "status": classification,
            "activity": activity,
            "evidence_score": round(float(score), 4),
            "data_sufficiency": data_sufficiency,
            "features": self._json_safe(features),
            "normalized_path": str(normalized_path),
            "product_id": product_id,
            "evaluated_at": now,
            "window_start": float(cleaned["time"][0]) if len(cleaned["time"]) else None,
            "window_end": float(cleaned["time"][-1]) if len(cleaned["time"]) else None,
            "header_metadata": light_curve.header_metadata,
        }
        return result

    @staticmethod
    def read_fits(path: Path) -> LightCurve:
        try:
            with fits.open(path, mode="readonly", memmap=True) as hdul:
                if len(hdul) < 2 or hdul[1].data is None:
                    raise Stage1ProcessingError("TESS FITS file has no binary table in HDU 1")
                table = hdul[1].data
                names = {name.upper(): name for name in table.names or []}
                time_name = names.get("TIME")
                flux_name = names.get("PDCSAP_FLUX") or names.get("SAP_FLUX")
                quality_name = names.get("QUALITY")
                if not time_name or not flux_name:
                    raise Stage1ProcessingError(
                        "TESS FITS table is missing TIME and PDCSAP_FLUX/SAP_FLUX columns"
                    )
                time = np.asarray(table[time_name], dtype=float)
                flux = np.asarray(table[flux_name], dtype=float)
                quality = (
                    np.asarray(table[quality_name], dtype=np.int64)
                    if quality_name
                    else np.zeros(len(time), dtype=np.int64)
                )
                header = hdul[1].header
                metadata = {
                    key: header[key]
                    for key in (
                        "TELESCOP",
                        "MISSION",
                        "SECTOR",
                        "CAMERA",
                        "CCD",
                        "TSTART",
                        "TSTOP",
                        "BJDREFI",
                        "BJDREFF",
                        "TIMEDEL",
                        "CADENCE",
                        "PROCVER",
                    )
                    if key in header
                }
                return LightCurve(time, flux, quality, flux_name, metadata)
        except Stage1ProcessingError:
            raise
        except (OSError, ValueError) as exc:
            raise Stage1ProcessingError(f"could not read FITS light curve {path}: {exc}") from exc

    def clean(self, light_curve: LightCurve) -> dict[str, Any]:
        time = light_curve.time
        flux = light_curve.flux
        quality = light_curve.quality
        total_samples = len(time)
        if total_samples == 0:
            raise Stage1ProcessingError("TESS light curve contains no samples")

        finite = np.isfinite(time) & np.isfinite(flux) & (flux > 0)
        good_quality = quality == 0
        valid = finite & good_quality
        usable_time = time[valid]
        usable_flux = flux[valid]
        if len(usable_time):
            order = np.argsort(usable_time)
            usable_time = usable_time[order]
            usable_flux = usable_flux[order]

        usable_fraction = len(usable_time) / total_samples
        quality_flag_rate = float(np.count_nonzero(~good_quality) / total_samples)
        if len(usable_time) > 1:
            coverage_days = float(usable_time[-1] - usable_time[0])
            cadence_days = float(np.nanmedian(np.diff(usable_time)))
        else:
            coverage_days = 0.0
            cadence_days = math.nan

        if len(usable_flux) < self.config.minimum_samples or coverage_days < self.config.minimum_coverage_days:
            normalized = self._robust_normalize(usable_flux)
            features = {
                "source_flux_column": light_curve.source_flux_column,
                "total_samples": total_samples,
                "usable_samples": len(usable_time),
                "usable_data_fraction": usable_fraction,
                "quality_flag_rate": quality_flag_rate,
                "coverage_days": coverage_days,
                "cadence_minutes": cadence_days * 24 * 60 if np.isfinite(cadence_days) else None,
                "missing_data_fraction": self._missing_fraction(usable_time, cadence_days),
                "cleaning_method": "quality_zero_and_robust_median_normalization",
            }
            return {"time": usable_time, "flux": normalized, "features": features, "sufficient": False}

        normalized = self._robust_normalize(usable_flux)
        normalized = self._remove_linear_trend(usable_time, normalized)
        features = {
            "source_flux_column": light_curve.source_flux_column,
            "total_samples": total_samples,
            "usable_samples": len(usable_time),
            "usable_data_fraction": usable_fraction,
            "quality_flag_rate": quality_flag_rate,
            "coverage_days": coverage_days,
            "cadence_minutes": cadence_days * 24 * 60,
            "missing_data_fraction": self._missing_fraction(usable_time, cadence_days),
            "cleaning_method": "quality_zero_median_normalization_linear_detrend",
            "baseline_flux": float(np.nanmedian(usable_flux)),
        }
        return {"time": usable_time, "flux": normalized, "features": features, "sufficient": True}

    def _bls_features(self, time: np.ndarray, flux: np.ndarray) -> dict[str, Any]:
        scatter = self._robust_scatter(flux)
        if not np.isfinite(scatter) or scatter <= 0:
            scatter = float(np.std(flux)) or 1e-6

        max_period = min(
            self.config.period_max_days,
            max(self.config.period_min_days * 1.1, (time[-1] - time[0]) / 2.5),
        )
        if max_period <= self.config.period_min_days:
            raise Stage1ProcessingError("light curve baseline cannot support the Stage 1 period search")

        periods = np.linspace(
            self.config.period_min_days,
            max_period,
            self.config.period_grid_size,
        )
        model = BoxLeastSquares(time - time[0], flux, dy=scatter)
        best: dict[str, Any] | None = None
        for duration_hours in (1.0, 2.0, 4.0, 6.0):
            duration_days = duration_hours / 24.0
            if duration_days >= self.config.period_min_days / 2:
                continue
            try:
                result = model.power(periods, duration_days, objective="snr")
            except ValueError:
                continue
            powers = np.asarray(result.power, dtype=float)
            finite = np.isfinite(powers)
            if not finite.any():
                continue
            index = int(np.nanargmax(np.where(finite, powers, np.nan)))
            candidate = {
                "period_days": float(result.period[index]),
                "duration_hours": float(result.duration[index] * 24),
                "transit_time": float(result.transit_time[index] + time[0]),
                "bls_power": float(result.power[index]),
                "depth": float(result.depth[index]),
                "depth_error": float(result.depth_err[index]),
            }
            if best is None or candidate["bls_power"] > best["bls_power"]:
                best = candidate

        if best is None:
            raise Stage1ProcessingError("BLS could not evaluate a finite periodogram")

        period = best["period_days"]
        duration = best["duration_hours"] / 24.0
        transit_time = best["transit_time"]
        mask = model.transit_mask(time - time[0], period, duration, transit_time - time[0])
        transit_count = self._transit_event_count(time, mask, period, transit_time)
        odd_even = self._odd_even_consistency(time, flux, mask, period, transit_time)
        out_of_event = flux[~mask] if np.any(~mask) else flux
        best["bls_snr"] = abs(best["depth"]) / max(abs(best["depth_error"]), 1e-9)
        best["observed_transit_count"] = transit_count
        best["odd_even_depth_consistency"] = odd_even
        best["out_of_event_scatter"] = self._robust_scatter(out_of_event)
        best["period_grid_spacing_days"] = float(periods[1] - periods[0])
        return best

    def _evidence_score(self, features: Mapping[str, Any]) -> float:
        def bounded(value: float, low: float, high: float) -> float:
            return float(np.clip((value - low) / (high - low), 0.0, 1.0))

        snr = bounded(float(features.get("bls_snr", 0.0)), 0.0, 12.0)
        depth = bounded(abs(float(features.get("depth", 0.0))), 0.0005, 0.05)
        transits = bounded(float(features.get("observed_transit_count", 0)), 1.0, 4.0)
        coverage = bounded(float(features.get("coverage_days", 0.0)), 5.0, 20.0)
        quality = float(np.clip(float(features.get("usable_data_fraction", 0.0)), 0.0, 1.0))
        odd_even = float(np.clip(float(features.get("odd_even_depth_consistency", 0.0)), 0.0, 1.0))
        return 100.0 * (
            0.30 * snr
            + 0.20 * depth
            + 0.15 * transits
            + 0.15 * coverage
            + 0.10 * quality
            + 0.10 * odd_even
        )

    def _classification(self, features: Mapping[str, Any], score: float) -> str:
        if (
            score >= self.config.promising_score
            and float(features.get("bls_snr", 0.0)) >= self.config.promising_snr
            and int(features.get("observed_transit_count", 0)) >= 2
        ):
            return "Promising"
        return "Inconclusive"

    @staticmethod
    def _robust_normalize(flux: np.ndarray) -> np.ndarray:
        baseline = float(np.nanmedian(flux)) if len(flux) else 1.0
        if not np.isfinite(baseline) or baseline <= 0:
            baseline = 1.0
        return np.asarray(flux / baseline, dtype=float)

    @staticmethod
    def _remove_linear_trend(time: np.ndarray, flux: np.ndarray) -> np.ndarray:
        if len(time) < 3 or not np.isfinite(flux).all():
            return flux
        centered = time - np.nanmedian(time)
        try:
            slope, intercept = np.polyfit(centered, flux, 1)
        except (np.linalg.LinAlgError, ValueError):
            return flux
        trend = slope * centered + intercept
        trend_median = float(np.nanmedian(trend))
        if not np.isfinite(trend).all() or np.any(trend <= 0) or trend_median <= 0:
            return flux
        return flux / trend * trend_median

    @staticmethod
    def _robust_scatter(values: np.ndarray) -> float:
        if len(values) == 0:
            return math.nan
        median = float(np.nanmedian(values))
        return float(1.4826 * np.nanmedian(np.abs(values - median)))

    @staticmethod
    def _missing_fraction(time: np.ndarray, cadence_days: float) -> float | None:
        if len(time) < 2 or not np.isfinite(cadence_days) or cadence_days <= 0:
            return None
        expected = max(1.0, (time[-1] - time[0]) / cadence_days + 1.0)
        return float(np.clip(1.0 - len(time) / expected, 0.0, 1.0))

    @staticmethod
    def _transit_event_count(
        time: np.ndarray, mask: np.ndarray, period: float, transit_time: float
    ) -> int:
        if not np.any(mask):
            return 0
        event_numbers = np.floor((time[mask] - transit_time) / period + 0.5).astype(int)
        return int(len(np.unique(event_numbers)))

    @staticmethod
    def _odd_even_consistency(
        time: np.ndarray,
        flux: np.ndarray,
        mask: np.ndarray,
        period: float,
        transit_time: float,
    ) -> float:
        if not np.any(mask):
            return 0.0
        event_numbers = np.floor((time - transit_time) / period + 0.5).astype(int)
        depths: dict[int, float] = {}
        for event in np.unique(event_numbers[mask]):
            values = flux[mask & (event_numbers == event)]
            if len(values):
                depths[int(event)] = max(0.0, 1.0 - float(np.nanmedian(values)))
        odd = [depth for event, depth in depths.items() if event % 2]
        even = [depth for event, depth in depths.items() if not event % 2]
        if not odd or not even:
            return 0.5
        odd_depth = float(np.nanmedian(odd))
        even_depth = float(np.nanmedian(even))
        return float(
            np.clip(
                1.0 - abs(odd_depth - even_depth) / (abs(odd_depth) + abs(even_depth) + 1e-9),
                0.0,
                1.0,
            )
        )

    @staticmethod
    def _candidate_id(target_id: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", target_id.lower()).strip("-")
        return f"stage1-{slug}"

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): Stage1Processor._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [Stage1Processor._json_safe(item) for item in value]
        if isinstance(value, (np.floating, np.integer)):
            return value.item()
        if isinstance(value, float) and not np.isfinite(value):
            return None
        return value


def publish_stage1_result(
    store: DuckDBStore,
    result: Mapping[str, Any],
    *,
    target_manifest: Mapping[str, Any],
    query_config: Mapping[str, Any],
    product_checksum: str | None = None,
) -> int:
    """Publish one evaluated target and its first Stage 1 replay snapshot."""

    latest_version = store.latest_published_version() or 0
    version = latest_version + 1
    evaluated_at = result["evaluated_at"]
    candidate_id = str(result["candidate_id"])
    target_id = str(result["target_id"])
    stable_suffix = (product_checksum or str(result["product_id"]))[-12:]
    model_run_id = f"stage1-run-{stable_suffix}"
    event_id = f"stage1-evaluated-{candidate_id}-{stable_suffix}"
    evaluation_id = f"stage1-evaluation-{candidate_id}-{stable_suffix}"
    ingest_run_id = f"stage1-ingest-{stable_suffix}"

    store.publish_batch(
        version=version,
        note=f"Stage 1 evaluation for {target_id}",
        targets=[
            {
                "target_id": target_id,
                "provider_target_id": target_manifest.get("provider_target_id", target_id),
                "ra_deg": target_manifest.get("ra_deg"),
                "dec_deg": target_manifest.get("dec_deg"),
                "sector": target_manifest.get("sector"),
                "cadence": target_manifest.get("cadence"),
                "source_product_family": target_manifest.get("source_product_family"),
                "selection_bucket": target_manifest.get("selection_bucket", "probe"),
                "manifest_status": "processed",
            }
        ],
        ingest_runs=[
            {
                "run_id": ingest_run_id,
                "provider": "MAST",
                "query_config_json": dict(query_config),
                "started_at": evaluated_at,
                "finished_at": evaluated_at,
                "status": "complete",
                "targets_seen": 1,
                "products_downloaded": 1,
                "products_failed": 0,
                "error_json": None,
            }
        ],
        candidates=[
            {
                "candidate_id": candidate_id,
                "target_id": target_id,
                "current_stage": "stage1",
                "status": result["status"],
                "overall_score": result["evidence_score"],
                "stage_score": result["evidence_score"],
                "rank_overall": 1,
                "rank_stage": 1,
                "activity": result["activity"],
                "rank_delta": 0,
                "movement_kind": "new",
                "last_evaluated_at": evaluated_at,
                "known_match_id": None,
                "active": True,
            }
        ],
        evaluations=[
            {
                "evaluation_id": evaluation_id,
                "candidate_id": candidate_id,
                "stage": "stage1",
                "model_version": result["model_version"],
                "data_window_start": None,
                "data_window_end": None,
                "input_product_ids": [result["product_id"]],
                "feature_json": {
                    **dict(result["features"]),
                    "normalized_path": result["normalized_path"],
                    "window_start": result["window_start"],
                    "window_end": result["window_end"],
                    "header_metadata": result["header_metadata"],
                },
                "metric_json": {
                    "evidence_score": result["evidence_score"],
                    "classification": result["status"],
                    "data_sufficiency": result["data_sufficiency"],
                },
                "classification": result["status"],
                "data_sufficiency": result["data_sufficiency"],
            }
        ],
        events=[
            {
                "event_id": event_id,
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": "stage1",
                "event_type": "stage1_evaluated",
                "payload_json": {
                    "evidence_score": result["evidence_score"],
                    "classification": result["status"],
                    "model_version": result["model_version"],
                    "features": result["features"],
                },
                "occurred_at": evaluated_at,
                "model_run_id": model_run_id,
            }
        ],
        snapshots=[
            {
                "snapshot_id": f"stage1-snapshot-{version}",
                "view": "stage1",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": [candidate_id],
                "active_candidate_count": 1,
                "processed_target_count": 1,
            }
        ],
    )
    return version


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if payload.get("status") != "success":
        raise Stage1ProcessingError(f"probe manifest is not successful: {path}")
    return payload


def resolve_artifact_path(cache_path: str, *, data_dir: Path) -> Path:
    path = Path(cache_path)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_dir.name:
        return data_dir.parent.joinpath(*path.parts)
    return data_dir / path


__all__ = [
    "MODEL_VERSION",
    "Stage1Config",
    "Stage1ProcessingError",
    "Stage1Processor",
    "load_manifest",
    "publish_stage1_result",
    "resolve_artifact_path",
]
