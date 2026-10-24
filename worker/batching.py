"""Bounded Stage 1 batching, checkpointing, and replacement selection."""

from __future__ import annotations

import hashlib
import itertools
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pipeline.stage1 import Stage1Config, Stage1ProcessingError, Stage1Processor, resolve_artifact_path
from pipeline.lifecycle import STATUS_PRIORITY, movement_for_ranks, overall_sort_key
from ingestion.mast_adapter import MastProbeError, attach_product_to_target
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


STAGE = "stage1"
SELECTION_POLICY = "random-stratified-v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class BatchConfig:
    batch_size: int = 16
    max_workers: int = 4
    top_n: int = 100
    stage_capacity: int = 25
    target_pool_size: int = 500
    selection_policy: str = SELECTION_POLICY
    adaptive_sampling: bool = False

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if self.top_n < 1:
            raise ValueError("top_n must be at least 1")
        if self.stage_capacity < 1:
            raise ValueError("stage_capacity must be at least 1")
        if self.target_pool_size < 1:
            raise ValueError("target_pool_size must be at least 1")
        if self.adaptive_sampling:
            raise ValueError("adaptive sampling is intentionally disabled until outcome statistics exist")


def _json_line(row: Mapping[str, Any]) -> str:
    return json.dumps(dict(row), sort_keys=True, separators=(",", ":")) + "\n"


def load_probe_seed(probe_manifest: Path) -> dict[str, Any]:
    payload = json.loads(probe_manifest.read_text())
    if payload.get("status") != "success":
        raise ValueError(f"probe manifest is not successful: {probe_manifest}")
    target = dict(payload["target_manifest"])
    artifact = payload["artifact"]
    target.update(
        {
            "artifact_path": artifact["cache_path"],
            "product_id": artifact.get("data_uri", ""),
            "product_checksum": artifact.get("sha256"),
            "manifest_status": "queued",
        }
    )
    return target


def generate_target_universe(
    path: Path,
    *,
    universe_size: int,
    seed_rows: Sequence[Mapping[str, Any]] = (),
    defer_placeholders: bool = False,
) -> int:
    """Write a compact queue manifest without loading it into the browser.

    Seed rows are real provider-backed targets. Remaining rows are intentionally
    metadata-only queue slots: the MAST adapter will attach products when those
    targets are selected. This keeps the universe size explicit without
    pretending that placeholder rows are observations.
    """

    if universe_size < 1:
        raise ValueError("universe_size must be at least 1")
    path.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        for source in seed_rows:
            row = dict(source)
            target_id = str(row.get("target_id", "")).strip()
            if not target_id or target_id in seen:
                continue
            seen.add(target_id)
            row.setdefault("provider_target_id", target_id)
            row.setdefault("selection_bucket", f"stratum-{written % 32:02d}")
            row.setdefault("manifest_status", "queued")
            handle.write(_json_line(row))
            written += 1
            if written >= universe_size:
                break

        placeholder_index = 0
        while written < universe_size:
            target_id = f"queued-target-{placeholder_index:09d}"
            placeholder_index += 1
            if target_id in seen:
                continue
            row = {
                "target_id": target_id,
                "provider_target_id": target_id,
                "ra_deg": None,
                "dec_deg": None,
                "sector": None,
                "cadence": "pending provider lookup",
                "source_product_family": "TESS SPOC light curve",
                "selection_bucket": f"stratum-{written % 32:02d}",
                "manifest_status": "deferred" if defer_placeholders else "queued",
            }
            handle.write(_json_line(row))
            written += 1
    return written


def iter_manifest(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("target_id"):
                yield row


def manifest_count(path: Path) -> int:
    return sum(1 for _ in iter_manifest(path))


def _processed_target_ids(store: DuckDBStore) -> set[str]:
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT target_id FROM target_manifest WHERE manifest_status = 'processed'"
        ).fetchall()
    return {str(row[0]) for row in rows}


def checkpoint_for(store: DuckDBStore, manifest_path: Path) -> dict[str, Any] | None:
    store.initialize()
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT stage, manifest_path, cursor, universe_count, processed_count,
                   last_batch_id, selection_policy, adaptive_sampling_enabled, updated_at
            FROM processing_checkpoints
            WHERE stage = ?
            """,
            [STAGE],
        ).fetchone()
    if not row:
        return None
    if Path(str(row[1])).resolve() != manifest_path.resolve():
        # A new populated queue is a legitimate deployment event: the earlier
        # checkpoint belongs to a different queue and must not move its cursor
        # into the new manifest.  The next publish atomically replaces the
        # checkpoint row for this stage.
        return None
    return {
        "stage": row[0],
        "manifest_path": row[1],
        "cursor": int(row[2]),
        "universe_count": int(row[3]),
        "processed_count": int(row[4]),
        "last_batch_id": row[5],
        "selection_policy": row[6],
        "adaptive_sampling_enabled": bool(row[7]),
        "updated_at": row[8],
    }


def select_next_targets(
    manifest_path: Path,
    *,
    cursor: int,
    count: int,
    processed_ids: set[str],
) -> tuple[list[dict[str, Any]], int]:
    """Stream the next deterministic stratified slice and return its new cursor."""

    if count < 1:
        return [], cursor
    stream = iter_manifest(manifest_path)
    skipped = itertools.islice(stream, cursor, None)
    selected: list[dict[str, Any]] = []
    next_cursor = cursor
    for row in skipped:
        next_cursor += 1
        if row.get("manifest_status") == "deferred":
            continue
        if row["target_id"] in processed_ids:
            continue
        selected.append(row)
        if len(selected) >= count:
            break
    return selected, next_cursor


def _process_target(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(payload["target"])
    data_dir = Path(str(payload["data_dir"]))
    artifact_value = row.get("artifact_path") or row.get("cache_path")
    if not artifact_value:
        try:
            row = attach_product_to_target(
                row,
                data_dir=data_dir,
                request_timeout_seconds=int(os.getenv("MAST_REQUEST_TIMEOUT_SECONDS", "60")),
            )
            artifact_value = row.get("artifact_path") or row.get("cache_path")
        except (MastProbeError, OSError, ValueError) as exc:
            return {"kind": "failed", "target": row, "error": str(exc)}
    if not artifact_value:
        return {"kind": "failed", "target": row, "error": "MAST returned no cached product"}
    artifact_path = resolve_artifact_path(str(artifact_value), data_dir=data_dir)
    if not artifact_path.exists():
        return {
            "kind": "failed",
            "target": row,
            "error": f"cached product is unavailable: {artifact_path}",
        }
    try:
        results = Stage1Processor(
            Stage1Config(
                max_signals_per_target=int(os.getenv("STAGE1_MAX_SIGNALS_PER_TARGET", "3")),
                additional_signal_snr=float(os.getenv("STAGE1_ADDITIONAL_SIGNAL_SNR", "7.0")),
            )
        ).process_signals(
            artifact_path,
            target_manifest=row,
            product_id=str(row.get("product_id") or row.get("data_uri") or artifact_path.name),
            normalized_dir=data_dir / "normalized",
        )
        return {"kind": "success", "target": row, "results": results}
    except (OSError, Stage1ProcessingError, ValueError) as exc:
        return {"kind": "failed", "target": row, "error": str(exc)}


def process_targets(
    targets: Sequence[Mapping[str, Any]],
    *,
    data_dir: Path,
    max_workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payloads = [{"target": dict(target), "data_dir": str(data_dir)} for target in targets]
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    if max_workers == 1 or len(payloads) <= 1:
        raw_results = [_process_target(payload) for payload in payloads]
    else:
        raw_results = []
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_process_target, payload) for payload in payloads]
            for future in as_completed(futures):
                try:
                    raw_results.append(future.result())
                except Exception as exc:  # keep one worker failure from losing the batch
                    raw_results.append({"kind": "failed", "target": {}, "error": repr(exc)})
    for result in raw_results:
        if result.get("kind") == "success":
            for signal_result in result.get("results", []):
                results.append({"target": result["target"], "result": signal_result})
        else:
            failures.append(result)
    results.sort(key=lambda item: str(item["result"]["candidate_id"]))
    failures.sort(key=lambda item: str(item.get("target", {}).get("target_id", "")))
    return results, failures


def _existing_candidates(store: DuckDBStore) -> list[dict[str, Any]]:
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT candidate_id, signal_id, target_id, current_stage, status, overall_score, stage_score,
                   rank_overall, rank_stage, activity, last_evaluated_at, known_match_id, active,
                   processing_slot_occupied
            FROM candidates
            """
        ).fetchall()
    return [
        {
            "candidate_id": row[0],
            "signal_id": row[1],
            "target_id": row[2],
            "current_stage": row[3],
            "status": row[4],
            "overall_score": row[5],
            "stage_score": row[6],
            "rank_overall": row[7],
            "rank_stage": row[8],
            "activity": row[9],
            "last_evaluated_at": row[10],
            "known_match_id": row[11],
            "active": bool(row[12]),
            "processing_slot_occupied": bool(row[13]),
        }
        for row in rows
    ]


def _stable_suffix(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _phase(period: float | None, transit_time: float | None) -> float | None:
    if period is None or period <= 0 or transit_time is None:
        return None
    return (transit_time % period) / period


def _signal_matches(existing: Mapping[str, Any], result: Mapping[str, Any]) -> bool:
    if str(existing.get("target_id")) != str(result.get("target_id")):
        return False
    existing_period = _finite_float(existing.get("period_days"))
    result_period = _finite_float(result.get("features", {}).get("period_days"))
    existing_time = _finite_float(existing.get("transit_time"))
    result_time = _finite_float(result.get("features", {}).get("transit_time"))
    if existing_period is None or result_period is None:
        return False
    period_tolerance = float(os.getenv("STAGE1_SIGNAL_PERIOD_TOLERANCE", "0.01"))
    if abs(existing_period - result_period) / max(existing_period, result_period) > period_tolerance:
        return False
    existing_phase = _phase(existing_period, existing_time)
    result_phase = _phase(result_period, result_time)
    if existing_phase is None or result_phase is None:
        return True
    phase_delta = abs(existing_phase - result_phase)
    phase_delta = min(phase_delta, 1.0 - phase_delta)
    return phase_delta <= float(os.getenv("STAGE1_SIGNAL_PHASE_TOLERANCE", "0.05"))


def _existing_stage1_signals(
    store: DuckDBStore,
    *,
    include_advanced: bool = False,
) -> list[dict[str, Any]]:
    """Load durable signal identities, with a legacy-evaluation fallback."""

    candidates = {
        str(candidate["candidate_id"]): candidate
        for candidate in _existing_candidates(store)
        if candidate["active"]
        and (include_advanced or candidate["current_stage"] == STAGE)
    }
    if not candidates:
        return []
    store.initialize()
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT cs.signal_id, cs.candidate_id, cs.target_id, cs.signal_index, cs.signal_key,
                   period_days, transit_time, duration_hours, depth, snr,
                   extraction_model_version, c.current_stage
            FROM candidate_signals cs
            JOIN candidates c ON c.candidate_id = cs.candidate_id
            WHERE c.active = true
              AND (? OR c.current_stage = 'stage1')
            """
            , [include_advanced]
        ).fetchall()
        by_candidate = {
            str(row[1]): {
                "signal_id": row[0],
                "candidate_id": row[1],
                "target_id": row[2],
                "signal_index": row[3],
                "signal_key": row[4],
                "period_days": row[5],
                "transit_time": row[6],
                "duration_hours": row[7],
                "depth": row[8],
                "snr": row[9],
                "extraction_model_version": row[10],
                "current_stage": row[11],
            }
            for row in rows
        }
        for candidate_id, candidate in candidates.items():
            if candidate_id in by_candidate:
                continue
            evaluation = connection.execute(
                """
                SELECT feature_json
                FROM evaluations
                WHERE candidate_id = ? AND stage = 'stage1'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [candidate_id],
            ).fetchone()
            features = {}
            if evaluation:
                try:
                    features = json.loads(evaluation[0])
                except (TypeError, ValueError, json.JSONDecodeError):
                    features = {}
            by_candidate[candidate_id] = {
                "signal_id": candidate.get("signal_id") or f"legacy-{_stable_suffix(candidate_id)}",
                "candidate_id": candidate_id,
                "target_id": candidate["target_id"],
                "signal_index": features.get("signal_index"),
                "signal_key": features.get("signal_key"),
                "period_days": features.get("period_days"),
                "transit_time": features.get("transit_time"),
                "duration_hours": features.get("duration_hours"),
                "depth": features.get("depth"),
                "snr": features.get("bls_snr"),
                "extraction_model_version": features.get("model_version", "legacy"),
                "current_stage": candidate["current_stage"],
            }
    return list(by_candidate.values())


def _bind_signal_identities(
    store: DuckDBStore,
    results: Sequence[Mapping[str, Any]],
    *,
    include_advanced: bool = False,
) -> list[dict[str, Any]]:
    """Reuse a legacy child when its signal still matches; otherwise keep the
    deterministic signal identity generated by Stage 1."""

    existing = _existing_stage1_signals(store, include_advanced=include_advanced)
    bound: list[dict[str, Any]] = []
    used: set[str] = set()
    for item in results:
        result = dict(item["result"])
        features = dict(result.get("features") or {})
        match = next(
            (
                signal
                for signal in existing
                if signal["candidate_id"] not in used and _signal_matches(signal, result)
            ),
            None,
        )
        if match:
            used.add(str(match["candidate_id"]))
            result["candidate_id"] = match["candidate_id"]
            result["signal_id"] = match["signal_id"]
            features["signal_id"] = match["signal_id"]
            features["legacy_candidate_reused"] = True
            old_path = Path(str(result.get("normalized_path"))) if result.get("normalized_path") else None
            new_path = old_path.with_name(f"{match['candidate_id']}.npz") if old_path else None
            if old_path and new_path and old_path.exists() and old_path != new_path:
                old_path.replace(new_path)
                result["normalized_path"] = str(new_path)
        result["features"] = features
        bound.append({"target": dict(item["target"]), "result": result})
    return bound


def publish_stage1_batch(
    store: DuckDBStore,
    *,
    config: BatchConfig,
    batch_id: str,
    manifest_path: Path,
    universe_count: int,
    cursor: int,
    processed_count: int,
    selected: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    failures: Sequence[Mapping[str, Any]],
    retire_candidate_ids: Sequence[str] = (),
    preserve_advanced_candidates: bool = False,
) -> int:
    """Publish one batch, its ranking, the checkpoint, and failure bookkeeping atomically."""

    results = _bind_signal_identities(
        store,
        results,
        include_advanced=preserve_advanced_candidates,
    )
    previous = _existing_candidates(store)
    retiring = set(retire_candidate_ids)
    selected_target_ids = {str(target["target_id"]) for target in selected}
    previous_by_id = {candidate["candidate_id"]: candidate for candidate in previous}
    advanced_result_ids = {
        str(item["result"]["candidate_id"])
        for item in results
        if previous_by_id.get(str(item["result"]["candidate_id"]), {}).get("current_stage") not in {None, STAGE}
    }
    stage1_results = [
        item for item in results if str(item["result"]["candidate_id"]) not in advanced_result_ids
    ]
    result_candidate_ids = {
        str(item["result"]["candidate_id"])
        for item in stage1_results
    }
    # A refreshed Stage 1 target may now emit signal-01, signal-02, etc. Remove
    # the pre-multi-signal legacy row from the active ranking so it cannot
    # coexist with its replacement and inflate the target's score.
    replaced_stage1 = {
        candidate["candidate_id"]
        for candidate in previous
        if candidate["active"]
        and candidate["current_stage"] == STAGE
        and str(candidate["target_id"]) in selected_target_ids
        and candidate["candidate_id"] not in result_candidate_ids
    }
    active_existing = [
        candidate
        for candidate in previous
        if candidate["active"]
        and candidate["candidate_id"] not in retiring
        and candidate["candidate_id"] not in replaced_stage1
        and candidate["candidate_id"] not in result_candidate_ids
    ]
    active_stage1 = [candidate for candidate in active_existing if candidate["current_stage"] == STAGE]
    new_candidates = [
        {
            "candidate_id": result["result"]["candidate_id"],
            "signal_id": result["result"].get("signal_id"),
            "target_id": result["result"]["target_id"],
            "current_stage": STAGE,
            "status": result["result"]["status"],
            "overall_score": result["result"]["evidence_score"],
            "stage_score": result["result"]["evidence_score"],
            "active": True,
            "activity": result["result"]["activity"],
            "last_evaluated_at": result["result"]["evaluated_at"],
            "known_match_id": None,
            "processing_slot_occupied": result["result"]["status"] != "Inconclusive",
        }
        for result in stage1_results
    ]
    stage1_pool = active_stage1 + new_candidates
    stage1_pool.sort(
        key=lambda candidate: (
            -STATUS_PRIORITY.get(str(candidate.get("status")), 0),
            -float(candidate["stage_score"] or 0.0),
            str(candidate["candidate_id"]),
        )
    )
    stage1_ranks = {candidate["candidate_id"]: index for index, candidate in enumerate(stage1_pool, 1)}
    overall_pool = active_existing + new_candidates
    overall_pool.sort(key=overall_sort_key)
    overall_ranks = {candidate["candidate_id"]: index for index, candidate in enumerate(overall_pool, 1)}
    prior_by_id = previous_by_id

    version = (store.latest_published_version() or 0) + 1
    evaluated_at = utc_now()
    candidate_rows: list[dict[str, Any]] = []
    candidate_updates: list[dict[str, Any]] = []
    for candidate in overall_pool:
        candidate_id = candidate["candidate_id"]
        overall_rank = overall_ranks[candidate_id]
        stage_rank = stage1_ranks.get(candidate_id, prior_by_id.get(candidate_id, {}).get("rank_stage"))
        old = prior_by_id.get(candidate_id)
        old_rank = old.get("rank_overall") if old else None
        rank_delta, movement = movement_for_ranks(old_rank, overall_rank)
        if old is None:
            candidate_rows.append(
                {
                    "candidate_id": candidate_id,
                    "signal_id": candidate.get("signal_id"),
                    "target_id": candidate["target_id"],
                    "current_stage": STAGE,
                    "status": candidate["status"],
                    "overall_score": candidate["overall_score"],
                    "stage_score": candidate["stage_score"],
                    "rank_overall": overall_rank,
                    "rank_stage": stage_rank,
                    "activity": candidate["activity"],
                    "rank_delta": rank_delta,
                    "movement_kind": movement,
                    "last_evaluated_at": candidate["last_evaluated_at"],
                    "known_match_id": None,
                    "active": True,
                    "processing_slot_occupied": candidate["processing_slot_occupied"],
                }
            )
            candidate_updates.append(
                {
                    "candidate_id": candidate_id,
                    "signal_id": candidate.get("signal_id"),
                    "current_stage": STAGE,
                    "rank_overall": overall_rank,
                    "rank_stage": stage_rank,
                    "rank_delta": rank_delta,
                    "movement_kind": movement,
                    "active": True,
                    "activity": candidate["activity"],
                    "status": candidate["status"],
                    "overall_score": candidate["overall_score"],
                    "stage_score": candidate["stage_score"],
                    "last_evaluated_at": candidate["last_evaluated_at"],
                    "known_match_id": None,
                    "processing_slot_occupied": candidate["processing_slot_occupied"],
                }
            )
        else:
            candidate_updates.append(
                {
                    "candidate_id": candidate_id,
                    "signal_id": candidate.get("signal_id"),
                    "current_stage": candidate["current_stage"],
                    "rank_overall": overall_rank,
                    "rank_stage": stage_rank,
                    "rank_delta": rank_delta,
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

    refreshed_ids = {str(item["result"]["candidate_id"]) for item in stage1_results}
    for candidate in previous:
        if (
            candidate["candidate_id"] in (retiring | replaced_stage1)
            and candidate["active"]
            and candidate["candidate_id"] not in refreshed_ids
        ):
            candidate_updates.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "rank_overall": None,
                    "rank_stage": None,
                    "rank_delta": -(candidate["rank_stage"] or 0),
                    "movement_kind": "removed",
                    "active": False,
                    "activity": "No current activity",
                    "status": candidate["status"],
                    "overall_score": candidate["overall_score"],
                    "stage_score": candidate["stage_score"],
                    "last_evaluated_at": candidate["last_evaluated_at"],
                    "known_match_id": candidate["known_match_id"],
                }
            )

    target_rows = []
    for target in selected:
        target_rows.append(
            {
                "target_id": target["target_id"],
                "provider_target_id": target.get("provider_target_id", target["target_id"]),
                "ra_deg": target.get("ra_deg"),
                "dec_deg": target.get("dec_deg"),
                "sector": target.get("sector"),
                "cadence": target.get("cadence"),
                "source_product_family": target.get("source_product_family"),
                "selection_bucket": target.get("selection_bucket", "unknown"),
                "manifest_status": "processed"
                if any(item["target"].get("target_id") == target["target_id"] for item in results)
                else "unavailable",
            }
        )

    evaluations = []
    events = []
    signals = []
    for item in results:
        result = item["result"]
        features = dict(result.get("features") or {})
        signals.append(
            {
                "signal_id": result.get("signal_id")
                or features.get("signal_id")
                or f"legacy-{_stable_suffix(result['candidate_id'])}",
                "candidate_id": result["candidate_id"],
                "target_id": result["target_id"],
                "signal_index": features.get("signal_index"),
                "signal_key": features.get("signal_key") or result["candidate_id"],
                "period_days": features.get("period_days"),
                "transit_time": features.get("transit_time"),
                "duration_hours": features.get("duration_hours"),
                "depth": features.get("depth"),
                "snr": features.get("bls_snr"),
                "extraction_model_version": result["model_version"],
                "first_seen_at": result["evaluated_at"],
                "last_seen_at": result["evaluated_at"],
                "active": True,
            }
        )
        suffix = _stable_suffix(str(result.get("product_id", result["target_id"])))
        evaluations.append(
            {
                "evaluation_id": f"stage1-evaluation-{result['candidate_id']}-{suffix}",
                "candidate_id": result["candidate_id"],
                "stage": STAGE,
                "model_version": result["model_version"],
                "data_window_start": None,
                "data_window_end": None,
                "input_product_ids": [result["product_id"]],
                "feature_json": {
                    **features,
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
        )
        events.append(
            {
                "event_id": f"stage1-evaluated-{result['candidate_id']}-{suffix}",
                "sequence_number": version,
                "candidate_id": result["candidate_id"],
                "stage": STAGE,
                "event_type": "stage1_evaluated",
                "payload_json": {
                    "evidence_score": result["evidence_score"],
                    "classification": result["status"],
                    "model_version": result["model_version"],
                    "features": result["features"],
                },
                "occurred_at": result["evaluated_at"],
                "model_run_id": f"stage1-run-{batch_id}",
            }
        )
    for failure in failures:
        target_id = failure.get("target", {}).get("target_id", "unknown")
        events.append(
            {
                "event_id": f"stage1-unavailable-{batch_id}-{target_id}",
                "sequence_number": version,
                "candidate_id": None,
                "stage": STAGE,
                "event_type": "target_unavailable",
                "payload_json": {"target_id": target_id, "error": failure.get("error")},
                "occurred_at": evaluated_at,
                "model_run_id": f"stage1-run-{batch_id}",
            }
        )
    for candidate_id in sorted(retiring | replaced_stage1):
        events.append(
            {
                "event_id": f"stage1-released-{batch_id}-{candidate_id}",
                "sequence_number": version,
                "candidate_id": candidate_id,
                "stage": STAGE,
                "event_type": "stage1_released",
                "payload_json": {
                    "reason": (
                        "multi_signal_replacement"
                        if candidate_id in replaced_stage1
                        else "replacement_slot_opened"
                    )
                },
                "occurred_at": evaluated_at,
                "model_run_id": f"stage1-run-{batch_id}",
            }
        )

    stage1_order = [candidate["candidate_id"] for candidate in stage1_pool[: config.top_n]]
    overall_order = [candidate["candidate_id"] for candidate in overall_pool[: config.top_n]]
    ingest_id = f"stage1-ingest-{batch_id}"
    batch_errors = [
        {"target_id": item.get("target", {}).get("target_id"), "error": item.get("error")}
        for item in failures
    ]
    store.publish_batch(
        version=version,
        note=f"Stage 1 batch {batch_id} using {config.selection_policy}",
        targets=target_rows,
        ingest_runs=[
            {
                "run_id": ingest_id,
                "provider": "MAST",
                "query_config_json": {
                    "stage": STAGE,
                    "batch_id": batch_id,
                    "selection_policy": config.selection_policy,
                    "adaptive_sampling": config.adaptive_sampling,
                    "max_workers": config.max_workers,
                },
                "started_at": evaluated_at,
                "finished_at": evaluated_at,
                "status": "complete" if not failures else "partial",
                "targets_seen": len(selected),
                "products_downloaded": len(results),
                "products_failed": len(failures),
                "error_json": batch_errors or None,
            }
        ],
        candidates=candidate_rows,
        signals=signals,
        evaluations=evaluations,
        events=events,
        coverage_watermarks=[
            {
                "candidate_id": item["result"]["candidate_id"],
                "stage": STAGE,
                "coverage_signature": _stable_suffix(
                    json.dumps([item["result"]["product_id"]], sort_keys=True)
                ),
                "coverage_start": item["result"].get("window_start"),
                "coverage_end": item["result"].get("window_end"),
                "product_count": 1,
                "window_count": 1,
                "updated_at": item["result"]["evaluated_at"],
            }
            for item in results
        ],
        snapshots=[
            {
                "snapshot_id": f"stage1-snapshot-{version}",
                "view": STAGE,
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": stage1_order,
                "active_candidate_count": len(stage1_pool),
                "processed_target_count": processed_count,
            },
            {
                "snapshot_id": f"overall-snapshot-{version}",
                "view": "overall",
                "sequence_number": version,
                "occurred_at": evaluated_at,
                "candidate_order_json": overall_order,
                "active_candidate_count": len(overall_pool),
                "processed_target_count": processed_count,
            }
        ],
        candidate_updates=candidate_updates,
        batch_run={
            "batch_id": batch_id,
            "stage": STAGE,
            "selection_policy": config.selection_policy,
            "requested_count": len(selected),
            "selected_count": len(selected),
            "evaluated_count": len(results),
            "failed_count": len(failures),
            "max_workers": config.max_workers,
            "status": "complete" if not failures else "partial",
            "started_at": evaluated_at,
            "finished_at": evaluated_at,
            "error_json": batch_errors or None,
        },
        checkpoint={
            "stage": STAGE,
            "manifest_path": str(manifest_path),
            "cursor": cursor,
            "universe_count": universe_count,
            "processed_count": processed_count,
            "last_batch_id": batch_id,
            "selection_policy": config.selection_policy,
            "adaptive_sampling_enabled": config.adaptive_sampling,
            "updated_at": evaluated_at,
        },
    )
    return version


def run_batch(
    *,
    data_dir: Path,
    manifest_path: Path,
    config: BatchConfig,
    retire_candidate_ids: Sequence[str] = (),
) -> dict[str, Any]:
    paths = DataPaths(data_dir)
    paths.ensure()
    store = DuckDBStore(paths.database)
    store.initialize()
    universe_count = manifest_count(manifest_path)
    checkpoint = checkpoint_for(store, manifest_path)
    cursor = int(checkpoint["cursor"]) if checkpoint else 0
    processed_count = int(checkpoint["processed_count"]) if checkpoint else 0
    if checkpoint and checkpoint["selection_policy"] != config.selection_policy:
        raise ValueError(
            f"selection policy differs from checkpoint: {checkpoint['selection_policy']} != {config.selection_policy}"
        )
    processed_ids = _processed_target_ids(store)
    requested_count = config.batch_size + len(retire_candidate_ids)
    selected, next_cursor = select_next_targets(
        manifest_path,
        cursor=cursor,
        count=requested_count,
        processed_ids=processed_ids,
    )
    if not selected:
        return {
            "status": "complete",
            "message": "manifest exhausted; no new targets selected",
            "cursor": next_cursor,
            "universe_count": universe_count,
            "processed_count": processed_count,
            "publish_version": store.latest_published_version(),
        }
    batch_id = f"stage1-batch-{cursor:09d}-{next_cursor:09d}"
    results, failures = process_targets(
        selected,
        data_dir=data_dir,
        max_workers=min(config.max_workers, len(selected)),
    )
    version = publish_stage1_batch(
        store,
        config=config,
        batch_id=batch_id,
        manifest_path=manifest_path,
        universe_count=universe_count,
        cursor=next_cursor,
        processed_count=processed_count + len(selected),
        selected=selected,
        results=results,
        failures=failures,
        retire_candidate_ids=retire_candidate_ids,
    )
    return {
        "status": "published",
        "publish_version": version,
        "batch_id": batch_id,
        "selection_policy": config.selection_policy,
        "adaptive_sampling": config.adaptive_sampling,
        "cursor": next_cursor,
        "universe_count": universe_count,
        "processed_count": processed_count + len(selected),
        "selected_count": len(selected),
        "evaluated_count": len(results),
        "failed_count": len(failures),
        "max_workers": min(config.max_workers, len(selected)),
        "candidate_ids": [item["result"]["candidate_id"] for item in results],
        "replacement_slots": len(retire_candidate_ids),
    }


def run_stage1_refresh(
    *,
    data_dir: Path,
    manifest_path: Path,
    targets: Sequence[Mapping[str, Any]],
    config: BatchConfig,
    preserve_advanced_candidates: bool = False,
) -> dict[str, Any]:
    """Re-evaluate Stage 1 candidates whose MAST coverage advanced.

    Refreshes do not consume new-universe cursor positions.  They replace the
    candidate's previous Stage 1 evaluation in the current ranking while
    preserving the manifest checkpoint and replay history.
    """

    if not targets:
        return {"status": "idle", "message": "no Stage 1 refresh targets"}
    paths = DataPaths(data_dir)
    paths.ensure()
    store = DuckDBStore(paths.database)
    store.initialize()
    checkpoint = checkpoint_for(store, manifest_path)
    cursor = int(checkpoint["cursor"]) if checkpoint else 0
    processed_count = int(checkpoint["processed_count"]) if checkpoint else 0
    universe_count = int(checkpoint["universe_count"]) if checkpoint else manifest_count(manifest_path)
    results, failures = process_targets(
        targets,
        data_dir=data_dir,
        max_workers=min(config.max_workers, len(targets)),
    )
    successful_ids = [str(item["result"]["candidate_id"]) for item in results]
    batch_id = f"stage1-refresh-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}"
    version = publish_stage1_batch(
        store,
        config=config,
        batch_id=batch_id,
        manifest_path=manifest_path,
        universe_count=universe_count,
        cursor=cursor,
        processed_count=processed_count,
        selected=targets,
        results=results,
        failures=failures,
        preserve_advanced_candidates=preserve_advanced_candidates,
    )
    return {
        "status": "published",
        "publish_version": version,
        "batch_id": batch_id,
        "refresh_count": len(targets),
        "evaluated_count": len(results),
        "failed_count": len(failures),
        "candidate_ids": successful_ids,
    }


__all__ = [
    "BatchConfig",
    "SELECTION_POLICY",
    "checkpoint_for",
    "generate_target_universe",
    "load_probe_seed",
    "manifest_count",
    "run_batch",
    "run_stage1_refresh",
    "select_next_targets",
]
