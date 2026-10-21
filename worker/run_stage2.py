#!/usr/bin/env python3
"""Evaluate one eligible candidate against its available historical TESS data."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from ingestion.mast_adapter import MastProbeConfig
from ingestion.mast_history import MastHistoryAdapter
from pipeline.stage2 import (
    Stage2Config,
    Stage2ProcessingError,
    Stage2Processor,
    load_stage1_input,
    publish_stage2_result,
)
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def parse_args() -> argparse.Namespace:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--data-dir", type=Path, default=data_dir)
    parser.add_argument("--history-path", action="append", type=Path, default=[])
    parser.add_argument("--history-manifest", type=Path)
    parser.add_argument("--fetch-sectors", help="Comma-separated TESS sectors to query through MAST")
    parser.add_argument("--mast-config", type=Path, default=Path("config/mast_probe.json"))
    parser.add_argument("--target-lookback-days", type=float, default=730.0)
    parser.add_argument("--minimum-coverage-days", type=float, default=180.0)
    parser.add_argument("--minimum-events", type=int, default=2)
    parser.add_argument("--allow-nonpromising", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def history_paths_from_manifest(path: Path, data_dir: Path) -> list[Path]:
    payload = json.loads(path.read_text())
    paths: list[Path] = []
    for record in payload.get("records", []):
        cache_path = record.get("artifact", {}).get("cache_path")
        if cache_path:
            paths.append(resolve_path(str(cache_path), data_dir))
    return paths


def resolve_path(value: str, data_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_dir.name:
        return data_dir.parent.joinpath(*path.parts)
    return data_dir / path


def discover_cached_paths(target_id: str, data_dir: Path) -> list[Path]:
    digits = re.sub(r"\D", "", target_id)
    if not digits:
        return []
    token = digits.zfill(16)
    return sorted(
        path
        for path in (data_dir / "raw").rglob("*.fits")
        if token in path.name or digits in path.name
    )


def main() -> int:
    args = parse_args()
    paths = DataPaths(args.data_dir)
    store = DuckDBStore(paths.database)
    stage1_input = load_stage1_input(store, args.candidate_id)
    if stage1_input is None:
        raise SystemExit(f"candidate or Stage 1 evaluation not found: {args.candidate_id}")
    if stage1_input["status"] != "Promising" and not args.allow_nonpromising:
        print(
            json.dumps(
                {
                    "status": "not_eligible",
                    "candidate_id": args.candidate_id,
                    "stage1_status": stage1_input["status"],
                    "message": "Stage 2 does not promote a candidate from rank alone; Stage 1 must be Promising.",
                },
                indent=2,
            )
        )
        return 0

    history_paths = list(args.history_path)
    if args.history_manifest:
        history_paths.extend(history_paths_from_manifest(args.history_manifest, args.data_dir))
    if args.fetch_sectors:
        values = (
            json.loads(args.mast_config.read_text())
            if args.mast_config.exists()
            else {
                "data_dir": str(args.data_dir),
                "sector": 1,
                "product_family": "TESS SPOC light curve",
                "max_observations": 5,
                "request_timeout_seconds": 60,
            }
        )
        values["target_name"] = re.sub(r"\D", "", stage1_input["target_id"])
        config = MastProbeConfig.from_mapping(values, data_dir_override=args.data_dir)
        sectors = [int(value.strip()) for value in args.fetch_sectors.split(",") if value.strip()]
        history = MastHistoryAdapter(config, sectors).fetch()
        history_paths.extend(history_paths_from_manifest(Path(history["manifest_path"]), args.data_dir))

    history_paths = sorted({path.resolve() for path in history_paths if path.exists()})
    processor = Stage2Processor(
        Stage2Config(
            target_lookback_days=args.target_lookback_days,
            minimum_coverage_days=args.minimum_coverage_days,
            minimum_events=args.minimum_events,
        )
    )
    if history_paths:
        result = processor.evaluate(
            history_paths,
            candidate_id=args.candidate_id,
            target_id=stage1_input["target_id"],
            stage1_features=stage1_input["features"],
            normalized_dir=paths.normalized,
        )
    else:
        result = processor.missing_history_result(
            candidate_id=args.candidate_id,
            target_id=stage1_input["target_id"],
            stage1_features=stage1_input["features"],
        )
        result["normalized_path"] = stage1_input["features"].get("normalized_path")

    if not args.dry_run:
        version = publish_stage2_result(
            store,
            result,
            stage1_score=stage1_input["stage1_score"],
            input_product_ids=[str(path) for path in history_paths],
        )
    else:
        version = None
    print(
        json.dumps(
            {
                "status": "evaluated",
                "publish_version": version,
                "candidate_id": args.candidate_id,
                "target_id": stage1_input["target_id"],
                "stage1_status": stage1_input["status"],
                "stage2_status": result["status"],
                "evidence_score": result["evidence_score"],
                "model_version": result["model_version"],
                "history_products": [str(path) for path in history_paths],
                "features": result["features"],
                "dry_run": args.dry_run,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
