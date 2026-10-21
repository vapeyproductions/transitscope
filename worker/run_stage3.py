#!/usr/bin/env python3
"""Evaluate one eligible candidate with a chronological Stage 3 holdout."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from ingestion.mast_adapter import MastProbeConfig
from ingestion.mast_history import MastHistoryAdapter
from pipeline.stage3 import (
    Stage3Config,
    Stage3Processor,
    load_stage2_input,
    publish_stage3_result,
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
    parser.add_argument("--minimum-total-samples", type=int, default=90)
    parser.add_argument("--minimum-samples-per-window", type=int, default=30)
    parser.add_argument("--allow-nonpromising", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def resolve_path(value: str, data_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_dir.name:
        return data_dir.parent.joinpath(*path.parts)
    return data_dir / path


def history_paths_from_manifest(path: Path, data_dir: Path) -> list[Path]:
    payload = json.loads(path.read_text())
    return [
        resolve_path(str(record["artifact"]["cache_path"]), data_dir)
        for record in payload.get("records", [])
        if record.get("artifact", {}).get("cache_path")
    ]


def main() -> int:
    args = parse_args()
    paths = DataPaths(args.data_dir)
    store = DuckDBStore(paths.database)
    stage2_input = load_stage2_input(store, args.candidate_id)
    if stage2_input is None:
        raise SystemExit(f"candidate or Stage 2 evaluation not found: {args.candidate_id}")
    if (
        stage2_input["current_stage"] != "stage2"
        or stage2_input["status"] != "Promising"
    ) and not args.allow_nonpromising:
        print(
            json.dumps(
                {
                    "status": "not_eligible",
                    "candidate_id": args.candidate_id,
                    "stage2_status": stage2_input["status"],
                    "current_stage": stage2_input["current_stage"],
                    "message": "Stage 3 requires an explicitly Promising Stage 2 candidate; rank alone does not promote it.",
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
        values["target_name"] = re.sub(r"\D", "", stage2_input["target_id"])
        config = MastProbeConfig.from_mapping(values, data_dir_override=args.data_dir)
        sectors = [int(value.strip()) for value in args.fetch_sectors.split(",") if value.strip()]
        history = MastHistoryAdapter(config, sectors).fetch()
        history_paths.extend(history_paths_from_manifest(Path(history["manifest_path"]), args.data_dir))

    history_paths = sorted({path.resolve() for path in history_paths if path.exists()})
    processor = Stage3Processor(
        Stage3Config(
            minimum_total_samples=args.minimum_total_samples,
            minimum_samples_per_window=args.minimum_samples_per_window,
        )
    )
    if history_paths:
        result = processor.evaluate(
            history_paths,
            candidate_id=args.candidate_id,
            target_id=stage2_input["target_id"],
            stage2_features=stage2_input["features"],
            normalized_dir=paths.normalized,
        )
    else:
        normalized_path = stage2_input["features"].get("normalized_path")
        if normalized_path:
            candidate_path = resolve_path(str(normalized_path), args.data_dir)
        else:
            candidate_path = None
        if candidate_path and candidate_path.exists():
            import numpy as np

            with np.load(candidate_path) as archive:
                result = processor.evaluate_series(
                    archive["time"],
                    archive["normalized_flux"],
                    candidate_id=args.candidate_id,
                    target_id=stage2_input["target_id"],
                    stage2_features=stage2_input["features"],
                    normalized_dir=paths.normalized,
                    product_count=1,
                )
        else:
            result = processor.missing_history_result(
                candidate_id=args.candidate_id,
                target_id=stage2_input["target_id"],
                stage2_features=stage2_input["features"],
            )

    if not args.dry_run:
        version = publish_stage3_result(
            store,
            result,
            stage1_score=stage2_input["stage1_score"],
            stage2_score=stage2_input["stage2_score"],
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
                "target_id": stage2_input["target_id"],
                "stage2_status": stage2_input["status"],
                "stage3_status": result["status"],
                "evidence_score": result["evidence_score"],
                "model_version": result["model_version"],
                "baseline_model_version": result["features"].get("baseline_model_version"),
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
