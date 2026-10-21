#!/usr/bin/env python3
"""Process one successful MAST probe manifest through Stage 1."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pipeline.stage1 import (
    Stage1ProcessingError,
    Stage1Processor,
    load_manifest,
    publish_stage1_result,
    resolve_artifact_path,
)
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, help="Successful MAST probe manifest")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("DATA_DIR", "/app/data")),
        help="Shared data directory (default: DATA_DIR or /app/data)",
    )
    return parser.parse_args()


def latest_manifest(data_dir: Path) -> Path:
    manifests = sorted(
        data_dir.glob("manifests/mast-*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in manifests:
        try:
            if json.loads(path.read_text()).get("status") == "success":
                return path
        except (OSError, json.JSONDecodeError):
            continue
    raise Stage1ProcessingError(f"no successful MAST manifest found under {data_dir / 'manifests'}")


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir
    manifest_path = args.manifest or latest_manifest(data_dir)
    manifest = load_manifest(manifest_path)
    artifact_path = resolve_artifact_path(manifest["artifact"]["cache_path"], data_dir=data_dir)
    if not artifact_path.exists():
        raise Stage1ProcessingError(f"cached FITS artifact does not exist: {artifact_path}")

    paths = DataPaths(data_dir)
    paths.ensure()
    result = Stage1Processor().process(
        artifact_path,
        target_manifest=manifest["target_manifest"],
        product_id=manifest["artifact"]["data_uri"],
        normalized_dir=paths.normalized,
    )
    version = publish_stage1_result(
        DuckDBStore(paths.database),
        result,
        target_manifest=manifest["target_manifest"],
        query_config=manifest["query"],
        product_checksum=manifest["artifact"].get("sha256"),
    )
    print(
        json.dumps(
            {
                "status": "published",
                "publish_version": version,
                "candidate_id": result["candidate_id"],
                "target_id": result["target_id"],
                "classification": result["status"],
                "evidence_score": result["evidence_score"],
                "model_version": result["model_version"],
                "normalized_path": result["normalized_path"],
                "features": result["features"],
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
