#!/usr/bin/env python3
"""Backfill multi-signal Stage 1 identities for already explored targets.

The command is intentionally explicit. It does not run from the normal worker
heartbeat, and it should only be launched while ``RUN_LIVE_PIPELINE`` is false.
It reuses the existing Stage 1 queue rows, fetches a current MAST product when
needed, preserves candidates already in Stage 2/3, and creates independent
children for newly detected signals.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from batching import BatchConfig, run_stage1_refresh
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def parse_args() -> argparse.Namespace:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=data_dir)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=data_dir / "manifests" / "stage1-real-target-universe.jsonl",
    )
    parser.add_argument("--max-targets", type=int, default=0)
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("STAGE1_MAX_WORKERS", "4")))
    parser.add_argument("--top-n", type=int, default=int(os.getenv("TOP_N_DISPLAY", "500")))
    return parser.parse_args()


def active_explored_targets(store: DuckDBStore, *, max_targets: int = 0) -> list[dict[str, object]]:
    store.initialize()
    limit = "LIMIT ?" if max_targets > 0 else ""
    params = [max_targets] if max_targets > 0 else []
    with store.connect() as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT c.target_id, tm.provider_target_id, tm.ra_deg, tm.dec_deg,
                   tm.sector, tm.cadence, tm.source_product_family, tm.selection_bucket
            FROM candidates c
            LEFT JOIN target_manifest tm ON tm.target_id = c.target_id
            WHERE c.active = true
            ORDER BY c.target_id
            {limit}
            """,
            params,
        ).fetchall()
    return [
        {
            "target_id": row[0],
            "provider_target_id": row[1] or row[0],
            "ra_deg": row[2],
            "dec_deg": row[3],
            "sector": row[4],
            "cadence": row[5],
            "source_product_family": row[6] or "TESS SPOC light curve",
            "selection_bucket": row[7] or "multisignal-backfill",
            "manifest_status": "queued",
        }
        for row in rows
    ]


def main() -> int:
    args = parse_args()
    if os.getenv("RUN_LIVE_PIPELINE", "false").strip().lower() in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing migration while RUN_LIVE_PIPELINE is true; pause the worker first.")
    if os.getenv("PIPELINE_PAUSED", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing migration while PIPELINE_PAUSED is false; pause the worker first.")
    paths = DataPaths(args.data_dir)
    paths.ensure()
    store = DuckDBStore(paths.database)
    targets = active_explored_targets(store, max_targets=args.max_targets)
    if not targets:
        print(json.dumps({"status": "idle", "message": "no active explored targets"}, indent=2))
        return 0
    result = run_stage1_refresh(
        data_dir=args.data_dir,
        manifest_path=args.manifest,
        targets=targets,
        config=BatchConfig(
            batch_size=len(targets),
            max_workers=max(1, args.max_workers),
            top_n=max(1, args.top_n),
            target_pool_size=int(os.getenv("STAGE1_POOL_SIZE", "500")),
        ),
        preserve_advanced_candidates=True,
    )
    print(json.dumps({"status": "published", "targets": len(targets), **result}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
