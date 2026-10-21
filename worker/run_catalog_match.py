#!/usr/bin/env python3
"""Cross-reference active candidates with a cached NASA Exoplanet Archive projection."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ingestion.exoplanet_archive import ExoplanetArchiveAdapter, ExoplanetArchiveConfig
from pipeline.lifecycle import candidate_match_inputs, publish_known_matches
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def parse_args() -> argparse.Namespace:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-id", action="append", default=[])
    parser.add_argument("--data-dir", type=Path, default=data_dir)
    parser.add_argument("--cache-path", type=Path)
    parser.add_argument("--config", type=Path, default=Path("config/exoplanet_archive.json"))
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = DataPaths(args.data_dir)
    store = DuckDBStore(paths.database)
    values = json.loads(args.config.read_text()) if args.config.exists() else {"data_dir": str(args.data_dir)}
    config = ExoplanetArchiveConfig.from_mapping(values, data_dir_override=args.data_dir)
    adapter = ExoplanetArchiveAdapter(config)
    if args.cache_path:
        payload = json.loads(args.cache_path.read_text())
        cache_path = str(args.cache_path)
    else:
        payload = adapter.fetch(force=args.force_refresh)
        cache_path = str(payload.get("cache_path") or adapter.cache_path)
    records = [adapter._record_from_cache(record) for record in payload.get("records", [])]
    candidates = candidate_match_inputs(store, args.candidate_id or None)
    matches: list[dict[str, object]] = []
    for candidate in candidates:
        match = adapter.match_candidate(
            target_id=candidate["target_id"],
            ra_deg=candidate["ra_deg"],
            dec_deg=candidate["dec_deg"],
            period_days=candidate["period_days"],
            records=records,
        )
        if match:
            matches.append({"candidate_id": candidate["candidate_id"], **match})

    version = None
    if matches and not args.dry_run:
        version = publish_known_matches(
            store,
            matches,
            catalog_cache_path=cache_path,
            top_n=args.top_n,
        )
    print(
        json.dumps(
            {
                "status": "matched" if matches else "no_matches",
                "publish_version": version,
                "cache_path": cache_path,
                "catalog_status": payload.get("status"),
                "candidate_count": len(candidates),
                "matches": matches,
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
