#!/usr/bin/env python3
"""Create a random, metadata-first target universe from public MAST observations.

The script queries observation metadata only.  FITS light curves are fetched
later, when a target enters a bounded Stage 1 batch, so initial population does
not require storing 126,000 raw products.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
from pathlib import Path
from typing import Any

from batching import generate_target_universe
from ingestion.mast_adapter import MastProbeConfig, MastProductAdapter


def _digits(value: object) -> str | None:
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def _sectors(value: str) -> list[int]:
    result: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = (int(item) for item in part.split("-", 1))
            result.extend(range(min(start, end), max(start, end) + 1))
        else:
            result.append(int(part))
    return sorted(set(sector for sector in result if sector > 0))


def parse_args() -> argparse.Namespace:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=data_dir,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=data_dir / "manifests" / "stage1-real-target-universe.jsonl",
    )
    parser.add_argument("--universe-size", type=int, default=126_000)
    parser.add_argument("--sectors", default=os.getenv("MAST_UNIVERSE_SECTORS", "1-26"))
    parser.add_argument(
        "--per-sector-limit",
        type=int,
        default=int(os.getenv("MAST_UNIVERSE_PER_SECTOR", "8000")),
    )
    parser.add_argument("--seed", type=int, default=20261024)
    parser.add_argument("--request-timeout-seconds", type=int, default=60)
    return parser.parse_args()


def _target_row(observation: dict[str, Any], sector: int) -> dict[str, Any] | None:
    provider_target_id = _digits(observation.get("target_name"))
    if not provider_target_id:
        return None
    return {
        "target_id": f"TIC {provider_target_id}",
        "provider_target_id": provider_target_id,
        "ra_deg": observation.get("s_ra"),
        "dec_deg": observation.get("s_dec"),
        "sector": sector,
        "cadence": "SPOC timeseries",
        "source_product_family": "TESS SPOC light curve",
        "source_observation_id": observation.get("obsid"),
        "source_observation_name": observation.get("obs_id"),
        "selection_bucket": f"mast-sector-{sector:04d}",
        "manifest_status": "queued",
    }


def main() -> int:
    args = parse_args()
    if args.universe_size < 1 or args.per_sector_limit < 1:
        raise SystemExit("universe-size and per-sector-limit must be positive")
    sectors = _sectors(args.sectors)
    if not sectors:
        raise SystemExit("at least one positive MAST sector is required")

    unique: dict[str, dict[str, Any]] = {}
    for sector in sectors:
        adapter = MastProductAdapter(
            MastProbeConfig(
                data_dir=args.data_dir,
                sector=sector,
                target_name=None,
                max_observations=min(5000, args.per_sector_limit),
                request_timeout_seconds=args.request_timeout_seconds,
            )
        )
        for observation in adapter.query_sector_observations(
            sector,
            max_records=args.per_sector_limit,
        ):
            row = _target_row(observation, sector)
            if row:
                # Preserve the first observed sector while deduplicating TICs.
                unique.setdefault(row["provider_target_id"], row)
        if len(unique) >= args.universe_size:
            break

    rows = list(unique.values())
    random.Random(args.seed).shuffle(rows)
    seeds = rows[: args.universe_size]
    written = generate_target_universe(
        args.output,
        universe_size=args.universe_size,
        seed_rows=seeds,
        defer_placeholders=True,
    )
    report = {
        "schema_version": "mast-target-universe.v1",
        "status": "success" if seeds else "failed",
        "provider": "MAST",
        "sectors": sectors,
        "seed": args.seed,
        "universe_size": written,
        "provider_backed_targets": len(seeds),
        "deferred_targets": max(0, written - len(seeds)),
        "manifest_path": str(args.output),
    }
    report_path = args.data_dir / "manifests" / "mast-target-universe-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if seeds else 1


if __name__ == "__main__":
    raise SystemExit(main())
