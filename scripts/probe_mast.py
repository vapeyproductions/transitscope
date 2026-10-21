#!/usr/bin/env python3
"""Run one bounded, provenance-preserving MAST/TESS ingestion probe."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "worker"))

from ingestion.mast_adapter import MastProbeConfig, MastProbeError, MastProductAdapter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "config" / "mast_probe.json",
        help="JSON probe configuration (default: config/mast_probe.json)",
    )
    parser.add_argument("--data-dir", type=Path, help="Override the configured data directory")
    parser.add_argument("--sector", type=int, help="Override the TESS sector")
    parser.add_argument("--target-name", help="Override the TIC target name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        values = json.loads(args.config.read_text())
        if args.sector is not None:
            values["sector"] = args.sector
        if args.target_name is not None:
            values["target_name"] = args.target_name
        config = MastProbeConfig.from_mapping(values, data_dir_override=args.data_dir)
        result = MastProductAdapter(config).probe()
    except (OSError, ValueError, json.JSONDecodeError, MastProbeError) as exc:
        print(f"MAST probe failed: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
