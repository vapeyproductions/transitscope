#!/usr/bin/env python3
"""Create the durable Stage 1 target queue manifest."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from batching import generate_target_universe, load_probe_seed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(os.getenv("DATA_DIR", "/app/data")) / "manifests" / "stage1-target-universe.jsonl",
    )
    parser.add_argument("--universe-size", type=int, default=126_000)
    parser.add_argument("--probe-manifest", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seed_rows = [load_probe_seed(args.probe_manifest)] if args.probe_manifest else []
    written = generate_target_universe(
        args.output,
        universe_size=args.universe_size,
        seed_rows=seed_rows,
    )
    print(
        json.dumps(
            {
                "status": "ready",
                "manifest": str(args.output),
                "universe_count": written,
                "selection_policy": "random-stratified-v1",
                "adaptive_sampling": False,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
