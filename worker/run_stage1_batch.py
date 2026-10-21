#!/usr/bin/env python3
"""Process one bounded, checkpointed Stage 1 batch."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from batching import BatchConfig, run_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser.add_argument("--data-dir", type=Path, default=data_dir)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(
            os.getenv(
                "STAGE1_MANIFEST",
                str(data_dir / "manifests" / "stage1-target-universe.jsonl"),
            )
        ),
    )
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("STAGE1_BATCH_SIZE", "4")))
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.getenv("STAGE1_MAX_WORKERS", "1")),
    )
    parser.add_argument("--top-n", type=int, default=int(os.getenv("TOP_N_DISPLAY", "100")))
    parser.add_argument("--stage-capacity", type=int, default=int(os.getenv("STAGE1_CAPACITY", "100")))
    parser.add_argument(
        "--retire-candidate",
        action="append",
        default=[],
        help="Release a candidate and fill its slot from the unprocessed queue.",
    )
    parser.add_argument(
        "--adaptive-sampling",
        action="store_true",
        help="Reserved for a later experiment; currently rejected by design.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_batch(
        data_dir=args.data_dir,
        manifest_path=args.manifest,
        config=BatchConfig(
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            top_n=args.top_n,
            stage_capacity=args.stage_capacity,
            adaptive_sampling=args.adaptive_sampling,
        ),
        retire_candidate_ids=args.retire_candidate,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
