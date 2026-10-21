#!/usr/bin/env python3
"""Populate a bounded, real MAST/TESS target pool from the NASA TOI table.

The large universe is represented by deferred queue rows.  Only the bounded
real seed pool is downloaded during a demo run, so the worker never pretends
that 126,000 FITS files are already available locally.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from batching import generate_target_universe
from ingestion.exoplanet_archive import ExoplanetArchiveAdapter, ExoplanetArchiveConfig
from ingestion.mast_adapter import MastProbeConfig, MastProbeError, MastProductAdapter


def parse_args() -> argparse.Namespace:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=data_dir)
    parser.add_argument(
        "--output",
        type=Path,
        default=data_dir / "manifests" / "stage1-real-target-universe.jsonl",
    )
    parser.add_argument("--universe-size", type=int, default=126_000)
    parser.add_argument("--target-count", type=int, default=8)
    parser.add_argument("--catalog-rows", type=int, default=500)
    parser.add_argument("--max-target-probes", type=int, default=80)
    parser.add_argument("--max-sector-probes", type=int, default=8)
    parser.add_argument("--request-timeout-seconds", type=int, default=60)
    parser.add_argument("--force-refresh", action="store_true")
    return parser.parse_args()


def _digits(value: object) -> str:
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        raise ValueError(f"target has no TIC identifier: {value!r}")
    return digits


def _toi_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in payload.get("records", []):
        if record.get("table") != "toi":
            continue
        disposition = str(record.get("disposition") or "").upper()
        target_id = record.get("target_id")
        if disposition not in {"PC", "CP"} or not target_id:
            continue
        try:
            tic_id = _digits(target_id)
        except ValueError:
            continue
        if record.get("ra_deg") is None or record.get("dec_deg") is None:
            continue
        records.append({**record, "tic_id": tic_id})
    return records


def _observation_by_sector(observations: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for observation in observations:
        try:
            sector = int(observation["sequence_number"])
        except (KeyError, TypeError, ValueError):
            continue
        if sector > 0:
            grouped.setdefault(sector, []).append(observation)
    selected: dict[int, dict[str, Any]] = {}
    for sector, rows in grouped.items():
        selected[sector] = sorted(rows, key=lambda row: int(row.get("obsid", 0)))[0]
    return selected


def _seed_target(
    record: dict[str, Any],
    *,
    data_dir: Path,
    request_timeout_seconds: int,
    max_sector_probes: int,
) -> dict[str, Any] | None:
    tic_id = record["tic_id"]
    base_config = MastProbeConfig(
        data_dir=data_dir,
        sector=1,
        target_name=tic_id,
        max_observations=5,
        request_timeout_seconds=request_timeout_seconds,
    )
    discovery_adapter = MastProductAdapter(base_config)
    observations = discovery_adapter.query_all_observations(target_name=tic_id)
    by_sector = _observation_by_sector(observations)
    if not by_sector:
        return None

    available_sectors = sorted(by_sector)
    for sector in available_sectors[:max_sector_probes]:
        adapter = MastProductAdapter(replace(base_config, sector=sector))
        observation = by_sector[sector]
        try:
            products = adapter.query_products(observation["obsid"])
            product = adapter.select_light_curve_product(products)
            artifact = adapter.download_product(product)
        except (KeyError, MastProbeError, TypeError, ValueError):
            continue
        target = adapter._target_manifest(observation, product)
        target.update(
            {
                "target_id": f"TIC {tic_id}",
                "provider_target_id": tic_id,
                "artifact_path": artifact["cache_path"],
                "cache_path": artifact["cache_path"],
                "product_id": artifact["data_uri"],
                "product_checksum": artifact["sha256"],
                "available_sectors": available_sectors,
                "selected_sector": sector,
                "source_catalog": "NASA Exoplanet Archive / toi",
                "source_catalog_id": record.get("record_id"),
                "source_catalog_object": record.get("object_name"),
                "source_catalog_disposition": record.get("disposition"),
                "source_catalog_period_days": record.get("period_days"),
                "manifest_status": "queued",
                "selection_bucket": "toi-seed",
            }
        )
        return target
    return None


def main() -> int:
    args = parse_args()
    if args.target_count < 1 or args.universe_size < args.target_count:
        raise SystemExit("target-count must be positive and no larger than universe-size")

    catalog_config = ExoplanetArchiveConfig(
        data_dir=args.data_dir,
        tables=("toi",),
        max_rows=args.catalog_rows,
        request_timeout_seconds=args.request_timeout_seconds,
    )
    catalog_adapter = ExoplanetArchiveAdapter(catalog_config)
    catalog_payload = catalog_adapter.fetch(force=args.force_refresh)
    records = _toi_records(catalog_payload)
    seeds: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    seen_tics: set[str] = set()
    for record in records[: args.max_target_probes]:
        if len(seeds) >= args.target_count:
            break
        if record["tic_id"] in seen_tics:
            continue
        seen_tics.add(record["tic_id"])
        try:
            seed = _seed_target(
                record,
                data_dir=args.data_dir,
                request_timeout_seconds=args.request_timeout_seconds,
                max_sector_probes=args.max_sector_probes,
            )
        except (MastProbeError, OSError, ValueError) as exc:
            failures.append({"target_id": record["tic_id"], "error": str(exc)})
            continue
        if seed:
            seeds.append(seed)
        else:
            failures.append({"target_id": record["tic_id"], "error": "no public MAST light curve found"})

    written = generate_target_universe(
        args.output,
        universe_size=args.universe_size,
        seed_rows=seeds,
        defer_placeholders=True,
    )
    run_id = f"real-population-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    report = {
        "schema_version": "real-target-population.v1",
        "run_id": run_id,
        "status": "success" if seeds else "failed",
        "provider_sources": ["NASA Exoplanet Archive / toi", "MAST / TESS SPOC light curves"],
        "catalog_cache_path": catalog_payload.get("cache_path"),
        "manifest_path": str(args.output),
        "universe_size": written,
        "downloaded_target_count": len(seeds),
        "targets": [
            {
                "target_id": seed["target_id"],
                "selected_sector": seed.get("selected_sector"),
                "available_sector_count": len(seed.get("available_sectors", [])),
                "source_catalog_object": seed.get("source_catalog_object"),
            }
            for seed in seeds
        ],
        "failures": failures,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    report_path = args.data_dir / "manifests" / f"{run_id}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**report, "report_path": str(report_path)}, indent=2, sort_keys=True))
    return 0 if seeds else 1


if __name__ == "__main__":
    raise SystemExit(main())
