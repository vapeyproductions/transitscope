"""Fetch a bounded set of historical MAST/TESS products for Stage 2."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .mast_adapter import MastProbeConfig, MastProbeError, MastProductAdapter


class MastHistoryAdapter:
    """Reuse the bounded MAST probe contract across selected TESS sectors."""

    def __init__(self, config: MastProbeConfig, sectors: Sequence[int]) -> None:
        self.config = config
        self.sectors = tuple(sorted({int(sector) for sector in sectors}))
        if not self.sectors:
            raise ValueError("at least one historical sector is required")

    def fetch(self) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for sector in self.sectors:
            sector_config = replace(self.config, sector=sector)
            adapter = MastProductAdapter(sector_config)
            try:
                observations = adapter.query_observations()
                if not observations:
                    failures.append({"sector": sector, "error": "no observations"})
                    continue
                for observation in observations:
                    try:
                        products = adapter.query_products(observation["obsid"])
                        product = adapter.select_light_curve_product(products)
                        artifact = adapter.download_product(product)
                        records.append(
                            {
                                "sector": sector,
                                "observation": observation,
                                "product": product,
                                "artifact": artifact,
                            }
                        )
                    except (KeyError, MastProbeError, TypeError, ValueError) as exc:
                        failures.append({"sector": sector, "error": str(exc)})
            except MastProbeError as exc:
                failures.append({"sector": sector, "error": str(exc)})

        payload = {
            "schema_version": "mast-history.v1",
            "run_id": f"mast-history-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}",
            "status": "success" if records else "partial" if failures else "empty",
            "provider": "MAST",
            "target_name": self.config.target_name,
            "sectors": list(self.sectors),
            "records": records,
            "failures": failures,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self.config.data_dir / "manifests" / f"{payload['run_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temporary.replace(path)
        payload["manifest_path"] = str(path)
        return payload


__all__ = ["MastHistoryAdapter"]
