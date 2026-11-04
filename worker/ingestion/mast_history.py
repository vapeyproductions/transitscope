"""Fetch a bounded set of historical MAST/TESS products for Stage 2."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .mast_adapter import MastProbeConfig, MastProbeError, MastProbeTimeout, MastProductAdapter


class MastHistoryAdapter:
    """Reuse the bounded MAST probe contract across selected TESS sectors."""

    def __init__(
        self,
        config: MastProbeConfig,
        sectors: Sequence[int] | None = None,
        progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.config = config
        self.sectors = tuple(sorted({int(sector) for sector in (sectors or ())}))
        self.progress_callback = progress_callback

    def _notify(self, event: str, **details: Any) -> None:
        if self.progress_callback:
            self.progress_callback(event, details)

    def discover_sectors(self) -> list[int]:
        """Discover sectors from MAST metadata without downloading FITS files."""

        self._notify("mast_history_discovery_started")
        adapter = MastProductAdapter(self.config)
        observations = adapter.query_all_observations()
        sectors: set[int] = set()
        for observation in observations:
            try:
                sector = int(observation["sequence_number"])
            except (KeyError, TypeError, ValueError):
                continue
            if sector > 0:
                sectors.add(sector)
        discovered = sorted(sectors)
        self._notify(
            "mast_history_discovery_completed",
            discovered_sector_count=len(discovered),
            sectors=discovered,
        )
        return discovered

    def fetch(self) -> dict[str, Any]:
        sectors = self.sectors or tuple(self.discover_sectors())
        if not sectors:
            payload = {
                "schema_version": "mast-history.v1",
                "run_id": f"mast-history-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}",
                "status": "empty",
                "provider": "MAST",
                "target_name": self.config.target_name,
                "sectors": [],
                "records": [],
                "failures": [{"error": "MAST returned no TESS sectors for target"}],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            path = self.config.data_dir / "manifests" / f"{payload['run_id']}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            payload["manifest_path"] = str(path)
            return payload

        self._notify("mast_history_fetch_started", sector_count=len(sectors), sectors=list(sectors))
        records: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for sector_index, sector in enumerate(sectors, start=1):
            self.config.ensure_deadline()
            self._notify(
                "mast_history_sector_started",
                sector=sector,
                sector_index=sector_index,
                sector_count=len(sectors),
            )
            sector_config = replace(self.config, sector=sector)
            adapter = MastProductAdapter(sector_config)
            try:
                observations = adapter.query_observations()
                if not observations:
                    failures.append({"sector": sector, "error": "no observations"})
                    self._notify(
                        "mast_history_sector_failed",
                        sector=sector,
                        sector_index=sector_index,
                        error="no observations",
                    )
                    continue
                observation = adapter._select_observation(observations)
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
                    self._notify(
                        "mast_history_sector_completed",
                        sector=sector,
                        sector_index=sector_index,
                        sector_count=len(sectors),
                        records=len(records),
                    )
                except MastProbeTimeout:
                    raise
                except (KeyError, MastProbeError, TypeError, ValueError) as exc:
                    failures.append({"sector": sector, "error": str(exc)})
                    self._notify(
                        "mast_history_sector_failed",
                        sector=sector,
                        sector_index=sector_index,
                        error=str(exc),
                    )
            except MastProbeTimeout:
                raise
            except MastProbeError as exc:
                failures.append({"sector": sector, "error": str(exc)})
                self._notify(
                    "mast_history_sector_failed",
                    sector=sector,
                    sector_index=sector_index,
                    error=str(exc),
                )

        payload = {
            "schema_version": "mast-history.v1",
            "run_id": f"mast-history-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}",
            "status": "success" if records else "partial" if failures else "empty",
            "provider": "MAST",
            "target_name": self.config.target_name,
            "sectors": list(sectors),
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
        self._notify(
            "mast_history_fetch_completed",
            records=len(records),
            failures=len(failures),
            sectors=list(sectors),
        )
        return payload


__all__ = ["MastHistoryAdapter"]
