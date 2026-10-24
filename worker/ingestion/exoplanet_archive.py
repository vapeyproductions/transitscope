"""NASA Exoplanet Archive TAP adapter and conservative match logic."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_TAP_ENDPOINT = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
CATALOG_CACHE_SCHEMA_VERSION = "exoplanet-archive-cache.v2"
CATALOG_PROJECTION_VERSION = "pscomppars-tic-id-v3-full"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class ExoplanetArchiveConfig:
    data_dir: Path
    tables: tuple[str, ...] = ("pscomppars", "toi")
    # A value of 0 means fetch the complete narrow projection.  A positive
    # value remains available for bounded local probes, but production uses
    # the complete projection so arbitrary TOP-N truncation cannot hide a
    # known object.
    max_rows: int = 0
    request_timeout_seconds: int = 60
    cache_ttl_hours: float = 168.0
    coordinate_tolerance_deg: float = 0.02
    period_tolerance_fraction: float = 0.02
    tap_endpoint: str = DEFAULT_TAP_ENDPOINT

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
        *,
        data_dir_override: Path | None = None,
    ) -> "ExoplanetArchiveConfig":
        raw_tables = values.get("tables", cls.tables)
        if isinstance(raw_tables, str):
            tables = tuple(value.strip() for value in raw_tables.split(",") if value.strip())
        else:
            tables = tuple(str(value).strip() for value in raw_tables if str(value).strip())
        config = cls(
            data_dir=data_dir_override or Path(str(values.get("data_dir", "data"))),
            tables=tables or cls.tables,
            max_rows=int(values.get("max_rows", 0)),
            request_timeout_seconds=int(values.get("request_timeout_seconds", 60)),
            cache_ttl_hours=float(values.get("cache_ttl_hours", 168.0)),
            coordinate_tolerance_deg=float(values.get("coordinate_tolerance_deg", 0.02)),
            period_tolerance_fraction=float(values.get("period_tolerance_fraction", 0.02)),
            tap_endpoint=str(values.get("tap_endpoint", DEFAULT_TAP_ENDPOINT)),
        )
        if config.max_rows < 0:
            raise ValueError("max_rows cannot be negative")
        if config.request_timeout_seconds < 1:
            raise ValueError("request_timeout_seconds must be positive")
        if config.cache_ttl_hours < 0:
            raise ValueError("cache_ttl_hours cannot be negative")
        if config.coordinate_tolerance_deg <= 0:
            raise ValueError("coordinate_tolerance_deg must be positive")
        if config.period_tolerance_fraction <= 0:
            raise ValueError("period_tolerance_fraction must be positive")
        return config


@dataclass(frozen=True)
class CatalogRecord:
    record_id: str
    table: str
    object_name: str | None
    target_id: str | None
    ra_deg: float | None
    dec_deg: float | None
    period_days: float | None
    disposition: str | None
    raw: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "table": self.table,
            "object_name": self.object_name,
            "target_id": self.target_id,
            "ra_deg": self.ra_deg,
            "dec_deg": self.dec_deg,
            "period_days": self.period_days,
            "disposition": self.disposition,
            "raw": self.raw,
        }


class ExoplanetArchiveError(RuntimeError):
    """Raised when the archive response cannot be interpreted."""


class ExoplanetArchiveAdapter:
    """Fetch a complete cached matching projection and match it conservatively."""

    TABLE_COLUMNS = {
        "pscomppars": (
            "pl_name,hostname,tic_id,ra,dec,pl_orbper,default_flag"
        ),
        "toi": "toi,tid,ra,dec,pl_orbper,tfopwg_disp",
    }

    def __init__(self, config: ExoplanetArchiveConfig) -> None:
        self.config = config

    @property
    def cache_path(self) -> Path:
        key = hashlib.sha256(
            json.dumps(
                {
                    "projection_version": CATALOG_PROJECTION_VERSION,
                    "tables": self.config.tables,
                    "max_rows": self.config.max_rows,
                    "tap_endpoint": self.config.tap_endpoint,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:12]
        return self.config.data_dir / "catalog" / f"exoplanet-archive-{key}.json"

    def fetch(self, *, force: bool = False) -> dict[str, Any]:
        cache_path = self.cache_path
        if not force and self._cache_is_fresh(cache_path):
            return json.loads(cache_path.read_text())

        records: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        queries: list[dict[str, str]] = []
        for table in self.config.tables:
            try:
                rows, query = self._query_table(table)
                queries.append({"table": table, "query": query})
                records.extend(record.as_dict() for record in rows)
            except (ExoplanetArchiveError, OSError, ValueError) as exc:
                errors.append({"table": table, "error": str(exc)})

        status = "success" if records and not errors else "partial" if records else "failed"
        payload = {
            "schema_version": CATALOG_CACHE_SCHEMA_VERSION,
            "status": status,
            "provider": "NASA Exoplanet Archive",
            "fetched_at": utc_now(),
            "cache_path": str(cache_path),
            "queries": queries,
            "errors": errors,
            "record_count": len(records),
            "complete_projection": self.config.max_rows == 0,
            "match_config": {
                "coordinate_tolerance_deg": self.config.coordinate_tolerance_deg,
                "period_tolerance_fraction": self.config.period_tolerance_fraction,
            },
            "records": records,
        }
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = cache_path.with_name(f".{cache_path.name}.part")
        part_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        os.replace(part_path, cache_path)
        return payload

    def _query_table(self, table: str) -> tuple[list[CatalogRecord], str]:
        columns = self.TABLE_COLUMNS.get(table)
        if columns is None:
            raise ExoplanetArchiveError(f"unsupported archive table: {table}")
        limit = f"top {self.config.max_rows} " if self.config.max_rows else ""
        query = f"select {limit}{columns} from {table}"
        params = urlencode({"request": "doQuery", "lang": "ADQL", "format": "json", "query": query})
        request = Request(
            f"{self.config.tap_endpoint}?{params}",
            headers={"Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError) as exc:
            raise ExoplanetArchiveError(f"TAP query failed for {table}: {exc}") from exc
        if isinstance(payload, dict):
            rows = payload.get("data") or payload.get("rows") or []
        else:
            rows = payload
        if not isinstance(rows, list):
            raise ExoplanetArchiveError(f"TAP response for {table} did not contain rows")
        return [self._record_from_row(table, row) for row in rows if isinstance(row, Mapping)], query

    @staticmethod
    def _record_from_row(table: str, row: Mapping[str, Any]) -> CatalogRecord:
        if table == "pscomppars":
            object_name = _string_value(row.get("pl_name")) or _string_value(row.get("hostname"))
            target_id = _string_value(row.get("tic_id"))
            record_id = object_name or target_id or f"pscomppars:{hashlib.sha1(json.dumps(dict(row), sort_keys=True, default=str).encode()).hexdigest()[:12]}"
            disposition = _string_value(row.get("default_flag"))
        else:
            object_name = _string_value(row.get("toi"))
            target_id = _string_value(row.get("tid"))
            record_id = object_name or target_id or f"toi:{hashlib.sha1(json.dumps(dict(row), sort_keys=True, default=str).encode()).hexdigest()[:12]}"
            disposition = _string_value(row.get("tfopwg_disp"))
        return CatalogRecord(
            record_id=record_id,
            table=table,
            object_name=object_name,
            target_id=target_id,
            ra_deg=_float_value(row.get("ra")),
            dec_deg=_float_value(row.get("dec")),
            period_days=_float_value(row.get("pl_orbper")),
            disposition=disposition,
            raw={str(key): value for key, value in row.items()},
        )

    def match_candidate(
        self,
        *,
        target_id: str | None,
        ra_deg: float | None,
        dec_deg: float | None,
        period_days: float | None,
        records: Sequence[CatalogRecord | Mapping[str, Any]],
    ) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        for record_value in records:
            record = record_value if isinstance(record_value, CatalogRecord) else self._record_from_cache(record_value)
            id_match = _same_target_id(target_id, record.target_id)
            separation = _angular_separation_deg(ra_deg, dec_deg, record.ra_deg, record.dec_deg)
            coordinate_match = separation is not None and separation <= self.config.coordinate_tolerance_deg
            period_delta = _period_delta_fraction(period_days, record.period_days)
            period_match = period_delta is not None and period_delta <= self.config.period_tolerance_fraction
            if id_match and (period_match or period_delta is None):
                match_kind = "target-id+period" if period_match else "target-id"
                score = 100.0 if period_match else 95.0
            elif coordinate_match and (period_match or period_days is None or record.period_days is None):
                match_kind = "coordinate+period" if period_match else "coordinate"
                score = 90.0 if period_match else 75.0
            else:
                continue
            candidate = {
                "catalog_id": record.record_id,
                "table": record.table,
                "object_name": record.object_name,
                "target_id": record.target_id,
                "disposition": record.disposition,
                "match_kind": match_kind,
                "match_score": score,
                "coordinate_separation_deg": separation,
                "period_delta_fraction": period_delta,
                "tolerances": {
                    "coordinate_tolerance_deg": self.config.coordinate_tolerance_deg,
                    "period_tolerance_fraction": self.config.period_tolerance_fraction,
                },
                "record": record.as_dict(),
            }
            if best is None or (
                float(candidate["match_score"]),
                -float(candidate["coordinate_separation_deg"] or 1e9),
            ) > (
                float(best["match_score"]),
                -float(best["coordinate_separation_deg"] or 1e9),
            ):
                best = candidate
        return best

    @staticmethod
    def _record_from_cache(value: Mapping[str, Any]) -> CatalogRecord:
        return CatalogRecord(
            record_id=str(value.get("record_id") or "unknown"),
            table=str(value.get("table") or "unknown"),
            object_name=_string_value(value.get("object_name")),
            target_id=_string_value(value.get("target_id")),
            ra_deg=_float_value(value.get("ra_deg")),
            dec_deg=_float_value(value.get("dec_deg")),
            period_days=_float_value(value.get("period_days")),
            disposition=_string_value(value.get("disposition")),
            raw=dict(value.get("raw") or {}),
        )

    def _cache_is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        try:
            payload = json.loads(path.read_text())
        except (OSError, ValueError, TypeError):
            return False
        if payload.get("schema_version") != CATALOG_CACHE_SCHEMA_VERSION:
            return False
        if payload.get("status") not in {"success", "partial"}:
            return False
        if not payload.get("records"):
            return False
        if self.config.max_rows == 0 and payload.get("complete_projection") is not True:
            return False
        age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
        return age_seconds <= self.config.cache_ttl_hours * 3600.0


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_value(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _digits(value: str | None) -> str | None:
    if value is None:
        return None
    digits = "".join(character for character in str(value) if character.isdigit())
    return digits or None


def _same_target_id(left: str | None, right: str | None) -> bool:
    left_digits = _digits(left)
    right_digits = _digits(right)
    return left_digits is not None and left_digits == right_digits


def _period_delta_fraction(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or left <= 0 or right <= 0:
        return None
    return abs(left - right) / max(abs(left), abs(right))


def _angular_separation_deg(
    ra_left: float | None,
    dec_left: float | None,
    ra_right: float | None,
    dec_right: float | None,
) -> float | None:
    if None in {ra_left, dec_left, ra_right, dec_right}:
        return None
    ra1, dec1, ra2, dec2 = map(math.radians, (float(ra_left), float(dec_left), float(ra_right), float(dec_right)))
    cosine = (
        math.sin(dec1) * math.sin(dec2)
        + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2)
    )
    return math.degrees(math.acos(max(-1.0, min(1.0, cosine))))


__all__ = [
    "CatalogRecord",
    "CATALOG_CACHE_SCHEMA_VERSION",
    "CATALOG_PROJECTION_VERSION",
    "DEFAULT_TAP_ENDPOINT",
    "ExoplanetArchiveAdapter",
    "ExoplanetArchiveConfig",
    "ExoplanetArchiveError",
]
