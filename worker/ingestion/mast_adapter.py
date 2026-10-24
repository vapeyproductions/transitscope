from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_API_ENDPOINT = "https://mast.stsci.edu/api/v0/invoke"
DEFAULT_DOWNLOAD_ENDPOINT = "https://mast.stsci.edu/api/v0.1/Download/file"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class MastProbeError(RuntimeError):
    """A provider or validation failure that should be visible to the operator."""


@dataclass(frozen=True)
class MastProbeConfig:
    data_dir: Path
    sector: int = 1
    target_name: str | None = "277691220"
    product_family: str = "TESS SPOC light curve"
    max_observations: int = 5
    request_timeout_seconds: int = 60
    api_endpoint: str = DEFAULT_API_ENDPOINT
    download_endpoint: str = DEFAULT_DOWNLOAD_ENDPOINT

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
        *,
        data_dir_override: Path | None = None,
    ) -> "MastProbeConfig":
        data_dir = data_dir_override or Path(str(values.get("data_dir", "data")))
        target_name = values.get("target_name")
        if target_name is not None:
            target_name = str(target_name).removeprefix("TIC ").strip() or None

        config = cls(
            data_dir=data_dir,
            sector=int(values.get("sector", 1)),
            target_name=target_name,
            product_family=str(values.get("product_family", cls.product_family)),
            max_observations=int(values.get("max_observations", 5)),
            request_timeout_seconds=int(values.get("request_timeout_seconds", 60)),
            api_endpoint=str(values.get("api_endpoint", DEFAULT_API_ENDPOINT)),
            download_endpoint=str(
                values.get("download_endpoint", DEFAULT_DOWNLOAD_ENDPOINT)
            ),
        )
        if config.sector < 1:
            raise ValueError("sector must be a positive integer")
        if config.max_observations < 1:
            raise ValueError("max_observations must be positive")
        if config.request_timeout_seconds < 1:
            raise ValueError("request_timeout_seconds must be positive")
        return config


class MastProductAdapter:
    """Small, bounded adapter for public MAST/TESS observation products."""

    def __init__(self, config: MastProbeConfig) -> None:
        self.config = config
        self.run_id = f"mast-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
        self.manifest_path = (
            self.config.data_dir / "manifests" / f"{self.run_id}.json"
        )

    def probe(self) -> dict[str, Any]:
        started_at = utc_now()
        query_config = self.query_config()
        try:
            observations = self.query_observations()
            if not observations:
                raise MastProbeError(
                    "MAST returned no TESS timeseries observations for "
                    f"sector={self.config.sector}, target_name={self.config.target_name!r}"
                )

            observation = self._select_observation(observations)
            products = self.query_products(observation["obsid"])
            product = self.select_light_curve_product(products)
            artifact = self.download_product(product)
            result = {
                "schema_version": "mast-probe.v1",
                "run_id": self.run_id,
                "status": "success",
                "provider": "MAST",
                "product_family": self.config.product_family,
                "started_at": started_at,
                "finished_at": utc_now(),
                "query": query_config,
                "observation": observation,
                "product": product,
                "target_manifest": self._target_manifest(observation, product),
                "artifact": artifact,
            }
        except (MastProbeError, KeyError, TypeError, ValueError) as exc:
            result = {
                "schema_version": "mast-probe.v1",
                "run_id": self.run_id,
                "status": "failed",
                "provider": "MAST",
                "product_family": self.config.product_family,
                "started_at": started_at,
                "finished_at": utc_now(),
                "query": query_config,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
            self.write_manifest(result)
            if isinstance(exc, MastProbeError):
                raise
            raise MastProbeError(f"MAST probe validation failed: {exc}") from exc

        self.write_manifest(result)
        return result

    def query_config(self) -> dict[str, Any]:
        return {
            "service": "Mast.Caom.Filtered",
            "sector": self.config.sector,
            "target_name": self.config.target_name,
            "dataproduct_type": "timeseries",
            "max_observations": self.config.max_observations,
            "api_endpoint": self.config.api_endpoint,
        }

    def query_observations(self) -> list[dict[str, Any]]:
        """Return observations for the configured target and sector."""

        return self._query_observations(
            sector=self.config.sector,
            target_name=self.config.target_name,
            page=1,
            page_size=self.config.max_observations,
        )

    def query_sector_observations(
        self,
        sector: int,
        *,
        max_records: int = 5000,
    ) -> list[dict[str, Any]]:
        """Return a bounded, paginated target list for one TESS sector."""

        if sector < 1 or max_records < 1:
            raise ValueError("sector and max_records must be positive")
        observations: list[dict[str, Any]] = []
        page_size = min(5000, max_records)
        page = 1
        while len(observations) < max_records:
            page_rows = self._query_observations(
                sector=sector,
                target_name=None,
                page=page,
                page_size=page_size,
            )
            if not page_rows:
                break
            observations.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1
        return observations[:max_records]

    def query_all_observations(
        self,
        *,
        target_name: str | None = None,
        max_records: int = 5000,
    ) -> list[dict[str, Any]]:
        """Return all TESS timeseries observations for a target across sectors.

        MAST is an archive rather than a continuously streaming sensor.  The live
        worker uses this method to discover newly available sectors without
        hard-coding a sector list.
        """

        if max_records < 1:
            raise ValueError("max_records must be positive")
        observations: list[dict[str, Any]] = []
        page_size = min(5000, max_records)
        page = 1
        while len(observations) < max_records:
            page_rows = self._query_observations(
                sector=None,
                target_name=self.config.target_name if target_name is None else target_name,
                page=page,
                page_size=page_size,
            )
            if not page_rows:
                break
            observations.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1
        return observations[:max_records]

    def _query_observations(
        self,
        *,
        sector: int | None,
        target_name: str | None,
        page: int = 1,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        filters: list[dict[str, Any]] = [
            {"paramName": "obs_collection", "values": ["TESS"]},
            {"paramName": "dataproduct_type", "values": ["timeseries"]},
        ]
        if sector is not None:
            filters.insert(1, {"paramName": "sequence_number", "values": [sector]})
        if target_name:
            filters.append(
                {"paramName": "target_name", "values": [str(target_name).removeprefix("TIC ").strip()]}
            )

        response = self._mast_request(
            {
                "service": "Mast.Caom.Filtered",
                "params": {
                    "columns": "obsid,obs_id,obs_collection,dataproduct_type,"
                    "sequence_number,target_name,s_ra,s_dec",
                    "filters": filters,
                },
                "format": "json",
                "pagesize": page_size or self.config.max_observations,
                "page": page,
                "removenullcolumns": True,
            }
        )
        return list(response.get("data", []))

    def query_products(self, obsid: object) -> list[dict[str, Any]]:
        response = self._mast_request(
            {
                "service": "Mast.Caom.Products",
                "params": {"obsid": obsid},
                "format": "json",
                "pagesize": 50,
                "page": 1,
                "removenullcolumns": True,
            }
        )
        return list(response.get("data", []))

    @staticmethod
    def select_light_curve_product(products: list[dict[str, Any]]) -> dict[str, Any]:
        candidates = [
            product
            for product in products
            if str(product.get("description", "")).lower() == "light curves"
            and str(product.get("productSubGroupDescription", "")).upper() == "LC"
            and str(product.get("dataRights", "")).upper() == "PUBLIC"
            and str(product.get("productFilename", "")).lower().endswith(".fits")
        ]
        if not candidates:
            raise MastProbeError(
                "MAST observation has no public FITS light-curve product in the "
                "configured product family"
            )
        return sorted(candidates, key=lambda item: str(item["productFilename"]))[0]

    def download_product(self, product: Mapping[str, Any]) -> dict[str, Any]:
        data_uri = str(product.get("dataURI", ""))
        filename = Path(str(product.get("productFilename", ""))).name
        if not data_uri or not filename or filename in {".", ".."}:
            raise MastProbeError("MAST product metadata is missing dataURI or filename")

        cache_dir = (
            self.config.data_dir
            / "raw"
            / "mast"
            / "tess"
            / f"sector-{self.config.sector:04d}"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / filename
        expected_size = self._optional_int(product.get("size"))
        cached = cache_path.exists() and (
            expected_size is None or cache_path.stat().st_size == expected_size
        )
        headers: dict[str, str] = {}

        if not cached:
            download_url = f"{self.config.download_endpoint}?{urlencode({'uri': data_uri})}"
            part_path = cache_path.with_name(f".{cache_path.name}.{self.run_id}.part")
            try:
                request = Request(
                    download_url,
                    headers={"Accept": "application/octet-stream"},
                )
                with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                    headers = {
                        name.lower(): value
                        for name, value in response.headers.items()
                        if name.lower() in {"content-length", "etag", "last-modified"}
                    }
                    byte_count = 0
                    with part_path.open("wb") as output:
                        while True:
                            chunk = response.read(1024 * 1024)
                            if not chunk:
                                break
                            output.write(chunk)
                            byte_count += len(chunk)
                if expected_size is not None and byte_count != expected_size:
                    raise MastProbeError(
                        f"MAST download size mismatch: expected {expected_size} bytes, "
                        f"received {byte_count}"
                    )
                os.replace(part_path, cache_path)
            except (HTTPError, URLError, TimeoutError) as exc:
                if part_path.exists():
                    part_path.unlink()
                raise MastProbeError(
                    f"MAST download failed for {data_uri}: {exc}"
                ) from exc
            except Exception:
                if part_path.exists():
                    part_path.unlink()
                raise

        return {
            "cache_path": str(cache_path),
            "cached": cached,
            "bytes": cache_path.stat().st_size,
            "sha256": self._sha256(cache_path),
            "data_uri": data_uri,
            "source_url": f"{self.config.download_endpoint}?{urlencode({'uri': data_uri})}",
            "retrieved_at": utc_now(),
            "response_headers": headers,
        }

    def write_manifest(self, payload: Mapping[str, Any]) -> Path:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.manifest_path.with_suffix(".json.tmp")
        temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        os.replace(temporary_path, self.manifest_path)
        return self.manifest_path

    def _mast_request(
        self, request_payload: dict[str, Any], *, poll_attempt: int = 0
    ) -> dict[str, Any]:
        request_body = urlencode({"request": json.dumps(request_payload)})
        request = Request(
            self.config.api_endpoint,
            data=request_body.encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "TransitScope/0.1 MAST probe",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError) as exc:
            raise MastProbeError(f"MAST metadata request failed: {exc}") from exc

        try:
            result = json.loads(body)
        except json.JSONDecodeError as exc:
            raise MastProbeError("MAST returned a non-JSON metadata response") from exc

        status = str(result.get("status", "")).upper()
        if status == "EXECUTING":
            if poll_attempt >= 3:
                raise MastProbeError(
                    "MAST metadata request remained EXECUTING after 3 polls"
                )
            time.sleep(1)
            return self._mast_request(request_payload, poll_attempt=poll_attempt + 1)
        if status != "COMPLETE":
            message = result.get("msg") or "unknown provider error"
            raise MastProbeError(f"MAST metadata request returned {status}: {message}")
        return result

    def _select_observation(self, observations: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            return sorted(observations, key=lambda item: int(item["obsid"]))[0]
        except (KeyError, TypeError, ValueError) as exc:
            raise MastProbeError("MAST observation response is missing a stable obsid") from exc

    def _target_manifest(
        self, observation: Mapping[str, Any], product: Mapping[str, Any]
    ) -> dict[str, Any]:
        provider_target_id = str(observation.get("target_name", ""))
        target_id = (
            provider_target_id
            if provider_target_id.upper().startswith("TIC ")
            else f"TIC {provider_target_id}"
        )
        return {
            "target_id": target_id,
            "provider_target_id": provider_target_id,
            "ra_deg": observation.get("s_ra"),
            "dec_deg": observation.get("s_dec"),
            "sector": observation.get("sequence_number"),
            "cadence": "SPOC timeseries",
            "source_product_family": self.config.product_family,
            "selection_bucket": "probe",
            "manifest_status": "queued",
            "source_observation_id": observation.get("obsid"),
            "source_observation_name": observation.get("obs_id"),
            "source_product_name": product.get("productFilename"),
        }

    @staticmethod
    def _optional_int(value: object) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


def attach_product_to_target(
    target: Mapping[str, Any],
    *,
    data_dir: Path,
    request_timeout_seconds: int = 60,
) -> dict[str, Any]:
    """Resolve and cache the current public light curve for a queued target.

    The target universe intentionally stores lightweight MAST observation
    metadata.  FITS files are fetched only when a target enters a processing
    batch, which keeps the 126,000-target queue inexpensive to maintain.
    """

    provider_target_id = str(
        target.get("provider_target_id") or target.get("target_id") or ""
    ).removeprefix("TIC ").strip()
    if not provider_target_id or provider_target_id.startswith("queued-target-"):
        raise MastProbeError("target has no provider-backed TIC identifier")
    try:
        sector = int(target.get("sector") or target.get("selected_sector") or 1)
    except (TypeError, ValueError) as exc:
        raise MastProbeError(f"target has invalid TESS sector: {target.get('sector')!r}") from exc

    config = MastProbeConfig(
        data_dir=data_dir,
        sector=sector,
        target_name=provider_target_id,
        max_observations=5,
        request_timeout_seconds=request_timeout_seconds,
    )
    adapter = MastProductAdapter(config)
    observations = adapter.query_observations()
    if not observations:
        observations = adapter.query_all_observations(target_name=provider_target_id, max_records=50)
    if not observations:
        raise MastProbeError(f"MAST returned no TESS observation for TIC {provider_target_id}")
    observation = adapter._select_observation(observations)
    products = adapter.query_products(observation["obsid"])
    product = adapter.select_light_curve_product(products)
    artifact = adapter.download_product(product)
    enriched = dict(target)
    enriched.update(
        {
            "provider_target_id": provider_target_id,
            "source_observation_id": observation.get("obsid"),
            "source_observation_name": observation.get("obs_id"),
            "source_product_name": product.get("productFilename"),
            "sector": observation.get("sequence_number", sector),
            "ra_deg": observation.get("s_ra", target.get("ra_deg")),
            "dec_deg": observation.get("s_dec", target.get("dec_deg")),
            "artifact_path": artifact["cache_path"],
            "cache_path": artifact["cache_path"],
            "product_id": artifact["data_uri"],
            "product_checksum": artifact["sha256"],
            "manifest_status": "queued",
        }
    )
    return enriched


__all__ = [
    "MastProbeConfig",
    "MastProbeError",
    "MastProductAdapter",
    "attach_product_to_target",
]
