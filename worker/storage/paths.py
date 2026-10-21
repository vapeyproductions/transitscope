from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """Stable filesystem locations shared by the worker and read-only API."""

    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def normalized(self) -> Path:
        return self.root / "normalized"

    @property
    def manifests(self) -> Path:
        return self.root / "manifests"

    @property
    def database(self) -> Path:
        return self.root / "transitscope.duckdb"

    def ensure(self) -> None:
        for path in (self.root, self.raw, self.normalized, self.manifests):
            path.mkdir(parents=True, exist_ok=True)

