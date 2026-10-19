from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PreparedTableSpec:
    path: Path
    id_column: str
    lon_column: str
    lat_column: str
    name_column: str | None = None
    family: str | None = None
    level: str | None = None
    country_iso3: str | None = None
    state_code: str | None = None
    extra_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class PointFilter:
    column: str
    op: str
    value: Any


@dataclass(frozen=True)
class PreparedInputsManifest:
    country_iso3: str
    state_code: str | None
    origin_level: str
    destination_family: str
    representative_point_method: str
    origins_path: str
    destinations_path: str
    origin_count: int
    destination_count: int
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AccessRunConfig:
    origins_path: Path
    destinations_path: Path
    output_path: Path
    checkpoint_path: Path
    thresholds_minutes: tuple[int, ...]
    edge_output_path: Path | None = None
    edge_within_minutes: int | None = None
    checkpoint_every: int = 100
    max_origins: int | None = None
    provider_name: str = "air_distance_debug"
    method_version: str = "road_access_v1"
