"""Small visual Admin0 fallback shipped with the local Mapping engine.

Natural Earth 1:50m outlines are display geometry only. They do not replace
DaedalMap's exact or point-query Admin0 data.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


def enabled() -> bool:
    import os

    return os.environ.get("DAEDALMAP_ACCESS_MODE", "").strip().lower() == "local_wrapper"


def _positions(value):
    if isinstance(value, list):
        if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
            yield value[0], value[1]
        else:
            for part in value:
                yield from _positions(part)


@lru_cache(maxsize=1)
def _features() -> tuple[dict, ...]:
    path = Path(__file__).resolve().parents[1] / "static" / "data" / "offline-basemap" / "ne_50m_admin_0_countries.geojson"
    if not path.is_file():
        return ()
    raw = json.loads(path.read_text(encoding="utf-8"))
    features = []
    for feature in raw.get("features", []):
        properties = feature.get("properties") or {}
        loc_id = str(properties.get("ADM0_A3") or "").strip().upper()
        if len(loc_id) != 3 or not loc_id.isalpha():
            continue
        positions = list(_positions((feature.get("geometry") or {}).get("coordinates")))
        if not positions:
            continue
        west = min(point[0] for point in positions)
        south = min(point[1] for point in positions)
        east = max(point[0] for point in positions)
        north = max(point[1] for point in positions)
        features.append({
            "type": "Feature",
            "geometry": feature.get("geometry"),
            "properties": {
                "loc_id": loc_id,
                "iso_a3": loc_id,
                "name": properties.get("ADMIN") or properties.get("NAME") or loc_id,
                "code": loc_id,
                "admin_level": 0,
                "parent_id": None,
                "has_polygon": True,
                "bbox_min_lon": west, "bbox_min_lat": south,
                "bbox_max_lon": east, "bbox_max_lat": north,
                "centroid_lon": (west + east) / 2,
                "centroid_lat": (south + north) / 2,
                "source_name": "Natural Earth 1:50m (offline display)",
            },
        })
    return tuple(features)


def countries() -> dict:
    features = list(_features())
    return {"geojson": {"type": "FeatureCollection", "features": features},
            "count": len(features), "level": "country", "debug": False,
            "display_fallback": "natural_earth_50m"}


def index(bbox: tuple | None = None) -> dict:
    rows = []
    for feature in _features():
        properties = feature["properties"]
        if bbox is not None and (properties["bbox_max_lon"] < bbox[0]
                                 or properties["bbox_min_lon"] > bbox[2]
                                 or properties["bbox_max_lat"] < bbox[1]
                                 or properties["bbox_min_lat"] > bbox[3]):
            continue
        rows.append({"loc_id": properties["loc_id"], "iso_a3": properties["iso_a3"],
                     "name": properties["name"], "code": properties["code"],
                     "admin_level": 0, "parent_id": None, "has_polygon": True,
                     "bbox_min_lon": properties["bbox_min_lon"],
                     "bbox_min_lat": properties["bbox_min_lat"],
                     "bbox_max_lon": properties["bbox_max_lon"],
                     "bbox_max_lat": properties["bbox_max_lat"]})
    return {"rows": rows, "count": len(rows), "parent_loc_id": None,
            "admin_level": 0, "display_fallback": "natural_earth_50m"}


def display_features(loc_ids: list[str]) -> dict:
    requested = {str(value).strip().upper() for value in loc_ids}
    return {"type": "FeatureCollection", "features": [feature for feature in _features()
                                                   if feature["properties"]["loc_id"] in requested]}
