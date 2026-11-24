"""Storage-independent viewport validation and equal-area overlap math."""
from __future__ import annotations

import math
from typing import Any

from pyproj import Transformer
from shapely import transform
from shapely.geometry import box

MAX_BBOX_CANDIDATES = 500
_AREA_TRANSFORM = Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True).transform


def rectangle_bounds(corners: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    """Require four distinct corners of one axis-aligned WGS84 rectangle."""
    if not isinstance(corners, list) or len(corners) != 4:
        raise ValueError("corners must contain exactly four lat/lon points")
    pairs = []
    for point in corners:
        if not isinstance(point, dict) or set(point) != {"lat", "lon"}:
            raise ValueError("each corner must contain only lat and lon")
        lat, lon = point["lat"], point["lon"]
        if (isinstance(lat, bool) or isinstance(lon, bool)
                or not isinstance(lat, (int, float)) or not isinstance(lon, (int, float))
                or not math.isfinite(lat) or not math.isfinite(lon)
                or abs(lat) > 85 or abs(lon) > 180):
            raise ValueError("corners require finite WGS84 coordinates within ±85 latitude and ±180 longitude")
        pairs.append((float(lon), float(lat)))
    lons, lats = sorted({p[0] for p in pairs}), sorted({p[1] for p in pairs})
    if (len(lons) != 2 or len(lats) != 2 or len(set(pairs)) != 4
            or set(pairs) != {(x, y) for x in lons for y in lats}
            or lons[1] - lons[0] > 180):
        raise ValueError("corners must define one non-wrapping axis-aligned rectangle")
    return lons[0], lats[0], lons[1], lats[1]


def projected_viewport(bounds):
    viewport = transform(box(*bounds), _AREA_TRANSFORM, interleaved=False)
    if viewport.area <= 0:
        raise ValueError("viewport has zero projected area")
    return viewport


def overlap_percentages(geometry, viewport):
    """Return the shared ranking metrics, or None for no positive-area overlap."""
    projected = transform(geometry, _AREA_TRANSFORM, interleaved=False)
    area = projected.area
    if projected.is_empty or area <= 0:
        return None
    overlap = viewport.intersection(projected).area
    if overlap <= 0:
        return None
    return {
        "view_coverage_pct": round(100 * overlap / viewport.area, 4),
        "loc_id_coverage_pct": round(100 * overlap / area, 4),
        "intersection_over_union_pct": round(100 * overlap / (viewport.area + area - overlap), 4),
    }
