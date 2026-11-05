"""Single-pack Global Admin0-2 point containment and preferred-ID translation."""
from __future__ import annotations

import logging
from typing import Any, Iterable

from shapely.geometry import Point
from shapely.wkb import loads as load_wkb

from ..duckdb_helpers import is_cloud_mode, parquet_available
from ..mcp_execution import mark_mcp_stage, record_mcp_file
from ..paths import GEOMETRY_DIR
from .geometry_catalog import load_geometry_catalog
from .geometry_predicate_query import read_bbox_candidates_for_points, read_rows_by_ids
from .geometry_storage_layout import catalog_artifact_path

logger = logging.getLogger(__name__)


def _selected_layout() -> dict[str, Any] | None:
    layout = (load_geometry_catalog().get("runtime_query_layouts") or {}).get("global_admin0_2_point")
    if not isinstance(layout, dict) or layout.get("layout_id") != "global_admin0_2_point_query_v1":
        return None
    if layout.get("representation") != "full" or layout.get("authoritative_for_containment") is not True:
        return None
    records = {"bbox_index": layout.get("bbox_index"),
               "preferred_lookup": layout.get("preferred_lookup")}
    records.update({f"admin_{level}": (layout.get("exact_members") or {}).get(str(level))
                    for level in range(3)})
    paths = {}
    for key, record in records.items():
        path = catalog_artifact_path(GEOMETRY_DIR.parent, record)
        relative = str((record or {}).get("path") or "")
        if (path is not None and not is_cloud_mode() and not path.is_file()
                and relative.startswith("geometry/global/local_full/global_baseline_1_1_1/")):
            path = GEOMETRY_DIR.parent / "wip" / relative
        paths[key] = path
    if any(path is None or not parquet_available(path) for path in paths.values()):
        return None
    return {**paths, "release": str(layout.get("source_release") or "")}


def resolve_global_admin_shallow_points(points: Iterable[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """Return one result per point, or None when no admitted layout is selected."""
    point_items = list(points)
    layout = _selected_layout()
    if layout is None:
        return None
    try:
        mark_mcp_stage("point_global_admin0_2_bbox")
        record_mcp_file(layout["bbox_index"])
        bbox = read_bbox_candidates_for_points(
            layout["bbox_index"], point_items, columns=["loc_id", "admin_level"],
        )
        source_ids = set(bbox["loc_id"].astype(str)) if not bbox.empty else set()
        mark_mcp_stage("point_global_admin0_2_exact")
        exact_rows: dict[str, dict[str, Any]] = {}
        for level in range(3):
            ids = {str(row.loc_id) for row in bbox.itertuples(index=False)
                   if int(row.admin_level) == level} if source_ids else set()
            if not ids:
                continue
            record_mcp_file(layout[f"admin_{level}"])
            frame = read_rows_by_ids(
                layout[f"admin_{level}"], ids, id_column="loc_id",
                columns=["name", "parent_id", "admin_level", "geometry_wkb"],
                raw_geoparquet=True,
            )
            exact_rows.update({str(row["loc_id"]): row for row in frame.to_dict("records")})
        record_mcp_file(layout["preferred_lookup"])
        translations = read_rows_by_ids(
            layout["preferred_lookup"], exact_rows,
            id_column="baseline_loc_id", columns=["preferred_loc_id"],
        )
        preferred = dict(zip(translations["baseline_loc_id"].astype(str),
                             translations["preferred_loc_id"].astype(str)))
        if len(preferred) != len(exact_rows):
            raise ValueError("Selected Global lookup does not cover every exact candidate")
    except Exception:
        logger.exception("Selected Global Admin0-2 point layout failed")
        return [{"point": {"lon": float(item["lon"]), "lat": float(item["lat"])},
                 "error": {"code": "selected_scope_unavailable",
                           "message": "Selected Global Admin0-2 point layout failed"}}
                for item in point_items]

    by_position: dict[int, set[str]] = {}
    for row in bbox.itertuples(index=False):
        by_position.setdefault(int(row.point_position), set()).add(str(row.loc_id))
    geometries = {}
    output = []
    for position, item in enumerate(point_items):
        coordinate = {"lon": float(item["lon"]), "lat": float(item["lat"])}
        point = Point(coordinate["lon"], coordinate["lat"])
        candidates: dict[int, list[tuple[float, str, dict[str, Any]]]] = {0: [], 1: [], 2: []}
        for loc_id in by_position.get(position, ()):
            row = exact_rows.get(loc_id)
            if row is None or row.get("geometry_wkb") is None:
                continue
            if loc_id not in geometries:
                geometries[loc_id] = load_wkb(bytes(row["geometry_wkb"]))
            shape = geometries[loc_id]
            if shape.covers(point):
                candidates[int(row["admin_level"])].append((shape.area, loc_id, row))
        stack = []
        for level in range(3):
            options = candidates[level]
            if level and stack:
                options = [value for value in options
                           if str(value[2].get("parent_id") or "") == stack[-1]["baseline_loc_id"]]
            if not options or (level and not stack):
                break
            _area, loc_id, row = min(options, key=lambda value: (value[0], value[1]))
            stack.append({"loc_id": preferred[loc_id], "baseline_loc_id": loc_id,
                          "preferred_loc_id": preferred[loc_id],
                          "name": str(row.get("name") or loc_id),
                          "admin_level": level, "family": "administrative"})
        if not stack:
            output.append({"point": coordinate, "error": {"code": "no_point_overlap",
                           "message": "No Global administrative polygon covers this point"}})
            continue
        deepest = stack[-1]
        output.append({"point": coordinate,
                       "country": {"loc_id": stack[0]["loc_id"], "name": stack[0]["name"]},
                       "matched": deepest, "stack": stack,
                       "deepest_resolved_loc_id": deepest["loc_id"],
                       "deepest_resolved_admin_level": deepest["admin_level"],
                       "deepest_resolved_family": "administrative",
                       "resolution_family": "administrative",
                       "query_layout": "global_admin0_2_point",
                       "geometry_release": layout["release"]})
    return output
