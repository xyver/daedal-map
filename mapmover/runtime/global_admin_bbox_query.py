"""Compare one rectangular viewport with the published Global Admin 0-2 spine."""
from __future__ import annotations

from typing import Any

from shapely.geometry import box
from shapely.ops import transform
from shapely.wkb import loads as load_wkb

from ..duckdb_helpers import lease_query_connection, parquet_available, path_to_uri, run_df
from ..mcp_execution import mark_mcp_stage, record_mcp_file
from .geometry_predicate_query import read_rows_by_ids
from .global_admin_shallow_query import GLOBAL_ADMIN2_SYSTEMS, _selected_layout
from .global_admin_spine_policy import admitted_admin_match
from .bbox_geometry import MAX_BBOX_CANDIDATES, _AREA_TRANSFORM, rectangle_bounds, overlap_percentages

_MAX_CANDIDATES = MAX_BBOX_CANDIDATES


def resolve_global_admin_bbox(corners: list[dict[str, Any]]) -> dict[str, Any]:
    """Return best scale and border overlaps; percentages use equal-area metres."""
    west, south, east, north = rectangle_bounds(corners)
    layout = _selected_layout()
    if layout is None:
        return {"error": {"code": "selected_scope_unavailable",
                          "message": "Published Global Admin 0-2 query layout is unavailable"}}
    viewport = transform(_AREA_TRANSFORM, box(west, south, east, north))
    viewport_area = viewport.area
    if viewport_area <= 0:
        raise ValueError("viewport has zero projected area")
    mark_mcp_stage("bbox_global_admin0_2_index")
    record_mcp_file(layout["bbox_index"])
    if not parquet_available(layout["bbox_index"]):
        return {"error": {"code": "selected_scope_unavailable", "message": "Global bbox index is unavailable"}}
    candidates = run_df(
        "SELECT loc_id, admin_level FROM read_parquet(?) WHERE "
        "bbox_min_lon < ? AND bbox_max_lon > ? AND bbox_min_lat < ? AND bbox_max_lat > ? "
        "LIMIT ?",
        [path_to_uri(layout["bbox_index"]), east, west, north, south, _MAX_CANDIDATES + 1],
    )
    if len(candidates) > _MAX_CANDIDATES:
        return {"error": {"code": "bbox_too_broad",
                          "message": "Viewport intersects too many Global admin candidates; zoom in"},
                "candidate_limit": _MAX_CANDIDATES}
    levels: dict[int, set[str]] = {0: set(), 1: set(), 2: set()}
    for row in candidates.itertuples(index=False):
        level = int(row.admin_level)
        if level in levels and admitted_admin_match(str(row.loc_id), level):
            levels[level].add(str(row.loc_id))
    mark_mcp_stage("bbox_global_admin0_2_exact")
    matched = []
    for level, ids in levels.items():
        if not ids:
            continue
        record_mcp_file(layout[f"admin_{level}"])
        rows = read_rows_by_ids(layout[f"admin_{level}"], ids, id_column="loc_id",
                                columns=["name", "parent_id", "geometry_wkb"], raw_geoparquet=True)
        for row in rows.to_dict("records"):
            if row.get("geometry_wkb") is None:
                continue
            percentages = overlap_percentages(load_wkb(bytes(row["geometry_wkb"])), viewport)
            if percentages is None:
                continue
            matched.append({"baseline_loc_id": str(row["loc_id"]),
                            "name": str(row.get("name") or row["loc_id"]),
                            "admin_level": level,
                            "parent_baseline_loc_id": str(row.get("parent_id") or ""),
                            **percentages})
    for row in matched:
        row["loc_id"] = row["baseline_loc_id"]
    matched.sort(key=lambda row: (-row["intersection_over_union_pct"],
                                  -row["view_coverage_pct"], row["loc_id"]))
    by_level = {str(level): [row for row in matched if row["admin_level"] == level][:5]
                for level in range(3)}
    best = matched[0] if matched else None
    return {"bbox": {"west": west, "south": south, "east": east, "north": north},
            "best_match": best, "matches_by_admin_level": by_level,
            "ranking": "highest equal-area intersection-over-union",
            "geometry_release": layout["release"], "query_layout": "global_admin0_2_point",
            "admin2_systems": GLOBAL_ADMIN2_SYSTEMS}


def _country_bbox_match_rows(rows: Any, viewport: Any, area: float) -> list[dict[str, Any]]:
    matches = []
    for row in rows.to_dict("records"):
        if row.get("geometry_wkb") is None:
            continue
        percentages = overlap_percentages(load_wkb(bytes(row["geometry_wkb"])), viewport)
        if percentages is None:
            continue
        match = {"loc_id": str(row["loc_id"]), "name": str(row.get("name") or row["loc_id"]),
                        "admin_level": int(row["admin_level"]),
                        "parent_loc_id": str(row.get("parent_id") or ""),
                        **percentages}
        owner = str(row.get("admin_1_loc_id") or "")
        if owner:
            match["deep_scope_loc_id"] = owner
        matches.append(match)
    return matches


def resolve_country_shallow_bbox(corners: list[dict[str, Any]], country: str) -> dict[str, Any]:
    """Compare one viewport with one country's selected Admin 0-3 bank."""
    from .admin_spine_query import layout_available, layout_root

    west, south, east, north = rectangle_bounds(corners)
    country = str(country or "").strip().upper()
    if not layout_available(country):
        return {"error": {"code": "selected_scope_unavailable",
                          "message": f"Selected {country} Admin 0-3 layout is unavailable"}}
    path = layout_root(country) / "admin_0_3.parquet"
    viewport = transform(_AREA_TRANSFORM, box(west, south, east, north))
    area = viewport.area
    if area <= 0:
        raise ValueError("viewport has zero projected area")
    mark_mcp_stage("bbox_country_admin_shallow")
    record_mcp_file(path)
    with lease_query_connection() as connection:
        try:
            connection.execute("LOAD spatial")
        except Exception:
            connection.execute("INSTALL spatial")
            connection.execute("LOAD spatial")
        rows = connection.execute(
            "SELECT loc_id, name, parent_id, admin_level, admin_1_loc_id, "
            "ST_AsWKB(geometry) AS geometry_wkb FROM read_parquet(?) "
            "WHERE admin_level <= 3 AND bbox_min_lon < ? AND bbox_max_lon > ? "
            "AND bbox_min_lat < ? AND bbox_max_lat > ? LIMIT ?",
            [path_to_uri(path), east, west, north, south, _MAX_CANDIDATES + 1],
        ).fetchdf()
    if len(rows) > _MAX_CANDIDATES:
        return {"error": {"code": "bbox_too_broad",
                          "message": "Viewport intersects too many country admin candidates; zoom in"},
                "candidate_limit": _MAX_CANDIDATES}
    matches = _country_bbox_match_rows(rows, viewport, area)
    matches.sort(key=lambda row: (-row["intersection_over_union_pct"],
                                  -row["view_coverage_pct"], row["loc_id"]))
    return {"bbox": {"west": west, "south": south, "east": east, "north": north},
            "scope_id": country, "best_match": matches[0] if matches else None,
            "matches_by_admin_level": {str(level): [row for row in matches
                                                  if row["admin_level"] == level][:5]
                                       for level in range(4)},
            "ranking": "highest equal-area intersection-over-union",
            "query_layout": "country_admin_spine_shallow"}


def resolve_country_admin_bbox(corners: list[dict[str, Any]], shallow_loc_id: str) -> dict[str, Any]:
    """Second pass inside one admitted country Admin 1 owner, through its deep bank."""
    from .admin_spine_query import _has_deep_partitions, layout_available, layout_root
    from .family_point_resolution import shallow_scope

    west, south, east, north = rectangle_bounds(corners)
    country, owner, scope_error = shallow_scope(shallow_loc_id)
    if scope_error:
        return {"error": scope_error}
    if not owner or not layout_available(country):
        return {"error": {"code": "shallow_scope_unavailable",
                          "message": "Selected country Admin 1 layout is unavailable"}}
    root = layout_root(country)
    banks = [(root / "admin_0_3.parquet", 1)]
    if _has_deep_partitions(country):
        banks.append((root / "deep" / f"{owner}.parquet", 4))
    viewport = transform(_AREA_TRANSFORM, box(west, south, east, north))
    area = viewport.area
    if area <= 0:
        raise ValueError("viewport has zero projected area")
    matches: list[dict[str, Any]] = []
    candidate_count = 0
    mark_mcp_stage("bbox_country_admin_deep")
    with lease_query_connection() as connection:
        try:
            connection.execute("LOAD spatial")
        except Exception:
            connection.execute("INSTALL spatial")
            connection.execute("LOAD spatial")
        for path, minimum_level in banks:
            record_mcp_file(path)
            rows = connection.execute(
                "SELECT loc_id, name, parent_id, admin_level, "
                "ST_AsWKB(geometry) AS geometry_wkb FROM read_parquet(?) "
                "WHERE admin_1_loc_id = ? AND admin_level >= ? "
                "AND bbox_min_lon < ? AND bbox_max_lon > ? "
                "AND bbox_min_lat < ? AND bbox_max_lat > ? LIMIT ?",
                [path_to_uri(path), owner, minimum_level, east, west, north, south,
                 _MAX_CANDIDATES + 1],
            ).fetchdf()
            candidate_count += len(rows)
            if candidate_count > _MAX_CANDIDATES:
                return {"error": {"code": "bbox_too_broad",
                                  "message": "Viewport intersects too many country admin candidates; zoom in"},
                        "candidate_limit": _MAX_CANDIDATES}
            matches.extend(_country_bbox_match_rows(rows, viewport, area))
    matches.sort(key=lambda row: (-row["intersection_over_union_pct"],
                                  -row["view_coverage_pct"], row["loc_id"]))
    return {"bbox": {"west": west, "south": south, "east": east, "north": north},
            "shallow_loc_id": shallow_loc_id, "admin_1_scope": owner,
            "best_match": matches[0] if matches else None,
            "matches_by_admin_level": {str(level): [row for row in matches
                                                  if row["admin_level"] == level][:5]
                                       for level in range(1, 7)},
            "ranking": "highest equal-area intersection-over-union",
            "query_layout": "country_admin_spine_deep"}
