"""Default countries use published Global Display rows for viewport shapes."""

import json
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd

from mapmover import geometry_handlers


def _shape(west, south, east, north):
    return json.dumps({
        "type": "Polygon",
        "coordinates": [[[west, south], [east, south], [east, north], [west, north], [west, south]]],
    })


def test_default_country_viewport_uses_global_display_and_bbox_index():
    with tempfile.TemporaryDirectory() as temp_dir:
        geometry_dir = Path(temp_dir) / "geometry"
        display_dir = geometry_dir / "global" / "display"
        display_dir.mkdir(parents=True)
        index_path = Path(temp_dir) / "bbox_index.parquet"
        inside = "CAF-G1-G2"
        outside = "CAF-G1-G3"
        pd.DataFrame([
            {"country_iso3": "CAF", "loc_id": inside, "parent_id": "CAF-G1",
             "admin_level": 2, "name": "Inside", "geometry": _shape(19, 5, 20, 6)},
            {"country_iso3": "CAF", "loc_id": outside, "parent_id": "CAF-G1",
             "admin_level": 2, "name": "Outside", "geometry": _shape(30, 5, 31, 6)},
        ]).to_parquet(display_dir / "admin_2.parquet", index=False)
        pd.DataFrame([
            {"country_iso3": "CAF", "loc_id": "CAF-G1", "parent_id": "CAF",
             "admin_level": 1, "name": "Parent", "geometry": _shape(18, 4, 25, 9)},
        ]).to_parquet(display_dir / "admin_1.parquet", index=False)
        pd.DataFrame([
            {"country_iso3": "CAF", "loc_id": inside, "admin_level": 2,
             "bbox_min_lon": 19, "bbox_min_lat": 5, "bbox_max_lon": 20, "bbox_max_lat": 6},
            {"country_iso3": "CAF", "loc_id": outside, "admin_level": 2,
             "bbox_min_lon": 30, "bbox_min_lat": 5, "bbox_max_lon": 31, "bbox_max_lat": 6},
            {"country_iso3": "CAF", "loc_id": "CAF-G1", "admin_level": 1,
             "bbox_min_lon": 18, "bbox_min_lat": 4, "bbox_max_lon": 25, "bbox_max_lat": 9},
        ]).to_parquet(index_path, index=False)

        with patch.object(geometry_handlers, "GEOMETRY_DIR", geometry_dir), \
             patch.object(geometry_handlers, "get_countries_in_bbox", return_value=["CAF"]), \
             patch.object(geometry_handlers, "load_geometry_catalog", return_value={"country_profiles": []}), \
             patch("mapmover.runtime.global_admin_shallow_query._selected_layout",
                   return_value={"bbox_index": index_path}):
            result = geometry_handlers.get_geometry_index(
                admin_level=2, bbox=(18, 4, 24, 9)
            )
            features = geometry_handlers.get_display_geometries([inside])

    assert [row["loc_id"] for row in result["rows"]] == [inside]
    assert len(features["features"]) == 1
    props = features["features"][0]["properties"]
    assert props["loc_id"] == inside
    assert props["bbox_min_lon"] == 19


def test_viewport_routes_profiled_and_default_countries_to_global_through_admin2():
    global_rows = pd.DataFrame([
        {"loc_id": "CAF-G1", "admin_level": 1},
        {"loc_id": "USA-G1", "admin_level": 1},
    ])
    with patch.object(geometry_handlers, "get_countries_in_bbox", return_value=["CAF", "USA"]), \
         patch.object(geometry_handlers, "load_geometry_catalog", return_value={
             "country_profiles": [{"country_code": "USA"}]
         }), \
         patch.object(geometry_handlers, "_load_global_display_viewport_rows",
                      return_value=global_rows) as global_loader, \
         patch.object(geometry_handlers, "load_country_display_rows") as country_loader:
        result = geometry_handlers.get_geometry_index(
            admin_level=1, bbox=(-105, 3, 25, 41)
        )

    global_loader.assert_called_once()
    assert global_loader.call_args.args[0] == ["CAF", "USA"]
    country_loader.assert_not_called()
    assert {row["loc_id"] for row in result["rows"]} == {"CAF-G1", "USA-G1"}


def test_shape_batch_reads_global_display_once_per_level_across_countries():
    with tempfile.TemporaryDirectory() as temp_dir:
        geometry_dir = Path(temp_dir) / "geometry"
        display_dir = geometry_dir / "global" / "display"
        display_dir.mkdir(parents=True)
        index_path = Path(temp_dir) / "bbox_index.parquet"
        levels = {1: ["CAF-G1", "TCD-G1"], 2: ["CAF-G1-G2", "TCD-G1-G2"]}
        index_rows = []
        for level, ids in levels.items():
            rows = []
            for position, loc_id in enumerate(ids):
                west = 18 + position * 3
                rows.append({"country_iso3": loc_id[:3], "loc_id": loc_id,
                             "parent_id": loc_id.rsplit("-", 1)[0],
                             "admin_level": level, "name": loc_id,
                             "geometry": _shape(west, 5, west + 1, 6)})
                index_rows.append({"country_iso3": loc_id[:3], "loc_id": loc_id,
                                   "admin_level": level, "bbox_min_lon": west,
                                   "bbox_min_lat": 5, "bbox_max_lon": west + 1,
                                   "bbox_max_lat": 6})
            pd.DataFrame(rows).to_parquet(display_dir / f"admin_{level}.parquet", index=False)
        pd.DataFrame(index_rows).to_parquet(index_path, index=False)

        original_select = geometry_handlers.select_rows
        reads = []

        def count_reads(path, **kwargs):
            reads.append(Path(path))
            return original_select(path, **kwargs)

        with patch.object(geometry_handlers, "GEOMETRY_DIR", geometry_dir), \
             patch.object(geometry_handlers, "load_geometry_catalog",
                          return_value={"country_profiles": []}), \
             patch("mapmover.runtime.global_admin_shallow_query._selected_layout",
                   return_value={"bbox_index": index_path}), \
             patch.object(geometry_handlers, "select_rows", side_effect=count_reads):
            features = geometry_handlers.get_display_geometries(
                [loc_id for ids in levels.values() for loc_id in ids]
            )["features"]

    assert {feature["properties"]["loc_id"] for feature in features} == {
        loc_id for ids in levels.values() for loc_id in ids
    }
    assert reads.count(index_path) == 2
    assert reads.count(display_dir / "admin_1.parquet") == 1
    assert reads.count(display_dir / "admin_2.parquet") == 1


def test_shape_batch_can_reuse_bbox_from_prior_viewport_index():
    with tempfile.TemporaryDirectory() as temp_dir:
        geometry_dir = Path(temp_dir) / "geometry"
        display_dir = geometry_dir / "global" / "display"
        display_dir.mkdir(parents=True)
        loc_id = "CAN-G1-G2"
        pd.DataFrame([{"country_iso3": "CAN", "loc_id": loc_id,
                       "parent_id": "CAN-G1", "admin_level": 2,
                       "name": "Example", "geometry": _shape(-112, 60, -111, 61)}]
                     ).to_parquet(display_dir / "admin_2.parquet", index=False)
        with patch.object(geometry_handlers, "GEOMETRY_DIR", geometry_dir), \
             patch("mapmover.runtime.global_admin_shallow_query._selected_layout",
                   return_value={"bbox_index": Path(temp_dir) / "unused.parquet"}) as layout, \
             patch.object(geometry_handlers, "select_rows",
                          wraps=geometry_handlers.select_rows) as reader:
            features = geometry_handlers.get_display_geometries(
                [loc_id], include_bbox=False,
            )["features"]
    layout.assert_called_once()
    assert reader.call_count == 1
    assert features[0]["properties"]["loc_id"] == loc_id
    assert "bbox_min_lon" not in features[0]["properties"]


def test_display_route_reports_timing_for_opt_in_read():
    from mapmover.routes import geometry as geometry_routes

    def handler(loc_ids, *, include_bbox=True, timing_ms=None):
        assert loc_ids == ["CAN-G1-G2"]
        assert include_bbox is False
        timing_ms["display"] = 3
        return {"type": "FeatureCollection", "features": []}

    with patch.object(geometry_routes, "decode_request_body", new_callable=AsyncMock,
                      return_value={"loc_ids": ["CAN-G1-G2"], "bbox_from_index": True}):
        response = asyncio.run(geometry_routes._explicit_geometry_features(
            None, handler, route_name="/geometry/display-features",
            allow_index_bounds=True,
        ))
    assert response.status_code == 200
    assert "display;dur=3" in response.headers["Server-Timing"]
    assert "handler;dur=" in response.headers["Server-Timing"]


def test_shape_batch_uses_global_for_profiled_country_shallow_ids():
    global_rows = pd.DataFrame([{"loc_id": "USA-G1", "admin_level": 1,
                                 "name": "Kansas", "geometry": _shape(-103, 37, -94, 40)}])
    with patch.object(geometry_handlers, "load_geometry_catalog", return_value={
             "country_profiles": [{"country_code": "USA"}]
         }), \
         patch.object(geometry_handlers, "_load_global_display_id_rows",
                      return_value=global_rows) as global_loader, \
         patch.object(geometry_handlers, "load_country_display_rows") as country_loader:
        features = geometry_handlers.get_display_geometries(["USA-G1"])["features"]

    global_loader.assert_called_once_with(["USA-G1"], 1)
    country_loader.assert_not_called()
    assert features[0]["properties"]["loc_id"] == "USA-G1"


def test_shape_batch_uses_country_display_at_admin3():
    country_rows = pd.DataFrame([{"loc_id": "USA-KS-1-2", "admin_level": 3,
                                  "name": "Deep", "geometry": _shape(-103, 37, -94, 40)}])
    with patch.object(geometry_handlers, "_load_global_display_id_rows") as global_loader, \
         patch.object(geometry_handlers, "load_country_display_rows",
                      return_value=country_rows) as country_loader:
        features = geometry_handlers.get_display_geometries(["USA-KS-1-2"])["features"]
    global_loader.assert_not_called()
    country_loader.assert_called_once_with("USA", loc_ids=["USA-KS-1-2"])
    assert features[0]["properties"]["loc_id"] == "USA-KS-1-2"
