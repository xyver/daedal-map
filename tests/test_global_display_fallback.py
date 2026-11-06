"""Default countries use published Global Display rows for viewport shapes."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

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


def test_viewport_routes_profiled_and_default_countries_to_separate_display_sources():
    global_rows = pd.DataFrame([{
        "loc_id": "CAF-G1", "admin_level": 1,
        "bbox_min_lon": 18, "bbox_min_lat": 4,
        "bbox_max_lon": 24, "bbox_max_lat": 9,
    }])
    country_rows = pd.DataFrame([{
        "loc_id": "USA-KS", "admin_level": 1,
        "bbox_min_lon": -103, "bbox_min_lat": 37,
        "bbox_max_lon": -94, "bbox_max_lat": 40,
    }])
    with patch.object(geometry_handlers, "get_countries_in_bbox", return_value=["CAF", "USA"]), \
         patch.object(geometry_handlers, "load_geometry_catalog", return_value={
             "country_profiles": [{"country_code": "USA"}]
         }), \
         patch.object(geometry_handlers, "_load_global_display_viewport_rows",
                      return_value=global_rows) as global_loader, \
         patch.object(geometry_handlers, "load_country_display_rows",
                      return_value=country_rows) as country_loader:
        result = geometry_handlers.get_geometry_index(
            admin_level=1, bbox=(-105, 3, 25, 41)
        )

    global_loader.assert_called_once()
    assert global_loader.call_args.args[0] == ["CAF"]
    country_loader.assert_called_once()
    assert country_loader.call_args.args[0] == "USA"
    assert {row["loc_id"] for row in result["rows"]} == {"CAF-G1", "USA-KS"}
