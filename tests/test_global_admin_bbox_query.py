"""Contract checks for one Global administrative viewport resolution."""
from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
from shapely.geometry import box

from mapmover.runtime import global_admin_bbox_query as bbox_query


class GlobalAdminBBoxTests(unittest.TestCase):
    def test_rectangle_rejects_diagonal_and_wrapped_inputs(self):
        with self.assertRaisesRegex(ValueError, "four"):
            bbox_query.rectangle_bounds([{"lat": 0, "lon": 0}])
        with self.assertRaisesRegex(ValueError, "axis-aligned"):
            bbox_query.rectangle_bounds([
                {"lat": 0, "lon": 0}, {"lat": 0, "lon": 2},
                {"lat": 2, "lon": 1}, {"lat": 2, "lon": 2},
            ])

    def test_overlap_ranks_scale_and_retains_border_matches(self):
        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 2},
                   {"lat": 2, "lon": 0}, {"lat": 2, "lon": 2}]
        layout = {"bbox_index": Path("bbox.parquet"),
                  "preferred_lookup": Path("lookup.parquet"),
                  **{f"admin_{level}": Path(f"admin_{level}.parquet") for level in range(3)},
                  "release": "1.1.1"}
        candidates = pd.DataFrame({"loc_id": ["country", "west", "east", "county"],
                                   "admin_level": [0, 1, 1, 2]})
        exact = {
            "admin_0.parquet": pd.DataFrame([{"loc_id": "country", "name": "Country", "parent_id": "",
                                               "geometry_wkb": box(-5, -5, 5, 5).wkb}]),
            "admin_1.parquet": pd.DataFrame([
                {"loc_id": "west", "name": "West", "parent_id": "country", "geometry_wkb": box(-2, -2, 1, 4).wkb},
                {"loc_id": "east", "name": "East", "parent_id": "country", "geometry_wkb": box(1, -2, 4, 4).wkb},
            ]),
            "admin_2.parquet": pd.DataFrame([{"loc_id": "county", "name": "County", "parent_id": "west",
                                               "geometry_wkb": box(0, 0, 1, 2).wkb}]),
            "lookup.parquet": pd.DataFrame({"baseline_loc_id": ["country", "west", "east", "county"],
                                            "preferred_loc_id": ["C", "C-W", "C-E", "C-W-1"]}),
        }

        def read(path, *_args, **_kwargs):
            return exact[path.name]

        with mock.patch.object(bbox_query, "_selected_layout", return_value=layout), \
                mock.patch.object(bbox_query, "parquet_available", return_value=True), \
                mock.patch.object(bbox_query, "path_to_uri", return_value="bbox.parquet"), \
                mock.patch.object(bbox_query, "run_df", return_value=candidates), \
                mock.patch.object(bbox_query, "read_rows_by_ids", side_effect=read):
            result = bbox_query.resolve_global_admin_bbox(corners)
        self.assertEqual(result["best_match"]["loc_id"], "C-W-1")
        self.assertEqual(result["best_match"]["view_coverage_pct"], 50.0)
        self.assertEqual({row["loc_id"] for row in result["matches_by_admin_level"]["1"]}, {"C-W", "C-E"})
        self.assertEqual(result["query_layout"], "global_admin0_2_point")

    def test_broad_view_returns_bounded_error(self):
        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 2},
                   {"lat": 2, "lon": 0}, {"lat": 2, "lon": 2}]
        with mock.patch.object(bbox_query, "_selected_layout", return_value={"bbox_index": Path("bbox")}), \
                mock.patch.object(bbox_query, "parquet_available", return_value=True), \
                mock.patch.object(bbox_query, "path_to_uri", return_value="bbox"), \
                mock.patch.object(bbox_query, "run_df", return_value=pd.DataFrame(range(501))):
            result = bbox_query.resolve_global_admin_bbox(corners)
        self.assertEqual(result["error"]["code"], "bbox_too_broad")

    def test_translated_canada_economic_region_stays_out_of_admin_ranking(self):
        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 2},
                   {"lat": 2, "lon": 0}, {"lat": 2, "lon": 2}]
        layout = {"bbox_index": Path("bbox.parquet"), "preferred_lookup": Path("lookup.parquet"),
                  **{f"admin_{level}": Path(f"admin_{level}.parquet") for level in range(3)},
                  "release": "1.1.2"}
        candidates = pd.DataFrame({"loc_id": ["CAN", "CAN-GNT", "CAN-GNT-GER"],
                                   "admin_level": [0, 1, 2]})
        exact = {
            f"admin_{level}.parquet": pd.DataFrame([{
                "loc_id": loc_id, "name": name, "parent_id": parent,
                "geometry_wkb": box(-1, -1, 3, 3).wkb,
            }]) for level, loc_id, parent, name in [
                (0, "CAN", "", "Canada"), (1, "CAN-GNT", "CAN", "Northwest Territories"),
                (2, "CAN-GNT-GER", "CAN-GNT", "NWT economic region")]
        }
        exact["lookup.parquet"] = pd.DataFrame({
            "baseline_loc_id": ["CAN", "CAN-GNT", "CAN-GNT-GER"],
            "preferred_loc_id": ["CAN", "CAN-NT", "CAN-NT-6101"],
        })
        def read(path, ids, **_kwargs):
            frame = exact[path.name]
            key = "baseline_loc_id" if path.name == "lookup.parquet" else "loc_id"
            return frame[frame[key].isin(ids)]

        with mock.patch.object(bbox_query, "_selected_layout", return_value=layout), \
                mock.patch.object(bbox_query, "parquet_available", return_value=True), \
                mock.patch.object(bbox_query, "path_to_uri", return_value="bbox.parquet"), \
                mock.patch.object(bbox_query, "run_df", return_value=candidates), \
                mock.patch.object(bbox_query, "read_rows_by_ids", side_effect=read):
            result = bbox_query.resolve_global_admin_bbox(corners)
        self.assertEqual(result["matches_by_admin_level"]["1"][0]["loc_id"], "CAN-NT")
        self.assertEqual(result["matches_by_admin_level"]["2"], [])

    def test_deep_view_uses_one_admin1_partition(self):
        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 1},
                   {"lat": 1, "lon": 0}, {"lat": 1, "lon": 1}]
        from mapmover.runtime import admin_spine_query, family_point_resolution

        class Connection:
            def __init__(self):
                self.paths = []
                self.rows = pd.DataFrame()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                pass

            def execute(self, sql, params=None):
                if params:
                    self.paths.append(params[0])
                    self.rows = (pd.DataFrame([{
                        "loc_id": "CAN-NT-6101", "name": "Region 1", "parent_id": "CAN-NT",
                        "admin_level": 2, "geometry_wkb": box(0, 0, 1, 1).wkb,
                    }]) if "admin_0_3" in str(params[0]) else pd.DataFrame())
                return self

            def fetchdf(self):
                return self.rows

        connection = Connection()
        with mock.patch.object(family_point_resolution, "shallow_scope", return_value=("CAN", "CAN-NT", None)), \
                mock.patch.object(admin_spine_query, "layout_available", return_value=True), \
                mock.patch.object(admin_spine_query, "layout_root", return_value=Path("/country")), \
                mock.patch.object(bbox_query, "lease_query_connection", return_value=connection), \
                mock.patch.object(bbox_query, "path_to_uri", side_effect=str):
            result = bbox_query.resolve_country_admin_bbox(corners, "CAN-NT")
        self.assertEqual(result["admin_1_scope"], "CAN-NT")
        self.assertEqual(result["best_match"]["loc_id"], "CAN-NT-6101")
        self.assertEqual(len(connection.paths), 2)
        self.assertTrue(str(connection.paths[1]).replace("\\", "/").endswith("deep/CAN-NT.parquet"))
