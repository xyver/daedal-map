"""Contract checks for one Global administrative viewport resolution."""
from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
from shapely.geometry import box

from mapmover.runtime import global_admin_bbox_query as bbox_query


class GlobalAdminBBoxTests(unittest.TestCase):
    def test_country_shallow_bbox_reads_one_admin_0_3_bank_and_returns_country_ids(self):
        from mapmover.runtime import admin_spine_query

        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 1},
                   {"lat": 1, "lon": 0}, {"lat": 1, "lon": 1}]
        class Connection:
            def __init__(self):
                self.query = ""
                self.params = []
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def execute(self, query, params=None):
                if params is not None:
                    self.query, self.params = query, params
                return self
            def fetchdf(self):
                return pd.DataFrame([{
                    "loc_id": "CAN-NT-6101", "name": "Country division",
                    "parent_id": "CAN-NT", "admin_level": 2,
                    "admin_1_loc_id": "CAN-NT", "geometry_wkb": box(0, 0, 1, 1).wkb,
                }])
        connection = Connection()
        with mock.patch.object(admin_spine_query, "layout_available", return_value=True), \
                mock.patch.object(admin_spine_query, "layout_root", return_value=Path("country")), \
                mock.patch.object(bbox_query, "lease_query_connection", return_value=connection), \
                mock.patch.object(bbox_query, "path_to_uri", return_value="country/admin_0_3.parquet"):
            result = bbox_query.resolve_country_shallow_bbox(corners, "CAN")
        self.assertEqual(result["best_match"]["loc_id"], "CAN-NT-6101")
        self.assertEqual(result["best_match"]["deep_scope_loc_id"], "CAN-NT")
        self.assertEqual(result["scope_id"], "CAN")
        self.assertEqual(result["query_layout"], "country_admin_spine_shallow")
        self.assertEqual(connection.params[0], "country/admin_0_3.parquet")
        self.assertIn("admin_level <= 3", connection.query)
        self.assertNotIn("admin_1_loc_id =", connection.query)

    def test_deep_scope_id_uses_country_route_without_global_lookup(self):
        from mapmover.runtime import family_point_resolution

        owner_id = "GBR-E92000001"
        route_row = pd.DataFrame([{"admin_level": 1, "admin_1_loc_id": owner_id}])
        with mock.patch.object(family_point_resolution, "layout_available", return_value=True), \
                mock.patch.object(family_point_resolution, "load_route_rows_by_loc_ids",
                                  return_value=route_row):
            country, owner, error = family_point_resolution.shallow_scope(owner_id)
        self.assertEqual((country, owner, error), ("GBR", owner_id, None))

    def test_global_admin2_id_is_not_a_country_deep_scope(self):
        from mapmover.runtime import family_point_resolution

        baseline_id = "GBR-G114339913B95766344400054-G29080712B97999030763342"
        with mock.patch.object(family_point_resolution, "layout_available", return_value=True), \
                mock.patch.object(family_point_resolution, "load_route_rows_by_loc_ids",
                                  return_value=pd.DataFrame()) as route:
            country, owner, error = family_point_resolution.shallow_scope(baseline_id)
        self.assertEqual((country, owner), ("", None))
        self.assertEqual(error["code"], "invalid_shallow_loc_id")
        self.assertEqual(route.call_count, 1)
        self.assertIn("scope_id", error["message"])
        self.assertIn("deep_scope_loc_id", error["message"])
        self.assertIn("Global baseline IDs", error["message"])

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
        self.assertEqual(result["best_match"]["loc_id"], "county")
        self.assertIn("different areas", result["admin2_systems"]["relationship"])
        self.assertNotIn("deep_scope_loc_id", result["best_match"])
        self.assertEqual(result["best_match"]["view_coverage_pct"], 50.0)
        self.assertEqual({row["loc_id"] for row in result["matches_by_admin_level"]["1"]}, {"west", "east"})
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
        layout = {"bbox_index": Path("bbox.parquet"),
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
        self.assertEqual(result["matches_by_admin_level"]["1"][0]["loc_id"], "CAN-GNT")
        self.assertEqual(result["matches_by_admin_level"]["2"], [])

    def test_deep_view_uses_one_admin1_partition(self):
        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 1},
                   {"lat": 1, "lon": 0}, {"lat": 1, "lon": 1}]
        from mapmover.runtime import admin_spine_query, family_point_resolution, global_admin_shallow_query

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

        for has_deep, expected_count in ((True, 2), (False, 1)):
            with self.subTest(has_deep=has_deep):
                connection = Connection()
                with mock.patch.object(family_point_resolution, "shallow_scope", return_value=("CAN", "CAN-NT", None)), \
                        mock.patch.object(admin_spine_query, "layout_available", return_value=True), \
                        mock.patch.object(admin_spine_query, "layout_root", return_value=Path("/country")), \
                        mock.patch.object(admin_spine_query, "_has_deep_partitions", return_value=has_deep), \
                        mock.patch.object(bbox_query, "lease_query_connection", return_value=connection), \
                        mock.patch.object(bbox_query, "path_to_uri", side_effect=str):
                    result = bbox_query.resolve_country_admin_bbox(corners, "CAN-NT")
                self.assertEqual(result["admin_1_scope"], "CAN-NT")
                self.assertEqual(result["best_match"]["loc_id"], "CAN-NT-6101")
                self.assertEqual(len(connection.paths), expected_count)
                if has_deep:
                    self.assertTrue(str(connection.paths[1]).replace("\\", "/").endswith("deep/CAN-NT.parquet"))

    def test_unknown_global_admin2_id_stays_invalid(self):
        from mapmover.runtime import family_point_resolution

        corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 1},
                   {"lat": 1, "lon": 0}, {"lat": 1, "lon": 1}]
        invalid = {"code": "invalid_shallow_loc_id", "message": "Unknown ID"}
        with mock.patch.object(family_point_resolution, "shallow_scope",
                               return_value=("", None, invalid)):
            result = bbox_query.resolve_country_admin_bbox(corners, "GBR-G-NOTREAL")
        self.assertEqual(result["error"]["code"], "invalid_shallow_loc_id")
