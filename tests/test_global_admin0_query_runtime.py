import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from shapely.geometry import MultiPolygon, box

from mapmover.runtime.geometry_predicate_query import stable_hash_shard
from mapmover.runtime.global_admin0_query import (
    _active_layout,
    load_global_admin0_geometries,
    resolve_global_admin0_query_points,
)


class GlobalAdmin0QueryRuntimeTests(unittest.TestCase):
    def test_active_layout_accepts_one_physical_point_bank(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bank = root / "point_bank.parquet"
            pd.DataFrame([{"candidate_id": "AAA~full"}]).to_parquet(bank, index=False)
            catalog = {
                "catalog_fingerprint": "single-bank-test",
                "runtime_query_layouts": {"global_admin0_point": {
                    "layout_id": "global_admin0_point_query_v2",
                    "source_fingerprint": "source-test",
                    "representation": "full",
                    "authoritative_for_containment": True,
                    "point_bank": {"path": "geometry/runtime/global_admin0_point/point_bank.parquet"},
                }},
            }
            with (
                patch("mapmover.runtime.global_admin0_query.load_geometry_catalog", return_value=catalog),
                patch("mapmover.runtime.global_admin0_query.GEOMETRY_DIR", root / "geometry"),
            ):
                # _artifact_path resolves relative to GEOMETRY_DIR.parent.
                expected = root / "geometry" / "runtime" / "global_admin0_point" / "point_bank.parquet"
                expected.parent.mkdir(parents=True)
                bank.replace(expected)
                layout = _active_layout()

        self.assertEqual(layout["point_bank"], expected)

    def test_exact_candidates_choose_smallest_covering_full_polygon(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            point_bank = root / "point_bank.parquet"
            candidates = [
                {
                    "candidate_id": "AAA~global", "loc_id": "AAA", "name": "Host",
                    "source_kind": "global_full", "bbox_min_lon": 0.0, "bbox_min_lat": 0.0,
                    "bbox_max_lon": 10.0, "bbox_max_lat": 10.0, "area_sq_degrees": 100.0,
                    "geometry_wkb": box(0, 0, 10, 10).wkb,
                },
                {
                    "candidate_id": "BBB~supplemental", "loc_id": "BBB", "name": "Territory",
                    "source_kind": "supplemental_admin0", "bbox_min_lon": 2.0, "bbox_min_lat": 2.0,
                    "bbox_max_lon": 4.0, "bbox_max_lat": 4.0, "area_sq_degrees": 4.0,
                    "geometry_wkb": box(2, 2, 4, 4).wkb,
                },
            ]
            pd.DataFrame(candidates).to_parquet(
                point_bank, index=False, row_group_size=1,
            )
            layout = {
                "layout_id": "test", "point_bank": point_bank,
            }
            with patch("mapmover.runtime.global_admin0_query._active_layout", return_value=layout):
                matches = resolve_global_admin0_query_points([
                    {"lon": 3.0, "lat": 3.0}, {"lon": 8.0, "lat": 8.0},
                ])

        self.assertEqual([match["loc_id"] for match in matches], ["BBB", "AAA"])
        self.assertEqual(matches[0]["geometry_review"]["conflicts"], [
            {"admin_level": "admin_0", "candidate_loc_ids": ["AAA", "BBB"]}
        ])
        self.assertNotIn("geometry_review", matches[1])

    def test_v3_layout_requires_component_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = root / "geometry/runtime/global_admin0_point"
            directory.mkdir(parents=True)
            bank = directory / "point_bank.parquet"
            index = directory / "component_bbox_index.parquet"
            pd.DataFrame([{"candidate_id": "AAA~full"}]).to_parquet(bank, index=False)
            catalog = {"catalog_fingerprint": "v3-test", "runtime_query_layouts": {
                "global_admin0_point": {
                    "layout_id": "global_admin0_point_query_v3",
                    "source_fingerprint": "source-v3",
                    "representation": "full",
                    "authoritative_for_containment": True,
                    "point_bank": {"path": "geometry/runtime/global_admin0_point/point_bank.parquet"},
                    "component_bbox_index": {
                        "path": "geometry/runtime/global_admin0_point/component_bbox_index.parquet"},
                },
            }}
            with (
                patch("mapmover.runtime.global_admin0_query.load_geometry_catalog", return_value=catalog),
                patch("mapmover.runtime.global_admin0_query.GEOMETRY_DIR", root / "geometry"),
                patch("mapmover.runtime.global_admin0_query.is_cloud_mode", return_value=False),
            ):
                self.assertIsNone(_active_layout())
                pd.DataFrame([{"candidate_id": "AAA~full"}]).to_parquet(index, index=False)
                layout = _active_layout()
                self.assertEqual(layout["component_bbox_index"], index)

    def test_missing_layout_requests_full_fallback(self):
        with patch("mapmover.runtime.global_admin0_query._active_layout", return_value=None):
            self.assertIsNone(resolve_global_admin0_query_points([{"lon": 0, "lat": 0}]))

    def test_component_boxes_skip_distant_parts_and_select_exact_land(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bank = root / "point_bank.parquet"
            index = root / "component_bbox_index.parquet"
            land = MultiPolygon([box(0, 0, 1, 1), box(10, 10, 11, 11)])
            pd.DataFrame([{
                "candidate_id": "AAA~full", "loc_id": "AAA", "name": "Archipelago",
                "source_kind": "global_full", "area_sq_degrees": land.area,
                "geometry_wkb": land.wkb,
                "bbox_min_lon": 0.0, "bbox_min_lat": 0.0,
                "bbox_max_lon": 11.0, "bbox_max_lat": 11.0,
            }]).to_parquet(bank, index=False, row_group_size=1)
            pd.DataFrame([
                {"candidate_id": "AAA~full", "bbox_min_lon": x,
                 "bbox_min_lat": y, "bbox_max_lon": x + 1,
                 "bbox_max_lat": y + 1}
                for x, y in [(0.0, 0.0), (10.0, 10.0)]
            ]).to_parquet(index, index=False)
            layout = {"layout_id": "global_admin0_point_query_v3",
                      "point_bank": bank, "component_bbox_index": index}
            with patch("mapmover.runtime.global_admin0_query._active_layout", return_value=layout):
                results = resolve_global_admin0_query_points([
                    {"lon": 5.0, "lat": 5.0}, {"lon": 0.5, "lat": 0.5},
                ])
        self.assertIsNone(results[0])
        self.assertEqual(results[1]["loc_id"], "AAA")

    def test_exact_shape_lookup_reads_only_selected_full_shards(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bbox_path = root / "bbox.parquet"
            candidates = [
                {
                    "candidate_id": "AAA~global", "loc_id": "AAA", "name": "Host",
                    "source_kind": "global_full", "bbox_min_lon": 0.0, "bbox_min_lat": 0.0,
                    "bbox_max_lon": 10.0, "bbox_max_lat": 10.0, "area_sq_degrees": 100.0,
                    "geometry_wkb": box(0, 0, 10, 10).wkb,
                },
                {
                    "candidate_id": "BBB~global", "loc_id": "BBB", "name": "Other",
                    "source_kind": "global_full", "bbox_min_lon": 20.0, "bbox_min_lat": 20.0,
                    "bbox_max_lon": 22.0, "bbox_max_lat": 22.0, "area_sq_degrees": 4.0,
                    "geometry_wkb": box(20, 20, 22, 22).wkb,
                },
            ]
            pd.DataFrame(candidates).drop(columns=["geometry_wkb"]).to_parquet(bbox_path, index=False)
            shard_paths = {}
            for index in range(2):
                shard = f"{index:02d}"
                path = root / f"{shard}.parquet"
                rows = [row for row in candidates if stable_hash_shard(row["candidate_id"], 2) == shard]
                pd.DataFrame(rows, columns=list(candidates[0])).to_parquet(path, index=False, row_group_size=1)
                shard_paths[shard] = path
            layout = {
                "layout_id": "test", "bbox_index": bbox_path,
                "point_shards": shard_paths, "shard_count": 2,
            }
            with patch("mapmover.runtime.global_admin0_query._active_layout", return_value=layout):
                rows = load_global_admin0_geometries(["BBB"])

        self.assertEqual(rows["loc_id"].tolist(), ["BBB"])
        self.assertEqual(rows.iloc[0]["geometry"], candidates[1]["geometry_wkb"])
        self.assertEqual(rows.iloc[0]["source_system"], "global_full")


if __name__ == "__main__":
    unittest.main()
