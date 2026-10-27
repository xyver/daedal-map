"""Node lookup is a candidate; full line coverage decides placement."""

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import LineString, box, mapping

from network.app.network_pack import Pack, write_pack
from network.geometry_assign import EXACT_ADMIN_MEMBER, assign


class GeometryAssignmentTests(unittest.TestCase):
    def test_bent_line_and_sibling_crossing_promote_to_full_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = [
                ("USA-CA", None, 1, box(0, 0, 4, 2)),
                ("USA-CA-001", "USA-CA", 2, box(0, 0, 2, 2)),
                ("USA-CA-003", "USA-CA", 2, box(2, 0, 4, 2)),
                ("USA-CA-001-A", "USA-CA-001", 3, box(0, 0, 1, 2)),
                ("USA-CA-001-B", "USA-CA-001", 3, box(1, 0, 2, 2)),
                ("USA-CA-003-A", "USA-CA-003", 3, box(2, 0, 3, 2)),
            ]
            table = pa.Table.from_pylist([
                {"loc_id": loc_id, "parent_id": parent, "admin_level": level,
                 "admin_1_loc_id": "USA-CA", "geometry": geometry.wkb}
                for loc_id, parent, level, geometry in rows
            ])
            bank = root / "admin.parquet"
            pq.write_table(table, bank)
            archive = root / "geometry.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("geometry.json", json.dumps({"country_code": "USA", "version": "1.3.5"}))
                bundle.write(bank, EXACT_ADMIN_MEMBER)

            lines = {
                "inside": [(0.2, 0.2), (0.8, 0.2)],
                "bend": [(0.2, 0.3), (1.5, 0.5), (0.8, 0.3)],
                "cross": [(1.5, 0.7), (2.5, 0.7)],
            }
            nodes = {"a": [0.2, 0.2], "b": [0.8, 0.2],
                     "c": [0.2, 0.3], "d": [0.8, 0.3],
                     "e": [1.5, 0.7], "f": [2.5, 0.7]}
            ends = {"inside": ("a", "b"), "bend": ("c", "d"), "cross": ("e", "f")}
            write_pack(root / "pack", pack_id="rail-test", version="1", network_type="rail",
                       bbox=[0, 0, 4, 2], capabilities=["inspect"], source={"name": "fixture"},
                       nodes=nodes,
                       links=[{"id": key, "feature_id": key, "from": ends[key][0],
                               "to": ends[key][1], "directed": False} for key in lines],
                       features=[{"type": "Feature", "geometry": mapping(LineString(points)),
                                  "properties": {"feature_id": key}} for key, points in lines.items()])
            report = assign(Pack.open(root / "pack"), archive, "USA-CA", root / "output")
            placements = {row["feature_id"]: row for row in
                          pq.read_table(root / "output" / "feature_loc_ids.parquet").to_pylist()}
            self.assertEqual(placements["inside"]["containing_loc_id"], "USA-CA-001-A")
            self.assertEqual(placements["bend"]["node_candidate_loc_id"], "USA-CA-001-A")
            self.assertEqual(placements["bend"]["containing_loc_id"], "USA-CA-001")
            self.assertEqual(placements["cross"]["containing_loc_id"], "USA-CA")
            self.assertEqual(report["node_candidate_promoted_after_line_check"], 1)
            assign(Pack.open(root / "pack"), archive, "USA-CA", root / "unprepared",
                   prepare_geometries=False)
            self.assertEqual(
                pq.read_table(root / "output" / "feature_loc_ids.parquet").to_pylist(),
                pq.read_table(root / "unprepared" / "feature_loc_ids.parquet").to_pylist(),
            )

            write_pack(root / "river", pack_id="river-test", version="1", network_type="river",
                       bbox=[0, 0, 4, 2], capabilities=["inspect"], source={"name": "fixture"},
                       nodes={"reach-a": [0.5, 0.5], "reach-b": [0.6, 0.5]},
                       links=[{"id": "reach-a", "feature_id": "reach-a", "from": "reach-a",
                               "to": "reach-b", "directed": True}],
                       features=[{"type": "Feature",
                                  "geometry": mapping(LineString([(0.5, 0.5), (2.5, 0.5)])),
                                  "properties": {"feature_id": "reach-a"}}])
            river_report = assign(Pack.open(root / "river"), archive, "USA-CA", root / "river_output")
            river_row = pq.read_table(root / "river_output" / "feature_loc_ids.parquet").to_pylist()[0]
            self.assertEqual(river_row["containing_loc_id"], "USA-CA")
            self.assertIsNone(river_row["node_candidate_loc_id"])
            self.assertEqual(river_report["node_candidate_method"], "not_applicable_reach_nodes")


if __name__ == "__main__":
    unittest.main()
