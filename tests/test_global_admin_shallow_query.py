import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from shapely.geometry import box

from mapmover.runtime.global_admin_shallow_query import resolve_global_admin_shallow_points


class GlobalAdminShallowQueryTests(unittest.TestCase):
    def test_one_global_pack_resolves_and_translates_bulk_points(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            boxes = [
                ("USA", 0, None, "United States", box(0, 0, 10, 10)),
                ("USA-GTX", 1, "USA", "Texas", box(0, 0, 10, 10)),
                ("USA-GTX-GHARRIS", 2, "USA-GTX", "Harris", box(1, 1, 3, 3)),
            ]
            pd.DataFrame([
                {"loc_id": loc_id, "admin_level": level,
                 "bbox_min_lon": shape.bounds[0], "bbox_min_lat": shape.bounds[1],
                 "bbox_max_lon": shape.bounds[2], "bbox_max_lat": shape.bounds[3]}
                for loc_id, level, _parent, _name, shape in boxes
            ]).to_parquet(root / "bbox.parquet", index=False)
            for level in range(3):
                pd.DataFrame([
                    {"loc_id": loc_id, "admin_level": level, "parent_id": parent,
                     "name": name, "geometry_wkb": shape.wkb}
                    for loc_id, row_level, parent, name, shape in boxes if row_level == level
                ]).to_parquet(root / f"admin_{level}.parquet", index=False)
            pd.DataFrame([
                {"baseline_loc_id": source, "preferred_loc_id": preferred}
                for source, preferred in (("USA", "USA"), ("USA-GTX", "USA-TX"),
                                          ("USA-GTX-GHARRIS", "USA-TX-201"))
            ]).to_parquet(root / "lookup.parquet", index=False)
            layout = {"bbox_index": root / "bbox.parquet",
                      "preferred_lookup": root / "lookup.parquet",
                      "release": "1.1.1",
                      **{f"admin_{level}": root / f"admin_{level}.parquet" for level in range(3)}}
            with patch("mapmover.runtime.global_admin_shallow_query._selected_layout", return_value=layout):
                results = resolve_global_admin_shallow_points([
                    {"lon": 2, "lat": 2}, {"lon": 5, "lat": 5}, {"lon": 20, "lat": 20},
                ])
        self.assertEqual(results[0]["deepest_resolved_loc_id"], "USA-TX-201")
        self.assertEqual([row["loc_id"] for row in results[0]["stack"]],
                         ["USA", "USA-TX", "USA-TX-201"])
        self.assertEqual(results[0]["matched"]["baseline_loc_id"], "USA-GTX-GHARRIS")
        self.assertEqual(results[1]["deepest_resolved_loc_id"], "USA-TX")
        self.assertEqual(results[2]["error"]["code"], "no_point_overlap")

    def test_unselected_layout_keeps_existing_admin0_path_available(self):
        with patch("mapmover.runtime.global_admin_shallow_query._selected_layout", return_value=None):
            self.assertIsNone(resolve_global_admin_shallow_points([{"lon": 0, "lat": 0}]))


if __name__ == "__main__":
    unittest.main()
