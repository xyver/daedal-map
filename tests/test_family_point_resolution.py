import unittest
from unittest.mock import patch

import pandas as pd
from shapely.geometry import Polygon, mapping

from mapmover.runtime.family_point_resolution import (
    normalize_requested_family,
    resolve_family_points,
)


class FamilyPointResolutionTests(unittest.TestCase):
    def test_family_input_requires_canonical_ids(self):
        family, error = normalize_requested_family("postal_area")
        self.assertEqual(family, "postal_area")
        self.assertIsNone(error)

        _family, error = normalize_requested_family("Postal")
        self.assertEqual(error["code"], "invalid_family_id")

    def test_bbox_candidates_are_exactly_checked(self):
        predicate = pd.DataFrame([
            {"point_position": 0, "loc_id": "USA-POSTAL-1"},
            {"point_position": 1, "loc_id": "USA-POSTAL-1"},
        ])
        shape_rows = pd.DataFrame([{
            "loc_id": "USA-POSTAL-1",
            "name": "Test postal area",
            "family": "postal_area",
            "geometry": mapping(Polygon([(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)])),
        }])
        with (
            patch(
                "mapmover.runtime.family_point_resolution.country_capability_record",
                return_value={"available_family_ids": ["postal_area"]},
            ),
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
            patch(
                "mapmover.runtime.family_point_resolution.family_artifacts",
                return_value={
                    "postal_area": {
                        "predicate_paths": ["predicate.parquet"],
                        "exact_paths": ["shapes.parquet"],
                    }
                },
            ),
            patch(
                "mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points",
                return_value=predicate,
            ),
            patch(
                "mapmover.runtime.family_point_resolution.load_reference_graph_geometry",
                return_value=shape_rows,
            ),
        ):
            results = resolve_family_points(
                "USA", ["postal_area"],
                [{"lon": 1, "lat": 1}, {"lon": 3, "lat": 3}],
            )

        self.assertEqual(results[0]["postal_area"]["status"], "matched")
        self.assertEqual(results[0]["postal_area"]["matches"][0]["loc_id"], "USA-POSTAL-1")
        self.assertEqual(results[1]["postal_area"]["status"], "no_point_overlap")
        self.assertEqual(results[1]["postal_area"]["matches"], [])

    def test_zero_bbox_candidates_do_not_open_shape_bank(self):
        with (
            patch(
                "mapmover.runtime.family_point_resolution.country_capability_record",
                return_value={"available_family_ids": ["land_management_region"]},
            ),
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
            patch(
                "mapmover.runtime.family_point_resolution.family_artifacts",
                return_value={
                    "land_management_region": {
                        "predicate_paths": ["predicate.parquet"],
                        "exact_paths": ["shapes.parquet"],
                    }
                },
            ),
            patch(
                "mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points",
                return_value=pd.DataFrame(columns=["point_position", "loc_id"]),
            ),
            patch("mapmover.runtime.family_point_resolution.load_reference_graph_geometry") as loader,
        ):
            result = resolve_family_points(
                "USA", ["land_management_region"], [{"lon": -74, "lat": 40}],
            )[0]["land_management_region"]

        self.assertEqual(result["status"], "no_point_overlap")
        self.assertEqual(result["candidate_count"], 0)
        loader.assert_not_called()

    def test_unavailable_family_is_distinct_from_no_overlap(self):
        with (
            patch(
                "mapmover.runtime.family_point_resolution.country_capability_record",
                return_value={"available_family_ids": ["postal_area"]},
            ),
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
            patch("mapmover.runtime.family_point_resolution.family_artifacts", return_value={}),
        ):
            result = resolve_family_points(
                "USA", ["forest"], [{"lon": -74, "lat": 40}],
            )[0]["forest"]

        self.assertEqual(result["status"], "family_not_available")
        self.assertEqual(result["matches"], [])


if __name__ == "__main__":
    unittest.main()
