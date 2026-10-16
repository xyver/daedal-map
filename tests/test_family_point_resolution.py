import unittest
import threading
from unittest.mock import patch

import pandas as pd
from shapely.geometry import Polygon, box, mapping

from mapmover.runtime.family_point_resolution import (
    _representation_manifest_cached,
    family_artifacts,
    normalize_requested_family,
    resolve_family_points,
    shallow_scope,
)


class FamilyPointResolutionTests(unittest.TestCase):
    def test_cloud_family_lookup_does_not_probe_unpublished_download_manifest(self):
        with (
            patch("mapmover.runtime.family_point_resolution.get_data_plane_mode", return_value="cloud"),
            patch("mapmover.runtime.family_point_resolution.read_artifact_json") as read,
        ):
            _representation_manifest_cached.cache_clear()
            self.assertEqual(_representation_manifest_cached("CAN", 1), {})
            read.assert_not_called()
            _representation_manifest_cached.cache_clear()

    def test_cancelled_predicate_route_stops_before_next_file_or_shapes(self):
        stop_event = threading.Event()

        def first_read(*args, **kwargs):
            stop_event.set()
            return pd.DataFrame(columns=["point_position", "loc_id"])

        with (
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
            patch("mapmover.runtime.family_point_resolution.country_capability_record",
                  return_value={"available_family_ids": ["watershed"]}),
            patch("mapmover.runtime.family_point_resolution.family_artifacts", return_value={
                "watershed": {"predicate_paths": ["first.parquet", "second.parquet"],
                              "exact_paths": ["shapes.parquet"]},
            }),
            patch("mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points",
                  side_effect=first_read) as read,
            patch("mapmover.runtime.family_point_resolution.load_reference_graph_geometry") as shapes,
        ):
            resolve_family_points("USA", ["watershed"], [{"lon": 1, "lat": 1}],
                                  stop_event=stop_event)
        read.assert_called_once()
        shapes.assert_not_called()

    def test_shallow_scope_uses_one_admin_route_index(self):
        rows = pd.DataFrame([{"loc_id": "USA-TX-201", "admin_level": 2,
                              "admin_1_loc_id": "USA-TX"}])
        with (
            patch("mapmover.runtime.family_point_resolution.layout_available", return_value=True),
            patch("mapmover.runtime.family_point_resolution.load_route_rows_by_loc_ids",
                  return_value=rows) as route,
        ):
            self.assertEqual(shallow_scope("USA-TX-201"), ("USA", "USA-TX", None))
        route.assert_called_once_with("USA", ["USA-TX-201"])

    def test_manifest_source_family_names_route_to_canonical_catalog_families(self):
        manifest = {"artifacts": [
            {"family": "named_geographical_feature", "role": "query_exact_predicate_index", "path": "named.index.parquet", "rows": 2},
            {"family": "named_geographical_feature", "role": "query_exact_geometry", "path": "named.parquet"},
            {"family": "map_grid", "role": "query_exact_predicate_index", "path": "grid.index.parquet", "rows": 3},
            {"family": "map_grid", "role": "query_exact_geometry", "path": "grid.parquet"},
        ]}
        with (
            patch("mapmover.runtime.family_point_resolution.representation_manifest", return_value=manifest),
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
        ):
            artifacts = family_artifacts("CAN")

        self.assertNotIn("named_geographical_feature", artifacts)
        self.assertNotIn("map_grid", artifacts)
        self.assertEqual(artifacts["named_feature"]["shape_count"], 2)
        self.assertEqual(artifacts["reference_grid"]["shape_count"], 3)

    def test_published_catalog_banks_fallback_when_download_manifest_is_absent(self):
        catalog = {"geometry_banks": [{
            "scope": "CAN",
            "family": "postal_area",
            "geometry_path": "geometry/countries/CAN/relationships/postal/shapes/postal_area.parquet",
            "bank_role": "graph_ready_sidechain",
            "material_policy": {"hosted_access": {"publication_ready": True, "paid_allowed": True}},
            "row_count": 10,
        }]}
        with (
            patch("mapmover.runtime.family_point_resolution.representation_manifest", return_value={}),
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value=catalog),
        ):
            artifacts = family_artifacts("CAN")

        self.assertEqual(artifacts["postal_area"]["predicate_paths"], [catalog["geometry_banks"][0]["geometry_path"]])
        self.assertEqual(artifacts["postal_area"]["exact_paths"], [catalog["geometry_banks"][0]["geometry_path"]])

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

    def test_bbox_candidates_without_exact_shapes_fail_closed(self):
        candidates = pd.DataFrame([{"point_position": 0, "loc_id": "USA-POSTAL-1"}])
        with (
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value={}),
            patch("mapmover.runtime.family_point_resolution.country_capability_record",
                  return_value={"available_family_ids": ["postal_area"]}),
            patch("mapmover.runtime.family_point_resolution.family_artifacts", return_value={
                "postal_area": {"predicate_paths": ["predicate.parquet"],
                                "exact_paths": ["shapes.parquet"]},
            }),
            patch("mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points",
                  return_value=candidates),
            patch("mapmover.runtime.family_point_resolution.load_reference_graph_geometry",
                  return_value=pd.DataFrame()),
        ):
            result = resolve_family_points(
                "USA", ["postal_area"], [{"lon": 1, "lat": 1}],
            )[0]["postal_area"]
        self.assertEqual(result["status"], "family_lookup_error")
        self.assertEqual(result["error"]["code"], "family_point_exact_shapes_missing")

    def test_declared_point_index_routes_shapes_without_graph_discovery(self):
        catalog = {
            "runtime_query_layouts": {"family_points": {"USA": {"watershed": {
                "country": "USA", "family": "watershed",
                "source_release_id": "usa_test", "index_path": "point_index.parquet",
            }}}},
            "country_profiles": [{"country_code": "USA", "active_release": {"release_id": "usa_test"}}],
        }
        candidates = pd.DataFrame([{
            "point_position": 0, "loc_id": "USA-WBD-HUC2-12",
            "shape_path": "geometry/countries/USA/shapes/shallow.parquet",
            "geometry_loc_id": "USA-WBD-HUC2-12", "family": "watershed",
        }])
        shapes = pd.DataFrame([{
            "loc_id": "USA-WBD-HUC2-12", "name": "Texas-Gulf Region",
            "family": "watershed", "geometry": mapping(box(0, 0, 2, 2)),
        }])
        with (
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value=catalog),
            patch("mapmover.runtime.family_point_resolution.country_capability_record",
                  return_value={"available_family_ids": ["watershed"]}),
            patch("mapmover.runtime.family_point_resolution.family_artifacts", return_value={}),
            patch("mapmover.runtime.family_point_resolution.parquet_available", return_value=True),
            patch("mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points",
                  return_value=candidates),
            patch("mapmover.runtime.family_point_resolution.load_routed_family_geometry",
                  return_value=shapes) as routed,
            patch("mapmover.runtime.family_point_resolution.load_reference_graph_geometry") as graph,
        ):
            result = resolve_family_points("USA", ["watershed"], [{"lon": 1, "lat": 1}])[0]["watershed"]
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["matches"][0]["loc_id"], "USA-WBD-HUC2-12")
        routed.assert_called_once()
        graph.assert_not_called()

    def test_declared_point_index_missing_file_fails_closed(self):
        catalog = {
            "runtime_query_layouts": {"family_points": {"USA": {"watershed": {
                "country": "USA", "family": "watershed",
                "source_release_id": "usa_test", "index_path": "missing.parquet",
            }}}},
            "country_profiles": [{"country_code": "USA", "active_release": {"release_id": "usa_test"}}],
        }
        with (
            patch("mapmover.runtime.family_point_resolution.load_geometry_catalog", return_value=catalog),
            patch("mapmover.runtime.family_point_resolution.country_capability_record",
                  return_value={"available_family_ids": ["watershed"]}),
            patch("mapmover.runtime.family_point_resolution.family_artifacts", return_value={}),
            patch("mapmover.runtime.family_point_resolution.parquet_available", return_value=False),
            patch("mapmover.runtime.family_point_resolution.read_bbox_candidates_for_points") as read,
            patch("mapmover.runtime.family_point_resolution.load_reference_graph_geometry") as graph,
        ):
            result = resolve_family_points("USA", ["watershed"], [{"lon": 1, "lat": 1}])[0]["watershed"]
        self.assertEqual(result["status"], "family_lookup_error")
        read.assert_not_called()
        graph.assert_not_called()

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
