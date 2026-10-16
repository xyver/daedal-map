from __future__ import annotations

import unittest
from unittest import mock

from mapmover.runtime.geography_relationships import (
    compare_geographies,
    compare_geographies_batch,
    historical_entity_info,
    resolve_historical_country_reference,
)


def _resolution(loc_id: str) -> dict:
    return {"ok": True, "requested_loc_id": loc_id, "loc_id": loc_id,
            "resolved_from_public_alias": False}


def _identity(loc_id: str, _when) -> dict:
    family = {"USA": "admin_0", "USA-TX": "admin_1"}.get(loc_id, "test_sidechain")
    return {"loc_id": loc_id, "family": family, "valid_at_requested_time": None,
            "direct_successors": [], "present_day_descendants": []}


class GeographyRelationshipRuntimeTests(unittest.TestCase):
    def test_yugoslavia_2000_selects_valid_federal_republic(self) -> None:
        result = resolve_historical_country_reference("YUG", as_of="2000")
        self.assertTrue(result["ok"])
        self.assertEqual(result["resolved_loc_id"], "HIST-YUG-FRY")
        self.assertTrue(result["valid_at_requested_time"])
        self.assertEqual(result["lifecycle"]["direct_successors"][0]["loc_id"], "HIST-SCG")

    def test_yugoslavia_2025_is_expired_and_has_many_terminal_descendants(self) -> None:
        result = resolve_historical_country_reference("Yugoslavia", as_of="2025-01-01")
        self.assertTrue(result["ok"])
        self.assertFalse(result["valid_at_requested_time"])
        self.assertEqual(result["lifecycle"]["successor_cardinality"], "one")
        self.assertEqual({row["loc_id"] for row in result["lifecycle"]["present_day_descendants"]}, {"SRB", "MNE"})

    def test_sfr_yugoslavia_has_five_equal_successors_and_six_current_descendants(self) -> None:
        result = historical_entity_info("HIST-YUG-SFRY", as_of="1990")
        self.assertTrue(result["valid_at_requested_time"])
        self.assertEqual(len(result["direct_successors"]), 5)
        self.assertEqual({row["loc_id"] for row in result["present_day_descendants"]},
                         {"BIH", "HRV", "SVN", "MKD", "SRB", "MNE"})

    def test_admin_spine_containment_never_queries_pair_graph(self) -> None:
        graph_fetcher = mock.Mock(side_effect=AssertionError("pair graph should not be read"))
        result = compare_geographies(
            "USA-TX", "USA", graph_fetcher=graph_fetcher,
            resolution_fetcher=_resolution, identity_fetcher=_identity,
        )
        self.assertEqual(result["spatial_relation"], "within")
        self.assertEqual(result["common_ancestor_loc_id"], "USA")
        self.assertFalse(result["geometry_loaded"])
        graph_fetcher.assert_not_called()

    def test_direct_graph_overlap_keeps_recorded_evidence(self) -> None:
        edge = {"source_loc_id": "USA-Z-00601", "target_loc_id": "USA-PR-001",
                "relationship_type": "crosswalk_overlap", "match_share": 0.97,
                "relationship_vintage": "2020"}
        result = compare_geographies(
            "USA-Z-00601", "USA-PR-001",
            resolution_fetcher=_resolution, identity_fetcher=_identity,
            graph_fetcher=lambda _left, _right: [edge],
        )
        self.assertEqual(result["spatial_relation"], "overlaps")
        self.assertEqual(result["relationship_basis"], "selected_reference_graph")
        self.assertEqual(result["graph_evidence"][0]["match_share"], 0.97)
        self.assertFalse(result["geometry_loaded"])

    def test_untyped_graph_edge_does_not_claim_overlap(self) -> None:
        edge = {"source_loc_id": "WATERSHED", "target_loc_id": "DISTRICT",
                "relationship_type": None, "source_area_share": 0.5}
        result = compare_geographies(
            "WATERSHED", "DISTRICT", resolution_fetcher=_resolution,
            identity_fetcher=_identity, graph_fetcher=lambda _left, _right: [edge],
        )
        self.assertEqual(result["spatial_relation"], "unknown")
        self.assertIsNone(result["intersects"])
        self.assertEqual(result["graph_evidence"][0]["source_area_share"], 0.5)

    def test_side_family_loc_id_prefix_is_not_admin_containment(self) -> None:
        edge = {"source_loc_id": "CAN-BC-5915-020-HEALTH-X",
                "target_loc_id": "CAN-BC-5915", "relationship_type": None}
        def identity(loc_id: str, _when) -> dict:
            return {"loc_id": loc_id,
                    "family": "admin_3" if loc_id == "CAN-BC-5915" else "can_provincial_health_region",
                    "valid_at_requested_time": None}
        result = compare_geographies(
            "CAN-BC-5915", "CAN-BC-5915-020-HEALTH-X",
            resolution_fetcher=_resolution, identity_fetcher=identity,
            graph_fetcher=lambda _left, _right: [edge],
        )
        self.assertEqual(result["spatial_relation"], "unknown")
        self.assertEqual(result["relationship_basis"], "selected_reference_graph")

    def test_missing_edge_does_not_claim_no_overlap(self) -> None:
        result = compare_geographies(
            "WATERSHED", "DISTRICT", resolution_fetcher=_resolution,
            identity_fetcher=_identity, graph_fetcher=lambda _left, _right: [],
        )
        self.assertEqual(result["spatial_relation"], "unknown")
        self.assertIn("does not prove no overlap", result["reason"])
        self.assertFalse(result["geometry_loaded"])

    def test_explicit_negative_edge_proves_no_overlap(self) -> None:
        for kind in ("disjoint", "no_overlap"):
            with self.subTest(kind=kind):
                edge = {"source_loc_id": "LEFT", "target_loc_id": "RIGHT",
                        "relationship_type": kind}
                result = compare_geographies(
                    "LEFT", "RIGHT", resolution_fetcher=_resolution,
                    identity_fetcher=_identity, graph_fetcher=lambda _left, _right: [edge],
                )
                self.assertEqual(result["spatial_relation"], "no_overlap")
                self.assertIs(result["intersects"], False)
                self.assertEqual(result["graph_evidence"][0]["relationship_type"], kind)

    def test_conflicting_positive_and_negative_edges_are_unknown(self) -> None:
        edges = [{"source_loc_id": "LEFT", "target_loc_id": "RIGHT",
                  "relationship_type": kind} for kind in ("no_overlap", "overlaps")]
        result = compare_geographies(
            "LEFT", "RIGHT", resolution_fetcher=_resolution,
            identity_fetcher=_identity, graph_fetcher=lambda _left, _right: edges,
        )
        self.assertEqual(result["spatial_relation"], "unknown")
        self.assertIsNone(result["intersects"])

    def test_dated_pair_requires_edge_validity_evidence(self) -> None:
        edge = {"source_loc_id": "LEFT", "target_loc_id": "RIGHT",
                "relationship_type": "overlaps", "valid_from": "2020-01-01",
                "valid_to": "2024-01-01"}
        options = {"resolution_fetcher": _resolution, "identity_fetcher": _identity,
                   "graph_fetcher": lambda _left, _right: [edge]}
        self.assertEqual(compare_geographies("LEFT", "RIGHT", as_of="2022", **options)["spatial_relation"], "overlaps")
        self.assertEqual(compare_geographies("LEFT", "RIGHT", as_of="2025", **options)["spatial_relation"], "unknown")
        self.assertEqual(compare_geographies(
            "LEFT", "RIGHT", left_as_of="2022", right_as_of="2023", **options
        )["spatial_relation"], "not_evaluated")

    def test_batch_reuses_one_pair_graph_read_in_both_directions(self) -> None:
        edge = {"source_loc_id": "LEFT", "target_loc_id": "RIGHT",
                "relationship_type": "overlaps"}
        with (
            mock.patch("mapmover.runtime.reference_exchange.resolve_loc_id_input", side_effect=_resolution),
            mock.patch("mapmover.runtime.reference_graph.relationships_between_loc_ids", return_value=[edge]) as graph_mock,
            mock.patch("mapmover.runtime.geography_relationships._identity_state", side_effect=_identity),
            mock.patch("mapmover.runtime.reference_exchange.get_geometry_reference", side_effect=AssertionError("shape read")) as geometry_mock,
        ):
            results = compare_geographies_batch([
                {"left_loc_id": "LEFT", "right_loc_id": "RIGHT"},
                {"left_loc_id": "RIGHT", "right_loc_id": "LEFT"},
            ])
        self.assertEqual([result["spatial_relation"] for result in results], ["overlaps", "overlaps"])
        self.assertEqual(graph_mock.call_count, 1)
        geometry_mock.assert_not_called()

    def test_invalid_historical_identity_skips_spatial_graph(self) -> None:
        graph_fetcher = mock.Mock(side_effect=AssertionError("pair graph should not be read"))
        result = compare_geographies(
            "HIST-YUG-FRY", "SRB", as_of="2025", graph_fetcher=graph_fetcher,
        )
        self.assertEqual(result["temporal_relation"], "one_or_more_not_valid")
        self.assertEqual(result["spatial_relation"], "not_evaluated")
        graph_fetcher.assert_not_called()

    def test_reference_graph_validity_controls_temporal_comparison(self) -> None:
        rows = {
            "OLD": {"loc_id": "OLD", "family": "test", "valid_from": "1996-01-01", "valid_to": "2004-01-01"},
            "NEW": {"loc_id": "NEW", "family": "test", "valid_from": "2023-01-01", "valid_to": ""},
        }
        with mock.patch("mapmover.runtime.reference_graph.identity_at", side_effect=lambda loc_id, _when: rows.get(loc_id)):
            result = compare_geographies("OLD", "NEW", as_of="2025",
                                         resolution_fetcher=_resolution,
                                         graph_fetcher=lambda _left, _right: [])
        self.assertEqual(result["temporal_relation"], "one_or_more_not_valid")
        self.assertFalse(result["left"]["valid_at_requested_time"])
        self.assertTrue(result["right"]["valid_at_requested_time"])
        self.assertEqual(result["spatial_relation"], "not_evaluated")


if __name__ == "__main__":
    unittest.main()
