from __future__ import annotations

import json
import unittest
from unittest import mock

from mcp_discovery_shared import compact_catalog_payload, compact_pack_detail
from mapmover.runtime import reference_exchange
from mapmover.runtime.reference_exchange import (
    get_geometry_reference,
    list_reference_systems,
    read_geometry_catalog,
)


class MCPDiscoveryCompactionTests(unittest.TestCase):
    def test_catalog_keeps_selection_fields_and_links_instead_of_nested_policy(self) -> None:
        source = {
            "catalog_version": "1.0",
            "packs": [{
                "pack_id": "demo",
                "title": "Demo",
                "short_description": "Small description",
                "metric_count": 2,
                "material_policy": {"citation": {"entries": [{"text": "large" * 1000}]}},
                "upstream_sources": [{"notes": "large" * 1000}],
            }],
        }

        result = compact_catalog_payload(source)

        self.assertEqual(result["view"], "lite")
        self.assertEqual(result["packs"][0]["pack_id"], "demo")
        self.assertEqual(result["packs"][0]["next_call"]["tool"], "get_pack")
        self.assertNotIn("material_policy", result["packs"][0])
        self.assertNotIn("upstream_sources", result["packs"][0])
        self.assertLess(len(json.dumps(result)), len(json.dumps(source)) // 5)

    def test_pack_lite_retains_routing_and_points_to_full_detail(self) -> None:
        source = {
            "pack_id": "demo",
            "title": "Demo",
            "quick_start": {"first_query_template": {"tool": "query_dataset"}},
            "sources": [{"metadata": "large" * 1000}],
            "material_policy": {"citation": {"text": "large" * 1000}},
        }

        result = compact_pack_detail(source)

        self.assertEqual(result["view"], "lite")
        self.assertEqual(result["quick_start"], source["quick_start"])
        self.assertEqual(result["detail"]["tool_call"]["arguments"]["detail"], "full")
        self.assertNotIn("sources", result)
        self.assertNotIn("material_policy", result)

    def test_public_full_geometry_view_redirects_to_download(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={"schema_version": "1.1", "crosswalks": [{"crosswalk_id": "large"}]},
        ):
            result = read_geometry_catalog(view="full")

        self.assertTrue(result["ok"])
        self.assertEqual(result["view"], "full_redirect")
        self.assertIn("/downloadable/geometry/geometry_catalog.json", result["download_url"])
        self.assertNotIn("catalog", result)

    def test_reference_system_lite_view_does_not_materialize_crosswalk_records(self) -> None:
        catalog = {
            "reference_systems": [{
                "system": "postal_area",
                "label": "Postal Area",
                "callable": True,
                "publication_status": "published",
                "country_code": "CAN",
            }],
            "crosswalks": [{
                "crosswalk_id": "can_postal",
                "country_code": "CAN",
                "publication_status": "published",
                "callable": True,
            }],
            "crosswalk_artifacts": [],
            "geometry_families": [],
        }
        with (
            mock.patch.object(reference_exchange, "load_geometry_catalog", return_value=catalog),
            mock.patch.object(reference_exchange, "admitted_external_adapters", return_value=[]),
            mock.patch.object(reference_exchange, "_catalog_crosswalks") as full_crosswalks,
        ):
            result = list_reference_systems()

        full_crosswalks.assert_not_called()
        self.assertEqual(result["detail"], "lite")
        self.assertEqual(result["crosswalk_count"], 1)
        self.assertEqual(result["returned_crosswalk_count"], 0)
        self.assertEqual(result["crosswalks"], [])

    def test_geometry_detail_controls_attached_info_independently_of_polygon(self) -> None:
        with mock.patch.object(
            reference_exchange,
            "get_geometry_references",
            return_value={"results": [{"ok": True, "loc_id": "CAN-BC"}]},
        ) as fetch:
            get_geometry_reference("CAN-BC", include_polygon=False, include_info=True)

        fetch.assert_called_once_with(
            ["CAN-BC"], include_polygon=False, include_info=True,
        )


if __name__ == "__main__":
    unittest.main()
