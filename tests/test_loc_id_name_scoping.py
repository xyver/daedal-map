"""Top-down admin-name resolution: parents, ambiguity, and populated places."""

import unittest
from unittest.mock import patch

import pandas as pd

from mapmover.runtime import loc_id_resolution
from mapmover.runtime.loc_id_resolution import resolve_admin_text_to_loc_id

STATES = pd.DataFrame([
    {"loc_id": "USA-OR", "name": "Oregon", "name_local": None, "code": "OR"},
    {"loc_id": "USA-NC", "name": "North Carolina", "name_local": None, "code": "NC"},
    {"loc_id": "USA-LA", "name": "Louisiana", "name_local": None, "code": "LA"},
    {"loc_id": "USA-IL", "name": "Illinois", "name_local": None, "code": "IL"},
])
COUNTIES = pd.DataFrame([
    {"loc_id": "USA-OR-067", "name": "Washington County", "name_local": None, "code": "067"},
    {"loc_id": "USA-NC-189", "name": "Washington County", "name_local": None, "code": "189"},
    {"loc_id": "USA-MO-149", "name": "Oregon County", "name_local": None, "code": "149"},
    {"loc_id": "USA-NC-021", "name": "Buncombe County", "name_local": None, "code": "021"},
    {"loc_id": "USA-LA-071", "name": "Orleans Parish", "name_local": None, "code": "071"},
    {"loc_id": "USA-NY-073", "name": "Orleans County", "name_local": None, "code": "073"},
])
PLACES = [
    {"loc_id": "USA-IL-167-PLACE-1772000", "display_name": "Springfield", "region_label": "IL", "subtype_rank": 0, "family": "place_or_municipality"},
    {"loc_id": "USA-MO-077-PLACE-2970000", "display_name": "Springfield", "region_label": "MO", "subtype_rank": 0, "family": "place_or_municipality"},
]


def _levels(country_hint, admin_level=None):
    if country_hint != "USA":
        return pd.DataFrame()
    return {1: STATES, 2: COUNTIES}.get(admin_level, pd.DataFrame())


class NameScopingTests(unittest.TestCase):
    def setUp(self):
        patches = [
            patch.object(loc_id_resolution, "load_country_parquet", side_effect=_levels),
            patch.object(loc_id_resolution, "is_cloud_mode", return_value=False),
            patch.object(loc_id_resolution, "_load_country_direct_location_aliases", return_value={}),
            patch.object(loc_id_resolution, "load_conversions", return_value={}),
            patch.object(loc_id_resolution, "_resolve_country_fallback_location_alias", return_value=None),
            patch.object(
                loc_id_resolution,
                "resolve_populated_place",
                side_effect=lambda query, country_hint=None, limit=10: (
                    {"status": "ambiguous", "candidates": PLACES} if query.strip().lower() == "springfield" else None
                ),
            ),
        ]
        standardizer = patch.object(loc_id_resolution, "_get_name_standardizer")
        get_standardizer = standardizer.start()
        get_standardizer.return_value.get_loc_id_from_name.return_value = None
        get_standardizer.return_value.get_country_code.return_value = None
        self.addCleanup(standardizer.stop)
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        loc_id_resolution._REGION_LABEL_CACHE.clear()

    def test_bare_name_resolves_top_down(self):
        # The state wins over Oregon County, Missouri.
        self.assertEqual(resolve_admin_text_to_loc_id("Oregon", country_hint="USA")["deepest_resolved_loc_id"], "USA-OR")

    def test_name_with_parent_text(self):
        for text in ("Washington County, Oregon", "Washington County, OR"):
            resolved = resolve_admin_text_to_loc_id(text, country_hint="USA")
            self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-OR-067", text)
            self.assertEqual(resolved["parent_loc_id"], "USA-OR")

    def test_subdivision_country_hint_scopes_the_name(self):
        resolved = resolve_admin_text_to_loc_id("Washington County", country_hint="USA-NC", admin_level_hint=2)
        self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-NC-189")

    def test_ambiguous_name_returns_candidates(self):
        resolved = resolve_admin_text_to_loc_id("Washington County", country_hint="USA", admin_level_hint=2)
        self.assertIsNone(resolved["deepest_resolved_loc_id"])
        self.assertEqual({c["loc_id"] for c in resolved["candidates"]}, {"USA-OR-067", "USA-NC-189"})
        self.assertIn("ambiguous", resolved["error"])

    def test_unstripped_suffix_breaks_the_tie(self):
        resolved = resolve_admin_text_to_loc_id("Orleans Parish", country_hint="USA", admin_level_hint=2)
        self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-LA-071")

    def test_populated_place_within_parent(self):
        for text in ("Springfield, Illinois", "Springfield, IL"):
            resolved = resolve_admin_text_to_loc_id(text, country_hint="USA")
            self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-IL-167-PLACE-1772000", text)
            self.assertEqual(resolved["match_type"], "populated_place")

    def test_ambiguous_populated_place_returns_candidates(self):
        resolved = resolve_admin_text_to_loc_id("Springfield", country_hint="USA")
        self.assertIsNone(resolved["deepest_resolved_loc_id"])
        self.assertEqual(resolved["candidate_count"], 2)


if __name__ == "__main__":
    unittest.main()
