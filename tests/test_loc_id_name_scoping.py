"""Top-down name resolution: parents, ambiguity, accents, and places."""

import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

from mapmover.runtime import graph_name_lookup, loc_id_resolution
from mapmover.runtime.loc_id_resolution import resolve_admin_text_to_loc_id

SPINE = pd.DataFrame([
    {"loc_id": "USA-OR", "admin_level": 1, "name": "Oregon"},
    {"loc_id": "USA-NC", "admin_level": 1, "name": "North Carolina"},
    {"loc_id": "USA-LA", "admin_level": 1, "name": "Louisiana"},
    {"loc_id": "USA-IL", "admin_level": 1, "name": "Illinois"},
    {"loc_id": "USA-OR-067", "admin_level": 2, "name": "Washington County"},
    {"loc_id": "USA-NC-189", "admin_level": 2, "name": "Washington County"},
    {"loc_id": "USA-MO-149", "admin_level": 2, "name": "Oregon County"},
    {"loc_id": "USA-NC-021", "admin_level": 2, "name": "Buncombe County"},
    {"loc_id": "USA-LA-071", "admin_level": 2, "name": "Orleans Parish"},
    {"loc_id": "USA-NY-073", "admin_level": 2, "name": "Orleans County"},
    {"loc_id": "USA-NC-021-001100", "admin_level": 3, "name": "K\u00f6ln Tract"},
])
PLACES = [
    {"loc_id": "USA-IL-167-PLACE-1772000", "name": "Springfield", "family": "place_or_municipality", "anchor_loc_id": "USA-IL-167"},
    {"loc_id": "USA-MO-077-PLACE-2970000", "name": "Springfield", "family": "place_or_municipality", "anchor_loc_id": "USA-MO-077"},
]


def _spine_table():
    frame = SPINE  # noqa: F841 - read by the DuckDB replacement scan below
    keys = graph_name_lookup._key_select("name")
    return duckdb.sql(
        f"""SELECT loc_id, admin_level, name, {keys},
                   CASE WHEN admin_level = 1 THEN lower(split_part(loc_id, '-', 2)) END AS k_code
            FROM frame"""
    ).arrow()


def _places(country, query, parent_loc_id=None):
    if query.strip().lower() != "springfield":
        return {"status": "not_found"}
    pool = [
        place for place in PLACES
        if graph_name_lookup._in_scope(place["loc_id"], parent_loc_id, place["anchor_loc_id"])
    ]
    if len(pool) == 1:
        return {"status": "matched", "match": pool[0]}
    return {"status": "ambiguous", "candidates": pool} if pool else {"status": "not_found"}


class NameScopingTests(unittest.TestCase):
    def setUp(self):
        table = _spine_table()
        if not hasattr(table, "column"):
            table = table.read_all()
        patches = [
            patch.object(graph_name_lookup, "spine_name_table", side_effect=lambda country: (table, 3, frozenset()) if country == "USA" else None),
            patch.object(loc_id_resolution, "place_candidates", side_effect=_places),
            patch.object(loc_id_resolution, "_load_country_direct_location_aliases", return_value={}),
            patch.object(loc_id_resolution, "load_conversions", return_value={}),
            patch.object(loc_id_resolution, "_resolve_country_fallback_location_alias", return_value=None),
        ]
        standardizer = patch.object(loc_id_resolution, "_get_name_standardizer")
        get_standardizer = standardizer.start()
        get_standardizer.return_value.get_loc_id_from_name.return_value = None
        get_standardizer.return_value.get_country_code.return_value = None
        self.standardizer = get_standardizer.return_value
        self.addCleanup(standardizer.stop)
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    def resolve(self, text, **kwargs):
        return resolve_admin_text_to_loc_id(text, country_hint=kwargs.pop("country_hint", "USA"), **kwargs)

    def test_bare_name_resolves_top_down(self):
        # The state wins over Oregon County, Missouri.
        self.assertEqual(self.resolve("Oregon")["deepest_resolved_loc_id"], "USA-OR")

    def test_admin1_code_matches(self):
        self.assertEqual(self.resolve("OR", admin_level_hint=1)["deepest_resolved_loc_id"], "USA-OR")

    def test_name_with_parent_text(self):
        for text in ("Washington County, Oregon", "Washington County, OR"):
            resolved = self.resolve(text)
            self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-OR-067", text)
            self.assertEqual(resolved["parent_loc_id"], "USA-OR")

    def test_subdivision_country_hint_scopes_the_name(self):
        resolved = self.resolve("Washington County", country_hint="USA-NC", admin_level_hint=2)
        self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-NC-189")

    def test_ambiguous_name_returns_candidates(self):
        resolved = self.resolve("Washington County", admin_level_hint=2)
        self.assertIsNone(resolved["deepest_resolved_loc_id"])
        self.assertEqual({c["loc_id"] for c in resolved["candidates"]}, {"USA-OR-067", "USA-NC-189"})
        self.assertIn("ambiguous", resolved["error"])
        # Graph ambiguity is authoritative: the legacy fallback never guesses.
        self.standardizer.get_loc_id_from_name.assert_not_called()

    def test_unstripped_suffix_breaks_the_tie(self):
        self.assertEqual(self.resolve("Orleans Parish", admin_level_hint=2)["deepest_resolved_loc_id"], "USA-LA-071")

    def test_accent_and_transliteration_match(self):
        for text in ("K\u00f6ln Tract", "Koln Tract", "Koeln Tract", "KOELN TRACT"):
            self.assertEqual(self.resolve(text, admin_level_hint=3)["deepest_resolved_loc_id"], "USA-NC-021-001100", text)

    def test_populated_place_within_parent(self):
        for text in ("Springfield, Illinois", "Springfield, IL"):
            resolved = self.resolve(text)
            self.assertEqual(resolved["deepest_resolved_loc_id"], "USA-IL-167-PLACE-1772000", text)
            self.assertEqual(resolved["match_type"], "populated_place")

    def test_ambiguous_populated_place_returns_candidates(self):
        resolved = self.resolve("Springfield")
        self.assertIsNone(resolved["deepest_resolved_loc_id"])
        self.assertEqual(resolved["candidate_count"], 2)


class NameKeyTests(unittest.TestCase):
    def test_keys_fold_accents_and_transliterate_umlauts(self):
        keys = graph_name_lookup.name_keys("K\u00f6ln")
        self.assertEqual(set(keys["exact"]), {"koln", "koeln"})
        self.assertEqual(graph_name_lookup.name_keys("S\u00e3o Paulo")["exact"], ("sao paulo",))
        self.assertEqual(graph_name_lookup.name_keys("Troms\u00f8")["exact"], ("tromso",))

    def test_stripped_key_drops_admin_suffixes(self):
        keys = graph_name_lookup.name_keys("Washington County")
        self.assertEqual(keys["exact"], ("washington county",))
        self.assertEqual(keys["stripped"], ("washington",))


if __name__ == "__main__":
    unittest.main()
