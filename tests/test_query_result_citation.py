"""Query results carry attribution and citation copied from generated catalog projections."""

import unittest

from material_policy_shared import daedalmap_release_reference, query_result_citation


WORLD_FACTBOOK_SOURCE = {
    "source_id": "world_factbook",
    "pack_id": "world_factbook",
    "pack_version": "1.1.0",
    "attribution_text": "Source: Central Intelligence Agency - The World Factbook (U.S. Government work, public domain).",
    "attribution_lines": ["Source: Central Intelligence Agency - The World Factbook (U.S. Government work, public domain)."],
    "citation": {
        "style": "daedalmap-1",
        "entries": [
            {"text": "Central Intelligence Agency - The World Factbook. CIA World Factbook. Public Domain (U.S. Government). https://www.cia.gov/the-world-factbook/.",
             "role": "data",
             "parts": {"role": "data", "agency": "Central Intelligence Agency - The World Factbook", "title": "CIA World Factbook",
                       "vintage": "", "url": "https://www.cia.gov/the-world-factbook/", "license": "Public Domain (U.S. Government)"}},
            {"text": "Wikipedia - The World Factbook. CIA World Factbook. CC BY SA 4.0. https://en.wikipedia.org/wiki/The_World_Factbook.",
             "role": "reference",
             "parts": {"role": "reference", "agency": "Wikipedia - The World Factbook", "title": "CIA World Factbook",
                       "vintage": "", "url": "https://en.wikipedia.org/wiki/The_World_Factbook", "license": "CC BY SA 4.0"}},
        ],
    },
}
WORLD_FACTBOOK_PACK = {"pack_id": "world_factbook", "pack_name": "CIA World Factbook", "pack_version": "1.1.0"}


class QueryResultCitationTests(unittest.TestCase):
    def test_reference_role_is_preserved_and_not_an_attribution_obligation(self):
        citation = query_result_citation(WORLD_FACTBOOK_SOURCE, WORLD_FACTBOOK_PACK, accessed="2026-11-14")
        self.assertEqual(citation["attribution"], WORLD_FACTBOOK_SOURCE["attribution_lines"])
        self.assertNotIn("Wikipedia", " ".join(citation["attribution"]))
        roles = {entry["agency"]: entry["role"] for entry in citation["sources"]}
        self.assertEqual(roles, {
            "Central Intelligence Agency - The World Factbook": "data",
            "Wikipedia - The World Factbook": "reference",
        })
        self.assertEqual(citation["sources"][0]["url"], "https://www.cia.gov/the-world-factbook/")
        self.assertEqual(citation["sources"][0]["license"], "Public Domain (U.S. Government)")
        self.assertNotIn("vintage", citation["sources"][0])  # empty parts are omitted
        self.assertEqual(citation["accessed"], "2026-11-14")

    def test_references_match_the_live_pack_page_text(self):
        # Exact entries shown on https://www.daedalmap.com/packs/world_factbook (fetched 2026-11-14).
        citation = query_result_citation(WORLD_FACTBOOK_SOURCE, WORLD_FACTBOOK_PACK, accessed="2026-11-14")
        self.assertEqual(
            citation["daedalmap"]["text"],
            "DaedalMap (2026). CIA World Factbook data pack, version 1.1.0 [Data set]. "
            "https://www.daedalmap.com/packs/world_factbook. Accessed 2026-11-14.",
        )
        self.assertEqual([entry["text"] for entry in citation["sources"]], [
            "Central Intelligence Agency - The World Factbook. CIA World Factbook [Data set]. "
            "https://www.cia.gov/the-world-factbook/. Accessed 2026-11-14.",
            "Wikipedia - The World Factbook. CIA World Factbook [Data set]. "
            "https://en.wikipedia.org/wiki/The_World_Factbook. Accessed 2026-11-14.",
        ])
        self.assertNotIn("Public Domain", citation["sources"][0]["text"])  # licence is a separate field
        self.assertEqual(citation["daedalmap"]["url"], "https://www.daedalmap.com/packs/world_factbook")

    def test_identical_references_are_listed_once(self):
        entry = {"text": "World Bank. WDI. CC BY 4.0.", "role": "data",
                 "parts": {"agency": "World Bank", "title": "World Development Indicators", "url": "https://example.org/wdi"}}
        citation = query_result_citation({"citation": {"entries": [entry, dict(entry)]}, "pack_id": "wdi"})
        self.assertEqual(len(citation["sources"]), 1)

    def test_maintainer_name_comes_from_the_pack_record(self):
        citation = query_result_citation(WORLD_FACTBOOK_SOURCE, {**WORLD_FACTBOOK_PACK, "pack_maintainer_name": "Example Org"},
                                         accessed="2026-11-14")
        self.assertTrue(citation["daedalmap"]["text"].startswith("Example Org (2026)."))

    def test_missing_pack_record_falls_back_to_source_pack_fields(self):
        citation = query_result_citation(WORLD_FACTBOOK_SOURCE, None)
        self.assertEqual(citation["daedalmap"]["version"], "1.1.0")
        self.assertIsNone(citation["daedalmap"]["pack_name"])
        self.assertNotIn("accessed", citation)
        self.assertEqual(
            daedalmap_release_reference("world_factbook", "", "1.1.0"),
            "DaedalMap. world_factbook data pack, version 1.1.0 [Data set]. https://www.daedalmap.com/packs/world_factbook.",
        )

    def test_records_without_citation_or_attribution_add_nothing(self):
        self.assertIsNone(query_result_citation({"source_id": "bare"}))
        self.assertIsNone(query_result_citation(None))
        text_only = query_result_citation({"citation": {"text": "Agency. Title."}})
        self.assertEqual(text_only["sources"], [{"text": "Agency. Title."}])
        no_parts = query_result_citation({"citation": {"entries": [{"text": "Generated text.", "role": "data"}]}})
        self.assertEqual(no_parts["sources"], [{"text": "Generated text.", "role": "data"}])
        self.assertEqual(text_only["attribution"], [])


if __name__ == "__main__":
    unittest.main()
