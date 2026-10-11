import unittest

from mapmover.runtime.region_expansion import expand_region


class RegionLocationAliasTests(unittest.TestCase):
    def test_ambiguous_name_does_not_become_admin1(self):
        result = expand_region(
            "New York",
            resolve_country_subdivision_slug_loc_id_func=lambda _value: None,
            regional_groups={},
            load_conversions_func=lambda: {
                "region_aliases": {},
                "regional_groupings": {},
            },
            load_iso_codes_func=lambda: {"iso3_to_name": {}},
            load_usa_admin_func=lambda: {"state_abbreviations": {}},
        )

        self.assertEqual(result, set())


if __name__ == "__main__":
    unittest.main()
