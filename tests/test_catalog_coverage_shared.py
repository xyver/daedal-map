from catalog_coverage_shared import (
    coverage_contract,
    coverage_matches_country,
    normalize_geographic_levels,
    normalize_scope,
    normalize_time_range,
    temporal_intersects,
)


def test_scope_vocabulary_is_canonical() -> None:
    assert normalize_scope("usa") == "USA"
    assert normalize_scope("USA") == "USA"
    assert normalize_scope("Worldwide") == "global"
    assert normalize_scope("ocean") == "marine"


def test_global_with_regional_exclusions_does_not_claim_worldwide() -> None:
    contract = coverage_contract({
        "scope": "global",
        "geographic_coverage": {"type": "regional", "common_missing": ["CAN"]},
    })
    assert contract["assertion"] == "regional_countries_not_declared"
    assert coverage_matches_country(contract, "CAN") is False
    assert coverage_matches_country(contract, "USA") is None


def test_explicit_country_and_empty_level_meanings() -> None:
    contract = coverage_contract({
        "scope": "usa",
        "data_type": "events",
        "geographic_levels": ["admin_2"],
    })
    assert contract["countries"] == ["USA"]
    assert contract["geographic_levels"] == [2]
    assert coverage_matches_country(contract, "USA") is True
    assert coverage_matches_country(contract, "CAN") is False
    assert normalize_geographic_levels({"scope": "marine"})["empty_meaning"] == "marine_non_administrative"
    assert normalize_geographic_levels({"data_type": "raster"})["empty_meaning"] == "raster_or_grid_non_administrative"
    assert normalize_geographic_levels({"data_type": "events"})["empty_meaning"] == "not_declared"


def test_time_range_and_intersection_are_deterministic() -> None:
    requested = normalize_time_range({"start": "2020", "end": "2022"})
    assert requested == {"start": "2020", "end": "2022"}
    assert temporal_intersects("2019", "2021", requested) is True
    assert temporal_intersects("2010", "2019", requested) is False
    assert temporal_intersects(None, None, requested) is None
