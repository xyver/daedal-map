"""Identify unknown geography identifiers against maintained reference banks.

This is the discovery step immediately before ``resolve_reference``.  It works
only from exact identifier evidence: format signatures, reference-graph alias
rows, and geometry-bank availability.  It does not inspect polygons or infer a
vintage that the evidence cannot support.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from .external_reference_adapters import (
    GERS_SYSTEM,
    admitted_external_adapters,
    external_equivalence_matches,
    external_system_aliases,
    identifier_matches,
)
from .family_admin_crosswalk import admin_level_name
from ..paths import DATA_ROOT


US_CENSUS_GEOID_SYSTEM = "us_census_geoid"

US_STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

_SYSTEM_ALIASES = {
    **external_system_aliases(),
    "loc_id": "daedalmap.loc_id",
    "locid": "daedalmap.loc_id",
    "daedalmap": "daedalmap.loc_id",
    "daedalmap_loc_id": "daedalmap.loc_id",
    "geoid": US_CENSUS_GEOID_SYSTEM,
    "census_geoid": US_CENSUS_GEOID_SYSTEM,
    "census_2020_geoid": US_CENSUS_GEOID_SYSTEM,
    "us_census_2020_geoid": US_CENSUS_GEOID_SYSTEM,
    "zip": "overlay_zcta",
    "zcta": "overlay_zcta",
    "usa.census.2020.zcta5.geoid": "overlay_zcta",
    "nws_zone": "overlay_nws_public_zone",
    "nws_fire": "overlay_nws_fire_weather_zone",
}


def normalize_identifier_system(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return _SYSTEM_ALIASES.get(text, text)


def census_geoid_level(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text.isdigit() or text[:2] not in US_STATE_FIPS_TO_ABBR:
        return None
    return {2: "admin_1", 5: "admin_2", 11: "admin_3", 12: "admin_4", 15: "admin_5"}.get(len(text))


def census_geoid_to_loc_id(value: Any) -> str | None:
    text = str(value or "").strip()
    level = census_geoid_level(text)
    if not level:
        return None
    state = US_STATE_FIPS_TO_ABBR[text[:2]]
    if level == "admin_1":
        return f"USA-{state}"
    county = text[2:5]
    if level == "admin_2":
        return f"USA-{state}-{county}"
    tract = text[5:11]
    if level == "admin_3":
        return f"USA-{state}-{county}-{tract}"
    block_group = text[11]
    if level == "admin_4":
        return f"USA-{state}-{county}-{tract}-{block_group}"
    return f"USA-{state}-{county}-{tract}-{block_group}-{text[11:15]}"


def _expected_level(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return admin_level_name(value)
    except Exception:
        return str(value).strip().lower().replace(" ", "_")


def _geometry_rows(loc_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not loc_ids:
        return {}
    from .reference_exchange import get_geometry_references

    payload = get_geometry_references(loc_ids, include_polygon=False, include_info=False)
    return {
        str(row.get("loc_id") or ""): row
        for row in payload.get("results") or []
        if isinstance(row, dict) and row.get("loc_id")
    }


#: Evidence strength per detection method, strongest first. Used only to pick
#: between candidates that already agree about the referent.
_METHOD_RANK = {
    "typed_external_equivalence": 0,
    "exact_identifier_crosswalk": 0,
    "exact_identifier_lookup": 1,
    "reference_graph_exact_alias": 2,
    "loc_id_passthrough": 3,
}


def _candidates_resolve_alike(candidates: list[dict[str, Any]]) -> bool:
    """True when every candidate maps every identifier to the same loc_ids.

    Several systems recognizing one identifier is only ambiguous when they
    disagree about what it refers to. A five-digit US county code is matched by
    both the census GEOID adapter and the reference graph's native admin id, and
    both return ``USA-NY-061`` - the caller already has an unambiguous answer, so
    asking them to choose between two names for it produces the same loc_id
    whichever they pick.
    """
    if len(candidates) < 2:
        return True
    baseline: dict[str, tuple[str, ...]] | None = None
    for candidate in candidates:
        resolved = {
            str(row.get("identifier")): tuple(sorted(str(item) for item in row.get("loc_ids") or []))
            for row in candidate.get("sample_matches") or []
        }
        if not resolved:
            return False
        if baseline is None:
            baseline = resolved
        elif resolved != baseline:
            return False
    return baseline is not None


def _verified_loc_ids(values: list[str]) -> set[str]:
    if not values:
        return set()
    from .reference_exchange import verify_loc_ids

    return verify_loc_ids(values)


def _country_supporting_identifier_evidence(
    identifiers: list[str], *, country_scope: str,
) -> list[dict[str, Any]]:
    """Find exact values in known, non-admitted country support crosswalks."""
    if not country_scope or not identifiers:
        return []
    from .geometry_catalog import load_country_geometry_catalog

    catalog = load_country_geometry_catalog(country_scope)
    wanted = set(identifiers)
    evidence: list[dict[str, Any]] = []
    for asset in catalog.get("supporting_crosswalk_assets") or []:
        if not isinstance(asset, dict) or int(asset.get("row_count") or 0) > 1_000_000:
            continue
        path = DATA_ROOT / str(asset.get("path") or "")
        if not path.is_file() or path.suffix.lower() not in {".csv", ".parquet"}:
            continue
        candidate_columns = [
            str(column) for column in asset.get("columns") or []
            if re.search(r"(?:fips|geoid|loc_?id|code)", str(column), re.IGNORECASE)
        ]
        if not candidate_columns:
            continue
        try:
            import pandas as pd

            if path.suffix.lower() == ".csv":
                frame = pd.read_csv(path, usecols=candidate_columns, dtype=str, keep_default_na=False)
            else:
                frame = pd.read_parquet(path, columns=candidate_columns).fillna("").astype(str)
        except Exception:
            continue
        matching_columns = []
        matched_values: set[str] = set()
        for column in candidate_columns:
            values = set(frame[column].astype(str).str.strip())
            matches = sorted(wanted & values)
            if matches:
                matched_values.update(matches)
                matching_columns.append({
                    "column": column,
                    "match_count": len(matches),
                    "sample_matches": matches[:10],
                })
        if matching_columns:
            evidence.append({
                "path": str(asset.get("path") or ""),
                "discovery_status": asset.get("discovery_status"),
                "callable": False,
                "match_count": len(matched_values),
                "unmatched_count": len(wanted - matched_values),
                "matching_columns": matching_columns,
                "usage_note": asset.get("usage_note"),
            })
    return evidence


def _catalog_bank(*, country_scope: str, admin_level: str, expected_vintage: str | None) -> dict[str, Any] | None:
    from .geometry_catalog import load_country_geometry_catalog, load_geometry_catalog

    country = str(country_scope or "").strip().upper()
    vintage = str(expected_vintage or "").strip().lower()
    country_catalog = load_country_geometry_catalog(country) if country else {}
    catalog = country_catalog if country_catalog.get("geometry_banks") else load_geometry_catalog()
    matches = []
    for bank in catalog.get("geometry_banks") or []:
        if not isinstance(bank, dict):
            continue
        if country and str(bank.get("scope") or "").strip().upper() != country:
            continue
        try:
            bank_level = admin_level_name(bank.get("admin_level"))
        except Exception:
            bank_level = str(bank.get("admin_level") or "").strip().lower()
        if bank_level != admin_level:
            continue
        bank_vintage = str(bank.get("source_vintage") or bank.get("release_id") or "").strip().lower()
        if vintage and vintage not in bank_vintage:
            continue
        matches.append(bank)
    matches.sort(key=lambda item: (
        str(item.get("spine_readiness") or "") != "ready",
        str(item.get("bank_id") or ""),
    ))
    return matches[0] if matches else None


def _candidate(
    *,
    system: str,
    identifiers: list[str],
    matches: dict[str, list[str]],
    levels: dict[str, str] | None = None,
    method: str,
    expected_vintage: str | None = None,
    country_scope: str = "",
    use_catalog_bank_coverage: bool = False,
) -> dict[str, Any]:
    matched_identifiers = [value for value in identifiers if matches.get(value)]
    loc_ids = list(dict.fromkeys(loc_id for value in matched_identifiers for loc_id in matches[value]))
    level_values = sorted({level for level in (levels or {}).values() if level})
    catalog_bank = None
    if len(level_values) == 1:
        catalog_bank = _catalog_bank(
            country_scope=country_scope,
            admin_level=level_values[0],
            expected_vintage=expected_vintage,
        )
    # A level-wide bank proves that a maintained geometry system exists, not
    # that every syntactically valid identifier exists in it. Verify exact
    # identities before reporting geometry availability. This remains cheaper
    # than hydrating polygons and prevents release-specific codes from being
    # fabricated into the canonical spine.
    if use_catalog_bank_coverage and catalog_bank:
        geometry: dict[str, dict[str, Any]] = {}
        shape_ids = _verified_loc_ids(loc_ids)
        geometry_availability_basis = "exact_identity_plus_catalog_bank"
    else:
        geometry = _geometry_rows(loc_ids)
        shape_ids = {loc_id for loc_id, row in geometry.items() if row.get("has_shape")}
        geometry_availability_basis = "exact_geometry_row"
    bank_ids = sorted({str(geometry[loc_id].get("bank_id")) for loc_id in shape_ids if geometry.get(loc_id, {}).get("bank_id")})
    geometry_vintages = sorted({str(geometry[loc_id].get("geometry_vintage")) for loc_id in shape_ids if geometry.get(loc_id, {}).get("geometry_vintage")})
    if catalog_bank and catalog_bank.get("bank_id") and str(catalog_bank["bank_id"]) not in bank_ids:
        bank_ids.append(str(catalog_bank["bank_id"]))
    sample_matches = [
        {
            "identifier": value,
            "loc_ids": matches[value][:5],
            "geo_level": (levels or {}).get(value),
            "geometry_available": any(loc_id in shape_ids for loc_id in matches[value]),
        }
        # Identification calls are already bounded (100 values on the public
        # surface). Return every checked sample match so browser previews can
        # draw the exact same evidence the user reviewed instead of a second,
        # smaller subset.
        for value in matched_identifiers[:100]
    ]
    return {
        "system": system,
        "method": method,
        "match_count": len(matched_identifiers),
        "unmatched_count": len(identifiers) - len(matched_identifiers),
        "match_rate": round(len(matched_identifiers) / len(identifiers), 6) if identifiers else 0.0,
        "ambiguous_identifier_count": sum(1 for value in matched_identifiers if len(matches[value]) > 1),
        "geo_levels": level_values,
        "loc_id_resolvable": bool(loc_ids),
        "geometry_available": bool(shape_ids),
        "geometry_availability_basis": geometry_availability_basis,
        "geometry_available_count": sum(
            1 for value in matched_identifiers if any(loc_id in shape_ids for loc_id in matches[value])
        ),
        "geometry_bank_ids": sorted(bank_ids),
        "geometry_vintages": geometry_vintages,
        "expected_vintage_supported": (catalog_bank is not None) if expected_vintage else None,
        "catalog_bank": {
            "bank_id": catalog_bank.get("bank_id"),
            "source_vintage": catalog_bank.get("source_vintage"),
            "release_id": catalog_bank.get("release_id"),
            "geometry_path": catalog_bank.get("geometry_path"),
            "feature_count": catalog_bank.get("feature_count"),
            "spine_readiness": catalog_bank.get("spine_readiness"),
        } if catalog_bank else None,
        "sample_matches": sample_matches,
        # Retained only while candidates from the same reference system are
        # reconciled. These are removed from the public response below.
        "_matches": {key: list(value) for key, value in matches.items()},
        "_levels": dict(levels or {}),
        "_shape_ids": sorted(shape_ids),
    }


def _reference_graph_candidates(identifiers: list[str], *, country_scope: str = "") -> list[dict[str, Any]]:
    try:
        from .reference_graph import identify_aliases

        alias_rows = identify_aliases(
            identifiers,
            limit=max(100, len(identifiers) * 25),
            iso3=country_scope or None,
        )
    except Exception:
        alias_rows = []
    grouped: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for row in alias_rows:
        system = normalize_identifier_system(row.get("reference_system"))
        external_id = str(row.get("external_id") or "")
        loc_id = str(row.get("loc_id") or "")
        if country_scope and not (loc_id == country_scope or loc_id.startswith(country_scope + "-")):
            continue
        if system and external_id and loc_id:
            grouped[system][external_id].append(loc_id)
    return [
        _candidate(
            system=system,
            identifiers=identifiers,
            matches=dict(matches),
            method="reference_graph_exact_alias",
        )
        for system, matches in grouped.items()
    ]


def _invalid_identification_contract(*, code: str, message: str, reason: str, question_id: str, prompt: str, maps_to: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "invalid_request",
        "error": {"code": code, "message": message},
        "warnings": [{
            "code": "strict_input_contract",
            "message": "Translate natural language into the documented JSON shape before calling this tool; identifier values must remain strings so leading zeros survive.",
        }],
        "guidance": {
            "action": "translate_then_retry",
            "required_shape": {"identifiers": ["06073000100", "06073000201"]},
            "example_call": {
                "tool": "identify_reference_system",
                "arguments": {
                    "identifiers": ["06073000100", "06073000201"],
                    "expected": {"system": "us_census_geoid", "geo_level": "tract", "vintage": "2020"},
                    "country_scope": "USA",
                },
            },
        },
        "clarification": {
            "required": True,
            "reason": reason,
            "questions": [{
                "id": question_id,
                "prompt": prompt,
                "answer_schema": {"type": "array", "minItems": 1, "items": {"type": "string"}} if maps_to == "identifiers" else {"type": "string"},
                "maps_to": maps_to,
            }],
        },
    }


def _dataset_interpretations(
    identifiers: list[str],
    *,
    dataset_context: dict[str, Any],
    candidates: list[dict[str, Any]],
    expected_level: str | None,
) -> list[dict[str, Any]]:
    """Combine cheap dataset clues with exact identifier verification.

    These scores are discovery guidance, not identity assertions. Exact match
    and geometry counts remain separate so a friendly confidence label can
    never hide incomplete maintained coverage.
    """
    if not dataset_context:
        return []

    column_name = str(dataset_context.get("column_name") or "").strip()
    raw_column_names = dataset_context.get("column_names")
    column_names = [
        str(value).strip()
        for value in (raw_column_names if isinstance(raw_column_names, list) else [])
        if str(value).strip()
    ]
    row_geography = str(dataset_context.get("row_geography") or "").strip().lower()
    normalized_column = re.sub(r"[^a-z0-9]+", "_", column_name.lower()).strip("_")
    normalized_columns = {
        re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
        for value in column_names
    }
    local_format_match_rate = dataset_context.get("local_format_match_rate")
    try:
        local_format_match_rate = max(0.0, min(1.0, float(local_format_match_rate)))
    except (TypeError, ValueError):
        local_format_match_rate = None

    by_system = {str(item.get("system") or ""): item for item in candidates}
    all_five_digits = bool(identifiers) and all(
        value.isdigit() and len(value) == 5 for value in identifiers
    )
    census_county_shape = bool(identifiers) and all(
        census_geoid_level(value) == "admin_2" for value in identifiers
    )
    has_state_component = bool(normalized_columns & {
        "statefp", "state_fips", "statefips", "stusps", "state_abbr",
    })
    has_county_component = bool(normalized_columns & {
        "countyfp", "county_fips", "countyfips", "county_code", "county_name",
    })
    header_says_zip = bool(re.search(r"(?:^|_)(?:zip|zipcode|zip_code|zcta|postal_code)(?:$|_)", normalized_column))
    header_says_county = "county" in normalized_column
    header_says_geoid = bool(re.search(r"(?:^|_)geoid\d*(?:$|_)", normalized_column))
    hint_says_county = "county" in row_geography
    hint_says_zip = "zip" in row_geography or "zcta" in row_geography

    def verified_evidence(system: str) -> tuple[float, float, list[str]]:
        candidate = by_system.get(system) or {}
        match_rate = float(candidate.get("match_rate") or 0.0)
        match_count = int(candidate.get("match_count") or 0)
        geometry_count = int(candidate.get("geometry_available_count") or 0)
        geometry_rate = geometry_count / match_count if match_count else 0.0
        evidence: list[str] = []
        if match_count:
            evidence.append(f"{match_count}/{len(identifiers)} sampled identifiers matched exactly")
            evidence.append(f"{geometry_count}/{match_count} exact matches have maintained geometry")
        else:
            evidence.append("format alternative; exact maintained matches were not checked in this call")
        return match_rate, geometry_rate, evidence

    interpretations: list[dict[str, Any]] = []
    if all_five_digits:
        match_rate, geometry_rate, evidence = verified_evidence(US_CENSUS_GEOID_SYSTEM)
        score = 0.0
        if census_county_shape:
            score += 0.25
            evidence.append("values have the five-digit state-plus-county GEOID shape")
        if header_says_county:
            score += 0.25
            evidence.append("the selected column name explicitly says county")
        elif header_says_geoid:
            score += 0.15
            evidence.append("the selected column is named GEOID")
        if has_state_component and has_county_component:
            score += 0.20
            evidence.append("state and county component columns corroborate the combined identifier")
        if hint_says_county:
            score += 0.15
            evidence.append("the caller described the row geography as county")
        score += 0.10 * match_rate + 0.05 * geometry_rate
        if expected_level and expected_level != "admin_2":
            score *= 0.35
        interpretations.append({
            "system": US_CENSUS_GEOID_SYSTEM,
            "geo_level": "admin_2",
            "label": "US Census county GEOID",
            "confidence_score": round(min(1.0, score), 3),
            "confidence": "high" if score >= 0.8 else "medium" if score >= 0.5 else "low",
            "verified": bool(match_rate),
            "match_rate": match_rate if match_rate else None,
            "geometry_availability_rate": round(geometry_rate, 6) if match_rate else None,
            "evidence": evidence,
        })

        match_rate, geometry_rate, evidence = verified_evidence("overlay_zcta")
        score = 0.20
        evidence.append("five-digit values can also resemble ZIP/ZCTA identifiers")
        if header_says_zip:
            score += 0.35
            evidence.append("the selected column name says ZIP, postal, or ZCTA")
        if hint_says_zip:
            score += 0.20
            evidence.append("the caller described the row geography as ZIP/ZCTA")
        if has_state_component and has_county_component:
            score -= 0.15
            evidence.append("separate state and county fields make the ZIP/ZCTA reading less likely")
        score += 0.20 * match_rate + 0.05 * geometry_rate
        score = max(0.0, min(1.0, score))
        interpretations.append({
            "system": "overlay_zcta",
            "geo_level": "zcta",
            "label": "US ZIP Code Tabulation Area (ZCTA)",
            "confidence_score": round(score, 3),
            "confidence": "high" if score >= 0.8 else "medium" if score >= 0.5 else "low",
            "verified": bool(match_rate),
            "match_rate": match_rate if match_rate else None,
            "geometry_availability_rate": round(geometry_rate, 6) if match_rate else None,
            "evidence": evidence,
        })

    for candidate in candidates:
        system = str(candidate.get("system") or "")
        if system == "admin.native_id" and any(
            item["system"] == US_CENSUS_GEOID_SYSTEM for item in interpretations
        ):
            continue
        if not system or any(item["system"] == system for item in interpretations):
            continue
        match_rate, geometry_rate, evidence = verified_evidence(system)
        geo_level = (candidate.get("geo_levels") or [None])[0]
        score = min(1.0, 0.55 * match_rate + 0.15 * geometry_rate)
        if local_format_match_rate is not None:
            score = min(1.0, score + 0.05 * local_format_match_rate)
            evidence.append(f"{local_format_match_rate:.0%} of populated values fit the locally detected format")
        if hint_says_county or (has_state_component and has_county_component):
            if geo_level == "admin_2" or "county" in system:
                score = min(1.0, score + 0.15)
            else:
                score *= 0.25
                evidence.append("county row/companion-column context makes this exact-code collision unlikely")
        interpretations.append({
            "system": system,
            "geo_level": geo_level,
            "label": system.replace("_", " "),
            "confidence_score": round(score, 3),
            "confidence": "high" if score >= 0.8 else "medium" if score >= 0.5 else "low",
            "verified": bool(match_rate),
            "match_rate": match_rate,
            "geometry_availability_rate": round(geometry_rate, 6),
            "evidence": evidence,
        })

    interpretations.sort(key=lambda item: (-float(item["confidence_score"]), str(item["system"])))
    return interpretations[:3]


def identify_reference_system(
    identifiers: list[Any],
    *,
    expected: dict[str, Any] | None = None,
    country_scope: str | None = None,
    validation_scope: str = "sample",
    dataset_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Rank maintained reference systems for a bounded identifier set."""
    if not isinstance(identifiers, list):
        return _invalid_identification_contract(
            code="invalid_identifiers_type",
            message="identifiers must be an array of strings, not prose or a comma-delimited string",
            reason="identifier_values_malformed",
            question_id="identifier_values",
            prompt="Provide the geography identifiers as a list of strings while preserving leading zeros.",
            maps_to="identifiers",
        )
    if any(not isinstance(value, str) for value in identifiers):
        return _invalid_identification_contract(
            code="identifier_strings_required",
            message="every identifier must be a string so leading zeros are preserved",
            reason="identifier_values_malformed",
            question_id="identifier_values",
            prompt="Provide the geography identifiers as strings, including any leading zeros.",
            maps_to="identifiers",
        )
    if expected is not None and not isinstance(expected, dict):
        return _invalid_identification_contract(
            code="invalid_expected_type",
            message="expected must be an object with optional system, geo_level, vintage, and country_scope fields",
            reason="expected_declaration_malformed",
            question_id="expected_declaration",
            prompt="What system, geography level, vintage, and country does the dataset declare?",
            maps_to="expected",
        )
    if dataset_context is not None and not isinstance(dataset_context, dict):
        return _invalid_identification_contract(
            code="invalid_dataset_context_type",
            message="dataset_context must be an object containing bounded column and row-geography clues",
            reason="dataset_context_malformed",
            question_id="dataset_context",
            prompt="Provide dataset context as an object, or omit it.",
            maps_to="dataset_context",
        )
    if validation_scope not in {"sample", "all_distinct_identifiers"}:
        return _invalid_identification_contract(
            code="invalid_validation_scope",
            message="validation_scope must be 'sample' or 'all_distinct_identifiers'",
            reason="validation_scope_malformed",
            question_id="validation_scope",
            prompt="Are these values a representative sample or every distinct identifier in the dataset?",
            maps_to="validation_scope",
        )
    values = list(dict.fromkeys(str(value).strip() for value in identifiers if str(value).strip()))
    if not values:
        return _invalid_identification_contract(
            code="identifiers_required",
            message="at least one non-empty identifier is required",
            reason="identifier_values_missing",
            question_id="identifier_values",
            prompt="Which column or values contain the geography identifiers? Provide a representative list while preserving leading zeros.",
            maps_to="identifiers",
        )
    expected = expected if isinstance(expected, dict) else {}
    expected_system = normalize_identifier_system(expected.get("system"))
    expected_level = _expected_level(expected.get("geo_level") or expected.get("admin_level"))
    expected_vintage = str(expected.get("vintage") or "").strip() or None
    expected_source_release = str(expected.get("source_release") or "").strip() or None
    expected_internal_release = str(expected.get("internal_release") or "").strip() or None
    country = str(country_scope or expected.get("country_scope") or "").strip().upper()

    candidates: list[dict[str, Any]] = []

    census_matches: dict[str, list[str]] = {}
    census_levels: dict[str, str] = {}
    if (not country or country == "USA") and (not expected_system or expected_system == US_CENSUS_GEOID_SYSTEM):
        for value in values:
            level = census_geoid_level(value)
            loc_id = census_geoid_to_loc_id(value)
            if loc_id and (not expected_level or level == expected_level):
                census_matches[value] = [loc_id]
                census_levels[value] = str(level)
        if census_matches:
            candidates.append(_candidate(
                system=US_CENSUS_GEOID_SYSTEM,
                identifiers=values,
                matches=census_matches,
                levels=census_levels,
                method="exact_identifier_crosswalk",
                expected_vintage=expected_vintage or "2020",
                country_scope=country or "USA",
                use_catalog_bank_coverage=True,
            ))

    if not expected_system or expected_system == "daedalmap.loc_id":
        # The regex is a prefilter, not the evidence. It only decides which
        # values are worth a lookup; matching it never means the identifier
        # exists. Verifying against maintained identities is what keeps a
        # foreign UUID from being reported as a confirmed loc_id at
        # match_rate 1.0, which is how Overture GERS ids read here before.
        shaped = [
            value for value in values
            if re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+", value)
        ]
        verified = _verified_loc_ids(shaped)
        loc_matches = {
            value: [value.upper()]
            for value in shaped
            if value.upper() in verified
        }
        if loc_matches:
            candidates.append(_candidate(
                system="daedalmap.loc_id", identifiers=values, matches=loc_matches,
                method="loc_id_passthrough", country_scope=country,
            ))

    for system, prefix in (
        ("overlay_nws_public_zone", "USA-NWSZ-"),
        ("overlay_nws_fire_weather_zone", "USA-NWSFZ-"),
    ):
        if expected_system and expected_system != system:
            continue
        matches = {
            value: [prefix + value.upper()]
            for value in values
            if re.fullmatch(r"[A-Za-z]{2}Z\d{3}", value)
        }
        if matches:
            candidates.append(_candidate(
                system=system, identifiers=values, matches=matches,
                method="exact_identifier_lookup", country_scope=country or "USA",
            ))

    for adapter in admitted_external_adapters():
        if expected_system and expected_system != adapter.system:
            continue
        # Identifier shape only bounds the lookup. A maintained
        # equivalence edge is the evidence; containment and overlap edges
        # deliberately cannot produce a recommended loc_id binding.
        shaped = [value for value in values if identifier_matches(adapter, value)]
        evidence = external_equivalence_matches(
            adapter.system,
            shaped,
            country_scope=country or None,
            source_release=expected_source_release,
            internal_release=expected_internal_release,
        ) if shaped else {"matches": {}}
        matches = evidence.get("matches") or {}
        if matches:
            candidate = _candidate(
                system=adapter.system,
                identifiers=values,
                matches=matches,
                method="typed_external_equivalence",
                country_scope=country,
            )
            candidate["source_releases"] = evidence.get("source_releases") or []
            candidate["internal_releases"] = evidence.get("internal_releases") or []
            candidate["source_levels"] = evidence.get("source_levels") or []
            candidates.append(candidate)

    # An explicitly declared system with complete maintained adapter evidence
    # does not need a second lookup through the reference graph. That graph
    # search exists to discover or corroborate an unknown/partial system; in a
    # hosted cold process it can otherwise hydrate alias partitions across the
    # object store after the answer is already known.
    expected_system_fully_matched = bool(expected_system) and any(
        candidate.get("system") == expected_system
        and candidate.get("match_count") == len(values)
        and (
            candidate.get("method") != "exact_identifier_crosswalk"
            or not candidate.get("catalog_bank")
            or candidate.get("geometry_available_count") == candidate.get("match_count")
        )
        and candidate.get("method") in {
            "exact_identifier_crosswalk",
            "typed_external_equivalence",
        }
        for candidate in candidates
    )
    if not expected_system_fully_matched:
        for candidate in _reference_graph_candidates(values, country_scope=country):
            if not expected_system or candidate["system"] == expected_system:
                candidates.append(candidate)

    # Merge duplicate systems per identifier. A format adapter can recognize
    # every value syntactically while an admitted graph alias corrects only the
    # release-specific values whose generated loc_ids do not exist (notably
    # Connecticut's 2022 tract GEOIDs). Choosing one whole candidate would
    # discard that exact alias evidence.
    by_system: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        prior = by_system.get(candidate["system"])
        if prior is None:
            by_system[candidate["system"]] = candidate
            continue
        prior_matches = prior.get("_matches") or {}
        incoming_matches = candidate.get("_matches") or {}
        prior_shapes = set(prior.get("_shape_ids") or [])
        incoming_shapes = set(candidate.get("_shape_ids") or [])
        combined: dict[str, list[str]] = {}
        for value in values:
            old = list(prior_matches.get(value) or [])
            new = list(incoming_matches.get(value) or [])
            if not old:
                combined[value] = new
            elif not new:
                combined[value] = old
            elif any(loc_id in incoming_shapes for loc_id in new) and not any(
                loc_id in prior_shapes for loc_id in old
            ):
                combined[value] = new
            elif any(loc_id in prior_shapes for loc_id in old) and not any(
                loc_id in incoming_shapes for loc_id in new
            ):
                combined[value] = old
            elif _METHOD_RANK.get(str(candidate.get("method") or ""), 99) < _METHOD_RANK.get(
                str(prior.get("method") or ""), 99
            ):
                combined[value] = new
            else:
                combined[value] = old
        levels = {**(prior.get("_levels") or {}), **(candidate.get("_levels") or {})}
        by_system[candidate["system"]] = _candidate(
            system=candidate["system"],
            identifiers=values,
            matches=combined,
            levels=levels,
            method=(
                "exact_identifier_crosswalk"
                if candidate["system"] == US_CENSUS_GEOID_SYSTEM
                else str(prior.get("method") or candidate.get("method") or "reference_graph_exact_alias")
            ),
            expected_vintage=expected_vintage,
            country_scope=country,
            use_catalog_bank_coverage=(candidate["system"] == US_CENSUS_GEOID_SYSTEM),
        )
    candidates = list(by_system.values())
    candidates.sort(key=lambda item: (
        -float(item.get("match_rate") or 0),
        -int(item.get("geometry_available_count") or 0),
        int(item.get("ambiguous_identifier_count") or 0),
        str(item.get("system") or ""),
    ))

    # Only a vintage explicitly declared by the caller may disqualify an
    # otherwise exact identifier-system match. Census adapters use their
    # maintained vintage while looking up a geometry bank, but a temporarily
    # unavailable catalog entry must not make a five-digit county/ZCTA value
    # appear unambiguous and silently select the other system.
    vintage_is_constrained = expected_vintage is not None
    full_matches = [
        candidate for candidate in candidates
        if candidate.get("match_rate") == 1.0
        and (
            not vintage_is_constrained
            or candidate.get("expected_vintage_supported") is not False
        )
    ]
    # Collapse concurring systems before deciding ambiguity. Systems that name
    # the same referent are not competing readings of the identifier, and
    # reporting them as a conflict blocks the binding on a question whose every
    # answer resolves identically.
    concurring_systems: list[str] = []
    if len(full_matches) > 1 and _candidates_resolve_alike(full_matches):
        concurring_systems = sorted(str(item.get("system") or "") for item in full_matches)
        # Bind the system carrying the most evidence, not the alphabetically
        # first one. A census GEOID adapter knows the level and vintage; a bare
        # reference-graph native id knows neither, and both naming USA-NY-061
        # does not make them equally good bindings.
        full_matches = sorted(full_matches, key=lambda item: _METHOD_RANK.get(
            str(item.get("method") or ""), len(_METHOD_RANK)
        ))[:1]

    if not candidates:
        status = "unmatched"
    elif expected_system:
        status = "matched" if candidates[0] in full_matches else "partial_match"
    elif len(full_matches) == 1:
        status = "matched"
    elif len(full_matches) > 1:
        status = "ambiguous"
    else:
        status = "partial_match"

    public_candidates = [
        {key: value for key, value in candidate.items() if not key.startswith("_")}
        for candidate in candidates
    ]
    dataset_interpretations = _dataset_interpretations(
        values,
        dataset_context=dataset_context or {},
        candidates=public_candidates,
        expected_level=expected_level,
    )
    selected = full_matches[0] if full_matches and status == "matched" else None
    country_catalog_evidence = _country_supporting_identifier_evidence(
        values, country_scope=country,
    )
    exact_geometry_check_required = bool(
        selected
        and selected.get("method") == "exact_identifier_crosswalk"
        and selected.get("catalog_bank")
    )
    exact_geometry_complete = bool(
        not exact_geometry_check_required
        or (
            selected
            and int(selected.get("geometry_available_count") or 0)
            == int(selected.get("match_count") or 0)
        )
    )
    recommended_binding = None
    if selected and exact_geometry_complete:
        levels = selected.get("geo_levels") or []
        recommended_binding = {
            "mode": "reference",
            "system": selected["system"],
            "geo_level": levels[0] if len(levels) == 1 else expected_level,
            "vintage": expected_vintage or (
                "2020" if selected["system"] == US_CENSUS_GEOID_SYSTEM else None
            ),
            "country_scope": country or ("USA" if selected["system"] == US_CENSUS_GEOID_SYSTEM else None),
        }
        if any(selected["system"] == adapter.system for adapter in admitted_external_adapters()):
            source_releases = selected.get("source_releases") or []
            internal_releases = selected.get("internal_releases") or []
            recommended_binding["source_release"] = expected_source_release or (
                source_releases[0] if len(source_releases) == 1 else None
            )
            recommended_binding["internal_release"] = expected_internal_release or (
                internal_releases[0] if len(internal_releases) == 1 else None
            )
    warnings: list[dict[str, Any]] = []
    guidance = None
    clarification = None
    if selected and exact_geometry_check_required and not exact_geometry_complete:
        missing_count = int(selected.get("match_count") or 0) - int(
            selected.get("geometry_available_count") or 0
        )
        warnings.append({
            "code": "identifier_geometry_coverage_incomplete",
            "message": (
                f"The identifier system matched, but {missing_count} distinct identifier(s) "
                "lack exact identity/geometry in the selected bank. No bulk binding was issued."
            ),
        })
        if country_catalog_evidence:
            warnings.append({
                "code": "known_supporting_crosswalk_not_admitted",
                "message": (
                    "The country catalog contains exact matching values in local supporting "
                    "crosswalk evidence, but that asset is not yet an admitted callable geometry crosswalk."
                ),
            })
        guidance = {
            "action": "inspect_country_catalog_then_admit_or_select_vintage",
            "message": (
                "Inspect the country catalog evidence and declared source vintage before conversion."
            ),
            "recommended_tool": "read_geometry_catalog",
        }
    if str(validation_scope or "sample") == "sample" and status in {"matched", "ambiguous", "partial_match"}:
        warnings.append({
            "code": "sample_validation_only",
            "message": "Only the supplied sample was checked. Use validation_scope='all_distinct_identifiers' with every distinct key before treating the binding as dataset-wide.",
        })
    if status == "ambiguous":
        choices = [
            {
                "value": candidate.get("system"),
                "label": str(candidate.get("system") or "").replace("_", " "),
                "geo_levels": candidate.get("geo_levels") or [],
                "match_rate": candidate.get("match_rate"),
                "geometry_available": candidate.get("geometry_available"),
            }
            for candidate in full_matches
        ]
        warnings.append({"code": "ambiguous_identifier_system", "message": "More than one maintained reference system matches every supplied identifier; no binding was selected."})
        guidance = {"action": "ask_user_then_retry", "message": "Ask which candidate system the dataset uses, then retry with expected.system (and level/vintage when known)."}
        clarification = {
            "required": True,
            "reason": "ambiguous_reference_system",
            "questions": [{
                "id": "reference_system",
                "prompt": "Which geography identifier system does this dataset use?",
                "answer_schema": {"type": "string", "enum": [choice["value"] for choice in choices]},
                "choices": choices,
                "maps_to": "expected.system",
            }],
            "retry": {
                "tool": "identify_reference_system",
                "base_arguments": {"identifiers": values, "country_scope": country or None, "validation_scope": validation_scope},
                "answer_mapping": {"reference_system": "expected.system"},
            },
        }
    elif status == "partial_match":
        vintage_conflict = any(candidate.get("expected_vintage_supported") is False for candidate in candidates)
        code = "expected_vintage_unavailable" if vintage_conflict else "partial_identifier_match"
        message = (
            "The identifiers match the declared system and level, but the requested vintage has no maintained matching geometry bank."
            if vintage_conflict else
            "Only part of the supplied identifier set matched the declared or detected system."
        )
        warnings.append({"code": code, "message": message})
        field = "vintage" if vintage_conflict else "reference_system"
        clarification = {
            "required": True,
            "reason": code,
            "questions": [{
                "id": field,
                "prompt": "What source vintage does the dataset use?" if vintage_conflict else "Do all rows use the same geography identifier system?",
                "answer_schema": {"type": "string" if vintage_conflict else "boolean"},
                "maps_to": "expected.vintage" if vintage_conflict else None,
            }],
        }
        guidance = {"action": "ask_user_then_retry", "message": message, "recommended_tool": "list_reference_systems" if vintage_conflict else "identify_reference_system"}
    elif status == "unmatched":
        code = "unknown_expected_system" if expected_system else "no_reference_system_match"
        warnings.append({
            "code": code,
            "message": "The expected.system value is not a supported canonical name or alias." if expected_system else "No maintained exact identifier system matched the supplied values.",
        })
        guidance = {
            "action": "inspect_contract_then_retry",
            "message": "Use list_reference_systems for canonical system names, or omit expected.system to detect from exact values.",
            "next_call": {"tool": "list_reference_systems", "arguments": {}},
            "accepted_examples": ["us_census_geoid", "daedalmap.loc_id", "overlay_zcta", "overlay_nws_public_zone", "overlay_nws_fire_weather_zone"],
        }
        clarification = {
            "required": True,
            "reason": code,
            "questions": [{
                "id": "reference_system_description",
                "prompt": "What organization, identifier system, geography level, and vintage produced these values?",
                "answer_schema": {"type": "string"},
                "maps_to": None,
            }],
        }

    return {
        "ok": status in {"matched", "ambiguous", "partial_match"},
        "status": status,
        "validation_scope": str(validation_scope or "sample"),
        "identifier_count": len(identifiers),
        "distinct_identifier_count": len(values),
        "expected": {
            "system": expected_system or None,
            "geo_level": expected_level,
            "vintage": expected_vintage,
            "source_release": expected_source_release,
            "internal_release": expected_internal_release,
            "country_scope": country or None,
        },
        "candidates": public_candidates,
        "dataset_context": dataset_context or None,
        "dataset_interpretations": dataset_interpretations,
        "concurring_systems": concurring_systems,
        "recommended_binding": recommended_binding,
        "country_catalog_evidence": country_catalog_evidence,
        "next_call": {
            "tool": "estimate_conversion_job",
            "arguments": {"geography_binding": recommended_binding},
        } if recommended_binding else None,
        "needed_context": ["system", "geo_level", "vintage"] if status == "ambiguous" else [],
        "warnings": warnings,
        "guidance": guidance,
        "clarification": clarification,
    }
