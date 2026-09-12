"""Identify unknown geography identifiers against maintained identity indexes.

This is the discovery step immediately before ``resolve_reference``.  It works
only from identifier formats and maintained reference/equivalence indexes.
Geometry availability belongs to the shape tools and is deliberately not read
here.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Callable

from .external_reference_adapters import (
    GERS_SYSTEM,
    admitted_external_adapters,
    external_equivalence_matches,
    external_system_aliases,
    identifier_matches,
)
from .family_admin_crosswalk import admin_level_name


US_CENSUS_GEOID_SYSTEM = "us_census_geoid"


class IdentificationCancelled(RuntimeError):
    """Raised cooperatively when the caller no longer needs identification."""


def _cancel_point(cancelled: Callable[[], bool] | None) -> None:
    if cancelled and cancelled():
        raise IdentificationCancelled("reference identification cancelled")

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


#: Evidence strength per detection method, strongest first. Used only to pick
#: between candidates that already agree about the referent.
_METHOD_RANK = {
    "typed_external_equivalence": 0,
    "exact_identifier_crosswalk": 0,
    "exact_identifier_lookup": 1,
    "reference_graph_exact_alias": 2,
    "loc_id_passthrough": 3,
}


def _admin_route_metadata_for_loc_ids(loc_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Read admin level and deep-shard ownership from country route indexes."""
    grouped: dict[str, list[str]] = defaultdict(list)
    for loc_id in dict.fromkeys(loc_ids):
        if loc_id:
            grouped[loc_id.split("-", 1)[0]].append(loc_id)
    found: dict[str, dict[str, Any]] = {}
    try:
        from .admin_spine_query import load_route_rows_by_loc_ids

        for country, values in grouped.items():
            route_rows = load_route_rows_by_loc_ids(country, values)
            for row in route_rows.to_dict("records"):
                loc_id = str(row.get("loc_id") or "")
                if loc_id:
                    found[loc_id] = row
    except Exception:
        pass
    return found


def _identity_metadata_for_loc_ids(loc_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Read bounded identity metadata without letting one country poison peers."""
    try:
        from .reference_graph import identities
    except Exception:
        return {}
    grouped: dict[str, list[str]] = defaultdict(list)
    for loc_id in dict.fromkeys(loc_ids):
        if loc_id:
            grouped[loc_id.split("-", 1)[0]].append(loc_id)
    # Admin identity metadata is already encoded in each country's compact
    # route index. Sidechain identities absent from the spine retain the graph
    # fallback because full reference-system identification may need them.
    found = _admin_route_metadata_for_loc_ids(loc_ids)
    for values in grouped.values():
        missing_values = [loc_id for loc_id in values if loc_id not in found]
        if not missing_values:
            continue
        try:
            found.update({str(row.get("loc_id") or ""): row for row in identities(missing_values)})
        except Exception:
            continue
    missing = [loc_id for loc_id in dict.fromkeys(loc_ids) if loc_id and loc_id not in found]
    if missing:
        try:
            from .reference_graph import identity

            for loc_id in missing[:100]:
                row = identity(loc_id)
                if row:
                    found[loc_id] = row
        except Exception:
            pass
    return found


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


def _candidate(
    *,
    system: str,
    identifiers: list[str],
    matches: dict[str, list[str]],
    levels: dict[str, str] | None = None,
    method: str,
    expected_vintage: str | None = None,
    country_scope: str = "",
) -> dict[str, Any]:
    matched_identifiers = [value for value in identifiers if matches.get(value)]
    loc_ids = list(dict.fromkeys(loc_id for value in matched_identifiers for loc_id in matches[value]))
    level_values = sorted({level for level in (levels or {}).values() if level})
    sample_matches = [
        {
            "identifier": value,
            "loc_ids": matches[value][:5],
            "geo_level": (levels or {}).get(value),
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
        "expected_vintage_supported": (
            expected_vintage in {"2020", "census_2020"}
            if system == US_CENSUS_GEOID_SYSTEM and expected_vintage
            else None
        ),
        "sample_matches": sample_matches,
        # Retained only while candidates from the same reference system are
        # reconciled. These are removed from the public response below.
        "_matches": {key: list(value) for key, value in matches.items()},
        "_levels": dict(levels or {}),
    }


def _reference_graph_candidates(
    identifiers: list[str], *, country_scope: str = "", reference_system: str | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    lookup_to_original: dict[str, list[str]] = defaultdict(list)
    try:
        from .reference_graph import identify_aliases
        from .reference_exchange import _normalize_source_loc_id

        for identifier in identifiers:
            lookup = (
                _normalize_source_loc_id(reference_system, identifier, country_scope)
                if reference_system else identifier
            )
            lookup_to_original[lookup].append(identifier)

        alias_rows = identify_aliases(
            list(lookup_to_original),
            limit=max(100, len(identifiers) * 25),
            iso3=country_scope or None,
            reference_system=reference_system,
            cancelled=lambda: _cancel_point(cancelled),
        )
    except IdentificationCancelled:
        raise
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
            for original in lookup_to_original.get(external_id, [external_id]):
                grouped[system][original].append(loc_id)
    identity_levels: dict[str, str] = {
        loc_id: "admin_0"
        for matches in grouped.values()
        for values in matches.values()
        for loc_id in values
        if re.fullmatch(r"[A-Z]{3}", loc_id)
    }
    try:
        loc_ids = list(dict.fromkeys(
            loc_id for matches in grouped.values() for values in matches.values() for loc_id in values
            if loc_id not in identity_levels
        ))
        for row in _identity_metadata_for_loc_ids(loc_ids).values():
            loc_id = str(row.get("loc_id") or "")
            raw_level = row.get("admin_level")
            if raw_level is None or str(raw_level).strip() == "":
                raw_level = row.get("level")
            level = str(raw_level if raw_level is not None else "").strip()
            if loc_id and level:
                identity_levels[loc_id] = level if level.startswith("admin_") else f"admin_{level}"
    except Exception:
        identity_levels = {}

    results = []
    for system, matches in grouped.items():
        levels = {}
        for external_id, loc_ids in matches.items():
            found = {identity_levels.get(loc_id) for loc_id in loc_ids if identity_levels.get(loc_id)}
            if len(found) == 1:
                levels[external_id] = found.pop()
        candidate = _candidate(
            system=system,
            identifiers=identifiers,
            matches=dict(matches),
            levels=levels,
            method="reference_graph_exact_alias",
        )
        scopes = sorted({loc_id.split("-", 1)[0] for values in matches.values() for loc_id in values if loc_id})
        candidate["country_scopes"] = scopes
        results.append(candidate)
    return results


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
    counts remain separate so a friendly confidence label cannot hide partial
    identifier coverage.
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

    def verified_evidence(system: str) -> tuple[float, list[str]]:
        candidate = by_system.get(system) or {}
        match_rate = float(candidate.get("match_rate") or 0.0)
        match_count = int(candidate.get("match_count") or 0)
        evidence: list[str] = []
        if match_count:
            evidence.append(f"{match_count}/{len(identifiers)} sampled identifiers matched exactly")
        else:
            evidence.append("format alternative; exact maintained matches were not checked in this call")
        return match_rate, evidence

    interpretations: list[dict[str, Any]] = []
    if all_five_digits:
        match_rate, evidence = verified_evidence(US_CENSUS_GEOID_SYSTEM)
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
        score += 0.15 * match_rate
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
            "evidence": evidence,
        })

        match_rate, evidence = verified_evidence("overlay_zcta")
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
        score += 0.25 * match_rate
        score = max(0.0, min(1.0, score))
        interpretations.append({
            "system": "overlay_zcta",
            "geo_level": "zcta",
            "label": "US ZIP Code Tabulation Area (ZCTA)",
            "confidence_score": round(score, 3),
            "confidence": "high" if score >= 0.8 else "medium" if score >= 0.5 else "low",
            "verified": bool(match_rate),
            "match_rate": match_rate if match_rate else None,
            "evidence": evidence,
        })

    for candidate in candidates:
        system = str(candidate.get("system") or "")
        if system == "admin.native_id" and any(
            item["system"] == US_CENSUS_GEOID_SYSTEM and item.get("verified")
            for item in interpretations
        ):
            continue
        if not system or any(item["system"] == system for item in interpretations):
            continue
        match_rate, evidence = verified_evidence(system)
        geo_level = (candidate.get("geo_levels") or [None])[0]
        score = min(1.0, 0.70 * match_rate)
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
    cancelled: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Rank maintained reference systems for a bounded identifier set."""
    _cancel_point(cancelled)
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
            ))

    geoboundaries_code_checked = False
    if not expected_system or expected_system == "geoboundaries.code":
        shaped = [value for value in values if re.fullmatch(r"[A-Za-z]{3}", value)]
        try:
            from .global_admin0_query import load_global_admin0_identities

            identity_rows = load_global_admin0_identities(shaped) or {}
        except Exception:
            identity_rows = {}
        matches = {
            value: [value.upper()]
            for value in shaped
            if value.upper() in identity_rows
        }
        if matches:
            candidates.append(_candidate(
                system="geoboundaries.code",
                identifiers=values,
                matches=matches,
                levels={value: "admin_0" for value in matches},
                method="exact_identifier_lookup",
                country_scope="",
            ))
        geoboundaries_code_checked = expected_system == "geoboundaries.code"

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
        _cancel_point(cancelled)
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
        and candidate.get("method") in {
            "exact_identifier_crosswalk",
            "typed_external_equivalence",
        }
        for candidate in candidates
    )
    if not expected_system_fully_matched and not geoboundaries_code_checked:
        graph_kwargs: dict[str, Any] = {
            "country_scope": country,
            "reference_system": expected_system or None,
        }
        if cancelled is not None:
            graph_kwargs["cancelled"] = cancelled
        for candidate in _reference_graph_candidates(values, **graph_kwargs):
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
        combined: dict[str, list[str]] = {}
        for value in values:
            old = list(prior_matches.get(value) or [])
            new = list(incoming_matches.get(value) or [])
            if not old:
                combined[value] = new
            elif not new:
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
        )
    candidates = list(by_system.values())
    # Every identifier path, including typed external adapters, should expose
    # the level/country carried by the canonical identities it resolved. This
    # keeps dataset identification generic instead of teaching each caller the
    # semantics of every published country system.
    try:
        resolved_loc_ids = list(dict.fromkeys(
            loc_id
            for candidate in candidates
            if not candidate.get("geo_levels")
            for loc_ids in (candidate.get("_matches") or {}).values()
            for loc_id in loc_ids
        ))
        identity_rows = _identity_metadata_for_loc_ids(resolved_loc_ids)
        for candidate in candidates:
            candidate_loc_ids = [
                loc_id for loc_ids in (candidate.get("_matches") or {}).values() for loc_id in loc_ids
            ]
            levels = set()
            for loc_id in candidate_loc_ids:
                row = identity_rows.get(loc_id) or {}
                raw_level = row.get("admin_level")
                if raw_level is None or str(raw_level).strip() == "":
                    raw_level = row.get("level")
                if raw_level is not None and str(raw_level).strip() != "":
                    text_level = str(raw_level).strip()
                    levels.add(text_level if text_level.startswith("admin_") else f"admin_{text_level}")
            if not candidate.get("geo_levels") and levels:
                candidate["geo_levels"] = sorted(levels)
            if not candidate.get("country_scopes"):
                candidate["country_scopes"] = sorted({loc_id.split("-", 1)[0] for loc_id in candidate_loc_ids if loc_id})
    except Exception:
        pass
    candidates.sort(key=lambda item: (
        -float(item.get("match_rate") or 0),
        int(item.get("ambiguous_identifier_count") or 0),
        str(item.get("system") or ""),
    ))

    # Only a vintage explicitly declared by the caller may disqualify an
    # otherwise exact identifier-system match. A five-digit county/ZCTA value
    # must not appear unambiguous and silently select the other system merely
    # because one maintained release is temporarily unavailable.
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
    if (
        selected is None
        and expected_system
        and candidates
        and candidates[0].get("match_count")
        and candidates[0].get("expected_vintage_supported") is not False
    ):
        # An explicit system confirmation can bind the rows that do match while
        # preserving partial_match and per-row failures for the rest. This is
        # the normal bulk-join contract: useful rows are not discarded because
        # a few identifiers need later review. An explicitly unavailable
        # vintage remains unbound because retrying with supported source
        # context is required before conversion.
        selected = candidates[0]
    recommended_binding = None
    if selected:
        levels = selected.get("geo_levels") or []
        selected_scopes = selected.get("country_scopes") or []
        recommended_binding = {
            "mode": "reference",
            "system": selected["system"],
            "geo_level": levels[0] if len(levels) == 1 else expected_level,
            "vintage": expected_vintage or (
                "2020" if selected["system"] == US_CENSUS_GEOID_SYSTEM else None
            ),
            "country_scope": country or (
                "USA" if selected["system"] == US_CENSUS_GEOID_SYSTEM
                else selected_scopes[0] if len(selected_scopes) == 1 else None
            ),
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
            "The identifiers match the declared system and level, but the requested vintage is not supported by this identifier adapter."
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
        "next_call": {
            "tool": "estimate_conversion_job",
            "arguments": {"geography_binding": recommended_binding},
        } if recommended_binding else None,
        "needed_context": ["system", "geo_level", "vintage"] if status == "ambiguous" else [],
        "warnings": warnings,
        "guidance": guidance,
        "clarification": clarification,
    }


_DATASET_GEO_HEADER_TOKENS = {
    "loc", "location", "place", "geo", "geoid", "id", "code", "fips", "iso",
    "country", "nation", "state", "province", "territory", "county", "tract",
    "district", "municipality", "municipal", "commune", "region", "area", "zone",
    "postal", "postcode", "zip", "zcta", "admin", "ward", "borough", "prefecture",
    "department", "nuts", "latitude", "longitude", "lat", "lon", "lng", "x", "y",
}

# Dataset discovery separates reviewable clues from an automatic choice. These
# are deliberately centralized so traffic-based tuning does not spread magic
# numbers across the MCP and browser.
DATASET_REVIEW_CONFIDENCE = 0.60
DATASET_AUTO_CONFIDENCE = 0.80
DATASET_CANDIDATE_LIMIT = 3


def _dataset_header_tokens(value: Any) -> set[str]:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value or ""))
    return set(filter(None, re.sub(r"[^a-z0-9]+", "_", text.lower()).split("_")))


def _coordinate_dataset_candidate(columns: list[dict[str, Any]]) -> dict[str, Any] | None:
    latitude = None
    longitude = None
    for column in columns:
        tokens = _dataset_header_tokens(column.get("name"))
        if latitude is None and tokens & {"latitude", "lat"}:
            latitude = column
        if longitude is None and tokens & {"longitude", "lon", "lng", "long"}:
            longitude = column
    if latitude is None or longitude is None:
        return None
    pairs = []
    lat_values = latitude.get("aligned_values") or latitude.get("values") or []
    lon_values = longitude.get("aligned_values") or longitude.get("values") or []
    for lat_value, lon_value in zip(lat_values, lon_values):
        try:
            lat = float(lat_value)
            lon = float(lon_value)
        except (TypeError, ValueError):
            continue
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            pairs.append(f"{lat},{lon}")
    if not pairs:
        return None
    return {
        "id": "dataset-coordinates",
        "kind": "coordinates",
        "header": f"{latitude.get('name')} + {longitude.get('name')}",
        "columns": [str(latitude.get("name") or ""), str(longitude.get("name") or "")],
        "sampleValues": pairs,
        "nonempty": len(pairs),
        "formatMatchRate": round(len(pairs) / max(1, min(len(lat_values), len(lon_values))), 6),
        "localConfidence": "high",
        "serverHinted": True,
        "catalog": {
            "ok": True,
            "status": "matched",
            "recommended_binding": {
                "mode": "coordinates",
                "columns": [str(latitude.get("name") or ""), str(longitude.get("name") or "")],
            },
            "candidates": [],
        },
    }


def _dataset_level_rank(candidate: dict[str, Any]) -> int:
    level = str((candidate.get("catalog", {}).get("recommended_binding") or {}).get("geo_level") or "")
    match = re.fullmatch(r"admin_(\d+)", level)
    return int(match.group(1)) if match else -1


def _dataset_expected_hint(
    column_name: str, column_names: list[str], values: list[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Return only strong, server-owned adapter hints.

    These hints live beside the maintained identifier adapters so browsers do
    not need their own reference-system registry. Catalog-native systems still
    flow through exact alias discovery with no code change here.
    """
    camel_split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column_name)
    normalized = re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")
    normalized_columns = {
        re.sub(
            r"[^a-z0-9]+", "_",
            re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name)).lower(),
        ).strip("_")
        for name in column_names
    }
    if re.fullmatch(r"(?:daedalmap_)?loc_?id", normalized):
        return {"system": "daedalmap.loc_id"}, None
    census_header = bool(re.search(r"(?:^|_)geoid\d*(?:$|_)", normalized))
    def census_expected() -> dict[str, Any] | None:
        levels = {census_geoid_level(str(value).strip()) for value in values or [] if str(value).strip()}
        levels.discard(None)
        if not levels:
            return None
        expected: dict[str, Any] = {"system": US_CENSUS_GEOID_SYSTEM}
        if len(levels) == 1:
            expected["geo_level"] = levels.pop()
        return expected

    if census_header:
        expected = census_expected()
        if expected:
            return expected, "USA"
        samples = [str(value).strip() for value in values or [] if str(value).strip()]
        if "placefp" in normalized_columns and samples and all(re.fullmatch(r"\d{7}", value) for value in samples):
            return {"system": "usa.census.2025.place.geoid"}, "USA"
    if "fips" in _dataset_header_tokens(column_name):
        expected = census_expected()
        if expected:
            return expected, "USA"
    if normalized == "aiannhce":
        return {"system": "usa.census.2025.aiannhce"}, "USA"
    if normalized in {"iso3", "iso_3", "country_code", "location_code"}:
        samples = [str(value).strip().upper() for value in values or [] if str(value).strip()]
        iso3_like = sum(bool(re.fullmatch(r"[A-Z]{3}", value)) for value in samples)
        if samples and iso3_like / len(samples) >= 0.8:
            return {"system": "geoboundaries.code", "geo_level": "admin_0"}, None
    return None, None


def _dataset_graph_evidence(
    columns: list[dict[str, Any]], *, country_scope: str | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> dict[str, tuple[int, int, str, str]]:
    """Score columns from one catalog-backed alias scan, independent of headers."""
    try:
        from .reference_graph import identify_aliases

        all_values = list(dict.fromkeys(
            value for column in columns for value in column.get("values") or [] if value
        ))
        aliases = identify_aliases(
            all_values,
            limit=max(500, len(all_values) * 10),
            iso3=country_scope,
            cancelled=lambda: _cancel_point(cancelled),
        )
        loc_ids = list(dict.fromkeys(
            str(alias.get("loc_id") or "") for alias in aliases if alias.get("loc_id")
        ))
        # Ranking a dataset column only needs to know whether an exact alias is
        # on the admin spine and how deep it is. Do not hydrate sidechain
        # identity graphs here; the selected column gets full verification in
        # identify_reference_system immediately afterward.
        identities = _admin_route_metadata_for_loc_ids(loc_ids)
    except IdentificationCancelled:
        raise
    except Exception:
        return {}
    levels_by_value: dict[str, list[int]] = defaultdict(list)
    for alias in aliases:
        value = str(alias.get("external_id") or "")
        node = identities.get(str(alias.get("loc_id") or "")) or {}
        raw_level = node.get("admin_level")
        try:
            level = int(str(raw_level).removeprefix("admin_"))
        except (TypeError, ValueError):
            level = -1
        levels_by_value[value].append(level)
    evidence: dict[str, tuple[int, int, str, str]] = {}
    countries_by_value: dict[str, list[str]] = defaultdict(list)
    systems_by_value: dict[str, list[str]] = defaultdict(list)
    for alias in aliases:
        external_id = str(alias.get("external_id") or "")
        loc_id = str(alias.get("loc_id") or "")
        country = str(alias.get("country_scope") or loc_id.split("-", 1)[0]).upper()
        system = normalize_identifier_system(alias.get("reference_system"))
        if external_id and len(country) == 3:
            countries_by_value[external_id].append(country)
        if external_id and system:
            systems_by_value[external_id].append(system)
    for column in columns:
        values = column.get("values") or []
        matched = sum(1 for value in values if value in levels_by_value)
        deepest = max(
            (level for value in values for level in levels_by_value.get(value, [])),
            default=-1,
        )
        country_counts: dict[str, int] = defaultdict(int)
        system_counts: dict[str, int] = defaultdict(int)
        for value in values:
            for country in set(countries_by_value.get(value, [])):
                country_counts[country] += 1
            for system in set(systems_by_value.get(value, [])):
                system_counts[system] += 1
        ranked = sorted(country_counts.items(), key=lambda item: (-item[1], item[0]))
        ranked_systems = sorted(system_counts.items(), key=lambda item: (-item[1], item[0]))
        inferred_country = (
            ranked[0][0]
            if ranked and ranked[0][1] >= max(1, round(len(values) * 0.8))
            and (len(ranked) == 1 or ranked[0][1] > ranked[1][1])
            else ""
        )
        threshold = max(1, round(len(values) * 0.8))
        # Native admin IDs often also carry public aliases for the same loc_id.
        # When the route index proves they are admin-spine identities, prefer
        # the native system instead of treating those concurring names as an
        # unresolved tie.
        inferred_system = (
            "admin.native_id"
            if system_counts.get("admin.native_id", 0) >= threshold and matched >= threshold
            else ranked_systems[0][0]
            if ranked_systems and ranked_systems[0][1] >= threshold
            and (len(ranked_systems) == 1 or ranked_systems[0][1] > ranked_systems[1][1])
            else ""
        )
        evidence[column["name"]] = (matched, deepest, inferred_country, inferred_system)
    return evidence


def identify_dataset_geography(
    columns: list[dict[str, Any]],
    *,
    dataset_context: dict[str, Any] | None = None,
    country_scope: str | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Identify geography columns from a bounded, browser-produced table profile.

    The browser owns parsing and deterministic sampling only. All geographic
    semantics—column choice, system, country, and level—are decided here from
    maintained reference indexes.
    """
    _cancel_point(cancelled)
    if not isinstance(columns, list) or not columns:
        return {
            "ok": False,
            "status": "invalid_request",
            "error": {"code": "dataset_columns_required", "message": "columns must contain at least one sampled column"},
        }
    clean_columns: list[dict[str, Any]] = []
    total_values = 0
    for raw in columns[:64]:
        _cancel_point(cancelled)
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        aligned_values = [
            str(value).strip() for value in (raw.get("values") or [])
            if isinstance(value, (str, int, float))
        ][:32]
        values = list(dict.fromkeys(
            value for value in aligned_values
            if isinstance(value, (str, int, float)) and str(value).strip()
        ))
        if not name or not values:
            continue
        total_values += len(values)
        if total_values > 2048:
            break
        clean_columns.append({"name": name, "values": values, "aligned_values": aligned_values, "nonempty_count": raw.get("nonempty_count")})
    if not clean_columns:
        return {"ok": False, "status": "unmatched", "error": {"code": "no_sample_values", "message": "No bounded scalar samples were supplied."}}

    candidates: list[dict[str, Any]] = []
    coordinate = _coordinate_dataset_candidate(clean_columns)
    if coordinate:
        candidates.append(coordinate)

    column_names = [item["name"] for item in clean_columns]
    authored_hints = {
        column["name"]: _dataset_expected_hint(column["name"], column_names, column.get("values"))
        for column in clean_columns
    }
    # A strong maintained adapter signature (loc_id itself or Census GEOID
    # with its companion fields) avoids a broad graph scan. Unknown country
    # schemas take the catalog-backed scan so unfamiliar headers still work.
    has_authored_hint = any(hint is not None for hint, _country in authored_hints.values())
    if coordinate and not has_authored_hint:
        return {
            "ok": True,
            "status": "matched",
            "column_count": len(clean_columns),
            "sample_value_count": total_values,
            "candidates": [coordinate],
            "recommended_candidate_id": coordinate["id"],
            "warnings": [],
        }
    graph_evidence = {} if has_authored_hint else _dataset_graph_evidence(
        clean_columns, country_scope=country_scope, cancelled=cancelled,
    )
    ranked_columns = sorted(
        clean_columns,
        key=lambda column: (
            0 if authored_hints[column["name"]][0] is not None else 1,
            -_dataset_level_rank({"catalog": {"recommended_binding": {
                "geo_level": (authored_hints[column["name"]][0] or {}).get("geo_level")
            }}}),
            -(graph_evidence.get(column["name"], (0, -1))[0] / max(1, len(column["values"]))),
            -graph_evidence.get(column["name"], (0, -1))[1],
            -len(_dataset_header_tokens(column["name"]) & _DATASET_GEO_HEADER_TOKENS),
            len(column["values"]),
            column["name"].lower(),
        ),
    )
    # The graph evidence pass has already ranked every supplied column. Probe
    # only the strongest bounded set instead of reopening reference partitions
    # for a long tail of low-signal scalar fields.
    for index, column in enumerate(ranked_columns[:DATASET_CANDIDATE_LIMIT]):
        _cancel_point(cancelled)
        expected_hint, expected_country = authored_hints[column["name"]]
        detected = graph_evidence.get(column["name"]) or (0, -1, "", "")
        if expected_hint is None and not detected[0]:
            continue
        inferred_country = detected[2]
        if expected_hint is None and detected[3]:
            expected_hint = {"system": detected[3]}
            if detected[1] >= 0:
                expected_hint["geo_level"] = f"admin_{detected[1]}"
        result = identify_reference_system(
            column["values"],
            expected=expected_hint,
            country_scope=country_scope or expected_country or inferred_country,
            validation_scope="sample",
            dataset_context={
                **(dataset_context or {}),
                "column_name": column["name"],
                "column_names": [item["name"] for item in clean_columns],
            },
            cancelled=cancelled,
        )
        interpretations = result.get("dataset_interpretations") or []
        if result.get("status") == "ambiguous" and interpretations:
            top = interpretations[0]
            runner_up = interpretations[1] if len(interpretations) > 1 else {}
            top_score = float(top.get("confidence_score") or 0)
            margin = top_score - float(runner_up.get("confidence_score") or 0)
            if top.get("verified") and top_score >= DATASET_AUTO_CONFIDENCE and margin >= 0.2:
                inferred_system = str(top.get("system") or "")
                inferred_scope = (
                    "USA" if inferred_system == US_CENSUS_GEOID_SYSTEM
                    else country_scope or expected_country or inferred_country
                )
                result = identify_reference_system(
                    column["values"],
                    expected={
                        "system": inferred_system,
                        "geo_level": top.get("geo_level"),
                    },
                    country_scope=inferred_scope,
                    validation_scope="sample",
                    dataset_context={
                        **(dataset_context or {}),
                        "column_name": column["name"],
                        "column_names": [item["name"] for item in clean_columns],
                    },
                    cancelled=cancelled,
                )
        matched = max((int(item.get("match_count") or 0) for item in result.get("candidates") or []), default=0)
        if not result.get("ok") or not matched:
            continue
        binding = result.get("recommended_binding") or {}
        system = str(binding.get("system") or ((result.get("candidates") or [{}])[0].get("system") or ""))
        kind = "loc_id" if system == "daedalmap.loc_id" else "reference"
        confidence_score = max(
            (float(item.get("confidence_score") or 0) for item in result.get("dataset_interpretations") or []),
            default=float(((result.get("candidates") or [{}])[0].get("match_rate") or 0) * 0.7),
        )
        # A maintained header adapter plus exact identifier coverage is stronger
        # than the generic interpretation score alone (for example 4/5 ISO3
        # codes). Unhinted columns keep the conservative contextual score so
        # incidental numeric/name collisions do not become automatic choices.
        if expected_hint is not None:
            confidence_score = max(
                confidence_score,
                max((float(item.get("match_rate") or 0) for item in result.get("candidates") or []), default=0.0),
            )
        if confidence_score < DATASET_REVIEW_CONFIDENCE:
            continue
        candidates.append({
            "id": f"dataset-column-{index + 1}",
            "kind": kind,
            "header": column["name"],
            "columns": [column["name"]],
            "sampleValues": column["values"],
            "nonempty": int(column.get("nonempty_count") or len(column["values"])),
            "formatMatchRate": 1.0,
            "localConfidence": "",
            "serverHinted": expected_hint is not None,
            "confidenceScore": round(confidence_score, 3),
            "expectedSystem": "",
            "expectedLevel": str(binding.get("geo_level") or ""),
            "countryScope": str(binding.get("country_scope") or ""),
            "catalog": result,
        })
        if (
            binding
            and (
                expected_hint is not None
                or (
                    result.get("status") == "matched"
                    and binding.get("geo_level")
                    and confidence_score >= DATASET_AUTO_CONFIDENCE
                )
            )
        ):
            break

    candidates.sort(key=lambda item: (
        0 if item.get("serverHinted") else 1,
        0 if (item.get("catalog", {}).get("recommended_binding") or {}).get("mode") else 1,
        -max((float(row.get("match_rate") or 0) for row in item.get("catalog", {}).get("candidates") or []), default=0.0),
        -_dataset_level_rank(item),
        -float(item.get("confidenceScore") or 0),
        str(item.get("header") or ""),
    ))
    candidates = candidates[:DATASET_CANDIDATE_LIMIT]
    status = "matched" if candidates else "unmatched"
    recommended = next((item for item in candidates if (
        item.get("kind") == "coordinates"
        or float(item.get("confidenceScore") or 0) >= DATASET_AUTO_CONFIDENCE
    )), None)
    return {
        "ok": True,
        "status": status,
        "column_count": len(clean_columns),
        "sample_value_count": total_values,
        "candidates": candidates,
        "recommended_candidate_id": recommended["id"] if recommended else None,
        "warnings": [] if candidates else [{"code": "no_dataset_geography_match", "message": "No maintained geography system matched the sampled columns."}],
    }
