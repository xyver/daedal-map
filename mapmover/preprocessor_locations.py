"""Location helper extraction for preprocessor.py."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Optional


_NAME_TO_ISO3_CACHE = None
_SUBREGION_TO_ISO3_CACHE = None


def build_name_to_iso3(*, reference_dir: Path, load_reference_file: Callable[[Path], Optional[dict]]) -> dict:
    """Build reverse lookup from country name to ISO3 code."""
    global _NAME_TO_ISO3_CACHE
    if _NAME_TO_ISO3_CACHE is not None:
        return _NAME_TO_ISO3_CACHE

    iso_path = reference_dir / "iso_codes.json"
    name_to_iso3 = {}
    if iso_path.exists():
        data = load_reference_file(iso_path) or {}
        iso3_to_name = data.get("iso3_to_name", {})
        for iso3, name in iso3_to_name.items():
            name_to_iso3[name.lower()] = iso3
            clean_name = name.lower()
            for suffix in [" islands", " island", " republic", " federation"]:
                if clean_name.endswith(suffix):
                    name_to_iso3[clean_name.replace(suffix, "").strip()] = iso3

    name_to_iso3.update(
        {
            "usa": "USA",
            "us": "USA",
            "united states": "USA",
            "america": "USA",
            "uk": "GBR",
            "britain": "GBR",
            "england": "GBR",
            "russia": "RUS",
            "ussr": "RUS",
            "korea": "KOR",
            "south korea": "KOR",
            "north korea": "PRK",
            "dprk": "PRK",
            "taiwan": "TWN",
            "republic of china": "TWN",
            "iran": "IRN",
            "persia": "IRN",
            "syria": "SYR",
            "uae": "ARE",
            "emirates": "ARE",
            "vietnam": "VNM",
            "viet nam": "VNM",
            "congo": "COD",
            "drc": "COD",
            "ivory coast": "CIV",
            "cote d'ivoire": "CIV",
            "turkey": "TUR",
            "turkiye": "TUR",
            "holland": "NLD",
            "netherlands": "NLD",
            "czech republic": "CZE",
            "czechia": "CZE",
        }
    )
    _NAME_TO_ISO3_CACHE = name_to_iso3
    return name_to_iso3


def build_subregion_to_iso3(*, reference_dir: Path, load_reference_file: Callable[[Path], Optional[dict]]) -> dict:
    """Build lookup from capitals to parent country ISO3."""
    global _SUBREGION_TO_ISO3_CACHE
    if _SUBREGION_TO_ISO3_CACHE is not None:
        return _SUBREGION_TO_ISO3_CACHE

    subregion_to_iso3 = {}
    metadata_path = reference_dir / "country_metadata.json"
    if metadata_path.exists():
        data = load_reference_file(metadata_path) or {}
        capitals = data.get("capitals", {})
        for iso3, capital in capitals.items():
            if isinstance(capital, str) and capital and not capital.startswith("_"):
                subregion_to_iso3[capital.lower()] = iso3

    _SUBREGION_TO_ISO3_CACHE = subregion_to_iso3
    return subregion_to_iso3


def extract_country_from_query(
    query: str,
    *,
    normalize_query_for_location_matching: Callable[[str], str],
    reference_dir: Path,
    load_reference_file: Callable[[Path], Optional[dict]],
) -> dict:
    """Extract country from query using hierarchical country/capital resolution."""
    result = {"match": None, "ambiguous": False, "matches": [], "source": None}
    normalized_query = normalize_query_for_location_matching(query)
    query_lower = normalized_query.lower()

    name_to_iso3 = build_name_to_iso3(reference_dir=reference_dir, load_reference_file=load_reference_file)
    for name in sorted(name_to_iso3.keys(), key=len, reverse=True):
        pattern = r"\b" + re.escape(name) + r"\b"
        if re.search(pattern, query_lower):
            result["match"] = (name, name_to_iso3[name], False)
            result["source"] = "country"
            return result

    subregion_to_iso3 = build_subregion_to_iso3(reference_dir=reference_dir, load_reference_file=load_reference_file)
    for subregion in sorted(subregion_to_iso3.keys(), key=len, reverse=True):
        pattern = r"\b" + re.escape(subregion) + r"\b"
        if re.search(pattern, query_lower):
            result["match"] = (subregion, subregion_to_iso3[subregion], True)
            result["source"] = "capital"
            return result

    return result


def detect_location_candidates(
    query: str,
    *,
    normalize_query_for_location_matching: Callable[[str], str],
    reference_dir: Path,
    load_reference_file: Callable[[Path], Optional[dict]],
) -> dict:
    """Detect all possible location matches in query with confidence scores."""
    normalized_query = normalize_query_for_location_matching(query)
    query_lower = normalized_query.lower()
    candidates = []
    iso_data = load_reference_file(reference_dir / "iso_codes.json") or {}
    iso3_to_name = iso_data.get("iso3_to_name", {})

    name_to_iso3 = build_name_to_iso3(reference_dir=reference_dir, load_reference_file=load_reference_file)
    for name in sorted(name_to_iso3.keys(), key=len, reverse=True):
        pattern = r"\b" + re.escape(name) + r"\b"
        if re.search(pattern, query_lower):
            iso3 = name_to_iso3[name]
            candidates.append(
                {
                    "matched_term": name,
                    "iso3": iso3,
                    "loc_id": iso3,
                    "country_name": iso3_to_name.get(iso3, name.title()),
                    "confidence": 1.0,
                    "match_type": "country",
                    "is_subregion": False,
                }
            )

    subregion_to_iso3 = build_subregion_to_iso3(reference_dir=reference_dir, load_reference_file=load_reference_file)
    for subregion in sorted(subregion_to_iso3.keys(), key=len, reverse=True):
        pattern = r"\b" + re.escape(subregion) + r"\b"
        if re.search(pattern, query_lower):
            iso3 = subregion_to_iso3[subregion]
            candidates.append(
                {
                    "matched_term": subregion,
                    "iso3": iso3,
                    "loc_id": iso3,
                    "country_name": iso3_to_name.get(iso3, subregion.title()),
                    "confidence": 0.9,
                    "match_type": "capital",
                    "is_subregion": True,
                }
            )

    candidates = sorted(candidates, key=lambda x: -x["confidence"])
    seen = set()
    unique_candidates = []
    for candidate in candidates:
        loc_key = candidate.get("loc_id") or candidate.get("iso3")
        if loc_key and loc_key not in seen:
            seen.add(loc_key)
            unique_candidates.append(candidate)
    return {"candidates": unique_candidates, "best": unique_candidates[0] if unique_candidates else None}


def detect_drilldown_pattern(
    query: str,
    *,
    extract_country_from_query_func: Callable[[str], dict],
) -> dict:
    """Detect drill-down patterns like 'texas counties' or 'counties in texas'."""
    query_lower = query.lower().strip()
    query_lower = re.sub(r"^(?:show\s+me\s+)?(?:all\s+)?(?:the\s+)?", "", query_lower)
    level_names = [
        "counties",
        "states",
        "cities",
        "districts",
        "regions",
        "provinces",
        "municipalities",
        "departments",
        "prefectures",
        "parishes",
        "boroughs",
    ]

    for level in level_names:
        pattern = rf"^{level}\s+(?:in|of)\s+(.+)$"
        match = re.match(pattern, query_lower)
        if match:
            location_part = match.group(1).strip()
            if location_part:
                result = extract_country_from_query_func(location_part)
                if result.get("match"):
                    matched_term, iso3, is_subregion = result["match"]
                    return {
                        "is_drilldown": True,
                        "parent_location": {
                            "matched_term": matched_term,
                            "iso3": iso3,
                            "loc_id": result.get("loc_id", iso3),
                            "country_name": result.get("country_name", matched_term),
                            "is_subregion": is_subregion,
                        },
                        "child_level_name": level,
                    }

    for level in level_names:
        if query_lower.endswith(level):
            location_part = query_lower[: -len(level)].strip()
            if not location_part:
                continue
            result = extract_country_from_query_func(location_part)
            if result.get("match"):
                matched_term, iso3, is_subregion = result["match"]
                return {
                    "is_drilldown": True,
                    "parent_location": {
                        "matched_term": matched_term,
                        "iso3": iso3,
                        "loc_id": result.get("loc_id", iso3),
                        "country_name": result.get("country_name", matched_term),
                        "is_subregion": is_subregion,
                    },
                    "child_level_name": level,
                }

    return {"is_drilldown": False}


def extract_multiple_locations(
    query: str,
    *,
    detect_drilldown_pattern_func: Callable[[str], dict],
    search_locations_globally: Callable[[str, int | None], list],
    extract_country_from_query_func: Callable[[str], dict],
    logger,
) -> dict:
    """Extract multiple locations from a query like 'X, Y, and Z counties'."""
    query_lower = query.lower()
    drilldown = detect_drilldown_pattern_func(query)
    if drilldown.get("is_drilldown"):
        parent = drilldown["parent_location"]
        parent["drill_to_level"] = drilldown["child_level_name"]
        return {"locations": [parent], "needs_disambiguation": False, "suffix_type": "plural"}

    singular_suffixes = {
        "county": 2,
        "parish": 2,
        "borough": 2,
        "state": 1,
        "province": 1,
        "region": 1,
        "city": 3,
        "town": 3,
        "place": 3,
        "district": 2,
    }
    plural_suffixes = {
        "counties": 2,
        "parishes": 2,
        "boroughs": 2,
        "states": 1,
        "provinces": 1,
        "regions": 1,
        "cities": 3,
        "towns": 3,
        "places": 3,
        "districts": 2,
    }

    suffix_found = None
    expected_admin_level = None
    suffix_type = None

    for suffix, level in singular_suffixes.items():
        if query_lower.endswith(suffix):
            suffix_found = suffix
            expected_admin_level = level
            suffix_type = "singular"
            query_lower = query_lower[: -len(suffix)].strip()
            break

    if not suffix_found:
        for suffix, level in plural_suffixes.items():
            if query_lower.endswith(suffix):
                suffix_found = suffix
                expected_admin_level = level
                suffix_type = "plural"
                query_lower = query_lower[: -len(suffix)].strip()
                break

    normalized = re.sub(r"\s+and\s+", ", ", query_lower)
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    parts = [p.strip() for p in normalized.split(",") if p.strip()]
    all_matches = []

    for part in parts:
        part_matches = []
        if expected_admin_level is not None:
            logger.debug(f"Viewport lookup empty for '{part}', doing global search at admin_level={expected_admin_level}")
            global_matches = search_locations_globally(part, admin_level=expected_admin_level)
            if global_matches:
                part_matches.extend(global_matches)
                logger.debug(f"Global search found {len(global_matches)} matches for '{part}'")

        if not part_matches and expected_admin_level is None:
            result = extract_country_from_query_func(part)
            if result.get("match"):
                matched_term, iso3, is_subregion = result["match"]
                part_matches.append(
                    {
                        "matched_term": matched_term,
                        "iso3": iso3,
                        "is_subregion": is_subregion,
                        "source": result.get("source", "country"),
                    }
                )
        all_matches.extend(part_matches)

    needs_disambiguation = suffix_type == "singular" and len(all_matches) > 1
    return {
        "locations": all_matches,
        "needs_disambiguation": needs_disambiguation,
        "suffix_type": suffix_type,
        "query_term": query.strip(),
    }
