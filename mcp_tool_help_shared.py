"""Blind-caller guidance for every public DaedalMap MCP tool.

Schemas and descriptions remain in ``mcp_surface_shared``. Access limits remain
in ``tool_access_shared``. This module owns the semantic help fields that cannot
be inferred safely from JSON Schema alone: when to use a tool, refusals,
examples, important outputs, provenance, and the next useful call.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from tool_access_shared import (
    tool_free_item_limit,
    tool_inline_item_limit,
    tool_paid_item_limit,
    tool_pricing,
    tool_profile,
)


def _g(use_when, do_not_use_for, example, outputs, next_calls=(), provenance=()):
    return {
        "use_when": list(use_when),
        "do_not_use_for": list(do_not_use_for),
        "examples": [example],
        "important_output_fields": list(outputs),
        "provenance_fields": list(provenance),
        "recommended_next_calls": list(next_calls),
    }


TOOL_GUIDANCE: dict[str, dict[str, Any]] = {
    "get_tool_help": _g(
        ["You found a tool through MCP discovery and need its usage contract or a working example."],
        ["Executing the target tool", "Replacing tools/list discovery"],
        {"tool_name": "resolve_point"},
        ["purpose", "input_schema", "interaction_contract", "examples", "access", "recommended_next_calls", "available_on_facades"],
    ),
    "how_geometry_works": _g(
        ["You are new to the geometry MCP or need to choose the correct workflow before inspecting one tool."],
        ["Executing a geometry operation", "Replacing get_tool_help for one exact tool"],
        {"question": "How do I match an uploaded Census dataset to geometry?"},
        ["core_rule", "interaction_contract", "workflows", "available_tools", "notes"],
        ["get_tool_help", "read_geometry_catalog", "identify_reference_system"],
        ["geometry catalog", "reference systems", "bank vintages", "loc_id doctrine"],
    ),
    "get_catalog": _g(
        ["You need to discover currently published data packs and tool families."],
        ["Querying pack rows", "Discovering detailed geography-bank coverage"],
        {}, ["packs", "tool_families", "public_catalogs"], ["get_pack", "read_geometry_catalog"]
    ),
    "get_pack": _g(
        ["You selected a pack or tool family and need its live contract before calling it."],
        ["Executing a dataset query", "Fetching geometry"],
        {"pack_id": "earthquakes"},
        ["quick_start", "routing", "pricing", "sources", "temporal_coverage"],
        ["query_dataset", "get_tool_help"], ["source metadata", "release/freshness fields"]
    ),
    "resolve_point": _g(
        ["You have WGS84 latitude/longitude and need an administrative loc_id chain."],
        ["Resolving names or outside codes", "Returning polygons", "Using deep mode without exactly one country_scope and one admin_1_scope"],
        {"points": [{"id": "row-1", "lat": 49.2827, "lon": -123.1207}], "lookup_mode": "standard"},
        ["deepest_resolved_loc_id", "stack", "resolution_mode", "available_deeper_admin_levels"],
        ["loc_id_info", "check_geometry", "get_geometry", "compare_geographies"]
    ),
    "loc_id_info": _g(
        ["You already have loc_id values and need identity, hierarchy, lifecycle, or attached references."],
        ["Returning full polygons", "Calculating pairwise overlap"],
        {"loc_id": "CAN-BC", "include_hierarchy": True},
        ["loc_id", "parent_id", "hierarchy", "valid_from", "valid_to", "supersession", "references"],
        ["check_geometry", "get_geometry", "compare_geographies"],
        ["source_system", "source_vintage", "release_id", "bank_id"]
    ),
    "read_geometry_catalog": _g(
        ["You need current geometry collections, families, banks, bridges, releases, or exports."],
        ["Resolving a place", "Returning shapes"],
        {"view": "capabilities"},
        ["capabilities", "counts", "download_url"],
        ["list_reference_systems", "resolve_reference", "check_geometry"],
        ["catalog fingerprint", "bank releases", "source licenses"]
    ),
    "list_reference_systems": _g(
        ["You need the canonical published list of callable crosswalks, systems, and vintages for a country."],
        ["Converting a value", "Resolving coordinates"],
        {"country_scope": "USA"}, ["systems", "crosswalks", "reserve_system", "bridge artifacts", "vintages"],
        ["identify_reference_system", "resolve_reference", "convert_reference"], ["source authority", "license", "relationship vintage", "crosswalk_id"]
    ),
    "identify_reference_system": _g(
        ["You have unknown geography identifiers or want to verify a declared system, level, vintage, and matching shape bank."],
        ["Passing the user's prose question as arguments", "Converting every dataset row", "Returning polygons", "Claiming full-dataset validation from a sample"],
        {"identifiers": ["06073000100", "06073000201"], "expected": {"system": "us_census_geoid", "geo_level": "tract", "vintage": "2020"}, "country_scope": "USA"},
        ["status", "candidates", "match_rate", "geometry_available_count", "geometry_bank_ids", "recommended_binding"],
        ["estimate_conversion_job", "resolve_reference", "check_geometry"],
        ["reference system", "source vintage", "geometry bank ids", "validation scope"],
    ),
    "identify_dataset_geography": _g(
        ["You have bounded samples from several table columns and need DaedalMap to select and identify the geography."],
        ["Uploading the complete source file", "Pre-classifying systems in browser code", "Returning polygons", "Converting every row"],
        {"columns": [{"name": "municipality_code", "values": ["1200013", "1200054"]}], "dataset_context": {"file_name": "population.csv", "row_count": 200}},
        ["status", "candidates", "recommended_candidate_id", "recommended_binding"],
        ["identify_reference_system", "estimate_conversion_job", "resolve_point"],
        ["selected input column", "reference system", "country scope", "admin level", "sample match rate"],
    ),
    "resolve_reference": _g(
        ["You have a name or external geography code and need ranked DaedalMap loc_id matches."],
        ["Converting coordinates", "Pretending overlap is strict parentage"],
        {"from_system": "zip", "value": "00601", "target_admin_level": "county"},
        ["resolved_loc_id", "matches", "relationship_type", "confidence"],
        ["loc_id_info", "convert_reference", "check_geometry"],
        ["source_system", "source_vintage", "bridge_artifact", "relationship_method"]
    ),
    "convert_reference": _g(
        ["You need one reference system expressed in another through loc_id."],
        ["Resolving coordinates", "Discarding one-to-many weights"],
        {"from_system": "zip", "value": "00601", "to_system": "nws_fire"},
        ["from", "to_system", "results", "relationship_type", "weight"],
        ["loc_id_info", "check_geometry"],
        ["source and target systems", "bridge vintage", "relationship method", "artifact id"]
    ),
    "compare_geographies": _g(
        ["You have two loc_ids and need spatial, temporal, containment, overlap, or successor evidence."],
        ["Resolving names", "Choosing one successor when evidence is one-to-many"],
        {"left_loc_id": "CAN-BC", "right_loc_id": "CAN"},
        ["spatial_relation", "temporal_relation", "left_area_share", "right_area_share", "successors"],
        ["loc_id_info", "get_geometry"],
        ["left.bank_id", "right.bank_id", "geometry vintages", "calculation method"]
    ),
    "check_geometry": _g(
        ["You have loc_ids and want a cheap exact-shape availability preflight."],
        ["Returning polygons", "Resolving identities or coordinates"],
        {"loc_ids": ["CAN-BC", "CAN-NOPE"]},
        ["requested", "available", "missing", "items", "supersession"],
        ["get_geometry", "estimate_geometry_package"],
        ["bank_id", "geometry_vintage", "source", "license"]
    ),
    "get_geometry": _g(
        ["You have exact loc_ids and need bbox, centroid, or opt-in polygons."],
        ["Explaining hierarchy", "Resolving names", "Bulk export packaging"],
        {"loc_id": "CAN-BC", "include_polygon": False},
        ["loc_id", "has_shape", "bbox", "centroid", "geometry", "supersession"],
        ["loc_id_info", "estimate_geometry_package"],
        ["bank_id", "geometry_vintage", "source", "license", "release_id"]
    ),
    "resolve_loc_id_scope": _g(
        ["You need strict descendants of one stored parent at a target administrative level."],
        ["Crossing mixed-release seams", "Natural-language place resolution"],
        {"parent_loc_id": "CAN-BC", "admin_level": "admin_2", "limit": 10},
        ["total_count", "returned_count", "truncated", "loc_ids"],
        ["check_geometry", "estimate_geometry_package"], ["hierarchy release", "bank ids"]
    ),
    "estimate_geometry_package": _g(
        ["You need a free preflight before creating a selected geometry export."],
        ["Publishing a canonical geometry release", "Creating an artifact"],
        {"loc_ids": ["CAN-BC"], "format": "geojson_gzip", "include_polygon": True},
        ["quote_id", "available_shape_count", "estimated_transfer_bytes", "recommended_delivery_mode", "create_call"],
        ["create_geometry_export"], ["contributing banks", "license/citation requirements", "vintages"]
    ),
    "create_geometry_export": _g(
        ["You accepted a synchronous geometry export plan within the advertised effective limit and want a real artifact."],
        ["Publishing or mutating official geometry", "Skipping estimate for large selections"],
        {"loc_ids": ["CAN-BC"], "format": "geojson", "include_polygon": False},
        ["job_id", "status", "result", "artifact", "next_call"],
        ["get_job_status"], ["contributing banks", "license/citations", "format", "artifact hash"]
    ),
    "estimate_conversion_job": _g(
        ["You need a free sample-based estimate before converting user-supplied reference rows."],
        ["Resolving coordinate rows", "Executing the conversion"],
        {"from_system": "admin.native_id", "items": [{"row_index": 1, "value": "10", "iso3": "CAN", "data": {"population": 1000}}], "output_format": "csv"},
        ["quote_id", "row_count", "sample_resolved", "estimated_output_bytes", "output_format", "create_call"],
        ["create_conversion_job"], ["input system", "bridge vintages", "sample evidence"]
    ),
    "create_conversion_job": _g(
        ["You accepted a synchronous reference-conversion plan within the advertised effective limit and want real output."],
        ["Resolving coordinate CSVs", "Modifying official identities"],
        {"from_system": "admin.native_id", "items": [{"row_index": 1, "value": "10", "iso3": "CAN", "data": {"population": 1000}}], "output_format": "csv", "output_name": "cleaned-population"},
        ["job_id", "status", "result", "output_rows", "artifact", "next_call"],
        ["get_job_status"], ["source systems", "bridge vintages", "row-level relationship evidence"]
    ),
    "get_job_status": _g(
        ["You have a job_id returned by a create tool and need its current state or result."],
        ["Starting or altering work", "Looking up an unknown job without its id"],
        {"job_id": "geometry_export_example"},
        ["job_id", "kind", "status", "progress", "result", "artifact", "callback_state"],
    ),
    "get_disaster_links_for_event": _g(
        ["You have an exact supported disaster event id and need its direct published links."],
        ["Fuzzy event search", "Inventing causal claims"],
        {"event_id": "NOAA-SIG-2", "cross_type_only": True},
        ["event", "links", "link_count"], ["get_disaster_link_chain"], ["link method", "source artifacts"]
    ),
    "get_disaster_link_chain": _g(
        ["You have an exact event id and need a bounded multi-hop related-event chain."],
        ["Unbounded graph traversal", "Fuzzy event search"],
        {"event_id": "NOAA-SIG-2", "depth": 1}, ["chains", "depth", "truncated"],
        ["get_disaster_links_for_event"], ["link method", "source artifacts"]
    ),
    "search_disaster_links": _g(
        ["You need to discover whether a published disaster-link family exists before you have an event id."],
        ["Querying event rows", "Claiming unsupported reverse directions"],
        {"start_event_type": "earthquake", "end_event_type": "tsunami"},
        ["links", "families", "count"], ["get_disaster_links_for_event"], ["published link family", "method"]
    ),
    "get_earthquake_events": _g(
        ["You need canonical enriched earthquake history with stable loc_id geography."],
        ["Preliminary live-only events", "Unbounded global scans"],
        {"metrics": ["event_count"], "filters": {"time": {"start": "2020-01-01", "end": "2020-12-31"}, "region_ids": ["USA"]}},
        ["rows", "row_count", "source_id", "provenance"], ["get_live_earthquake_events", "get_pack"], ["source", "canonical window", "last_updated"]
    ),
    "get_live_earthquake_events": _g(
        ["You explicitly need recent preliminary USGS earthquake events beyond the canonical window."],
        ["Canonical historical analysis"],
        {"hours": 24, "min_magnitude": 4, "limit": 20},
        ["events", "row_count", "fetched_at"], ["get_earthquake_events"], ["upstream URL", "fetch time"]
    ),
    "get_volcanic_activity": _g(
        ["You need canonical historical eruption records or VEI metrics."],
        ["Recent preliminary upstream updates"],
        {"metrics": ["event_count"], "filters": {"time": {"start": 2000, "end": 2020}}},
        ["rows", "row_count", "source_id", "provenance"], ["get_live_volcano_events", "get_pack"], ["source", "canonical window"]
    ),
    "get_live_volcano_events": _g(
        ["You explicitly need recent preliminary Smithsonian/GVP eruption updates."],
        ["Canonical historical eruption analysis"],
        {"days": 30, "limit": 20}, ["events", "row_count", "fetched_at"],
        ["get_volcanic_activity"], ["upstream URL", "fetch time"]
    ),
    "get_tsunami_events": _g(
        ["You need canonical historical tsunami records, counts, or runup metrics."],
        ["Unbounded scans", "Live warning data"],
        {"metrics": ["event_count"], "filters": {"time": {"start": 2000, "end": 2020}, "region_ids": ["JPN"]}},
        ["rows", "row_count", "source_id", "provenance"], ["get_pack"], ["source", "canonical window"]
    ),
    "get_fx_rates": _g(
        ["You need USD-normalized daily, weekly, or monthly FX history."],
        ["Current trading quotes", "Non-country geography"],
        {"filters": {"region_ids": ["CAN"], "time": {"start": "2024-01-01", "end": "2024-12-31", "granularity": "monthly"}}},
        ["rows", "row_count", "granularity", "provenance"], ["get_pack"], ["upstream sources", "last_updated"]
    ),
    "query_dataset": _g(
        ["You need a structured query against a published data pack or source."],
        ["Calling geography tool families", "Guessing metrics without get_pack", "Unbounded event scans"],
        {"pack_id": "currency", "metrics": ["local_per_usd"], "filters": {"region_ids": ["CAN"], "time": {"start": "2024-01-01", "end": "2024-01-31"}}},
        ["rows", "row_count", "source_id", "pack_id", "provenance"], ["get_pack"], ["source metadata", "release/freshness", "license"]
    ),
}


def geometry_family_help_payload(
    question: str | None = None,
    *,
    catalog_capabilities: dict[str, Any] | None = None,
) -> dict[str, Any]:
    capabilities = dict(catalog_capabilities or {})
    return {
        "ok": True,
        "tool_name": "how_geometry_works",
        "summary": "Start here before using DaedalMap geometry tools. The tools resolve coordinates and identifiers onto loc_id, inspect geography, and return bounded shape results.",
        "core_rule": "Learn the durable geometry model here, then read the live catalog for the selected country's current depths, families, and query guidance before constructing a large call.",
        "coverage": capabilities,
        "start_here": [
            {
                "step": 1,
                "tool": "read_geometry_catalog",
                "arguments": {"view": "capabilities", "country_scope": "<ISO3 when known>"},
                "purpose": "Read the selected country's current admin depth, available families, and query guidance. Omit country_scope for the concise global coverage model.",
            },
            {
                "step": 2,
                "tool": "get_tool_help",
                "arguments": {"tool_name": "<tool selected below>"},
                "purpose": "Read the exact input schema before constructing the call.",
            },
            {
                "step": 3,
                "purpose": "Use the country result and one of the workflows below; keep identifiers as strings.",
            },
        ],
        "concepts": {
            "loc_id": "The stable DaedalMap geography identifier shared by geometry and data tools.",
            "administrative_spine": {
                "rule": "Each country selects one complete, nested administrative hierarchy. Countries have different depths and native tier names.",
                "discovery": "Use read_geometry_catalog(view='capabilities', country_scope='<ISO3>') for the active depth and current query guidance.",
            },
            "reference_families": {
                "rule": "Postal areas, places, watersheds, electoral districts, Indigenous regions, weather zones, water bodies, and other families are independent reference systems unless the catalog says they belong to the selected spine.",
                "discovery": "Use the country's available_family_ids, then list_reference_systems(country_scope='<ISO3>') for the canonical published and callable crosswalk subset. Family coverage alone does not promise a conversion.",
            },
            "catalog_authority": "Coverage is generated from admitted releases. Do not assume that every country has the same depth, families, or physical query layout.",
            "query_cost": "Opening and searching geometry partitions is the main cold-path cost. Item count still matters, but calls aligned with the catalog's query guidance are usually faster than calls spread across unrelated regions or families.",
        },
        "request_rules": [
            {
                "request": "one exploratory point",
                "rule": "Call resolve_point in standard mode. It infers the country and returns the deepest available result through Admin 3 without opening deep partitions.",
            },
            {
                "request": "multiple administrative points",
                "rule": "Use standard mode for a cross-country batch; it performs global Admin 0 discovery and then opens only each discovered country's Admin 0-3 bank.",
            },
            {
                "request": "points at a partitioned deep level",
                "rule": "Use lookup_mode='deep' with exactly one country_scope and one admin_1_scope. First run standard mode, then split points by the returned Admin 1 loc_id.",
            },
            {
                "request": "geometry for known loc_ids",
                "rule": "Keep administrative loc_ids aligned with the country's query guidance. Keep independent reference-family loc_ids grouped by country and family; do not infer administrative ownership from overlap.",
            },
            {
                "request": "large or nationwide deep coverage",
                "rule": "Use the country catalog entry to enumerate the declared partition owners and make bounded MCP calls for one owner at a time.",
            },
        ],
        "interaction_contract": {
            "natural_language_owner": "calling_client_llm",
            "execution_input": "strict_json_schema",
            "per_tool_help": {"tool": "get_tool_help", "arguments": {"tool_name": "<exact tool name from tools/list>"}},
            "rules": [
                "The calling LLM interprets ordinary user language; deterministic geometry tools do not parse prose unless their schema explicitly accepts a question field.",
                "Use tools/list, then get_tool_help for the exact tool before an unfamiliar call.",
                "Preserve external identifiers as strings, especially when they have leading zeros.",
                "Inspect error.code, warnings, guidance, and clarification after a failed or ambiguous call.",
                "Ask the person only when clarification.required is true; otherwise repair the call directly.",
            ],
        },
        "workflows": [
            {
                "name": "coordinates_to_geography",
                "steps": ["resolve_point", "loc_id_info only when details are requested", "check_geometry then get_geometry only when shapes are requested"],
            },
            {
                "name": "country_scoped_administrative_points",
                "example": {
                    "tool": "resolve_point",
                    "arguments": {
                        "points": [{"lat": 45.039641, "lon": -103.313618}],
                        "country_scope": "USA",
                        "target_admin_level": 3,
                    },
                },
                "steps": ["confirm coverage", "send points from one country", "reuse the returned loc_id chain"],
            },
            {
                "name": "partitioned_deep_points_across_multiple_regions",
                "steps": [
                    "read the selected country's catalog entry and query_guidance",
                    "resolve to the declared partition-owner level",
                    "group points by the returned owner loc_id",
                    "call resolve_point separately for each owner group at the requested deeper level",
                ],
                "important": "Use only fields accepted by resolve_point. Grouping is expressed by putting one declared owner region's points in each call.",
            },
            {
                "name": "known_loc_ids_to_shapes",
                "steps": [
                    "separate administrative loc_ids from independent reference-family loc_ids",
                    "group them according to the country catalog entry and family",
                    "call check_geometry once per group",
                    "call get_geometry for the same group only when bbox, centroid, or polygon output is needed",
                ],
            },
            {
                "name": "known_or_suspected_dataset_identifiers",
                "steps": ["identify_reference_system on representative or all distinct string keys", "use the unambiguous geography_binding", "estimate_conversion_job", "create_conversion_job within the advertised hosted limit (7,500 rows by default)", "get_job_status to retrieve the completed result"],
            },
            {
                "name": "one_external_reference",
                "steps": ["list_reference_systems when support is unknown", "resolve_reference to loc_id", "convert_reference only when another external system is requested"],
            },
            {
                "name": "relationships_and_time",
                "steps": ["loc_id_info for identity/hierarchy/lifecycle", "compare_geographies for spatial overlap, validity, or successors"],
            },
        ],
        "available_tools": sorted(
            name for name in TOOL_GUIDANCE
            if name == "get_tool_help" or tool_profile(name).get("family") == "geography"
        ),
        "notes": [
            capabilities.get("public_claim") or "Coverage is read from geometry_catalog.json; do not hardcode a country list or depth.",
            "loc_id is the reserve geography identifier used by data packs and geometry tools.",
            "Known identifier crosswalks bypass point and polygon rediscovery.",
            "A mixed-vintage point chain is context, not strict stored parentage.",
            "Use bbox/centroid by default; full polygons are opt-in and more tightly bounded.",
            "Batch limits are safety ceilings, not a promise that unrelated partitions should be mixed in one call.",
            "Paying raises hosted throughput; it does not unlock a different geometry truth set.",
        ],
        "input_question": str(question or "").strip() or None,
    }


def tool_help_payload(
    tool_name: str,
    *,
    tool_definition: dict[str, Any],
    available_on_facades: list[str],
    effective_limits: dict[str, Any] | None = None,
    local_installed: bool = False,
) -> dict[str, Any]:
    name = str(tool_name or "").strip()
    guidance = deepcopy(TOOL_GUIDANCE.get(name) or {})
    profile = tool_profile(name)
    limits = {
        "free_item_limit": tool_free_item_limit(name),
        "paid_item_limit": tool_paid_item_limit(name),
        "inline_item_limit": tool_inline_item_limit(name),
    }
    limits = {key: value for key, value in limits.items() if value is not None}
    if effective_limits:
        limits.update({key: value for key, value in effective_limits.items() if value is not None})
    pricing = tool_pricing(name)
    access = {
        "pricing": pricing,
        "free_discovery": name in {"get_tool_help", "how_geometry_works", "get_catalog", "get_pack", "read_geometry_catalog", "list_reference_systems", "identify_dataset_geography", "identify_reference_system"},
        "limits": limits,
        "above_free_limit": "payment_required" if pricing.startswith("paid") else (
            "bounded_inline_limit_error" if name in {"create_geometry_export", "create_conversion_job"} else "typed_cap_error"
        ),
        "rate_limited_independently": True,
        "trusted_artifact_bypass": [
            "shared_route_call_limit",
            "per_tool_call_limit",
            "item_cap",
            "payment_challenge",
        ],
    }
    if local_installed:
        access.update({
            "access_lane": "local_installed",
            "rate_limited_independently": False,
            "service_item_caps_enforced": True,
            "payment_required": False,
            "resource_boundary": "local machine memory, disk, and process availability",
        })
    if name == "resolve_point" and not local_installed:
        access["caller_tiers"] = {
            "anonymous": {"included_items": limits.get("free_item_limit"), "above_limit": "payment_required"},
            "verified_account": {"included_items": limits.get("paid_item_limit"), "above_limit": "paid_export_or_dashboard"},
        }
        access["bulk_shape"] = {
            "threshold": limits.get("free_item_limit"),
            "standard_mode": "cross-country; global Admin0 discovery then Admin0-3 country banks only",
            "deep_mode": "exactly one country_scope and one admin_1_scope; target_admin_level also required above threshold",
            "deep_required_above_threshold": ["country_scope", "target_admin_level"],
            "cross_country_presets": ["global_admin_0", "global_admin_1"],
            "preset_field": "bulk_preset",
        }
    return {
        "ok": True,
        "tool_name": name,
        "title": tool_definition.get("title"),
        "purpose": tool_definition.get("description"),
        "input_schema": deepcopy(tool_definition.get("inputSchema") or {}),
        "interaction_contract": {
            "natural_language_owner": "calling_client_llm",
            "execution_input": "strict_json_schema",
            "rules": [
                "Translate the user's request into this tool's input_schema; do not pass prose unless the schema explicitly defines a natural-language question or query field.",
                "Preserve identifiers as strings when leading zeros or source formatting may matter.",
                "Do not invent pack ids, source ids, metric ids, filter fields, reference systems, loc_ids, or vintages; use discovery tools and returned contracts.",
                "On error, inspect error.code, warnings, guidance, and clarification before retrying.",
                "Ask the user only when clarification.required is true; otherwise correct the tool call without burdening the user.",
            ],
            "clarification_shape": {
                "required": "boolean",
                "reason": "stable_machine_code",
                "questions": [{
                    "id": "stable_answer_id",
                    "prompt": "one concise user-facing question",
                    "answer_schema": "JSON Schema for the answer",
                    "maps_to": "target argument path when directly mappable",
                }],
            },
        },
        **guidance,
        "access": access,
        "capability_id": profile.get("capability_id"),
        "family": profile.get("family"),
        "available_on_facades": sorted(set(available_on_facades)),
    }


def validate_tool_guidance(tool_names: set[str]) -> list[str]:
    errors: list[str] = []
    missing = sorted(tool_names - set(TOOL_GUIDANCE))
    extra = sorted(set(TOOL_GUIDANCE) - tool_names)
    if missing:
        errors.append("missing guidance: " + ", ".join(missing))
    if extra:
        errors.append("guidance for unpublished tools: " + ", ".join(extra))
    for name in sorted(tool_names & set(TOOL_GUIDANCE)):
        guidance = TOOL_GUIDANCE[name]
        for field in ("use_when", "do_not_use_for", "examples", "important_output_fields", "recommended_next_calls", "provenance_fields"):
            if field not in guidance or not isinstance(guidance[field], list):
                errors.append(f"{name}: {field} must be a list")
        if not guidance.get("use_when") or not guidance.get("do_not_use_for") or not guidance.get("examples"):
            errors.append(f"{name}: blind-caller guidance is incomplete")
    return errors


def validate_guidance_examples(tool_definitions: list[dict[str, Any]]) -> list[str]:
    """Catch stale top-level example arguments when a tool schema changes."""
    errors: list[str] = []
    definitions = {str(item.get("name") or ""): item for item in tool_definitions}
    for name, guidance in sorted(TOOL_GUIDANCE.items()):
        definition = definitions.get(name)
        if definition is None:
            continue
        schema = definition.get("inputSchema") or {}
        properties = set((schema.get("properties") or {}).keys())
        required = set(schema.get("required") or ())
        for index, example in enumerate(guidance.get("examples") or (), start=1):
            if not isinstance(example, dict):
                errors.append(f"{name}: example {index} must be an object")
                continue
            unknown = sorted(set(example) - properties)
            missing = sorted(required - set(example))
            if unknown and schema.get("additionalProperties") is False:
                errors.append(f"{name}: example {index} has unknown arguments: {', '.join(unknown)}")
            if missing:
                errors.append(f"{name}: example {index} is missing required arguments: {', '.join(missing)}")
    return errors
