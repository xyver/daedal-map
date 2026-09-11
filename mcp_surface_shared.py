from __future__ import annotations

from pack_pricing_shared import FREE_PACK_IDS, PAID_PACK_IDS

def _pack_id_description() -> str:
    return "Pack identifier from get_catalog. Newly catalog-admitted packs require no MCP schema change."


def _query_props() -> dict:
    return {
        "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
        "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return."},
        "filters": {"type": "object", "description": "Structured filters including time ranges, region_ids, and compare clauses."},
        "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
        "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return."},
        "output": {"type": "object", "description": "Optional output controls such as response format hints."},
    }


def _query_tool(name: str, title: str, description: str, required: list[str]) -> dict:
    return {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": dict(_query_props()),
            "required": list(required),
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    }


def build_mcp_instructions(*, safety_notice: str | None = None) -> str:
    free = ", ".join(sorted(FREE_PACK_IDS))
    paid = ", ".join(sorted(PAID_PACK_IDS))
    base = (
        f"Geospatial data MCP server. Free packs: {free}. Paid packs: {paid} "
        "(x402 Base USDC). The calling LLM translates the user's natural-language request into strict tool JSON; execution tools do not parse prose. Start with get_catalog, then get_pack before querying a new pack. Call get_tool_help before an unfamiliar tool. On a typed error, preserve the user's intent, inspect error/guidance/clarification, correct the arguments from the schema, and ask the user only when clarification.required is true."
    )
    if safety_notice:
        return f"{base} Safety: {safety_notice}"
    return base


def build_tool_definitions() -> list[dict]:
    return [
        {
            "name": "get_tool_help",
            "title": "Get Tool Help",
            "description": "Free blind-caller guidance for one tool visible on this MCP facade. Returns when to use it, what it refuses, a working example, effective access limits, important outputs, provenance fields, recommended next calls, and the shared natural-language-to-strict-JSON interaction contract. Use tools/list to discover names, then call this before an unfamiliar tool.",
            "inputSchema": {
                "type": "object",
                "properties": {"tool_name": {"type": "string", "description": "Exact tool name from tools/list."}},
                "required": ["tool_name"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "how_geometry_works",
            "title": "How Geometry MCP Works",
            "description": "Free starting guide for the DaedalMap geography/geometry MCP. Call this first to learn the durable loc_id, administrative-spine, reference-family, discovery, point-lookup, and bounded-shape concepts. Then read the live geometry catalog for country-specific depths, families, and query guidance, and use get_tool_help for one exact tool.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Optional natural-language question about how to use the geometry tool family."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_catalog",
            "title": "Get Catalog",
            "description": "Free discovery. Returns the list of live agent-ready data packs available on DaedalMap.",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_pack",
            "title": "Get Pack",
            "description": "Free discovery. Returns detailed metadata, coverage, freshness, preferred canonical tool guidance, and first-query examples for one pack. Call this before querying a new pack so you can see time shape, coverage limits, and the paste-ready first query.",
            "inputSchema": {
                "type": "object",
                "properties": {"pack_id": {"type": "string", "description": _pack_id_description()}},
                "required": ["pack_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_disaster_links_for_event",
            "title": "Get Disaster Links For Event",
            "description": "Free linked-disaster helper. Resolves one exact disaster event id into its published related-disaster links. Use this only when you already have an exact event id from a supported pack such as earthquakes, tsunamis, volcanoes, or wildfires.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "string", "description": "Exact disaster event id from a supported pack row, such as 'NOAA-SIG-2' or 'USA-CA-FIRE-215'."},
                    "pack_id": {"type": "string", "description": "Optional pack id hint when the event id is ambiguous. Supported exact-event link packs are earthquakes, tsunamis, volcanoes, and wildfires."},
                    "cross_type_only": {"type": "boolean", "description": "When true, only return cross-hazard links. Default true."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["event_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_disaster_link_chain",
            "title": "Get Disaster Link Chain",
            "description": "Free linked-disaster helper. Expands one exact disaster event id into a bounded related-event chain. Use this only when you already have an exact event id from a supported pack such as earthquakes, tsunamis, volcanoes, or wildfires.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "string", "description": "Exact disaster event id from a supported pack row, such as 'NOAA-SIG-2' or 'USA-CA-FIRE-215'."},
                    "pack_id": {"type": "string", "description": "Optional pack id hint when the event id is ambiguous. Supported exact-event link packs are earthquakes, tsunamis, volcanoes, and wildfires."},
                    "depth": {"type": "integer", "minimum": 1, "maximum": 2, "description": "Maximum link-chain depth to traverse. Default 1."},
                    "cross_type_only": {"type": "boolean", "description": "When true, only return cross-hazard links. Default true."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["event_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "search_disaster_links",
            "title": "Search Disaster Links",
            "description": "Free linked-disaster discovery helper. Searches published cross-disaster link families by event-type direction, optional via-event type, and optional year window. Use this when you want to discover whether a relationship family exists before you have an exact event id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "start_event_type": {"type": "string", "description": "Optional starting event type such as earthquake, hurricane, volcano, wildfire, flood, tornado, or tsunami."},
                    "via_event_type": {"type": "string", "description": "Optional intermediate event type for bounded chain discovery."},
                    "end_event_type": {"type": "string", "description": "Optional ending event type such as tsunami, flood, tornado, or earthquake."},
                    "year_start": {"type": "integer", "description": "Optional inclusive starting year filter."},
                    "year_end": {"type": "integer", "description": "Optional inclusive ending year filter."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Maximum number of matching chains to return. Default 10."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_point",
            "title": "Resolve Point to loc_id",
            "description": "Compact reverse geocoding. Converts one WGS84 point, or a bounded point list, into the latest-available administrative loc_id chain. Each chain row is intentionally small: loc_id, name, admin level, and vintage when available. This tool does not return polygons, hierarchy analysis, references, overlap percentages, lifecycle, provenance, or release-conversion detail. Pass the returned stack loc_ids to loc_id_info for details; use get_geometry for shapes and compare_geographies for relationships. Small exploratory calls may omit scope and resolve through the deepest served tier. Batches above the 25-point preview must declare exactly one country_scope and one target_admin_level; split multi-country input into one call per country. Cross-country admin-0/admin-1 batches may instead use bulk_preset. Anonymous callers pay above 25, while verified accounts receive included bulk throughput through 10,000 points.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "minimum": -90, "maximum": 90, "description": "Latitude in WGS84 decimal degrees."},
                    "lon": {"type": "number", "minimum": -180, "maximum": 180, "description": "Longitude in WGS84 decimal degrees."},
                    "points": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "lat": {"type": "number", "minimum": -90, "maximum": 90, "description": "Latitude in WGS84 decimal degrees."},
                                "lon": {"type": "number", "minimum": -180, "maximum": 180, "description": "Longitude in WGS84 decimal degrees."},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller row identifier echoed in the result."},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller point identifier echoed in the result."},
                            },
                            "required": ["lat", "lon"],
                            "additionalProperties": False,
                        },
                        "description": "Points to resolve. Up to 25 may be exploratory. Above 25, country_scope and target_admin_level are required. Anonymous callers receive a payment challenge; verified accounts have included throughput through 10,000 points.",
                    },
                    "target_admin_level": {
                        "anyOf": [{"type": "string"}, {"type": "integer"}],
                        "description": "Stopping level such as admin_0 through admin_5. Optional for up to 100 exploratory points and required for larger batches.",
                    },
                    "country_scope": {"type": "string", "description": "ISO3/admin_0 loc_id scope such as USA or CAN. Optional for up to 100 exploratory points and required for larger batches; every point must belong to this one country."},
                    "country_hint": {"type": "string", "description": "Alias for country_scope for clients that already use hint terminology."},
                    "bulk_preset": {"type": "string", "enum": ["global_admin_0", "global_admin_1"], "description": "Cross-country fast path that fixes the result level to admin_0 or admin_1. Use instead of country_scope; target_admin_level may be omitted."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id echoed in the result."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["lat", "lon"]},
                    {"required": ["points"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "loc_id_info",
            "title": "Get loc_id / Chain Details",
            "description": "The drill-down tool for loc_ids returned by resolve_point and other geography calls. Pass one loc_id, or pass the point result's stack loc_ids together, to retrieve metadata, strict stored parentage, shape status, vintage/lifecycle fields, and child counts. Historical records are returned as requested; when an evidenced successor exists, supersession separately asks whether the caller wants it and never substitutes or fetches it automatically. Set include_hierarchy for the strict same-release ancestor chain and include_references for external or side-chain crosswalks. This is where detailed chain explanation belongs; resolve_point intentionally stays compact. For exact polygons use get_geometry, and for overlap or successor analysis use compare_geographies. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id, e.g. 'USA-CA'."},
                    "loc_ids": {"type": "array", "items": {"type": "string"}, "description": "DaedalMap loc_ids to inspect together, including every loc_id from a resolve_point stack. Default public cap is deployment-configurable."},
                    "include_hierarchy": {"type": "boolean", "description": "When true, include strict stored parent and ancestor data. This never invents a parent edge across mixed releases. Default false."},
                    "include_references": {"type": "boolean", "description": "When true, include known external or side-chain references attached to each loc_id. Default false."},
                    "systems": {"type": "array", "items": {"type": "string"}, "description": "Optional reference systems to include when include_references is true, such as zcta, nws_fire, overlay_tribal, or overlay_nws_public_zone."},
                    "iso3": {"type": "string", "description": "Optional country hint for crosswalk artifacts. Defaults to the loc_id country when possible."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Admin level for crosswalk-backed reverse reference lookup. Inferred when omitted."},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum target-area share for reverse overlap references."},
                    "limit_per_system": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum overlap references to return per bridge/system. Default 10."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "read_geometry_catalog",
            "title": "Read Geometry Catalog",
            "description": "Free geography discovery. Reads the published DaedalMap geometry catalog projection by default, excluding staged and candidate work. Use view='capabilities' first for the global baseline and enhanced countries; use focused inventory views for families, banks, crosswalk products, and named objects. A local loopback MCP may set read_wip=true for internal review. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "enum": ["capabilities", "summary", "countries", "admin_coverage", "crosswalk_artifacts", "crosswalks", "products", "named_reference_objects", "full"],
                        "description": "Catalog view to return. Use capabilities for the concise first-user coverage model. Default summary for compatibility.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "description": "Maximum named reference objects to return. Default 50.",
                    },
                    "country_scope": {
                        "type": "string",
                        "description": "Optional ISO3 country code for view='capabilities'. Returns the selected country's baseline, active depth, families, and query guidance.",
                    },
                    "read_wip": {
                        "type": "boolean",
                        "description": "Local loopback MCP only. When true, reads the internal geometry catalog projection, including staged and in-progress records. Hosted/public MCP requests are denied. Default false.",
                    },
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "list_reference_systems",
            "title": "List Geographic Reference Systems",
            "description": "Free geography utility. Reads the canonical crosswalk registry and lists published, callable geographic reference systems, direct crosswalk artifacts, row counts, vintages, target levels, and source license metadata. Pass country_scope whenever the country is known. Call this first to learn whether ZIP/ZCTA, postal, census, electoral, watershed, health, tribal, marine, or other identifiers can be exchanged through loc_id. Public calls never expose WIP or relationship-only records. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "country_scope": {"type": "string", "description": "Optional ISO3 country filter. Use this for a focused country capability answer."},
                    "include_crosswalks": {"type": "boolean", "description": "Include actionable source-to-target crosswalk records. Default true."},
                    "read_wip": {"type": "boolean", "description": "Local loopback MCP only. Include staged or non-callable preprocessing records for operator review. Default false."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "identify_dataset_geography",
            "title": "Identify Dataset Geography",
            "description": "Free dataset-orchestration utility. Accepts bounded samples from plausible scalar columns and determines which column contains geography, then identifies its maintained reference system, country, and administrative level. The caller performs only structural parsing and sampling; it must not assign geographic meaning in advance. Returns ranked bindings for identifier columns or coordinate pairs. No geometry is loaded and no full dataset is retained. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "values": {"type": "array", "maxItems": 32, "items": {"type": "string"}},
                                "nonempty_count": {"type": "integer", "minimum": 0},
                            },
                            "required": ["name", "values"],
                            "additionalProperties": False,
                        },
                        "description": "Structurally filtered columns with deterministic representative scalar values. Do not pre-label their geographic system.",
                    },
                    "dataset_context": {
                        "type": "object",
                        "properties": {
                            "file_name": {"type": "string"},
                            "sheet_name": {"type": "string"},
                            "row_count": {"type": "integer", "minimum": 0},
                        },
                        "additionalProperties": False,
                    },
                    "country_scope": {"type": "string", "description": "Optional caller-declared ISO3 hint. Omit when unknown."},
                    "request_id": {"type": "string"},
                },
                "required": ["columns"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "identify_reference_system",
            "title": "Identify Geographic Reference System",
            "description": "Free geography utility. Checks a bounded sample of identifiers plus optional dataset/column context against maintained reference indexes and geometry banks. LLM clients must extract identifier values from the user's natural-language request and pass them as strings; do not put the prose question in the arguments, and preserve leading zeros. Use it when a caller is unsure which system or level their keys belong to. It returns one to three interpretations with confidence and preserves ambiguity until the user confirms one by retrying with expected.system. A caller who already knows the system can provide expected on the first call and receive a verified geography_binding directly. It does not convert the full dataset or return polygons. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "One geography identifier to inspect."},
                    "identifiers": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"},
                        "description": "A bounded representative identifier sample. Duplicate values are checked once. Values must be strings so leading zeros are preserved.",
                    },
                    "expected": {
                        "type": "object",
                        "properties": {
                            "system": {"type": "string", "description": "Caller-declared reference system, such as us_census_geoid, loc_id, zcta, or a catalog reference system. Supply it immediately when known, or after choosing an identification result."},
                            "geo_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Expected geography level, such as tract or admin_3."},
                            "vintage": {"type": "string", "description": "Expected source/reference vintage, such as 2020."},
                            "country_scope": {"type": "string", "description": "Expected ISO3 country scope."},
                        },
                        "additionalProperties": False,
                    },
                    "country_scope": {"type": "string", "description": "Optional ISO3 country hint used to narrow candidate banks."},
                    "dataset_context": {
                        "type": "object",
                        "description": "Bounded, non-row dataset clues used to rank plausible interpretations without replacing exact identifier verification.",
                        "properties": {
                            "file_name": {"type": "string"},
                            "sheet_name": {"type": "string"},
                            "column_name": {"type": "string"},
                            "column_names": {"type": "array", "items": {"type": "string"}, "maxItems": 1000},
                            "row_geography": {"type": "string", "description": "Optional plain-language clue such as county, tract, or ZIP area."},
                            "local_format_match_rate": {"type": "number", "minimum": 0, "maximum": 1},
                        },
                        "additionalProperties": False,
                    },
                    "validation_scope": {"type": "string", "enum": ["sample", "all_distinct_identifiers"], "description": "Describes whether the supplied identifiers are a sample or the complete distinct-key set. The tool validates every supplied identifier."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["identifier"]},
                    {"required": ["identifiers"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_reference",
            "title": "Resolve Reference to loc_id",
            "description": "Free geography utility. Converts one value, or a bounded list of values, from an external or adjacent geographic reference system into the DaedalMap loc_id universe. Examples: from_system='zip' value='00601'; from_system='nws_fire' value='AKZ317'; from_system='admin_boundary' value='Fairfax County'. Returns ranked loc_id matches with bridge vintage, overlap weights, and provenance where applicable. Historical references are returned as requested; an evidenced successor is a separate optional question and is never substituted automatically. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from_system": {"type": "string", "description": "Input reference system, such as loc_id, census_geoid/us_census_geoid, admin_boundary, zip, zcta, overlay_zcta, nws_zone, nws_fire, overlay_nws_fire_weather_zone, tribal, water_body, marine_eez, nuts, historical_country/iso3166_3, or a catalog family id."},
                    "value": {"type": "string", "description": "Identifier or name in the input system. Examples: 00601, USA-Z-00601, AKZ317, USA-NWSFZ-AKZ317, Fairfax County, Mediterranean Sea."},
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "from_system": {"type": "string", "description": "Input reference system for this row. Defaults to top-level from_system when omitted."},
                                "value": {"type": "string", "description": "Identifier or name in the input system."},
                                "iso3": {"type": "string", "description": "Optional country hint for this row."},
                                "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional admin target level for this row."},
                                "relationship_vintage": {"type": "string", "description": "Optional relationship vintage for this row."},
                                "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum area-share threshold for this row."},
                                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked matches for this row."},
                                "country_hint": {"type": "string", "description": "Optional country hint for admin/name resolution."},
                                "admin_level_hint": {"type": "integer", "minimum": 0, "maximum": 5, "description": "Optional admin-level hint for admin/name resolution."},
                                "as_of": {"type": "string", "description": "ISO date or year used to select a time-bounded identity assertion."},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller row identifier echoed in the result."},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller identifier echoed in the result."},
                            },
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                        "description": "Reference values to resolve in one call. Default public cap is deployment-configurable.",
                    },
                    "iso3": {"type": "string", "description": "Optional country hint for system-specific crosswalks. Omit for a globally scoped identifier system."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Admin target level for crosswalk-backed resolution. Default admin_2. Accepts admin_0..admin_5, 0..5, or names such as country, state, county, tract, block_group, or block."},
                    "relationship_vintage": {"type": "string", "description": "Optional relationship vintage to require, such as usa_geometry_current or census_2020_relationship_files."},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum area-share threshold for overlap matches."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked matches to return. Default 10."},
                    "country_hint": {"type": "string", "description": "Optional country hint for admin/name resolution."},
                    "admin_level_hint": {"type": "integer", "minimum": 0, "maximum": 5, "description": "Optional admin-level hint for admin/name resolution."},
                    "as_of": {"type": "string", "description": "ISO date or year used to select a time-bounded identity assertion, especially for historical names and codes."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["from_system", "value"]},
                    {"required": ["items"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "convert_reference",
            "title": "Convert Geographic Reference",
            "description": "Free geography utility. Converts one reference, or a bounded list of references, from one geographic reference system into another by resolving through DaedalMap loc_id: X -> loc_id -> Y. Use this for workflows like ZIP/ZCTA to NWS fire zones, NWS zone to counties, county to overlapping ZCTAs, or any future catalog-backed crosswalk. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from_system": {"type": "string", "description": "Input reference system, such as zip, overlay_zcta, nws_fire, tribal, admin_boundary, or loc_id."},
                    "value": {"type": "string", "description": "Identifier or name in the input system."},
                    "to_system": {"type": "string", "description": "Output reference system, such as loc_id, zcta, nws_fire, overlay_nws_public_zone, overlay_tribal, admin_local, or admin_geometry."},
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "from_system": {"type": "string", "description": "Input reference system for this row. Defaults to top-level from_system when omitted."},
                                "value": {"type": "string", "description": "Identifier or name in the input system."},
                                "to_system": {"type": "string", "description": "Output reference system for this row. Defaults to top-level to_system when omitted."},
                                "iso3": {"type": "string", "description": "Optional country hint for this row."},
                                "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional intermediate admin target level for this row."},
                                "relationship_vintage": {"type": "string", "description": "Optional relationship vintage for this row."},
                                "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum overlap share threshold for this row."},
                                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked output references for this row."},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller row identifier echoed in the result."},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller identifier echoed in the result."},
                            },
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                        "description": "Reference conversions to run in one call. Default public cap is deployment-configurable.",
                    },
                    "iso3": {"type": "string", "description": "Country hint for crosswalk artifacts. Default USA."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Admin level used as the intermediate crosswalk target. Default admin_2."},
                    "relationship_vintage": {"type": "string", "description": "Optional source relationship vintage to require."},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum overlap share threshold."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked output references to return. Default 10."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["from_system", "value", "to_system"]},
                    {"required": ["items"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "check_geometry",
            "title": "Check loc_id Geometry Availability",
            "description": "Fast shape-only preflight for one loc_id or a bounded loc_id list. Reports whether each exact identity has reusable geometry and its geometry vintage. Historical geometry remains the primary result; an evidenced current successor is only an explicit follow-up choice. It does not resolve points or explain other identity relationships. Use before get_geometry or an export. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id to check for available geometry."},
                    "loc_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "DaedalMap loc_ids to check for available geometry. Default public cap is deployment-configurable.",
                    },
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "compare_geographies",
            "title": "Compare Geographic Identities",
            "description": "Detailed relationship tool for two geographic identities. Returns temporal validity, N-way successor context, topology, geodesic intersection area, and directional overlap shares when approved geometry exists. Use this after a compact point lookup when the caller asks whether two tiers/releases really contain or overlap one another. A point-chain seam is not proof of strict parentage. Use resolve_reference first for names or outside identifiers. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "left_loc_id": {"type": "string", "description": "First DaedalMap loc_id."},
                    "right_loc_id": {"type": "string", "description": "Second DaedalMap loc_id."},
                    "as_of": {"type": "string", "description": "ISO date or year applied to both identities."},
                    "left_as_of": {"type": "string", "description": "Optional ISO date or year for the left identity; overrides as_of."},
                    "right_as_of": {"type": "string", "description": "Optional ISO date or year for the right identity; overrides as_of."},
                    "include_successors": {"type": "boolean", "description": "Include direct successors and present-day descendants for maintained historical identities. Default true."},
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "left_loc_id": {"type": "string"},
                                "right_loc_id": {"type": "string"},
                                "as_of": {"type": "string"},
                                "left_as_of": {"type": "string"},
                                "right_as_of": {"type": "string"},
                                "include_successors": {"type": "boolean"},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                            },
                            "required": ["left_loc_id", "right_loc_id"],
                            "additionalProperties": False,
                        },
                        "description": "Bounded geography pairs to compare in one call.",
                    },
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["left_loc_id", "right_loc_id"]},
                    {"required": ["items"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_geometry",
            "title": "Get loc_id Geometry",
            "description": "Shape retrieval for exact loc_ids, including loc_ids from any level of a resolve_point chain. Returns the requested geometry metadata, vintage, centroid, bounding box, and optional GeoJSON polygon. Historical geometry is returned first; an evidenced successor appears only as a separate question and is never substituted or fetched automatically. It does not explain hierarchy or crosswalks; use loc_id_info for those details. Prefer bbox/centroid unless exact rendering or clipping requires the polygon. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id, such as USA-CA-037, USA-Z-00601, USA-NWSFZ-AKZ317, EEZ-USA, or IHO1953-240001002."},
                    "loc_ids": {"type": "array", "items": {"type": "string"}, "description": "DaedalMap loc_ids to fetch in one call. Default public cap is deployment-configurable and lower when include_polygon is true."},
                    "include_polygon": {"type": "boolean", "description": "When true, include the full GeoJSON geometry. Default false."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_loc_id_scope",
            "title": "Resolve loc_id Scope",
            "description": "Strict hierarchy traversal. Given one stored parent loc_id and target admin level, returns descendants from that coherent parent chain. This is not the mixed-vintage latest-per-depth point resolver and must not bridge release seams. Use it before shape exports such as every county in a selected parent scope. No natural-language decoding is performed.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "parent_loc_id": {"type": "string", "description": "Parent DaedalMap loc_id, such as USA or CAN-BC."},
                    "admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Target level, such as admin_2, 2, county, or state."},
                    "scope": {
                        "type": "object",
                        "properties": {
                            "parent_loc_id": {"type": "string"},
                            "admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "bbox": {"anyOf": [{"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}, {"type": "string"}]},
                        },
                        "additionalProperties": False,
                    },
                    "bbox": {"anyOf": [{"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}, {"type": "string"}], "description": "Optional minLon,minLat,maxLon,maxLat filter."},
                    "limit": {"type": "integer", "minimum": 0, "maximum": 1000, "description": "Maximum rows to return inline. Counts are returned even when rows are truncated."},
                    "offset": {"type": "integer", "minimum": 0, "description": "Offset for preview paging."},
                    "count_only": {"type": "boolean", "description": "When true, return counts without loc_id rows."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["parent_loc_id", "admin_level"]},
                    {"required": ["scope"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "estimate_geometry_package",
            "title": "Estimate Geometry Package",
            "description": "Dry-run estimate for a selected geometry export: exact loc_id count, shape/vintage availability, bytes, delivery mode, citation requirements, and charge units. This estimates an export artifact, not a canonical DaedalMap geometry release bundle.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "Single loc_id to package."},
                    "loc_ids": {"type": "array", "items": {"type": "string"}, "description": "Explicit loc_ids to package."},
                    "scope": {
                        "type": "object",
                        "properties": {
                            "parent_loc_id": {"type": "string"},
                            "admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "bbox": {"anyOf": [{"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}, {"type": "string"}]},
                        },
                        "additionalProperties": False,
                    },
                    "format": {"type": "string", "enum": ["geojson", "geojson_gzip", "zip"], "description": "Implemented delivery format. Unsupported format names are rejected rather than silently returning another representation."},
                    "output_name": {"type": "string", "maxLength": 80, "description": "Optional safe base filename for the export."},
                    "include_polygon": {"type": "boolean", "description": "Estimate full shapes when true; metadata-only when false."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                    {"required": ["scope"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "create_geometry_export",
            "title": "Create Geometry Export",
            "description": "Creates a synchronous v0 geometry export from exact loc_ids or one strict scope as real GeoJSON, gzipped GeoJSON, or zipped GeoJSON. Hosted service default: 250 selected loc_ids, sized around a 10-20 second response budget and configurable by deployment. A direct local-runtime loopback caller has no service item cap. Use estimate_geometry_package or get_tool_help for the effective access lane.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "quote_id": {"type": "string", "description": "Quote id returned by estimate_geometry_package, when available."},
                    "loc_id": {"type": "string"},
                    "loc_ids": {"type": "array", "items": {"type": "string"}, "description": "Selected loc_ids. The default synchronous limit is 250; larger calls return a typed operational-limit response."},
                    "scope": {
                        "type": "object",
                        "properties": {
                            "parent_loc_id": {"type": "string"},
                            "admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "bbox": {"anyOf": [{"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}, {"type": "string"}]},
                        },
                        "additionalProperties": False,
                    },
                    "format": {"type": "string", "enum": ["geojson", "geojson_gzip", "zip"]},
                    "output_name": {"type": "string", "maxLength": 80},
                    "include_polygon": {"type": "boolean"},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                    {"required": ["scope"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": False},
        },
        {
            "name": "estimate_conversion_job",
            "title": "Estimate loc_id Conversion Job",
            "description": "Free dry-run quote for uploaded or pasted user data conversion. Estimates rows, sample resolvability, output bytes, errors, and charge units before execution.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from_system": {"type": "string", "description": "Input reference system for rows."},
                    "geography_binding": {
                        "type": "object",
                        "properties": {
                            "mode": {"type": "string", "enum": ["reference", "loc_id"]},
                            "system": {"type": "string", "description": "Declared identifier system. Used when from_system is omitted."},
                            "geo_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "vintage": {"type": "string"},
                            "id_column": {"type": "string", "description": "Identifier-column name for future artifact inputs; inline items continue to use value."},
                            "country_scope": {"type": "string"},
                        },
                        "required": ["system"],
                        "additionalProperties": False,
                        "description": "Known dataset-geography declaration. The estimate verifies it against distinct identifiers and avoids point containment.",
                    },
                    "to_system": {"type": "string", "description": "Optional output reference system. Omit to normalize to loc_id."},
                    "items": {"type": "array", "items": {"type": "object", "properties": {
                        "value": {"type": "string", "description": "Identifier value; keep as a string to preserve leading zeros."},
                        "data": {"type": "object", "maxProperties": 200, "additionalProperties": {"anyOf": [{"type": "string"}, {"type": "number"}, {"type": "integer"}, {"type": "boolean"}, {"type": "null"}]}, "description": "Original spreadsheet columns to preserve. Column names beginning daedalmap_ are reserved for generated output fields."},
                        "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                        "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                        "from_system": {"type": "string"}, "to_system": {"type": "string"}, "iso3": {"type": "string"},
                        "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                        "relationship_vintage": {"type": "string"}, "min_share": {"type": "number", "minimum": 0, "maximum": 1},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    }, "required": ["value"], "additionalProperties": False}, "description": "Sample or full rows; row-level fields may override top-level defaults."},
                    "row_count": {"type": "integer", "minimum": 0, "description": "Expected total row count when only a sample or artifact pointer is provided."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                    "iso3": {"type": "string"},
                    "relationship_vintage": {"type": "string"},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "output_format": {"type": "string", "enum": ["json_rows", "csv", "jsonl", "parquet"], "description": "Enriched-row output format. CSV is spreadsheet-friendly; Parquet is compact and typed; JSON Lines is stream-friendly."},
                    "output_name": {"type": "string", "maxLength": 80, "description": "Optional safe base filename."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["from_system", "items"]},
                    {"required": ["from_system", "row_count"]},
                    {"required": ["geography_binding", "items"]},
                    {"required": ["geography_binding", "row_count"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "create_conversion_job",
            "title": "Create loc_id Conversion Job",
            "description": "Creates a synchronous v0 user-data conversion job with preserved scalar fields and JSON rows, CSV, JSON Lines, or Parquet output. Hosted service default: 7,500 rows, tuned around a 10-20 second response budget. A direct local-runtime loopback caller has no service item cap; local machine resources are the boundary. Identifier deduplication keeps repeated geography keys efficient.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "quote_id": {"type": "string", "description": "Quote id returned by estimate_conversion_job, when available."},
                    "from_system": {"type": "string"},
                    "geography_binding": {
                        "type": "object",
                        "properties": {
                            "mode": {"type": "string", "enum": ["reference", "loc_id"]},
                            "system": {"type": "string"},
                            "geo_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "vintage": {"type": "string"},
                            "id_column": {"type": "string"},
                            "country_scope": {"type": "string"},
                        },
                        "required": ["system"],
                        "additionalProperties": False,
                    },
                    "to_system": {"type": "string", "description": "Optional output reference system. Omit to normalize to loc_id."},
                    "items": {"type": "array", "items": {"type": "object", "properties": {
                        "value": {"type": "string", "description": "Identifier value; keep as a string to preserve leading zeros."},
                        "data": {"type": "object", "maxProperties": 200, "additionalProperties": {"anyOf": [{"type": "string"}, {"type": "number"}, {"type": "integer"}, {"type": "boolean"}, {"type": "null"}]}, "description": "Original spreadsheet columns to preserve. Column names beginning daedalmap_ are reserved for generated output fields."},
                        "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                        "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                        "from_system": {"type": "string"}, "to_system": {"type": "string"}, "iso3": {"type": "string"},
                        "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                        "relationship_vintage": {"type": "string"}, "min_share": {"type": "number", "minimum": 0, "maximum": 1},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    }, "required": ["value"], "additionalProperties": False}, "description": "Rows to convert; row-level fields may override top-level defaults."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                    "iso3": {"type": "string"},
                    "relationship_vintage": {"type": "string"},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "output_format": {"type": "string", "enum": ["json_rows", "csv", "jsonl", "parquet"]},
                    "output_name": {"type": "string", "maxLength": 80},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["items"],
                "anyOf": [
                    {"required": ["from_system"]},
                    {"required": ["geography_binding"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": False},
        },
        {
            "name": "get_job_status",
            "title": "Get Geometry Job Status",
            "description": "Retrieves a completed bounded v0 geometry export or conversion job by job_id. The current public contract creates completed inline jobs only; durable queued jobs and downloadable artifact links remain a future Custom Data Builder capability.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job id returned by create_geometry_export or create_conversion_job."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_earthquake_events",
            "title": "Get Earthquake Events",
            "description": "Paid x402 canonical tool. Queries the published earthquakes_events lane. Use this first for earthquake questions because it is the enriched DaedalMap history lane with stable loc_id geography, not the preliminary upstream wrapper. Call without payment first - the server returns HTTP 402 with the exact USDC price before any charge. Small queries stay cheap; broad scans cost more or need narrower filters.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return, such as 'event_count' or event attributes like 'magnitude'."},
                    "filters": {"type": "object", "description": "Structured filters including time ranges, region_ids, and compare clauses."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return. For top-N requests, include a narrow time range or region_ids before sorting."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "required": ["metrics", "filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_live_earthquake_events",
            "title": "Get Live Earthquake Events",
            "description": "Free live wrapper. Calls the USGS FDSN API for recent preliminary earthquake events normalized to DaedalMap event fields. Use this only when the caller explicitly wants live/preliminary upstream results or needs a very recent window not yet present in the published canonical earthquake lane. This is not the enriched canonical history lane.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                    "hours": {"type": "integer", "minimum": 1, "maximum": 168, "description": "Recent lookback window in hours. Ignored when start_time is provided."},
                    "start_time": {"type": "string", "description": "Optional inclusive ISO-8601 start datetime."},
                    "end_time": {"type": "string", "description": "Optional exclusive-ish ISO-8601 end datetime. Defaults to now."},
                    "min_magnitude": {"type": "number", "description": "Minimum earthquake magnitude. Defaults to 2.5."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum live rows to return."},
                    "orderby": {"type": "string", "enum": ["time", "time-asc", "magnitude", "magnitude-asc"], "description": "USGS result ordering."},
                    "min_latitude": {"type": "number", "description": "Optional bounding box minimum latitude."},
                    "max_latitude": {"type": "number", "description": "Optional bounding box maximum latitude."},
                    "min_longitude": {"type": "number", "description": "Optional bounding box minimum longitude."},
                    "max_longitude": {"type": "number", "description": "Optional bounding box maximum longitude."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_volcanic_activity",
            "title": "Get Volcanic Activity",
            "description": "Free canonical tool. Queries volcanoes_events for historical eruption records and volcanic activity metrics. Best for eruption counts, VEI thresholds, and top-event lookups. Volcano queries normally use year-style time filters rather than ISO date strings.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return, such as 'event_count', 'VEI', or eruption attributes."},
                    "filters": {"type": "object", "description": "Structured filters including year-based time ranges, region_ids, and compare clauses. For most volcano queries, pass numeric years or time.value."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return. For top-N VEI or latest-eruption requests, include a narrow year range or region_ids before sorting."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "required": ["metrics", "filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_live_volcano_events",
            "title": "Get Live Volcano Events",
            "description": "Free live wrapper. Calls the Smithsonian/GVP WFS for recent preliminary volcanic eruption updates normalized to DaedalMap event fields. This is not the enriched canonical history lane.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "description": "Recent lookback window in days. Ignored when start_time is provided."},
                    "start_time": {"type": "string", "description": "Optional inclusive ISO-8601 start datetime or date."},
                    "end_time": {"type": "string", "description": "Optional inclusive ISO-8601 end datetime or date. Defaults to now."},
                    "min_vei": {"type": "number", "description": "Optional minimum Volcanic Explosivity Index."},
                    "ongoing_only": {"type": "boolean", "description": "When true, only return eruptions marked continuing by GVP."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum live rows to return."},
                    "orderby": {"type": "string", "enum": ["time", "time-asc", "vei", "vei-asc"], "description": "Result ordering."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_tsunami_events",
            "title": "Get Tsunami Events",
            "description": "Paid x402 canonical tool. Queries tsunamis_events for historical tsunami records and water-height/runup metrics. Best for event counts, max water height thresholds, and top-event lookups. Region filters may use ISO3 country ids or reviewed named-water loc_ids such as IHO1953-240001002 for the Mediterranean Sea; XOO is deprecated. Call without payment first - the server returns HTTP 402 with the exact USDC price before any charge.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return, such as 'event_count', 'max_water_height_m', or event attributes."},
                    "filters": {"type": "object", "description": "Structured filters including time ranges, region_ids, and compare clauses. Tsunami queries commonly use year-style windows and may use geometry-backed ocean/sea ids such as XSM."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return. For largest-wave or latest-event requests, include a narrow time range or region_ids before sorting."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "required": ["metrics", "filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_fx_rates",
            "title": "Get FX Rates",
            "description": "Free tool. Queries the currency pack using filters.region_ids plus filters.time.granularity to return daily, weekly, or monthly FX data.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Optional metric ids. Defaults to 'local_per_usd' for FX rate queries."},
                    "filters": {"type": "object", "description": "Structured filters including region_ids with loc_id country codes, time range, and granularity."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Maximum number of rows to return for the requested granularity and time span."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "required": ["filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "query_dataset",
            "title": "Query Dataset",
            "description": "Generic structured query for direct source_id or pack_id access using the same contract as POST /api/v1/query/dataset. Free packs: "
            + ", ".join(sorted(FREE_PACK_IDS))
            + ". Paid packs: "
            + ", ".join(sorted(PAID_PACK_IDS))
            + " (x402 Base USDC).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "source_id": {"type": "string", "description": "Concrete source id such as 'earthquakes_events', 'volcanoes_events', 'hurricanes_events', or 'un_sdg/01'."},
                    "pack_id": {"type": "string", "description": _pack_id_description()},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return. Use event_count for aggregate counts when supported."},
                    "filters": {"type": "object", "description": "Structured filters including time, region_ids, and compare clauses."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return for the requested source or pack."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
    ]
