from __future__ import annotations

from mcp_data_contract_shared import decorate_data_tool_definition, decorate_shared_help_definition
from mcp_discovery_shared import data_access_workflow

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
    flow = " -> ".join(step["tool"] for step in data_access_workflow()["steps"])
    base = (
        f"Geospatial data MCP server. Autonomous data workflow: {flow}. "
        "get_catalog is the current authority for "
        "available packs and each pack's free or paid access lane. The calling "
        "LLM translates the user's natural-language request into strict tool "
        "JSON; execution tools do not parse prose. Start with get_catalog, then "
        "get_pack before querying a new pack. Call get_tool_help before an "
        "unfamiliar tool. On a typed error, preserve the user's intent, inspect "
        "error/guidance/clarification, correct the arguments from the schema, "
        "and ask the user only when clarification.required is true."
    )
    if safety_notice:
        return f"{base} Safety: {safety_notice}"
    return base


def build_tool_definitions() -> list[dict]:
    definitions = [
        {
            "name": "get_tool_help",
            "title": "Get Tool Help",
            "description": "Free unified guidance. Pass tool_name for the exact contract of one visible tool, or topic for an overview and workflow guidance. topic='geometry' contains the loc_id and geometry-family orientation formerly published as a separate helper.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool_name": {"type": "string", "description": "Exact tool name from tools/list. Do not combine with topic."},
                    "topic": {"type": "string", "enum": ["overview", "data", "disasters", "custom_data", "geometry"], "description": "Workflow overview. Do not combine with tool_name."},
                    "question": {"type": "string", "description": "Optional question used only with a topic overview."},
                },
                "oneOf": [
                    {"required": ["tool_name"], "not": {"required": ["topic"]}},
                    {"required": ["topic"], "not": {"required": ["tool_name"]}},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_catalog",
            "title": "Get Catalog",
            "description": "Free progressive discovery for the data or geometry catalog. detail='lite' lists concise pack coverage and topics, detail='full' adds metric/query inventories, and detail='download' returns the complete raw catalog URL. Select one pack, then call get_pack.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "enum": ["data", "geometry"], "default": "data", "description": "Catalog family. The geography facade defaults to geometry; other facades default to data."},
                    "detail": {"type": "string", "enum": ["lite", "full", "download"], "default": "lite", "description": "Use lite to select a pack, full for expanded metric/query inventories, or download for the complete raw catalog URL."},
                    "country_scope": {"type": "string", "description": "Optional ISO3 focus for catalog='geometry' with detail='lite'."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_pack",
            "title": "Get Pack",
            "description": "Free progressive metadata for one selected data pack or geometry tool family. detail='lite' returns bounded selection and starter-query fields, detail='full' returns detailed MCP query metadata, and detail='download' returns the complete raw metadata URL. Use its next_step to retrieve data.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "enum": ["data", "geometry"], "description": "Metadata family. When omitted, geometry facades default to geometry and known geometry-family ids are inferred; all other calls default to data."},
                    "pack_id": {"type": "string", "description": _pack_id_description()},
                    "detail": {"type": "string", "enum": ["lite", "full", "download"], "default": "lite", "description": "Use lite to decide and start, full for detailed MCP query metadata, or download for the complete raw metadata file."},
                },
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
            "title": "Resolve Point (Shallow)",
            "description": "Compact first-pass reverse geocoding for one WGS84 coordinate. Returns an administrative loc_id chain through Admin 3 without opening deep partitions or side-family shape banks. Use its deepest shallow loc_id with resolve_deep_point for Admin 4-6 or one explicit family. For multiple coordinates use resolve_points.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "minimum": -90, "maximum": 90, "description": "Latitude in WGS84 decimal degrees."},
                    "lon": {"type": "number", "minimum": -180, "maximum": 180, "description": "Longitude in WGS84 decimal degrees."},
                    "target_admin_level": {
                        "anyOf": [{"type": "string"}, {"type": "integer"}],
                        "description": "Optional exact requested level from Admin 0-3. Omit it to return the deepest available shallow level.",
                    },
                    "include_marine_context": {"type": "boolean", "description": "Include parallel Marine overlaps for land matches. Defaults to true; set false for fast administrative loc_id previews. Offshore Marine fallback still applies."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["lat", "lon"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_points",
            "title": "Resolve Points (Shallow Bulk)",
            "description": "Bulk first-pass reverse geocoding through Admin 3. Accepts a bounded cross-country WGS84 point array and never opens deep partitions or side-family shape banks. Group results by shallow scope before calling resolve_deep_points once per scope and family.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "points": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "lat": {"type": "number", "minimum": -90, "maximum": 90},
                                "lon": {"type": "number", "minimum": -180, "maximum": 180},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                            },
                            "required": ["lat", "lon"],
                            "additionalProperties": False,
                        },
                    },
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional exact Admin 0-3 level."},
                    "include_marine_context": {"type": "boolean", "description": "Include parallel Marine overlaps. Defaults to true."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id echoed in the result."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["points"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_deep_point",
            "title": "Resolve Point (Deep)",
            "description": "Second-pass resolution for one WGS84 coordinate. Supply a shallow_loc_id returned by resolve_point and one canonical family from read_geometry_catalog. family defaults to administrative; shape-backed families use direct bbox-to-exact-shape lookup without crosswalks. For multiple coordinates use resolve_deep_points.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "minimum": -90, "maximum": 90, "description": "Latitude in WGS84 decimal degrees."},
                    "lon": {"type": "number", "minimum": -180, "maximum": 180, "description": "Longitude in WGS84 decimal degrees."},
                    "shallow_loc_id": {"type": "string", "minLength": 5, "description": "The deepest canonical Admin 1-3 loc_id returned by resolve_point, such as USA-NY-061-009903."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional exact Admin 4-6 level. Omit for the deepest available match."},
                    "family": {"type": "string", "pattern": "^[a-z0-9_]+$", "default": "administrative", "description": "One family selector. Defaults to administrative; use marine for the direct Marine resolver, or a canonical country family such as postal_area, watershed, or land_management_region."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["lat", "lon", "shallow_loc_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_deep_points",
            "title": "Resolve Points (Deep Bulk)",
            "description": "Bulk second-pass resolution for one family. Supply a bounded WGS84 point array, one shared shallow_loc_id scope, and one canonical family. family defaults to administrative; other shape-backed families use direct bbox candidates followed by exact containment without crosswalks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "points": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 100,
                        "items": {
                            "type": "object",
                            "properties": {
                                "lat": {"type": "number", "minimum": -90, "maximum": 90},
                                "lon": {"type": "number", "minimum": -180, "maximum": 180},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                            },
                            "required": ["lat", "lon"],
                            "additionalProperties": False,
                        },
                        "description": "Points already known to fall within the supplied shallow_loc_id scope. Maximum 100 per call during the initial technical rollout.",
                    },
                    "shallow_loc_id": {"type": "string", "minLength": 5, "description": "A canonical Admin 1-3 loc_id returned by resolve_points. All coordinates in the call must belong to its scope."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional exact Admin 4-6 level. Omit for the deepest available match."},
                    "family": {"type": "string", "pattern": "^[a-z0-9_]+$", "default": "administrative", "description": "One canonical family ID for the entire batch."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id echoed in the result."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["points", "shallow_loc_id"],
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
            "description": "Free compact geography discovery. Reads a small published projection and excludes staged or candidate work. Use view='capabilities' with country_scope to learn available families. Focused views return bounded inventory detail. Public view='full' redirects to the downloadable catalog instead of placing the raw catalog in an MCP response; local loopback may use read_wip=true for operator review. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "enum": ["capabilities", "summary", "countries", "admin_coverage", "crosswalk_artifacts", "crosswalks", "products", "named_reference_objects", "full"],
                        "description": "Catalog view to return. Capabilities is the compact default and first-user coverage model. Public full returns the bulk-download location rather than embedding the raw catalog.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "description": "Maximum named reference objects to return. Default 50.",
                    },
                    "country_scope": {
                        "type": "string",
                        "description": "Optional ISO3 country code for view='capabilities'. Returns active depth, published families, and query guidance.",
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
            "description": "Free reference-system discovery. By default returns a compact list of published systems and whether each can exchange through loc_id. Pass country_scope whenever known. Set include_crosswalks=true only after selecting a country or system and needing bridge rows, vintages, counts, or license metadata. Geometry families themselves are listed by read_geometry_catalog. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "country_scope": {"type": "string", "description": "Optional ISO3 country filter. Use this for a focused country capability answer."},
                    "include_crosswalks": {"type": "boolean", "description": "Include actionable crosswalk and artifact records. Default false because these records are much larger than the system index."},
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
            "description": "Free dataset-orchestration utility. Accepts bounded samples from plausible scalar columns and determines which column contains geography, then identifies its maintained reference system, country, and administrative level. The caller performs only structural parsing and sampling; it must not assign geographic meaning in advance. Returns up to three reviewable bindings at 60% confidence or better, plus a coordinate fallback when present; only an unambiguous result at 80% or better is recommended automatically. No geometry is loaded and no full dataset is retained. No payment required.",
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
                    "validation_scope": {"type": "string", "enum": ["sample", "all_distinct_identifiers"], "description": "Describes whether the bounded input is a sample or, only when it fits the identification cap, the complete distinct-key set. This tool validates every supplied identifier; create_conversion_job validates every row in the full dataset."},
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
            "description": "Availability check and bounded shape retrieval for exact loc_ids or one administrative scope. Exact selection accepts loc_id or loc_ids. Scope selection accepts parent_loc_id plus admin_level and uses the optimized Admin Spine layout: Admin 0-3 stays on one national bank, while deeper levels require an Admin 1 parent and stay on one deep partition. The default response is the fast preflight: it projects has_shape, shape metadata, centroid, and bounding box without reading polygon coordinates. Set include_polygon=true only when exact coordinates are needed. Independent geometry families are selected by exact loc_ids, not inferred as administrative descendants. Use loc_id_info for hierarchy or crosswalk details. Historical geometry is returned first and successors are never substituted automatically. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id, such as USA-CA-037, USA-Z-00601, USA-NWSFZ-AKZ317, EEZ-USA, or IHO1953-240001002."},
                    "loc_ids": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string"}, "description": "Exact DaedalMap loc_ids to fetch in one call. Default public cap is deployment-configurable and lower when include_polygon is true."},
                    "scope": {
                        "type": "object",
                        "properties": {
                            "parent_loc_id": {"type": "string", "description": "Administrative parent loc_id, such as USA-TX or CAN-BC."},
                            "admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Descendant level to retrieve, such as admin_2, 2, or county."},
                            "bbox": {"anyOf": [{"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}, {"type": "string"}], "description": "Optional minLon,minLat,maxLon,maxLat intersection filter."},
                        },
                        "required": ["parent_loc_id", "admin_level"],
                        "additionalProperties": False,
                    },
                    "include_polygon": {"type": "boolean", "description": "When true, include the full GeoJSON geometry. Default false."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "oneOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                    {"required": ["scope"]},
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
            "description": "Free dry-run quote for uploaded or pasted user data conversion. Checks a fixed representative sample of up to 32 supplied rows through the real conversion resolver, then estimates total resolvable rows, output bytes, errors, and charge units. row_count is the full dataset size and is not replaced by the sample size. Conversion execution validates every submitted row and reports structured failures for unmatched keys. For a coordinate file, set geography_binding.mode to coordinates and send point_count (valid coordinate pairs counted locally), row_count, and request_id instead of items: no point is resolved, the quote is the ceiling for exactly those points, and resolve_points called with the same request_id reuses its quote_id. Quotes are maximums; execution charges only successfully resolved rows or points.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from_system": {"type": "string", "description": "Input reference system for rows."},
                    "geography_binding": {
                        "type": "object",
                        "properties": {
                            "mode": {"type": "string", "enum": ["reference", "loc_id", "coordinates"]},
                            "system": {"type": "string", "description": "Declared identifier system. Used when from_system is omitted. Not used for mode coordinates."},
                            "geo_level": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                            "vintage": {"type": "string"},
                            "id_column": {"type": "string", "description": "Identifier-column name for future artifact inputs; inline items continue to use value."},
                            "country_scope": {"type": "string"},
                        },
                        "additionalProperties": False,
                        "description": "Known dataset-geography declaration. Identifier modes require system; the estimate verifies it against distinct identifiers and avoids point containment. Mode coordinates quotes point resolution from point_count.",
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
                    }, "required": ["value"], "additionalProperties": False}, "description": "Representative sample or full rows; the estimate resolves at most the first 32. Row-level fields may override top-level defaults."},
                    "row_count": {"type": "integer", "minimum": 0, "description": "Expected total row count when only a sample or artifact pointer is provided."},
                    "point_count": {"type": "integer", "minimum": 0, "description": "Coordinate mode only: rows with a valid latitude/longitude pair. The quote ceiling; blank or invalid rows are excluded and never charged."},
                    "batch_id": {"type": "string", "description": "Coordinate mode only: batch id the resolve_points run will send."},
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
                    {"required": ["geography_binding", "point_count", "request_id"]},
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
            "name": "get_data",
            "title": "Get Data",
            "description": "The single workhorse for published data packs inside the loc_id universe. After get_pack, pass its pack_id, exact metric ids, structured filters, sort, and row limit. filters.region_ids accepts canonical loc_ids; an administrative parent such as USA-TX matches descendant rows at the pack's published geography grain. Use resolution/onboarding tools before get_data when the input is coordinates, outside codes, names, or an unidentified user column. Pack metadata owns source routing, time grain, geography, access, and pack-specific rules; callers do not choose internal source_id values. Geometry tool families use their focused next_step tools instead of this row-query contract.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "pack_id": {"type": "string", "description": _pack_id_description()},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metric ids to return. Use event_count for aggregate counts when supported."},
                    "filters": {"type": "object", "description": "Structured filters including time, region_ids, and compare clauses."},
                    "sort": {"anyOf": [{"type": "array"}, {"type": "object"}], "description": "Optional sort instructions for row-returning queries."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return for the requested source or pack."},
                    "output": {"type": "object", "description": "Optional output controls such as response format hints."},
                },
                "required": ["pack_id", "metrics", "filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
    ]
    return [
        decorate_shared_help_definition(decorate_data_tool_definition(definition))
        for definition in definitions
    ]
