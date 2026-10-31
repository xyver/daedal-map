from __future__ import annotations

from mcp_data_contract_shared import decorate_data_tool_definition, decorate_shared_help_definition
from mcp_discovery_shared import data_access_workflow
from mcp_geometry_contract_shared import decorate_geometry_tool_definition


MARINE_JURISDICTION_FAMILIES = (
    "exclusive_economic_zone",
    "territorial_seas",
    "contiguous_zones",
    "extended_continental_shelves",
    "internal_waters",
    "archipelagic_waters",
    "high_seas",
)
MARINE_POINT_FAMILIES = (*MARINE_JURISDICTION_FAMILIES, "water_body")


def public_geometry_family_id(family: str | None, subtype: str | None = None) -> str | None:
    """Map an identity/storage label to the shared public family vocabulary."""
    if family == "marine_jurisdiction":
        return subtype if subtype in MARINE_JURISDICTION_FAMILIES else None
    if isinstance(family, str) and family.startswith("admin_"):
        return "administrative"
    return family

def _pack_id_description() -> str:
    return "Pack identifier from get_catalog. Newly catalog-admitted packs require no MCP schema change."


def _data_filters_schema() -> dict:
    scalar_or_list = {
        "anyOf": [
            {"type": ["string", "number", "integer", "boolean", "null"]},
            {"type": "array", "items": {"type": ["string", "number", "integer", "boolean"]}},
        ]
    }
    return {
        "type": "object",
        "properties": {
            "region_ids": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": {"type": "string", "minLength": 1},
                "description": "Canonical country or hierarchical loc_ids from get_pack, such as JPN or USA-TX.",
            },
            "time": {
                "type": "object",
                "properties": {
                    "value": {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "string", "format": "date"}, {"type": "integer"}]},
                    "start": {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "string", "format": "date"}, {"type": "integer"}]},
                    "end": {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "string", "format": "date"}, {"type": "integer"}]},
                    "granularity": {"type": "string", "enum": ["daily", "weekly", "monthly", "yearly", "timestamp"]},
                },
                "anyOf": [
                    {"required": ["value"]},
                    {"required": ["start"]},
                    {"required": ["end"]},
                ],
                "additionalProperties": False,
                "description": "Inclusive ISO-8601 date/datetime or integer-year selection. Use YYYY-MM-DD, not partial YYYY-MM strings.",
            },
            "equals": {
                "type": "object",
                "additionalProperties": scalar_or_list,
                "description": "Exact field/value filters using filterable fields published by get_pack.",
            },
            "compare": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string", "minLength": 1},
                        "op": {"type": "string", "enum": ["=", "!=", ">", ">=", "<", "<="]},
                        "value": {},
                    },
                    "required": ["field", "op", "value"],
                    "additionalProperties": False,
                },
                "description": "Threshold filters using filterable fields or selected metric ids published by get_pack.",
            },
        },
        "additionalProperties": False,
    }


def _data_sort_schema() -> dict:
    item = {
        "type": "object",
        "properties": {
            "field": {"type": "string", "minLength": 1},
            "direction": {"type": "string", "enum": ["asc", "desc"]},
        },
        "required": ["field"],
        "additionalProperties": False,
    }
    return {
        "anyOf": [item, {"type": "array", "items": item}],
        "description": "Sort by a sortable field from get_pack; direction defaults to asc.",
    }


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
        "unfamiliar tool. When the only place input is coordinates, call "
        "resolve_point with one explicit scope_id and family. Use GLOBAL "
        "administrative to identify the country, then call the chosen ISO3 "
        "administrative scope for a shallow loc_id. Use MARINE with one "
        "selected Marine family for Marine-only containment. Never infer a "
        "Marine fallback from a Global miss. Only resolve_point "
        "and resolve_deep_point accept coordinates; use the latter only with "
        "a returned shallow_loc_id and the original point. Use "
        "convert_reference for scoped names or outside "
        "identifiers. identify_dataset_geography and identify_reference_system "
        "prepare an uncertain dataset binding before conversion. Keep all "
        "relevant returned loc_ids when place identity overlaps. Pass canonical "
        "loc_ids to geography readers and place-filtered data calls. "
        "On a typed error, preserve the user's intent, inspect "
        "error/guidance/clarification, correct the arguments from the schema, "
        "and ask the user only when clarification.required is true."
    )
    if safety_notice:
        return f"{base} Safety: {safety_notice}"
    return base


PAUSED_PUBLIC_TOOL_NAMES = frozenset({
    "get_live_earthquake_events",
    "get_live_volcano_events",
    # Retained for the future custom-data/bulk-export product, but hidden from
    # ordinary public MCP discovery and execution until that workflow has a
    # durable queue/artifact lifecycle.
    "resolve_loc_id_scope",
    "estimate_geometry_package",
    "create_geometry_export",
    "estimate_conversion_job",
    "create_conversion_job",
    "get_job_status",
})


def build_tool_definitions(*, include_paused: bool = False) -> list[dict]:
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
            "title": "List Published Packs or Geographies",
            "description": "Choose data or geometry. Data returns pack IDs and brief labels. Geometry returns family IDs and scope IDs from the supplied active coverage catalogs; missing catalogs return an error. Use get_pack for a data pack or get_geography with two of location, family, and year to discover the third.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "enum": ["data", "geometry"], "description": "Required directory selector."},
                },
                "required": ["catalog"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_pack",
            "title": "Inspect One Data Pack",
            "description": "After get_catalog(data), supply one exact data pack_id. Returns its metrics, time coverage, filters, and access metadata. Geometry families and scopes use get_geography.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pack_id": {"type": "string", "description": _pack_id_description()},
                },
                "required": ["pack_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_geography",
            "title": "Discover or Read Geography Coverage",
            "description": "Supply any two of family_id, scope_id, and year to list values of the third from the supplied active coverage catalog. Supply all three to read a bounded page from the explicitly configured active Geometry source. An absent catalog, catalog cell, or payload member returns an error. This tool does not search releases or substitute another year. page, page_size, and include_geometry are transport options.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "family_id": {"type": "string", "minLength": 1},
                    "scope_id": {"type": "string", "minLength": 3},
                    "year": {"type": "integer", "minimum": 1, "maximum": 9999},
                    "page": {"type": "integer", "minimum": 0},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 10},
                    "include_geometry": {"type": "boolean"},
                },
                "anyOf": [{"required": ["family_id"]}, {"required": ["scope_id"]}],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_event",
            "title": "Get Event",
            "description": "Retrieve one exact disaster event. Pass the same pack_id used for get_data, its result's source_id, and one exact event_id from its row. The default returns event identity and core attributes without heavy shape columns. Add relationships, affected_places, observations, or geometry only when needed. Invalid routing inputs return an error; the tool queries one declared event source once. Use get_pack to inspect published relationship families.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "string", "minLength": 1, "description": "Exact stable event_id returned by a disaster-pack get_data row."},
                    "pack_id": {"type": "string", "minLength": 1, "description": "Required disaster pack_id that returned this event_id from get_data. The tool never searches other packs."},
                    "source_id": {"type": "string", "minLength": 1, "description": "Required source_id from the same get_data result. Must identify one published event source in pack_id."},
                    "include": {
                        "type": "array",
                        "uniqueItems": True,
                        "items": {"type": "string", "enum": ["relationships", "affected_places", "observations", "geometry"]},
                        "description": "Optional companion layers. Omit for the lightweight event summary. Geometry is never returned implicitly.",
                    },
                    "relationship_depth": {"type": "integer", "minimum": 1, "maximum": 2, "default": 1, "description": "Bounded cross-hazard traversal depth when relationships is included."},
                    "geometry_mode": {"type": "string", "enum": ["current", "timeline"], "default": "current", "description": "current returns the representative/current shape; timeline returns bounded progression frames where supported."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100, "description": "Per-section ceiling for relationships, affected places, observations, or geometry frames."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["pack_id", "source_id", "event_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_point",
            "title": "Resolve Point(s) in One Scope",
            "description": "Resolve WGS84 coordinates in exactly one selected scope and family. GLOBAL administrative finds Admin0; an ISO3 administrative scope finds its shallow country bank. MARINE requires one specific jurisdiction family (for example exclusive_economic_zone or high_seas), or water_body with water_kind=named or zone. The broad marine_jurisdiction category has no point route. No call crosses scopes or opens another family's bank. Discover families with get_catalog(catalog='geometry').",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope_id": {"type": "string", "minLength": 3, "description": "GLOBAL, MARINE, or one published ISO3 country. No automatic fallback into another scope."},
                    "family": {"type": "string", "enum": ["administrative", *MARINE_POINT_FAMILIES], "description": "One exact family within the selected scope."},
                    "water_kind": {"type": "string", "enum": ["named", "zone"], "description": "Required only for MARINE water_body: named seas or water zones."},
                    "lat": {"type": "number", "minimum": -90, "maximum": 90, "description": "Latitude in WGS84 decimal degrees."},
                    "lon": {"type": "number", "minimum": -180, "maximum": 180, "description": "Longitude in WGS84 decimal degrees."},
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
                        "description": "Bounded WGS84 point array. Use top-level lat/lon instead for one point.",
                    },
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id echoed for point arrays."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "oneOf": [
                    {"required": ["scope_id", "family", "lat", "lon"], "not": {"required": ["points"]}},
                    {"required": ["scope_id", "family", "points"], "not": {"anyOf": [{"required": ["lat"]}, {"required": ["lon"]}]}},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_deep_point",
            "title": "Resolve Point(s) (Deep)",
            "description": "Second-pass containment only after resolve_point. Supply the original WGS84 coordinate or a bounded point array, one shared shallow_loc_id returned by resolve_point, and one canonical family from get_catalog(catalog='geometry'). family defaults to administrative; shape-backed families use direct bbox-to-exact-shape lookup without crosswalks. Single and bulk requests share this scoped contract.",
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
                                "lat": {"type": "number", "minimum": -90, "maximum": 90},
                                "lon": {"type": "number", "minimum": -180, "maximum": 180},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}]},
                            },
                            "required": ["lat", "lon"],
                            "additionalProperties": False,
                        },
                        "description": "Points already known to fall within the supplied shallow_loc_id scope.",
                    },
                    "shallow_loc_id": {"type": "string", "minLength": 5, "description": "The deepest canonical Admin 1-3 loc_id returned by resolve_point, such as USA-NY-061-009903."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional exact Admin 4-6 level. Omit for the deepest available match."},
                    "family": {"type": "string", "pattern": "^[a-z0-9_]+$", "default": "administrative", "description": "One family selector. Defaults to administrative; use marine for the direct Marine resolver, or a canonical country family such as postal_area, watershed, or land_management_region."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id echoed for point arrays."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "required": ["shallow_loc_id"],
                "oneOf": [
                    {"required": ["lat", "lon"], "not": {"required": ["points"]}},
                    {"required": ["points"], "not": {"anyOf": [{"required": ["lat"]}, {"required": ["lon"]}]}},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_loc_id_info",
            "title": "Inspect Direct loc_id Connections",
            "description": "Look up one known loc_id or a small batch in the selected reference graph. Return its identity and directly recorded loc_id connections, with family, direction, and relationship evidence. Each ID returns at most 50 connections and an explicit has_more flag. This tool does not expand descendants, infer containment, read shapes, search catalogs, or consult external adapters. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id, e.g. 'USA-CA'."},
                    "loc_ids": {"type": "array", "minItems": 1, "maxItems": 10, "items": {"type": "string"}, "description": "Up to 10 known loc_ids, including a resolve_point stack."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "oneOf": [
                    {"required": ["loc_id"]},
                    {"required": ["loc_ids"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "identify_dataset_geography",
            "title": "Choose Geography Columns From a Dataset",
            "description": "Fast structural column selector: pass bounded named columns to find recognized geography headers or latitude/longitude pairs. It does not infer a country, level, or identifier system, open country indexes, or validate values. If more than one column is plausible, select one; then call identify_reference_system with the selected identifier column and one country_scope. No payment required.",
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
                    "country_scope": {"type": "string", "description": "Optional caller-declared ISO3 country, passed through to the selected column."},
                    "request_id": {"type": "string"},
                },
                "required": ["columns"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "identify_reference_system",
            "title": "Identify One Identifier Column's Reference System",
            "description": "Identify one selected identifier column within one required ISO3 country, GLOBAL, or MARINE scope. Pass bounded strings preserving leading zeros. It checks maintained identifier evidence, not dataset columns or polygons. Ambiguous systems stay unselected until expected.system is supplied. Use identify_dataset_geography first when the column is unknown. No payment required.",
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
                    "country_scope": {"type": "string", "description": "Required single ISO3 country, GLOBAL, or MARINE scope. Missing scope fails before identifier reads."},
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
                "required": ["country_scope"],
                "anyOf": [
                    {"required": ["identifier"]},
                    {"required": ["identifiers"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "convert_reference",
            "title": "Convert Geographic Reference",
            "description": "Convert within one declared scope per call: iso3 is required and names one ISO3 country, GLOBAL, or MARINE. A batch uses one from_system, to_system, and crosswalk_id, with at most 25000 requested result rows. Convert X -> loc_id -> Y; omit to_system to enter the loc_id universe. For admin or city names use from_system admin_boundary with a country_hint inside the declared country. A published direct crosswalk uses one selected artifact; if the system pair has multiple artifacts, supply crosswalk_id from the typed ambiguity error. An exact miss ends on that artifact. Use get_geography when family or scope coverage is unknown. The tools/list access metadata states authored free, account, and paid item allowances; active operator policy may waive payment.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from_system": {"type": "string", "description": "Input reference system, such as admin_boundary (admin and city names; admin_text and place_name are accepted aliases), zip, overlay_zcta, nws_fire, tribal, or loc_id."},
                    "value": {"type": "string", "description": "Identifier or name in the input system, for example 'Springfield, Illinois' with admin_boundary."},
                    "to_system": {"type": "string", "default": "daedalmap.loc_id", "description": "Optional output reference system. Defaults to DaedalMap loc_id; provide zcta, nws_fire, overlay_nws_public_zone, overlay_tribal, admin_local, or another catalog system for any-to-any conversion."},
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "from_system": {"type": "string", "description": "Input reference system for this row. Defaults to top-level from_system when omitted."},
                                "value": {"type": "string", "description": "Identifier or name in the input system."},
                                "to_system": {"type": "string", "description": "Output reference system for this row. Defaults to top-level to_system when omitted."},
                                "iso3": {"type": "string", "description": "Optional repeat of the top-level scope; must match it. Split mixed countries or packs into separate calls."},
                                "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Optional intermediate admin target level for this row."},
                                "relationship_vintage": {"type": "string", "description": "Optional relationship vintage for this row."},
                                "crosswalk_id": {"type": "string", "description": "Exact published crosswalk ID when the source/target system pair has multiple artifacts."},
                                "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum overlap share threshold for this row."},
                                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked output references for this row."},
                                "country_hint": {"type": "string", "description": "Optional country hint for admin/name resolution (USA), or a subdivision to search within (USA-OR). Names may also carry their parent: 'Washington County, Oregon'. Ambiguous names return candidates instead of a match."},
                                "admin_level_hint": {"type": "integer", "minimum": 0, "maximum": 6, "description": "Optional admin-level hint for admin/name resolution."},
                                "as_of": {"type": "string", "description": "ISO date or year used for time-bounded identities."},
                                "row_index": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller row identifier echoed in the result."},
                                "id": {"anyOf": [{"type": "integer"}, {"type": "string"}], "description": "Optional caller identifier echoed in the result."},
                            },
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                        "description": "Reference conversions within one scope and one source/target/crosswalk route. Split mixed routes. Maximum requested item count times per-item limit is 25000.",
                    },
                    "iso3": {"type": "string", "minLength": 3, "description": "Required scope pack for the entire call: one ISO3 country, GLOBAL, or MARINE. Every item must stay within this scope."},
                    "target_admin_level": {"anyOf": [{"type": "string"}, {"type": "integer"}], "description": "Admin level used as the intermediate crosswalk target. Default admin_2."},
                    "relationship_vintage": {"type": "string", "description": "Optional source relationship vintage to require."},
                    "crosswalk_id": {"type": "string", "description": "Exact published crosswalk ID when the source/target system pair has multiple artifacts. A route ambiguity error lists valid IDs."},
                    "min_share": {"type": "number", "minimum": 0, "maximum": 1, "description": "Optional minimum overlap share threshold."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Maximum ranked output references to return. Default 10."},
                    "country_hint": {"type": "string", "description": "Optional country hint for admin/name resolution (USA), or a subdivision to search within (USA-OR). Names may also carry their parent: 'Washington County, Oregon'. Ambiguous names return candidates instead of a match."},
                    "admin_level_hint": {"type": "integer", "minimum": 0, "maximum": 6, "description": "Optional admin-level hint for admin/name resolution."},
                    "as_of": {"type": "string", "description": "ISO date or year used for time-bounded identities."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "anyOf": [
                    {"required": ["iso3", "from_system", "value"]},
                    {"required": ["iso3", "items"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "compare_geographies",
            "title": "Compare Geographic Identities",
            "description": "Compare two known loc_ids using selected reference-graph identity and direct relationship evidence. Verified Admin ancestry can prove containment; a typed graph edge can prove only the relation it declares. An explicit negative edge returns no_overlap; a missing edge returns unknown. This tool never reads polygons or computes shape intersections. Use an external identifier bridge before this call when the input is not a loc_id. No payment required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "left_loc_id": {"type": "string", "minLength": 1, "description": "First DaedalMap loc_id."},
                    "right_loc_id": {"type": "string", "minLength": 1, "description": "Second DaedalMap loc_id."},
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
                                "left_loc_id": {"type": "string", "minLength": 1},
                                "right_loc_id": {"type": "string", "minLength": 1},
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
                "oneOf": [
                    {"required": ["left_loc_id", "right_loc_id"]},
                    {"required": ["items"]},
                ],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_shape",
            "title": "Retrieve Shapes for Known loc_ids",
            "description": "Shape lookup for exact existing loc_ids only. The default returns shape availability, centroid, and bounding box; include_polygon=true adds GeoJSON coordinates. Obtain any descendant IDs before this call. Historical geometry is returned first and successors are never substituted automatically. Year selection is not yet supported.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "loc_id": {"type": "string", "description": "DaedalMap loc_id, such as USA-CA-037, USA-Z-00601, USA-NWSFZ-AKZ317, EEZ-USA, or IHO1953-240001002."},
                    "loc_ids": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string"}, "description": "Exact DaedalMap loc_ids to fetch in one call. Default public cap is deployment-configurable and lower when include_polygon is true."},
                    "include_polygon": {"type": "boolean", "description": "When true, include the full GeoJSON geometry. Default false."},
                    "batch_id": {"type": "string", "description": "Optional caller-supplied batch id for tracing."},
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing."},
                },
                "oneOf": [
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
            "description": "Free dry-run quote for uploaded or pasted user data conversion. Checks a fixed representative sample of up to 32 supplied rows through the real conversion resolver, then estimates total resolvable rows, output bytes, errors, and charge units. row_count is the full dataset size and is not replaced by the sample size. Conversion execution validates every submitted row and reports structured failures for unmatched keys. For a coordinate file, set geography_binding.mode to coordinates and send point_count (valid coordinate pairs counted locally), row_count, and request_id instead of items: no point is resolved, the quote is the ceiling for exactly those points, and resolve_point called with the same request_id reuses its quote_id. Quotes are maximums; execution charges only successfully resolved rows or points.",
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
                    "batch_id": {"type": "string", "description": "Coordinate mode only: batch id the resolve_point run will send."},
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
            "description": "The single workhorse for published data-pack rows inside the loc_id universe. After get_pack, pass its pack_id, exact metric ids, structured filters, sort, and row limit. filters.region_ids accepts canonical loc_ids; an administrative parent such as USA-TX matches descendant rows at the pack's published geography grain. For disaster event rows, pass the same pack_id, result's source_id, and one row's exact event_id to get_event. Use resolution/onboarding tools before get_data when the input is coordinates, outside codes, names, or an unidentified user column. Pack metadata owns get_data source routing, time grain, geography, access, and pack-specific rules; callers do not choose internal source_id values for get_data. Geometry tool families use their focused next_step tools instead of this row-query contract.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "request_id": {"type": "string", "description": "Optional caller-supplied request id for tracing and idempotency."},
                    "pack_id": {"type": "string", "minLength": 1, "description": _pack_id_description()},
                    "metrics": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string", "minLength": 1}, "description": "Exact metric ids from get_pack. Use event_count for aggregate counts only when get_pack publishes it."},
                    "filters": _data_filters_schema(),
                    "sort": _data_sort_schema(),
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum number of rows to return for the requested source or pack."},
                    "output": {
                        "type": "object",
                        "properties": {
                            "format": {"type": "string", "enum": ["rows"], "default": "rows"},
                            "include_provenance": {"type": "boolean", "default": False},
                        },
                        "additionalProperties": False,
                        "description": "Optional response controls. v1 supports rows only.",
                    },
                },
                "required": ["pack_id", "metrics", "filters"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
    ]
    return [
        decorate_shared_help_definition(
            decorate_geometry_tool_definition(decorate_data_tool_definition(definition))
        )
        for definition in definitions
        if include_paused or str(definition.get("name") or "") not in PAUSED_PUBLIC_TOOL_NAMES
    ]
