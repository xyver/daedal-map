from __future__ import annotations

from access_policy_shared import resolve_effective_access
from tool_access_shared import (
    tool_free_item_limit,
    tool_inline_item_limit,
    tool_is_paid_bulk,
    tool_paid_item_limit,
    tool_pricing,
)

# Shared structured registry for the live agent/API/MCP pack set.
# This is the intended single authored source for:
# - published pack order
# - free vs paid pricing lane
# - MCP facade metadata
# - public display names
# - preferred MCP routing hints
#
# Keep this module stdlib-only so runtime, site, stdio, and helper scripts can
# import it safely.

PACK_REGISTRY: dict[str, dict] = {
    "currency": {
        "display_name": "currency",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "get_fx_rates"),
        "mcp_prompt_allowlist": ("fx_history_for_country",),
        "mcp_name": "com.daedalmap/currency",
        "mcp_title": "DaedalMap Historical FX Rates",
        "mcp_description": "Historical daily FX rates for 100+ currencies normalized to USD, from 1940 to present. Free - no payment required. Supports daily, weekly, and monthly granularity.",
        "registry_meta": {
            "categories": ["economics", "data", "geospatial"],
            "highlights": [
                "Historical foreign exchange rate comparisons",
                "Country-level FX lookups tied to DaedalMap loc_id geography",
                "Free structured MCP access for historical currency data",
            ],
        },
        "routing": {
            "preferred_tool": "get_fx_rates",
        },
    },
    "earthquakes": {
        "display_name": "earthquakes",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "get_disaster_links_for_event", "get_disaster_link_chain", "search_disaster_links", "get_earthquake_events", "get_live_earthquake_events"),
        "mcp_prompt_allowlist": ("largest_earthquake_in_range", "count_disaster_events"),
        "mcp_name": "com.daedalmap/earthquakes",
        "mcp_title": "DaedalMap Earthquake Data",
        "mcp_description": "Historical earthquake events from 2150 BC to present. Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Historical earthquake event data with structured filters",
                "Paid MCP access for earthquake counts and event rows",
                "Country and region lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "get_earthquake_events",
            "live_fallback_tool": "get_live_earthquake_events",
            "live_fallback_when": "Only when the caller explicitly asks for live/preliminary upstream data or needs time beyond canonical_available_through.",
        },
    },
    "floods": {
        "display_name": "floods",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "search_disaster_links", "query_dataset"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/floods",
        "mcp_title": "DaedalMap Flood Events",
        "mcp_description": "Global large flood events from 1985 to present from the Dartmouth Flood Observatory Global Flood Records (CC0 public domain), including flood extent polygons, fatalities, displaced, severity, main cause, and a flood impact index, plus MODIS satellite-mapped extents from the Global Flood Database. Free - no payment required.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Global flood event records 1985-present with extent polygons",
                "Free MCP access for flood counts, fatalities, displaced, and severity",
                "Country and region lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "hurricanes": {
        "display_name": "hurricanes",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "search_disaster_links", "query_dataset"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/hurricanes",
        "mcp_title": "DaedalMap Hurricane and Tropical Cyclone Data",
        "mcp_description": "Global tropical cyclone tracks from IBTrACS, 1842-present. Wind, pressure, and paths. Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Global tropical cyclone and hurricane track records",
                "Paid MCP access for hurricane and cyclone event queries",
                "Country and basin lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "tornadoes": {
        "display_name": "tornadoes",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "search_disaster_links", "query_dataset"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/tornadoes",
        "mcp_title": "DaedalMap Tornado Events",
        "mcp_description": "United States tornado events from 1950 to present from the NOAA Storm Prediction Center, including track paths, EF/Fujita intensity ratings, casualties, and damage estimates. Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "US tornado event records 1950-present with track paths and intensity",
                "Paid MCP access for tornado counts, casualties, and damage estimates",
                "State and region lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "tsunamis": {
        "display_name": "tsunamis",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "get_disaster_links_for_event", "get_disaster_link_chain", "search_disaster_links", "get_tsunami_events"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/tsunamis",
        "mcp_title": "DaedalMap Tsunami Data",
        "mcp_description": "Historical tsunami events from 2000 BC to present. Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Historical tsunami event data and wave-height metrics",
                "Paid MCP access for tsunami counts and event rows",
                "Country and coastal-region lookups tied to DaedalMap geography",
            ],
        },
        "routing": {
            "preferred_tool": "get_tsunami_events",
        },
    },
    "wildfires": {
        "display_name": "wildfires",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "get_disaster_links_for_event", "get_disaster_link_chain", "search_disaster_links", "query_dataset"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/wildfires",
        "mcp_title": "DaedalMap Wildfire Events",
        "mcp_description": "Global, U.S., and Canada wildfire event and aggregate data, including burned area, duration, and source-aware regional routing. Paid via x402 on Base mainnet USDC. Start with an unpaid call to inspect the exact price before committing.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Source-aware wildfire coverage across global, U.S., and Canada event lanes",
                "Paid MCP access for wildfire event discovery, burned-area questions, and regional rollups",
                "Country, province, state, and county lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "un_sdg": {
        "display_name": "UN SDG",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/un_sdg",
        "mcp_title": "DaedalMap UN Sustainable Development Goals",
        "mcp_description": "UN Sustainable Development Goal indicators across all 17 goals - poverty, health, education, gender, energy, climate, and institutions - as curated country-year panels normalized to the DaedalMap loc_id spine. Free.",
        "registry_meta": {
            "categories": ["development", "data", "geospatial"],
            "highlights": [
                "210 curated SDG indicators across all 17 goal families",
                "Normalized country-year panels on the shared DaedalMap loc_id spine",
                "Free MCP access; complements World Development Indicators and country reference data",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "world_bank_wdi": {
        "display_name": "World Bank WDI",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/world-development-indicators",
        "mcp_title": "DaedalMap World Development Indicators",
        "mcp_description": "World Bank World Development Indicators as curated country-year panels: economy, health, education, environment, debt, infrastructure, and social - normalized to the DaedalMap loc_id spine with tiered metrics. Free.",
        "registry_search_alias": "world-development-indicators",
        "registry_meta": {
            "categories": ["development", "economics", "data", "geospatial"],
            "highlights": [
                "Curated World Development Indicators across seven category sources",
                "Free MCP access for economic, health, education, and environment metrics",
                "Country-level lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "distributed_manufacturing": {
        "display_name": "Distributed Manufacturing",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/distributed-manufacturing",
        "mcp_title": "DaedalMap Distributed Manufacturing Locations",
        "mcp_description": "Global fab lab, makerspace, hackerspace, Precious Plastic, and Prusa World location data normalized to the DaedalMap loc_id spine. Free.",
        "registry_search_alias": "distributed-manufacturing",
        "registry_meta": {
            "categories": ["manufacturing", "geospatial", "data"],
            "highlights": [
                "Global registry of open manufacturing and maker facilities",
                "Free MCP access for country and facility-type location queries",
                "Point locations tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "owid": {
        "display_name": "Our World in Data",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/our-world-in-data",
        "mcp_title": "DaedalMap Our World in Data",
        "mcp_description": "Our World in Data as curated country-year panels across twelve topic sources: CO2 and greenhouse gases, emissions by sector, energy, health and mortality, population, food and agriculture, education, poverty and inequality, water and sanitation, labor and gender, land and biodiversity, and governance - normalized to the DaedalMap loc_id spine with tiered metrics. Free.",
        "registry_search_alias": "our-world-in-data",
        "registry_meta": {
            "categories": ["development", "environment", "health", "data", "geospatial"],
            "highlights": [
                "Curated Our World in Data country-year panels across twelve topic sources",
                "Free MCP access for emissions, energy, health, population, and poverty metrics",
                "Country-level lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "un_wpp": {
        "display_name": "UN World Population Prospects",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/world-population-prospects",
        "mcp_title": "DaedalMap UN World Population Prospects",
        "mcp_description": "UN World Population Prospects country-year population, births, deaths, migration, and life expectancy metrics from 1950 through projections to 2100. Free.",
        "registry_search_alias": "world-population-prospects",
        "registry_meta": {
            "categories": ["demographic", "development", "data", "geospatial"],
            "highlights": [
                "UN WPP historical estimates and medium-variant projections",
                "Free MCP access for population, fertility, mortality, and migration metrics",
                "Country-level lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "nri": {
        "display_name": "FEMA NRI",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/fema-nri",
        "mcp_title": "DaedalMap FEMA National Risk Index",
        "mcp_description": "FEMA National Risk Index county hazard-risk layers, including baseline risk, expected annual loss, social vulnerability, resilience, and selected future scenario fields. Free.",
        "registry_search_alias": "fema-nri",
        "registry_meta": {
            "categories": ["hazard", "risk", "geospatial", "data"],
            "highlights": [
                "County-level FEMA National Risk Index hazard members",
                "Free MCP access for risk scores, expected annual loss, social vulnerability, and resilience",
                "USA county loc_id filtering for hazard-specific risk layers",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "volcanoes": {
        "display_name": "volcanoes",
        "pricing": "free",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "get_disaster_links_for_event", "get_disaster_link_chain", "search_disaster_links", "get_volcanic_activity", "get_live_volcano_events"),
        "mcp_prompt_allowlist": ("count_disaster_events",),
        "mcp_name": "com.daedalmap/volcanoes",
        "mcp_title": "DaedalMap Volcanic Activity",
        "mcp_description": "Historical volcanic eruption records from Holocene to present, including VEI and location data. Free - no payment required.",
        "registry_meta": {
            "categories": ["hazard", "geospatial", "data"],
            "highlights": [
                "Historical volcanic eruption records and VEI data",
                "Free MCP access for volcanic activity queries",
                "Country and region lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "get_volcanic_activity",
            "live_fallback_tool": "get_live_volcano_events",
            "live_fallback_when": "Only when the caller explicitly asks for live/preliminary upstream data or needs time beyond canonical_available_through.",
        },
    },
    "world_factbook": {
        "display_name": "World Factbook",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/world_factbook",
        "mcp_title": "DaedalMap CIA World Factbook",
        "mcp_description": "CIA World Factbook country indicators for infrastructure, energy, demographics, and economy. Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["economic", "data", "geospatial"],
            "highlights": [
                "111 CIA World Factbook indicators from 2002-2026 editions",
                "Paid MCP access for country infrastructure and economy metrics",
                "Country-level lookups tied to DaedalMap loc_id geography",
            ],
        },
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "worldpop": {
        "display_name": "WorldPop",
        "pricing": "paid_x402_base_usdc",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "query_dataset"),
        "mcp_name": "com.daedalmap/population",
        "mcp_title": "DaedalMap Population Estimates",
        "mcp_description": "Global population estimates from WorldPop, 2000-2030, at country and sub-national admin levels. Source: WorldPop (CC-BY 4.0). Paid via x402 on Base mainnet USDC. Small queries stay cheap; very broad scans cost more or need narrower filters. Call unpaid first to see the exact price before committing.",
        "registry_meta": {
            "categories": ["demographic", "data", "geospatial"],
            "highlights": [
                "WorldPop population estimates across multiple admin levels",
                "Paid MCP access for country and sub-national population queries",
                "Country and regional lookups tied to DaedalMap loc_id geography",
            ],
        },
        "registry_search_alias": "population",
        "routing": {
            "preferred_tool": "query_dataset",
        },
    },
    "geography": {
        "display_name": "Geography Tools",
        "kind": "tool_family",
        "pricing": "mixed",
        "mcp_tool_allowlist": (
            "get_catalog",
            "get_pack",
            "how_geometry_works",
            "read_geometry_catalog",
            "list_reference_systems",
            "identify_dataset_geography",
            "identify_reference_system",
            "resolve_reference",
            "convert_reference",
            "compare_geographies",
            "check_geometry",
            "get_geometry",
            "resolve_point",
            "loc_id_info",
            "resolve_loc_id_scope",
            "estimate_geometry_package",
            "create_geometry_export",
            "estimate_conversion_job",
            "create_conversion_job",
            "get_job_status",
        ),
        "mcp_name": "com.daedalmap/geography",
        "mcp_version": "1.1.0",
        "mcp_title": "DaedalMap Geography Tools (loc_id)",
        "mcp_description": "Geography utilities built on the DaedalMap loc_id spine. Dataset identification accepts bounded column samples and selects geography through the current catalog-backed identity graph. Point lookup returns a compact latest-available chain; separate tools provide identity details, strict hierarchy, references, relationships, shapes, and exports. A utility family, not a queryable dataset pack. Interactive discovery and small lookups are free; large batches and exports may be quoted.",
        "registry_meta": {
            "categories": ["geospatial", "geocoding", "data"],
            "highlights": [
                "Catalog-backed reference exchange: external geography systems <-> DaedalMap loc_id",
                "Convert ZIP/ZCTA, tribal, NWS public zones, and NWS fire weather zones through the same loc_id spine",
                "Compact reverse geocoding: latitude/longitude to the complete latest-available administrative chain",
                "Boundary and bounding-box lookup for any loc_id",
                "Walk the loc_id hierarchy up and down to clip to any admin level",
            ],
        },
        "routing": {
            "preferred_tool": "read_geometry_catalog",
        },
        "tool_summaries": (
            {"name": "how_geometry_works", "summary": "family workflow, loc_id mental model, strict-call boundary, and clarification rules"},
            {"name": "read_geometry_catalog", "summary": "discover geometry coverage, families, bridges, named geometries, and packages"},
            {"name": "list_reference_systems", "summary": "discover exchangeable geography systems, bridge vintages, counts, and licenses"},
            {"name": "identify_dataset_geography", "summary": "bounded table-column samples -> ranked geography column, country, level, and reference binding"},
            {"name": "identify_reference_system", "summary": "unknown or declared identifiers -> ranked reference systems and matching geometry banks"},
            {"name": "resolve_reference", "summary": "one reference or reference batch -> ranked DaedalMap loc_id matches"},
            {"name": "convert_reference", "summary": "one reference or reference batch -> loc_id -> target reference system"},
            {"name": "compare_geographies", "summary": "two loc_ids -> temporal validity, successors, topology, intersection area, and directional overlap shares"},
            {"name": "check_geometry", "summary": "loc_id or loc_ids -> available/missing shape preflight"},
            {"name": "get_geometry", "summary": "loc_id or loc_ids -> geometry metadata, bbox, centroid, and optional polygon"},
            {"name": "resolve_point", "summary": "point(s) -> compact complete latest-available admin chain"},
            {"name": "loc_id_info", "summary": "point-chain loc_ids or other loc_ids -> detailed metadata, strict hierarchy, lifecycle, and references"},
            {"name": "resolve_loc_id_scope", "summary": "strict parent loc_id + admin level -> coherent descendants"},
            {"name": "estimate_geometry_package", "summary": "dry-run selected geometry export count/bytes/price/delivery estimate"},
            {"name": "create_geometry_export", "summary": "create a real synchronous geometry artifact within the effective operational limit"},
            {"name": "estimate_conversion_job", "summary": "dry-run user-data conversion row/error/cost estimate"},
            {"name": "create_conversion_job", "summary": "convert and preserve up to the adjustable hosted limit (7,500 rows by default) as JSON rows, CSV, JSONL, or Parquet"},
            {"name": "get_job_status", "summary": "retrieve completed bounded geometry and conversion jobs"},
        ),
    },
    "reverse-geocoding": {
        "display_name": "Reverse Geocoding",
        "kind": "tool_family_alias",
        "pricing": "mixed",
        "mcp_tool_allowlist": ("get_catalog", "get_pack", "resolve_point"),
        "mcp_name": "com.daedalmap/reverse-geocoding",
        "mcp_version": "1.0.4",
        "mcp_title": "DaedalMap Reverse Geocoding (coordinates to loc_id)",
        "mcp_description": "Compact reverse geocoding: convert one WGS84 point or a small point batch into the complete latest-available administrative loc_id chain. Use the main geography family's loc_id_info or get_geometry tools only when details or shapes are requested.",
        "registry_meta": {
            "categories": ["geospatial", "geocoding", "data"],
            "highlights": [
                "Latitude/longitude to the complete latest-available administrative chain",
                "Small point batches in one MCP call for table cleanup",
                "Small chain rows with loc_id, name, level, and available vintage",
                "The first 100 points per call are free; larger hosted batches use paid throughput",
            ],
        },
        "routing": {"preferred_tool": "resolve_point"},
        "tool_summaries": (
            {"name": "resolve_point", "summary": "one point or point batch -> compact complete latest-available admin chain"},
        ),
    },
    "boundaries": {
        "display_name": "Boundaries",
        "kind": "tool_family_alias",
        "pricing": "free",
        "mcp_tool_allowlist": (
            "get_catalog",
            "get_pack",
            "check_geometry",
            "get_geometry",
            "compare_geographies",
            "loc_id_info",
            "resolve_loc_id_scope",
            "estimate_geometry_package",
            "create_geometry_export",
            "get_job_status",
        ),
        "mcp_name": "com.daedalmap/boundaries",
        "mcp_version": "1.0.4",
        "mcp_title": "DaedalMap Administrative Boundaries (loc_id to polygon)",
        "mcp_description": "Boundaries: a loc_id to its bounding box, centroid, and polygon, plus name and admin level.",
        "registry_meta": {
            "categories": ["geospatial", "boundaries", "data"],
            "highlights": [
                "Bounding box and centroid for any loc_id",
                "Full boundary polygon on request",
                "Clip or index your own grid/raster data against administrative areas",
            ],
        },
        "routing": {"preferred_tool": "get_geometry"},
        "tool_summaries": (
            {"name": "check_geometry", "summary": "loc_id or loc_ids -> available/missing shape preflight"},
            {"name": "get_geometry", "summary": "exact loc_id shape/vintage -> bounding box, centroid, and optional polygon"},
            {"name": "compare_geographies", "summary": "two loc_ids -> exact spatial and temporal relationship"},
            {"name": "loc_id_info", "summary": "loc_id or point-chain loc_ids -> detailed metadata, strict hierarchy, lifecycle, and references"},
            {"name": "resolve_loc_id_scope", "summary": "strict parent loc_id + admin level -> coherent descendants"},
            {"name": "estimate_geometry_package", "summary": "dry-run geometry export count/bytes/price/delivery estimate"},
            {"name": "create_geometry_export", "summary": "create a real synchronous geometry artifact within the effective operational limit"},
            {"name": "get_job_status", "summary": "retrieve completed bounded geometry export jobs"},
        ),
    },
}


def pack_kind(pack_id: str | None) -> str:
    profile = PACK_REGISTRY.get(str(pack_id or "").strip()) or {}
    return str(profile.get("kind") or "data_pack")


def published_pack_ids() -> tuple[str, ...]:
    # Data packs only. Tool families (e.g. geography) are listed separately via
    # tool_family_ids() so existing pack/catalog/llms surfaces stay unchanged.
    return tuple(pid for pid, p in PACK_REGISTRY.items() if str(p.get("kind") or "data_pack") == "data_pack")


def tool_family_ids() -> tuple[str, ...]:
    return tuple(pid for pid, p in PACK_REGISTRY.items() if str(p.get("kind") or "data_pack") == "tool_family")


def tool_family_alias_ids() -> tuple[str, ...]:
    # Registry-discovery-only facades that route to the same geography tools under
    # their own searchable name/endpoint (the disaster-pack pattern). Excluded
    # from tool_family_ids() so the catalog still shows one geography family.
    return tuple(pid for pid, p in PACK_REGISTRY.items() if str(p.get("kind") or "data_pack") == "tool_family_alias")


def tool_family_catalog_entry(pack_id: str | None) -> dict:
    normalized = str(pack_id or "").strip()
    profile = PACK_REGISTRY.get(normalized) or {}
    pricing = str(profile.get("pricing") or "free")
    tools = []
    for raw_tool in profile.get("tool_summaries") or ():
        tool = dict(raw_tool)
        name = str(tool.get("name") or "")
        limits = {
            "free_item_limit": tool_free_item_limit(name),
            "paid_item_limit": tool_paid_item_limit(name),
            "inline_item_limit": tool_inline_item_limit(name),
        }
        tool["access"] = {
            "pricing": tool_pricing(name),
            "limits": {key: value for key, value in limits.items() if value is not None},
        }
        tool["effective_access"] = resolve_effective_access(
            resource_kind="tool",
            resource_id=name,
            authored_pricing=tool_pricing(name),
            # This public discovery projection answers what the operator policy
            # does to an otherwise eligible tool. Exact geometry licence and
            # release clearance are rechecked by the execution route.
            license_permissions={"paid"},
        )
        tools.append(tool)
    paid_tools = [tool["name"] for tool in tools if tool_is_paid_bulk(str(tool.get("name") or ""))]
    return {
        "pack_id": normalized,
        "kind": "tool_family",
        "display_name": str(profile.get("display_name") or normalized.replace("_", " ")),
        "title": profile.get("mcp_title"),
        "description": profile.get("mcp_description"),
        "pricing": pricing,
        "paid_data_calls": pricing != "free",
        "effective_paid_data_calls": any(
            bool((tool.get("effective_access") or {}).get("settlement_required"))
            for tool in tools
        ),
        "tools": tools,
        "access_summary": {
            "model": "mixed" if paid_tools else "free",
            "free_discovery_tool": "get_tool_help",
            "paid_bulk_tools": paid_tools,
            "rule": "Paying raises throughput; it does not unlock different geography data.",
            "trusted_artifact_qa": "Bypasses route/tool call limits, item caps, and payment challenges with a separately labeled analytics lane.",
        },
        "shared_discovery_tools": [
            {"name": "get_tool_help", "pricing": "free", "summary": "usage contract and working example for any tool visible on this facade"}
        ],
    }


def tool_family_pack_detail(pack_id: str | None) -> dict:
    normalized = str(pack_id or "").strip()
    profile = PACK_REGISTRY.get(normalized) or {}
    entry = tool_family_catalog_entry(normalized)
    tools = [dict(tool) for tool in entry.get("tools") or ()]
    preferred_tool = str((profile.get("routing") or {}).get("preferred_tool") or "").strip()
    first_arguments: dict[str, object]
    start_here: list[str]
    important_rules: list[str]
    if preferred_tool == "read_geometry_catalog":
        first_arguments = {"view": "summary"}
        start_here = [
            "Call read_geometry_catalog first to see what coverage, geometry families, bridges, named geometries, and packages exist.",
            "Call list_reference_systems next when you need to know which geography systems can be exchanged.",
            "Use identify_dataset_geography when the caller has a table sample but has not selected or classified its geography column.",
            "Use identify_reference_system when the caller has identifier values but needs to discover or verify their system, level, vintage, and shape bank. Translate natural language into the strict schema first and preserve identifiers as strings with leading zeros.",
            "Use resolve_reference for outside identifiers or names that need to become DaedalMap loc_ids.",
            "Use convert_reference when the caller wants one external geography system expressed in another.",
        ]
        important_rules = [
            "These are direct utility tools, not a query_dataset pack; discovery and small calls are free, while resolve_point batches above 25 use paid hosted throughput.",
            "loc_id is the reserve identifier: generic conversions should flow X -> loc_id -> Y.",
            "Use read_geometry_catalog for live catalog-backed coverage and package discovery instead of assuming a fixed list of countries or admin depths.",
            "Use list_reference_systems for live catalog-backed availability instead of assuming a fixed list of systems.",
            "Use identify_dataset_geography for neutral table samples; callers must not hard-code country or reference-system eligibility before that call.",
            "Use identify_reference_system before bulk conversion when geography identifiers are unknown or only informally declared.",
            "Use resolve_reference for ZIP/ZCTA, tribal-area, NWS public forecast-zone, NWS fire weather-zone, admin-name, and named-geometry inputs.",
            "Use loc_id_info with include_references=true for reverse lookup from an existing loc_id to overlapping or equivalent external references.",
            "Use get_geometry only when geometry metadata, bbox, centroid, or polygon is needed.",
            "Use convert_reference for both side-chain-to-admin and admin-to-side-chain conversions.",
        ]
    elif preferred_tool == "resolve_point":
        first_arguments = {"lat": 34.0522, "lon": -118.2437}
        start_here = [
            "Call resolve_point first when you have coordinates; its compact stack is the normal answer.",
            "Take the returned deepest_resolved_loc_id or any level from the stack for filtering.",
            "Only when more detail is requested, pass all stack loc_ids to loc_id_info; call get_geometry separately for shapes.",
        ]
        important_rules = [
            "These are direct utility tools, not a query_dataset pack; the first 100 resolve_point items per call are free and larger hosted batches use paid throughput.",
            "Coordinates must be WGS84 decimal degrees.",
            "resolve_point defaults to standard mode: deepest available through Admin 3 without deep-partition reads. For Admin 4-6, group the standard results by Admin 1 and use deep mode with one country_scope and one admin_1_scope per call.",
            "A mixed-vintage point chain is context. loc_id_info hierarchy and resolve_loc_id_scope follow strict stored parentage within a coherent release.",
            "get_geometry returns bbox and centroid by default; request include_polygon only when you need the full geometry payload.",
            "Use convert_reference for ZIP/ZCTA, tribal-area, NWS public forecast-zone, or NWS fire weather-zone conversions in either direction.",
            "Use loc_id_info for chain details and set include_references=true only for attached or overlapping reference systems.",
        ]
    elif preferred_tool == "get_geometry":
        first_arguments = {"loc_id": "USA-CA-037"}
        start_here = [
            "Call get_geometry when you already have a loc_id and need its extent.",
            "Use bbox and centroid for lightweight indexing and clipping.",
            "Request include_polygon only when you need the exact perimeter.",
        ]
        important_rules = [
            "These are free utility tools, not a query_dataset pack.",
            "Use canonical loc_ids such as USA, CAN-BC, or USA-CA-037.",
            "BBox/centroid is the default response shape because full polygons can be large.",
            "Use check_geometry first for larger shape lists, then get_geometry for the available loc_ids.",
            "Use loc_id_info for hierarchy, lifecycle, provenance, or non-geometry references; get_geometry stays shape-focused.",
        ]
    else:
        first_arguments = {"loc_id": "USA-CA-037"}
        start_here = [
            "Call the preferred tool first, then expand to the other geography helpers as needed.",
        ]
        important_rules = [
            "These are free utility tools, not a query_dataset pack.",
        ]
    entry["mcp"] = {"name": profile.get("mcp_name"), "facade_url": f"/mcp/{normalized}"}
    entry["registry_meta"] = dict(profile.get("registry_meta") or {})
    entry["routing"] = dict(profile.get("routing") or {})
    entry["quick_start"] = {
        "why_it_exists": "Use the geography tool family to map coordinates and loc_ids onto the same shared loc_id spine the DaedalMap packs use.",
        "start_here": start_here,
        "first_query_template": {
            "tool": preferred_tool,
            "arguments": first_arguments,
        },
        "bridge_examples": [
            {
                "question": "What geometry coverage does DaedalMap expose right now?",
                "tool": "read_geometry_catalog",
                "arguments": {"view": "summary"},
            },
            {
                "question": "What can DaedalMap exchange right now?",
                "tool": "list_reference_systems",
                "arguments": {},
            },
            {
                "question": "Do these values match 2020 US Census tract GEOIDs and a shape bank?",
                "tool": "identify_reference_system",
                "arguments": {
                    "identifiers": ["06073000100", "06073000201"],
                    "expected": {"system": "us_census_geoid", "geo_level": "tract", "vintage": "2020"},
                    "country_scope": "USA",
                },
            },
            {
                "question": "What county does ZIP/ZCTA 00601 resolve to?",
                "tool": "resolve_reference",
                "arguments": {
                    "from_system": "zip",
                    "value": "00601",
                    "target_admin_level": "county",
                    "limit": 5,
                },
            },
            {
                "question": "Which NWS fire weather zones overlap this county?",
                "tool": "loc_id_info",
                "arguments": {
                    "loc_id": "USA-AK-282",
                    "include_references": True,
                    "systems": ["nws_fire"],
                    "target_admin_level": "county",
                    "limit_per_system": 5,
                },
            },
            {
                "question": "Convert ZIP/ZCTA 00601 to overlapping NWS fire weather zones.",
                "tool": "convert_reference",
                "arguments": {
                    "from_system": "zip",
                    "value": "00601",
                    "to_system": "nws_fire",
                    "target_admin_level": "county",
                    "limit": 5,
                },
            },
        ],
        "important_rules": important_rules,
        "starter_tools": [tool.get("name") for tool in tools if tool.get("name")],
    }
    entry["notes"] = (
        "Utility tool family on the DaedalMap loc_id spine. Free, and not a queryable "
        "dataset pack - call the listed tools directly rather than query_dataset."
    )
    return entry


def free_pack_ids() -> tuple[str, ...]:
    return tuple(
        pack_id
        for pack_id, profile in PACK_REGISTRY.items()
        if str(profile.get("kind") or "data_pack") == "data_pack"
        and not str(profile.get("pricing") or "free").startswith("paid")
    )


def paid_pack_ids() -> tuple[str, ...]:
    return tuple(
        pack_id
        for pack_id, profile in PACK_REGISTRY.items()
        if str(profile.get("kind") or "data_pack") == "data_pack"
        and str(profile.get("pricing") or "free").startswith("paid")
    )


def pack_profile(pack_id: str | None) -> dict:
    normalized = str(pack_id or "").strip()
    return dict(PACK_REGISTRY.get(normalized) or {})


def pack_display_name(pack_id: str | None) -> str:
    normalized = str(pack_id or "").strip()
    profile = PACK_REGISTRY.get(normalized) or {}
    return str(profile.get("display_name") or normalized.replace("_", " "))


def pack_registry_alias(pack_id: str | None) -> str | None:
    normalized = str(pack_id or "").strip()
    profile = PACK_REGISTRY.get(normalized) or {}
    alias = str(profile.get("registry_search_alias") or "").strip()
    return alias or None


def pack_mcp_server_profile(pack_id: str | None) -> dict:
    normalized = str(pack_id or "").strip()
    profile = PACK_REGISTRY.get(normalized) or {}
    return {
        "name": profile.get("mcp_name"),
        "title": profile.get("mcp_title"),
        "description": profile.get("mcp_description"),
        "version": profile.get("mcp_version"),
        "pricing": profile.get("pricing"),
        "registry_meta": dict(profile.get("registry_meta") or {}),
    }


def pack_routing_hints() -> dict[str, dict[str, str]]:
    hints: dict[str, dict[str, str]] = {}
    for pack_id, profile in PACK_REGISTRY.items():
        routing = profile.get("routing")
        if isinstance(routing, dict) and routing:
            hints[pack_id] = dict(routing)
    return hints


def pack_tool_allowlists() -> dict[str, set[str]]:
    allowlists: dict[str, set[str]] = {}
    for pack_id, profile in PACK_REGISTRY.items():
        raw = profile.get("mcp_tool_allowlist") or ()
        allowlists[pack_id] = {str(tool) for tool in raw if str(tool).strip()} | {"get_tool_help"}
    return allowlists


def pack_prompt_allowlists() -> dict[str, set[str]]:
    allowlists: dict[str, set[str]] = {}
    for pack_id, profile in PACK_REGISTRY.items():
        raw = profile.get("mcp_prompt_allowlist") or ()
        allowlists[pack_id] = {str(prompt) for prompt in raw if str(prompt).strip()}
    return allowlists
