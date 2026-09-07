from __future__ import annotations

from pack_registry_shared import (
    pack_display_name,
    pack_mcp_server_profile,
    pack_registry_alias,
    published_pack_ids,
    tool_family_catalog_entry,
    tool_family_ids,
)
from pack_pricing_shared import FREE_PACK_IDS, PAID_PACK_IDS

CURRENT_HOSTED_PACK_IDS: tuple[str, ...] = published_pack_ids()


def current_pack_ids() -> tuple[str, ...]:
    return CURRENT_HOSTED_PACK_IDS


def free_pack_ids() -> tuple[str, ...]:
    return tuple(pack_id for pack_id in CURRENT_HOSTED_PACK_IDS if pack_id in FREE_PACK_IDS)


def paid_pack_ids() -> tuple[str, ...]:
    return tuple(pack_id for pack_id in CURRENT_HOSTED_PACK_IDS if pack_id in PAID_PACK_IDS)


def pack_count() -> int:
    return len(CURRENT_HOSTED_PACK_IDS)


def pack_label(pack_id: str) -> str:
    return pack_display_name(pack_id)


def free_pack_csv() -> str:
    return ", ".join(free_pack_ids())


def paid_pack_csv() -> str:
    return ", ".join(paid_pack_ids())


def free_pack_display_csv() -> str:
    return ", ".join(pack_label(pack_id) for pack_id in free_pack_ids())


def paid_pack_display_csv() -> str:
    return ", ".join(pack_label(pack_id) for pack_id in paid_pack_ids())


def current_pack_code_bullets() -> str:
    return "\n".join(f"- `{pack_id}`" for pack_id in CURRENT_HOSTED_PACK_IDS)


def facade_link_bullets(app_origin: str) -> str:
    lines: list[str] = []
    base = app_origin.rstrip("/")
    for pack_id in CURRENT_HOSTED_PACK_IDS:
        line = f"- [{pack_id}]({base}/mcp/{pack_id})"
        alias = pack_registry_alias(pack_id)
        if alias:
            line += f" - registry-published under the searchable name `{alias}`"
        lines.append(line)
    return "\n".join(lines)


def facade_server_json_lines(app_origin: str) -> str:
    base = app_origin.rstrip("/")
    return "\n".join(f"- `GET {base}/mcp/{pack_id}/server.json`" for pack_id in CURRENT_HOSTED_PACK_IDS)


def facade_transport_lines(app_origin: str) -> str:
    base = app_origin.rstrip("/")
    lines: list[str] = []
    for pack_id in CURRENT_HOSTED_PACK_IDS:
        lines.append(f"- `GET {base}/mcp/{pack_id}`")
        lines.append(f"- `POST {base}/mcp/{pack_id}`")
    return "\n".join(lines)


def geography_tools_section(app_origin: str) -> str:
    """Registry-driven markdown block for geography/utility tool families.

    Tool families (e.g. geography) are not data packs, so they are not in
    published_pack_ids(); they are surfaced here from tool_family_ids() so the
    machine-readable agent surfaces advertise the free loc_id spine tools.
    """
    base = app_origin.rstrip("/")
    blocks: list[str] = []
    for family_id in tool_family_ids():
        entry = tool_family_catalog_entry(family_id)
        profile = pack_mcp_server_profile(family_id)
        tool_line = ", ".join(f"`{tool.get('name')}`" for tool in (entry.get("tools") or []))
        blocks.append(
            f"- `{family_id}` utility family (free) - {entry.get('description')}\n"
            f"  - facade: `{base}/mcp/{family_id}` (registry name `{profile.get('name')}`)\n"
            f"  - tools: {tool_line}\n"
            f"  - also reachable on the umbrella `{base}/mcp`; no payment required"
        )
    return "\n".join(blocks)


def geography_workflow_section() -> str:
    """Return question-first instructions for the current geography MCP roster."""
    return (
        "1. Point to loc_id: call `resolve_point`. For a bounded batch use `points`, one `country_scope`, and one `target_admin_level`.\n"
        "2. What a loc_id is connected to: call `loc_id_info` with `include_references=true`; add `include_hierarchy=true` for its strict stored ancestry.\n"
        "3. Outside code or name to loc_id: call `resolve_reference`. If the input system is unknown, call `identify_reference_system` first. Use `convert_reference` only when another reference system is the desired output.\n"
        "4. Shape lookup: call `check_geometry` for availability, then `get_geometry`. Metadata, bbox, and centroid are the default; set `include_polygon=true` only when shape coordinates are needed. If the requested record is historical, return it first; present any `supersession` prompt as a second question and do not fetch the successor until the caller chooses it.\n"
        "5. Coverage discovery: call `read_geometry_catalog` with `view=capabilities` and `country_scope=<ISO3>`, or `list_reference_systems` with `country_scope=<ISO3>` for the canonical published crosswalk registry. Only callable crosswalks appear publicly; preserve all matches and weights. A catalog family alone does not promise a conversion path.\n"
        "6. Relationship between two loc_ids: call `compare_geographies`. For descendants under one parent and level, call `resolve_loc_id_scope`.\n"
        "7. Batch rule: use bounded arrays where supported; split deep work by country, level, and Admin1 owner.\n"
        "8. If the right path is unclear: call `how_geometry_works`, then `get_tool_help` for one exact tool.\n"
        "9. Advanced builder foundation, not the normal entry path: `estimate_geometry_package`, `create_geometry_export`, `estimate_conversion_job`, `create_conversion_job`, and `get_job_status` expose bounded contracts only. Durable uploads, saved projects, and custom downloadable artifacts remain future builder capabilities."
    )


def pack_sentence() -> str:
    return ", ".join(CURRENT_HOSTED_PACK_IDS)


def free_vs_paid_sentence() -> str:
    return (
        f"Free packs: {free_pack_csv()}. "
        f"Paid packs via x402 on Base USDC: {paid_pack_csv()}."
    )


def agent_ai_plugin_description_for_model(*, app_origin: str, docs_origin: str, include_examples: bool) -> str:
    base = (
        "Query structured geographic data across disasters (earthquakes, tsunamis, volcanoes, "
        "hurricanes, tornadoes, floods), FX rates, UN SDG indicators, World Factbook country "
        "profiles, and WorldPop population estimates. "
        f"If your runtime supports remote MCP, start with {app_origin.rstrip('/')}/mcp"
    )
    if include_examples:
        base += (
            f" and read {docs_origin.rstrip('/')}/docs/for-agents. "
            "For raw HTTP discovery, start with GET /api/v1/catalog, then GET /api/v1/packs/{pack_id}, "
            "then POST /api/v1/query/dataset. "
        )
    else:
        base += (
            " and /mcp/server.json first. "
            "For raw HTTP discovery, start with GET /api/v1/catalog, then GET /api/v1/packs/{pack_id}, "
            "then POST /api/v1/query/dataset with the structured body from quick_start.first_query_template. "
        )
    base += (
        "All packs share a loc_id key (ISO3 for countries, hierarchical for sub-national) "
        "enabling cross-pack joins on a single column with no geography normalization. "
        "Geography tools use loc_id as the reserve identifier. Start with how_geometry_works, "
        "read_geometry_catalog for country and family coverage, or list_reference_systems for "
        "usable crosswalks. Use identify_reference_system when an input column is unknown. "
        "resolve_point, loc_id_info, check_geometry, and get_geometry accept bounded batches. "
        "request_id is optional but recommended for tracing and idempotency. "
        f"{free_vs_paid_sentence()}"
    )
    return base


def render_app_llms_txt() -> str:
    return (
        "# DaedalMap App\n\n"
        "This host is the human-facing app at app.daedalmap.com.\n"
        "If you are an agent, crawler, or developer bot, use the MCP server or agent docs instead of the app UI.\n\n"
        "## MCP first\n"
        "- Remote MCP server: https://app.daedalmap.com/mcp\n"
        "- MCP server metadata: https://app.daedalmap.com/mcp/server.json\n"
        "- Registry identity: com.daedalmap/county-map\n"
        "- Current transport: streamable HTTP\n"
        "- MCP wraps the same discovery and execution lane as the hosted API\n\n"
        "## Agent lane\n"
        "- Start here for docs: https://daedalmap.com/docs/for-agents\n"
        "- Agent examples: https://daedalmap.com/docs/agent-examples\n"
        "- loc_id guide: https://daedalmap.com/docs/loc-id\n"
        "- Geometry tools: https://daedalmap.com/docs/geometry-tools\n"
        "- Full machine-readable guide: https://daedalmap.com/llms-full.txt\n\n"
        "## Live machine-facing endpoints\n"
        "- GET https://app.daedalmap.com/mcp/server.json\n"
        "- GET https://app.daedalmap.com/mcp\n"
        "- POST https://app.daedalmap.com/mcp\n"
        "- GET https://app.daedalmap.com/api/v1/guide\n"
        "- GET https://app.daedalmap.com/api/v1/catalog\n"
        "- GET https://app.daedalmap.com/api/v1/packs/{pack_id}\n"
        "- POST https://app.daedalmap.com/api/v1/query/dataset\n\n"
        "## Complete public catalog snapshots\n"
        "- Data catalog: https://downloads.daedalmap.com/downloadable/catalog.json\n"
        "- Geometry catalog: https://downloads.daedalmap.com/downloadable/geometry/geometry_catalog.json\n"
        "- Read a catalog directly when you need the complete inventory; do not crawl the underlying object paths.\n\n"
        "## Current hosted packs\n"
        "The published packs are listed below. "
        "Call GET https://app.daedalmap.com/api/v1/catalog for the live current pack index.\n"
        f"{current_pack_code_bullets()}\n\n"
        "## Geography utility tools (free)\n"
        "Free loc_id-spine tools: reverse geocode coordinates to administrative areas, fetch boundaries, and walk the loc_id hierarchy. Map any spatial reference onto the same loc_id the data packs use.\n"
        f"{geography_tools_section('https://app.daedalmap.com')}\n\n"
        "### Choose the tool by question\n"
        f"{geography_workflow_section()}\n\n"
        "## App UI\n"
        "- Human-facing app: https://app.daedalmap.com\n"
        "- Website and docs: https://daedalmap.com\n"
        "- Source coverage: https://daedalmap.com/docs/source-map\n"
        "- Data packs: https://daedalmap.com/packs\n"
        "- GitHub (open runtime): https://github.com/xyver/daedal-map\n\n"
        "## Crawlers and bots\n"
        "Use the MCP server and agent API lane above. request_id is optional but recommended for tracing and idempotency. Keep requests narrow and respect rate limits; broad live scans may be rejected with guidance.\n"
    )


def render_site_llms_txt(*, app_origin: str = "https://app.daedalmap.com", site_origin: str = "https://daedalmap.com") -> str:
    return (
        "# DaedalMap\n\n"
        "> If you are an agent, crawler, or developer bot, start with the agent lane, not the consumer app.\n\n"
        "DaedalMap exposes a live remote MCP server and a hosted geography-aware HTTP API.\n"
        "This file is the machine-readable router. For explanation, examples, and current pack notes, follow the agent docs.\n\n"
        "## Start here\n\n"
        f"- Human/developer landing page: [{site_origin}/docs/for-agents]({site_origin}/docs/for-agents)\n"
        f"- Umbrella MCP endpoint: [{app_origin}/mcp]({app_origin}/mcp)\n"
        f"- MCP server metadata: [{app_origin}/mcp/server.json]({app_origin}/mcp/server.json)\n"
        f"- HTTP discovery: [{app_origin}/api/v1/guide]({app_origin}/api/v1/guide), [{app_origin}/api/v1/catalog]({app_origin}/api/v1/catalog), [{app_origin}/api/v1/packs/earthquakes]({app_origin}/api/v1/packs/earthquakes)\n"
        "- Catalog directories: [data](https://downloads.daedalmap.com/downloadable/catalog.json), [geometry](https://downloads.daedalmap.com/downloadable/geometry/geometry_catalog.json); geometry links to country detail and the crosswalk registry\n"
        f"- Expanded machine guide: [{site_origin}/llms-full.txt]({site_origin}/llms-full.txt)\n\n"
        "## Product surface\n\n"
        "DaedalMap is a geographic query engine with maintained data packs. The product has four user-facing modes over one engine; the agent lane above is one of them.\n\n"
        f"- [{site_origin}/]({site_origin}/) - project overview and four-mode router\n"
        f"- [{site_origin}/explore]({site_origin}/explore) - Explore mode (demo entry, ask a question, no account)\n"
        f"- [{site_origin}/research]({site_origin}/research) - Research mode (build a corpus, cross-domain history)\n"
        f"- [{site_origin}/ops]({site_origin}/ops) - Ops mode (live operational watch, focused feeds)\n"
        f"- [{site_origin}/feeds]({site_origin}/feeds) - hosted live-feed status surface under Ops\n"
        f"- [{site_origin}/docs/for-agents]({site_origin}/docs/for-agents) - Agents mode (this lane)\n"
        f"- [{site_origin}/packs]({site_origin}/packs) - public pack catalog\n"
        f"- [{site_origin}/about]({site_origin}/about) - founder, mission, open-engine framing\n"
        "- [https://github.com/xyver/daedal-map](https://github.com/xyver/daedal-map) - open engine source (self-host path)\n\n"
        "## Recommended hierarchy\n\n"
        "- Humans and developers start at `/docs/for-agents`\n"
        "- Bots and crawlers start at `/llms.txt`\n"
        "- MCP-capable clients should use `/mcp`\n"
        "- Direct HTTP clients should use `/api/v1/guide`, `/api/v1/catalog`, `/api/v1/packs/{pack_id}`, and `/api/v1/query/dataset`\n"
        "- Narrow pack facades such as `/mcp/currency` and `/mcp/earthquakes` are for registry discoverability, not the main product entry point\n\n"
        "## Live endpoints\n\n"
        f"- [GET /mcp/server.json]({app_origin}/mcp/server.json)\n"
        f"- [GET /mcp]({app_origin}/mcp)\n"
        f"- [POST /mcp]({app_origin}/mcp)\n"
        f"- [GET /api/v1/guide]({app_origin}/api/v1/guide)\n"
        f"- [GET /api/v1/catalog]({app_origin}/api/v1/catalog)\n"
        f"- [GET /api/v1/packs/{{pack_id}}]({app_origin}/api/v1/packs/earthquakes)\n"
        f"- [POST /api/v1/query/dataset]({app_origin}/api/v1/query/dataset)\n\n"
        "## Current hosted packs\n\n"
        f"The published packs are listed below.\nCall GET {app_origin}/api/v1/catalog for the live current pack index.\n\n"
        f"{current_pack_code_bullets()}\n\n"
        "## Registry facades\n\n"
        f"{facade_link_bullets(app_origin)}\n\n"
        "## Geography utility tools (free)\n\n"
        "A free utility tool family on the loc_id spine - reverse geocode coordinates, fetch boundaries, and walk the loc_id hierarchy to map any spatial reference onto the same loc_id the data packs use.\n\n"
        f"{geography_tools_section(app_origin)}\n\n"
        "### Choose the tool by question\n\n"
        f"{geography_workflow_section()}\n\n"
        "## Current live contract\n\n"
        "- MCP discovery first is valid: read `server.json`, then call `tools/list`, then use a named tool or `query_dataset`\n"
        "- Free discovery first: `guide`, `catalog`, and pack detail\n"
        "- `request_id` is optional but recommended for tracing and idempotency\n"
        "- Paid execution second: unpaid request to `POST /api/v1/query/dataset` returns `402`, then a payment-aware client retries\n"
        "- MCP is a wrapper over the same underlying hosted discovery and paid execution lanes\n"
        "- The current hosted paid rail is Base mainnet USDC via x402 exact payment\n\n"
        "## Related docs\n\n"
        f"- [For Agents]({site_origin}/docs/for-agents)\n"
        f"- [Agent Examples]({site_origin}/docs/agent-examples)\n"
        f"- [loc_id Guide]({site_origin}/docs/loc-id)\n"
        f"- [Geometry Tools]({site_origin}/docs/geometry-tools)\n"
        "- [GitHub (open runtime)](https://github.com/xyver/daedal-map)\n"
    )


def render_site_llms_full(*, app_origin: str = "https://app.daedalmap.com", site_origin: str = "https://daedalmap.com") -> str:
    return (
        "# DaedalMap Bot Surface\n\n"
        "This file is the expanded machine-oriented guide to the live hosted DaedalMap bot lane.\n"
        "If you are an agent or crawler, prefer this over the human marketing pages and do not start with the human app UI.\n\n"
        "## Lane split\n\n"
        "- `www.daedalmap.com` is the documentation and marketing lane\n"
        "- `app.daedalmap.com` is the human-facing runtime\n"
        f"- `{app_origin}/mcp` is the bot-facing runtime\n"
        "- bots should use the MCP server first and the direct HTTP API second\n"
        "- bots should not treat the human mapping app as the primary programmable interface\n\n"
        "## Canonical bot docs\n\n"
        f"- [For Agents]({site_origin}/docs/for-agents) - canonical public listing for agent directories, MCP clients, current packs, and first-call flow\n"
        f"- [Agent Examples]({site_origin}/docs/agent-examples)\n"
        f"- [loc_id Guide]({site_origin}/docs/loc-id)\n\n"
        "## Publishing and directory layers\n\n"
        "DaedalMap's public agent surface is organized into three discovery layers:\n\n"
        f"- Ultimate listing: `{site_origin}/docs/for-agents`\n"
        f"- Machine-readable listing: `{site_origin}/llms.txt` and `{site_origin}/llms-full.txt`\n"
        f"- Umbrella MCP: `{app_origin}/mcp`\n"
        f"- Individual MCP facades: pack-specific endpoints such as `{app_origin}/mcp/currency`\n\n"
        "Use the ultimate listing for broad directories and GitHub awesome lists. Use\n"
        "the umbrella MCP when a directory requires a direct MCP endpoint. Use the\n"
        "individual MCP facades only where pack-specific search matters, such as the\n"
        "official MCP Registry.\n\n"
        "Recommended hierarchy:\n\n"
        f"- Humans and developers start at `{site_origin}/docs/for-agents`\n"
        f"- Bots and crawlers start at `{site_origin}/llms.txt`\n"
        f"- MCP-capable clients should use `{app_origin}/mcp`\n"
        "- Direct HTTP clients should use the hosted `/api/v1/...` discovery and query endpoints\n"
        "- Pack-specific facades are for registry discoverability, not the main product entry point\n\n"
        "Suggested broad directory entry:\n\n"
        "DaedalMap Geographic Data - Geographic interoperability infrastructure\n"
        "for agents and researchers. Identify reference systems, transform\n"
        "coordinates and boundaries, build crosswalks, assign stable loc_id values,\n"
        "prepare your own data, discover available geography dynamically, and\n"
        f"connect compatible datasets. Start at {site_origin}/docs/for-agents.\n\n"
        "## Hosted bot entry points\n\n"
        f"- `GET {app_origin}/mcp/server.json`\n"
        f"{facade_server_json_lines(app_origin)}\n"
        f"- `GET {app_origin}/api/v1/guide`\n"
        f"- `GET {app_origin}/api/v1/catalog`\n"
        f"- `GET {app_origin}/api/v1/packs/{{pack_id}}`\n"
        f"- `POST {app_origin}/api/v1/query/dataset`\n"
        f"- `GET {app_origin}/mcp`\n"
        f"- `POST {app_origin}/mcp`\n"
        f"{facade_transport_lines(app_origin)}\n\n"
        "## Complete public catalog snapshots\n\n"
        "- Data: `https://downloads.daedalmap.com/downloadable/catalog.json`\n"
        "- Geometry: `https://downloads.daedalmap.com/downloadable/geometry/geometry_catalog.json`\n"
        "- These are complete cached inventories. Read them directly instead of crawling the downloadable object tree.\n\n"
        "## Current hosted packs\n\n"
        f"The published packs are listed below.\nCall GET {app_origin}/api/v1/catalog for the live current pack index.\n\n"
        f"{current_pack_code_bullets()}\n\n"
        "## Geography utility tools (free)\n\n"
        "Beyond the data packs, DaedalMap exposes a free geography/geocoding utility family on the loc_id spine. These are free tools, not a queryable dataset pack: reverse geocode coordinates to administrative areas, fetch boundaries and bounding boxes, and walk the loc_id hierarchy. They are the on-ramp that maps any spatial reference onto the same loc_id the paid data packs use.\n\n"
        f"{geography_tools_section(app_origin)}\n\n"
        "### Choose the tool by question\n\n"
        f"{geography_workflow_section()}\n\n"
        "## Registry summary\n\n"
        "DaedalMap is a remote MCP server and hosted geographic data API for deterministic,\n"
        f"geography-aware queries across curated packs: {pack_sentence()}.\n"
        "Free discovery lives at `GET /api/v1/guide`, `GET /api/v1/catalog`, and\n"
        "`GET /api/v1/packs/{pack_id}`. Execution lives at `POST /api/v1/query/dataset`.\n"
        f"{free_pack_display_csv()} are free lanes. {paid_pack_display_csv()} challenge via HTTP `402`.\n"
        "MCP is available at `/mcp` and wraps the same hosted discovery and execution lane.\n"
        "Floods and tornadoes now have dedicated MCP facades, and worldpop is\n"
        "registry-published under the stronger search noun `population` while the live\n"
        "pack and facade path remain `worldpop`.\n\n"
        "Short bot-facing positioning:\n\n"
        "- remote MCP server for earthquake, tsunami, volcano, hurricane, flood, tornado, SDG, World Factbook, population, and FX data queries\n"
        "- free discovery plus mixed free and paid structured retrieval\n"
        "- deterministic outputs over a maintained shared geography layer\n"
        f"- the umbrella MCP is the canonical live product; pack facades such as `{pack_sentence()}` are narrow official-registry entrypoints over the same shared backend\n"
        "- Smithery remains pointed at the umbrella MCP entrypoint\n\n"
        "Suggested tags:\n\n"
        "- geographic intelligence\n"
        "- disaster intelligence\n"
        "- data API\n"
        "- MCP\n"
        "- remote MCP\n\n"
        "## Expected first flow\n\n"
        "1. Read `GET /mcp/server.json`\n"
        "2. Call `tools/list` on `POST /mcp` or on a narrow facade such as `POST /mcp/currency`, `POST /mcp/earthquakes`, `POST /mcp/floods`, `POST /mcp/tornadoes`, `POST /mcp/volcanoes`, `POST /mcp/tsunamis`, `POST /mcp/hurricanes`, `POST /mcp/un_sdg`, `POST /mcp/world_factbook`, or `POST /mcp/worldpop`\n"
        "3. Read `GET /api/v1/catalog`\n"
        "4. Read one pack detail from `GET /api/v1/packs/{pack_id}`\n"
        "5. Make one free request for `currency`, `floods`, `un_sdg`, or `volcanoes`\n"
        "6. Make one unpaid request for `earthquakes` or `tornadoes`\n"
        "7. Expect HTTP `402` on the paid pack\n"
        "8. Retry with a payment-aware client and expect structured rows on success\n"
        "9. `request_id` is optional but recommended for tracing and idempotency\n\n"
        "## Current access behavior\n\n"
        f"- {free_pack_display_csv()} are free lanes\n"
        f"- {paid_pack_display_csv()} are challenge-first via HTTP `402`\n"
        "- The current live hosted payment rail is x402 exact on Base mainnet\n"
        "- The currently challenged asset is Base mainnet USDC\n"
        "- Requests above the live source maximum reject before payment instead of charging\n\n"
        "## Current proven hosted examples\n\n"
        "- Free proof:\n"
        "  - `currency`\n"
        "  - `limit = 3`\n"
        "  - returns rows directly\n"
        "- Paid minimal proof:\n"
        "  - `earthquakes`\n"
        "  - `limit = 3`\n"
        "  - challenge amount `10000` base units\n"
        "- Free max proof:\n"
        "  - `volcanoes`\n"
        "  - `limit = 500`\n"
        "  - returns rows directly\n"
        "- Paid mid-sized proof:\n"
        "  - `earthquakes`\n"
        "  - `limit = 250`\n"
        "  - challenge amount `25000` base units\n"
        "- Above-max rejection proof:\n"
        "  - `tsunamis`\n"
        "  - `limit = 501`\n"
        "  - status `400`\n"
        "  - code `result_too_large`\n"
        "  - no payment challenge\n\n"
        "## MCP note\n\n"
        "The MCP endpoint is not a separate data product.\n"
        "It wraps the same discovery and paid execution lanes, with the same flagship packs, limits, and payment behavior.\n"
        f"If a client supports remote MCP, use `{app_origin}/mcp` as the first choice.\n"
        "If not, use the hosted HTTP API lane directly.\n"
        f"For official-registry discoverability, narrow facades such as `{app_origin}/mcp/currency`, `{app_origin}/mcp/earthquakes`, `{app_origin}/mcp/floods`, `{app_origin}/mcp/tornadoes`, `{app_origin}/mcp/volcanoes`, `{app_origin}/mcp/tsunamis`, `{app_origin}/mcp/hurricanes`, `{app_origin}/mcp/un_sdg`, `{app_origin}/mcp/world_factbook`, and `{app_origin}/mcp/worldpop` can be listed as pack-specific entrypoints without changing the runtime architecture.\n\n"
        "## Supported clients\n\n"
        "Document directly:\n\n"
        "- Claude Code\n"
        "- Codex\n"
        "- ChatGPT developer mode\n"
        "- Gemini SDKs\n"
        "- Mistral SDK workflows\n\n"
        "Treat as runtime-dependent rather than model-specific:\n\n"
        "- Qwen\n"
        "- DeepSeek\n"
        "- Gemma\n"
        "- Llama\n\n"
        "For those local or open-weight model families, the important question is whether the host runtime can connect to a remote MCP server.\n"
        f"If yes, use `{app_origin}/mcp`.\n"
        "If not, use the hosted HTTP API lane directly.\n\n"
        "## Geography note\n\n"
        "Read the `loc_id` guide before assuming region filters behave the same across all packs.\n"
        "Country ids and hierarchical geography are common, but tsunami examples may also use reviewed named-water loc_ids such as `IHO1953-240001002` for the Mediterranean Sea; `XOO` is deprecated.\n\n"
        "## Human-facing pages\n\n"
        "DaedalMap is a geographic query engine with maintained data packs. The human site is organized around four modes over one engine; the agent lane documented above is one of them. An AI summarizer indexing daedalmap.com should treat the site as a product overview plus four audience-specific mode landings, not as marketing fluff.\n\n"
        f"- [{site_origin}/]({site_origin}/) - project overview and four-mode router. Three blocks: one-line product frame, four-mode chooser, open-engine panel.\n"
        f"- [{site_origin}/explore]({site_origin}/explore) - Explore mode. Demo entry for an undecided visitor. No account required. Primary action: open the hosted app and ask a question. Audience: basic map explorers, undecided visitors.\n"
        f"- [{site_origin}/research]({site_origin}/research) - Research mode. Bounded analytical workspace pitch. Primary action: build a corpus. Audience: academics, researchers, lab teams. Carries the \"every pack joins on loc_id\" credibility moment and the preserved-data section.\n"
        f"- [{site_origin}/ops]({site_origin}/ops) - Ops mode. Live operational watch. Primary action: choose your feeds. Audience: emergency-management teams, partners, operational buyers.\n"
        f"- [{site_origin}/feeds]({site_origin}/feeds) - Hosted status surface listing active live feeds, source agencies, cadence, and matching pack links. This is part of the private hosted control plane, not the self-host runtime tree.\n"
        f"- [{site_origin}/docs/for-agents]({site_origin}/docs/for-agents) - Agents mode (this lane). MCP + HTTP integration in 30 seconds. Audience: developers, MCP/agent builders.\n"
        f"- [{site_origin}/packs]({site_origin}/packs) - Public pack catalog.\n"
        f"- [{site_origin}/about]({site_origin}/about) - Founder, mission, open-engine framing.\n"
        f"- [{site_origin}/docs]({site_origin}/docs) - Docs index.\n"
        f"- [{site_origin}/pricing]({site_origin}/pricing) - Pricing.\n"
        f"- [{app_origin}]({app_origin}) - Hosted app (Explore mode default surface).\n"
        "- [https://github.com/xyver/daedal-map](https://github.com/xyver/daedal-map) - Open engine source (self-host path, contributor pipeline).\n"
    )
