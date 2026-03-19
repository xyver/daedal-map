"""
Order Taker - interprets user requests into structured orders.
Single LLM call using catalog.json and conversions.json for data awareness.

This replaces the old multi-LLM chat system with a simpler "Fast Food Kiosk" model:
1. User describes what they want in natural language
2. Order Taker LLM interprets and builds structured "order"
3. User confirms/modifies order in UI
4. System executes confirmed order directly (no second LLM)
"""

import json
import os
from datetime import datetime
from pathlib import Path
from anthropic import Anthropic
from dotenv import load_dotenv

from .data_loading import load_catalog, load_source_metadata, get_source_path
from .preprocessor import build_tier3_context, build_tier4_context
from .constants import CHAT_HISTORY_LLM_LIMIT
from .llm_tools import format_tools_for_provider, execute_tool, format_tool_result_for_llm
from .aggregation_system import validate_aggregation_policy
from .paths import APP_URL, SITE_URL

load_dotenv()

CONVERSIONS_PATH = Path(__file__).parent / "conversions.json"
REFERENCE_DIR = Path(__file__).parent / "reference"


def load_conversions() -> dict:
    """Load the conversions/regional groupings."""
    with open(CONVERSIONS_PATH, encoding='utf-8') as f:
        return json.load(f)


def load_usa_admin() -> dict:
    """Load USA admin data from reference/usa_admin.json."""
    usa_path = REFERENCE_DIR / "usa_admin.json"
    if usa_path.exists():
        with open(usa_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def build_regions_text(conversions: dict) -> str:
    """Build regions text dynamically from conversions.json and usa_admin.json."""
    groupings = conversions.get("regional_groupings", {})
    usa_admin = load_usa_admin()
    state_abbrevs = usa_admin.get("state_abbreviations", {})

    # Mapping for readable display names (use underscore version for orders)
    display_names = {
        "European_Union": "eu",
        "NATO": "nato",
        "G7": "g7",
        "G20": "g20",
        "BRICS": "brics",
        "ASEAN": "asean",
        "Arab_League": "arab_league",
        "African_Union": "african_union",
        "Commonwealth": "commonwealth",
        "Gulf_Cooperation_Council": "gcc",
        "South_America": "south_america",
        "North_America": "north_america",
        "Latin_America": "latin_america",
        "Central_America": "central_america",
        "Caribbean": "caribbean",
        "Nordic_Countries": "nordic",
        "Baltic_States": "baltic",
        "Benelux": "benelux",
        "Maghreb": "maghreb",
        "Pacific_Islands": "pacific_islands",
        "Asia": "asia",
        "Oceania": "oceania"
    }

    # Categorize groupings
    continents = []
    political = []
    economic = []
    geographic = []
    subregions = []

    for name, data in groupings.items():
        count = len(data.get("countries", []))
        display = display_names.get(name, name.lower().replace(" ", "_"))

        # Categorize based on name patterns
        if name in ["Asia", "Oceania"]:
            continents.append(f"{display} ({count})")
        elif name in ["European_Union", "NATO", "G7", "G20", "BRICS"]:
            political.append(f"{display} ({count})")
        elif name in ["ASEAN", "Arab_League", "African_Union", "Commonwealth", "Gulf_Cooperation_Council"]:
            economic.append(f"{display} ({count})")
        elif name in ["South_America", "North_America", "Latin_America", "Central_America", "Caribbean"]:
            geographic.append(f"{display} ({count})")
        elif name in ["Nordic_Countries", "Baltic_States", "Benelux", "Maghreb", "Pacific_Islands"]:
            subregions.append(f"{display} ({count})")
        elif name.startswith("WHO_"):
            # WHO regions map to continent names
            if "African" in name:
                continents.append(f"africa ({count})")
            elif "Americas" in name:
                continents.append(f"americas ({count})")
            elif "European" in name:
                continents.append(f"europe ({count})")

    # Remove duplicates and sort
    continents = sorted(set(continents))

    lines = []
    if continents:
        lines.append(f"- Continents: {', '.join(continents)}")
    if political:
        lines.append(f"- Political: {', '.join(political)}")
    if economic:
        lines.append(f"- Economic: {', '.join(economic)}")
    if geographic:
        lines.append(f"- Geographic: {', '.join(geographic)}")
    if subregions:
        lines.append(f"- Sub-regions: {', '.join(subregions)}")

    # Add US states info
    lines.append(f"- US States: use state name or abbreviation (e.g., \"California\" or \"CA\") - {len(state_abbrevs)} states/territories")

    return "\n".join(lines)


def get_source_visibility_mode() -> str:
    """
    Order Taker source visibility policy.

    live:
      Only published sources (those with pack_id) are visible/selectable.
    test:
      All sources are visible for QA, but prompt guidance should prefer
      published pack_id sources when multiple candidates overlap.
    """
    configured = os.getenv("ORDER_TAKER_SOURCE_MODE", "").strip().lower()
    if configured in {"live", "test"}:
        return configured
    deployment = os.getenv("DEPLOYMENT", "railway").strip().lower()
    return "test" if deployment == "local" else "live"


def build_system_prompt(catalog: dict, conversions: dict) -> str:
    """
    Build system prompt with catalog organized by geographic scope.

    Groups sources by scope and combines related sources (UN SDGs, World Factbook).
    """

    source_visibility_mode = get_source_visibility_mode()
    all_sources = catalog["sources"]
    published_sources = [src for src in all_sources if src.get("pack_id")]
    visible_sources = published_sources if source_visibility_mode == "live" else all_sources

    # Group visible sources by scope for source selection.
    sources_by_scope = {}
    chat_first_sources = []
    hybrid_sources = []
    for src in visible_sources:
        scope = src.get("scope", "global")
        if scope not in sources_by_scope:
            sources_by_scope[scope] = []
        sources_by_scope[scope].append(src)

        interaction_mode = src.get("interaction_mode", "order_first")
        if interaction_mode == "chat_first":
            chat_first_sources.append(src.get("source_id"))
        elif interaction_mode == "hybrid":
            hybrid_sources.append(src.get("source_id"))

    # Build sources text with grouping of related sources
    sources_text = ""
    published_pack_text = ", ".join(sorted({src.get("pack_id") for src in published_sources if src.get("pack_id")})) or "(none)"

    def format_source_group(sources, scope_label):
        """Format sources, grouping related ones together."""
        lines = []

        # Separate SDGs and Factbook for grouping - detect by topic_tags, not hardcoded source_id
        sdg_sources = [s for s in sources if any(tag.startswith('goal') for tag in s.get('topic_tags', []))]
        factbook_sources = [s for s in sources if 'factbook' in s.get('category', '').lower() or
                           any('factbook' in tag.lower() for tag in s.get('topic_tags', []))]
        other_sources = [s for s in sources if s.get('source_id') and s not in sdg_sources and s not in factbook_sources]

        # Add individual sources with human-readable names AND source_id
        for src in other_sources:
            temp = src.get("temporal_coverage", {})
            name = src.get("source_name", src["source_id"])
            sid = src["source_id"]
            pid = src.get("pack_id")
            publish_note = f"pack_id: {pid}" if pid else "pre-release: no pack_id yet"
            lines.append(f"- {name} [{publish_note}; source_id: {sid}]: {temp.get('start', '?')}-{temp.get('end', '?')}")

        # List SDG sources individually with human-readable goal titles
        if sdg_sources:
            # Sort by goal number extracted from topic_tags
            def get_goal_num(src):
                for tag in src.get('topic_tags', []):
                    if tag.startswith('goal'):
                        try:
                            return int(tag[4:])
                        except ValueError:
                            pass
                return 999
            sdg_sources_sorted = sorted(sdg_sources, key=get_goal_num)

            for src in sdg_sources_sorted:
                sid = src.get('source_id', '')
                temp = src.get("temporal_coverage", {})
                year_range = f"{temp.get('start', '?')}-{temp.get('end', '?')}"

                # Get goal title from catalog reference data or source_name
                goal_title = None
                reference = src.get("reference", {})
                if reference.get("goal"):
                    goal_info = reference["goal"]
                    goal_num = goal_info.get("number", "")
                    goal_name = goal_info.get("name", "")
                    if goal_num and goal_name:
                        goal_title = f"SDG {goal_num}: {goal_name}"

                # Fallback to source_name
                if not goal_title:
                    goal_title = src.get("source_name", sid)

                publish_note = f"pack_id: {src.get('pack_id')}" if src.get("pack_id") else "pre-release: no pack_id yet"
                lines.append(f"- {goal_title} [{publish_note}; source_id: {sid}]: {year_range}")

        # Group World Factbook
        if factbook_sources:
            by_id = {s["source_id"]: s for s in factbook_sources}
            unique = by_id.get("world_factbook")
            overlap = by_id.get("world_factbook_overlap")
            static = by_id.get("world_factbook_static")

            if unique:
                pid = unique.get("pack_id")
                publish_note = f"pack_id: {pid}" if pid else "pre-release: no pack_id yet"
                lines.append(
                    f"- CIA World Factbook [{publish_note}; source_id: world_factbook]: "
                    f"yearly country metrics such as internet users, military expenditure (% of GDP), railways (km), airports, telephones"
                )
            if overlap:
                pid = overlap.get("pack_id")
                publish_note = f"pack_id: {pid}" if pid else "pre-release: no pack_id yet"
                lines.append(
                    f"- CIA World Factbook Overlap [{publish_note}; source_id: world_factbook_overlap]: "
                    f"yearly country metrics such as life expectancy, GDP per capita PPP, birth rate, death rate, population"
                )
            if static:
                pid = static.get("pack_id")
                publish_note = f"pack_id: {pid}" if pid else "pre-release: no pack_id yet"
                lines.append(
                    f"- CIA World Factbook Static Geography [{publish_note}; source_id: world_factbook_static]: "
                    f"country-level static numeric fields such as total area, coastline length, highest point elevation, mean elevation, border count, capital coordinates"
                )

        return "\n".join(lines)

    # Country-specific sources FIRST (more relevant when asking about a country)
    for scope in sorted(sources_by_scope.keys()):
        if scope == "global":
            continue

        scope_sources = sources_by_scope[scope]
        geo_level = scope_sources[0].get("geographic_level", "admin_2") if scope_sources else "admin_2"

        sources_text += f"\n=== {scope.upper()} ONLY ({geo_level}) ===\n"
        sources_text += format_source_group(scope_sources, scope) + "\n"

    # Global sources SECOND
    if "global" in sources_by_scope:
        sources_text += "\n=== GLOBAL (available for all countries at admin_0) ===\n"
        sources_text += format_source_group(sources_by_scope["global"], "global") + "\n"

    # Build regions text from conversions
    regions_text = build_regions_text(conversions)
    unpublished_visibility_note = (
        'Sources marked "pre-release: no pack_id yet" may still exist for internal QA and direct map requests, '
        'but they are not public library items yet.'
        if source_visibility_mode == "test"
        else "In live mode, unpublished sources with no pack_id are invisible and must never be selected."
    )
    chat_first_text = ", ".join(sorted(chat_first_sources)) if chat_first_sources else "(none)"
    hybrid_text = ", ".join(sorted(hybrid_sources)) if hybrid_sources else "(none)"

    return f"""You are an Order Taker for a map data visualization system.

FORMATTING: Never use emojis or special unicode characters in responses. Use plain text only with standard punctuation. Use bullet points (- or *) for lists.

DATA HIERARCHY (what you know vs. what you can fetch):
- CATALOG (below): Summary of all sources with names and year ranges - you always have this
- METADATA: Detailed metrics, statistics, coverage - use get_source_details or list_source_metrics tool
- REFERENCE: Background, methodology, context - use get_source_reference tool

When user asks about metrics for a SPECIFIC source, use the list_source_metrics tool to get accurate info.
When user asks about MULTIPLE sources generally (e.g., "what SDG data"), offer to show details for specific ones.
Example: "I have 17 SDG goals available. Which would you like to explore, or should I pick a few to highlight?"

DATA SOURCES:
{sources_text}
IMPORTANT: Country-specific sources can ONLY be used for that country.
Published pack_ids currently in the public library: {published_pack_text}
Order Taker source visibility mode: {source_visibility_mode}
Only sources with a pack_id are published and should be described to users in general catalog/library answers.
{unpublished_visibility_note}

REGIONS:
{regions_text}

WHEN USER ASKS "what data for [country]" or "what do you have":
1. List that country's specific sources FIRST (if any)
2. Then mention global sources are also available
3. Only mention published packs/sources with a pack_id
4. Be CONCISE - use human-readable names, group related sources
5. End with:
   - Public pack library: {SITE_URL}/packs
   - More packs when logged in: {APP_URL}/settings

FACTBOOK-SPECIFIC RULES:
- The World Factbook sources are all country-level (admin_0), similar to SDG country choropleths.
- If the user explicitly asks to show/map/rank a numeric Factbook metric, prefer type="order", even if the source is pre-release.
- For world_factbook_static numeric fields (highest_point_m, mean_elevation_m, coastline_km, area_total_sq_km, border_countries_count), prefer a map order rather than a chat explanation.
- Use world_factbook_overlap for explicit overlap requests and metrics like life expectancy, GDP per capita, birth rate, death rate, and population.
- Use world_factbook_static for static geography metrics like highest peaks, coastline length, mean elevation, and total area.
- Use world_factbook for unique infrastructure/security metrics like internet users, military expenditure, railways, airports, and telephones.
- Reserve chat/reference behavior for text-heavy static fields like climate, terrain, natural_resources, or named peak/capital descriptions.

WHEN USER ASKS about a specific source ("what's in X?" or "show me metrics"):
- Use the list_source_metrics tool to get the actual metrics
- List the available metrics using ONLY the human-readable names (never show column names)
- If there are 10 or fewer metrics, list them all
- If there are more than 10, say "There are X metrics available, here are the key ones:" and show 5-8
- Mention the year range available
- Say "I can get them all" or "I can show any of these" (never mention "*" or wildcards to the user)

INTERACTION POLICY:
- Default for all sources: order_first.
- If a source has interaction_mode="chat_first", prefer conversational/reference response unless user explicitly asks to map/query metrics.
- If a source has interaction_mode="hybrid", use judgment between chat and order.
- For source-backed analytical questions, prefer returning type="order" over type="chat".
- chat_first sources: {chat_first_text}
- hybrid sources: {hybrid_text}

ORDER FORMAT (JSON when user requests data):
```json
{{"items": [{{"source_id": "owid_co2", "metric": "co2", "region": "europe", "year": 2022}}], "summary": "CO2 for Europe 2022"}}
```

OPTIONAL AGGREGATION FIELDS (only when user explicitly asks):
- `time_granularity`: `daily | weekly | monthly | yearly`
- `aggregation`: `period_end | period_avg` (FX default is period_end if omitted)
- `date_start` / `date_end`: ISO date bounds for time filtering when needed

RULES:
- source_id: Must EXACTLY match one of the available sources
- metric: Must be an EXACT column name from the source, OR use "*" for ALL metrics from that source
- region: lowercase (europe, g7, australia) or null for global
- year: null = most recent
- Only include aggregation fields when the user asks for a specific time granularity or averaging behavior

WILDCARD METRICS (internal - never mention "*" to users):
Use "metric": "*" when user asks for "all data", "everything", or "all metrics" from a source.
Example: {{"source_id": "abs_population", "metric": "*", "region": "australia"}}
This will be expanded to include ALL metrics from that source.
In your response, say "I'll get all the metrics" - never show the "*" symbol to users.

RESPONSE TYPES (return JSON with "type" field):

1. DATA ORDER - User wants to see data on the map:
```json
{{"type": "order", "items": [{{"source_id": "...", "metric": "...", "region": "..."}}], "summary": "..."}}
```

2. GEOMETRY ORDER - User wants to see boundary overlays (ZIP codes, tribal areas, watersheds, etc.):
```json
{{"type": "order", "items": [{{"source_id": "geometry_xxx", "region": "USA-CA", "overlay_type": "xxx"}}], "summary": "..."}}
```
Geometry sources are in the catalog with category="geography" and data_type containing "geometry".
Match the user's request to the appropriate source_id from the catalog based on source_name/description.
The overlay_type is derived from the source_id (e.g., geometry_zcta -> overlay_type="zcta", geometry_tribal -> overlay_type="tribal").

Examples:
- "show me ZIP codes in California": find the ZCTA source in catalog, use its source_id
- "show me tribal areas in Arizona": find the tribal/reservation source in catalog, use its source_id
- For "remove California": use action="remove" at order level
- For mixed "remove Texas, add California": use item-level action for each item

3. NAVIGATION - User wants to zoom/navigate to a location:
```json
{{"type": "navigate", "locations": [{{"loc_id": "USA-CA", "name": "California"}}], "message": "Zooming to California"}}
```

4. DISAMBIGUATION - Multiple locations match, need user to pick:
```json
{{"type": "disambiguate", "message": "Which Washington did you mean?", "options": [{{"loc_id": "USA-WA", "name": "Washington State"}}, {{"loc_id": "USA-DC", "name": "Washington DC"}}]}}
```

5. FILTER UPDATE - User wants to change disaster overlay filters:
```json
{{"type": "filter_update", "overlay": "earthquakes", "filters": {{"minMagnitude": 5.0}}, "message": "Filtering to magnitude 5+"}}
```

6. OVERLAY TOGGLE - User wants to enable/disable a disaster overlay:
```json
{{"type": "overlay_toggle", "overlay": "earthquakes", "enabled": true, "message": "Enabling earthquakes overlay"}}
```
Overlays: earthquakes, hurricanes, volcanoes, tsunamis, tornadoes, wildfires, floods

7. CHAT - General response, information, or clarifying question:
```json
{{"type": "chat", "message": "..."}}
```

INTERPRETATION RULES:
- Check [INTERPRETATION CANDIDATES] section for possible intents with confidence scores
- If "data_request" has highest confidence, return type "order"
- If "navigation" has highest confidence AND no data keywords, return type "navigate"
- If location is marked [LIKELY FALSE POSITIVE], ignore that location match
- If query mentions a data source by name, it's almost certainly a data request, NOT navigation
- "show me data from X" = data request, NOT navigation to a place called "data"
- When multiple sources could satisfy the same metric/request, prefer a published source with a pack_id over a pre-release source with no pack_id
- If the user explicitly names a source or pack, honor that source/pack even if another published source could also answer
- Do not mention source_id to users unless necessary for internal QA; prefer pack/source display names in explanations
- In live mode, never select a source that has no pack_id

INCREMENTAL ORDERS (IMPORTANT):
- Orders describe ONLY what's changing, not the total map state
- The system automatically maintains loaded data - users don't need to repeat previous items
- "add Alaska" = only include Alaska in the order items, NOT previously loaded regions
- "remove Iowa" = only include Iowa with action="remove"
- NEVER include items from previous orders unless the user explicitly asks for them again
- If user says "add X and remove Y", the order should have exactly 2 items: X (add) and Y (remove)

CLARIFYING QUESTIONS - BE SPECIFIC:
- "Which metric?" if they didn't specify what data
- "Which location/country?" if no region specified
- "Which time period/year?" if time is ambiguous
- Example: "Which metric would you like? Population, GDP, births, or I can get them all?"
- NEVER show internal column names (like co2_per_capita) - always use human-readable names only
"""


def interpret_request(user_query: str, chat_history: list = None, hints: dict = None) -> dict:
    """
    Interpret user request and return structured order or response.

    Args:
        user_query: The user's natural language query
        chat_history: Previous messages for context
        hints: Preprocessor hints (topics, regions, time patterns, reference lookups)

    Returns:
        {"type": "order", "order": {...}, "summary": "..."} or
        {"type": "chat", "message": "..."} or
        {"type": "clarify", "message": "..."}
    """
    catalog = load_catalog()
    conversions = load_conversions()
    system_prompt = build_system_prompt(catalog, conversions)

    # Build messages
    messages = [{"role": "system", "content": system_prompt}]

    # Inject Tier 3/Tier 4 context BEFORE chat history
    # This ensures current location/metric context takes priority over old conversations
    if hints:
        context_parts = []

        # Tier 3: Just-in-time context (includes metric column hints for location/topic)
        tier3_context = build_tier3_context(hints)
        if tier3_context:
            context_parts.append(tier3_context)

        # Tier 4: Reference document content (SDG, data sources, country info)
        tier4_context = build_tier4_context(hints)
        if tier4_context:
            context_parts.append(tier4_context)

        # Add context as a system message BEFORE chat history
        # This makes current context more prominent than historical messages
        if context_parts:
            messages.append({
                "role": "system",
                "content": "[CURRENT CONTEXT - USE THIS FOR THE CURRENT QUERY]\n" + "\n".join(context_parts)
            })

    if chat_history:
        for msg in chat_history[-CHAT_HISTORY_LLM_LIMIT:]:
            content = msg.get("content", "")
            # Skip messages with empty content (API rejects them)
            if not content or not content.strip():
                continue
            messages.append({
                "role": msg.get("role", "user"),
                "content": content
            })

    messages.append({"role": "user", "content": user_query})

    # LLM call with tool support
    client = Anthropic()

    # Extract system prompt from messages (Anthropic handles it separately)
    system_content = ""
    chat_messages = []
    for msg in messages:
        if msg["role"] == "system":
            system_content += msg["content"] + "\n\n"
        else:
            chat_messages.append(msg)

    # Get tools in Anthropic format
    tools = format_tools_for_provider("anthropic")

    # Tool use loop - allow up to 3 tool calls per request
    max_tool_iterations = 3
    for iteration in range(max_tool_iterations + 1):
        response = client.messages.create(
            model="claude-haiku-4-5",
            system=system_content.strip(),
            messages=chat_messages,
            tools=tools,
            temperature=0.3,
            max_tokens=500
        )

        # Check if LLM wants to use a tool
        if response.stop_reason == "tool_use":
            # Find tool use block(s) in response
            tool_results = []
            assistant_content = []

            for block in response.content:
                if block.type == "tool_use":
                    # Execute the tool
                    tool_name = block.name
                    tool_input = block.input
                    result = execute_tool(tool_name, tool_input)

                    # Format result for context
                    formatted_result = format_tool_result_for_llm(result)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": formatted_result
                    })
                    assistant_content.append(block)
                elif block.type == "text":
                    assistant_content.append(block)

            # Add assistant message with tool calls
            chat_messages.append({
                "role": "assistant",
                "content": assistant_content
            })

            # Add tool results
            chat_messages.append({
                "role": "user",
                "content": tool_results
            })

            # Continue loop for next LLM response
            continue

        # No tool use - we have the final response
        break

    # Extract final text response
    content = ""
    for block in response.content:
        if hasattr(block, 'text'):
            content += block.text

    content = content.strip()

    # Parse response
    return parse_llm_response(content, hints=hints)


def validate_order_item(item: dict) -> dict:
    """
    Validate an order item against actual source metadata.
    Returns item with validation info added.
    """
    _normalize_item_year_fields(item)
    source_id = item.get("source_id")
    metric = item.get("metric")
    year = item.get("year")

    if not source_id:
        item["_valid"] = False
        item["_error"] = "Missing source_id"
        return item

    # Load source metadata
    metadata = load_source_metadata(source_id)
    if not metadata:
        item["_valid"] = False
        item["_error"] = f"Unknown source: {source_id}"
        return item
    if get_source_visibility_mode() == "live" and not metadata.get("pack_id"):
        item["_valid"] = False
        item["_error"] = f"Source '{source_id}' is not published in live mode"
        return item

    # Check metric exists
    metrics = metadata.get("metrics", {})
    # Skip wildcard metrics - they'll be expanded or handled by the postprocessor
    if metric in ("*", "all", "all_metrics"):
        item["_valid"] = True
        return item
    if metric and metric not in metrics:
        # Try case-insensitive exact match on key first
        metric_lower = metric.lower()
        exact_match = None
        for k in metrics.keys():
            if k.lower() == metric_lower:
                exact_match = k
                break

        # If no key match, try matching by display name (handles LLM outputs like "Proportion of urban population...")
        if not exact_match:
            for k, v in metrics.items():
                if isinstance(v, dict):
                    name = v.get("name", "")
                    if name and name.lower() == metric_lower:
                        exact_match = k
                        break

        if exact_match:
            # Auto-correct to the actual metric key
            item["metric"] = exact_match
            metric = exact_match
        else:
            # No exact match - suggest close matches by key or display name
            close_matches = []
            for k, v in metrics.items():
                name = v.get("name", "") if isinstance(v, dict) else ""
                if metric_lower in k.lower() or k.lower() in metric_lower:
                    close_matches.append(k)
                elif name and (metric_lower in name.lower() or name.lower() in metric_lower):
                    close_matches.append(k)
            close_matches = list(dict.fromkeys(close_matches))  # Remove duplicates
            if close_matches:
                item["_valid"] = False
                item["_error"] = f"Column '{metric}' not found. Did you mean: {', '.join(close_matches[:3])}?"
            else:
                item["_valid"] = False
                item["_error"] = f"Column '{metric}' not found in {source_id}"
            return item

    # Check year is in range
    temp = metadata.get("temporal_coverage", {})
    start_year = _coerce_year(temp.get("start"))
    end_year = _coerce_year(temp.get("end"))

    # Handle single year
    if year and start_year and end_year:
        if year < start_year or year > end_year:
            item["_valid"] = False
            item["_error"] = f"Year {year} outside range {start_year}-{end_year}"
            return item

    # Handle year range
    year_start = item.get("year_start")
    year_end = item.get("year_end")
    if year_start and year_end and start_year and end_year:
        if year_start < start_year:
            item["_valid"] = False
            item["_error"] = f"Year start {year_start} before available data ({start_year})"
            return item
        if year_end > end_year:
            item["_valid"] = False
            item["_error"] = f"Year end {year_end} after available data ({end_year})"
            return item
        if year_start > year_end:
            item["_valid"] = False
            item["_error"] = f"Year start {year_start} is after year end {year_end}"
            return item

    # Validate optional aggregation fields against canonical policy.
    metric_info = metrics.get(metric, {}) if metric else {}
    policy_ok, policy_error, policy_trace = validate_aggregation_policy(
        item,
        source_metadata=metadata,
        metric_name=metric,
        metric_info=metric_info,
    )
    item["_aggregation_policy"] = policy_trace
    if not policy_ok:
        item["_valid"] = False
        item["_error"] = policy_error or "Invalid aggregation policy"
        return item

    # Valid - add metric label if missing
    if metric and not item.get("metric_label"):
        name = metric_info.get("name", metric)
        unit = metric_info.get("unit", "")
        if unit and unit != "unknown":
            item["metric_label"] = f"{name} ({unit})"
        else:
            item["metric_label"] = name

    item["_valid"] = True
    return item


def validate_order(order: dict) -> dict:
    """Validate all items in an order and add validation results."""
    items = order.get("items", [])
    validated_items = []
    all_valid = True

    for item in items:
        validated = validate_order_item(item)
        validated_items.append(validated)
        if not validated.get("_valid", False):
            all_valid = False

    order["items"] = validated_items
    order["_all_valid"] = all_valid
    return order


def _coerce_year(value):
    """Best-effort conversion for LLM year values (e.g., '2020', 2020.0, '2020-01-01')."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
        return None


def _normalize_item_year_fields(item: dict) -> None:
    """Normalize year fields on order item in place."""
    year = _coerce_year(item.get("year"))
    year_start = _coerce_year(item.get("year_start"))
    year_end = _coerce_year(item.get("year_end"))

    if year is not None:
        item["year"] = year
    if year_start is not None:
        item["year_start"] = year_start
    if year_end is not None:
        item["year_end"] = year_end


def _build_currency_fallback_order(hints: dict | None) -> dict | None:
    """
    Build a narrow fallback FX order when LLM returns chat for an analytical currency query.
    This is intentionally constrained to avoid hijacking genuine conversational asks.
    """
    if not hints:
        return None

    query = str(hints.get("original_query") or "").strip()
    if not query:
        return None

    query_lower = query.lower()
    currency_terms = ("currency", "fx", "exchange rate", "usd", "yen", "lira", "peso")
    analytics_terms = (
        "compare", "vs", "against", "drop", "depreciat", "appreciat",
        "volatility", "trend", "moved", "move", "above", "below", "over the last"
    )
    fact_lookup_terms = ("what currency", "currency of", "uses which currency", "monetary unit")
    mixed_source_terms = (
        "inflation", "earthquake", "earthquakes", "hurricane", "hurricanes", "cyclone", "cyclones",
        "tornado", "tornadoes", "wildfire", "wildfires", "flood", "floods", "tsunami", "tsunamis",
        "volcano", "volcanoes", "disaster", "disasters", "factbook", "sdg", "health", "co2"
    )
    broad_scope_terms = ("all countries", "every country", "global", "worldwide")

    if not any(t in query_lower for t in currency_terms):
        return None
    if any(t in query_lower for t in fact_lookup_terms) and not any(t in query_lower for t in analytics_terms):
        return None
    if not any(t in query_lower for t in analytics_terms):
        return None
    if any(t in query_lower for t in mixed_source_terms):
        return None

    item = {
        "source_id": "fx_usd_historical",
        "metric": "local_per_usd",
        "metric_label": "Local currency per USD",
        "region": None,
    }

    location = hints.get("location") or {}
    iso3 = location.get("iso3")
    if iso3:
        item["region"] = iso3
    elif not any(t in query_lower for t in broad_scope_terms):
        return None

    time_hint = hints.get("time") or {}
    year = _coerce_year(time_hint.get("year"))
    year_start = _coerce_year(time_hint.get("year_start"))
    year_end = _coerce_year(time_hint.get("year_end"))

    if year_start is not None and year_end is not None:
        item["year_start"] = year_start
        item["year_end"] = year_end
    elif year is not None:
        item["year"] = year
    elif "last decade" in query_lower or "10-year" in query_lower or "10 year" in query_lower:
        current_year = datetime.utcnow().year
        item["year_start"] = current_year - 10
        item["year_end"] = current_year

    order = {
        "items": [item],
        "summary": "FX trend analysis request",
    }
    return validate_order(order)


def _build_sdg_fallback_order(hints: dict | None) -> dict | None:
    """
    Build a compact SDG fallback order for broad natural-language prompts that
    clearly imply a default metric or comparison.
    """
    if not hints:
        return None

    query = str(hints.get("original_query") or "").strip()
    if not query:
        return None

    query_lower = query.lower()
    current_year = datetime.utcnow().year

    def make_item(source_id: str, metric: str, metric_label: str, **extra) -> dict:
        item = {"source_id": source_id, "metric": metric, "metric_label": metric_label}
        item.update({k: v for k, v in extra.items() if v is not None})
        return item

    def make_order(items: list[dict], summary: str) -> dict | None:
        return validate_order({"items": items, "summary": summary})

    if "child mortality" in query_lower:
        return make_order(
            [make_item("03", "child mortality", "Child mortality", sort={"by": "child mortality", "order": "desc", "limit": 20})],
            "SDG 3 child mortality ranking",
        )

    if "access to electricity" in query_lower or "electricity access" in query_lower:
        return make_order(
            [make_item("07", "access to electricity", "Access to electricity", sort={"by": "access to electricity", "order": "desc"})],
            "SDG 7 electricity access",
        )

    if "gender equality" in query_lower:
        return make_order(
            [make_item("05", "gender equality", "Gender equality", sort={"by": "gender equality", "order": "desc", "limit": 20})],
            "SDG 5 gender equality ranking",
        )

    if "gdp growth" in query_lower:
        return make_order(
            [make_item("08", "gdp growth", "GDP growth", year_start=2010, year_end=current_year)],
            "SDG 8 GDP growth since 2010",
        )

    if "reducing inequality" in query_lower or "inequality the fastest" in query_lower:
        return make_order(
            [make_item("10", "inequality", "Inequality", year_start=current_year - 10, year_end=current_year, sort={"by": "inequality", "order": "asc", "limit": 20})],
            "SDG 10 inequality trend",
        )

    if "co2 emissions progress" in query_lower and "sdg 13" in query_lower:
        return make_order(
            [make_item("13", "greenhouse gas emissions", "Greenhouse gas emissions", year_start=2005, year_end=current_year)],
            "SDG 13 emissions trend since 2005",
        )

    if "education enrollment" in query_lower:
        return make_order(
            [make_item("04", "education enrollment", "Education enrollment", year_start=2000, year_end=current_year)],
            "SDG 4 education enrollment trend",
        )

    if "hunger" in query_lower and "sub-saharan africa" in query_lower:
        return make_order(
            [make_item("02", "undernourishment", "Undernourishment", region="sub-saharan africa", year_start=current_year - 10, year_end=current_year)],
            "SDG 2 hunger in Sub-Saharan Africa",
        )

    if "poverty rates" in query_lower and "education access" in query_lower:
        return make_order(
            [
                make_item("01", "poverty rate", "Poverty rate", region="south asia"),
                make_item("04", "education access", "Education access", region="south asia"),
            ],
            "SDG poverty and education comparison for South Asia",
        )

    if "clean water" in query_lower and "health outcomes" in query_lower:
        return make_order(
            [
                make_item("06", "drinking water access", "Clean water access"),
                make_item("03", "health outcomes", "Health outcomes"),
            ],
            "SDG clean water and health comparison",
        )

    if "sdg poverty indicators" in query_lower and "factbook gdp per capita" in query_lower:
        return make_order(
            [
                make_item("01", "poverty rate", "Poverty rate"),
                make_item("world_factbook_overlap", "gdp per capita", "GDP per capita"),
            ],
            "SDG poverty and Factbook GDP comparison",
        )

    if "natural disasters" in query_lower and "poverty scores" in query_lower:
        return make_order(
            [
                make_item("01", "poverty rate", "Poverty rate"),
                make_item("earthquakes", "events", "Earthquake exposure"),
            ],
            "SDG poverty and disaster exposure comparison",
        )

    if "sustainable cities" in query_lower and "worst performing" in query_lower:
        return make_order(
            [make_item("11", "air quality", "Air quality", sort={"by": "air quality", "order": "desc", "limit": 10})],
            "SDG 11 worst-performing countries",
        )

    return None


def parse_llm_response(content: str, hints: dict = None) -> dict:
    """
    Parse LLM response into structured result.

    Handles all response types from LLM:
    - order: Data request
    - navigate: Zoom to location(s)
    - disambiguate: Multiple locations match, need user to pick
    - filter_update: Change overlay filters
    - chat: General response
    - clarify: Need more information
    """
    parsed_json = None

    # Try to extract JSON from response
    if "```json" in content:
        try:
            json_str = content.split("```json")[1].split("```")[0].strip()
            parsed_json = json.loads(json_str)
        except (json.JSONDecodeError, IndexError):
            pass
    elif content.strip().startswith("{"):
        try:
            parsed_json = json.loads(content.strip())
        except json.JSONDecodeError:
            pass

    # If we got valid JSON, route based on type field
    if parsed_json and isinstance(parsed_json, dict):
        response_type = parsed_json.get("type", "order")  # Default to order for backwards compat

        if response_type == "navigate":
            # Navigation request - zoom to location(s)
            return {
                "type": "navigate",
                "locations": parsed_json.get("locations", []),
                "message": parsed_json.get("message", "Navigating to location")
            }

        elif response_type == "geometry_remove":
            # Remove geometry regions from display
            return {
                "type": "geometry_remove",
                "regions": parsed_json.get("regions", []),
                "geometry_type": parsed_json.get("geometry_type", "zcta"),
                "message": parsed_json.get("message", "Removing geometry")
            }

        elif response_type == "disambiguate":
            # Disambiguation needed - multiple locations match
            return {
                "type": "disambiguate",
                "options": parsed_json.get("options", []),
                "message": parsed_json.get("message", "Multiple locations found"),
                "query_term": parsed_json.get("query_term", "location")
            }

        elif response_type == "filter_update":
            # Filter update for disaster overlays
            return {
                "type": "filter_update",
                "overlay": parsed_json.get("overlay", ""),
                "filters": parsed_json.get("filters", {}),
                "message": parsed_json.get("message", "Updating filters")
            }

        elif response_type == "overlay_toggle":
            # Toggle overlay on/off (binary choice, no confidence needed)
            return {
                "type": "overlay_toggle",
                "overlay": parsed_json.get("overlay", ""),
                "enabled": parsed_json.get("enabled", True),
                "message": parsed_json.get("message", "")
            }

        elif response_type == "chat":
            # General chat response
            fallback_order = _build_currency_fallback_order(hints)
            if fallback_order:
                return {
                    "type": "order",
                    "order": fallback_order,
                    "summary": fallback_order.get("summary", "FX trend analysis request"),
                }
            return {
                "type": "chat",
                "message": parsed_json.get("message", "")
            }

        elif response_type == "clarify":
            # Need more information
            message = parsed_json.get("message", "Could you provide more details?")
            message = _improve_clarify_message(message, hints)
            return {"type": "clarify", "message": message}

        else:
            # Default: treat as order (type == "order" or legacy format without type)
            order = validate_order(parsed_json)
            return {
                "type": "order",
                "order": order,
                "summary": order.get("summary", "Data request")
            }

    # No valid JSON - check if it's a clarifying question
    if "?" in content and len(content) < 200:
        message = _improve_clarify_message(content, hints)
        return {"type": "clarify", "message": message}

    # Otherwise it's a chat response
    fallback_order = _build_currency_fallback_order(hints)
    if fallback_order:
        return {
            "type": "order",
            "order": fallback_order,
            "summary": fallback_order.get("summary", "FX trend analysis request"),
        }
    return {"type": "chat", "message": content}


def _improve_clarify_message(message: str, hints: dict = None) -> str:
    """
    If the clarify message is too generic, improve it based on what we know is missing.

    Enhanced to be context-aware for disaster queries:
    - Suggests enabling overlays when disaster keywords detected but no overlay active
    - Uses viewport context to suggest relevant locations
    - Explains loc_prefix vs affected_loc_id when relevant
    """
    # Generic phrases that should be improved
    generic_phrases = [
        "could you be more specific",
        "can you be more specific",
        "please be more specific",
        "i need more information",
        "what do you mean",
        "could you clarify",
    ]

    message_lower = message.lower()
    is_generic = any(phrase in message_lower for phrase in generic_phrases)

    if not is_generic or not hints:
        return message

    # Check for disaster overlay intent without active overlay
    overlay_intent = hints.get("overlay_intent")
    active_overlays = hints.get("active_overlays", {})
    overlay_type = active_overlays.get("type")

    if overlay_intent and overlay_intent.get("action") == "enable":
        overlay = overlay_intent.get("overlay", "disaster")
        return f"Would you like me to turn on the {overlay} overlay to see this data?"

    # Check viewport for location suggestion
    viewport = hints.get("viewport")
    location = hints.get("location")
    navigation = hints.get("navigation")
    has_location = location or (navigation and navigation.get("locations"))

    # If disaster query but no location specified, suggest based on viewport
    if overlay_intent and not has_location:
        overlay = overlay_intent.get("overlay", "disaster")
        if viewport and viewport.get("bounds"):
            zoom = viewport.get("zoom", 0)
            if zoom >= 3:
                # User is zoomed in - suggest using their view location
                return f"Which location would you like to see {overlay} for? I can show data for the area you are currently viewing."

        # Ask about location type preference
        return f"Do you want {overlay} that occurred IN a specific location, or {overlay} that AFFECTED a specific location?"

    # If overlay is active and user seems confused about filters
    if overlay_type:
        filters = active_overlays.get("filters", {})
        if filters:
            filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
            return f"The {overlay_type} overlay is currently filtered to: {filter_desc}. What would you like to change?"
        else:
            return f"What filter would you like to apply to the {overlay_type} overlay? (e.g., magnitude, category, location)"

    # Fallback: analyze what's missing based on hints
    missing = []

    if not has_location:
        missing.append("location/country")

    # Check if topics/metrics are clear
    topics = hints.get("topics", [])
    if not topics:
        missing.append("metric/data type")

    # Build improved message
    if missing:
        if len(missing) == 1:
            return f"Which {missing[0]} would you like to see data for?"
        else:
            return f"Could you specify the {' and '.join(missing)}?"

    return message
