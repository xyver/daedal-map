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
from pathlib import Path
from anthropic import Anthropic
from dotenv import load_dotenv

from .data_loading import load_catalog, load_source_metadata, get_source_path
from .preprocessor import build_tier3_context, build_tier4_context
from .constants import CHAT_HISTORY_LLM_LIMIT
from .llm_tools import format_tools_for_provider, execute_tool, format_tool_result_for_llm

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


def build_system_prompt(catalog: dict, conversions: dict) -> str:
    """
    Build system prompt with catalog organized by geographic scope.

    Groups sources by scope and combines related sources (UN SDGs, World Factbook).
    """

    # Group sources by scope
    sources_by_scope = {}
    for src in catalog["sources"]:
        scope = src.get("scope", "global")
        if scope not in sources_by_scope:
            sources_by_scope[scope] = []
        sources_by_scope[scope].append(src)

    # Build sources text with grouping of related sources
    sources_text = ""

    def format_source_group(sources, scope_label):
        """Format sources, grouping related ones together."""
        lines = []

        # Separate UN SDGs and World Factbook for grouping
        sdg_sources = [s for s in sources if s.get('source_id') and s['source_id'].startswith('un_sdg_')]
        factbook_sources = [s for s in sources if s.get('source_id') and 'world_factbook' in s['source_id']]
        other_sources = [s for s in sources if s.get('source_id') and s not in sdg_sources and s not in factbook_sources]

        # Add individual sources with human-readable names AND source_id
        for src in other_sources:
            temp = src.get("temporal_coverage", {})
            name = src.get("source_name", src["source_id"])
            sid = src["source_id"]
            # Show both name and source_id so LLM knows exact ID to use
            lines.append(f"- {name} [source_id: {sid}]: {temp.get('start', '?')}-{temp.get('end', '?')}")

        # List UN SDGs individually with human-readable goal titles
        if sdg_sources:
            # Sort by source_id to show in order (un_sdg_01, un_sdg_02, etc.)
            sdg_sources_sorted = sorted(sdg_sources, key=lambda s: s.get('source_id', ''))
            for src in sdg_sources_sorted:
                sid = src.get('source_id', '')
                temp = src.get("temporal_coverage", {})
                year_range = f"{temp.get('start', '?')}-{temp.get('end', '?')}"

                # Get goal title - try catalog first, then fall back to file loading
                goal_title = None

                # Option 1: Check catalog's reference data (if catalog was rebuilt)
                reference = src.get("reference", {})
                if reference.get("goal"):
                    goal_info = reference["goal"]
                    goal_num = goal_info.get("number", "")
                    goal_name = goal_info.get("name", "")
                    if goal_num and goal_name:
                        goal_title = f"SDG {goal_num}: {goal_name}"

                # Option 2: Load reference.json directly (fallback for older catalogs)
                if not goal_title:
                    try:
                        source_path = get_source_path(sid)
                        if source_path:
                            ref_path = source_path / "reference.json"
                            if ref_path.exists():
                                with open(ref_path, encoding='utf-8') as f:
                                    ref_data = json.load(f)
                                goal_info = ref_data.get("goal", {})
                                goal_num = goal_info.get("number", "")
                                goal_name = goal_info.get("name", "")
                                if goal_num and goal_name:
                                    goal_title = f"SDG {goal_num}: {goal_name}"
                    except Exception:
                        pass

                # Option 3: Fall back to generic name
                if not goal_title:
                    try:
                        goal_num = int(sid.split('_')[-1])
                        goal_title = f"SDG {goal_num}"
                    except:
                        goal_title = src.get("source_name", sid)

                lines.append(f"- {goal_title} [source_id: {sid}]: {year_range}")

        # Group World Factbook
        if factbook_sources:
            names = [s['source_id'] for s in factbook_sources]
            lines.append(f"- World Factbook ({', '.join(names)}): country profiles, demographics, infrastructure")

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

REGIONS:
{regions_text}

WHEN USER ASKS "what data for [country]" or "what do you have":
1. List that country's specific sources FIRST (if any)
2. Then mention global sources are also available
3. Be CONCISE - use human-readable names, group related sources

WHEN USER ASKS about a specific source ("what's in X?" or "show me metrics"):
- Use the list_source_metrics tool to get the actual metrics
- List the available metrics using ONLY the human-readable names (never show column names)
- If there are 10 or fewer metrics, list them all
- If there are more than 10, say "There are X metrics available, here are the key ones:" and show 5-8
- Mention the year range available
- Say "I can get them all" or "I can show any of these" (never mention "*" or wildcards to the user)

ORDER FORMAT (JSON when user requests data):
```json
{{"items": [{{"source_id": "owid_co2", "metric": "co2", "region": "europe", "year": 2022}}], "summary": "CO2 for Europe 2022"}}
```

RULES:
- source_id: Must EXACTLY match one of the available sources
- metric: Must be an EXACT column name from the source, OR use "*" for ALL metrics from that source
- region: lowercase (europe, g7, australia) or null for global
- year: null = most recent

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

2. NAVIGATION - User wants to zoom/navigate to a location (no data):
```json
{{"type": "navigate", "locations": [{{"loc_id": "USA-CA", "name": "California"}}], "message": "Zooming to California"}}
```

3. DISAMBIGUATION - Multiple locations match, need user to pick:
```json
{{"type": "disambiguate", "message": "Which Washington did you mean?", "options": [{{"loc_id": "USA-WA", "name": "Washington State"}}, {{"loc_id": "USA-DC", "name": "Washington DC"}}]}}
```

4. FILTER UPDATE - User wants to change disaster overlay filters:
```json
{{"type": "filter_update", "overlay": "earthquakes", "filters": {{"minMagnitude": 5.0}}, "message": "Filtering to magnitude 5+"}}
```

5. OVERLAY TOGGLE - User wants to enable/disable a disaster overlay:
```json
{{"type": "overlay_toggle", "overlay": "earthquakes", "enabled": true, "message": "Enabling earthquakes overlay"}}
```
Overlays: earthquakes, hurricanes, volcanoes, tsunamis, tornadoes, wildfires, floods

6. CHAT - General response, information, or clarifying question:
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
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", "")
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

    # Check metric exists
    metrics = metadata.get("metrics", {})
    # Skip wildcard metrics - they'll be expanded or handled by the postprocessor
    if metric in ("*", "all", "all_metrics"):
        item["_valid"] = True
        return item
    if metric and metric not in metrics:
        # Try to find close match
        close_matches = [k for k in metrics.keys() if metric.lower() in k.lower() or k.lower() in metric.lower()]
        if close_matches:
            item["_valid"] = False
            item["_error"] = f"Column '{metric}' not found. Did you mean: {', '.join(close_matches[:3])}?"
        else:
            item["_valid"] = False
            item["_error"] = f"Column '{metric}' not found in {source_id}"
        return item

    # Check year is in range
    temp = metadata.get("temporal_coverage", {})
    start_year = temp.get("start")
    end_year = temp.get("end")

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

    # Valid - add metric label if missing
    if metric and not item.get("metric_label"):
        metric_info = metrics.get(metric, {})
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
