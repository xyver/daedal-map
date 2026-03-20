"""
Postprocessor - validates orders and expands derived fields.

Runs AFTER the LLM call and:
1. Validates each order item against catalog
2. Expands derived field shortcuts (per_capita, density, etc.)
3. Expands cross-source derived fields
4. Returns processed order with validation results

The postprocessor ensures:
- All items reference valid sources and metrics
- Derived fields are expanded into component items + calculation spec
- Items marked for_derivation are hidden from user display
"""

import json
from pathlib import Path
from typing import Optional

from .data_loading import load_catalog, load_source_metadata


# =============================================================================
# Derived Field Expansion Tables
# =============================================================================

# Shortcut expansions for common derived fields
DERIVED_EXPANSIONS = {
    "per_capita": {
        "denominator": "population",
        "denominator_source": "owid_co2",  # Canonical source for population
        "label_suffix": "Per Capita",
    },
    "density": {
        "denominator": "area_sq_km",
        "denominator_source": "world_factbook_static",  # Static area data
        "label_suffix": "Density",
    },
    "per_1000": {
        "denominator": "population",
        "denominator_source": "owid_co2",
        "multiplier": 1000,
        "label_suffix": "Per 1000",
    },
}

# =============================================================================
# Validation
# =============================================================================

def validate_item(item: dict, catalog: dict) -> dict:
    """
    Validate an order item against catalog.

    Returns item with validation fields added:
    - _valid: bool
    - _error: str (if invalid)
    - metric_label: str (if valid)
    """
    source_id = item.get("source_id")
    metric = item.get("metric")

    # Skip derived_result items - they're calculated, not fetched
    if item.get("type") == "derived_result":
        item["_valid"] = True
        return item

    # Skip derived items that need expansion first
    if item.get("type") == "derived":
        item["_valid"] = True
        item["_needs_expansion"] = True
        return item

    # Skip event mode items - they don't require metric validation
    if item.get("mode") == "events":
        item["_valid"] = True
        return item

    if not source_id:
        item["_valid"] = False
        item["_error"] = "Missing source_id"
        return item

    # Check source exists in catalog (sources is a list)
    sources = catalog.get("sources", [])
    source_ids = [s.get("source_id") for s in sources] if isinstance(sources, list) else list(sources.keys())
    if source_id not in source_ids:
        item["_valid"] = False
        item["_error"] = f"Unknown source: {source_id}"
        return item

    # Load full metadata for metric validation
    metadata = load_source_metadata(source_id)
    if not metadata:
        # Source in catalog but no metadata file - still valid
        item["_valid"] = True
        return item

    # Check metric exists (case-insensitive matching with auto-correction)
    metrics = metadata.get("metrics", {})
    if metric and metric not in metrics:
        # Try case-insensitive exact match on key first
        metric_lower = metric.lower()
        exact_match = None
        for k in metrics.keys():
            if k.lower() == metric_lower:
                exact_match = k
                break

        # If no key match, try matching by display name
        if not exact_match:
            for k, v in metrics.items():
                if isinstance(v, dict):
                    name = v.get("name", "")
                    if name.lower() == metric_lower:
                        exact_match = k
                        break

        if exact_match:
            # Auto-correct to the actual metric key
            item["metric"] = exact_match
            metric = exact_match
        else:
            # No exact match, suggest close matches (by key or name)
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
                item["_error"] = f"Metric '{metric}' not found. Did you mean: {', '.join(close_matches[:3])}?"
            else:
                item["_valid"] = False
                item["_error"] = f"Metric '{metric}' not found in {source_id}"
            return item

    # Add metric label
    if metric:
        metric_info = metrics.get(metric, {})
        name = metric_info.get("name", metric)
        unit = metric_info.get("unit", "")
        if unit and unit != "unknown":
            item["metric_label"] = f"{name} ({unit})"
        else:
            item["metric_label"] = name

    item["_valid"] = True
    return item


# =============================================================================
# Wildcard Metric Expansion
# =============================================================================

def expand_wildcard_metrics(items: list) -> list:
    """
    Expand wildcard metrics (metric: "*" or metric: "all") into individual items.

    When LLM outputs {"source_id": "abs_population", "metric": "*", "region": "australia"},
    this expands it into one item per actual metric in that source's metadata.

    This allows the LLM to express "all metrics from this source" without needing
    to know every metric name, keeping the prompt small while enabling full access.
    """
    expanded = []

    for item in items:
        # Skip event mode items - they don't use metrics, "*" means "all events"
        if item.get("mode") == "events":
            expanded.append(item)
            continue

        metric = item.get("metric")

        # Check for wildcard
        if metric in ("*", "all", "all_metrics"):
            source_id = item.get("source_id")
            if not source_id:
                # Can't expand without knowing the source
                expanded.append(item)
                continue

            # Load full metadata for this source
            metadata = load_source_metadata(source_id)
            if not metadata or not metadata.get("metrics"):
                # No metadata found, keep original item (will fail validation)
                expanded.append(item)
                continue

            # Create one item per metric, using per-metric year ranges from metadata
            metrics = metadata.get("metrics", {})
            for metric_key, metric_info in metrics.items():
                new_item = {
                    "source_id": source_id,
                    "metric": metric_key,
                    "region": item.get("region"),
                }

                # Use per-metric year range if available in metadata
                # metadata.metrics.{metric}.years = [start, end]
                metric_years = metric_info.get("years")
                if metric_years and len(metric_years) == 2:
                    new_item["year_start"] = metric_years[0]
                    new_item["year_end"] = metric_years[1]
                else:
                    # Fallback to item-level years if no per-metric range
                    if item.get("year"):
                        new_item["year"] = item.get("year")
                    if item.get("year_start"):
                        new_item["year_start"] = item.get("year_start")
                    if item.get("year_end"):
                        new_item["year_end"] = item.get("year_end")

                # Remove None values
                new_item = {k: v for k, v in new_item.items() if v is not None}
                expanded.append(new_item)

            # Log expansion for debugging
            import logging
            logging.getLogger(__name__).info(
                f"Expanded wildcard metric for {source_id}: {len(metrics)} metrics"
            )
        else:
            # Not a wildcard, keep as-is
            expanded.append(item)

    return expanded


# =============================================================================
# Derived Field Expansion
# =============================================================================

def expand_derived_shortcut(item: dict) -> list:
    """
    Expand a derived shortcut (e.g., derived: "per_capita") into component items.

    Input: {"source_id": "owid_co2", "metric": "gdp", "region": "EU", "derived": "per_capita"}

    Output: [
        {"source_id": "owid_co2", "metric": "gdp", "region": "EU", "for_derivation": True},
        {"source_id": "owid_co2", "metric": "population", "region": "EU", "for_derivation": True},
        {"type": "derived_result", "numerator": "gdp", "denominator": "population", "label": "GDP Per Capita"}
    ]
    """
    derived_type = item.get("derived")
    if not derived_type or derived_type not in DERIVED_EXPANSIONS:
        return [item]  # Return unchanged if not a known shortcut

    expansion = DERIVED_EXPANSIONS[derived_type]
    source_id = item.get("source_id")
    metric = item.get("metric")
    region = item.get("region")
    year = item.get("year")
    year_start = item.get("year_start")
    year_end = item.get("year_end")

    # Build base item properties
    base_props = {"region": region}
    if year:
        base_props["year"] = year
    if year_start:
        base_props["year_start"] = year_start
    if year_end:
        base_props["year_end"] = year_end

    expanded = []

    # 1. Numerator item (the original metric)
    numerator_item = {
        "source_id": source_id,
        "metric": metric,
        "for_derivation": True,
        **base_props
    }
    expanded.append(numerator_item)

    # 2. Denominator item (from canonical source)
    denom_metric = expansion["denominator"]
    denom_source = expansion.get("denominator_source", source_id)
    denominator_item = {
        "source_id": denom_source,
        "metric": denom_metric,
        "for_derivation": True,
        **base_props
    }
    expanded.append(denominator_item)

    # 3. Derived result specification
    label = f"{metric} {expansion['label_suffix']}"
    derived_result = {
        "type": "derived_result",
        "numerator": metric,
        "denominator": denom_metric,
        "label": label,
    }
    if expansion.get("multiplier"):
        derived_result["multiplier"] = expansion["multiplier"]
    expanded.append(derived_result)

    return expanded


def expand_cross_source_derived(item: dict) -> list:
    """
    Expand a cross-source derived field into component items.

    Input: {
        "type": "derived",
        "numerator": {"source_id": "owid_co2", "metric": "gdp"},
        "denominator": {"source_id": "imf_bop", "metric": "exports"},
        "region": "EU"
    }

    Output: [
        {"source_id": "owid_co2", "metric": "gdp", "region": "EU", "for_derivation": True},
        {"source_id": "imf_bop", "metric": "exports", "region": "EU", "for_derivation": True},
        {"type": "derived_result", "numerator": "gdp", "denominator": "exports", "label": "GDP/Exports"}
    ]
    """
    if item.get("type") != "derived":
        return [item]

    numerator = item.get("numerator", {})
    denominator = item.get("denominator", {})
    region = item.get("region")
    year = item.get("year")
    year_start = item.get("year_start")
    year_end = item.get("year_end")

    # Handle simple string numerator/denominator (same source assumed)
    if isinstance(numerator, str):
        numerator = {"metric": numerator}
    if isinstance(denominator, str):
        denominator = {"metric": denominator}

    # Build base item properties
    base_props = {"region": region}
    if year:
        base_props["year"] = year
    if year_start:
        base_props["year_start"] = year_start
    if year_end:
        base_props["year_end"] = year_end

    expanded = []

    # 1. Numerator item
    num_source = numerator.get("source_id", item.get("source_id"))
    num_metric = numerator.get("metric")
    if num_source and num_metric:
        expanded.append({
            "source_id": num_source,
            "metric": num_metric,
            "for_derivation": True,
            **base_props
        })

    # 2. Denominator item
    denom_source = denominator.get("source_id", item.get("source_id"))
    denom_metric = denominator.get("metric")
    if denom_source and denom_metric:
        expanded.append({
            "source_id": denom_source,
            "metric": denom_metric,
            "for_derivation": True,
            **base_props
        })

    # 3. Derived result
    label = item.get("label", f"{num_metric}/{denom_metric}")
    derived_result = {
        "type": "derived_result",
        "numerator": num_metric,
        "denominator": denom_metric,
        "label": label,
    }
    if item.get("multiplier"):
        derived_result["multiplier"] = item["multiplier"]
    expanded.append(derived_result)

    return expanded


def expand_all_derived_fields(items: list) -> list:
    """
    Expand all derived fields in an items list.

    Handles both:
    - Shortcut syntax: {"derived": "per_capita"}
    - Cross-source syntax: {"type": "derived", "numerator": {...}, "denominator": {...}}
    """
    expanded = []

    for item in items:
        # Check for shortcut syntax first
        if item.get("derived") and item.get("derived") in DERIVED_EXPANSIONS:
            expanded.extend(expand_derived_shortcut(item))

        # Check for cross-source syntax
        elif item.get("type") == "derived":
            expanded.extend(expand_cross_source_derived(item))

        # Regular item - keep as is
        else:
            expanded.append(item)

    return expanded


# =============================================================================
# Event Mode Detection
# =============================================================================

# Source IDs that support event mode (individual events vs aggregates)
EVENT_SOURCES = {
    "earthquakes": "events",
    "floods": "events",
    "hurricanes": "storms",
    "landslides": "events",
    "tornadoes": "events",
    "tsunamis": "events",
    "volcanoes": "events",
    "wildfires": "fires",
}


def detect_event_mode(items: list, hints: dict = None) -> list:
    """
    Detect if items should use event mode instead of aggregate mode.

    Event mode is triggered when:
    1. Source has an events file (events.parquet, fires.parquet, etc.)
    2. Query intent suggests viewing individual events (not aggregates)

    Query intent detection:
    - "show me earthquakes" / "display wildfires" -> EVENT mode (markers on map)
    - "how many earthquakes" / "count of fires" -> AGGREGATE mode (county choropleth)

    Adds mode: "events" to items that should use event display.
    """
    query = ""
    if hints:
        query = hints.get("original_query", "").lower()

    # Patterns that suggest user wants to SEE individual events
    event_display_patterns = [
        "show me", "show the", "display", "map of", "map the",
        "where are", "where were", "where did", "where have",
        "which", "what", "list", "find",
        "struck", "hit", "affected", "impacted",
        "occurred", "happened",
        "magnitude", "category", "m4", "m5", "m6", "m7",  # Specific magnitude
        "cat 1", "cat 2", "cat 3", "cat 4", "cat 5",      # Hurricane categories
    ]

    # Patterns that suggest user wants AGGREGATE counts/statistics
    aggregate_patterns = [
        "how many", "how much", "count", "total", "number of",
        "statistics", "stats", "average", "sum",
        "per year", "annually", "yearly", "over time",
        "trend", "compare", "frequency", "exposure",
        "per capita", "historically", "highest", "most",
        "last 10 years", "last 20 years", "last 30 years",
        "past 10 years", "past 20 years", "past 30 years",
        "rolling", "between the 1990s", "between the 2010s"
    ]

    geography_aggregate_terms = [
        "counties", "county", "countries", "country",
        "regions", "region", "areas"
    ]

    # Determine intent from query
    wants_events = any(p in query for p in event_display_patterns)
    wants_aggregate = any(p in query for p in aggregate_patterns)

    # If both or neither detected, use heuristics
    if wants_events == wants_aggregate:
        # Check for event-type nouns as main subject (default to events for disaster queries)
        event_nouns = ["earthquake", "quake", "volcano", "eruption", "wildfire",
                      "fire", "hurricane", "cyclone", "storm", "tsunami", "tornado"]
        has_event_noun = any(noun in query for noun in event_nouns)
        has_geo_agg = any(term in query for term in geography_aggregate_terms)
        # If user is asking for regions/counties/countries plus a disaster noun,
        # prefer aggregate choropleth behavior over raw event display.
        if has_event_noun and has_geo_agg:
            wants_aggregate = True
            wants_events = False
        else:
            # Default: if disaster noun present without aggregate words, show events
            wants_events = has_event_noun

    updated_items = []

    for item in items:
        source_id = item.get("source_id", "")

        # Check if source supports events
        event_file_key = EVENT_SOURCES.get(source_id)

        if event_file_key and wants_events and not wants_aggregate:
            # Check if metric explicitly requests aggregate
            metric = item.get("metric", "")
            explicit_aggregate = metric and any(
                agg in metric.lower() for agg in ["count", "total", "sum", "avg", "mean"]
            )

            if not explicit_aggregate:
                # Add event mode
                item["mode"] = "events"
                item["event_file"] = event_file_key
                # Remove metric if it's just a placeholder
                if metric in ("*", "all", "all_metrics", ""):
                    item.pop("metric", None)
        elif event_file_key and wants_aggregate:
            item["mode"] = "aggregate"
            item.pop("event_file", None)

            metric = item.get("metric", "")
            metric_lower = str(metric).lower() if metric is not None else ""

            # Default aggregate metric for broad hazard-frequency requests.
            if metric_lower in ("", "*", "all", "all_metrics"):
                item["metric"] = "event_count"
            elif metric_lower in {"tornado_count", "earthquake_count", "hurricane_count", "wildfire_count", "tsunami_count", "volcano_count", "flood_count"}:
                item["metric"] = "event_count"
            elif "frequency" in query and metric_lower not in {"event_count", "deaths", "injuries"}:
                item["metric"] = "event_count"

            # Annotate rolling-window intent so executor can choose aggregate files directly.
            if item.get("aggregate_use_rolling") is None and ("rolling" in query or "last 10 years" in query or "past 10 years" in query):
                item["aggregate_use_rolling"] = True
                item["aggregate_window_years"] = 10
            elif item.get("aggregate_use_rolling") is None and ("last 20 years" in query or "past 20 years" in query):
                item["aggregate_use_rolling"] = True
                item["aggregate_window_years"] = 20

            # Trend/history queries often want accumulated historical aggregate output
            # rather than raw yearly/event display.
            if "historically" in query:
                item["aggregate_all_years"] = True

            # Country-level wording should roll admin2 aggregates up to admin0.
            if "countries" in query or "country" in query:
                item["aggregate_rollup_level"] = "admin_0"
            elif "counties" in query or "county" in query:
                item["aggregate_rollup_level"] = "admin_2"

        updated_items.append(item)

    return updated_items


# =============================================================================
# Main Postprocessor
# =============================================================================

def postprocess_order(order: dict, hints: dict = None) -> dict:
    """
    Main postprocessor function.

    Takes an order from the LLM and:
    1. Injects time range from preprocessor hints
    2. Expands derived fields
    3. Validates all items
    4. Returns processed order with validation results

    Args:
        order: The order dict from LLM (with "items" list)
        hints: Preprocessor hints (for context if needed)

    Returns:
        Processed order with:
        - items: list of validated items (may be expanded)
        - derived_specs: list of derived calculation specs
        - validation_summary: str describing validation results
    """
    catalog = load_catalog()
    items = order.get("items", [])

    # Step 0: Inject time range from preprocessor hints if LLM left year as null
    time_hints = hints.get("time", {}) if hints else {}
    if time_hints.get("is_time_series"):
        for item in items:
            # If year is null/None and no year_start/year_end, inject time range
            if item.get("year") is None and not item.get("year_start") and not item.get("year_end"):
                # Case 1: Preprocessor detected specific year range (e.g., "from 2010 to now")
                if time_hints.get("year_start") and time_hints.get("year_end"):
                    item["year_start"] = time_hints["year_start"]
                    item["year_end"] = time_hints["year_end"]
                # Case 2: Trend detected but no specific years (e.g., "all years")
                # Look up the source metadata to get actual available range
                elif time_hints.get("pattern_type") == "trend" and item.get("source_id"):
                    metadata = load_source_metadata(item["source_id"])
                    if metadata:
                        temp = metadata.get("temporal_coverage", {})
                        if temp.get("start") and temp.get("end"):
                            item["year_start"] = temp["start"]
                            item["year_end"] = temp["end"]

    # Step 1: Detect event mode for disaster/event sources
    items = detect_event_mode(items, hints)

    # Step 2: Expand wildcard metrics (metric: "*" -> all metrics from source)
    items = expand_wildcard_metrics(items)

    # Step 2b: Check metric count for display warning
    METRIC_DISPLAY_WARN = 15
    metric_count = sum(1 for item in items if item.get("type") != "derived_result")

    # Step 3: Expand derived fields
    expanded_items = expand_all_derived_fields(items)

    # Step 4: Separate derived specs from regular items
    regular_items = []
    derived_specs = []

    for item in expanded_items:
        if item.get("type") == "derived_result":
            derived_specs.append(item)
        else:
            regular_items.append(item)

    # Step 4: Validate regular items
    validated_items = []
    errors = []
    valid_count = 0

    for item in regular_items:
        validated = validate_item(item, catalog)
        validated_items.append(validated)
        if validated.get("_valid"):
            valid_count += 1
        else:
            errors.append(validated.get("_error", "Unknown error"))

    # Build validation summary
    total = len(validated_items)
    if errors:
        summary = f"{valid_count}/{total} items valid. Errors: {'; '.join(errors)}"
    else:
        summary = f"All {total} items validated successfully"

    # Build metric warning if count exceeds threshold
    metric_warning = None
    if metric_count > METRIC_DISPLAY_WARN:
        metric_warning = {
            "count": metric_count,
            "message": f"Your request has {metric_count} metrics. More than 15 is hard to display well in popups. Would you like all of them in your order?"
        }

    # Return processed order
    result = {
        "items": validated_items,
        "derived_specs": derived_specs,
        "validation_summary": summary,
        "all_valid": len(errors) == 0,
        # Preserve original order fields
        "summary": order.get("summary"),
        "region": order.get("region"),
        "year": order.get("year"),
        "year_start": order.get("year_start"),
        "year_end": order.get("year_end"),
    }
    if metric_warning:
        result["metric_warning"] = metric_warning
    return result


def get_display_items(items: list, derived_specs: list = None) -> list:
    """
    Get items for display in the order panel.

    Filters out items with for_derivation=True.
    Adds display representations for derived specs.
    """
    display = []

    # Add non-derivation regular items
    for item in items:
        if not item.get("for_derivation"):
            display.append(item)

    # Add display items for derived specs
    if derived_specs:
        for spec in derived_specs:
            display.append({
                "type": "derived",
                "metric": spec.get("label", "Derived"),
                "metric_label": f"{spec.get('label', 'Derived')} (calculated)",
                "_valid": True,
                "_is_derived": True,
            })

    return display


def format_validation_messages(order: dict) -> list:
    """
    Format validation results as chat messages.

    Returns list of strings for display to user.
    """
    messages = []
    items = order.get("items", [])

    for item in items:
        if item.get("for_derivation"):
            continue  # Don't show derivation source items

        if item.get("_valid"):
            source = item.get("source_id", "?")
            metric = item.get("metric_label") or item.get("metric", "?")
            messages.append(f"+ {metric}: Found in {source}")
        else:
            metric = item.get("metric", "?")
            error = item.get("_error", "Unknown error")
            messages.append(f"- {metric}: {error}")

    # Add derived field info
    derived = order.get("derived_specs", [])
    for spec in derived:
        label = spec.get("label", "Derived")
        messages.append(f"+ {label} (calculated)")

    return messages
