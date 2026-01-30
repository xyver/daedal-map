"""
Order Executor - executes confirmed orders against parquet data.
No LLM calls - direct data operations.

Implements the "Empty Box" model from CHAT_REDESIGN.md:
1. Expand regions to loc_ids
2. Create empty boxes for each location
3. Process each order item independently (may be from different sources)
4. Fill boxes with values from each source
5. Join with geometry
6. Return GeoJSON with all filled properties
"""

import pandas as pd
import json
from pathlib import Path
from typing import Optional

from .geometry_handlers import (
    load_global_countries,
    load_country_parquet,
    df_to_geojson,
)

from .paths import DATA_ROOT, CATALOG_PATH
from .data_loading import load_source_metadata

CONVERSIONS_PATH = Path(__file__).parent / "conversions.json"
REFERENCE_DIR = Path(__file__).parent / "reference"

# Cache conversions to avoid repeated file reads
_conversions_cache = None
_iso_codes_cache = None
_usa_admin_cache = None
_catalog_cache = None


def _load_catalog() -> dict:
    """Load catalog.json with caching."""
    global _catalog_cache
    if _catalog_cache is None:
        if CATALOG_PATH.exists():
            with open(CATALOG_PATH, encoding='utf-8') as f:
                _catalog_cache = json.load(f)
        else:
            _catalog_cache = {"sources": []}
    return _catalog_cache


def _get_source_data_type(source_id: str) -> str:
    """
    Get data_type for a source from catalog.
    Returns: 'events', 'metrics', 'gridded', or 'metrics' as default.
    """
    catalog = _load_catalog()
    for src in catalog.get("sources", []):
        if src.get("source_id") == source_id:
            return src.get("data_type", "metrics")
    return "metrics"  # Default to metrics if not found


def _get_source_path(source_id: str) -> Path:
    """Get the full path to a source directory using catalog path field."""
    catalog = _load_catalog()
    for source in catalog.get("sources", []):
        if source.get("source_id") == source_id:
            # Use path field if present, otherwise fall back to old structure
            source_path = source.get("path", f"global/{source_id}")
            return DATA_ROOT / source_path

    # Source not in catalog - try old path as fallback
    return DATA_ROOT / "global" / source_id


def _load_conversions() -> dict:
    """Load conversions.json with caching."""
    global _conversions_cache
    if _conversions_cache is None:
        with open(CONVERSIONS_PATH, encoding='utf-8') as f:
            _conversions_cache = json.load(f)
    return _conversions_cache


def _load_iso_codes() -> dict:
    """Load reference/iso_codes.json with caching."""
    global _iso_codes_cache
    if _iso_codes_cache is None:
        iso_path = REFERENCE_DIR / "iso_codes.json"
        if iso_path.exists():
            with open(iso_path, encoding='utf-8') as f:
                _iso_codes_cache = json.load(f)
        else:
            _iso_codes_cache = {}
    return _iso_codes_cache


def _load_usa_admin() -> dict:
    """Load reference/usa_admin.json with caching."""
    global _usa_admin_cache
    if _usa_admin_cache is None:
        usa_path = REFERENCE_DIR / "usa_admin.json"
        if usa_path.exists():
            with open(usa_path, encoding='utf-8') as f:
                _usa_admin_cache = json.load(f)
        else:
            _usa_admin_cache = {}
    return _usa_admin_cache


def load_source_data(source_id: str) -> tuple:
    """
    Load parquet and metadata for a source.

    Returns:
        tuple: (DataFrame, metadata dict)
    """
    source_dir = _get_source_path(source_id)

    # Find parquet file - prefer all_countries.parquet or USA.parquet
    parquet_files = list(source_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet file found for {source_id} in {source_dir}")

    # Prefer specific files over generic names
    parquet_path = parquet_files[0]
    for name in ["all_countries.parquet", "USA.parquet"]:
        candidate = source_dir / name
        if candidate.exists():
            parquet_path = candidate
            break

    df = pd.read_parquet(parquet_path)

    # Load metadata
    meta_path = source_dir / "metadata.json"
    with open(meta_path, encoding='utf-8') as f:
        metadata = json.load(f)

    return df, metadata


def load_event_data(source_id: str, event_file_key: str = "events") -> tuple:
    """
    Load event-level parquet (e.g., events.parquet, fires.parquet) for a source.

    Args:
        source_id: e.g., "usgs_earthquakes", "mtbs_wildfires"
        event_file_key: Key from metadata.files (e.g., "events", "fires", "positions")

    Returns:
        tuple: (DataFrame, metadata dict)
    """
    source_dir = _get_source_path(source_id)
    meta_path = source_dir / "metadata.json"

    with open(meta_path, encoding='utf-8') as f:
        metadata = json.load(f)

    # Get filename from metadata.files
    files_info = metadata.get("files", {})
    file_info = files_info.get(event_file_key)

    if not file_info:
        # Try common event file names as fallback
        fallback_names = ["events.parquet", "fires.parquet", "positions.parquet"]
        for name in fallback_names:
            candidate = source_dir / name
            if candidate.exists():
                df = pd.read_parquet(candidate)
                return df, metadata
        raise ValueError(f"No event file '{event_file_key}' found in {source_id}")

    # Get filename - handle both 'name' and 'filename' keys
    filename = file_info.get("name") or file_info.get("filename")
    if not filename:
        raise ValueError(f"No filename specified for '{event_file_key}' in {source_id}")

    parquet_path = source_dir / filename
    if not parquet_path.exists():
        raise ValueError(f"Event file not found: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    return df, metadata


def expand_region(region: str) -> set:
    """
    Expand a region name to a set of country codes (ISO3).

    Supports:
    - Region aliases (e.g., "europe" -> WHO_European_Region countries)
    - Direct grouping names (e.g., "European_Union")
    - Single country names (returns that country code)
    - "global" or null -> empty set (means no filtering)

    Returns:
        set: Country codes (ISO3), or empty set for global/all
    """
    if not region or region.lower() in ("global", "all", "world"):
        return set()

    conversions = _load_conversions()
    region_lower = region.lower()

    # Check region_aliases first (maps friendly names to grouping keys)
    region_aliases = conversions.get("region_aliases", {})
    for alias, grouping_key in region_aliases.items():
        if alias.lower() == region_lower:
            grouping = conversions.get("regional_groupings", {}).get(grouping_key, {})
            return set(grouping.get("countries", []))

    # Check direct grouping names
    regional_groupings = conversions.get("regional_groupings", {})
    for key, grouping in regional_groupings.items():
        if key.lower() == region_lower or key.lower().replace("_", " ") == region_lower:
            return set(grouping.get("countries", []))

    # Check if it's a country name -> return its ISO3 code
    iso_data = _load_iso_codes()
    iso3_to_name = iso_data.get("iso3_to_name", {})
    for code, name in iso3_to_name.items():
        if name.lower() == region_lower:
            return {code}

    # Check if it's already an ISO3 code
    if region.upper() in iso3_to_name:
        return {region.upper()}

    # Check US state abbreviations for state-level queries
    usa_admin = _load_usa_admin()
    state_abbrevs = usa_admin.get("state_abbreviations", {})
    for abbrev, name in state_abbrevs.items():
        if name.lower() == region_lower or abbrev.lower() == region_lower:
            # Return special marker for US state filtering
            return {f"USA-{abbrev}"}

    return set()


def find_metric_column(df: pd.DataFrame, metric: str) -> Optional[str]:
    """
    Find matching column name for a metric (fuzzy match).

    Returns:
        Column name or None if not found
    """
    metric_lower = metric.lower().replace("_", " ").replace("-", " ")
    metric_words = set(metric_lower.split())

    # Exact match first (normalized)
    for col in df.columns:
        col_norm = col.lower().replace("_", " ").replace("-", " ")
        if col_norm == metric_lower:
            return col

    # Metric contained in column name
    for col in df.columns:
        col_norm = col.lower().replace("_", " ").replace("-", " ")
        if metric_lower in col_norm:
            return col

    # Column name contained in metric (reverse)
    for col in df.columns:
        if col in ("loc_id", "year"):
            continue
        col_norm = col.lower().replace("_", " ").replace("-", " ")
        if col_norm in metric_lower:
            return col

    # Word overlap - at least 2 words match
    if len(metric_words) >= 2:
        for col in df.columns:
            if col in ("loc_id", "year"):
                continue
            col_words = set(col.lower().replace("_", " ").replace("-", " ").split())
            overlap = metric_words & col_words
            if len(overlap) >= 2:
                return col

    # Single significant word match (last resort)
    significant_words = metric_words - {"of", "the", "a", "an", "for", "in", "on", "to"}
    for col in df.columns:
        if col in ("loc_id", "year"):
            continue
        col_words = set(col.lower().replace("_", " ").replace("-", " ").split())
        if significant_words & col_words:
            return col

    return None


# =============================================================================
# Derived Field Calculations
# =============================================================================

def apply_derived_fields(boxes: dict, derived_specs: list, year: int = None) -> list:
    """
    Apply derived field calculations to filled boxes.

    Args:
        boxes: Dict of loc_id -> {metric: value, ...}
        derived_specs: List of derived field specifications from postprocessor
        year: Year for context (unused, kept for API compatibility)

    Returns:
        List of warning messages for missing data
    """
    warnings = []

    for spec in derived_specs:
        numerator_name = spec.get("numerator")
        denominator_name = spec.get("denominator")
        label = spec.get("label", f"{numerator_name}/{denominator_name}")
        multiplier = spec.get("multiplier", 1)

        for loc_id, metrics in boxes.items():
            # Get numerator value
            num_val = metrics.get(numerator_name)
            if num_val is None:
                # Try with different case/formats
                for key in metrics.keys():
                    if key.lower() == numerator_name.lower():
                        num_val = metrics[key]
                        break

            if num_val is None:
                continue  # Skip silently if numerator not available

            # Get denominator value
            denom_val = metrics.get(denominator_name)
            if denom_val is None:
                # Try with different case/formats
                for key in metrics.keys():
                    if key.lower() == denominator_name.lower():
                        denom_val = metrics[key]
                        break

            # Calculate derived value
            if denom_val is None:
                warnings.append(f"{loc_id}: {denominator_name} unavailable")
                continue

            if denom_val == 0:
                warnings.append(f"{loc_id}: {denominator_name} is zero")
                continue

            result = (float(num_val) / float(denom_val)) * multiplier
            metrics[f"{label} (calculated)"] = result

    return warnings


# =============================================================================
# Event Mode Execution (for disaster/event data)
# =============================================================================

# Default event limits (can be overridden by metadata.default_limit)
DEFAULT_EVENT_LIMIT = 1000
MAX_EVENT_LIMIT = 5000


def _get_source_from_catalog(source_id: str) -> dict:
    """Get source info from catalog by source_id."""
    catalog = _load_catalog()
    if not catalog:
        return {}
    for source in catalog.get("sources", []):
        if source.get("source_id") == source_id:
            return source
    return {}


def _detect_event_type(source_id: str) -> str:
    """Detect event type from catalog metadata."""
    source = _get_source_from_catalog(source_id)
    return source.get("event_type", "unknown")


def _get_significance_column(source_id: str) -> str:
    """Get significance column from catalog metadata."""
    source = _get_source_from_catalog(source_id)
    return source.get("significance_column")


def _get_coordinate_columns(df: pd.DataFrame) -> tuple:
    """Find lat/lon column names in DataFrame."""
    lat_candidates = ["lat", "latitude", "centroid_lat"]
    lon_candidates = ["lon", "longitude", "centroid_lon"]

    lat_col = None
    lon_col = None

    for col in lat_candidates:
        if col in df.columns:
            lat_col = col
            break

    for col in lon_candidates:
        if col in df.columns:
            lon_col = col
            break

    return lat_col, lon_col


def _get_time_column(df: pd.DataFrame) -> str:
    """Find timestamp column name in DataFrame."""
    time_candidates = ["time", "timestamp", "event_date", "date", "ignition_date"]
    for col in time_candidates:
        if col in df.columns:
            return col
    return None


def _get_id_column(df: pd.DataFrame, event_type: str) -> str:
    """Find event ID column name in DataFrame."""
    id_candidates = ["event_id", f"{event_type}_id", "id", "storm_id", "fire_id"]
    for col in id_candidates:
        if col in df.columns:
            return col
    return None


def execute_event_order(order: dict) -> dict:
    """
    Execute order in event mode - returns individual events as GeoJSON points.

    Args:
        order: {items: [{source_id, mode, event_file, region, year_start, year_end, filters, limit}]}

    Returns:
        {
            type: "events",
            event_type: "earthquake",
            geojson: {type: "FeatureCollection", features: [...]},
            time_range: {min, max, granularity},
            summary: str,
            count: int,
            sources: [...]
        }
    """
    items = order.get("items", [])
    summary = order.get("summary", "")

    if not items:
        return {
            "type": "error",
            "message": "No items in order",
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0
        }

    # Event mode typically uses single source
    item = items[0]
    source_id = item.get("source_id")
    event_file_key = item.get("event_file", "events")
    region = item.get("region")
    year_start = item.get("year_start")
    year_end = item.get("year_end")
    year = item.get("year")
    filters = item.get("filters", {})
    requested_limit = item.get("limit")

    # Load event data
    try:
        df, metadata = load_event_data(source_id, event_file_key)
    except Exception as e:
        return {
            "type": "error",
            "message": f"Failed to load event data: {e}",
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0
        }

    event_type = _detect_event_type(source_id)
    print(f"Event mode: {source_id} -> {event_type}, {len(df)} raw events")

    # Find coordinate columns
    lat_col, lon_col = _get_coordinate_columns(df)
    if not lat_col or not lon_col:
        return {
            "type": "error",
            "message": f"No coordinate columns found in {source_id}",
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0
        }

    # Find time column
    time_col = _get_time_column(df)

    # Find ID column
    id_col = _get_id_column(df, event_type)

    # Apply year filter
    if year_start and year_end:
        if "year" in df.columns:
            df = df[(df["year"] >= year_start) & (df["year"] <= year_end)]
        elif time_col:
            # Extract year from timestamp
            df["_year"] = pd.to_datetime(df[time_col]).dt.year
            df = df[(df["_year"] >= year_start) & (df["_year"] <= year_end)]
    elif year:
        if "year" in df.columns:
            df = df[df["year"] == year]
        elif time_col:
            df["_year"] = pd.to_datetime(df[time_col]).dt.year
            df = df[df["_year"] == year]

    # Apply region filter
    region_codes = expand_region(region)
    if region_codes and "loc_id" in df.columns:
        # Check for US state filtering
        us_state_prefixes = [c for c in region_codes if c.startswith("USA-")]
        country_codes = [c for c in region_codes if not c.startswith("USA-")]

        if us_state_prefixes:
            mask = df["loc_id"].str.startswith(tuple(us_state_prefixes), na=False)
            df = df[mask]
        elif country_codes:
            df["_country"] = df["loc_id"].str.split("-").str[0]
            df = df[df["_country"].isin(country_codes)]

    # Apply filters (e.g., magnitude_min, category)
    for field, value in filters.items():
        if field.endswith("_min"):
            col = field[:-4]
            if col in df.columns:
                df = df[df[col] >= value]
        elif field.endswith("_max"):
            col = field[:-4]
            if col in df.columns:
                df = df[df[col] <= value]
        elif field in df.columns:
            df = df[df[field] == value]

    print(f"  After filters: {len(df)} events")

    # Apply limit (use requested limit, capped at max)
    limit = min(requested_limit or DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT)

    if len(df) > limit:
        # Sort by significance column from metadata and take top N
        sig_col = _get_significance_column(source_id)
        if sig_col and sig_col in df.columns:
            df = df.nlargest(limit, sig_col)
        else:
            df = df.head(limit)
        print(f"  Limited to {limit} events (sorted by {sig_col or 'order'})")

    # Build GeoJSON features
    features = []
    for idx, row in df.iterrows():
        lat = row.get(lat_col)
        lon = row.get(lon_col)

        if pd.isna(lat) or pd.isna(lon):
            continue

        # Build properties - include all columns except geometry
        properties = {}
        for col in df.columns:
            if col.startswith("_"):  # Skip temp columns
                continue
            val = row.get(col)
            if pd.notna(val):
                # Convert numpy types to Python types
                if hasattr(val, 'item'):
                    val = val.item()
                # Convert timestamps to ISO string
                if isinstance(val, pd.Timestamp):
                    val = val.isoformat()
                properties[col] = val

        # Ensure event_id exists
        if "event_id" not in properties and id_col:
            properties["event_id"] = properties.get(id_col, idx)
        elif "event_id" not in properties:
            properties["event_id"] = idx

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
            "properties": properties
        })

    # Calculate time range
    time_range = {"min": None, "max": None, "granularity": "daily"}
    if time_col and len(df) > 0:
        times = pd.to_datetime(df[time_col])
        time_range["min"] = int(times.min().timestamp() * 1000)
        time_range["max"] = int(times.max().timestamp() * 1000)

    # Build source info
    source_info = [{
        "id": source_id,
        "name": metadata.get("source_name", source_id),
        "url": metadata.get("source_url", "")
    }]

    return {
        "type": "events",
        "data_type": "events",
        "source_id": source_id,
        "event_type": event_type,
        "geojson": {
            "type": "FeatureCollection",
            "features": features
        },
        "time_range": time_range,
        "summary": summary or f"Showing {len(features)} {event_type} events",
        "count": len(features),
        "sources": source_info
    }


def execute_order(order: dict) -> dict:
    """
    Execute a confirmed order and return GeoJSON response.

    Implements the "Empty Box" model:
    1. Expand all regions to loc_ids
    2. Create empty boxes for each target location
    3. Process each item, fill boxes with values
    4. Join with geometry
    5. Build GeoJSON

    Supports multi-year mode when year_start/year_end provided:
    - Returns base geometry + year_data dict for efficient time slider

    Supports event mode when mode="events":
    - Returns individual events as GeoJSON points

    Args:
        order: {items: [{source_id, metric, region, year, year_start, year_end, sort, mode}, ...], summary: str}

    Returns:
        Single year: {type, geojson, summary, count, sources}
        Multi-year: {type, geojson, year_data, year_range, multi_year, summary, count, sources}
        Event mode: {type: "events", event_type, geojson, time_range, summary, count, sources}
    """
    items = order.get("items", [])
    summary = order.get("summary", "")

    if not items:
        return {
            "type": "error",
            "message": "No items in order",
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0
        }

    # Determine data_type for this order from first item's source
    # (for tagging the response so frontend knows which pipeline to use)
    primary_source_id = items[0].get("source_id") if items else None
    order_data_type = _get_source_data_type(primary_source_id) if primary_source_id else "metrics"

    # Check if this is an events order (explicit mode or data_type from catalog)
    def is_event_item(item):
        if item.get("mode") == "events":
            return True
        source_id = item.get("source_id")
        return _get_source_data_type(source_id) == "events" if source_id else False

    # If any item is events type, route entire order to event pipeline
    # (mixed orders with events+metrics should be split by chat before sending)
    if any(is_event_item(item) for item in items):
        result = execute_event_order(order)
        result["data_type"] = "events"
        result["source_id"] = primary_source_id
        return result

    # Otherwise, continue with metrics pipeline below

    # Check if any item uses year range (multi-year mode)
    multi_year_mode = any(
        item.get("year_start") and item.get("year_end")
        for item in items
    )

    # Step 1: Determine all target loc_ids and collect metadata
    target_countries = set()
    geo_levels = set()
    sources_used = {}

    for item in items:
        region = item.get("region")
        countries = expand_region(region)
        if countries:
            target_countries.update(countries)

        # Track sources
        source_id = item.get("source_id")
        if source_id and source_id not in sources_used:
            try:
                _, metadata = load_source_data(source_id)
                sources_used[source_id] = metadata
                geo_levels.add(metadata.get("geographic_level", "country"))
            except Exception:
                pass

    # For multi-year: year_data[year][loc_id] = {metric: value}
    # For single-year: boxes[loc_id] = {metric: value}
    year_data = {} if multi_year_mode else None
    boxes = {} if not multi_year_mode else None
    all_years = set()
    metric_key = None  # Track the primary metric label for frontend
    all_metrics = []  # Track ALL metric labels for multi-metric support
    metric_year_ranges = {}  # Track year range per metric for time slider adjustment
    metric_source_map = {}  # Track which metric belongs to which source
    requested_year_start = None  # Track requested range for comparison
    requested_year_end = None

    # Step 3: Process each order item
    for item in items:
        source_id = item.get("source_id")
        metric = item.get("metric")
        region = item.get("region")
        year = item.get("year")
        year_start = item.get("year_start")
        year_end = item.get("year_end")
        sort_spec = item.get("sort")

        # Track requested range for comparison with actual data
        if year_start and year_end:
            requested_year_start = year_start
            requested_year_end = year_end

        if not source_id:
            continue

        try:
            df, metadata = load_source_data(source_id)
        except Exception as e:
            print(f"Error loading {source_id}: {e}")
            continue

        # Find the metric column first (needed for smart year filtering)
        if metric:
            metric_col = find_metric_column(df, metric)
        else:
            numeric_cols = df.select_dtypes(include=['float64', 'int64', 'Float64', 'Int64']).columns
            metric_col = numeric_cols[0] if len(numeric_cols) > 0 else None

        # Store metric label for frontend
        item_label = item.get("metric_label", metric_col)
        if metric_col and item_label:
            if not metric_key:
                metric_key = item_label  # First metric is the default
            if item_label not in all_metrics:
                all_metrics.append(item_label)  # Track all metrics
            # Track year range per metric
            if year_start and year_end:
                metric_year_ranges[item_label] = {"min": year_start, "max": year_end}

        # Filter by year (different logic for single vs range)
        if year_start and year_end and "year" in df.columns:
            # Multi-year range mode
            df = df[(df["year"] >= year_start) & (df["year"] <= year_end)]
        elif year and "year" in df.columns:
            # Single year mode
            df = df[df["year"] == year]
        elif "year" in df.columns:
            # Use latest year that has data for this metric
            if metric_col and metric_col in df.columns:
                years_with_data = df[df[metric_col].notna()]["year"].unique()
                if len(years_with_data) > 0:
                    df = df[df["year"] == max(years_with_data)]
                else:
                    df = df[df["year"] == df["year"].max()]
            else:
                df = df[df["year"] == df["year"].max()]

        # Filter by region
        region_codes = expand_region(region)
        if region_codes and "loc_id" in df.columns:
            # Check for US state filtering (loc_ids starting with USA-)
            us_state_prefixes = [c for c in region_codes if c.startswith("USA-")]
            country_codes = [c for c in region_codes if not c.startswith("USA-")]

            if us_state_prefixes:
                # Filter to US locations matching state prefix
                mask = df["loc_id"].str.startswith(tuple(us_state_prefixes))
                df = df[mask]
            elif country_codes:
                # Filter to country-level or sub-national within those countries
                df["_country_code"] = df["loc_id"].str.split("-").str[0]
                df = df[df["_country_code"].isin(country_codes)]
                df = df.drop(columns=["_country_code"])

        # Apply sort/limit if specified (only for single-year mode)
        if sort_spec and not multi_year_mode:
            sort_col = sort_spec.get("by")
            if sort_col:
                matched_col = find_metric_column(df, sort_col)
                if matched_col:
                    ascending = sort_spec.get("order", "desc") == "asc"
                    df = df.sort_values(matched_col, ascending=ascending, na_position='last')
                    if sort_spec.get("limit"):
                        df = df.head(sort_spec["limit"])

        # metric_col already found above for year filtering
        if not metric_col:
            continue

        # Fill data structures
        label = item.get("metric_label", metric_col)
        if source_id and label not in metric_source_map:
            metric_source_map[label] = source_id

        for _, row in df.iterrows():
            loc_id = row.get("loc_id")
            if not loc_id:
                continue

            val = row.get(metric_col)
            if pd.notna(val):
                if hasattr(val, 'item'):
                    val = val.item()

                if multi_year_mode:
                    # Multi-year: organize by year -> loc_id
                    row_year = int(row.get("year")) if "year" in df.columns else 0
                    all_years.add(row_year)

                    if row_year not in year_data:
                        year_data[row_year] = {}
                    if loc_id not in year_data[row_year]:
                        year_data[row_year][loc_id] = {}

                    year_data[row_year][loc_id][label] = val
                else:
                    # Single year: organize by loc_id
                    if loc_id not in boxes:
                        boxes[loc_id] = {"year": row.get("year")} if "year" in df.columns else {}

                    boxes[loc_id][label] = val

    # Step 3.5: Apply derived field calculations
    derived_specs = order.get("derived_specs", [])
    if derived_specs and boxes:
        # Get year from first item or first box
        calc_year = None
        if items:
            calc_year = items[0].get("year")
        if not calc_year and boxes:
            first_box = next(iter(boxes.values()))
            calc_year = first_box.get("year")

        derivation_warnings = apply_derived_fields(boxes, derived_specs, calc_year)
        if derivation_warnings:
            print(f"Derivation warnings: {derivation_warnings[:5]}")  # Log first 5

    # Step 4: Join with geometry
    # Determine geographic level from sources
    primary_level = "country" if "country" in geo_levels else list(geo_levels)[0] if geo_levels else "country"

    if primary_level == "country":
        geometry_df = load_global_countries()
    else:
        # Load geometry for all relevant countries
        iso3_codes = set()
        loc_ids_to_check = boxes.keys() if boxes else set()
        if year_data:
            for year_locs in year_data.values():
                loc_ids_to_check = loc_ids_to_check | set(year_locs.keys())

        for loc_id in loc_ids_to_check:
            iso3 = loc_id.split("-")[0] if "-" in loc_id else loc_id
            iso3_codes.add(iso3)

        geometry_rows = []
        for iso3 in iso3_codes:
            country_geom = load_country_parquet(iso3)
            if country_geom is not None:
                geometry_rows.append(country_geom[["loc_id", "name", "geometry"]])

        geometry_df = pd.concat(geometry_rows, ignore_index=True) if geometry_rows else None

    # Step 5: Build GeoJSON features
    features = []

    if geometry_df is not None:
        geom_lookup = geometry_df.set_index("loc_id")[["name", "geometry"]].to_dict("index")

        if multi_year_mode:
            # Multi-year: build base geometry features (no year-specific data)
            # Collect all loc_ids across all years
            all_loc_ids = set()
            for year_locs in year_data.values():
                all_loc_ids.update(year_locs.keys())

            for loc_id in all_loc_ids:
                geom_data = geom_lookup.get(loc_id)
                if not geom_data:
                    continue

                geom_str = geom_data.get("geometry")
                if pd.isna(geom_str) or not geom_str:
                    continue

                try:
                    geom = json.loads(geom_str) if isinstance(geom_str, str) else geom_str
                except (json.JSONDecodeError, TypeError):
                    continue

                # Base properties (no year-specific values - those come from year_data)
                properties = {"loc_id": loc_id, "name": geom_data.get("name", loc_id)}

                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": properties
                })
        else:
            # Single year: include values in properties
            for loc_id, props in boxes.items():
                geom_data = geom_lookup.get(loc_id)
                if not geom_data:
                    continue

                geom_str = geom_data.get("geometry")
                if pd.isna(geom_str) or not geom_str:
                    continue

                try:
                    geom = json.loads(geom_str) if isinstance(geom_str, str) else geom_str
                except (json.JSONDecodeError, TypeError):
                    continue

                # Build properties
                properties = {"loc_id": loc_id, "name": geom_data.get("name", loc_id)}
                properties.update(props)

                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": properties
                })

    # Build source info for response (include URL and category)
    source_info = [
        {
            "id": sid,
            "name": meta.get("source_name", sid),
            "url": meta.get("source_url", ""),
            "category": meta.get("category", "general")
        }
        for sid, meta in sources_used.items()
    ]

    # Build response
    # Determine primary source_id for this response
    primary_source = list(sources_used.keys())[0] if sources_used else None

    response = {
        "type": "data",
        "data_type": "metrics",
        "source_id": primary_source,
        "geojson": {
            "type": "FeatureCollection",
            "features": features
        },
        "summary": summary or f"Showing {len(features)} locations",
        "count": len(features),
        "sources": source_info,
        "metric_sources": metric_source_map
    }

    # Add multi-year data if applicable
    if multi_year_mode and year_data:
        sorted_years = sorted(all_years)
        actual_min = sorted_years[0] if sorted_years else 0
        actual_max = sorted_years[-1] if sorted_years else 0

        response["multi_year"] = True
        response["year_data"] = year_data
        response["year_range"] = {
            "min": actual_min,
            "max": actual_max,
            "available_years": sorted_years
        }
        response["metric_key"] = metric_key
        response["available_metrics"] = all_metrics  # All metrics from order items
        response["metric_year_ranges"] = metric_year_ranges  # Per-metric year ranges for slider

        # Add data note if year range differs from requested
        data_notes = []
        if requested_year_start and requested_year_end:
            if actual_min != requested_year_start or actual_max != requested_year_end:
                data_notes.append(f"Note: Data available for {actual_min}-{actual_max} (requested {requested_year_start}-{requested_year_end})")
            # Check for sparse years
            expected_years = set(range(actual_min, actual_max + 1))
            missing_years = expected_years - all_years
            if missing_years:
                data_notes.append(f"Some years have no data: {sorted(missing_years)[:5]}{'...' if len(missing_years) > 5 else ''}")
        if data_notes:
            response["data_note"] = " | ".join(data_notes)

    return response
