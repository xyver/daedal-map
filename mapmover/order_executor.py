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

import logging
import pandas as pd
import json
from pathlib import Path
from typing import Optional

logger = logging.getLogger("mapmover")

from .geometry_handlers import (
    load_global_countries,
    load_country_parquet,
    df_to_geojson,
)

from .paths import DATA_ROOT, CATALOG_PATH
from .data_loading import load_source_metadata
from .aggregation_system import build_aggregation_spec, apply_temporal_aggregation
from .duckdb_helpers import (
    can_query_event_source,
    is_s3_mode,
    parquet_columns,
    path_to_uri,
    quote_ident,
    resolve_event_parquet_path,
    run_df,
    select_columns_from_parquet,
    select_event_ids_by_regions,
    select_peak_positions_by_storm_ids,
    select_rows,
)

CONVERSIONS_PATH = Path(__file__).parent / "conversions.json"
REFERENCE_DIR = Path(__file__).parent / "reference"

# Cache conversions to avoid repeated file reads
_conversions_cache = None
_iso_codes_cache = None
_usa_admin_cache = None
_catalog_cache = None


def _coerce_year(value) -> Optional[int]:
    """Best-effort year coercion for LLM-generated order fields."""
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
        # Accept ISO-ish strings like "2015-01-01" by reading first 4 digits.
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
        return None


def _normalize_year_filters(item: dict) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Normalize year/year_start/year_end in-place and return coerced values."""
    year = _coerce_year(item.get("year"))
    year_start = _coerce_year(item.get("year_start"))
    year_end = _coerce_year(item.get("year_end"))

    if year is not None:
        item["year"] = year
    if year_start is not None:
        item["year_start"] = year_start
    if year_end is not None:
        item["year_end"] = year_end

    return year, year_start, year_end


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
    Returns: 'events', 'metrics', 'gridded', 'geometry', or 'metrics' as default.
    """
    catalog = _load_catalog()
    for src in catalog.get("sources", []):
        if src.get("source_id") == source_id:
            return src.get("data_type", "metrics")
    return "metrics"  # Default to metrics if not found


def _get_source_path(source_id: str) -> Optional[str]:
    """Get the path for a source from catalog."""
    catalog = _load_catalog()
    for src in catalog.get("sources", []):
        if src.get("source_id") == source_id:
            return src.get("path")
    return None


# Special geographic levels that need geometry from dual sources (not standard admin hierarchy)
SPECIAL_GEOMETRY_LEVELS = {"zcta", "tribal"}


def _has_geometry_data_type(data_type) -> bool:
    """Check if data_type includes geometry (handles both string and array formats)."""
    if data_type is None:
        return False
    if isinstance(data_type, list):
        return "geometry" in data_type
    return data_type == "geometry"


def _find_geometry_source_for_level(geo_level: str, scope: str = None) -> Optional[dict]:
    """
    Find a catalog source that provides geometry for a special geographic level.

    For special levels like 'zcta' or 'tribal', we need to find the dual source
    that has both geometry data and matches the geographic_level.

    Args:
        geo_level: The geographic level (e.g., 'zcta', 'tribal')
        scope: Optional scope filter (e.g., 'usa')

    Returns:
        Source dict from catalog if found, None otherwise
    """
    catalog = _load_catalog()
    for src in catalog.get("sources", []):
        # Match geographic_level
        if src.get("geographic_level") != geo_level:
            continue
        # Must have geometry in data_type
        if not _has_geometry_data_type(src.get("data_type")):
            continue
        # Optional scope filter
        if scope and src.get("scope", "").lower() != scope.lower():
            continue
        return src
    return None


def _load_geometry_from_source(source_info: dict, filter_regions: set = None) -> Optional[pd.DataFrame]:
    """
    Load geometry dataframe from a catalog source, optionally filtered by region.

    Args:
        source_info: Source dict from catalog with 'path' key
        filter_regions: Optional set of parent region codes to filter by (e.g., {"USA-FL"})

    Returns:
        DataFrame with loc_id, name, geometry columns, or None
    """
    import logging
    logger = logging.getLogger(__name__)

    source_path = source_info.get("path")
    if not source_path:
        return None

    full_path = DATA_ROOT / source_path
    parquet_files = list(full_path.glob("*.parquet")) if full_path.is_dir() else []

    if not parquet_files:
        logger.warning(f"No parquet files found in {full_path}")
        return None

    # Load the parquet file
    parquet_path = parquet_files[0]
    logger.info(f"Loading geometry from dual source: {parquet_path}")

    try:
        columns = ["loc_id", "name", "geometry", "parent_id"]
        df = select_columns_from_parquet(parquet_path, columns)
        if df.empty:
            df = pd.read_parquet(parquet_path, columns=columns)

        # Filter by parent region if specified (e.g., filter_region = "USA-FL" for Florida ZIPs)
        # parent_id is at county level (USA-FL-12001), so use prefix matching
        # Use vectorized str.startswith with tuple for efficiency (same pattern as metrics pipeline)
        if filter_regions and "parent_id" in df.columns:
            # Build prefix tuple with trailing dash for hierarchy matching
            prefixes = tuple(f"{r}-" for r in filter_regions)
            # Vectorized: match prefix OR exact match
            mask = df["parent_id"].str.startswith(prefixes, na=False) | df["parent_id"].isin(filter_regions)
            df = df[mask]
            logger.info(f"Filtered to {len(df)} features matching regions: {filter_regions}")

        # Return only geometry-relevant columns
        cols = ["loc_id", "name", "geometry", "parent_id"]
        available_cols = [c for c in cols if c in df.columns]
        if "loc_id" not in available_cols or "geometry" not in available_cols:
            logger.warning(f"Missing required columns in {parquet_path}")
            return None
        return df[available_cols]
    except Exception as e:
        logger.error(f"Error loading geometry from {parquet_path}: {e}")
        return None


def execute_geometry_overlay(geometry_overlay: dict, filter_loc_ids: list = None) -> dict:
    """
    Load geometry overlay data and return as GeoJSON.

    Used for "show me ZIP codes in California" type queries.

    Args:
        geometry_overlay: {source_id, overlay_type}
        filter_loc_ids: List of loc_ids to filter by (e.g., ["USA-CA"] for California)

    Returns:
        GeoJSON FeatureCollection with geometry features
    """
    import logging
    logger = logging.getLogger(__name__)

    source_id = geometry_overlay.get("source_id")
    if not source_id:
        logger.warning("No source_id in geometry_overlay")
        return {"type": "FeatureCollection", "features": []}

    # Get source path from catalog
    source_path = _get_source_path(source_id)
    if not source_path:
        logger.warning(f"Source not found in catalog: {source_id}")
        return {"type": "FeatureCollection", "features": []}

    # Build full path to parquet file
    # Path format: countries/USA/geometry/zcta -> countries/USA/geometry/zcta/USA.parquet
    full_path = DATA_ROOT / source_path
    parquet_files = list(full_path.glob("*.parquet")) if full_path.is_dir() else []

    if not parquet_files:
        logger.warning(f"No parquet files found in {full_path}")
        return {"type": "FeatureCollection", "features": []}

    # Load the parquet file (use first one found)
    parquet_path = parquet_files[0]
    logger.info(f"Loading geometry overlay from {parquet_path}")

    try:
        columns = ["loc_id", "name", "geometry", "parent_id"]
        df = select_columns_from_parquet(parquet_path, columns)
        if df.empty:
            df = pd.read_parquet(parquet_path, columns=columns)
        logger.info(f"Loaded {len(df)} features from {parquet_path}")

        # Filter by region if specified
        # For ZCTA, parent_id contains the county loc_id (e.g., USA-CA-6037)
        # To filter by state, we check if parent_id starts with the state prefix
        if filter_loc_ids and len(filter_loc_ids) > 0 and "parent_id" in df.columns:
            filter_conditions = []
            for loc_id in filter_loc_ids:
                # Match parent_id that starts with the filter loc_id
                # e.g., filter_loc_id="USA-CA" matches parent_id="USA-CA-6037"
                filter_conditions.append(df["parent_id"].str.startswith(loc_id + "-", na=False))
                # Also match exact parent_id
                filter_conditions.append(df["parent_id"] == loc_id)

            if filter_conditions:
                combined_filter = filter_conditions[0]
                for cond in filter_conditions[1:]:
                    combined_filter = combined_filter | cond
                df = df[combined_filter]
                logger.info(f"Filtered to {len(df)} features for regions: {filter_loc_ids}")

        # Convert to GeoJSON
        geojson = df_to_geojson(df, polygon_only=True)
        logger.info(f"Returning {len(geojson.get('features', []))} geometry features")

        return geojson

    except Exception as e:
        logger.error(f"Error loading geometry overlay: {e}")
        return {"type": "FeatureCollection", "features": []}


def execute_geometry_order(order: dict) -> dict:
    """
    Execute geometry order, returning GeoJSON with all requested features.

    Routes through the order system to enable:
    - Accumulating multiple geometry requests in an order
    - Using cache system with dedup by loc_id
    - Add/remove regions incrementally

    Args:
        order: {items: [{source_id, region, overlay_type}], summary: str}

    Returns:
        {
            type: "geometry",
            data_type: "geometry",
            geojson: {type: "FeatureCollection", features: [...]},
            count: int,
            overlay_type: str,
            summary: str
        }
    """
    import logging
    logger = logging.getLogger(__name__)

    items = order.get("items", [])
    summary = order.get("summary", "")

    if not items:
        return {
            "type": "geometry",
            "data_type": "geometry",
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "message": "No items in order"
        }

    all_features = []
    overlay_type = None

    for item in items:
        source_id = item.get("source_id")
        region = item.get("region")
        item_overlay_type = item.get("overlay_type")

        if not source_id:
            continue

        # Track overlay_type for response
        if item_overlay_type and not overlay_type:
            overlay_type = item_overlay_type

        # Build filter_loc_ids from region
        # Region can be "USA-CA" for California or "USA-CA-6037" for a county
        filter_loc_ids = [region] if region else None

        logger.info(f"Executing geometry order: source={source_id}, region={region}, overlay_type={item_overlay_type}")

        # Execute geometry overlay for this item
        geojson = execute_geometry_overlay(
            {"source_id": source_id, "overlay_type": item_overlay_type},
            filter_loc_ids=filter_loc_ids
        )

        # Accumulate features
        item_features = geojson.get("features", [])
        all_features.extend(item_features)
        logger.info(f"Added {len(item_features)} features from {source_id}")

    return {
        "type": "geometry",
        "data_type": "geometry",
        "overlay_type": overlay_type or "zcta",
        "geojson": {
            "type": "FeatureCollection",
            "features": all_features
        },
        "count": len(all_features),
        "summary": summary or f"Showing {len(all_features)} geometry features"
    }


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

    # Load metadata (always available locally - small files are synced on startup)
    meta_path = source_dir / "metadata.json"
    with open(meta_path, encoding='utf-8') as f:
        metadata = json.load(f)

    if is_s3_mode():
        # In S3 mode, no local parquet files exist - pick preferred filename and let
        # select_rows() fetch from R2 via DuckDB httpfs (path_to_uri handles the s3:// conversion).
        parquet_path = None
        for name in ["all_countries.parquet", "USA.parquet"]:
            parquet_path = source_dir / name
            break  # use first preferred name; DuckDB will error if missing on R2
        if parquet_path is None:
            # Fall back to filename from metadata files section
            for _key, info in metadata.get("files", {}).items():
                fname = (info.get("name") or info.get("filename")) if isinstance(info, dict) else None
                if fname and fname.endswith(".parquet"):
                    parquet_path = source_dir / fname
                    break
        if parquet_path is None:
            raise ValueError(f"Cannot determine parquet path for {source_id} in S3 mode")
        uri = path_to_uri(parquet_path)
        logger.info(f"[S3] load_source_data({source_id}): uri={uri}")
        df = select_rows(parquet_path)
        logger.info(f"[S3] load_source_data({source_id}): rows={len(df)}")
    else:
        # Local mode: glob for parquet files on disk
        parquet_files = list(source_dir.glob("*.parquet"))
        if not parquet_files:
            raise ValueError(f"No parquet file found for {source_id} in {source_dir}")

        parquet_path = parquet_files[0]
        for name in ["all_countries.parquet", "USA.parquet"]:
            candidate = source_dir / name
            if candidate.exists():
                parquet_path = candidate
                break

        df = select_rows(parquet_path)
        if df.empty:
            df = pd.read_parquet(parquet_path)

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
        fallback_names = [
            f"{event_file_key}.parquet",
            "events.parquet",
            "fires.parquet",
            "positions.parquet",
            "storms.parquet",
        ]
        for name in fallback_names:
            candidate = source_dir / name
            if is_s3_mode() or candidate.exists():
                df = select_rows(candidate)
                if df.empty and not is_s3_mode():
                    df = pd.read_parquet(candidate)
                return df, metadata
        if not is_s3_mode():
            # Last-resort fallback: use any parquet in source dir (local mode only)
            parquet_candidates = sorted(source_dir.glob("*.parquet"))
            for candidate in parquet_candidates:
                if candidate.name in ("all_countries.parquet", "all_regions.parquet"):
                    continue
                df = select_rows(candidate)
                if df.empty:
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

    df = select_rows(parquet_path)
    if df.empty:
        df = pd.read_parquet(parquet_path)
    return df, metadata


def _resolve_event_parquet_path(source_id: str, event_file_key: str = "events") -> tuple[Path, dict]:
    """Resolve event parquet path from source metadata without loading the full dataframe."""
    source_dir = _get_source_path(source_id)
    return resolve_event_parquet_path(source_dir, event_file_key)


def _duckdb_can_query_events(source_id: str) -> bool:
    return can_query_event_source(source_id)


def _load_event_data_duckdb(source_id: str, item: dict, event_file_key: str = "events") -> tuple[pd.DataFrame, dict]:
    """
    Load and filter event data with DuckDB for first-pass migration sources.

    This keeps the response-building contract unchanged while moving the heavy
    parquet scan/filter work into DuckDB.
    """
    parquet_path, metadata = _resolve_event_parquet_path(source_id, event_file_key)

    available_cols = parquet_columns(parquet_path)

    region = item.get("region")
    year, year_start, year_end = _normalize_year_filters(item)
    filters = item.get("filters", {}) or {}
    requested_limit = item.get("limit")
    time_col = "year" if "year" in available_cols else ("timestamp" if "timestamp" in available_cols else None)
    loc_id_col = "loc_id" if "loc_id" in available_cols else None

    where_clauses = []
    params = [path_to_uri(parquet_path)]

    if year_start is not None and year_end is not None:
        if time_col == "year":
            where_clauses.append('"year" BETWEEN ? AND ?')
            params.extend([year_start, year_end])
        elif time_col:
            where_clauses.append(f"year({quote_ident(time_col)}) BETWEEN ? AND ?")
            params.extend([year_start, year_end])
    elif year is not None:
        if time_col == "year":
            where_clauses.append('"year" = ?')
            params.append(year)
        elif time_col:
            where_clauses.append(f"year({quote_ident(time_col)}) = ?")
            params.append(year)

    region_codes = expand_region(region)
    if region_codes and loc_id_col:
        us_state_prefixes = sorted(c for c in region_codes if c.startswith("USA-"))
        country_codes = sorted(c for c in region_codes if not c.startswith("USA-"))
        region_parts = []

        for prefix in us_state_prefixes:
            region_parts.append(f"{quote_ident(loc_id_col)} LIKE ?")
            params.append(f"{prefix}%")

        if country_codes:
            placeholders = ", ".join("?" for _ in country_codes)
            region_parts.append(f"split_part({quote_ident(loc_id_col)}, '-', 1) IN ({placeholders})")
            params.extend(country_codes)

        if region_parts:
            where_clauses.append("(" + " OR ".join(region_parts) + ")")

    for field, value in filters.items():
        if field.endswith("_min"):
            col = field[:-4]
            if col in available_cols:
                where_clauses.append(f"{quote_ident(col)} >= ?")
                params.append(value)
        elif field.endswith("_max"):
            col = field[:-4]
            if col in available_cols:
                where_clauses.append(f"{quote_ident(col)} <= ?")
                params.append(value)
        elif field in available_cols:
            where_clauses.append(f"{quote_ident(field)} = ?")
            params.append(value)

    sql = "SELECT * FROM read_parquet(?)"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    limit = min(requested_limit or DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT)
    sig_col = metadata.get("significance_column")
    if sig_col and sig_col in available_cols:
        sql += f" ORDER BY {quote_ident(sig_col)} DESC NULLS LAST"
    sql += " LIMIT ?"
    params.append(limit)

    df = run_df(sql, params)
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

    # If it's already a loc_id format (e.g., USA-FL, USA-CA-6037), return as-is
    if "-" in region and region.split("-")[0].isupper() and len(region.split("-")[0]) == 3:
        return {region}

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


def _extract_date_window(item: dict) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Infer date window from order item fields."""
    date_start = pd.to_datetime(item.get("date_start"), errors="coerce")
    date_end = pd.to_datetime(item.get("date_end"), errors="coerce")

    year, year_start, year_end = _normalize_year_filters(item)

    if pd.isna(date_start) and year_start:
        date_start = pd.Timestamp(year_start, 1, 1)
    if pd.isna(date_end) and year_end:
        date_end = pd.Timestamp(year_end, 12, 31)
    if pd.isna(date_start) and year:
        date_start = pd.Timestamp(year, 1, 1)
    if pd.isna(date_end) and year:
        date_end = pd.Timestamp(year, 12, 31)

    return (None if pd.isna(date_start) else date_start, None if pd.isna(date_end) else date_end)


def _load_fx_with_aggregation(source_id: str, item: dict, metadata: dict) -> tuple[Optional[pd.DataFrame], dict]:
    """
    Load FX data with temporal aggregation contract.

    Returns:
        (df_or_none, trace)
    """
    trace = {
        "source_id": source_id,
        "requested": {
            "time_granularity": item.get("time_granularity"),
            "aggregation": item.get("aggregation"),
            "date_start": item.get("date_start"),
            "date_end": item.get("date_end"),
            "year": item.get("year"),
            "year_start": item.get("year_start"),
            "year_end": item.get("year_end"),
        },
    }

    spec = build_aggregation_spec(item, metadata)
    trace["spec"] = spec.to_dict()

    # Runtime metrics contract is yearly; use default runtime parquet when no override is requested.
    has_temporal_override = bool(item.get("time_granularity") or item.get("aggregation") or item.get("date_start") or item.get("date_end"))
    if not has_temporal_override:
        trace["applied"] = {"path": "all_countries.parquet", "mode": "native_yearly"}
        return None, trace

    source_dir = _get_source_path(source_id)
    daily_path = source_dir / "fx_staging_daily.parquet"
    if not daily_path.exists():
        trace["applied"] = {"path": "all_countries.parquet", "mode": "fallback_no_daily_staging"}
        return None, trace

    try:
        fx = select_columns_from_parquet(daily_path, ["date", "loc_id", "local_per_usd"])
        if fx.empty:
            fx = pd.read_parquet(daily_path, columns=["date", "loc_id", "local_per_usd"])
    except Exception as e:
        trace["applied"] = {"path": "all_countries.parquet", "mode": "fallback_read_error", "error": str(e)}
        return None, trace

    start_ts, end_ts = _extract_date_window(item)
    if start_ts is not None:
        fx = fx[pd.to_datetime(fx["date"], errors="coerce") >= start_ts]
    if end_ts is not None:
        fx = fx[pd.to_datetime(fx["date"], errors="coerce") <= end_ts]

    if fx.empty:
        trace["applied"] = {"path": str(daily_path), "mode": "empty_after_filter"}
        return pd.DataFrame(columns=["loc_id", "year", "source", "local_per_usd"]), trace

    aggregated = apply_temporal_aggregation(
        fx,
        spec,
        date_col="date",
        value_col="local_per_usd",
        group_cols=("loc_id",),
    )

    if aggregated.empty:
        trace["applied"] = {"path": str(daily_path), "mode": "empty_after_aggregation"}
        return pd.DataFrame(columns=["loc_id", "year", "source", "local_per_usd"]), trace

    aggregated["year"] = pd.to_datetime(aggregated["date"], errors="coerce").dt.year
    aggregated = aggregated.dropna(subset=["year"])
    aggregated["year"] = aggregated["year"].astype(int)

    # Keep runtime map contract stable: loc_id + year + metric.
    yearly_method = "last" if spec.method == "last" else "mean"
    if yearly_method == "last":
        yearly = (
            aggregated.sort_values(["loc_id", "year", "date"])
            .groupby(["loc_id", "year"], as_index=False)
            .tail(1)[["loc_id", "year", "local_per_usd"]]
            .reset_index(drop=True)
        )
    else:
        yearly = (
            aggregated.groupby(["loc_id", "year"], as_index=False)
            .agg(local_per_usd=("local_per_usd", "mean"))
        )

    yearly["source"] = source_id
    trace["applied"] = {
        "path": str(daily_path),
        "mode": "daily_staging_temporal_aggregation",
        "requested_granularity": spec.time_granularity,
        "requested_method": spec.method,
        "coerced_output": "yearly_for_runtime",
        "input_rows": int(len(fx)),
        "post_agg_rows": int(len(aggregated)),
        "yearly_rows": int(len(yearly)),
    }
    return yearly[["loc_id", "year", "source", "local_per_usd"]], trace


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


def _find_source_files(source_id: str) -> list:
    """
    Find parquet files for a source_id.

    Args:
        source_id: Source ID (e.g., "geometry_zcta")

    Returns:
        List of Path objects to parquet files, or empty list if not found
    """
    source = _get_source_from_catalog(source_id)
    if not source:
        return []

    source_path = source.get("path")
    if not source_path:
        return []

    full_path = DATA_ROOT / source_path
    if full_path.is_dir():
        return list(full_path.glob("*.parquet"))
    elif full_path.with_suffix(".parquet").exists():
        return [full_path.with_suffix(".parquet")]
    return []


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
    year, year_start, year_end = _normalize_year_filters(item)
    filters = item.get("filters", {})
    requested_limit = item.get("limit")

    # Load event data
    try:
        if _duckdb_can_query_events(source_id):
            df, metadata = _load_event_data_duckdb(source_id, item, event_file_key)
        else:
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

    if (
        source_id == "hurricanes"
        and event_file_key == "storms"
        and ("latitude" not in df.columns or "longitude" not in df.columns)
    ):
        positions_path, _ = _resolve_event_parquet_path(source_id, "positions")
        peak_positions = select_peak_positions_by_storm_ids(positions_path, df.get("storm_id", []).tolist())
        if not peak_positions.empty:
            df = df.merge(
                peak_positions[["storm_id", "latitude", "longitude"]],
                on="storm_id",
                how="left",
                suffixes=("", "_pos"),
            )

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

    if not _duckdb_can_query_events(source_id):
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
    else:
        print(f"  DuckDB filtered to {len(df)} events")

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


def _execute_removal_order(order: dict, items: list, source_id: str) -> dict:
    """
    Execute a removal order - returns minimal identifiers for frontend to remove.

    Scalable for all data types:
    - Geometry: returns loc_ids (filter features by loc_id)
    - Events: returns event_ids (filter features by event_id)
    - Metrics: returns loc_ids + years + metric (delete column from year_data)

    Backend queries its cache/parquet to find matching items, returns them
    to frontend for removal. This keeps caches synchronized.

    Args:
        order: The full order dict with action="remove"
        items: Order items (each has region/criteria to remove)
        source_id: Primary source ID

    Returns:
        Geometry: {data_type, action, source_id, loc_ids, regions, summary, count}
        Events: {data_type, action, source_id, event_ids, regions, summary, count}
        Metrics: {data_type, action, source_id, loc_ids, years, metric, regions, summary, count}
    """
    logger = logging.getLogger(__name__)
    from .session_cache import session_manager

    # Determine data_type from catalog (events, geometry, or metrics)
    data_type = _get_source_data_type(source_id) if source_id else "metrics"
    source_info = _get_source_from_catalog(source_id)
    geo_level = source_info.get("geographic_level") if source_info else None

    # Override: special geometry levels are geometry type
    if geo_level in ("zcta", "tribal", "watershed", "park"):
        data_type = "geometry"

    # Collect regions from items
    regions = []
    for item in items:
        region = item.get("region")
        if region:
            expanded = expand_region(region)
            regions.extend(expanded)
    regions = list(set(regions))  # deduplicate

    # Collect metric/year info for metrics removal
    metric_to_remove = None
    years_to_remove = []
    for item in items:
        if item.get("metric"):
            metric_to_remove = item.get("metric")
        item_year = _coerce_year(item.get("year"))
        item_year_start = _coerce_year(item.get("year_start"))
        item_year_end = _coerce_year(item.get("year_end"))
        if item_year is not None:
            years_to_remove.append(item_year)
        if item_year_start is not None and item_year_end is not None:
            years_to_remove.extend(range(item_year_start, item_year_end + 1))
    years_to_remove = list(set(years_to_remove))

    # Get session cache
    session_id = order.get("session_id")
    cache = session_manager.get(session_id) if session_id else None

    # Build response based on data_type
    response = {
        "data_type": data_type,
        "action": "remove",
        "source_id": source_id,
        "regions": regions,
    }

    if data_type == "geometry":
        # Query parquet for loc_ids matching regions
        loc_ids = _get_loc_ids_by_region(source_id, regions) if regions else []
        response["loc_ids"] = loc_ids
        response["geographic_level"] = geo_level
        response["count"] = len(loc_ids)
        response["summary"] = order.get("summary", f"Removed {len(loc_ids)} areas from {', '.join(regions)}")

        # Clear from session cache
        if cache and loc_ids:
            removed = cache.remove_geometry_by_loc_ids(source_id, loc_ids)
            logger.info(f"Removed {removed} geometry items from session cache")

    elif data_type == "events":
        # Query parquet for event_ids matching regions/time
        event_ids = _get_event_ids_by_region(source_id, regions) if regions else []
        response["event_ids"] = event_ids
        response["count"] = len(event_ids)
        response["summary"] = order.get("summary", f"Removed {len(event_ids)} events from {', '.join(regions)}")

        # Clear from session cache
        if cache and event_ids:
            for eid in event_ids:
                cache._sent_all.discard(eid)
            # Also clear from source tracking
            source_set = cache._sent_by_source.get(source_id, set())
            for eid in event_ids:
                source_set.discard(eid)
            logger.info(f"Removed {len(event_ids)} event items from session cache")

    else:  # metrics
        # For metrics, we remove a "column" - all cells for given metric + optional region/year filter
        loc_ids = _get_loc_ids_by_region(source_id, regions) if regions else []
        response["loc_ids"] = loc_ids
        response["years"] = years_to_remove
        response["metric"] = metric_to_remove
        response["count"] = len(loc_ids) * max(len(years_to_remove), 1)
        response["summary"] = order.get("summary", f"Removed {metric_to_remove or 'data'} from {', '.join(regions) or 'selection'}")

        # Clear from session cache (metric-based keys)
        if cache and metric_to_remove:
            removed = cache.clear_source(metric_to_remove)
            logger.info(f"Removed {removed} metric items from session cache")

    return response


def _get_event_ids_by_region(source_id: str, regions: list) -> list:
    """
    Query parquet file to get event_ids matching regions.

    Args:
        source_id: Source ID (e.g., "earthquakes_usgs")
        regions: List of region prefixes (e.g., ["USA-CA"])

    Returns:
        List of matching event_ids
    """
    logger = logging.getLogger(__name__)
    try:
        parquet_files = _find_source_files(source_id)
        if not parquet_files:
            return []

        if _duckdb_can_query_events(source_id):
            event_ids = select_event_ids_by_regions(parquet_files[0], regions)
            logger.info(f"Found {len(event_ids)} event_ids matching regions {regions} in {source_id} via DuckDB")
            return event_ids

        columns = ["loc_id", "parent_id"]
        df = select_columns_from_parquet(parquet_files[0], columns)
        if df.empty:
            df = pd.read_parquet(parquet_files[0], columns=columns)

        if "event_id" not in df.columns:
            return []

        # Events use loc_id for region matching (where the event occurred)
        if "loc_id" in df.columns and regions:
            prefixes = tuple(f"{r}-" for r in regions)
            region_set = set(regions)
            mask = df["loc_id"].str.startswith(prefixes, na=False) | df["loc_id"].isin(region_set)
            matching = df[mask]
        else:
            matching = df

        event_ids = matching["event_id"].tolist()
        logger.info(f"Found {len(event_ids)} event_ids matching regions {regions} in {source_id}")
        return event_ids

    except Exception as e:
        logger.error(f"Error getting event_ids by region: {e}")
        return []


def _get_loc_ids_by_region(source_id: str, regions: list) -> list:
    """
    Query parquet file to get loc_ids matching regions by parent_id prefix.

    Args:
        source_id: Source ID (e.g., "geometry_zcta")
        regions: List of region prefixes (e.g., ["USA-FL"])

    Returns:
        List of matching loc_ids
    """
    logger = logging.getLogger(__name__)
    try:
        # Find parquet file for this source
        parquet_files = _find_source_files(source_id)
        if not parquet_files:
            logger.warning(f"No parquet files found for source: {source_id}")
            return []

        # Load only needed columns for region matching
        columns = ["loc_id", "parent_id"]
        df = select_columns_from_parquet(parquet_files[0], columns)
        if df.empty:
            df = pd.read_parquet(parquet_files[0], columns=columns)

        if "parent_id" not in df.columns:
            logger.warning(f"No parent_id column in {source_id}")
            return []

        # Build prefix tuple for matching
        prefixes = tuple(f"{r}-" for r in regions)
        region_set = set(regions)

        # Vectorized filter
        mask = df["parent_id"].str.startswith(prefixes, na=False) | df["parent_id"].isin(region_set)
        matching = df[mask]

        loc_ids = matching["loc_id"].tolist() if "loc_id" in matching.columns else []
        logger.info(f"Found {len(loc_ids)} loc_ids matching regions {regions} in {source_id}")
        return loc_ids

    except Exception as e:
        logger.error(f"Error getting loc_ids by region: {e}")
        return []


def _execute_mixed_order_if_needed(order: dict, items: list, source_id: str) -> dict:
    """
    Check if order has mixed add/remove items and execute accordingly.

    Checks for:
    1. Explicit item.action = "remove" on some items
    2. Session cache: regions already loaded should be removed, new regions added

    If mixed, splits into two operations and returns combined results.
    Returns None if not a mixed order (let normal flow handle it).
    """
    logger = logging.getLogger(__name__)
    from .session_cache import session_manager

    session_id = order.get("session_id")
    cache = session_manager.get(session_id) if session_id else None

    # Check for explicit item-level actions (works for all data types: geometry, metrics, events)
    add_items = []
    remove_items = []

    for item in items:
        item_action = item.get("action", "add")
        if item_action == "remove":
            remove_items.append(item)
        else:
            add_items.append(item)

    # If we have explicit removes, handle the split
    if remove_items:
        logger.info(f"Mixed order detected: {len(add_items)} adds, {len(remove_items)} removes")
        return _execute_split_order(order, add_items, remove_items, source_id)

    # No explicit removes - check cache to see if any regions already exist
    # (user says "show california" when texas is loaded = remove texas, add california)
    # This is optional behavior - for now, just return None and let normal accumulation happen
    return None


def _execute_split_order(order: dict, add_items: list, remove_items: list, source_id: str) -> dict:
    """
    Execute a split order with both adds and removes.

    Executes removals first, then adds, returns combined response.
    """
    logger = logging.getLogger(__name__)
    results = []

    # Execute removals first
    if remove_items:
        remove_order = {
            **order,
            "action": "remove",
            "items": remove_items,
            "summary": f"Removing {len(remove_items)} region(s)"
        }
        remove_result = _execute_removal_order(remove_order, remove_items, source_id)
        results.append(remove_result)
        logger.info(f"Split order: removed {remove_result.get('count', 0)} items")

    # Execute adds second
    add_result = None
    if add_items:
        add_order = {
            **order,
            "action": "add",
            "items": add_items,
        }
        # Call execute_order recursively for adds (but it won't recurse again since no removes)
        add_result = execute_order(add_order)
        results.append(add_result)
        logger.info(f"Split order: added {add_result.get('count', 0)} items")

    # Return combined response
    if len(results) == 1:
        return results[0]

    # Combine results for mixed response
    return {
        "type": "mixed_order",
        "results": results,
        "summary": order.get("summary", f"Processed {len(add_items)} adds and {len(remove_items)} removes"),
        "add_count": add_result.get("count", 0) if add_result else 0,
        "remove_count": results[0].get("count", 0) if remove_items else 0
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
    action = order.get("action", "add")  # "add" (default) or "remove"

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

    # Handle removal orders (negative orders)
    if action == "remove":
        return _execute_removal_order(order, items, primary_source_id)

    # Handle mixed orders (some items to add, some to remove based on item.action or cache state)
    # This allows "remove texas, add california" in a single order
    mixed_result = _execute_mixed_order_if_needed(order, items, primary_source_id)
    if mixed_result:
        return mixed_result

    # Check if this is an events order (explicit mode or data_type from catalog)
    def is_event_item(item):
        if item.get("mode") == "events":
            return True
        source_id = item.get("source_id")
        return _get_source_data_type(source_id) == "events" if source_id else False

    # If any item is events type, route to event pipeline.
    # For mixed event+metric orders, execute only the event subset here
    # to avoid trying to load metric sources as event files.
    event_items = [it for it in items if is_event_item(it)]
    if event_items:
        event_order = {**order, "items": event_items}
        result = execute_event_order(event_order)
        result["data_type"] = "events"
        result["source_id"] = event_items[0].get("source_id")
        return result

    # Note: Geometry orders (dual sources like ZCTA) go through metrics pipeline
    # They get special handling in Step 4 based on geographic_level

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
    aggregation_trace = []  # Track applied aggregation contract per item
    requested_year_start = None  # Track requested range for comparison
    requested_year_end = None
    all_region_codes = set()  # Track all requested region codes for GeoJSON

    # Step 3: Process each order item
    for item in items:
        source_id = item.get("source_id")
        metric = item.get("metric")
        region = item.get("region")
        year, year_start, year_end = _normalize_year_filters(item)
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
            logger.error(f"Error loading {source_id}: {e}", exc_info=True)
            continue

        # Apply shared aggregation contract for FX temporal requests.
        if source_id == "fx_usd_historical":
            fx_df, trace = _load_fx_with_aggregation(source_id, item, metadata)
            aggregation_trace.append(trace)
            if fx_df is not None:
                df = fx_df

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
        if region_codes:
            all_region_codes.update(region_codes)  # Track for GeoJSON building
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

    geometry_df = None

    if primary_level in SPECIAL_GEOMETRY_LEVELS:
        # Special levels (zcta, tribal) - find geometry from dual source with matching geographic_level
        # The source has data_type: ["geometry", "metrics"] and geographic_level matching primary_level
        geometry_source = _find_geometry_source_for_level(primary_level)
        if geometry_source:
            # Filter by requested regions (e.g., USA-FL for Florida ZIPs)
            geometry_df = _load_geometry_from_source(geometry_source, filter_regions=all_region_codes if all_region_codes else None)
            print(f"Loaded {len(geometry_df) if geometry_df is not None else 0} geometries from dual source: {geometry_source.get('source_id')} (filtered to {len(all_region_codes) if all_region_codes else 'all'} regions)")
        else:
            print(f"Warning: No geometry source found for special level: {primary_level}")

    elif primary_level == "country":
        geometry_df = load_global_countries()
        logger.info(f"[DEBUG] load_global_countries returned: {len(geometry_df) if geometry_df is not None else None} rows")
        logger.info(f"[DEBUG] all_region_codes sample: {list(all_region_codes)[:5]}, year_data years: {list(year_data.keys())[:3] if year_data else []}")
        # Filter to requested region if specified (so all region countries appear, with or without data)
        if all_region_codes and geometry_df is not None and "loc_id" in geometry_df.columns:
            geometry_df = geometry_df[geometry_df["loc_id"].isin(all_region_codes)]
            logger.info(f"[DEBUG] After region filter: {len(geometry_df)} rows")
    else:
        # Standard admin levels (admin_1, admin_2) - load from country parquet files
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
    # Include ALL locations in geometry (region), with or without data
    features = []

    if geometry_df is not None:
        geom_lookup = geometry_df.set_index("loc_id")[["name", "geometry"]].to_dict("index")

        if multi_year_mode:
            # Multi-year: build base geometry features (no year-specific data)
            # Include ALL geometry rows, not just those with data
            for loc_id in geom_lookup.keys():
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
            # Single year: include ALL geometry rows, with data where available
            for loc_id in geom_lookup.keys():
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

                # Build properties - get data from boxes if available
                properties = {"loc_id": loc_id, "name": geom_data.get("name", loc_id)}
                if boxes and loc_id in boxes:
                    properties.update(boxes[loc_id])

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

    # Determine response data_type - use "geometry" for special levels, "metrics" otherwise
    # This tells frontend whether to render as geometry overlay or choropleth
    response_data_type = "geometry" if primary_level in SPECIAL_GEOMETRY_LEVELS else "metrics"

    response = {
        "type": "data",
        "data_type": response_data_type,
        "geographic_level": primary_level,
        "source_id": primary_source,
        "geojson": {
            "type": "FeatureCollection",
            "features": features
        },
        "summary": summary or f"Showing {len(features)} locations",
        "count": len(features),
        "sources": source_info,
        "metric_sources": metric_source_map,
        "aggregation_trace": aggregation_trace,
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
