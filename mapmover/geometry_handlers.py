"""
Geometry endpoint handlers.
Handles loading geometry files and country hierarchy for drill-down navigation.

Data source (resolved via paths.py DATA_ROOT):
  geometry/global.csv      - All countries (admin_0)
  geometry/{ISO3}.parquet  - All admin levels per country

Schema (13 columns):
  loc_id, parent_id, admin_level, name, name_local, code, iso_3166_2,
  centroid_lon, centroid_lat, has_polygon, geometry, timezone, iso_a3
"""

import json
import logging
import pandas as pd
from pathlib import Path

# Try orjson for faster JSON parsing (3-10x faster than stdlib json)
try:
    import orjson
    def fast_json_loads(s):
        return orjson.loads(s)
except ImportError:
    def fast_json_loads(s):
        return json.loads(s)

from .paths import GEOMETRY_DIR, DATA_ROOT, COUNTRIES_DIR
from .duckdb_helpers import select_rows, is_s3_mode, parquet_columns

logger = logging.getLogger("mapmover")

# Cache for country parquet data - keyed by (iso3, admin_level) or just iso3 for full
_country_parquet_cache = {}

# Cache for global countries data
_global_countries_cache = None

# Cache for country bounding boxes (for viewport filtering)
_country_bounds_cache = None

# Cache for admin level names from reference/admin_levels.json
_admin_levels_cache = None


def _parquet_accessible(path: Path) -> bool:
    """Returns True if a parquet file exists locally or is accessible via S3/DuckDB."""
    if not is_s3_mode():
        return path.exists()
    try:
        cols = parquet_columns(path)
        return bool(cols)
    except Exception:
        return False


def get_geometry_path():
    """Get the geometry folder path using centralized path resolution."""
    if GEOMETRY_DIR.exists():
        return GEOMETRY_DIR
    return None


def load_global_countries():
    """
    Load global.csv (all countries, admin_0 only) from backup path.
    Returns DataFrame or None if file doesn't exist.
    """
    global _global_countries_cache
    if _global_countries_cache is not None:
        return _global_countries_cache

    geom_path = get_geometry_path()
    if not geom_path:
        logger.warning("No backup path configured")
        return None

    global_file = geom_path / "global.csv"
    if not global_file.exists():
        logger.warning(f"global.csv not found at {global_file}")
        return None

    try:
        _global_countries_cache = pd.read_csv(global_file)
        logger.info(f"Loaded {len(_global_countries_cache)} countries from global.csv")
        return _global_countries_cache
    except Exception as e:
        logger.error(f"Error loading global.csv: {e}")
        return None


def load_country_parquet(iso3: str, admin_level: int = None):
    """
    Load country geometry parquet file into cache.
    Returns DataFrame or None if file doesn't exist.

    Priority order (3-tier fallback):
    1. countries/{ISO3}/geometry.parquet - Country-specific geometry (NUTS, ABS LGA, etc.)
    2. countries/{ISO3}/crosswalk.json + geometry/{ISO3}.parquet - Crosswalk translation to geoBoundaries
    3. geometry/{ISO3}.parquet - Global geoBoundaries geometry (fallback)

    If admin_level is specified, uses predicate pushdown for efficiency.
    """
    # Check cache - if admin_level specified, cache by (iso3, level)
    cache_key = (iso3, admin_level) if admin_level is not None else iso3
    if cache_key in _country_parquet_cache:
        return _country_parquet_cache[cache_key]

    # If we have the full dataframe cached, filter from it
    if admin_level is not None and iso3 in _country_parquet_cache:
        full_df = _country_parquet_cache[iso3]
        filtered = full_df[full_df['admin_level'] == admin_level]
        _country_parquet_cache[cache_key] = filtered
        return filtered

    # Priority 1: Country-specific geometry (matches data loc_ids like NUTS)
    country_geom_file = DATA_ROOT / "countries" / iso3 / "geometry.parquet"
    # Priority 2: Crosswalk file (for translating loc_ids)
    crosswalk_file = DATA_ROOT / "countries" / iso3 / "crosswalk.json"
    # Priority 3: Global geometry folder (GADM fallback)
    global_geom_file = GEOMETRY_DIR / f"{iso3}.parquet"

    # Try country-specific first
    parquet_file = None
    crosswalk_data = None

    if _parquet_accessible(country_geom_file):
        parquet_file = country_geom_file
        logger.debug(f"Using country-specific geometry: {country_geom_file}")
    elif crosswalk_file.exists() and _parquet_accessible(global_geom_file):
        # Load crosswalk for later translation (crosswalk.json is synced locally in S3 mode)
        try:
            with open(crosswalk_file, 'r') as f:
                crosswalk_data = json.load(f)
            parquet_file = global_geom_file
            logger.debug(f"Using crosswalk + geoBoundaries geometry: {crosswalk_file}")
        except Exception as e:
            logger.warning(f"Error loading crosswalk {crosswalk_file}: {e}")
            parquet_file = global_geom_file
    elif _parquet_accessible(global_geom_file):
        parquet_file = global_geom_file
        logger.debug(f"Using global geometry fallback: {global_geom_file}")
    else:
        logger.debug(f"No geometry file found for {iso3}")
        return None

    try:
        # Use predicate pushdown if admin_level specified
        if admin_level is not None:
            df = select_rows(
                parquet_file,
                exact_filters={"admin_level": admin_level},
            )
            if df.empty and not is_s3_mode():
                df = pd.read_parquet(
                    parquet_file,
                    filters=[('admin_level', '==', admin_level)]
                )
        else:
            if is_s3_mode():
                df = select_rows(parquet_file)
            else:
                df = pd.read_parquet(parquet_file)

        # If crosswalk exists, add reverse mapping for lookup
        # This allows data with local loc_ids to find GADM geometry
        if crosswalk_data:
            mappings = crosswalk_data.get('mappings', {})
            # Create reverse lookup: GADM loc_id -> local loc_id
            reverse_map = {v: k for k, v in mappings.items()}
            # Add local_loc_id column for joining
            df['local_loc_id'] = df['loc_id'].map(reverse_map)
            logger.debug(f"Applied crosswalk: {len(reverse_map)} mappings")

        _country_parquet_cache[cache_key] = df
        logger.debug(f"Loaded {len(df)} features for {iso3} (level={admin_level}) from {parquet_file.name}")
        return df
    except Exception as e:
        logger.error(f"Error loading geometry for {iso3}: {e}")
        return None


def load_country_bounds():
    """
    Load country bounding boxes from global.csv for fast viewport filtering.
    Returns dict of iso3 -> (min_lon, min_lat, max_lon, max_lat).
    """
    global _country_bounds_cache
    if _country_bounds_cache is not None:
        return _country_bounds_cache

    _country_bounds_cache = {}

    df = load_global_countries()
    if df is None:
        return _country_bounds_cache

    try:
        from shapely.geometry import shape

        for _, row in df.iterrows():
            loc_id = row.get('loc_id')
            geom_str = row.get('geometry')

            if not loc_id or pd.isna(geom_str) or not geom_str:
                continue

            try:
                geom_data = json.loads(geom_str) if isinstance(geom_str, str) else geom_str
                geom = shape(geom_data)
                bounds = geom.bounds  # (minx, miny, maxx, maxy)
                _country_bounds_cache[loc_id] = bounds
            except Exception:
                continue

        logger.info(f"Loaded bounds for {len(_country_bounds_cache)} countries")
    except ImportError:
        logger.warning("shapely not available for country bounds computation")

    return _country_bounds_cache


def get_countries_in_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    """
    Return ISO3 codes whose bounds intersect the query bbox.
    """
    bounds = load_country_bounds()
    result = []

    for iso3, (c_min_lon, c_min_lat, c_max_lon, c_max_lat) in bounds.items():
        # Check bbox intersection
        if (c_max_lon >= min_lon and c_min_lon <= max_lon and
            c_max_lat >= min_lat and c_min_lat <= max_lat):
            result.append(iso3)

    return result


def calculate_coverage_from_parquet(iso3: str, from_level: int = 1):
    """
    Calculate coverage stats on-the-fly from actual parquet data.

    Args:
        iso3: Country ISO3 code
        from_level: Start counting from this level (default 1, excludes country level)

    Returns:
        dict with level_counts, geometry_counts, coverage, actual_depth, drillable_depth
    """
    # Load full parquet for the country
    df = load_country_parquet(iso3)
    if df is None or len(df) == 0:
        return {
            "level_counts": {},
            "geometry_counts": {},
            "coverage": 0,
            "actual_depth": 0,
            "drillable_depth": 0
        }

    # Calculate stats from actual data
    level_counts = {}
    geometry_counts = {}

    for level in df['admin_level'].unique():
        level = int(level)
        if level < from_level:
            continue
        level_df = df[df['admin_level'] == level]
        level_counts[str(level)] = len(level_df)
        # Count rows with actual geometry (not null/empty)
        has_geom = level_df['geometry'].notna() & (level_df['geometry'] != '')
        geometry_counts[str(level)] = int(has_geom.sum())

    # Calculate coverage
    total = sum(level_counts.values())
    with_geom = sum(geometry_counts.values())
    coverage = with_geom / total if total > 0 else 0

    # Calculate depth
    if level_counts:
        max_level = max(int(k) for k in level_counts.keys())
        min_level = min(int(k) for k in level_counts.keys())
        actual_depth = max_level - min_level + 1
        # Drillable depth = deepest level with geometry
        levels_with_geom = [int(k) for k, v in geometry_counts.items() if v > 0]
        drillable_depth = max(levels_with_geom) if levels_with_geom else min_level
    else:
        actual_depth = 0
        drillable_depth = 0

    return {
        "level_counts": level_counts,
        "geometry_counts": geometry_counts,
        "coverage": coverage,
        "actual_depth": actual_depth,
        "drillable_depth": drillable_depth
    }


def df_to_geojson(df, polygon_only=False):
    """
    Convert a DataFrame with geometry column to GeoJSON FeatureCollection.

    Args:
        df: DataFrame with geometry column (GeoJSON string)
        polygon_only: If True, skip Point geometries

    Performance notes:
        - Uses to_dict('records') instead of iterrows() (10-100x faster)
        - Uses orjson for JSON parsing when available (3-10x faster)
        - Pre-computes column list to avoid repeated lookups
    """
    if df is None or len(df) == 0:
        return {"type": "FeatureCollection", "features": []}

    # Get property columns once (all except geometry)
    prop_cols = [c for c in df.columns if c != 'geometry']

    # Convert to list of dicts - MUCH faster than iterrows()
    records = df.to_dict('records')

    features = []
    for row in records:
        geom_str = row.get('geometry')
        if not geom_str or (isinstance(geom_str, float) and pd.isna(geom_str)):
            continue

        try:
            geometry = fast_json_loads(geom_str) if isinstance(geom_str, str) else geom_str
        except (ValueError, TypeError):
            continue

        # Skip Point geometries if polygon_only
        if polygon_only and geometry.get('type') == 'Point':
            continue

        # Build properties - only include non-null values
        properties = {col: row[col] for col in prop_cols
                      if row.get(col) is not None and not (isinstance(row[col], float) and pd.isna(row[col]))}

        features.append({
            "type": "Feature",
            "properties": properties,
            "geometry": geometry
        })

    return {"type": "FeatureCollection", "features": features}


def get_countries_geometry(debug: bool = False):
    """
    Get all country geometries for initial map display.
    Returns a GeoJSON FeatureCollection with polygon countries only.

    If debug=True, calculates coverage info on-the-fly from parquet files.
    """
    df = load_global_countries()

    if df is None:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "country",
            "debug": debug,
            "error": "No geometry data available. Configure backup path in settings."
        }

    # Convert to GeoJSON (polygons only)
    geojson = df_to_geojson(df, polygon_only=True)

    # If debug mode, calculate coverage info on-the-fly from parquet
    if debug:
        for feature in geojson.get("features", []):
            loc_id = feature.get("properties", {}).get("loc_id")
            if loc_id:
                # Calculate from actual parquet data, starting from level 1
                cov_info = calculate_coverage_from_parquet(loc_id, from_level=1)
                feature["properties"]["actual_depth"] = cov_info.get("actual_depth", 0)
                feature["properties"]["expected_depth"] = cov_info.get("actual_depth", 0)
                feature["properties"]["coverage"] = cov_info.get("coverage", 0)
                feature["properties"]["level_counts"] = cov_info.get("level_counts", {})
                feature["properties"]["geometry_counts"] = cov_info.get("geometry_counts", {})
                feature["properties"]["drillable_depth"] = cov_info.get("drillable_depth", 0)
            else:
                feature["properties"]["actual_depth"] = 0
                feature["properties"]["expected_depth"] = 0
                feature["properties"]["coverage"] = 0
                feature["properties"]["level_counts"] = {}
                feature["properties"]["geometry_counts"] = {}
                feature["properties"]["drillable_depth"] = 0

    return {
        "geojson": geojson,
        "count": len(geojson.get("features", [])),
        "level": "country",
        "debug": debug
    }


def get_location_children(loc_id: str):
    """
    Get child geometries for a location (drill-down).
    Uses parquet files with parent_id filtering.

    If direct children have no geometry (hierarchy-only levels),
    recursively finds the first descendant level with geometry.

    Examples:
    - loc_id="USA" -> Returns US counties (skips admin_1 which has no geometry)
    - loc_id="USA-CA" -> Returns California counties (admin_2)
    - loc_id="FRA" -> Returns French communes (skips intermediate levels)
    """
    # Extract country code from loc_id
    parts = loc_id.split("-")
    if not parts:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "none",
            "parent_loc_id": loc_id,
            "error": "Invalid loc_id format"
        }

    iso3 = parts[0]

    # Load country parquet
    df = load_country_parquet(iso3)
    if df is None:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "none",
            "parent_loc_id": loc_id,
            "error": f"No geometry data for {iso3}. Download GADM data first."
        }

    # Find children with geometry, drilling through hierarchy-only levels
    current_parents = {loc_id}
    children = pd.DataFrame()
    max_depth = 10  # Safety limit

    for _ in range(max_depth):
        # Get all direct children of current parent set
        children = df[df["parent_id"].isin(current_parents)]

        if len(children) == 0:
            return {
                "geojson": {"type": "FeatureCollection", "features": []},
                "count": 0,
                "level": "none",
                "parent_loc_id": loc_id,
                "message": f"No child locations for {loc_id}"
            }

        # Check if these children have geometry
        children_with_geom = children[children["geometry"].notna()]

        if len(children_with_geom) > 0:
            # Found children with geometry
            children = children_with_geom
            break

        # No geometry at this level - drill down further
        # Use these children as the new parent set
        current_parents = set(children["loc_id"].tolist())
        logger.debug(f"Level has no geometry, drilling to {len(current_parents)} children")

    # Determine child level name
    child_level = int(children["admin_level"].iloc[0])
    level_names = {0: "country", 1: "state", 2: "county", 3: "place", 4: "locality", 5: "neighborhood"}
    level_name = level_names.get(child_level, f"admin_{child_level}")

    # Convert to GeoJSON
    geojson = df_to_geojson(children)

    return {
        "geojson": geojson,
        "count": len(geojson.get("features", [])),
        "level": level_name,
        "admin_level": child_level,
        "parent_loc_id": loc_id
    }


def get_location_places(loc_id: str):
    """
    Get places (cities/towns) for a location as a separate overlay layer.
    Used to display city markers on top of county boundaries.

    Returns the deepest admin level available for this location.
    """
    parts = loc_id.split("-")
    if not parts:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "none",
            "parent_loc_id": loc_id
        }

    iso3 = parts[0]

    # Load country parquet
    df = load_country_parquet(iso3)
    if df is None:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "none",
            "parent_loc_id": loc_id,
            "error": f"No geometry data for {iso3}"
        }

    # Find the deepest admin level that has this loc_id as ancestor
    # Get all features where parent_id starts with loc_id
    if len(parts) == 1:
        # Country level - find all places in country
        descendants = df[df["iso_a3"] == iso3]
    else:
        # Sub-national - find descendants
        # Match either exact parent_id or parent_id starting with loc_id-
        mask = (df["parent_id"] == loc_id) | (df["parent_id"].str.startswith(loc_id + "-", na=False))
        descendants = df[mask]

    if len(descendants) == 0:
        return {
            "geojson": {"type": "FeatureCollection", "features": []},
            "count": 0,
            "level": "none",
            "parent_loc_id": loc_id
        }

    # Get the deepest level available
    max_level = descendants["admin_level"].max()
    places = descendants[descendants["admin_level"] == max_level]

    # Convert to GeoJSON
    geojson = df_to_geojson(places)

    level_names = {0: "country", 1: "state", 2: "county", 3: "place", 4: "locality", 5: "neighborhood"}
    level_name = level_names.get(int(max_level), f"admin_{max_level}")

    return {
        "geojson": geojson,
        "count": len(geojson.get("features", [])),
        "level": level_name,
        "admin_level": int(max_level),
        "parent_loc_id": loc_id
    }


def get_location_info(loc_id: str):
    """
    Get detailed information about a specific location for popup display.

    Returns:
        - Basic info: loc_id, name, admin_level, parent_id
        - Children counts: children_count, children_by_level, descendants_count, descendants_by_level
        - Memberships: regional groupings (G20, BRICS, etc.) for countries
        - Dataset count: number of datasets available for this location

    Uses pre-computed children counts from parquet when available.
    """
    parts = loc_id.split("-")
    if not parts:
        return {"error": "Invalid loc_id"}

    iso3 = parts[0]
    result = {
        "loc_id": loc_id,
        "admin_level": len(parts) - 1,  # USA=0, USA-CA=1, USA-CA-6037=2
        "memberships": [],
        "dataset_count": 0
    }

    # For country level, check global.csv first
    if len(parts) == 1:
        df = load_global_countries()
        if df is not None:
            location = df[df["loc_id"] == loc_id]
            if len(location) > 0:
                row = location.iloc[0]
                result["name"] = row.get("name")
                result["admin_level"] = 0

                # Get children info from country parquet
                country_df = load_country_parquet(iso3)
                if country_df is not None and len(country_df) > 0:
                    country_row = country_df[country_df["loc_id"] == loc_id]
                    if len(country_row) > 0:
                        cr = country_row.iloc[0]
                        result["children_count"] = int(cr.get("children_count", 0)) if pd.notna(cr.get("children_count")) else 0
                        result["children_by_level"] = cr.get("children_by_level", "{}")
                        result["descendants_count"] = int(cr.get("descendants_count", 0)) if pd.notna(cr.get("descendants_count")) else 0
                        result["descendants_by_level"] = cr.get("descendants_by_level", "{}")
                    else:
                        # Calculate from parquet if not in parquet (country-only entry)
                        children = country_df[country_df["parent_id"] == loc_id]
                        result["children_count"] = len(children)
                        result["descendants_count"] = len(country_df) - 1  # Exclude country itself
                    result["max_depth"] = int(country_df['admin_level'].max())
                    result["has_children"] = result.get("children_count", 0) > 0 or result["max_depth"] > 0
                else:
                    result["children_count"] = 0
                    result["descendants_count"] = 0
                    result["max_depth"] = 0
                    result["has_children"] = False

                # Get memberships from conversions.json
                result["memberships"] = _get_country_memberships(iso3)

                # Get dataset counts by level
                result["dataset_counts"] = _get_dataset_counts_by_level(loc_id)
                result["dataset_count"] = sum(result["dataset_counts"].values())

                # Get country-specific level names
                result["level_names"] = _get_level_names(iso3)

                return result

    # For sub-national, check country parquet
    df = load_country_parquet(iso3)
    if df is None:
        return {"error": f"No data for {iso3}"}

    location = df[df["loc_id"] == loc_id]
    if len(location) == 0:
        return {"error": f"Location not found: {loc_id}"}

    row = location.iloc[0]
    result["name"] = row.get("name")
    result["admin_level"] = int(row.get("admin_level", 0))
    result["parent_id"] = row.get("parent_id")

    # Get parent name for "Part of" display
    parent_id = row.get("parent_id")
    if parent_id:
        parent_names = _get_parent_hierarchy(df, parent_id, iso3)
        result["memberships"] = [f"Part of: {', '.join(parent_names)}"] if parent_names else []
    else:
        result["memberships"] = []

    # Use pre-computed children counts if available
    result["children_count"] = int(row.get("children_count", 0)) if pd.notna(row.get("children_count")) else 0
    result["children_by_level"] = row.get("children_by_level", "{}")
    result["descendants_count"] = int(row.get("descendants_count", 0)) if pd.notna(row.get("descendants_count")) else 0
    result["descendants_by_level"] = row.get("descendants_by_level", "{}")
    result["has_children"] = result["children_count"] > 0

    # Get dataset counts by level
    result["dataset_counts"] = _get_dataset_counts_by_level(loc_id)
    result["dataset_count"] = sum(result["dataset_counts"].values())

    # Get country-specific level names
    result["level_names"] = _get_level_names(iso3)

    return result


def _get_country_memberships(iso3: str) -> list:
    """
    Get regional grouping memberships for a country from conversions.json.
    Returns list of group names this country belongs to.

    Regional groupings are stored as {group_name: {code, countries}} in conversions.json.
    """
    try:
        from .geography import get_conversions_data
        conversions = get_conversions_data()
        if not conversions:
            return []

        regional_groupings = conversions.get("regional_groupings", {})
        memberships = []

        # Priority groups to show first (most recognizable)
        priority_groups = ['G7', 'G20', 'European_Union', 'NATO', 'BRICS', 'OECD', 'OPEC', 'ASEAN']

        for group_name, group_data in regional_groupings.items():
            # Handle both dict format {code, countries} and list format
            if isinstance(group_data, dict):
                countries = group_data.get("countries", [])
            else:
                countries = group_data if isinstance(group_data, list) else []

            if iso3 in countries:
                # Use code if available, otherwise format group_name
                if isinstance(group_data, dict) and group_data.get("code"):
                    display_name = group_data["code"]
                else:
                    display_name = group_name.replace("_", " ")
                memberships.append((group_name, display_name))

        # Sort: priority groups first, then alphabetically
        def sort_key(item):
            group_name, display_name = item
            if group_name in priority_groups:
                return (0, priority_groups.index(group_name))
            return (1, display_name)

        memberships.sort(key=sort_key)
        return [display_name for _, display_name in memberships]
    except Exception:
        return []


def _get_dataset_counts_by_level(loc_id: str) -> dict:
    """
    Count datasets by geographic level for this location.
    Returns dict like {"country": 20, "state": 0, "county": 3}
    """
    try:
        from .data_loading import get_data_catalog
        catalog = get_data_catalog()
        if not catalog:
            return {}

        # Extract country code from loc_id
        iso3 = loc_id.split("-")[0] if "-" in loc_id else loc_id

        counts = {}
        for source in catalog.get("sources", []):
            geo_coverage = source.get("geographic_coverage", {})
            country_codes = geo_coverage.get("country_codes_all", [])
            geo_level = source.get("geographic_level", "country")

            if iso3 in country_codes:
                counts[geo_level] = counts.get(geo_level, 0) + 1

        return counts
    except Exception:
        return {}


def _get_dataset_count(loc_id: str) -> int:
    """
    Count how many datasets in the catalog cover this location.
    Uses geographic_coverage.country_codes_all field from catalog.json.
    """
    counts = _get_dataset_counts_by_level(loc_id)
    return sum(counts.values())


# Default level names (fallback if conversions.json unavailable)
DEFAULT_LEVEL_NAMES = {
    1: "first-level divisions",
    2: "second-level divisions",
    3: "third-level divisions",
    4: "localities",
    5: "neighborhoods",
    6: "blocks"
}

# Cache for sub-county geometry files (ZCTAs, tracts, block groups, blocks)
_subcounty_geometry_cache = {}


def load_subcounty_geometry(iso3: str, admin_level: int, state_abbrev: str = None):
    """
    Load sub-county geometry for deep admin levels (3+).

    Supports tiered geometry files stored in:
    - geometry_{type}.parquet (national files, e.g., geometry_zcta.parquet)
    - geometry_{type}/{ISO3}-{region}.parquet (regional files)

    For USA, the structure is:
    - Level 3 (postal/ZCTA): geometry_zcta.parquet (national)
    - Level 4 (tract): geometry_tract/USA-{state}.parquet
    - Level 5 (block group): geometry_blockgroup/USA-{state}.parquet
    - Level 6 (block): geometry_block/USA-{state}.parquet

    Other countries can use similar patterns:
    - geometry_postal.parquet for postal codes
    - geometry_district/{ISO3}-{region}.parquet for sub-county divisions

    Args:
        iso3: Country code
        admin_level: Admin level (3+)
        state_abbrev: Region/state code (required for state-partitioned levels)

    Returns:
        DataFrame or None
    """
    countries_dir = DATA_ROOT / "countries" / iso3

    # Country-specific level mappings for sub-county geometry
    #
    # Navigation layers (contiguous, smooth zoom):
    #   0: Countries (global.csv)
    #   1: States (geometry/{ISO3}.parquet admin_level=1)
    #   2: Counties (geometry/{ISO3}.parquet admin_level=2)
    #   3+: Sub-county from countries/{ISO3}/geometry/ files OR GADM admin_level=3+
    #
    # GADM level 3+ works smoothly for MOST countries (European communes, Asian
    # hierarchies, etc.) - they have contiguous administrative coverage at all levels.
    #
    # FRAGMENTED EXCEPTIONS (GADM level 3 doesn't cover all land):
    #   - USA: L3 = "incorporated places" (cities/towns) - much land is unincorporated
    #          Solution: Use Census tracts/blockgroups/blocks instead
    #   - CAN: L3 = Census subdivisions - varies by province, northern areas sparse
    #          Solution: May need StatsCan geometry (TODO)
    #   - RUS: L3 has only 127 regions vs 2,409 at L2 - selective coverage
    #          Solution: Stay at L2 or find better source (TODO)
    #
    # Overlay geometry (non-contiguous, chat-accessible via data_type="geometry"):
    #   - Cities/places (GADM level 3 for USA)
    #   - Tribal lands, ZCTAs, watersheds, parks, etc.
    #
    level_file_mapping = {
        "USA": {
            # USA is fragmented at GADM L3 - use Census geometry instead
            # Levels 0-2: geometry/USA.parquet (GADM states + counties)
            # Levels 3-5: countries/USA/geometry/{tract,blockgroup,block}/
            3: {"type": "tract", "partitioned": True},
            4: {"type": "blockgroup", "partitioned": True},
            5: {"type": "block", "partitioned": True}
        }
        # CAN: TODO - needs StatsCan dissemination areas for smooth L3+
        # RUS: TODO - needs better source or stay at L2
    }

    # Get file mapping for this country/level
    country_mapping = level_file_mapping.get(iso3, {})
    level_config = country_mapping.get(admin_level)

    if not level_config:
        # No sub-county geometry defined for this country/level
        # Fall back to checking the main geometry.parquet
        return None

    geom_type = level_config["type"]
    is_partitioned = level_config["partitioned"]

    if not is_partitioned:
        # National file
        cache_key = f"{iso3}_{geom_type}"
        if cache_key in _subcounty_geometry_cache:
            return _subcounty_geometry_cache[cache_key]

        file_path = countries_dir / "geometry" / f"{geom_type}.parquet"
        if not file_path.exists():
            logger.debug(f"Sub-county geometry not found: {file_path}")
            return None

        try:
            df = select_rows(file_path)
            if df.empty:
                df = pd.read_parquet(file_path)
            _subcounty_geometry_cache[cache_key] = df
            logger.debug(f"Loaded {len(df)} features from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading sub-county geometry: {e}")
            return None

    else:
        # Partitioned by region/state
        if not state_abbrev:
            logger.warning(f"Region/state code required for {iso3} admin level {admin_level}")
            return None

        subdir = geom_type  # e.g., "tract", "blockgroup", "block"
        cache_key = f"{iso3}_geometry_{subdir}_{state_abbrev}"

        if cache_key in _subcounty_geometry_cache:
            return _subcounty_geometry_cache[cache_key]

        file_path = countries_dir / "geometry" / subdir / f"{iso3}-{state_abbrev}.parquet"
        if not file_path.exists():
            logger.debug(f"Sub-county geometry not found: {file_path}")
            return None

        try:
            df = select_rows(file_path)
            if df.empty:
                df = pd.read_parquet(file_path)
            _subcounty_geometry_cache[cache_key] = df
            logger.debug(f"Loaded {len(df)} features for {state_abbrev} level {admin_level}")
            return df
        except Exception as e:
            logger.error(f"Error loading sub-county geometry: {e}")
            return None


def get_states_in_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    """
    Return state abbreviations whose bounds intersect the query bbox.
    Uses the USA geometry.parquet to find states (admin_level=1).
    """
    df = load_country_parquet("USA", admin_level=1)
    if df is None or len(df) == 0:
        return []

    result = []
    for _, row in df.iterrows():
        # Check bbox intersection
        if 'bbox_min_lon' in df.columns:
            c_min_lon = row.get('bbox_min_lon')
            c_min_lat = row.get('bbox_min_lat')
            c_max_lon = row.get('bbox_max_lon')
            c_max_lat = row.get('bbox_max_lat')

            if pd.notna(c_min_lon) and pd.notna(c_max_lon):
                if (c_max_lon >= min_lon and c_min_lon <= max_lon and
                    c_max_lat >= min_lat and c_min_lat <= max_lat):
                    # Extract state abbrev from loc_id (e.g., "USA-CA" -> "CA")
                    loc_id = row.get('loc_id', '')
                    if '-' in loc_id:
                        state_abbrev = loc_id.split('-')[1]
                        result.append(state_abbrev)

    return result


def _extract_display_name(value):
    """Extract display name from level name value (handles both string and array formats)."""
    if isinstance(value, list) and len(value) > 0:
        return value[0]  # First element is display name
    return value  # Already a string


def _load_admin_levels():
    """Load admin level names from reference/admin_levels.json."""
    global _admin_levels_cache
    if _admin_levels_cache is not None:
        return _admin_levels_cache

    try:
        admin_levels_path = Path(__file__).parent / "reference" / "admin_levels.json"
        if admin_levels_path.exists():
            with open(admin_levels_path, 'r', encoding='utf-8') as f:
                _admin_levels_cache = json.load(f)
                logger.debug(f"Loaded admin_levels.json with {len(_admin_levels_cache)} entries")
        else:
            logger.warning("admin_levels.json not found")
            _admin_levels_cache = {}
    except Exception as e:
        logger.warning(f"Failed to load admin_levels.json: {e}")
        _admin_levels_cache = {}

    return _admin_levels_cache


def _get_level_names(iso3: str) -> dict:
    """
    Get country-specific level names from reference/admin_levels.json.
    Returns dict like {1: "states", 2: "counties", 3: "places"} for USA.

    Format in admin_levels.json is array: [display_name, synonym1, synonym2, ...]
    This function extracts only the display name (first element).

    Falls back to DEFAULT_LEVEL_NAMES if country not found.
    """
    try:
        admin_levels = _load_admin_levels()
        if not admin_levels:
            return DEFAULT_LEVEL_NAMES

        # Get country-specific names
        country_names = admin_levels.get(iso3)
        if country_names:
            # Convert string keys to int keys, extract display name from arrays
            return {int(k): _extract_display_name(v) for k, v in country_names.items() if not k.startswith("_")}

        # Use _default from admin_levels.json if available
        default_names = admin_levels.get("_default")
        if default_names:
            return {int(k): _extract_display_name(v) for k, v in default_names.items() if not k.startswith("_")}

        return DEFAULT_LEVEL_NAMES
    except Exception as e:
        logger.debug(f"Error loading level names for {iso3}: {e}")
        return DEFAULT_LEVEL_NAMES


def _get_parent_hierarchy(df, parent_id: str, iso3: str) -> list:
    """
    Get list of parent names from immediate parent up to country.
    Returns list like ["California", "United States of America"].
    """
    names = []
    current_id = parent_id
    max_depth = 5  # Safety limit

    for _ in range(max_depth):
        if not current_id:
            break

        # Check if it's the country level
        if current_id == iso3:
            # Get country name from global.csv
            global_df = load_global_countries()
            if global_df is not None:
                country_row = global_df[global_df["loc_id"] == iso3]
                if len(country_row) > 0:
                    names.append(country_row.iloc[0].get("name", iso3))
            else:
                names.append(iso3)
            break

        # Find in country parquet
        parent_row = df[df["loc_id"] == current_id]
        if len(parent_row) == 0:
            break

        parent_name = parent_row.iloc[0].get("name", current_id)
        names.append(parent_name)

        # Move up to next parent
        current_id = parent_row.iloc[0].get("parent_id")

    return names


def _load_subcounty_for_viewport(iso3: str, admin_level: int, buffered_bbox: tuple, debug: bool = False):
    """
    Load sub-county geometry (levels 3+) from tiered files for a specific country.

    Args:
        iso3: Country code
        admin_level: Target admin level (3+)
        buffered_bbox: (min_lon, min_lat, max_lon, max_lat) with buffer
        debug: If True, add debug properties

    Returns:
        List of GeoJSON features
    """
    all_features = []
    logger.info(f"Loading subcounty geometry for {iso3} level {admin_level}, bbox={buffered_bbox}")

    # Check if this country has sub-county geometry at this level
    # First try non-partitioned (national file)
    df = load_subcounty_geometry(iso3, admin_level=admin_level)

    if df is not None and len(df) > 0:
        # National file exists - filter by bbox
        logger.info(f"Found national file with {len(df)} features for {iso3} level {admin_level}")
        df_filtered = _filter_df_by_bbox(df, buffered_bbox)
        logger.info(f"After bbox filter: {len(df_filtered)} features")
        geojson = df_to_geojson(df_filtered, polygon_only=True)
        if debug:
            for feature in geojson.get("features", []):
                feature["properties"]["current_admin_level"] = admin_level
        all_features.extend(geojson.get("features", []))

    else:
        # Try partitioned files (by state/region)
        # Get regions that intersect the bbox
        logger.info(f"No national file for {iso3} level {admin_level}, trying partitioned files")
        regions = get_regions_in_bbox(iso3, *buffered_bbox)
        logger.info(f"Regions in bbox: {regions}")

        if regions:
            for region_code in regions:
                df = load_subcounty_geometry(iso3, admin_level=admin_level, state_abbrev=region_code)
                if df is None or len(df) == 0:
                    logger.debug(f"No data for {iso3}-{region_code} level {admin_level}")
                    continue

                logger.info(f"Loaded {len(df)} features for {iso3}-{region_code} level {admin_level}")
                df_filtered = _filter_df_by_bbox(df, buffered_bbox)
                logger.info(f"After bbox filter: {len(df_filtered)} features")
                geojson = df_to_geojson(df_filtered, polygon_only=True)
                if debug:
                    for feature in geojson.get("features", []):
                        feature["properties"]["current_admin_level"] = admin_level
                all_features.extend(geojson.get("features", []))
        else:
            logger.warning(f"No regions found in bbox for {iso3}")

    logger.info(f"Total subcounty features for {iso3} level {admin_level}: {len(all_features)}")
    return all_features


def _filter_df_by_bbox(df, buffered_bbox):
    """Filter DataFrame by bounding box using bbox or centroid columns."""
    if 'bbox_min_lon' in df.columns:
        mask = (
            (df['bbox_max_lon'] >= buffered_bbox[0]) &
            (df['bbox_min_lon'] <= buffered_bbox[2]) &
            (df['bbox_max_lat'] >= buffered_bbox[1]) &
            (df['bbox_min_lat'] <= buffered_bbox[3])
        )
        return df[mask]
    elif 'centroid_lon' in df.columns:
        mask = (
            (df['centroid_lon'] >= buffered_bbox[0]) &
            (df['centroid_lon'] <= buffered_bbox[2]) &
            (df['centroid_lat'] >= buffered_bbox[1]) &
            (df['centroid_lat'] <= buffered_bbox[3])
        )
        return df[mask]
    return df


_crosswalk_reverse_cache: dict = {}


def _get_crosswalk_reverse(iso3: str) -> dict:
    """
    Load crosswalk.json for iso3 and return a reverse map:
      geo_loc_id -> local_abbrev  (e.g. "USA-G109436" -> "NY")

    Result is cached in memory. Returns empty dict if no crosswalk exists.
    """
    if iso3 in _crosswalk_reverse_cache:
        return _crosswalk_reverse_cache[iso3]

    crosswalk_path = COUNTRIES_DIR / iso3 / "crosswalk.json"
    if not crosswalk_path.exists():
        _crosswalk_reverse_cache[iso3] = {}
        return {}

    try:
        with open(crosswalk_path, encoding="utf-8") as f:
            cw = json.load(f)
        mappings = cw.get("mappings", {})
        # mappings: "USA-NY" -> "USA-G109436"
        # reverse:  "USA-G109436" -> "NY"
        reverse = {}
        for local_loc_id, geo_loc_id in mappings.items():
            # Extract the local abbreviation from "USA-NY" -> "NY"
            parts = local_loc_id.split("-", 1)
            if len(parts) == 2:
                reverse[geo_loc_id] = parts[1]
        _crosswalk_reverse_cache[iso3] = reverse
        return reverse
    except Exception as e:
        logger.warning(f"Failed to load crosswalk for {iso3}: {e}")
        _crosswalk_reverse_cache[iso3] = {}
        return {}


def get_regions_in_bbox(iso3: str, min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    """
    Return local region/state codes whose bounds intersect the query bbox.
    Uses the country's geometry.parquet to find admin_level=1 regions, then
    resolves geo loc_ids back to local codes via the crosswalk reverse map.
    """
    df = load_country_parquet(iso3, admin_level=1)
    if df is None or len(df) == 0:
        logger.debug(f"No admin_level=1 data found for {iso3}")
        return []

    crosswalk_reverse = _get_crosswalk_reverse(iso3)

    def resolve_region_code(loc_id: str) -> str:
        """Convert a geo loc_id to a local region code via crosswalk, or fall back to raw segment."""
        if loc_id in crosswalk_reverse:
            return crosswalk_reverse[loc_id]
        # No crosswalk entry - extract second segment as-is
        parts = loc_id.split("-", 1)
        return parts[1] if len(parts) == 2 else loc_id

    result = []
    has_bbox = 'bbox_min_lon' in df.columns
    has_centroid = 'centroid_lon' in df.columns

    if not has_bbox and not has_centroid:
        logger.warning(f"No bbox or centroid columns in {iso3} admin_level=1 parquet")
        for _, row in df.iterrows():
            loc_id = row.get('loc_id', '')
            if loc_id:
                result.append(resolve_region_code(loc_id))
        logger.debug(f"Returning all {len(result)} regions for {iso3} (no spatial filter)")
        return result

    for _, row in df.iterrows():
        intersects = False

        if has_bbox:
            c_min_lon = row.get('bbox_min_lon')
            c_min_lat = row.get('bbox_min_lat')
            c_max_lon = row.get('bbox_max_lon')
            c_max_lat = row.get('bbox_max_lat')

            if pd.notna(c_min_lon) and pd.notna(c_max_lon):
                intersects = (c_max_lon >= min_lon and c_min_lon <= max_lon and
                              c_max_lat >= min_lat and c_min_lat <= max_lat)
        elif has_centroid:
            c_lon = row.get('centroid_lon')
            c_lat = row.get('centroid_lat')
            if pd.notna(c_lon) and pd.notna(c_lat):
                intersects = (c_lon >= min_lon and c_lon <= max_lon and
                              c_lat >= min_lat and c_lat <= max_lat)

        if intersects:
            loc_id = row.get('loc_id', '')
            if loc_id:
                result.append(resolve_region_code(loc_id))

    logger.debug(f"Found {len(result)} regions in bbox for {iso3}: {result}")
    return result


def get_viewport_geometry(admin_level: int, bbox: tuple, debug: bool = False):
    """
    Load features at admin_level within bounding box.

    Args:
        admin_level: Target admin level (0=countries, 1=states, 2=counties, 3=ZCTAs,
                     4=census tracts, 5=block groups, 6=blocks)
        bbox: (min_lon, min_lat, max_lon, max_lat)
        debug: If True, add coverage info for level 0 features

    Returns:
        GeoJSON FeatureCollection with features in viewport
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    # Buffer for smooth panning - proportional to viewport size.
    # Cloud/S3 mode: all levels at 1% to keep feature caps focused on visible area.
    # Local mode (restore when switching back): smart scaling by level:
    #   level 0-1: 0.50  (world/country - few large shapes, prefetch aggressively)
    #   level 2:   0.30  (county/district view)
    #   level 3+:  0.15  (tracts/blocks - many small shapes, tight budget)
    if admin_level >= 3:
        buffer_factor = 0.01
    elif admin_level == 2:
        buffer_factor = 0.01
    else:
        buffer_factor = 0.01
    viewport_width = max_lon - min_lon
    viewport_height = max_lat - min_lat
    buffer_lon = viewport_width * buffer_factor
    buffer_lat = viewport_height * buffer_factor
    buffered_bbox = (
        min_lon - buffer_lon,
        min_lat - buffer_lat,
        max_lon + buffer_lon,
        max_lat + buffer_lat
    )

    # For level 0 (countries), just return from global.csv
    if admin_level == 0:
        df = load_global_countries()
        if df is None:
            return {"type": "FeatureCollection", "features": []}

        # Filter by bbox if bbox columns exist
        if 'bbox_min_lon' in df.columns:
            mask = (
                (df['bbox_max_lon'] >= buffered_bbox[0]) &
                (df['bbox_min_lon'] <= buffered_bbox[2]) &
                (df['bbox_max_lat'] >= buffered_bbox[1]) &
                (df['bbox_min_lat'] <= buffered_bbox[3])
            )
            df = df[mask]

        geojson = df_to_geojson(df, polygon_only=True)

        # Add coverage info for debug mode (calculate on-the-fly from parquet)
        if debug:
            for feature in geojson.get("features", []):
                loc_id = feature.get("properties", {}).get("loc_id")
                feature["properties"]["current_admin_level"] = admin_level

                if loc_id:
                    # Calculate from actual parquet data, starting from level 1
                    cov_info = calculate_coverage_from_parquet(loc_id, from_level=1)
                    feature["properties"]["actual_depth"] = cov_info.get("actual_depth", 0)
                    feature["properties"]["expected_depth"] = cov_info.get("actual_depth", 0)
                    feature["properties"]["coverage"] = cov_info.get("coverage", 0)
                    feature["properties"]["level_counts"] = cov_info.get("level_counts", {})
                    feature["properties"]["geometry_counts"] = cov_info.get("geometry_counts", {})
                    feature["properties"]["drillable_depth"] = cov_info.get("drillable_depth", 0)
                else:
                    feature["properties"]["actual_depth"] = 0
                    feature["properties"]["expected_depth"] = 0
                    feature["properties"]["coverage"] = 0
                    feature["properties"]["level_counts"] = {}
                    feature["properties"]["geometry_counts"] = {}
                    feature["properties"]["drillable_depth"] = 0

        return geojson

    # Find countries that intersect the viewport
    countries = get_countries_in_bbox(*buffered_bbox)

    if not countries:
        return {"type": "FeatureCollection", "features": []}

    all_features = []

    # For admin levels 3+, try sub-county geometry files for each country
    countries_with_subcounty = []
    if admin_level >= 3:
        for iso3 in countries:
            subcounty_features = _load_subcounty_for_viewport(iso3, admin_level, buffered_bbox, debug)
            if subcounty_features:
                all_features.extend(subcounty_features)
                countries_with_subcounty.append(iso3)
        # Remove countries that were handled via subcounty geometry
        countries = [c for c in countries if c not in countries_with_subcounty]

    for iso3 in countries:
        # Load only this level from parquet (predicate pushdown)
        df = load_country_parquet(iso3, admin_level=admin_level)

        if df is None or len(df) == 0:
            # Fallback: try one level up if no data at this level
            if admin_level > 0:
                df = load_country_parquet(iso3, admin_level=admin_level - 1)
            if df is None or len(df) == 0:
                continue

        # Filter by bbox intersection using pre-computed bbox columns
        if 'bbox_min_lon' in df.columns:
            mask = (
                (df['bbox_max_lon'] >= buffered_bbox[0]) &
                (df['bbox_min_lon'] <= buffered_bbox[2]) &
                (df['bbox_max_lat'] >= buffered_bbox[1]) &
                (df['bbox_min_lat'] <= buffered_bbox[3])
            )
            df_filtered = df[mask]
        else:
            # No bbox columns - use centroid as fallback
            if 'centroid_lon' in df.columns and 'centroid_lat' in df.columns:
                mask = (
                    (df['centroid_lon'] >= buffered_bbox[0]) &
                    (df['centroid_lon'] <= buffered_bbox[2]) &
                    (df['centroid_lat'] >= buffered_bbox[1]) &
                    (df['centroid_lat'] <= buffered_bbox[3])
                )
                df_filtered = df[mask]
            else:
                df_filtered = df

        # Convert to features
        geojson = df_to_geojson(df_filtered, polygon_only=True)

        # Add debug info for sub-country levels (calculate on-the-fly from parquet)
        if debug:
            # Calculate from actual parquet data, starting from current admin_level
            cov_info = calculate_coverage_from_parquet(iso3, from_level=admin_level)

            for feature in geojson.get("features", []):
                feature["properties"]["current_admin_level"] = admin_level
                feature["properties"]["actual_depth"] = cov_info.get("actual_depth", 0)
                feature["properties"]["expected_depth"] = cov_info.get("actual_depth", 0)
                feature["properties"]["coverage"] = cov_info.get("coverage", 0)
                feature["properties"]["level_counts"] = cov_info.get("level_counts", {})
                feature["properties"]["geometry_counts"] = cov_info.get("geometry_counts", {})
                feature["properties"]["drillable_depth"] = cov_info.get("drillable_depth", 0)

        all_features.extend(geojson.get("features", []))

    # Per-level feature cap to limit browser memory and S3 transfer volume.
    # Tighter caps at deep zoom where shapes are small and viewport covers fewer.
    MAX_FEATURES_BY_LEVEL = {
        0: 300,   # Countries - global.csv is local anyway
        1: 500,   # States / provinces
        2: 1000,  # Counties / districts
        3: 500,   # Tracts / ZCTAs
        4: 300,   # Block groups
        5: 200,   # Blocks
    }
    MAX_FEATURES = MAX_FEATURES_BY_LEVEL.get(admin_level, 200)
    truncated = False
    if len(all_features) > MAX_FEATURES:
        logger.warning(f"Truncating {len(all_features)} features to {MAX_FEATURES} for admin level {admin_level}")

        # Calculate viewport center
        center_lon = (min_lon + max_lon) / 2
        center_lat = (min_lat + max_lat) / 2

        # Pre-compute distances once (O(n)) instead of during sort (O(n log n) function calls)
        distances = []
        for f in all_features:
            props = f.get("properties", {})
            f_lon = props.get("centroid_lon")
            f_lat = props.get("centroid_lat")
            if f_lon is None or f_lat is None:
                # Fallback to bbox center
                b1, b2 = props.get("bbox_min_lon"), props.get("bbox_max_lon")
                b3, b4 = props.get("bbox_min_lat"), props.get("bbox_max_lat")
                if b1 is not None and b2 is not None:
                    f_lon, f_lat = (b1 + b2) / 2, (b3 + b4) / 2
                else:
                    distances.append(float('inf'))
                    continue
            distances.append((f_lon - center_lon) ** 2 + (f_lat - center_lat) ** 2)

        # Sort indices by distance, take first N
        sorted_indices = sorted(range(len(all_features)), key=lambda i: distances[i])
        all_features = [all_features[i] for i in sorted_indices[:MAX_FEATURES]]
        truncated = True

    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "admin_level": admin_level,
            "countries_searched": len(countries),
            "feature_count": len(all_features),
            "truncated": truncated
        }
    }


def clear_cache():
    """Clear all cached geometry data. Useful when data files are updated."""
    global _country_parquet_cache, _global_countries_cache, _country_bounds_cache, _subcounty_geometry_cache
    _country_parquet_cache = {}
    _global_countries_cache = None
    _country_bounds_cache = None
    _subcounty_geometry_cache = {}
    logger.info("Geometry cache cleared")


def get_selection_geometries(loc_ids: list):
    """
    Get geometries for specific loc_ids for disambiguation selection mode.

    Args:
        loc_ids: List of location IDs to fetch geometries for

    Returns:
        GeoJSON FeatureCollection with requested geometries
    """
    if not loc_ids:
        return {"type": "FeatureCollection", "features": []}

    features = []

    # Group by country (first part of loc_id) for efficient loading
    by_country = {}
    for loc_id in loc_ids:
        parts = loc_id.split("-")
        iso3 = parts[0]
        if iso3 not in by_country:
            by_country[iso3] = []
        by_country[iso3].append(loc_id)

    # For each country, load parquet and filter to requested loc_ids
    for iso3, country_loc_ids in by_country.items():
        # Check if any are country-level (just the ISO3 code)
        country_level_ids = [lid for lid in country_loc_ids if lid == iso3]
        sub_level_ids = [lid for lid in country_loc_ids if lid != iso3]

        # Fetch country-level from global.csv
        if country_level_ids:
            global_df = load_global_countries()
            if global_df is not None:
                country_rows = global_df[global_df["loc_id"].isin(country_level_ids)]
                if len(country_rows) > 0:
                    country_geojson = df_to_geojson(country_rows, polygon_only=True)
                    features.extend(country_geojson.get("features", []))

        # Fetch sub-country levels from parquet
        if sub_level_ids:
            country_df = load_country_parquet(iso3)
            if country_df is not None:
                filtered = country_df[country_df["loc_id"].isin(sub_level_ids)]
                if len(filtered) > 0:
                    sub_geojson = df_to_geojson(filtered, polygon_only=True)
                    features.extend(sub_geojson.get("features", []))

    logger.debug(f"Loaded {len(features)} geometries for selection from {len(loc_ids)} loc_ids")

    return {"type": "FeatureCollection", "features": features}
