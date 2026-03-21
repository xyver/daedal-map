"""
Data loading, catalog management, and metadata functions.

Handles loading the unified catalog.json and source metadata from the parquet-based
data structure.

Data Structure (layered):
    county-map-data/
        catalog.json              # Unified catalog with 'path' field per source

        global/                   # Country-level datasets
            geometry.csv          # Country outlines
            {source_id}/          # e.g., owid_co2/, imf_bop/
                metadata.json
                *.parquet
            un_sdg/               # Nested folder for SDGs
                01/ ... 17/

        countries/                # Sub-national data
            USA/
                geometry.parquet  # States + counties
                index.json        # Country-level metadata
                {source_id}/      # e.g., noaa_storms/, census_agesex/
                    metadata.json
                    *.parquet

        geometry/                 # Bank of all country geometries (fallback)
            {ISO3}.parquet

Path resolution uses catalog.json 'path' field:
    source_id='usgs_earthquakes' -> path='countries/USA/usgs_earthquakes'
"""

import json
import logging
import os
import time
from pathlib import Path

from .pack_state import build_active_catalog
from .paths import CATALOG_PATH, COUNTRIES_DIR, DATA_ROOT, GEOMETRY_DIR
from .duckdb_helpers import select_rows
from .runtime_config import get_runtime_config

logger = logging.getLogger("mapmover")

# Global data catalog
data_catalog = {}

# Cache for source metadata
_metadata_cache = {}

# Cache for catalog.json with TTL so R2 updates are picked up without a restart.
# After the TTL expires the next request re-reads catalog.json from disk.
_catalog_cache = None
_catalog_cache_time = 0.0
_CATALOG_TTL_SECONDS = 300  # 5 minutes
_catalog_missing_time = 0.0
_CATALOG_MISS_TTL_SECONDS = 15


def get_data_folder():
    """Get the data folder path (resolved via paths.py)."""
    return DATA_ROOT


def get_catalog_path():
    """Get the catalog.json path (resolved via paths.py)."""
    return CATALOG_PATH


def _refresh_catalog_from_s3(catalog_path: Path) -> None:
    """Re-download catalog.json from R2 to local disk. Logs and swallows errors."""
    try:
        import boto3 as _boto3
        cloud_cfg = get_runtime_config().get("cloud", {})
        bucket = os.environ.get("S3_BUCKET", "").strip() or str(cloud_cfg.get("bucket", "")).strip()
        if not bucket:
            return
        prefix = (os.environ.get("S3_PREFIX", "") or str(cloud_cfg.get("prefix", ""))).strip().strip("/")
        key = f"{prefix}/catalog.json" if prefix else "catalog.json"
        endpoint_url = os.environ.get("S3_ENDPOINT_URL") or cloud_cfg.get("endpoint_url")
        region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "auto"
        client = _boto3.client("s3", endpoint_url=endpoint_url, region_name=region)
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(catalog_path))
        logger.info("catalog.json refreshed from R2")
    except Exception as exc:
        logger.warning(f"catalog.json R2 refresh failed, using cached copy: {exc}")


def load_catalog():
    """
    Load the unified catalog.json file.
    Cached with a 5-minute TTL. In S3 mode, re-downloads catalog.json from R2
    on each TTL expiry so catalog updates go live without a Railway restart.

    Returns:
        dict: Catalog with sources, or empty dict if not found
    """
    global _catalog_cache, _catalog_cache_time, _catalog_missing_time

    now = time.time()
    if _catalog_cache is not None and (now - _catalog_cache_time) < _CATALOG_TTL_SECONDS:
        return _catalog_cache

    catalog_path = get_catalog_path()
    runtime_mode = str(get_runtime_config().get("runtime_mode", "local")).strip().lower()

    if runtime_mode == "cloud" and not catalog_path.exists() and (now - _catalog_missing_time) < _CATALOG_MISS_TTL_SECONDS:
        return {"sources": [], "total_sources": 0}

    if runtime_mode == "cloud":
        _refresh_catalog_from_s3(catalog_path)

    if not catalog_path or not catalog_path.exists():
        _catalog_missing_time = now
        logger.warning(f"Catalog not found at {catalog_path}")
        return {"sources": [], "total_sources": 0}

    try:
        with open(catalog_path, 'r', encoding='utf-8') as f:
            raw_catalog = json.load(f)
            _catalog_cache = raw_catalog
            _catalog_cache_time = now
            active_catalog = build_active_catalog(raw_catalog)
            logger.debug(f"Loaded catalog.json with {len(raw_catalog.get('sources', []))} sources")
            return active_catalog
    except Exception as e:
        logger.error(f"Error loading catalog.json: {e}")
        return {"sources": [], "total_sources": 0}


def load_full_catalog():
    """Load the full catalog without active-pack filtering."""
    global _catalog_cache, _catalog_cache_time

    now = time.time()
    if _catalog_cache is not None and (now - _catalog_cache_time) < _CATALOG_TTL_SECONDS:
        return _catalog_cache

    _ = load_catalog()
    return _catalog_cache or {"sources": [], "total_sources": 0}


def get_source_path(source_id: str):
    """
    Get the path to a source folder using the path field from catalog.

    Args:
        source_id: Source identifier (e.g., 'owid_co2', 'usgs_earthquakes')

    Returns:
        Path: Full path to source folder, or None if not found
    """
    catalog = load_catalog()
    for source in catalog.get("sources", []):
        if source.get("source_id") == source_id:
            # Use path field if present, otherwise fall back to old structure
            source_path = source.get("path", f"global/{source_id}")
            return DATA_ROOT / source_path

    # Source not in catalog - try old path as fallback
    return DATA_ROOT / "global" / source_id


def load_source_metadata(source_id: str):
    """
    Load metadata.json for a specific source.

    Args:
        source_id: Source identifier (e.g., 'owid_co2', 'census_population')

    Returns:
        dict: Source metadata or None if not found
    """
    if source_id in _metadata_cache:
        return _metadata_cache[source_id]

    source_folder = get_source_path(source_id)
    if not source_folder or not source_folder.exists():
        return None

    metadata_path = source_folder / "metadata.json"
    if not metadata_path.exists():
        return None

    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            _metadata_cache[source_id] = metadata
            return metadata
    except Exception as e:
        logger.error(f"Error loading metadata for {source_id}: {e}")
        return None


def get_source_by_topic(topic: str):
    """
    Find sources that match a topic keyword.

    Args:
        topic: Topic to search for (e.g., 'co2', 'population', 'health')

    Returns:
        list: Matching source entries from catalog
    """
    global data_catalog
    topic_lower = topic.lower()

    matches = []
    for source in data_catalog.get("sources", []):
        # Check topic_tags
        if any(topic_lower in tag.lower() for tag in source.get("topic_tags", [])):
            matches.append(source)
            continue
        # Check keywords
        if any(topic_lower in kw.lower() for kw in source.get("keywords", [])):
            matches.append(source)
            continue
        # Check source_id
        if topic_lower in source.get("source_id", "").lower():
            matches.append(source)

    return matches


def initialize_catalog():
    """
    Initialize the data catalog by loading catalog.json.
    Called at server startup.
    """
    global data_catalog

    data_catalog = load_catalog()

    if data_catalog.get("total_sources", 0) > 0:
        logger.info(f"Data catalog loaded: {data_catalog['total_sources']} sources")
        for source in data_catalog.get("sources", [])[:5]:
            logger.info(f"  - {source.get('source_id')}: {source.get('geographic_level')}")
        if data_catalog['total_sources'] > 5:
            logger.info(f"  ... and {data_catalog['total_sources'] - 5} more")
    else:
        logger.warning("Data catalog is empty or not found")


def get_data_catalog():
    """Get the current data catalog."""
    return data_catalog


def clear_metadata_cache():
    """Clear the metadata cache."""
    global _metadata_cache
    _metadata_cache = {}
    logger.info("Metadata cache cleared")


def get_geometry_folder():
    """Get the geometry folder path (resolved via paths.py)."""
    return GEOMETRY_DIR


def get_countries_folder():
    """Get the countries folder path (resolved via paths.py)."""
    return COUNTRIES_DIR


def load_geometry_for_country(iso3: str):
    """
    Load geometry for a country using 3-tier fallback:
    1. countries/{ISO3}/geometry.parquet (local/official source like NUTS)
    2. countries/{ISO3}/crosswalk.json -> geometry/{ISO3}.parquet (translated)
    3. geometry/{ISO3}.parquet (GADM fallback)

    Returns:
        tuple: (GeoDataFrame, crosswalk_dict or None)
    """
    import pandas as pd

    countries_folder = get_countries_folder()
    geometry_folder = get_geometry_folder()

    # Tier 1: Country-specific geometry (NUTS, ABS LGA, etc.)
    if countries_folder:
        country_geom_path = countries_folder / iso3 / "geometry.parquet"
        if country_geom_path.exists():
            try:
                gdf = select_rows(country_geom_path)
                if gdf.empty:
                    gdf = pd.read_parquet(country_geom_path)
                logger.debug(f"Loaded {len(gdf)} features from {country_geom_path}")
                return gdf, None
            except Exception as e:
                logger.warning(f"Error loading {country_geom_path}: {e}")

    # Tier 2: Load crosswalk if exists (for translating loc_ids)
    crosswalk = None
    if countries_folder:
        crosswalk_path = countries_folder / iso3 / "crosswalk.json"
        if crosswalk_path.exists():
            try:
                import json
                with open(crosswalk_path, 'r') as f:
                    crosswalk = json.load(f)
                logger.debug(f"Loaded crosswalk for {iso3}: {len(crosswalk.get('mappings', {}))} mappings")
            except Exception as e:
                logger.warning(f"Error loading crosswalk {crosswalk_path}: {e}")

    # Tier 3: GADM fallback geometry
    if geometry_folder:
        gadm_path = geometry_folder / f"{iso3}.parquet"
        if gadm_path.exists():
            try:
                gdf = select_rows(gadm_path)
                if gdf.empty:
                    gdf = pd.read_parquet(gadm_path)
                logger.debug(f"Loaded {len(gdf)} features from GADM {gadm_path}")
                return gdf, crosswalk
            except Exception as e:
                logger.warning(f"Error loading {gadm_path}: {e}")

    logger.warning(f"No geometry found for {iso3}")
    return None, crosswalk


def fetch_geometries_by_loc_ids(loc_ids: list) -> dict:
    """
    Fetch geometries from parquet files for a list of loc_ids.
    Uses 3-tier geometry fallback: country folder -> crosswalk -> GADM.
    Used for "show borders" functionality.

    Args:
        loc_ids: List of location IDs (e.g., ["USA-WA-53073", "USA-OR-41067"])

    Returns:
        GeoJSON FeatureCollection with geometries
    """
    import pandas as pd
    import json as json_module

    if not loc_ids:
        return {"type": "FeatureCollection", "features": []}

    # Group loc_ids by country (first part before dash, or whole ID for country-level)
    country_loc_ids = {}
    for loc_id in loc_ids:
        parts = loc_id.split("-")
        country = parts[0] if parts else loc_id
        if country not in country_loc_ids:
            country_loc_ids[country] = []
        country_loc_ids[country].append(loc_id)

    all_features = []

    for country, lids in country_loc_ids.items():
        countries_folder = get_countries_folder()
        geometry_folder = get_geometry_folder()
        country_geom_path = countries_folder / country / "geometry.parquet" if countries_folder else None
        crosswalk_path = countries_folder / country / "crosswalk.json" if countries_folder else None
        fallback_geom_path = geometry_folder / f"{country}.parquet" if geometry_folder else None

        parquet_path = None
        crosswalk = None
        uses_crosswalk = False
        if country_geom_path and country_geom_path.exists():
            parquet_path = country_geom_path
        elif crosswalk_path and crosswalk_path.exists() and fallback_geom_path and fallback_geom_path.exists():
            parquet_path = fallback_geom_path
            uses_crosswalk = True
            try:
                with open(crosswalk_path, "r", encoding="utf-8") as f:
                    crosswalk = json_module.load(f)
            except Exception as e:
                logger.warning(f"Error loading crosswalk {crosswalk_path}: {e}")
        elif fallback_geom_path and fallback_geom_path.exists():
            parquet_path = fallback_geom_path

        if parquet_path is None:
            logger.warning(f"No geometry found for {country}")
            continue

        remaining_lids = set(lids)
        requested_ids = set(remaining_lids)
        if uses_crosswalk and crosswalk:
            mappings = crosswalk.get("mappings", {})
            requested_ids.update(mappings.get(loc_id) for loc_id in remaining_lids if mappings.get(loc_id))

        gdf = select_rows(
            parquet_path,
            columns=["loc_id", "name", "admin_level", "parent_id", "geometry"],
            in_filters={"loc_id": sorted(requested_ids)},
        )
        if gdf.empty:
            # Fall back to the old whole-file load path if the selective read fails.
            gdf, crosswalk = load_geometry_for_country(country)
        elif uses_crosswalk and crosswalk:
            mappings = crosswalk.get("mappings", {})
            reverse_map = {v: k for k, v in mappings.items()}
            gdf["local_loc_id"] = gdf["loc_id"].map(reverse_map)

        if gdf is None or len(gdf) == 0:
            logger.warning(f"No geometry rows found for {country}")
            continue

        found_lids = set()

        try:
            # First try direct match
            filtered = gdf[gdf['loc_id'].isin(remaining_lids)]

            if len(filtered) > 0:
                for _, row in filtered.iterrows():
                    # Handle geometry - could be string or shapely geometry
                    geom = row.get('geometry')
                    if geom is None:
                        continue

                    # Convert to dict if needed
                    if hasattr(geom, '__geo_interface__'):
                        geom_dict = geom.__geo_interface__
                    elif isinstance(geom, str):
                        geom_dict = json_module.loads(geom)
                    else:
                        continue

                    feature = {
                        "type": "Feature",
                        "geometry": geom_dict,
                        "properties": {
                            "loc_id": row.get("loc_id"),
                            "name": row.get("name"),
                            "admin_level": row.get("admin_level"),
                            "parent_id": row.get("parent_id"),
                        }
                    }
                    all_features.append(feature)
                    found_lids.add(row.get("loc_id"))

            remaining_lids -= found_lids

            # If crosswalk exists and we still have unmatched loc_ids, try translation
            if crosswalk and remaining_lids:
                mappings = crosswalk.get('mappings', {})
                for loc_id in list(remaining_lids):
                    gadm_id = mappings.get(loc_id)
                    if gadm_id:
                        match = gdf[gdf['loc_id'] == gadm_id]
                        if len(match) > 0:
                            row = match.iloc[0]
                            geom = row.get('geometry')
                            if geom is None:
                                continue

                            if hasattr(geom, '__geo_interface__'):
                                geom_dict = geom.__geo_interface__
                            elif isinstance(geom, str):
                                geom_dict = json_module.loads(geom)
                            else:
                                continue

                            feature = {
                                "type": "Feature",
                                "geometry": geom_dict,
                                "properties": {
                                    "loc_id": loc_id,  # Use original loc_id
                                    "name": row.get("name"),
                                    "admin_level": row.get("admin_level"),
                                    "parent_id": row.get("parent_id"),
                                    "_crosswalk_from": gadm_id,  # Track translation
                                }
                            }
                            all_features.append(feature)
                            remaining_lids.discard(loc_id)

            if remaining_lids:
                logger.debug(f"No geometry found for {len(remaining_lids)} loc_ids in {country}: {list(remaining_lids)[:5]}")

        except Exception as e:
            logger.error(f"Error processing geometry for {country}: {e}")

    logger.info(f"Fetched {len(all_features)} geometries for {len(loc_ids)} loc_ids")

    return {
        "type": "FeatureCollection",
        "features": all_features
    }
