"""
County Map API - FastAPI Entry Point

This is the main entry point for the county-map application.
All business logic is in the mapmover/ package - this file only handles:
- FastAPI app setup
- CORS middleware
- Static file serving
- Route definitions (thin wrappers calling handler functions)
"""

import sys
import io
import json
import hashlib
import logging
import traceback
from datetime import datetime
from pathlib import Path

# Force UTF-8 encoding for all output streams
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import msgpack
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directory for file paths (works in Docker and locally)
BASE_DIR = Path(__file__).resolve().parent

# Import from mapmover package
from mapmover import (
    # Data loading
    initialize_catalog,
    # Geography
    load_conversions,
    # Logging
    logger,
    log_error_to_cloud,
    # Paths
    DATA_ROOT,
    GLOBAL_DIR,
    COUNTRIES_DIR,
    get_dataset_path,
    # Disaster filters
    apply_location_filters,
    get_default_min_year,
    # Session cache
    session_manager,
    # Cache signature (used by /api/cache/delta endpoint)
    CacheSignature,
)

# Order Taker system (Phase 1B - replaces old multi-LLM chat)
from mapmover.order_taker import interpret_request
from mapmover.order_executor import execute_order

# Preprocessor for tiered context system
from mapmover.preprocessor import preprocess_query

# Postprocessor for validation and derived field expansion
from mapmover.postprocessor import postprocess_order, get_display_items

# Order Queue for async processing (Phase 2)
from mapmover.order_queue import order_queue, processor as order_processor

# Geometry handlers (parquet-based)
from mapmover.geometry_handlers import (
    get_countries_geometry as get_countries_geometry_handler,
    get_location_children as get_location_children_handler,
    get_location_places as get_location_places_handler,
    get_location_info,
    get_viewport_geometry as get_viewport_geometry_handler,
    get_selection_geometries as get_selection_geometries_handler,
    clear_cache as clear_geometry_cache,
)

# Settings management
from mapmover.settings import (
    get_settings_with_status,
    save_settings,
    init_backup_folders,
)


# === MessagePack Response Helpers ===

def msgpack_response(data: dict, status_code: int = 200) -> Response:
    """Standard MessagePack response for all API endpoints.

    Usage:
        return msgpack_response({"data": result, "count": len(result)})
    """
    return Response(
        content=msgpack.packb(data, use_bin_type=True),
        media_type="application/msgpack",
        status_code=status_code
    )


def msgpack_error(message: str, status_code: int = 500) -> Response:
    """Standard error response in MessagePack format."""
    return msgpack_response({"error": message}, status_code)


async def decode_request_body(request: Request) -> dict:
    """Decode MessagePack request body."""
    body_bytes = await request.body()
    return msgpack.unpackb(body_bytes, raw=False)


# === Data Processing Helpers ===

def ensure_year_column(df):
    """
    Extract year from timestamp column if not already present.
    Modifies DataFrame in place and returns it.

    Usage:
        df = ensure_year_column(df)
    """
    import pandas as pd
    if 'year' not in df.columns and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['year'] = df['timestamp'].dt.year
    return df


def filter_by_proximity(df, lat: float, lon: float, radius_km: float,
                        lat_col: str = 'latitude', lon_col: str = 'longitude'):
    """
    Filter DataFrame to rows within radius_km of a point.
    Uses approximate Haversine distance (rectangular bounding box).

    Usage:
        df = filter_by_proximity(df, 35.0, -120.0, 150.0)
    """
    import numpy as np
    lat_range = radius_km / 111.0
    lon_range = radius_km / (111.0 * max(0.01, np.cos(np.radians(lat))))

    return df[
        (df[lat_col] >= lat - lat_range) &
        (df[lat_col] <= lat + lat_range) &
        (df[lon_col] >= lon - lon_range) &
        (df[lon_col] <= lon + lon_range)
    ]


def filter_by_time_window(df, timestamp: str, days_before: int, days_after: int,
                          time_col: str = 'timestamp'):
    """
    Filter DataFrame to rows within a time window around a timestamp.
    Handles timezone conversion automatically.

    Usage:
        df = filter_by_time_window(df, "2024-01-15T12:00:00Z", 30, 60)
    """
    import pandas as pd
    from datetime import timedelta

    try:
        event_time = pd.to_datetime(timestamp)
        if event_time.tzinfo is not None:
            event_time = event_time.tz_convert('UTC').tz_localize(None)

        start_time = event_time - timedelta(days=days_before)
        end_time = event_time + timedelta(days=days_after)

        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        if df[time_col].dt.tz is not None:
            df[time_col] = df[time_col].dt.tz_convert('UTC').dt.tz_localize(None)

        return df[(df[time_col] >= start_time) & (df[time_col] <= end_time)]
    except Exception as e:
        logger.warning(f"Could not parse timestamp {timestamp}: {e}")
        return df


def filter_by_time_range(df, start: str = None, end: str = None, time_col: str = 'timestamp'):
    """
    Filter DataFrame by start/end timestamp range.
    Accepts ISO format strings or millisecond timestamps.
    Falls back to year-based filtering if no timestamp column exists.

    Usage:
        df = filter_by_time_range(df, "2025-12-24T00:00:00Z", "2026-01-23T00:00:00Z")
        df = filter_by_time_range(df, "1706140800000", "1708819200000")
    """
    import pandas as pd

    if start is None and end is None:
        return df

    try:
        # Parse start/end (support both ISO strings and millisecond timestamps)
        def parse_ts(val):
            if val is None:
                return None
            # Try as milliseconds first (all digits)
            if str(val).isdigit():
                return pd.Timestamp(int(val), unit='ms')
            return pd.to_datetime(val)

        start_ts = parse_ts(start)
        end_ts = parse_ts(end)

        # Strip timezone info for comparison
        if start_ts and start_ts.tzinfo is not None:
            start_ts = start_ts.tz_convert('UTC').tz_localize(None)
        if end_ts and end_ts.tzinfo is not None:
            end_ts = end_ts.tz_convert('UTC').tz_localize(None)

        # If timestamp column exists, filter precisely
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
            if df[time_col].dt.tz is not None:
                df[time_col] = df[time_col].dt.tz_convert('UTC').dt.tz_localize(None)
            if start_ts is not None:
                df = df[df[time_col] >= start_ts]
            if end_ts is not None:
                df = df[df[time_col] <= end_ts]
        elif 'year' in df.columns:
            # Fallback: derive year range from timestamps
            if start_ts is not None:
                df = df[df['year'] >= start_ts.year]
            if end_ts is not None:
                df = df[df['year'] <= end_ts.year]

        return df
    except Exception as e:
        logger.warning(f"Could not filter by time range ({start} - {end}): {e}")
        return df


def build_geojson_features(df, property_builders: dict,
                           lat_col: str = 'latitude', lon_col: str = 'longitude'):
    """
    Build GeoJSON Point features from a DataFrame.

    Args:
        df: DataFrame with lat/lon columns
        property_builders: Dict mapping property names to builder functions.
            Each function receives a row dict and returns the property value.
            Example: {"magnitude": lambda r: safe_float(r, 'magnitude')}
        lat_col: Name of latitude column
        lon_col: Name of longitude column

    Returns:
        List of GeoJSON Feature dicts

    Usage:
        features = build_geojson_features(df, {
            "event_id": lambda r: r.get('event_id', ''),
            "magnitude": lambda r: safe_float(r, 'magnitude'),
        })

    Performance: Uses to_dict('records') instead of iterrows() for 10-100x speedup.
    """
    import pandas as pd

    # Filter out rows with null coordinates using vectorized operation
    valid_mask = df[lat_col].notna() & df[lon_col].notna()
    valid_df = df[valid_mask]

    if valid_df.empty:
        return []

    # Convert to list of dicts - MUCH faster than iterrows() (10-100x)
    records = valid_df.to_dict('records')

    features = []
    for row in records:
        props = {name: builder(row) for name, builder in property_builders.items()}

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row[lon_col]), float(row[lat_col])]
            },
            "properties": props
        })

    return features


# Property extraction helpers for build_geojson_features
def safe_float(row, col, default=None):
    """Safely extract float from row, returns default if NA."""
    import pandas as pd
    val = row.get(col)
    return float(val) if pd.notna(val) else default

def safe_int(row, col, default=None):
    """Safely extract int from row, returns default if NA."""
    import pandas as pd
    val = row.get(col)
    return int(val) if pd.notna(val) else default

def safe_str(row, col, default=''):
    """Safely extract string from row, returns default if NA."""
    import pandas as pd
    val = row.get(col)
    return str(val) if pd.notna(val) else default

def safe_bool(row, col, default=False):
    """Safely extract bool from row, returns default if NA."""
    import pandas as pd
    val = row.get(col)
    return bool(val) if pd.notna(val) else default


# Reusable property builders for common event types
def get_earthquake_property_builders():
    """Return property builders dict for earthquake GeoJSON features."""
    return {
        "event_id": lambda r: r.get('event_id', ''),
        "magnitude": lambda r: safe_float(r, 'magnitude'),
        "depth_km": lambda r: safe_float(r, 'depth_km'),
        "felt_radius_km": lambda r: safe_float(r, 'felt_radius_km', 0),
        "damage_radius_km": lambda r: safe_float(r, 'damage_radius_km', 0),
        "place": lambda r: r.get('place', ''),
        "time": lambda r: safe_str(r, 'timestamp', None),
        "timestamp": lambda r: safe_str(r, 'timestamp', None),
        "year": lambda r: safe_int(r, 'year'),
        "loc_id": lambda r: r.get('loc_id', ''),
        "latitude": lambda r: safe_float(r, 'latitude'),
        "longitude": lambda r: safe_float(r, 'longitude'),
        "mainshock_id": lambda r: r.get('mainshock_id') if r.get('mainshock_id') else None,
        "sequence_id": lambda r: r.get('sequence_id') if r.get('sequence_id') else None,
        "is_mainshock": lambda r: safe_bool(r, 'is_mainshock', False),
        "aftershock_count": lambda r: safe_int(r, 'aftershock_count', 0),
    }


def get_eruption_property_builders():
    """Return property builders dict for volcanic eruption GeoJSON features."""
    return {
        "event_id": lambda r: r.get('event_id', ''),
        "eruption_id": lambda r: safe_int(r, 'eruption_id'),
        "volcano_name": lambda r: r.get('volcano_name', ''),
        "VEI": lambda r: safe_int(r, 'vei') or safe_int(r, 'VEI'),
        "felt_radius_km": lambda r: safe_float(r, 'felt_radius_km', 10.0),
        "damage_radius_km": lambda r: safe_float(r, 'damage_radius_km', 3.0),
        "activity_type": lambda r: r.get('activity_type', ''),
        "activity_area": lambda r: safe_str(r, 'activity_area', None),
        "year": lambda r: safe_int(r, 'year'),
        "end_year": lambda r: safe_int(r, 'end_year'),
        "timestamp": lambda r: safe_str(r, 'timestamp', None),
        "end_timestamp": lambda r: safe_str(r, 'end_timestamp', None),
        "duration_days": lambda r: safe_float(r, 'duration_days'),
        "is_ongoing": lambda r: safe_bool(r, 'is_ongoing', False),
        "loc_id": lambda r: r.get('loc_id', ''),
        "latitude": lambda r: safe_float(r, 'latitude'),
        "longitude": lambda r: safe_float(r, 'longitude'),
    }


def get_volcano_catalog_property_builders():
    """Return property builders dict for volcano catalog (not eruption events)."""
    return {
        "volcano_id": lambda r: r.get('volcano_id', ''),
        "volcano_name": lambda r: r.get('volcano_name', ''),
        "VEI": lambda r: safe_int(r, 'last_known_VEI'),
        "eruption_count": lambda r: safe_int(r, 'eruption_count', 0),
        "last_eruption_year": lambda r: safe_int(r, 'last_eruption_year'),
        "loc_id": lambda r: r.get('loc_id', ''),
    }


def get_tsunami_property_builders():
    """Return property builders dict for tsunami source event GeoJSON features."""
    return {
        "event_id": lambda r: r.get('event_id', ''),
        "year": lambda r: safe_int(r, 'year'),
        "timestamp": lambda r: safe_str(r, 'timestamp', None),
        "country": lambda r: r.get('country', ''),
        "location": lambda r: safe_str(r, 'location', None),
        "cause": lambda r: r.get('cause', ''),
        "cause_code": lambda r: safe_int(r, 'cause_code'),
        "eq_magnitude": lambda r: safe_float(r, 'eq_magnitude'),
        "max_water_height_m": lambda r: safe_float(r, 'max_water_height_m'),
        "intensity": lambda r: safe_float(r, 'intensity'),
        "runup_count": lambda r: safe_int(r, 'runup_count', 0),
        "deaths": lambda r: safe_int(r, 'deaths'),
        "damage_millions": lambda r: safe_float(r, 'damage_millions'),
        "loc_id": lambda r: r.get('loc_id', ''),
        "latitude": lambda r: safe_float(r, 'latitude'),
        "longitude": lambda r: safe_float(r, 'longitude'),
        "is_source": lambda r: True,  # Mark as source event
    }


def get_landslide_property_builders():
    """Return property builders dict for landslide GeoJSON features.

    Intensity is based on deaths using log scale for circle sizing:
    - 0 deaths = intensity 1 (base size)
    - 1 death = intensity 1
    - 10 deaths = intensity 2
    - 100 deaths = intensity 3
    - 1000 deaths = intensity 4
    """
    import math
    return {
        "event_id": lambda r: r.get('event_id', ''),
        "year": lambda r: safe_int(r, 'year'),
        "timestamp": lambda r: safe_str(r, 'timestamp', None),
        "event_name": lambda r: safe_str(r, 'event_name', None),
        "deaths": lambda r: safe_int(r, 'deaths', 0),
        "injuries": lambda r: safe_int(r, 'injuries', 0),
        "missing": lambda r: safe_int(r, 'missing', 0),
        "affected": lambda r: safe_int(r, 'affected', 0),
        "houses_destroyed": lambda r: safe_int(r, 'houses_destroyed', 0),
        "damage_usd": lambda r: safe_float(r, 'damage_usd'),
        "source": lambda r: r.get('source', ''),
        "loc_id": lambda r: r.get('loc_id', ''),
        "latitude": lambda r: safe_float(r, 'latitude'),
        "longitude": lambda r: safe_float(r, 'longitude'),
        # Intensity based on deaths for circle sizing (log scale, capped at 5)
        "intensity": lambda r: min(5, 1 + math.log10(max(1, safe_int(r, 'deaths', 0) or 1))),
        # Radius based on deaths (5-30km visual range)
        "felt_radius_km": lambda r: 5 + 5 * min(5, math.log10(max(1, safe_int(r, 'deaths', 0) or 1))),
        "damage_radius_km": lambda r: 2 + 3 * min(5, math.log10(max(1, safe_int(r, 'deaths', 0) or 1))),
    }


# Create FastAPI app
app = FastAPI(
    title="County Map API",
    description="Geographic data exploration API",
    version="2.0.0"
)

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Enable CORS so browser frontend can communicate with backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (JS, CSS, etc.)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# === Startup Event ===

@app.on_event("startup")
async def startup_event():
    """Initialize data catalog, conversions, and order processor on startup."""
    logger.info("Starting county-map API...")
    load_conversions()
    initialize_catalog()

    # Set up order processor with async wrapper for execute_order
    async def async_execute_order(items, hints):
        """Async wrapper for synchronous execute_order."""
        import asyncio
        loop = asyncio.get_event_loop()
        order = {"items": items, "summary": hints.get("summary", "")}
        # Run synchronous execute_order in thread pool
        result = await loop.run_in_executor(None, execute_order, order)
        return result

    order_processor.set_executor(async_execute_order)
    await order_processor.start()

    logger.info("Startup complete - data catalog and order processor initialized")


# === Health Check ===

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway/Docker deployments."""
    return {"status": "healthy", "service": "county-map-api"}


# === Catalog API ===

@app.get("/api/catalog/overlays")
async def get_catalog_overlays():
    """
    Get overlay tree from catalog for frontend layer panel.

    Returns the overlay_tree which maps overlay paths to sources.
    Frontend uses this to dynamically build the overlay toggle UI.
    """
    from mapmover.data_loading import load_catalog

    catalog = load_catalog()
    overlay_tree = catalog.get("overlay_tree", {})

    return msgpack_response({
        "overlay_tree": overlay_tree,
        "overlay_count": catalog.get("overlay_count", 0)
    })


# === Frontend ===

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the frontend HTML file."""
    template_path = BASE_DIR / "templates" / "index.html"
    return template_path.read_text(encoding='utf-8')


# === Geometry Endpoints ===

@app.get("/geometry/countries")
async def get_countries_geometry_endpoint(debug: bool = False):
    """
    Get all country geometries for initial map display.
    Returns a GeoJSON FeatureCollection with polygon countries only.
    """
    try:
        result = get_countries_geometry_handler(debug=debug)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/countries: {e}")
        return msgpack_error(str(e), 500)


@app.get("/geometry/{loc_id}/children")
async def get_location_children_endpoint(loc_id: str):
    """
    Get child geometries for a location (drill-down).
    Examples:
    - /geometry/USA/children -> US states
    - /geometry/USA-CA/children -> California counties
    - /geometry/FRA/children -> French regions
    """
    try:
        result = get_location_children_handler(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/children: {e}")
        return msgpack_error(str(e), 500)


@app.get("/geometry/{loc_id}/places")
async def get_location_places_endpoint(loc_id: str):
    """
    Get places (cities/towns) for a location as a separate overlay layer.
    """
    try:
        result = get_location_places_handler(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/places: {e}")
        return msgpack_error(str(e), 500)


@app.get("/geometry/{loc_id}/info")
async def get_location_info_endpoint(loc_id: str):
    """
    Get information about a specific location.
    Returns name, admin_level, and whether children are available.
    """
    try:
        result = get_location_info(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/info: {e}")
        return msgpack_error(str(e), 500)


@app.get("/geometry/viewport")
async def get_viewport_geometry_endpoint(level: int = 0, bbox: str = None, debug: bool = False):
    """
    Get geometry features within a viewport bounding box.

    Args:
        level: Admin level (0=countries, 1=states, 2=counties, 3=subdivisions)
        bbox: Bounding box as "minLon,minLat,maxLon,maxLat"
        debug: If true, include coverage info for level 0 features

    Returns:
        GeoJSON FeatureCollection with features intersecting the viewport
    """
    try:
        if bbox:
            # Parse bbox string
            parts = [float(x) for x in bbox.split(',')]
            if len(parts) != 4:
                return msgpack_error("bbox must be minLon,minLat,maxLon,maxLat", 400)
            bbox_tuple = tuple(parts)
        else:
            # Default to world view
            bbox_tuple = (-180, -90, 180, 90)

        result = get_viewport_geometry_handler(level, bbox_tuple, debug=debug)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/viewport: {e}")
        return msgpack_error(str(e), 500)


@app.post("/geometry/cache/clear")
async def clear_geometry_cache_endpoint():
    """Clear the geometry cache. Useful after updating data files."""
    try:
        clear_geometry_cache()
        return msgpack_response({"message": "Geometry cache cleared"})
    except Exception as e:
        logger.error(f"Error clearing geometry cache: {e}")
        return msgpack_error(str(e), 500)


@app.post("/geometry/selection")
async def get_selection_geometry_endpoint(req: Request):
    """
    Get geometries for specific loc_ids for disambiguation selection mode.
    Used by SelectionManager to highlight candidate locations.

    Body: { loc_ids: ["CAN-BC", "USA-WA", ...] }
    Returns: GeoJSON FeatureCollection
    """
    try:
        body = await decode_request_body(req)
        loc_ids = body.get("loc_ids", [])

        if not loc_ids:
            return msgpack_response({"type": "FeatureCollection", "features": []})

        result = get_selection_geometries_handler(loc_ids)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/selection: {e}")
        return msgpack_error(str(e), 500)


# === Earthquake Data Endpoints ===

@app.get("/api/earthquakes/geojson")
async def get_earthquakes_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_magnitude: float = None,
    limit: int = None,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get earthquakes as GeoJSON points for map display.
    No default magnitude filter - frontend controls filtering.

    Location filters:
    - loc_prefix: Filter by event epicenter location (e.g., "USA", "USA-CA")
    - affected_loc_id: Filter to events that affected this admin region (uses event_areas table)
    """
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)
        df = ensure_year_column(df)

        # Apply time filters (start/end takes precedence if year not specified)
        if year is not None and 'year' in df.columns:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        if min_magnitude is not None:
            df = df[df['magnitude'] >= min_magnitude]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'earthquakes',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        if limit is not None and limit > 0:
            df = df.nlargest(limit, 'magnitude')

        features = build_geojson_features(df, get_earthquake_property_builders())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features
        })

    except Exception as e:
        logger.error(f"Error fetching earthquakes GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/earthquakes/sequence/{sequence_id}")
async def get_earthquake_sequence(sequence_id: str, min_magnitude: float = None):
    """
    Get all earthquakes in a specific aftershock sequence.
    No magnitude filter by default - returns ALL events in the sequence.
    """
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)
        df = df[df['sequence_id'] == sequence_id]

        if len(df) == 0:
            return msgpack_error(f"Sequence {sequence_id} not found", 404)

        if min_magnitude is not None:
            df = df[df['magnitude'] >= min_magnitude]

        df = ensure_year_column(df)
        features = build_geojson_features(df, get_earthquake_property_builders())

        logger.info(f"Returning {len(features)} events for sequence {sequence_id}")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "sequence_id": sequence_id
        })

    except Exception as e:
        logger.error(f"Error fetching earthquake sequence {sequence_id}: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/earthquakes/aftershocks/{event_id}")
async def get_earthquake_aftershocks(event_id: str, min_magnitude: float = None):
    """
    Get all aftershocks for a specific mainshock by event_id.
    Returns the mainshock plus all events where mainshock_id = event_id.
    """
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)

        mainshock_df = df[df['event_id'] == event_id]
        if len(mainshock_df) == 0:
            return msgpack_error(f"Event {event_id} not found", 404)

        aftershocks_df = df[df['mainshock_id'] == event_id]
        result_df = pd.concat([mainshock_df, aftershocks_df], ignore_index=True)

        if min_magnitude is not None:
            result_df = result_df[result_df['magnitude'] >= min_magnitude]

        result_df = ensure_year_column(result_df)
        features = build_geojson_features(result_df, get_earthquake_property_builders())

        logger.info(f"Returning {len(features)} events for mainshock {event_id}")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "event_id": event_id,
                "event_type": "earthquake",
                "total_count": len(features),
                "aftershock_count": len(features) - 1
            }
        })

    except Exception as e:
        logger.error(f"Error fetching aftershocks for {event_id}: {e}")
        return msgpack_error(str(e), 500)


# === Volcano Data Endpoints ===

@app.get("/api/volcanoes/geojson")
async def get_volcanoes_geojson(active_only: bool = None):
    """Get volcanoes as GeoJSON points for map display."""
    import pandas as pd

    try:
        volcanoes_path = GLOBAL_DIR / "disasters/volcanoes/volcanoes.parquet"
        if not volcanoes_path.exists():
            return msgpack_error("Volcano data not available", 404)

        df = pd.read_parquet(volcanoes_path)
        features = build_geojson_features(df, get_volcano_catalog_property_builders())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features
        })

    except Exception as e:
        logger.error(f"Error fetching volcanoes GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/eruptions/geojson")
async def get_eruptions_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_vei: int = None,
    min_year: int = None,
    exclude_ongoing: bool = False,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get volcanic eruptions as GeoJSON points for map display.
    Radii are pre-calculated in the data pipeline using VEI-based formulas.

    Location filters:
    - loc_prefix: Filter by volcano location (e.g., "IDN" for Indonesia, "USA" for US)
    - affected_loc_id: Filter to eruptions that affected this admin region
    """
    import pandas as pd

    try:
        eruptions_path = GLOBAL_DIR / "disasters/volcanoes/events.parquet"
        if not eruptions_path.exists():
            return msgpack_error("Eruption data not available", 404)

        df = pd.read_parquet(eruptions_path)
        df = ensure_year_column(df)

        # Apply time filters
        if year is not None and 'year' in df.columns:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        elif min_year is not None and 'year' in df.columns:
            df = df[df['year'] >= min_year]
        if min_vei is not None and 'vei' in df.columns:
            df = df[df['vei'] >= min_vei]
        if exclude_ongoing and 'is_ongoing' in df.columns:
            df = df[df['is_ongoing'] != True]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'volcanoes',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        features = build_geojson_features(df, get_eruption_property_builders())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features
        })

    except Exception as e:
        logger.error(f"Error fetching eruptions GeoJSON: {e}")
        return msgpack_error(str(e), 500)


# === Tsunami Data Endpoints ===

@app.get("/api/tsunamis/geojson")
async def get_tsunamis_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    cause: str = None,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get tsunami source events as GeoJSON points for map display.
    Default: tsunamis from tide gauge era (1900) to present.

    Location filters:
    - loc_prefix: Filter by event origin location (e.g., "XOO" for Pacific, "JPN" for Japan)
    - affected_loc_id: Filter to events that affected this admin region (via runup locations)
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('tsunamis', fallback=1900)
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"
        if not events_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        df = pd.read_parquet(events_path)

        # Apply time filters
        if year is not None:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        elif min_year is not None:
            df = df[df['year'] >= min_year]
        if cause is not None:
            df = df[df['cause'].str.lower() == cause.lower()]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'tsunamis',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        features = build_geojson_features(df, get_tsunami_property_builders())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "count": len(features),
                "year_range": [int(df['year'].min()), int(df['year'].max())] if len(df) > 0 else None
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tsunamis GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/tsunamis/{event_id}/runups")
async def get_tsunami_runups(event_id: str):
    """
    Get runup observations for a specific tsunami event.
    Returns points where the tsunami was observed at coastlines.
    """
    import pandas as pd

    try:
        runups_path = GLOBAL_DIR / "disasters/tsunamis/runups.parquet"
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"

        if not runups_path.exists():
            return msgpack_error("Runup data not available", 404)

        # Load runups for this event
        runups_df = pd.read_parquet(runups_path)
        runups_df = runups_df[runups_df['event_id'] == event_id]

        if len(runups_df) == 0:
            return msgpack_error(f"No runups found for event {event_id}", 404)

        # Load source event for reference
        source_event = None
        if events_path.exists():
            events_df = pd.read_parquet(events_path)
            event_row = events_df[events_df['event_id'] == event_id]
            if len(event_row) > 0:
                row = event_row.iloc[0]
                source_event = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(row['longitude']), float(row['latitude'])]
                    },
                    "properties": {
                        "event_id": event_id,
                        "year": int(row['year']) if pd.notna(row.get('year')) else None,
                        "timestamp": str(row['timestamp']) if pd.notna(row.get('timestamp')) else None,
                        "cause": row.get('cause', ''),
                        "eq_magnitude": float(row['eq_magnitude']) if pd.notna(row.get('eq_magnitude')) else None,
                        "max_water_height_m": float(row['max_water_height_m']) if pd.notna(row.get('max_water_height_m')) else None,
                        "_isSource": True,
                        "is_source": True
                    }
                }

        # Build runup features
        features = []
        for _, row in runups_df.iterrows():
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                continue

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row['longitude']), float(row['latitude'])]
                },
                "properties": {
                    "runup_id": row.get('runup_id', ''),
                    "event_id": event_id,
                    "year": int(row['year']) if pd.notna(row.get('year')) else None,
                    "country": row.get('country', ''),
                    "location_name": row.get('location', '') if pd.notna(row.get('location')) else None,
                    "water_height_m": float(row['water_height_m']) if pd.notna(row.get('water_height_m')) else None,
                    "dist_from_source_km": float(row['dist_from_source_km']) if pd.notna(row.get('dist_from_source_km')) else None,
                    "travel_time_hours": float(row['arrival_travel_time_min']) / 60 if pd.notna(row.get('arrival_travel_time_min')) else None,
                    "deaths": int(row['deaths']) if pd.notna(row.get('deaths')) else None,
                    "_isSource": False,
                    "is_source": False
                }
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "source": source_event,
            "metadata": {
                "event_id": event_id,
                "event_type": "tsunami",
                "total_count": len(features),
                "runup_count": len(features)
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tsunami runups: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/tsunamis/{event_id}/animation")
async def get_tsunami_animation_data(event_id: str):
    """
    Get combined source + runups data formatted for radial animation.
    Includes source event marked with is_source=true and runups with distance data.
    """
    import pandas as pd

    try:
        runups_path = GLOBAL_DIR / "disasters/tsunamis/runups.parquet"
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"

        if not events_path.exists() or not runups_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        # Load source event
        events_df = pd.read_parquet(events_path)
        event_row = events_df[events_df['event_id'] == event_id]

        if len(event_row) == 0:
            return msgpack_error(f"Event {event_id} not found", 404)

        row = event_row.iloc[0]

        # Build all features (source + runups)
        features = []

        # Source event
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row['longitude']), float(row['latitude'])]
            },
            "properties": {
                "event_id": event_id,
                "year": int(row['year']) if pd.notna(row.get('year')) else None,
                "timestamp": str(row['timestamp']) if pd.notna(row.get('timestamp')) else None,
                "cause": row.get('cause', ''),
                "eq_magnitude": float(row['eq_magnitude']) if pd.notna(row.get('eq_magnitude')) else None,
                "max_water_height_m": float(row['max_water_height_m']) if pd.notna(row.get('max_water_height_m')) else None,
                "deaths": int(row['deaths']) if pd.notna(row.get('deaths')) else None,
                "is_source": True
            }
        })

        # Load runups
        runups_df = pd.read_parquet(runups_path)
        runups_df = runups_df[runups_df['event_id'] == event_id]

        for _, rrow in runups_df.iterrows():
            if pd.isna(rrow['latitude']) or pd.isna(rrow['longitude']):
                continue

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(rrow['longitude']), float(rrow['latitude'])]
                },
                "properties": {
                    "runup_id": rrow.get('runup_id', ''),
                    "event_id": event_id,
                    "country": rrow.get('country', ''),
                    "location_name": rrow.get('location', '') if pd.notna(rrow.get('location')) else None,
                    "water_height_m": float(rrow['water_height_m']) if pd.notna(rrow.get('water_height_m')) else None,
                    "dist_from_source_km": float(rrow['dist_from_source_km']) if pd.notna(rrow.get('dist_from_source_km')) else None,
                    "arrival_travel_time_min": float(rrow['arrival_travel_time_min']) if pd.notna(rrow.get('arrival_travel_time_min')) else None,
                    "timestamp": str(rrow['timestamp']) if pd.notna(rrow.get('timestamp')) else None,
                    "deaths": int(rrow['deaths']) if pd.notna(rrow.get('deaths')) else None,
                    "is_source": False
                }
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "event_id": event_id,
                "event_type": "tsunami",
                "total_count": len(features),
                "source_timestamp": str(row['timestamp']) if pd.notna(row.get('timestamp')) else None,
                "runup_count": len(features) - 1,
                "animation_mode": "radial"
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tsunami animation data: {e}")
        return msgpack_error(str(e), 500)


# === Landslide Data Endpoints ===

@app.get("/api/landslides/geojson")
async def get_landslides_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_deaths: int = 1,
    require_coords: bool = True
):
    """
    Get landslide events as GeoJSON points for map display.

    Args:
        year: Filter by specific year
        min_deaths: Minimum deaths to include (default 1 to filter minor events)
        require_coords: Only return events with valid coordinates (default True)
    """
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/landslides/events.parquet"
        if not events_path.exists():
            return msgpack_error("Landslide data not available", 404)

        df = pd.read_parquet(events_path)

        # Filter for events with coordinates if required
        if require_coords:
            df = df[df['latitude'].notna() & df['longitude'].notna()]

        # Apply time filter
        if year is not None:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)

        # Apply deaths filter
        if min_deaths > 0:
            df['deaths_val'] = df['deaths'].fillna(0)
            df = df[df['deaths_val'] >= min_deaths]
            df = df.drop(columns=['deaths_val'])

        features = build_geojson_features(df, get_landslide_property_builders())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "count": len(features),
                "year_range": [int(df['year'].min()), int(df['year'].max())] if len(df) > 0 else None
            }
        })

    except Exception as e:
        logger.error(f"Error fetching landslides GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/events/nearby-earthquakes")
async def get_nearby_earthquakes(
    lat: float,
    lon: float,
    timestamp: str = None,
    year: int = None,
    radius_km: float = 150.0,
    days_before: int = 30,
    days_after: int = 60,
    min_magnitude: float = 3.0
):
    """Find earthquakes near a location within a time window."""
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)
        df = filter_by_proximity(df, lat, lon, radius_km)

        if timestamp:
            df = filter_by_time_window(df, timestamp, days_before, days_after)
        elif year:
            df = ensure_year_column(df)
            df = df[df['year'] == year]

        df = df[df['magnitude'] >= min_magnitude]

        if len(df) == 0:
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "count": 0,
                "search_params": {"lat": lat, "lon": lon, "radius_km": radius_km}
            })

        df = ensure_year_column(df)
        features = build_geojson_features(df, get_earthquake_property_builders())

        logger.info(f"Found {len(features)} earthquakes within {radius_km}km of ({lat}, {lon})")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "search_params": {
                "lat": lat, "lon": lon, "radius_km": radius_km,
                "days_after": days_after, "min_magnitude": min_magnitude
            }
        })

    except Exception as e:
        logger.error(f"Error finding nearby earthquakes: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/events/nearby-volcanoes")
async def get_nearby_volcanoes(
    lat: float,
    lon: float,
    timestamp: str = None,
    year: int = None,
    radius_km: float = 150.0,
    days_before: int = 30,
    min_vei: int = None
):
    """Find volcanic eruptions near a location within a time window."""
    import pandas as pd

    try:
        eruptions_path = GLOBAL_DIR / "disasters/volcanoes/events.parquet"
        if not eruptions_path.exists():
            return msgpack_error("Volcano data not available", 404)

        df = pd.read_parquet(eruptions_path)
        df = filter_by_proximity(df, lat, lon, radius_km)

        if timestamp:
            df = filter_by_time_window(df, timestamp, days_before, 0)  # Only look before
        elif year:
            df = ensure_year_column(df)
            df = df[df['year'] == year]

        if min_vei is not None:
            vei_col = 'vei' if 'vei' in df.columns else 'VEI'
            if vei_col in df.columns:
                df = df[df[vei_col] >= min_vei]

        if len(df) == 0:
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "count": 0,
                "search_params": {"lat": lat, "lon": lon, "radius_km": radius_km}
            })

        features = build_geojson_features(df, get_eruption_property_builders())

        logger.info(f"Found {len(features)} eruptions within {radius_km}km of ({lat}, {lon})")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "search_params": {
                "lat": lat, "lon": lon, "radius_km": radius_km,
                "days_before": days_before, "min_vei": min_vei
            }
        })

    except Exception as e:
        logger.error(f"Error finding nearby volcanoes: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/events/nearby-tsunamis")
async def get_nearby_tsunamis(
    lat: float,
    lon: float,
    timestamp: str = None,
    year: int = None,
    radius_km: float = 300.0,
    days_before: int = 1,
    days_after: int = 30
):
    """Find tsunamis near a location within a time window."""
    import pandas as pd

    try:
        tsunamis_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"
        if not tsunamis_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        df = pd.read_parquet(tsunamis_path)
        df = filter_by_proximity(df, lat, lon, radius_km)

        if timestamp:
            df = filter_by_time_window(df, timestamp, days_before, days_after)
        elif year:
            df = ensure_year_column(df)
            df = df[df['year'] == year]

        if len(df) == 0:
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "count": 0,
                "search_params": {"lat": lat, "lon": lon, "radius_km": radius_km}
            })

        features = build_geojson_features(df, get_tsunami_property_builders())

        logger.info(f"Found {len(features)} tsunamis within {radius_km}km of ({lat}, {lon})")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "search_params": {
                "lat": lat, "lon": lon, "radius_km": radius_km,
                "days_before": days_before, "days_after": days_after
            }
        })

    except Exception as e:
        logger.error(f"Error finding nearby tsunamis: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/events/related/{loc_id:path}")
async def get_related_events(loc_id: str):
    """
    Get related disaster events for a given event loc_id.

    Uses links.parquet to find:
    - Events this event triggered (children)
    - Events that triggered this event (parents)

    Returns event details with link type information.
    """
    import pandas as pd

    try:
        links_path = GLOBAL_DIR / "disasters/links.parquet"
        if not links_path.exists():
            return msgpack_response({
                "event_id": loc_id,
                "related": [],
                "message": "Links data not available"
            })

        links_df = pd.read_parquet(links_path)

        # Find events where this event is the parent (triggered)
        children = links_df[links_df['parent_loc_id'] == loc_id].copy()
        children['direction'] = 'triggered'
        children['related_loc_id'] = children['child_loc_id']

        # Find events where this event is the child (triggered by)
        parents = links_df[links_df['child_loc_id'] == loc_id].copy()
        parents['direction'] = 'triggered_by'
        parents['related_loc_id'] = parents['parent_loc_id']

        # Combine and deduplicate
        related = pd.concat([children, parents], ignore_index=True)

        if len(related) == 0:
            return msgpack_response({
                "event_id": loc_id,
                "related": [],
                "count": 0
            })

        # Extract event type from loc_id (format: {region}-{TYPE}-{id})
        def extract_event_type(lid):
            parts = lid.split('-')
            if len(parts) >= 2:
                type_code = parts[-2] if len(parts) >= 3 else parts[0]
                type_map = {
                    'EQ': 'earthquake',
                    'TSUN': 'tsunami',
                    'VOLC': 'volcano',
                    'HRCN': 'hurricane',
                    'TORN': 'tornado',
                    'FIRE': 'wildfire',
                    'FLOOD': 'flood',
                    'LAND': 'landslide'
                }
                return type_map.get(type_code, 'unknown')
            return 'unknown'

        def extract_event_id(lid):
            # Last part after final hyphen (handles IDs with hyphens)
            parts = lid.split('-')
            if len(parts) >= 3:
                # Format: REGION-TYPE-ID (ID may contain hyphens)
                # Find the type code position
                for i, part in enumerate(parts):
                    if part in ['EQ', 'TSUN', 'VOLC', 'HRCN', 'TORN', 'FIRE', 'FLOOD', 'LAND']:
                        return '-'.join(parts[i+1:])
            return parts[-1] if parts else lid

        # Build related events list
        related_list = []
        for _, row in related.iterrows():
            rel_loc_id = row['related_loc_id']
            related_list.append({
                'loc_id': rel_loc_id,
                'event_id': extract_event_id(rel_loc_id),
                'event_type': extract_event_type(rel_loc_id),
                'link_type': row['link_type'],
                'direction': row['direction'],
                'source': row['source'],
                'confidence': row['confidence']
            })

        # Group by type for summary
        type_counts = {}
        for item in related_list:
            t = item['event_type']
            type_counts[t] = type_counts.get(t, 0) + 1

        return msgpack_response({
            "event_id": loc_id,
            "related": related_list,
            "count": len(related_list),
            "by_type": type_counts
        })

    except Exception as e:
        logger.error(f"Error fetching related events for {loc_id}: {e}")
        return msgpack_error(str(e), 500)


# === Wildfire Data Endpoints ===

@app.get("/api/wildfires/geojson")
async def get_wildfires_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    max_year: int = None,
    min_area_km2: float = None,
    include_perimeter: bool = False,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get wildfires as GeoJSON for map display.
    Default: fires from 2010 to present.

    Data sources (automatically selected based on loc_prefix):
    - USA fires: countries/USA/wildfires/fires_enriched.parquet (30K fires)
    - CAN fires: countries/CAN/cnfdb/fires_enriched.parquet (442K fires)
    - Global fires: global/disasters/wildfires/by_year_enriched/ (20M+ fires, excludes USA/CAN)

    No default area filter - frontend controls filtering.
    Set include_perimeter=true to get polygon geometries.

    Location filters:
    - loc_prefix: Filter by fire location (e.g., "USA", "CAN", "AUS" for country, "USA-CA" for state)
    - affected_loc_id: Filter to fires that affected this admin region

    Memory-efficient: Uses yearly parquet files with pyarrow predicate pushdown.
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('wildfires', fallback=2010)

    import pyarrow.parquet as pq
    import pyarrow as pa
    import pandas as pd
    import json as json_lib

    try:
        # Determine data source based on loc_prefix
        # USA and CAN have dedicated higher-quality data files
        usa_fires_path = COUNTRIES_DIR / "USA/wildfires/fires_enriched.parquet"
        can_fires_path = COUNTRIES_DIR / "CAN/cnfdb/fires_enriched.parquet"
        global_by_year_path = GLOBAL_DIR / "disasters/wildfires/by_year_enriched"

        # Fallback to raw global files if enriched not available
        if not global_by_year_path.exists():
            global_by_year_path = GLOBAL_DIR / "disasters/wildfires/by_year"

        # Columns to read (exclude perimeter for fast initial load)
        # Include loc_id columns for location filtering
        base_columns = ['event_id', 'timestamp', 'latitude', 'longitude', 'area_km2',
                        'burned_acres', 'duration_days', 'source', 'has_progression',
                        'loc_id', 'parent_loc_id', 'sibling_level', 'iso3', 'loc_confidence']

        # Determine year range for global data
        # start/end timestamps derive year range for file loading
        start_ts_parsed = None
        end_ts_parsed = None
        if start is not None or end is not None:
            import pandas as _pd
            if start:
                start_ts_parsed = _pd.to_datetime(int(start), unit='ms') if str(start).isdigit() else _pd.to_datetime(start)
            if end:
                end_ts_parsed = _pd.to_datetime(int(end), unit='ms') if str(end).isdigit() else _pd.to_datetime(end)

        if year is not None:
            years_to_load = [year]
        elif start_ts_parsed is not None or end_ts_parsed is not None:
            s_year = start_ts_parsed.year if start_ts_parsed else min_year
            e_year = end_ts_parsed.year if end_ts_parsed else (max_year or 2026)
            years_to_load = list(range(s_year, e_year + 1))
        else:
            end_year = max_year if max_year else 2024
            years_to_load = list(range(min_year, end_year + 1))

        all_dfs = []
        source_used = []

        # Load USA data if prefix matches or no prefix (load all)
        if loc_prefix is None or loc_prefix.startswith("USA"):
            if usa_fires_path.exists():
                # USA file columns differ slightly
                usa_columns = [c for c in base_columns if c not in ['land_cover']]
                if include_perimeter:
                    usa_columns.append('perimeter')

                # Read available columns only
                usa_df = pd.read_parquet(usa_fires_path)

                # Filter by year if specified
                usa_df['timestamp'] = pd.to_datetime(usa_df['timestamp'], errors='coerce')
                usa_df['year'] = usa_df['timestamp'].dt.year
                if year is not None:
                    usa_df = usa_df[usa_df['year'] == year]
                elif years_to_load:
                    usa_df = usa_df[usa_df['year'].isin(years_to_load)]

                # Filter by area if specified
                if min_area_km2 is not None and 'area_km2' in usa_df.columns:
                    usa_df = usa_df[usa_df['area_km2'] >= min_area_km2]
                elif min_area_km2 is not None and 'burned_acres' in usa_df.columns:
                    # Convert acres to km2 for filtering (1 acre = 0.00404686 km2)
                    usa_df = usa_df[usa_df['burned_acres'] * 0.00404686 >= min_area_km2]

                # Add missing columns with defaults
                if 'land_cover' not in usa_df.columns:
                    usa_df['land_cover'] = ''
                if 'area_km2' not in usa_df.columns and 'burned_acres' in usa_df.columns:
                    usa_df['area_km2'] = usa_df['burned_acres'] * 0.00404686
                if 'duration_days' not in usa_df.columns:
                    usa_df['duration_days'] = None
                if 'source' not in usa_df.columns:
                    usa_df['source'] = 'NIFC'
                if 'has_progression' not in usa_df.columns:
                    usa_df['has_progression'] = False

                if len(usa_df) > 0:
                    all_dfs.append(usa_df)
                    source_used.append('USA')

        # Load CAN data if prefix matches or no prefix
        if loc_prefix is None or loc_prefix.startswith("CAN"):
            if can_fires_path.exists():
                can_df = pd.read_parquet(can_fires_path)

                # Filter by year if specified
                can_df['timestamp'] = pd.to_datetime(can_df['timestamp'], errors='coerce')
                can_df['year'] = can_df['timestamp'].dt.year
                if year is not None:
                    can_df = can_df[can_df['year'] == year]
                elif years_to_load:
                    can_df = can_df[can_df['year'].isin(years_to_load)]

                # Filter by area if specified
                if min_area_km2 is not None and 'area_km2' in can_df.columns:
                    can_df = can_df[can_df['area_km2'] >= min_area_km2]

                # Add missing columns with defaults
                if 'land_cover' not in can_df.columns:
                    can_df['land_cover'] = ''
                if 'source' not in can_df.columns:
                    can_df['source'] = 'CNFDB'

                if len(can_df) > 0:
                    all_dfs.append(can_df)
                    source_used.append('CAN')

        # Load global data if prefix is NOT USA or CAN (or no prefix)
        # Global data excludes USA and CAN, so only load if needed
        if loc_prefix is None or (not loc_prefix.startswith("USA") and not loc_prefix.startswith("CAN")):
            if global_by_year_path.exists():
                columns = base_columns + ['land_cover'] if 'land_cover' not in base_columns else base_columns
                if include_perimeter:
                    columns.append('perimeter')

                # Load from yearly partition files with pyarrow filters
                all_tables = []
                for yr in years_to_load:
                    # Use enriched files first, fallback to raw
                    year_file = global_by_year_path / f"fires_{yr}_enriched.parquet"
                    if not year_file.exists():
                        year_file = global_by_year_path / f"fires_{yr}.parquet"
                    if not year_file.exists():
                        continue

                    # Pyarrow predicate pushdown - only reads matching row groups
                    filters = [('area_km2', '>=', min_area_km2)] if min_area_km2 is not None else None
                    try:
                        table = pq.read_table(
                            year_file,
                            columns=[c for c in columns if c != 'land_cover'],  # land_cover may not exist
                            filters=filters
                        )
                        if table.num_rows > 0:
                            all_tables.append(table)
                    except Exception:
                        # Try without column filtering if columns don't match
                        table = pq.read_table(year_file, filters=filters)
                        if table.num_rows > 0:
                            all_tables.append(table)

                if all_tables:
                    combined = pa.concat_tables(all_tables)
                    global_df = combined.to_pandas()
                    global_df['timestamp'] = pd.to_datetime(global_df['timestamp'], errors='coerce')
                    global_df['year'] = global_df['timestamp'].dt.year

                    # Add missing columns
                    if 'land_cover' not in global_df.columns:
                        global_df['land_cover'] = ''
                    if 'source' not in global_df.columns:
                        global_df['source'] = 'global_fire_atlas'

                    # Filter out USA and CAN fires from global data
                    # These countries have dedicated higher-quality data files
                    if 'iso3' in global_df.columns:
                        before_filter = len(global_df)
                        global_df = global_df[~global_df['iso3'].isin(['USA', 'CAN'])]
                        filtered_out = before_filter - len(global_df)
                        if filtered_out > 0:
                            logger.debug(f"Filtered {filtered_out:,} USA/CAN fires from global data")

                    all_dfs.append(global_df)
                    source_used.append('global')

        if not all_dfs:
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "metadata": {"count": 0, "min_area_km2": min_area_km2, "min_year": min_year, "sources": []}
            })

        # Combine all dataframes
        df = pd.concat(all_dfs, ignore_index=True)

        # Extract year from timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['year'] = df['timestamp'].dt.year

        # Apply precise timestamp filtering if start/end provided
        if start_ts_parsed is not None or end_ts_parsed is not None:
            if start_ts_parsed is not None:
                if start_ts_parsed.tzinfo:
                    start_ts_parsed = start_ts_parsed.tz_convert('UTC').tz_localize(None)
                df = df[df['timestamp'] >= start_ts_parsed]
            if end_ts_parsed is not None:
                if end_ts_parsed.tzinfo:
                    end_ts_parsed = end_ts_parsed.tz_convert('UTC').tz_localize(None)
                df = df[df['timestamp'] <= end_ts_parsed]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'wildfires',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        # Build GeoJSON features using to_dict('records') for 10-100x speedup
        # Filter valid coordinates first using vectorized operation
        valid_mask = df['latitude'].notna() & df['longitude'].notna()
        valid_df = df[valid_mask]
        records = valid_df.to_dict('records')

        features = []
        for row in records:
            # Use perimeter polygon if requested and available
            if include_perimeter and row.get('perimeter') and pd.notna(row.get('perimeter')):
                try:
                    geom = json_lib.loads(row['perimeter']) if isinstance(row['perimeter'], str) else row['perimeter']
                except:
                    geom = {"type": "Point", "coordinates": [float(row['longitude']), float(row['latitude'])]}
            else:
                geom = {"type": "Point", "coordinates": [float(row['longitude']), float(row['latitude'])]}

            # Handle timestamp - may be Timestamp object or string after to_dict
            ts = row.get('timestamp')
            if ts is not None and pd.notna(ts):
                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
            else:
                ts_str = None

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "event_id": row.get('event_id', ''),
                    "area_km2": float(row['area_km2']) if pd.notna(row.get('area_km2')) else None,
                    "burned_acres": float(row['burned_acres']) if pd.notna(row.get('burned_acres')) else None,
                    "duration_days": int(row['duration_days']) if pd.notna(row.get('duration_days')) else None,
                    "year": int(row['year']) if pd.notna(row.get('year')) else None,
                    "timestamp": ts_str,
                    "land_cover": row.get('land_cover', ''),
                    "source": row.get('source', 'global_fire_atlas'),
                    "latitude": float(row['latitude']),
                    "longitude": float(row['longitude']),
                    "has_progression": bool(row.get('has_progression', False)),
                    # Location assignment columns
                    "loc_id": row.get('loc_id', ''),
                    "parent_loc_id": row.get('parent_loc_id', ''),
                    "sibling_level": int(row['sibling_level']) if pd.notna(row.get('sibling_level')) else None,
                    "iso3": row.get('iso3', ''),
                    "loc_confidence": float(row['loc_confidence']) if pd.notna(row.get('loc_confidence')) else None
                }
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "count": len(features),
                "min_area_km2": min_area_km2,
                "min_year": min_year,
                "max_year": max_year or 2024,
                "include_perimeter": include_perimeter,
                "sources": source_used
            }
        })

    except Exception as e:
        logger.error(f"Error fetching wildfires GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/wildfires/{event_id}/perimeter")
async def get_wildfire_perimeter(event_id: str, year: int = None):
    """
    Get perimeter polygon for a single wildfire.
    Used for on-demand loading when user clicks a fire.

    If year is provided, reads from yearly partition (~90MB) instead of main file (2GB).
    Much more memory efficient when year is known (frontend has it from the point data).
    """
    import pyarrow.parquet as pq
    import json as json_lib

    try:
        by_year_path = GLOBAL_DIR / "disasters/wildfires/by_year"
        main_path = GLOBAL_DIR / "disasters/wildfires/fires.parquet"

        # Try yearly partition first if year provided (much more efficient)
        if year is not None and by_year_path.exists():
            year_file = by_year_path / f"fires_{year}.parquet"
            if year_file.exists():
                table = pq.read_table(
                    year_file,
                    columns=['event_id', 'perimeter'],
                    filters=[('event_id', '=', event_id)]
                )
                if table.num_rows > 0:
                    perimeter_str = table.column('perimeter')[0].as_py()
                    if perimeter_str:
                        perimeter = json_lib.loads(perimeter_str) if isinstance(perimeter_str, str) else perimeter_str
                        return msgpack_response({
                            "type": "Feature",
                            "geometry": perimeter,
                            "properties": {"event_id": event_id, "year": year}
                        })

        # Fallback: search main file (slower but works without year)
        if main_path.exists():
            table = pq.read_table(
                main_path,
                columns=['event_id', 'perimeter'],
                filters=[('event_id', '=', event_id)]
            )

            if table.num_rows == 0:
                return msgpack_error(f"Fire {event_id} not found", 404)

            perimeter_str = table.column('perimeter')[0].as_py()

            if perimeter_str is None:
                return msgpack_error("No perimeter data for this fire", 404)

            perimeter = json_lib.loads(perimeter_str) if isinstance(perimeter_str, str) else perimeter_str

            return msgpack_response({
                "type": "Feature",
                "geometry": perimeter,
                "properties": {"event_id": event_id}
            })

        return msgpack_error("Wildfire data not available", 404)

    except Exception as e:
        logger.error(f"Error fetching wildfire perimeter: {e}")
        return msgpack_error(str(e), 500)




@app.get("/api/wildfires/{event_id}/progression")
async def get_wildfire_progression(event_id: str, year: int = None):
    """
    Get daily fire progression snapshots for animation.
    Returns an array of daily perimeters showing fire spread over time.
    """
    import pyarrow.parquet as pq
    import json as json_lib

    try:
        progression_path = GLOBAL_DIR / "wildfires"

        # Try year-specific file first
        if year:
            prog_file = progression_path / f"fire_progression_{year}.parquet"
        else:
            prog_file = progression_path / "fire_progression_2024.parquet"

        if not prog_file.exists():
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "event_id": event_id,
                    "event_type": "wildfire",
                    "total_count": 0,
                    "error": "No progression data available"
                }
            })

        # Read progression data for this fire
        table = pq.read_table(
            prog_file,
            filters=[('event_id', '=', str(event_id))]
        )

        if table.num_rows == 0:
            return msgpack_response({
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "event_id": event_id,
                    "event_type": "wildfire",
                    "total_count": 0,
                    "error": "Fire not found in progression data"
                }
            })

        # Convert to GeoJSON FeatureCollection
        df = table.to_pandas()
        df = df.sort_values('day_num')

        features = []
        for _, row in df.iterrows():
            perimeter = json_lib.loads(row['perimeter']) if isinstance(row['perimeter'], str) else row['perimeter']
            date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
            features.append({
                "type": "Feature",
                "geometry": perimeter,
                "properties": {
                    "date": date_str,
                    "day_num": int(row['day_num']),
                    "area_km2": float(row['area_km2'])
                }
            })

        # Get time range from dates
        time_start = df['date'].min()
        time_end = df['date'].max()

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "event_id": event_id,
                "event_type": "wildfire",
                "total_count": len(features),
                "time_range": {
                    "start": time_start.strftime('%Y-%m-%d') if hasattr(time_start, 'strftime') else str(time_start),
                    "end": time_end.strftime('%Y-%m-%d') if hasattr(time_end, 'strftime') else str(time_end)
                }
            }
        })

    except Exception as e:
        logger.error(f"Error fetching wildfire progression: {e}")
        return msgpack_error(str(e), 500)


# === Flood Data Endpoints ===

@app.get("/api/floods/geojson")
async def get_floods_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    max_year: int = None,
    include_geometry: bool = False,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get global floods as GeoJSON for map display.
    Data sources: Global Flood Database (2000-2018) + Dartmouth Flood Observatory (1985-2019).

    Default: Returns flood events as points (centroid of flood extent).
    Set include_geometry=true to load full flood extent polygons from GeoJSON files.

    Query params:
    - year: Filter to single year
    - min_year: Start year (default from metadata: 1985)
    - max_year: End year (default current)
    - include_geometry: Load flood extent polygons (slower, more data)

    Location filters:
    - loc_prefix: Filter by flood location (e.g., "USA", "BGD" for country)
    - affected_loc_id: Filter to floods that affected this admin region
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('floods', fallback=1985)

    import pandas as pd
    import json as json_lib

    try:
        # Use enriched file with loc_id columns
        events_path = GLOBAL_DIR / "disasters/floods/events_enriched.parquet"
        # Fallback to raw file if enriched not available
        if not events_path.exists():
            events_path = GLOBAL_DIR / "disasters/floods/events.parquet"

        if not events_path.exists():
            return msgpack_error("Flood data not available", 404)

        df = pd.read_parquet(events_path)

        # Apply time filters
        if year is not None:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        else:
            if min_year:
                df = df[df['year'] >= min_year]
            if max_year:
                df = df[df['year'] <= max_year]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'floods',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        # Build GeoJSON features using to_dict('records') for faster iteration
        # Filter valid coordinates first
        valid_mask = df['latitude'].notna() & df['longitude'].notna()
        valid_df = df[valid_mask]
        records = valid_df.to_dict('records')

        features = []
        for row in records:
            event_id = row.get('event_id', '')

            # Load geometry from perimeter column if requested
            geom = None
            if include_geometry:
                perimeter = row.get('perimeter')
                if pd.notna(perimeter) and perimeter:
                    try:
                        geom = json_lib.loads(perimeter) if isinstance(perimeter, str) else perimeter
                    except Exception as e:
                        logger.warning(f"Failed to parse flood perimeter for {event_id}: {e}")

            # Fall back to point if no geometry loaded
            if not geom:
                geom = {"type": "Point", "coordinates": [float(row['longitude']), float(row['latitude'])]}

            # Handle timestamps - may be Timestamp or datetime after to_dict
            ts = row.get('timestamp')
            ts_str = ts.isoformat() if ts is not None and pd.notna(ts) and hasattr(ts, 'isoformat') else (str(ts) if pd.notna(ts) else None)
            end_ts = row.get('end_timestamp')
            end_ts_str = end_ts.isoformat() if end_ts is not None and pd.notna(end_ts) and hasattr(end_ts, 'isoformat') else (str(end_ts) if pd.notna(end_ts) else None)

            # Build properties
            props = {
                "event_id": event_id,
                "year": int(row['year']) if pd.notna(row.get('year')) else None,
                "timestamp": ts_str,
                "end_timestamp": end_ts_str,
                "duration_days": int(row['duration_days']) if pd.notna(row.get('duration_days')) else None,
                "country": str(row.get('country', '')) if pd.notna(row.get('country')) else None,
                "area_km2": float(row['area_km2']) if pd.notna(row.get('area_km2')) else None,
                "severity": int(row['severity']) if pd.notna(row.get('severity')) else None,
                "deaths": int(row['deaths']) if pd.notna(row.get('deaths')) else None,
                "displaced": int(row['displaced']) if pd.notna(row.get('displaced')) else None,
                "source": str(row.get('source', '')) if pd.notna(row.get('source')) else None,
                "has_geometry": bool(row.get('has_geometry', False)),
                "latitude": float(row['latitude']),
                "longitude": float(row['longitude']),
                # Location assignment columns
                "loc_id": str(row.get('loc_id', '')) if pd.notna(row.get('loc_id')) else None,
                "parent_loc_id": str(row.get('parent_loc_id', '')) if pd.notna(row.get('parent_loc_id')) else None,
                "sibling_level": int(row['sibling_level']) if pd.notna(row.get('sibling_level')) else None,
                "iso3": str(row.get('iso3', '')) if pd.notna(row.get('iso3')) else None,
                "loc_confidence": float(row['loc_confidence']) if pd.notna(row.get('loc_confidence')) else None
            }

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "count": len(features),
                "min_year": min_year,
                "max_year": max_year or 2019,
                "include_geometry": include_geometry
            }
        })

    except Exception as e:
        logger.error(f"Error fetching floods: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/drought/geojson")
async def get_drought_geojson(
    country: str = 'CAN',
    year: int = None,
    start: str = None,
    end: str = None,
    month: int = None,
    severity: str = None,
    min_year: int = None,
    max_year: int = None
):
    """
    Get drought monitoring data as GeoJSON for choropleth animation.
    Data sources: Agriculture Canada Drought Monitor (2019-present).

    Returns monthly drought area polygons colored by severity (D0-D4).

    Query params:
    - country: Country code (default 'CAN')
    - year: Filter to single year
    - month: Filter to specific month (1-12)
    - severity: Filter to severity level (D0, D1, D2, D3, D4)
    - min_year: Start year (default 2019)
    - max_year: End year (default current)
    """
    import pandas as pd
    import json as json_lib

    try:
        # Route to correct country data file
        if country == 'CAN':
            data_path = COUNTRIES_DIR / "CAN" / "drought/snapshots.parquet"
        else:
            return msgpack_error(f"Drought data not available for country: {country}", 404)

        if not data_path.exists():
            return msgpack_error("Drought data not available", 404)

        df = pd.read_parquet(data_path)

        # Apply time filters
        if year is not None:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        else:
            if min_year:
                df = df[df['year'] >= min_year]
            if max_year:
                df = df[df['year'] <= max_year]

        if month is not None:
            df = df[df['month'] == month]

        if severity:
            df = df[df['severity'] == severity.upper()]

        # Sort by severity_code so D0 renders first, D4 renders last (on top)
        df = df.sort_values('severity_code')

        # Helper to convert pandas/numpy types to Python native types for msgpack
        def to_python(val):
            if pd.isna(val):
                return None
            if hasattr(val, 'item'):  # numpy scalar
                return val.item()
            return val

        # Build GeoJSON features using to_dict('records') for faster iteration
        records = df.to_dict('records')

        features = []
        for row in records:
            # Parse GeoJSON geometry (stored as JSON string in parquet)
            geom = None
            geom_val = row.get('geometry')
            if pd.notna(geom_val):
                try:
                    geom = json_lib.loads(geom_val)
                except Exception as e:
                    logger.warning(f"Failed to parse drought geometry for {row.get('snapshot_id')}: {e}")
                    continue

            if not geom:
                continue

            # Handle timestamps
            ts = row.get('timestamp')
            ts_str = ts.isoformat() if ts is not None and pd.notna(ts) and hasattr(ts, 'isoformat') else None
            end_ts = row.get('end_timestamp')
            end_ts_str = end_ts.isoformat() if end_ts is not None and pd.notna(end_ts) and hasattr(end_ts, 'isoformat') else None

            # Build properties - convert all numpy types to native Python
            props = {
                "snapshot_id": str(row.get('snapshot_id', '')),
                "timestamp": ts_str,
                "end_timestamp": end_ts_str,
                "duration_days": to_python(row.get('duration_days')),
                "year": to_python(row.get('year')),
                "month": to_python(row.get('month')),
                "severity": str(row.get('severity', '')),
                "severity_code": to_python(row.get('severity_code')),
                "severity_name": str(row.get('severity_name', '')),
                "area_km2": to_python(row.get('area_km2')),
                "iso3": str(row.get('iso3', '')),
                "provinces_affected": str(row.get('provinces_affected', '')) if pd.notna(row.get('provinces_affected')) else None
            }

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props
            })

        # Calculate max_year safely (convert numpy type to Python int)
        max_year_value = None
        if max_year:
            max_year_value = max_year
        elif len(df) > 0:
            max_year_value = int(df['year'].max())

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "count": len(features),
                "country": country,
                "min_year": min_year or 2019,
                "max_year": max_year_value
            }
        })

    except Exception as e:
        logger.error(f"Error fetching drought data: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/floods/{event_id}/geometry")
async def get_flood_geometry(event_id: str):
    """
    Get the flood extent polygon for a specific flood event.
    Returns the GeoJSON geometry for displaying the flooded area.
    """
    import json as json_lib

    try:
        geometry_dir = GLOBAL_DIR / "disasters/floods/geometries"
        geom_file = geometry_dir / f"flood_{event_id}.geojson"

        if not geom_file.exists():
            return msgpack_error(f"Geometry not found for {event_id}", 404)

        with open(geom_file, 'r') as f:
            geom_data = json_lib.load(f)

        return msgpack_response(geom_data)

    except Exception as e:
        logger.error(f"Error fetching flood geometry: {e}")
        return msgpack_error(str(e), 500)


# === Tornado Data Endpoints ===

@app.get("/api/tornadoes/geojson")
async def get_tornadoes_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    min_scale: str = None,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get tornadoes as GeoJSON points for map display.
    Default: tornadoes from Doppler radar era (1990) to present.
    Filter by EF/F scale (e.g., 'EF3' or 'F3').

    Only returns "starter" tornadoes for initial display:
    - Standalone tornadoes (no sequence)
    - First tornado in each sequence (sequence_position == 1)

    Linked tornadoes are fetched on-demand via /api/tornadoes/{id}/sequence
    when user clicks "View tornado sequence".

    Location filters:
    - loc_prefix: Filter by event location (e.g., "USA-TX" for Texas tornadoes)
    - affected_loc_id: Filter to events that affected this admin region

    Data sources: USA (NOAA 1950+), Canada (CNTD 1980-2009, NTP 2017+)
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('tornadoes', fallback=1990)
    import pandas as pd

    try:
        # Global tornadoes dataset (USA + Canada)
        events_path = GLOBAL_DIR / "disasters/tornadoes/events.parquet"

        if not events_path.exists():
            return msgpack_error("Tornado data not available", 404)

        df = pd.read_parquet(events_path)

        # Already filtered to tornadoes only in global dataset
        # Year column now pre-computed in parquet (no datetime parsing needed)

        # Apply time filters
        if year is not None and 'year' in df.columns:
            df = df[df['year'] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        elif min_year is not None and 'year' in df.columns:
            df = df[df['year'] >= min_year]

        if min_scale is not None and 'tornado_scale' in df.columns:
            # Parse scale to numeric for comparison
            def parse_scale(s):
                if pd.isna(s):
                    return -1
                s = str(s).upper().replace('EF', '').replace('F', '')
                try:
                    return int(s)
                except:
                    return -1
            df['_scale_num'] = df['tornado_scale'].apply(parse_scale)
            min_num = parse_scale(min_scale)
            df = df[df['_scale_num'] >= min_num]

        # Location filters (shared helper)
        df = apply_location_filters(
            df, 'tornadoes',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id
        )

        # Filter to starter events only:
        # - Standalone tornadoes (sequence_id is null/NA)
        # - First tornado in each sequence (sequence_position == 1)
        if 'sequence_id' in df.columns and 'sequence_position' in df.columns:
            is_standalone = df['sequence_id'].isna()
            is_sequence_start = df['sequence_position'] == 1
            df = df[is_standalone | is_sequence_start]

        # Build GeoJSON features using to_dict('records') for 10-100x speedup
        valid_mask = df['latitude'].notna() & df['longitude'].notna()
        valid_df = df[valid_mask]
        records = valid_df.to_dict('records')

        features = []
        for row in records:
            time_val = row.get('timestamp') or row.get('time')

            # Include sequence info so frontend knows if "View sequence" is available
            sequence_count = int(row['sequence_count']) if pd.notna(row.get('sequence_count')) else None
            has_sequence = sequence_count is not None and sequence_count > 1

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row['longitude']), float(row['latitude'])]
                },
                "properties": {
                    "event_id": str(row.get('event_id', '')),
                    "tornado_scale": row.get('tornado_scale', ''),
                    "tornado_length_mi": float(row['tornado_length_mi']) if pd.notna(row.get('tornado_length_mi')) else 0,
                    "tornado_width_yd": int(row['tornado_width_yd']) if pd.notna(row.get('tornado_width_yd')) else 0,
                    "felt_radius_km": float(row['felt_radius_km']) if pd.notna(row.get('felt_radius_km')) else 5,
                    "damage_radius_km": float(row['damage_radius_km']) if pd.notna(row.get('damage_radius_km')) else 0.05,
                    "timestamp": str(time_val) if pd.notna(time_val) else None,
                    "year": int(row['year']) if 'year' in row and pd.notna(row.get('year')) else None,
                    "deaths_direct": int(row['deaths_direct']) if pd.notna(row.get('deaths_direct')) else 0,
                    "injuries_direct": int(row['injuries_direct']) if pd.notna(row.get('injuries_direct')) else 0,
                    "damage_property": int(row['damage_property']) if pd.notna(row.get('damage_property')) else 0,
                    "location": row.get('location', ''),
                    "loc_id": row.get('loc_id', ''),
                    "latitude": float(row['latitude']),
                    "longitude": float(row['longitude']),
                    # Track end point for drill-down
                    "end_latitude": float(row['end_latitude']) if pd.notna(row.get('end_latitude')) else None,
                    "end_longitude": float(row['end_longitude']) if pd.notna(row.get('end_longitude')) else None,
                    # Sequence info for "View sequence" button
                    "sequence_count": sequence_count,
                    "has_sequence": has_sequence,
                    # Event type for model routing
                    "event_type": "tornado"
                }
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features
        })

    except Exception as e:
        logger.error(f"Error fetching tornadoes GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/tornadoes/{event_id}")
async def get_tornado_detail(event_id: str):
    """
    Get detailed info for a single tornado including track endpoints.
    Returns start point, end point, track line, and impact radius for drill-down view.
    """
    import pandas as pd

    try:
        # Global tornadoes dataset (USA + Canada)
        events_path = GLOBAL_DIR / "disasters/tornadoes/events.parquet"

        if not events_path.exists():
            return msgpack_error("Tornado data not available", 404)

        df = pd.read_parquet(events_path)

        # Find the specific tornado (event_id is always string in parquet)
        tornado = df[df['event_id'].astype(str) == str(event_id)]

        if len(tornado) == 0:
            return msgpack_error("Tornado not found", 404)

        row = tornado.iloc[0]

        # Build response with track data
        time_col = 'timestamp' if 'timestamp' in row.index else 'time'
        time_val = row.get(time_col)
        timestamp_str = str(time_val) if pd.notna(time_val) else None

        # Calculate impact width in km (yards to km)
        width_km = (row.get('tornado_width_yd', 0) or 0) * 0.0009144

        # Common properties for all features
        props = {
            "event_id": str(row['event_id']),
            "tornado_scale": row.get('tornado_scale', ''),
            "tornado_length_mi": float(row['tornado_length_mi']) if pd.notna(row.get('tornado_length_mi')) else 0,
            "tornado_width_yd": int(row['tornado_width_yd']) if pd.notna(row.get('tornado_width_yd')) else 0,
            "felt_radius_km": float(row['felt_radius_km']) if pd.notna(row.get('felt_radius_km')) else 5,
            "damage_radius_km": float(row['damage_radius_km']) if pd.notna(row.get('damage_radius_km')) else 0.05,
            "width_km": width_km,
            "timestamp": timestamp_str,
            "deaths_direct": int(row['deaths_direct']) if pd.notna(row.get('deaths_direct')) else 0,
            "deaths_indirect": int(row['deaths_indirect']) if pd.notna(row.get('deaths_indirect')) else 0,
            "injuries_direct": int(row['injuries_direct']) if pd.notna(row.get('injuries_direct')) else 0,
            "injuries_indirect": int(row['injuries_indirect']) if pd.notna(row.get('injuries_indirect')) else 0,
            "damage_property": int(row['damage_property']) if pd.notna(row.get('damage_property')) else 0,
            "damage_crops": int(row['damage_crops']) if pd.notna(row.get('damage_crops')) else 0,
            "location": row.get('location', ''),
            "loc_id": row.get('loc_id', '')
        }

        features = []

        # Start point feature
        start_lat = float(row['latitude'])
        start_lon = float(row['longitude'])
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [start_lon, start_lat]
            },
            "properties": {**props, "point_type": "start"}
        })

        # Add track LineString if we have both endpoints
        end_lat = float(row['end_latitude']) if pd.notna(row.get('end_latitude')) else None
        end_lon = float(row['end_longitude']) if pd.notna(row.get('end_longitude')) else None

        if end_lat is not None and end_lon is not None:
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[start_lon, start_lat], [end_lon, end_lat]]
                },
                "properties": {**props, "geometry_type": "track"}
            })

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "event_id": str(row['event_id']),
                "event_type": "tornado",
                "total_count": len(features),
                "time_range": {
                    "start": timestamp_str,
                    "end": timestamp_str
                }
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tornado detail: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/tornadoes/{event_id}/sequence")
async def get_tornado_sequence(event_id: str):
    """
    Get a sequence of linked tornadoes (same storm system).
    Uses pre-computed sequence_id from data import (like earthquake aftershocks).
    Note: Sequences currently only available for USA tornadoes (1hr/10km linking).
    """
    import pandas as pd

    try:
        # Global tornadoes dataset (USA + Canada)
        events_path = GLOBAL_DIR / "disasters/tornadoes/events.parquet"

        if not events_path.exists():
            return msgpack_error("Tornado data not available", 404)

        df = pd.read_parquet(events_path)
        # Already filtered to tornadoes only in global dataset

        # Find the seed tornado (event_id is always string in parquet)
        seed = df[df['event_id'].astype(str) == str(event_id)]

        if len(seed) == 0:
            return msgpack_error("Tornado not found", 404)

        seed_row = seed.iloc[0]

        # Check if this tornado has a sequence_id (pre-computed during import)
        sequence_id = seed_row.get('sequence_id')
        if pd.isna(sequence_id) or sequence_id is None:
            # No linked sequence - return just this tornado for single-path animation
            sequence_df = seed.copy()
        else:
            # Get all tornadoes in this sequence
            sequence_df = df[df['sequence_id'] == sequence_id].copy()

        # Sort by sequence_position (or timestamp as fallback)
        if 'sequence_position' in sequence_df.columns and sequence_df['sequence_position'].notna().any():
            sequence_df = sequence_df.sort_values('sequence_position')
        elif 'timestamp' in sequence_df.columns:
            sequence_df = sequence_df.sort_values('timestamp')

        # Extract year from timestamp if not present
        if 'year' not in sequence_df.columns and 'timestamp' in sequence_df.columns:
            sequence_df['timestamp'] = pd.to_datetime(sequence_df['timestamp'], errors='coerce')
            sequence_df['year'] = sequence_df['timestamp'].dt.year

        # Build GeoJSON features
        features = []
        for pos, (idx, row) in enumerate(sequence_df.iterrows(), 1):
            time_val = row.get('timestamp')
            raw_scale = row.get('tornado_scale', '')
            scale = str(raw_scale).upper() if pd.notna(raw_scale) else ''

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row['longitude']), float(row['latitude'])]
                },
                "properties": {
                    "event_id": str(row.get('event_id', '')),
                    "tornado_scale": scale if scale else '',
                    "tornado_length_mi": float(row['tornado_length_mi']) if pd.notna(row.get('tornado_length_mi')) else 0,
                    "tornado_width_yd": int(row['tornado_width_yd']) if pd.notna(row.get('tornado_width_yd')) else 0,
                    "felt_radius_km": float(row['felt_radius_km']) if pd.notna(row.get('felt_radius_km')) else 5,
                    "damage_radius_km": float(row['damage_radius_km']) if pd.notna(row.get('damage_radius_km')) else 0.05,
                    "timestamp": str(time_val) if pd.notna(time_val) else None,
                    "year": int(row['year']) if 'year' in row and pd.notna(row['year']) else None,
                    "deaths_direct": int(row['deaths_direct']) if pd.notna(row.get('deaths_direct')) else 0,
                    "injuries_direct": int(row['injuries_direct']) if pd.notna(row.get('injuries_direct')) else 0,
                    "damage_property": float(row['damage_property']) if pd.notna(row.get('damage_property')) else 0,
                    "latitude": float(row['latitude']),
                    "longitude": float(row['longitude']),
                    "end_latitude": float(row['end_latitude']) if pd.notna(row.get('end_latitude')) else None,
                    "end_longitude": float(row['end_longitude']) if pd.notna(row.get('end_longitude')) else None,
                    "is_seed": str(row.get('event_id', '')) == str(seed_row.get('event_id', '')),
                    "sequence_position": int(row['sequence_position']) if pd.notna(row.get('sequence_position')) else pos,
                    "sequence_count": int(row['sequence_count']) if pd.notna(row.get('sequence_count')) else len(sequence_df),
                    "event_type": "tornado",
                    "location": str(row.get('location', '')) if pd.notna(row.get('location')) else ''
                }
            }

            # Add track geometry if end coordinates exist
            if pd.notna(row.get('end_latitude')) and pd.notna(row.get('end_longitude')):
                feature["properties"]["track"] = {
                    "type": "LineString",
                    "coordinates": [
                        [float(row['longitude']), float(row['latitude'])],
                        [float(row['end_longitude']), float(row['end_latitude'])]
                    ]
                }

            features.append(feature)

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "event_id": str(seed_row.get('event_id', '')),
                "event_type": "tornado",
                "total_count": len(features),
                "sequence_id": str(sequence_id) if pd.notna(sequence_id) else None
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tornado sequence: {e}")
        return msgpack_error(str(e), 500)


# === Tropical Storm Data Endpoints ===

@app.get("/api/storms/geojson")
async def get_storms_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    basin: str = None,
    min_category: str = None,
    loc_prefix: str = None,
    affected_loc_id: str = None
):
    """
    Get tropical storms as GeoJSON points for map display.
    Each storm is represented by a single point at its maximum intensity location.
    Default: storms from satellite era (1950) to present.

    Location filters:
    - loc_prefix: Filter by storm origin (e.g., "ATL" for Atlantic basin storms)
    - affected_loc_id: Filter by areas the storm affected (e.g., "USA-FL" for storms that hit Florida)
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('hurricanes', fallback=1950)
    import pandas as pd

    try:
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"

        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)
        positions_df = pd.read_parquet(positions_path)

        # Apply time filters
        if year is not None:
            storms_df = storms_df[storms_df['year'] == year]
        elif start is not None or end is not None:
            storms_df = filter_by_time_range(storms_df, start, end, time_col='start_date')
        elif min_year is not None:
            storms_df = storms_df[storms_df['year'] >= min_year]

        # Basin filter
        if basin is not None:
            storms_df = storms_df[storms_df['basin'] == basin.upper()]

        # Category filter
        if min_category is not None:
            cat_order = {'TD': 0, 'TS': 1, 'Cat1': 2, 'Cat2': 3, 'Cat3': 4, 'Cat4': 5, 'Cat5': 6}
            min_cat_val = cat_order.get(min_category, 0)
            storms_df = storms_df[storms_df['max_category'].map(lambda x: cat_order.get(x, 0) >= min_cat_val)]

        # Location filters (shared helper)
        storms_df = apply_location_filters(
            storms_df, 'hurricanes',
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id,
            event_id_col='loc_id',
            loc_id_col='loc_id'
        )

        # Get max intensity position for each storm
        storm_ids = storms_df['storm_id'].tolist()
        positions_subset = positions_df[positions_df['storm_id'].isin(storm_ids)]

        # Find position with max wind for each storm
        max_positions = positions_subset.loc[positions_subset.groupby('storm_id')['wind_kt'].idxmax()]

        # Build GeoJSON features using merge for efficiency
        # Merge storm data with max positions to avoid nested lookup
        storms_with_pos = storms_df.merge(
            max_positions[['storm_id', 'latitude', 'longitude']],
            on='storm_id',
            how='inner',
            suffixes=('', '_pos')
        )

        # Filter valid coordinates and convert to records
        valid_mask = storms_with_pos['latitude'].notna() & storms_with_pos['longitude'].notna()
        records = storms_with_pos[valid_mask].to_dict('records')

        features = []
        for storm in records:
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(storm['longitude']), float(storm['latitude'])]
                },
                "properties": {
                    "storm_id": storm['storm_id'],
                    "name": storm.get('name') if pd.notna(storm.get('name')) else None,
                    "year": int(storm['year']),
                    "basin": storm['basin'],
                    "max_wind_kt": int(storm['max_wind_kt']) if pd.notna(storm.get('max_wind_kt')) else None,
                    "min_pressure_mb": int(storm['min_pressure_mb']) if pd.notna(storm.get('min_pressure_mb')) else None,
                    "max_category": storm['max_category'],
                    "num_positions": int(storm['num_positions']),
                    "start_date": str(storm['start_date']) if pd.notna(storm.get('start_date')) else None,
                    "end_date": str(storm['end_date']) if pd.notna(storm.get('end_date')) else None,
                    "made_landfall": bool(storm.get('made_landfall', False)),
                    "latitude": float(storm['latitude']),
                    "longitude": float(storm['longitude'])
                }
            })

        logger.info(f"Returning {len(features)} storms for year={year}, min_year={min_year}, basin={basin}")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "count": len(features)
        })

    except Exception as e:
        logger.error(f"Error fetching storms GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/storms/{storm_id}/track")
async def get_storm_track(storm_id: str):
    """
    Get full track positions for a specific storm.
    Returns all 6-hourly positions with wind radii data for animation.
    """
    import pandas as pd

    try:
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"

        if not positions_path.exists():
            return msgpack_error("Storm data not available", 404)

        positions_df = pd.read_parquet(positions_path)
        storm_positions = positions_df[positions_df['storm_id'] == storm_id].sort_values('timestamp')

        if len(storm_positions) == 0:
            return msgpack_error(f"Storm {storm_id} not found", 404)

        # Get storm metadata
        storms_df = pd.read_parquet(storms_path)
        storm_meta = storms_df[storms_df['storm_id'] == storm_id]
        storm_name = storm_meta.iloc[0]['name'] if len(storm_meta) > 0 and pd.notna(storm_meta.iloc[0]['name']) else storm_id

        # Build positions array
        positions = []
        for _, pos in storm_positions.iterrows():
            positions.append({
                "timestamp": str(pos['timestamp']) if pd.notna(pos['timestamp']) else None,
                "latitude": float(pos['latitude']),
                "longitude": float(pos['longitude']),
                "wind_kt": int(pos['wind_kt']) if pd.notna(pos['wind_kt']) else None,
                "pressure_mb": int(pos['pressure_mb']) if pd.notna(pos['pressure_mb']) else None,
                "category": pos['category'],
                "status": pos.get('status') if pd.notna(pos.get('status')) else None,
                # Wind radii
                "r34_ne": int(pos['r34_ne']) if pd.notna(pos.get('r34_ne')) else None,
                "r34_se": int(pos['r34_se']) if pd.notna(pos.get('r34_se')) else None,
                "r34_sw": int(pos['r34_sw']) if pd.notna(pos.get('r34_sw')) else None,
                "r34_nw": int(pos['r34_nw']) if pd.notna(pos.get('r34_nw')) else None,
                "r50_ne": int(pos['r50_ne']) if pd.notna(pos.get('r50_ne')) else None,
                "r50_se": int(pos['r50_se']) if pd.notna(pos.get('r50_se')) else None,
                "r50_sw": int(pos['r50_sw']) if pd.notna(pos.get('r50_sw')) else None,
                "r50_nw": int(pos['r50_nw']) if pd.notna(pos.get('r50_nw')) else None,
                "r64_ne": int(pos['r64_ne']) if pd.notna(pos.get('r64_ne')) else None,
                "r64_se": int(pos['r64_se']) if pd.notna(pos.get('r64_se')) else None,
                "r64_sw": int(pos['r64_sw']) if pd.notna(pos.get('r64_sw')) else None,
                "r64_nw": int(pos['r64_nw']) if pd.notna(pos.get('r64_nw')) else None,
            })

        return msgpack_response({
            "storm_id": storm_id,
            "name": storm_name,
            "positions": positions,
            "count": len(positions)
        })

    except Exception as e:
        logger.error(f"Error fetching storm track: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/storms/tracks/geojson")
async def get_storm_tracks_geojson(year: int = None, start: str = None, end: str = None, min_year: int = None, basin: str = None, min_category: str = None):
    """
    Get storm tracks as GeoJSON LineStrings for yearly overview display.
    Each storm is a LineString colored by max category.
    Loads all storms from satellite era (1950) to present.
    Optional min_category filter: TD, TS, Cat1, Cat2, Cat3, Cat4, Cat5
    """
    # Get default min_year from metadata if not provided
    if min_year is None:
        min_year = get_default_min_year('hurricanes', fallback=1950)

    import pandas as pd

    try:
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"

        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)
        positions_df = pd.read_parquet(positions_path)

        # Apply time filters - min_year defaults to 1950
        if year is not None:
            storms_df = storms_df[storms_df['year'] == year]
        elif start is not None or end is not None:
            storms_df = filter_by_time_range(storms_df, start, end, time_col='start_date')
        elif min_year is not None:
            storms_df = storms_df[storms_df['year'] >= min_year]

        # Basin filter
        if basin is not None:
            storms_df = storms_df[storms_df['basin'] == basin.upper()]

        # Category filter - filter by minimum category
        if min_category is not None:
            cat_order = {'TD': 0, 'TS': 1, 'Cat1': 2, 'Cat2': 3, 'Cat3': 4, 'Cat4': 5, 'Cat5': 6}
            min_cat_val = cat_order.get(min_category, 0)
            storms_df['cat_val'] = storms_df['max_category'].map(lambda x: cat_order.get(x, 0))
            storms_df = storms_df[storms_df['cat_val'] >= min_cat_val]
            storms_df = storms_df.drop(columns=['cat_val'])

        # Build storm metadata lookup dict (O(1) access)
        storms_df = storms_df.set_index('storm_id')
        storm_ids_set = set(storms_df.index.tolist())

        # Filter positions to only those storms, sort once
        positions_subset = positions_df[positions_df['storm_id'].isin(storm_ids_set)].copy()
        positions_subset = positions_subset.dropna(subset=['latitude', 'longitude'])
        positions_subset = positions_subset.sort_values(['storm_id', 'timestamp'])

        # Build coordinate lists using groupby (vectorized, much faster than iterrows)
        coords_by_storm = {}
        for storm_id, group in positions_subset.groupby('storm_id'):
            coords = list(zip(group['longitude'].tolist(), group['latitude'].tolist()))
            if len(coords) >= 2:
                coords_by_storm[storm_id] = [[float(lon), float(lat)] for lon, lat in coords]

        # Build features from storms that have valid tracks
        features = []
        for storm_id, coords in coords_by_storm.items():
            storm = storms_df.loc[storm_id]
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords
                },
                "properties": {
                    "storm_id": storm_id,
                    "name": storm.get('name') if pd.notna(storm.get('name')) else None,
                    "year": int(storm['year']),
                    "basin": storm['basin'],
                    "max_wind_kt": int(storm['max_wind_kt']) if pd.notna(storm['max_wind_kt']) else None,
                    "min_pressure_mb": int(storm['min_pressure_mb']) if pd.notna(storm['min_pressure_mb']) else None,
                    "max_category": storm['max_category'],
                    "num_positions": int(storm['num_positions']),
                    "start_date": str(storm['start_date']) if pd.notna(storm.get('start_date')) else None,
                    "end_date": str(storm['end_date']) if pd.notna(storm.get('end_date')) else None,
                    "made_landfall": bool(storm.get('made_landfall', False))
                }
            })

        logger.info(f"Returning {len(features)} storm tracks for year={year}, min_year={min_year}, basin={basin}, min_category={min_category}")

        return msgpack_response({
            "type": "FeatureCollection",
            "features": features,
            "count": len(features)
        })

    except Exception as e:
        logger.error(f"Error fetching storm tracks GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/storms/list")
async def get_storms_list(year: int = None, min_year: int = None, basin: str = None, limit: int = 100):
    """
    Get list of storms with metadata for filtering/selection.
    Returns compact list without track data.
    """
    import pandas as pd

    try:
        # Use metadata-driven default for min_year
        if min_year is None:
            min_year = get_default_min_year('hurricanes', fallback=1950)

        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"

        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)

        # Apply filters
        if year is not None:
            storms_df = storms_df[storms_df['year'] == year]
        elif min_year is not None:
            storms_df = storms_df[storms_df['year'] >= min_year]

        if basin is not None:
            storms_df = storms_df[storms_df['basin'] == basin.upper()]

        # Sort by max wind (strongest first)
        storms_df = storms_df.sort_values('max_wind_kt', ascending=False)

        # Apply limit
        if limit is not None and limit > 0:
            storms_df = storms_df.head(limit)

        # Build list
        storms = []
        for _, storm in storms_df.iterrows():
            storms.append({
                "storm_id": storm['storm_id'],
                "name": storm.get('name') if pd.notna(storm.get('name')) else None,
                "year": int(storm['year']),
                "basin": storm['basin'],
                "max_wind_kt": int(storm['max_wind_kt']) if pd.notna(storm['max_wind_kt']) else None,
                "max_category": storm['max_category'],
                "start_date": str(storm['start_date']) if pd.notna(storm.get('start_date')) else None,
            })

        return msgpack_response({
            "storms": storms,
            "count": len(storms)
        })

    except Exception as e:
        logger.error(f"Error fetching storms list: {e}")
        return msgpack_error(str(e), 500)


# === Weather Grid Endpoints ===

@app.get("/api/weather/grid")
async def get_weather_grid(
    tier: str,
    variable: str = None,
    variables: str = None,
    year: int = None
):
    """
    Get weather grid data for animation.

    Loads parquet files and pivots to wide format for efficient animation.
    Returns timestamps array + values dict (keyed by variable, 16,020 values per timestamp).

    Args:
        tier: 'hourly', 'weekly', or 'monthly'
        variable: Single variable (legacy, for backwards compatibility)
        variables: Comma-separated list of variables (e.g., 'temp_c,humidity,snow_depth_m')
        year: For monthly tier, which year to load

    Response format:
        Single variable: { values: [[...], ...], variable: 'temp_c', ... }
        Multiple variables: { values: { temp_c: [[...], ...], humidity: [[...], ...] }, variables: [...], ... }
    """
    import pandas as pd
    import numpy as np
    from glob import glob
    from datetime import datetime, timezone

    try:
        weather_base = GLOBAL_DIR / "climate" / "weather"

        # Validate tier
        if tier not in ('hourly', 'weekly', 'monthly'):
            return msgpack_error(f"Invalid tier: {tier}. Must be hourly, weekly, or monthly", 400)

        # Parse variables - support both single 'variable' and multi 'variables' params
        valid_vars = {
            'temp_c', 'humidity', 'snow_depth_m',
            'precipitation_mm', 'cloud_cover_pct', 'pressure_hpa',
            'solar_radiation', 'soil_temp_c', 'soil_moisture'
        }
        if variables:
            # Multi-variable request
            requested_vars = [v.strip() for v in variables.split(',')]
            invalid = [v for v in requested_vars if v not in valid_vars]
            if invalid:
                return msgpack_error(f"Invalid variables: {invalid}. Must be one of: {valid_vars}", 400)
        elif variable:
            # Single variable (backwards compatible)
            if variable not in valid_vars:
                return msgpack_error(f"Invalid variable: {variable}. Must be one of: {valid_vars}", 400)
            requested_vars = [variable]
        else:
            # Default to temp_c
            requested_vars = ['temp_c']

        is_multi = len(requested_vars) > 1

        # Tier cascade: if requested tier unavailable for year, fall back to finer resolution
        actual_tier = tier
        files = []

        def get_files_for_tier(t, y):
            """Get parquet files for a tier/year combo."""
            t_dir = weather_base / t
            if not t_dir.exists():
                return []

            if t == 'monthly':
                if y is None:
                    return []
                y_dir = t_dir / str(y)
                if not y_dir.exists():
                    return []
                return sorted(glob(str(y_dir / "*.parquet")))
            elif t == 'weekly':
                # For weekly, filter to requested year if specified
                all_files = sorted(glob(str(t_dir / "**" / "*.parquet"), recursive=True))
                if y is not None:
                    return [f for f in all_files if f"/{y}/" in f.replace("\\", "/") or f"\\{y}\\" in f]
                return all_files
            else:  # hourly
                # For hourly, filter to requested year if specified
                all_files = sorted(glob(str(t_dir / "**" / "*.parquet"), recursive=True))
                if y is not None:
                    return [f for f in all_files if f"/{y}/" in f.replace("\\", "/") or f"\\{y}\\" in f]
                return all_files

        # Try requested tier first
        files = get_files_for_tier(tier, year)

        # Cascade: try finer resolution first, then coarser
        # weekly -> hourly (finer) -> monthly (coarser)
        # monthly -> weekly (finer) -> hourly (finest)
        if not files and tier == 'weekly':
            logger.info(f"No weekly data for {year}, trying hourly")
            files = get_files_for_tier('hourly', year)
            if files:
                actual_tier = 'hourly'
            else:
                logger.info(f"No hourly data for {year}, trying monthly")
                files = get_files_for_tier('monthly', year)
                if files:
                    actual_tier = 'monthly'

        if not files and tier == 'monthly':
            logger.info(f"No monthly data for {year}, trying weekly")
            files = get_files_for_tier('weekly', year)
            if files:
                actual_tier = 'weekly'
            else:
                logger.info(f"No weekly data for {year}, trying hourly")
                files = get_files_for_tier('hourly', year)
                if files:
                    actual_tier = 'hourly'

        if not files:
            return msgpack_error(f"No {tier} data files found for year {year}", 404)

        # Read and pivot data - support multiple variables in one pass
        timestamps = []
        all_values = {var: [] for var in requested_vars}  # Dict of lists per variable
        grid_info = None  # Will be set from first file

        # Columns to read: lat, lon, plus all requested variables
        columns_to_read = ['lat', 'lon'] + requested_vars

        for filepath in files:
            try:
                df = pd.read_parquet(filepath, columns=columns_to_read)

                # Parse timestamp from path based on actual tier (not requested tier)
                path_parts = Path(filepath).parts
                if actual_tier == 'monthly':
                    # monthly/YYYY/MM.parquet
                    yr = int(path_parts[-2])
                    mo = int(path_parts[-1].replace('.parquet', ''))
                    ts = datetime(yr, mo, 1, tzinfo=timezone.utc)
                elif actual_tier == 'weekly':
                    # weekly/YYYY/WW.parquet
                    yr = int(path_parts[-2])
                    wk = int(path_parts[-1].replace('.parquet', ''))
                    # Convert ISO week to timestamp (Monday of that week)
                    ts = datetime.strptime(f'{yr}-W{wk:02d}-1', '%G-W%V-%u').replace(tzinfo=timezone.utc)
                else:  # hourly
                    # hourly/YYYY/MM/DD/HH.parquet
                    yr = int(path_parts[-4])
                    mo = int(path_parts[-3])
                    dy = int(path_parts[-2])
                    hr = int(path_parts[-1].replace('.parquet', ''))
                    ts = datetime(yr, mo, dy, hr, tzinfo=timezone.utc)

                # Sort by lat (desc) then lon (asc) for consistent grid ordering
                df = df.sort_values(['lat', 'lon'], ascending=[False, True])

                # Extract actual grid coordinates from first file
                if grid_info is None:
                    unique_lats = sorted(df['lat'].unique(), reverse=True)  # Descending
                    unique_lons = sorted(df['lon'].unique())  # Ascending

                    # Calculate step size from actual data
                    lat_step = abs(unique_lats[1] - unique_lats[0]) if len(unique_lats) > 1 else 2
                    lon_step = abs(unique_lons[1] - unique_lons[0]) if len(unique_lons) > 1 else 2

                    grid_info = {
                        'lat_start': float(unique_lats[0]),  # First (highest) latitude
                        'lon_start': float(unique_lons[0]),  # First (lowest) longitude
                        'lat_step': float(lat_step),
                        'lon_step': float(lon_step),
                        'rows': len(unique_lats),
                        'cols': len(unique_lons)
                    }

                # Extract values for each requested variable
                ts_ms = int(ts.timestamp() * 1000)
                timestamps.append(ts_ms)

                for var in requested_vars:
                    values = df[var].values.tolist()
                    # Replace NaN with None for JSON compatibility
                    values = [None if (isinstance(v, float) and np.isnan(v)) else v for v in values]
                    all_values[var].append(values)

            except Exception as e:
                logger.warning(f"Could not read {filepath}: {e}")
                continue

        if not timestamps:
            return msgpack_error("No valid data files could be read", 500)

        # Sort by timestamp - need to sort all variables together
        sort_indices = sorted(range(len(timestamps)), key=lambda i: timestamps[i])
        timestamps = [timestamps[i] for i in sort_indices]
        for var in requested_vars:
            all_values[var] = [all_values[var][i] for i in sort_indices]

        # Color scale configuration
        color_scales = {
            'temp_c': {
                'min': -40, 'max': 45,
                'stops': [
                    [-40, '#00008B'], [-30, '#0000FF'], [-10, '#87CEEB'],
                    [0, '#FFFFFF'], [10, '#FFFF99'], [25, '#FFA500'],
                    [35, '#FF0000'], [45, '#8B0000']
                ]
            },
            'humidity': {
                'min': 0, 'max': 100,
                'stops': [
                    [0, '#FFFFFF'], [25, '#E0FFFF'], [50, '#87CEEB'],
                    [75, '#4682B4'], [100, '#000080']
                ]
            },
            'snow_depth_m': {
                'min': 0, 'max': 2,
                'stops': [
                    [0, '#FFFFFF'], [0.1, '#FFFFFF'], [0.5, '#E6E6FA'],
                    [1.0, '#9370DB'], [2.0, '#4B0082']
                ]
            },
            'precipitation_mm': {
                'min': 0, 'max': 50,
                'stops': [
                    [0, '#FFFFFF'], [1, '#E0FFE0'], [5, '#90EE90'],
                    [15, '#228B22'], [30, '#006400'], [50, '#00008B']
                ]
            },
            'cloud_cover_pct': {
                'min': 0, 'max': 100,
                'stops': [
                    [0, '#87CEEB'], [25, '#B0C4DE'], [50, '#A9A9A9'],
                    [75, '#696969'], [100, '#404040']
                ]
            },
            'pressure_hpa': {
                'min': 970, 'max': 1050,
                'stops': [
                    [970, '#8B0000'], [990, '#FF6347'], [1010, '#FFFFFF'],
                    [1030, '#87CEEB'], [1050, '#00008B']
                ]
            },
            'solar_radiation': {
                'min': 0, 'max': 1000,
                'stops': [
                    [0, '#000000'], [100, '#4B0082'], [300, '#FF8C00'],
                    [600, '#FFD700'], [1000, '#FFFFFF']
                ]
            },
            'soil_temp_c': {
                'min': -20, 'max': 40,
                'stops': [
                    [-20, '#00008B'], [-10, '#0000FF'], [0, '#8B4513'],
                    [15, '#D2691E'], [30, '#FF4500'], [40, '#8B0000']
                ]
            },
            'soil_moisture': {
                'min': 0, 'max': 0.5,
                'stops': [
                    [0, '#DEB887'], [0.1, '#D2B48C'], [0.2, '#8FBC8F'],
                    [0.3, '#228B22'], [0.5, '#006400']
                ]
            }
        }

        # Use actual grid info from data, with fallback defaults
        if grid_info is None:
            grid_info = {
                'lat_start': 89, 'lon_start': -179,
                'lat_step': 2, 'lon_step': 2,
                'rows': 90, 'cols': 180
            }

        # Build response - different format for single vs multi variable
        if is_multi:
            # Multi-variable response
            return msgpack_response({
                'tier': actual_tier,
                'requested_tier': tier,
                'variables': requested_vars,
                'timestamps': timestamps,
                'values': all_values,  # Dict: { 'temp_c': [[...], ...], 'humidity': [[...], ...] }
                'grid': grid_info,
                'color_scales': {var: color_scales.get(var, color_scales['temp_c']) for var in requested_vars},
                'count': len(timestamps)
            })
        else:
            # Single variable response (backwards compatible)
            single_var = requested_vars[0]
            return msgpack_response({
                'tier': actual_tier,
                'requested_tier': tier,
                'variable': single_var,
                'timestamps': timestamps,
                'values': all_values[single_var],  # List: [[...], ...]
                'grid': grid_info,
                'color_scale': color_scales.get(single_var, color_scales['temp_c']),
                'count': len(timestamps)
            })

    except Exception as e:
        logger.error(f"Error fetching weather grid: {e}")
        traceback.print_exc()
        return msgpack_error(str(e), 500)


@app.get("/api/weather/available")
async def get_weather_available():
    """
    Get available time ranges for each weather tier.
    Used by frontend to know what data can be requested.
    """
    from glob import glob
    from datetime import datetime, timezone

    try:
        weather_base = GLOBAL_DIR / "climate" / "weather"
        result = {}

        # Check hourly
        hourly_dir = weather_base / "hourly"
        if hourly_dir.exists():
            files = sorted(glob(str(hourly_dir / "**" / "*.parquet"), recursive=True))
            if files:
                # Parse first and last file timestamps
                first = files[0]
                last = files[-1]
                # hourly/YYYY/MM/DD/HH.parquet
                fp = Path(first).parts
                lp = Path(last).parts
                first_ts = datetime(int(fp[-4]), int(fp[-3]), int(fp[-2]), int(fp[-1].replace('.parquet', '')), tzinfo=timezone.utc)
                last_ts = datetime(int(lp[-4]), int(lp[-3]), int(lp[-2]), int(lp[-1].replace('.parquet', '')), tzinfo=timezone.utc)
                result['hourly'] = {
                    'min': first_ts.isoformat(),
                    'max': last_ts.isoformat(),
                    'count': len(files)
                }

        # Check weekly
        weekly_dir = weather_base / "weekly"
        if weekly_dir.exists():
            files = sorted(glob(str(weekly_dir / "**" / "*.parquet"), recursive=True))
            if files:
                # Get year range
                years = set()
                for f in files:
                    fp = Path(f).parts
                    years.add(int(fp[-2]))
                result['weekly'] = {
                    'min_year': min(years),
                    'max_year': max(years),
                    'count': len(files)
                }

        # Check monthly
        monthly_dir = weather_base / "monthly"
        if monthly_dir.exists():
            # Get year directories
            year_dirs = sorted([d for d in monthly_dir.iterdir() if d.is_dir() and d.name.isdigit()])
            if year_dirs:
                # Count all files
                all_files = glob(str(monthly_dir / "**" / "*.parquet"), recursive=True)
                result['monthly'] = {
                    'min_year': int(year_dirs[0].name),
                    'max_year': int(year_dirs[-1].name),
                    'years': [int(d.name) for d in year_dirs],
                    'count': len(all_files)
                }

        # Default min year for time slider (matches disaster APIs)
        # Earlier years still accessible via chat/explicit request
        result['default_min_year'] = 2000

        return msgpack_response(result)

    except Exception as e:
        logger.error(f"Error checking weather availability: {e}")
        return msgpack_error(str(e), 500)


# === Reference Data Endpoints ===

@app.get("/reference/admin-levels")
async def get_admin_levels():
    """
    Get admin level names for all countries.
    Used by frontend for popup display (e.g., "Clackamas" -> "Clackamas County").
    """
    try:
        ref_path = BASE_DIR / "mapmover" / "reference" / "admin_levels.json"
        if ref_path.exists():
            with open(ref_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return msgpack_response(data)
        else:
            return msgpack_error("admin_levels.json not found", 404)
    except Exception as e:
        logger.error(f"Error loading admin_levels.json: {e}")
        return msgpack_error(str(e), 500)


# === Settings Endpoints ===

@app.get("/settings")
async def get_settings():
    """
    Get current application settings.
    Returns backup path and folder existence status.
    """
    try:
        settings = get_settings_with_status()
        return msgpack_response(settings)
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        return msgpack_error(str(e), 500)


@app.post("/settings")
async def update_settings(req: Request):
    """
    Update application settings.
    Accepts: { backup_path: "..." }
    """
    try:
        data = await decode_request_body(req)
        backup_path = data.get("backup_path", "")

        # Save the settings
        success = save_settings({"backup_path": backup_path})

        if success:
            settings = get_settings_with_status()
            return msgpack_response({"success": True, "settings": settings})
        else:
            return msgpack_error("Failed to save settings", 500)
    except Exception as e:
        logger.error(f"Error updating settings: {e}")
        return msgpack_error(str(e), 500)


@app.post("/settings/init-folders")
async def initialize_folders(req: Request):
    """
    Initialize the backup folder structure.
    Creates geometry/ and data/ folders at the backup path.
    """
    try:
        data = await decode_request_body(req)
        backup_path = data.get("backup_path", "")

        if not backup_path:
            return msgpack_error("Backup path is required", 400)

        # Save the path and create folders
        save_settings({"backup_path": backup_path})
        folders = init_backup_folders(backup_path)

        return msgpack_response({
            "success": True,
            "folders": folders,
            "message": f"Initialized folders at {backup_path}"
        })
    except Exception as e:
        logger.error(f"Error initializing folders: {e}")
        return msgpack_error(str(e), 500)


# === Filter Intent Handler (Overlay Integration) ===

def handle_filter_intent(filter_intent: dict, cache_stats: dict, active_overlays: dict) -> dict:
    """
    Handle filter-related queries without LLM call.

    Returns response dict or None if should fall through to LLM.
    """
    if not filter_intent:
        return None

    intent_type = filter_intent.get("type")
    overlay = filter_intent.get("overlay")

    if intent_type == "read_filters":
        # User is asking about current filters - respond from cache
        if not overlay:
            return {
                "type": "chat",
                "message": "No overlay is currently active. Enable an overlay from the right panel to see event data.",
                "from_cache": True
            }

        stats = cache_stats.get(overlay, {}) if cache_stats else {}
        filters = active_overlays.get("filters", {}) if active_overlays else {}
        count = stats.get("count", 0)

        # Build response message based on overlay type
        if overlay == "earthquakes":
            min_mag = stats.get("minMag") or filters.get("minMagnitude", "?")
            max_mag = stats.get("maxMag") or "?"
            message = f"Currently showing {count} earthquakes"
            if min_mag != "?":
                message += f", magnitude {min_mag} to {max_mag}"
            message += "."
        elif overlay == "hurricanes":
            cats = stats.get("categories", [])
            message = f"Currently showing {count} hurricanes"
            if cats:
                message += f" (categories: {', '.join(str(c) for c in cats)})"
            message += "."
        elif overlay == "wildfires":
            min_area = stats.get("minAreaKm2") or filters.get("minAreaKm2", "?")
            message = f"Currently showing {count} wildfires"
            if min_area != "?":
                message += f" (minimum {min_area} km2)"
            message += "."
        elif overlay == "volcanoes":
            min_vei = stats.get("minVei") or filters.get("minVei", "?")
            max_vei = stats.get("maxVei") or "?"
            message = f"Currently showing {count} volcanic eruptions"
            if min_vei != "?":
                message += f", VEI {min_vei} to {max_vei}"
            message += "."
        elif overlay == "tornadoes":
            scales = stats.get("scales", [])
            message = f"Currently showing {count} tornadoes"
            if scales:
                message += f" (scales: {', '.join(str(s) for s in scales)})"
            message += "."
        else:
            message = f"Currently showing {count} {overlay} events."

        # Add year range if available
        years = stats.get("years", [])
        if years and len(years) > 0:
            message += f" Data loaded for {years[0]}-{years[-1]}."

        return {
            "type": "cache_answer",
            "message": message,
            "from_cache": True,
            "overlay": overlay,
            "stats": stats
        }

    elif intent_type == "change_filters":
        # User wants to change filters - check if can satisfy from cache
        new_filters = {}

        if "minMagnitude" in filter_intent:
            new_filters["minMagnitude"] = filter_intent["minMagnitude"]
        if "maxMagnitude" in filter_intent:
            new_filters["maxMagnitude"] = filter_intent["maxMagnitude"]
        if "minVei" in filter_intent:
            new_filters["minVei"] = filter_intent["minVei"]
        if "minCategory" in filter_intent:
            new_filters["minCategory"] = filter_intent["minCategory"]
        if "minScale" in filter_intent:
            new_filters["minScale"] = filter_intent["minScale"]
        if "minAreaKm2" in filter_intent:
            new_filters["minAreaKm2"] = filter_intent["minAreaKm2"]
        if filter_intent.get("clear"):
            new_filters["clear"] = True

        # Phase 7: Check if request can be satisfied from cache (no API call needed)
        stats = cache_stats.get(overlay, {}) if cache_stats else {}
        loaded_filters = stats.get("loadedFilters", {})
        can_filter_from_cache = False

        if loaded_filters and not new_filters.get("clear"):
            # Check if new filter is MORE restrictive than what's loaded
            # More restrictive = higher min values = can be satisfied from cache
            can_filter_from_cache = True

            if "minMagnitude" in new_filters:
                loaded_min = loaded_filters.get("minMagnitude")
                if loaded_min is not None and new_filters["minMagnitude"] < loaded_min:
                    can_filter_from_cache = False  # Requesting data we don't have

            if "minVei" in new_filters:
                loaded_min = loaded_filters.get("minVei")
                if loaded_min is not None and new_filters["minVei"] < loaded_min:
                    can_filter_from_cache = False

            if "minAreaKm2" in new_filters:
                loaded_min = loaded_filters.get("minAreaKm2")
                if loaded_min is not None and new_filters["minAreaKm2"] < loaded_min:
                    can_filter_from_cache = False

        # Build confirmation message
        if new_filters.get("clear"):
            message = f"Clearing filters for {overlay}. Showing all events."
        else:
            filter_parts = []
            if "minMagnitude" in new_filters and "maxMagnitude" in new_filters:
                filter_parts.append(f"magnitude {new_filters['minMagnitude']}-{new_filters['maxMagnitude']}")
            elif "minMagnitude" in new_filters:
                filter_parts.append(f"magnitude {new_filters['minMagnitude']}+")
            elif "maxMagnitude" in new_filters:
                filter_parts.append(f"magnitude up to {new_filters['maxMagnitude']}")
            if "minVei" in new_filters:
                filter_parts.append(f"VEI {new_filters['minVei']}+")
            if "minCategory" in new_filters:
                filter_parts.append(f"category {new_filters['minCategory']}+")
            if "minScale" in new_filters:
                filter_parts.append(f"EF{new_filters['minScale']}+")
            if "minAreaKm2" in new_filters:
                filter_parts.append(f"area {new_filters['minAreaKm2']}+ km2")

            if can_filter_from_cache:
                message = f"Filtering to show " + ", ".join(filter_parts) + " (from cached data)."
            else:
                message = f"Updating {overlay} to show " + ", ".join(filter_parts) + "."

        # Return appropriate response type based on cache analysis
        if can_filter_from_cache:
            return {
                "type": "filter_existing",
                "message": message,
                "overlay": overlay,
                "filters": new_filters,
                "from_cache": True
            }
        else:
            return {
                "type": "filter_update",
                "message": message,
                "overlay": overlay,
                "filters": new_filters
            }

    return None


# === Chat Endpoint (Order Taker Model) ===

@app.post("/chat")
async def chat_endpoint(req: Request):
    """
    Chat endpoint - Order Taker model (Phase 1B).

    Flow:
    1. User sends query -> Returns order for confirmation
    2. User confirms order -> Executes and returns GeoJSON data

    Request body:
    - query: str - Natural language query
    - chatHistory: list - Previous messages for context
    - confirmed_order: dict - If present, execute this order directly
    """
    try:
        body = await decode_request_body(req)

        # Get or create session cache for this client
        session_id = body.get("sessionId", "anonymous")
        cache = session_manager.get_or_create(session_id)

        # Check if this is a confirmed order execution
        if body.get("confirmed_order"):
            try:
                confirmed_order = body["confirmed_order"]

                # Generate request key from order for caching
                order_str = json.dumps(confirmed_order, sort_keys=True)
                request_key = hashlib.md5(order_str.encode()).hexdigest()[:16]

                # Execute the order (DB fetch is cheap - local parquets)
                result = execute_order(confirmed_order)

                # Force flag: skip dedup (used for recovery when frontend cache is cleared)
                force_refetch = body.get("force", False)
                if force_refetch:
                    logger.info("Force refetch requested - clearing session cache for this data")
                    cache.clear()  # Clear session cache to allow re-sending

                # Post-fetch dedup: filter response by what's already on the frontend
                # Session cache mirrors frontend exactly
                is_events = result.get("type") == "events"
                # source_id must match what frontend sends in clear-source
                # Events use overlay ID (matches frontend overlay naming)
                EVENT_TYPE_TO_OVERLAY = {
                    "earthquake": "earthquakes", "volcano": "volcanoes",
                    "tsunami": "tsunamis", "hurricane": "hurricanes",
                    "wildfire": "wildfires", "tornado": "tornadoes",
                    "flood": "floods", "drought": "drought",
                    "landslide": "landslides",
                }
                event_type = result.get("event_type", "")
                if is_events:
                    source_id = EVENT_TYPE_TO_OVERLAY.get(event_type, event_type)
                else:
                    source_id = result.get("metric_key", "data")
                geojson = result["geojson"]
                features = geojson.get("features", [])
                original_count = len(features)

                if is_events:
                    # Event dedup: filter by event_id (all-or-nothing per event)
                    new_features = cache.filter_events(features)
                    delta_count = len(new_features)
                    filtered_geojson = {"type": "FeatureCollection", "features": new_features}
                    filtered_year_data = None

                elif result.get("multi_year") and result.get("year_data"):
                    # Multi-year data: filter year_data at cell level (loc_id:year:metric)
                    year_data = result["year_data"]
                    year_count = len(year_data)
                    total_cells = sum(
                        sum(len(metrics) for metrics in loc_data.values())
                        for loc_data in year_data.values()
                    )
                    logger.info(f"Multi-year data: {year_count} years, {total_cells} total cells, {original_count} features")

                    filtered_year_data = cache.filter_year_data(year_data)
                    filtered_cells = sum(
                        sum(len(metrics) for metrics in loc_data.values())
                        for loc_data in filtered_year_data.values()
                    )
                    logger.info(f"After dedup: {filtered_cells} new cells (filtered {total_cells - filtered_cells})")

                    # Features needed only if their loc_id has new data cells
                    new_loc_ids = set()
                    for loc_data in filtered_year_data.values():
                        new_loc_ids.update(loc_data.keys())

                    new_features = [
                        f for f in features
                        if (f.get("properties", {}).get("loc_id") or f.get("id")) in new_loc_ids
                    ]
                    delta_count = len(new_features)
                    logger.info(f"Features with new data: {delta_count}/{original_count}")
                    filtered_geojson = {"type": "FeatureCollection", "features": new_features}

                else:
                    # Single-year data without year_data: send everything (typically small)
                    new_features = features
                    delta_count = original_count
                    filtered_geojson = geojson
                    filtered_year_data = None

                # If nothing new after dedup, return already_loaded
                if delta_count == 0 and original_count > 0:
                    logger.debug(f"Dedup: all {original_count} features already sent, returning already_loaded")
                    return msgpack_response({
                        "type": "already_loaded",
                        "message": f"This data ({original_count} features) is already loaded on your map.",
                        "summary": result.get("summary", ""),
                    })

                # Build response
                response = {
                    "type": result.get("type", "data"),
                    "data_type": result.get("data_type"),
                    "source_id": result.get("source_id"),
                    "geojson": filtered_geojson,
                    "summary": result["summary"],
                    "count": delta_count,
                    "sources": result.get("sources", [])
                }

                # Include event metadata if present
                if is_events:
                    response["event_type"] = result.get("event_type")
                    response["time_range"] = result.get("time_range")

                # Include multi-year data if present (for time slider)
                if result.get("multi_year"):
                    response["multi_year"] = True
                    response["year_range"] = result["year_range"]
                    response["metric_key"] = result.get("metric_key")
                    response["available_metrics"] = result.get("available_metrics", [])
                    response["metric_year_ranges"] = result.get("metric_year_ranges", {})
                    response["year_data"] = filtered_year_data if filtered_year_data else {}

                # Register what was actually sent (mirrors frontend cache)
                if is_events and new_features:
                    cache.register_sent_events(new_features, source_id)
                elif filtered_year_data:
                    # Each metric registers as its own source (clearing = deleting a column)
                    cache.register_sent_year_data(filtered_year_data)

                cache.touch()

                if delta_count < original_count:
                    logger.info(f"Delta sent: {delta_count}/{original_count} features ({original_count - delta_count} deduped)")

                return msgpack_response(response)
            except Exception as e:
                logger.error(f"Order execution error: {e}")
                return msgpack_response({
                    "type": "error",
                    "message": str(e)
                }, status_code=400)

        # Otherwise, interpret the natural language request
        query = body.get("query", "")
        chat_history = body.get("chatHistory", [])
        viewport = body.get("viewport")  # {center, zoom, bounds, adminLevel}
        resolved_location = body.get("resolved_location")  # From disambiguation selection
        active_overlays = body.get("activeOverlays")  # {type, filters, allActive}
        cache_stats = body.get("cacheStats")  # {overlayId: {count, years, minMag, ...}}
        time_state = body.get("timeState")  # {isLiveLocked, currentTime, timezone, ...}
        saved_order_names = body.get("savedOrderNames", [])  # Phase 7: Saved order names for load/save

        if not query:
            return msgpack_error("No query provided", 400)

        logger.debug(f"Chat query: {query[:100]}...")
        if active_overlays and active_overlays.get("type"):
            logger.debug(f"Active overlay: {active_overlays.get('type')} with filters: {active_overlays.get('filters')}")

        # Run preprocessor to extract hints (Tier 2) with viewport context
        hints = preprocess_query(query, viewport=viewport, active_overlays=active_overlays, cache_stats=cache_stats, saved_order_names=saved_order_names, time_state=time_state)
        if hints.get("summary"):
            logger.debug(f"Preprocessor hints: {hints['summary']}")

        # If resolved_location is provided, skip disambiguation and use it directly
        if resolved_location:
            logger.debug(f"Using resolved location: {resolved_location}")
            # Override preprocessor hints with resolved location
            hints["location"] = {
                "matched_term": resolved_location.get("matched_term"),
                "iso3": resolved_location.get("iso3"),
                "country_name": resolved_location.get("country_name"),
                "loc_id": resolved_location.get("loc_id"),
                "is_subregion": resolved_location.get("loc_id") != resolved_location.get("iso3"),
                "source": "disambiguation_selection"
            }
            hints["disambiguation"] = None  # Clear disambiguation flag

        # Check for "show borders" intent - display geometry from previous disambiguation
        if hints.get("show_borders"):
            # Check for previous_disambiguation passed from frontend
            previous_options = body.get("previous_disambiguation_options", [])

            if previous_options:
                loc_ids_to_show = [opt.get("loc_id") for opt in previous_options if opt.get("loc_id")]
            else:
                # Fallback: search for the term from recent chat if available
                loc_ids_to_show = []

            if loc_ids_to_show:
                logger.debug(f"Show borders: displaying {len(loc_ids_to_show)} locations")
                # Fetch geometry for these locations
                from mapmover.data_loading import fetch_geometries_by_loc_ids
                geojson = fetch_geometries_by_loc_ids(loc_ids_to_show)

                return msgpack_response({
                    "type": "navigate",
                    "message": f"Showing {len(loc_ids_to_show)} locations on the map. Click any location to see data options.",
                    "locations": previous_options if previous_options else [{"loc_id": lid} for lid in loc_ids_to_show],
                    "loc_ids": loc_ids_to_show,
                    "original_query": query,
                    "geojson": geojson,
                })
            else:
                # No previous disambiguation found - tell user
                return msgpack_response({
                    "type": "chat",
                    "reply": "I don't have a list of locations to display. Please first ask about specific locations (e.g., 'show me washington county') to get a list.",
                })

        # Check for drill-down pattern (e.g., "texas counties" -> show counties of Texas)
        # This is a distinct UI action that doesn't need LLM interpretation
        navigation = hints.get("navigation")
        if navigation and navigation.get("is_navigation"):
            locations = navigation.get("locations", [])
            if len(locations) == 1 and locations[0].get("drill_to_level"):
                loc = locations[0]
                loc_id = loc.get("loc_id")
                drill_level = loc.get("drill_to_level")
                name = loc.get("matched_term", loc_id)

                logger.debug(f"Drill-down request: {name} -> {drill_level}")

                return msgpack_response({
                    "type": "drilldown",
                    "message": f"Showing {drill_level} of {name}...",
                    "loc_id": loc_id,
                    "name": name,
                    "drill_to_level": drill_level,
                    "original_query": query,
                })

        # =======================================================================
        # PHASE 2 REFACTOR: No more early returns for navigation/disambiguation
        # All queries go to LLM which sees candidates and decides interpretation
        # Navigation, disambiguation, and filter intents are now in hints.candidates
        # =======================================================================

        # Single LLM call to interpret request (with Tier 3/4 context from hints)
        result = interpret_request(query, chat_history, hints=hints)

        # =======================================================================
        # POST-LLM ROUTING (Phase 4 Refactor)
        # Handle all response types from LLM
        # =======================================================================

        if result["type"] == "order":
            # Run postprocessor to validate and expand derived fields
            processed = postprocess_order(result["order"], hints)
            logger.debug(f"Postprocessor: {processed.get('validation_summary')}")

            # Check for metric count warning (pre-order gate)
            if processed.get("metric_warning") and not body.get("force_metrics"):
                # Get display items for the pending order
                display_items = get_display_items(
                    processed.get("items", []),
                    processed.get("derived_specs", [])
                )
                full_order = {
                    **result["order"],
                    "items": display_items,
                    "derived_specs": processed.get("derived_specs", []),
                }
                return msgpack_response({
                    "type": "metric_warning",
                    "message": processed["metric_warning"]["message"],
                    "metric_count": processed["metric_warning"]["count"],
                    "pending_order": full_order,
                    "full_order": processed,
                    "summary": result.get("summary"),
                })

            # Get display items (filtered for user view - hides for_derivation items, adds derived specs)
            display_items = get_display_items(
                processed.get("items", []),
                processed.get("derived_specs", [])
            )

            # Return order for UI confirmation
            return msgpack_response({
                "type": "order",
                "order": {
                    **result["order"],
                    "items": display_items,
                    "derived_specs": processed.get("derived_specs", []),
                },
                "full_order": processed,  # Full order for execution
                "summary": result["summary"],
                "validation_summary": processed.get("validation_summary"),
                "all_valid": processed.get("all_valid", True)
            })

        elif result["type"] == "navigate":
            # LLM decided this is a navigation request (Phase 4 - post-LLM routing)
            locations = result.get("locations", [])
            loc_ids = [loc.get("loc_id") for loc in locations if loc.get("loc_id")]
            message = result.get("message", f"Showing {len(locations)} location(s)")

            logger.debug(f"LLM navigation: {len(locations)} locations")

            return msgpack_response({
                "type": "navigate",
                "message": message,
                "locations": locations,
                "loc_ids": loc_ids,
                "original_query": query,
                "geojson": {"type": "FeatureCollection", "features": []},
            })

        elif result["type"] == "disambiguate":
            # LLM decided disambiguation is needed (Phase 4 - post-LLM routing)
            options = result.get("options", [])
            message = result.get("message", f"Multiple locations found. Please select one.")

            logger.debug(f"LLM disambiguation: {len(options)} options")

            return msgpack_response({
                "type": "disambiguate",
                "message": message,
                "query_term": result.get("query_term", "location"),
                "original_query": query,
                "options": options,
                "geojson": {"type": "FeatureCollection", "features": []},
            })

        elif result["type"] == "filter_update":
            # LLM decided to update overlay filters (Phase 4 - post-LLM routing)
            overlay = result.get("overlay", "")
            filters = result.get("filters", {})
            message = result.get("message", f"Updating {overlay} filters")

            logger.debug(f"LLM filter update: {overlay} -> {filters}")

            return msgpack_response({
                "type": "filter_update",
                "overlay": overlay,
                "filters": filters,
                "message": message,
            })

        elif result["type"] == "overlay_toggle":
            # LLM decided to toggle an overlay on/off (Phase 4 - post-LLM routing)
            overlay = result.get("overlay", "")
            enabled = result.get("enabled", True)
            action = "Enabling" if enabled else "Disabling"
            message = result.get("message", f"{action} {overlay} overlay")

            logger.debug(f"LLM overlay toggle: {overlay} -> {enabled}")

            return msgpack_response({
                "type": "overlay_toggle",
                "overlay": overlay,
                "enabled": enabled,
                "message": message,
            })

        elif result["type"] == "clarify":
            # Need more information from user
            return msgpack_response({
                "type": "clarify",
                "message": result["message"],
                "geojson": {"type": "FeatureCollection", "features": []},
                "needsMoreInfo": True
            })

        else:
            # General chat response (not a data request)
            return msgpack_response({
                "type": "chat",
                "message": result.get("message", "I'm not sure how to help with that."),
                "geojson": {"type": "FeatureCollection", "features": []},
                "needsMoreInfo": False
            })

    except Exception as e:
        logger.error(f"Chat error: {e}")
        traceback.print_exc()
        return msgpack_response({
            "type": "error",
            "message": "Sorry, I encountered an error. Please try again.",
            "geojson": {"type": "FeatureCollection", "features": []},
            "error": str(e)
        }, status_code=500)


# === Chat Streaming Endpoint (Phase 1 - Progress Updates) ===

@app.post("/chat/stream")
async def chat_stream_endpoint(req: Request):
    """
    Streaming chat endpoint - sends progress updates via SSE.

    Sends events:
    - stage: analyzing (preprocessor running)
    - stage: thinking (LLM call in progress)
    - stage: preparing (postprocessor for orders)
    - stage: complete (final result)
    """
    import asyncio
    import time

    # Parse request body - streaming uses JSON (not msgpack) for browser compatibility
    t_start = time.time()
    body_bytes = await req.body()
    try:
        body = json.loads(body_bytes.decode('utf-8'))
    except json.JSONDecodeError:
        # Fallback to msgpack if JSON fails
        body = msgpack.unpackb(body_bytes, raw=False)
    t_parse = time.time()
    logger.debug(f"[TIMING] Body parse: {(t_parse - t_start)*1000:.0f}ms")

    async def generate_events():
        try:

            # Check if this is a confirmed order execution (no streaming needed)
            if body.get("confirmed_order"):
                yield f"data: {json.dumps({'stage': 'fetching', 'message': 'Fetching data...'})}\n\n"
                try:
                    result = execute_order(body["confirmed_order"])
                    response = {
                        "type": "data",
                        "geojson": result["geojson"],
                        "summary": result["summary"],
                        "count": result["count"],
                        "sources": result.get("sources", [])
                    }
                    if result.get("multi_year"):
                        response["multi_year"] = True
                        response["year_data"] = result["year_data"]
                        response["year_range"] = result["year_range"]
                        response["metric_key"] = result.get("metric_key")
                        response["available_metrics"] = result.get("available_metrics", [])
                        response["metric_year_ranges"] = result.get("metric_year_ranges", {})
                    yield f"data: {json.dumps({'stage': 'complete', 'result': response})}\n\n"
                except Exception as e:
                    logger.error(f"Order execution error: {e}")
                    yield f"data: {json.dumps({'stage': 'complete', 'result': {'type': 'error', 'message': str(e)}})}\n\n"
                return

            # Extract request parameters
            query = body.get("query", "")
            chat_history = body.get("chatHistory", [])
            viewport = body.get("viewport")
            resolved_location = body.get("resolved_location")
            active_overlays = body.get("activeOverlays")
            cache_stats = body.get("cacheStats")
            time_state = body.get("timeState")  # {isLiveLocked, currentTime, timezone, ...}
            saved_order_names = body.get("savedOrderNames", [])  # Phase 7: Saved order names

            if not query:
                yield f"data: {json.dumps({'stage': 'complete', 'result': {'type': 'error', 'message': 'No query provided'}})}\n\n"
                return

            # Stage 1: Analyzing (preprocessor)
            t_preprocess_start = time.time()
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': 'Analyzing your request...'})}\n\n"
            await asyncio.sleep(0)  # Allow event to flush

            hints = preprocess_query(query, viewport=viewport, active_overlays=active_overlays, cache_stats=cache_stats, saved_order_names=saved_order_names, time_state=time_state)
            t_preprocess_end = time.time()
            logger.info(f"[TIMING] Preprocessing: {(t_preprocess_end - t_preprocess_start)*1000:.0f}ms")

            # Handle resolved location
            if resolved_location:
                hints["location"] = {
                    "matched_term": resolved_location.get("matched_term"),
                    "iso3": resolved_location.get("iso3"),
                    "country_name": resolved_location.get("country_name"),
                    "loc_id": resolved_location.get("loc_id"),
                    "is_subregion": resolved_location.get("loc_id") != resolved_location.get("iso3"),
                    "source": "disambiguation_selection"
                }
                hints["disambiguation"] = None

            # Check for show_borders (early return)
            if hints.get("show_borders"):
                previous_options = body.get("previous_disambiguation_options", [])
                if previous_options:
                    loc_ids_to_show = [opt.get("loc_id") for opt in previous_options if opt.get("loc_id")]
                    if loc_ids_to_show:
                        from mapmover.data_loading import fetch_geometries_by_loc_ids
                        geojson = fetch_geometries_by_loc_ids(loc_ids_to_show)
                        result = {
                            "type": "navigate",
                            "message": f"Showing {len(loc_ids_to_show)} locations on the map.",
                            "locations": previous_options,
                            "loc_ids": loc_ids_to_show,
                            "original_query": query,
                            "geojson": geojson,
                        }
                        yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                        return
                result = {"type": "chat", "reply": "I don't have a list of locations to display."}
                yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                return

            # Check for drill-down (early return)
            navigation = hints.get("navigation")
            if navigation and navigation.get("is_navigation"):
                locations = navigation.get("locations", [])
                if len(locations) == 1 and locations[0].get("drill_to_level"):
                    loc = locations[0]
                    result = {
                        "type": "drilldown",
                        "message": f"Showing {loc.get('drill_to_level')} of {loc.get('matched_term', loc.get('loc_id'))}...",
                        "loc_id": loc.get("loc_id"),
                        "name": loc.get("matched_term", loc.get("loc_id")),
                        "drill_to_level": loc.get("drill_to_level"),
                        "original_query": query,
                    }
                    yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                    return

            # Stage 2: Thinking (LLM call)
            t_llm_start = time.time()
            yield f"data: {json.dumps({'stage': 'thinking', 'message': 'Understanding your intent...'})}\n\n"
            await asyncio.sleep(0)

            result = interpret_request(query, chat_history, hints=hints)
            t_llm_end = time.time()
            logger.info(f"[TIMING] LLM call: {(t_llm_end - t_llm_start)*1000:.0f}ms")

            # Stage 3: Preparing (postprocessor for orders)
            if result["type"] == "order":
                t_post_start = time.time()
                yield f"data: {json.dumps({'stage': 'preparing', 'message': 'Preparing your order...'})}\n\n"
                await asyncio.sleep(0)

                processed = postprocess_order(result["order"], hints)
                display_items = get_display_items(
                    processed.get("items", []),
                    processed.get("derived_specs", [])
                )
                final_result = {
                    "type": "order",
                    "order": {
                        **result["order"],
                        "items": display_items,
                        "derived_specs": processed.get("derived_specs", []),
                    },
                    "full_order": processed,
                    "summary": result["summary"],
                    "validation_summary": processed.get("validation_summary"),
                    "all_valid": processed.get("all_valid", True)
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            elif result["type"] == "navigate":
                locations = result.get("locations", [])
                loc_ids = [loc.get("loc_id") for loc in locations if loc.get("loc_id")]
                final_result = {
                    "type": "navigate",
                    "message": result.get("message", f"Showing {len(locations)} location(s)"),
                    "locations": locations,
                    "loc_ids": loc_ids,
                    "original_query": query,
                    "geojson": {"type": "FeatureCollection", "features": []},
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            elif result["type"] == "disambiguate":
                final_result = {
                    "type": "disambiguate",
                    "message": result.get("message", "Multiple locations found. Please select one."),
                    "query_term": result.get("query_term", "location"),
                    "original_query": query,
                    "options": result.get("options", []),
                    "geojson": {"type": "FeatureCollection", "features": []},
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            elif result["type"] == "filter_update":
                final_result = {
                    "type": "filter_update",
                    "overlay": result.get("overlay", ""),
                    "filters": result.get("filters", {}),
                    "message": result.get("message", "Updating filters"),
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            elif result["type"] == "overlay_toggle":
                final_result = {
                    "type": "overlay_toggle",
                    "overlay": result.get("overlay", ""),
                    "enabled": result.get("enabled", True),
                    "message": result.get("message", "Toggling overlay"),
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            elif result["type"] == "clarify":
                final_result = {
                    "type": "clarify",
                    "message": result["message"],
                    "geojson": {"type": "FeatureCollection", "features": []},
                    "needsMoreInfo": True
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

            else:
                final_result = {
                    "type": "chat",
                    "message": result.get("message", "I'm not sure how to help with that."),
                    "geojson": {"type": "FeatureCollection", "features": []},
                    "needsMoreInfo": False
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"

        except Exception as e:
            logger.error(f"Chat stream error: {e}")
            traceback.print_exc()
            error_result = {
                "type": "error",
                "message": "Sorry, I encountered an error. Please try again.",
                "geojson": {"type": "FeatureCollection", "features": []},
                "error": str(e)
            }
            yield f"data: {json.dumps({'stage': 'complete', 'result': error_result})}\n\n"

    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


# === Order Queue Endpoints ===

@app.post("/api/orders/queue")
async def queue_order_endpoint(req: Request):
    """
    Add an order to the processing queue.

    Request body:
        items: List of validated order items
        hints: Preprocessor hints
        session_id: Optional session identifier

    Returns:
        queue_id: ID for polling status
        status: "queued"
        position: Position in queue
    """
    try:
        body = await decode_request_body(req)
        items = body.get("items", [])
        hints = body.get("hints", {})
        session_id = body.get("session_id", "default")

        if not items:
            return msgpack_error("No items provided", 400)

        queue_id = order_queue.add(items, hints, session_id)
        order = order_queue.get(queue_id)

        return msgpack_response({
            "queue_id": queue_id,
            "status": "queued",
            "position": order.position if order else 0,
            "message": order.message if order else "Queued"
        })

    except Exception as e:
        logger.error(f"Error queueing order: {e}")
        return msgpack_error(str(e), 500)


@app.post("/api/orders/status")
async def get_order_status_endpoint(req: Request):
    """
    Get status of one or more queued orders.

    Request body:
        queue_ids: List of queue IDs to check

    Returns:
        Dict mapping queue_id -> status info
    """
    try:
        body = await decode_request_body(req)
        queue_ids = body.get("queue_ids", [])

        if not queue_ids:
            return msgpack_error("No queue_ids provided", 400)

        statuses = {}
        for qid in queue_ids:
            status = order_queue.get_status(qid)
            if status:
                statuses[qid] = status
            else:
                statuses[qid] = {"error": "Not found", "status": "not_found"}

        return msgpack_response(statuses)

    except Exception as e:
        logger.error(f"Error getting order status: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/orders/status/{queue_id}")
async def get_single_order_status_endpoint(queue_id: str):
    """
    Get status of a single queued order.

    Returns status info including result when complete.
    """
    try:
        status = order_queue.get_status(queue_id)
        if not status:
            return msgpack_error("Order not found", 404)

        return msgpack_response(status)

    except Exception as e:
        logger.error(f"Error getting order status: {e}")
        return msgpack_error(str(e), 500)


@app.post("/api/orders/cancel")
async def cancel_order_endpoint(req: Request):
    """
    Cancel a pending order.

    Request body:
        queue_id: ID of order to cancel

    Returns:
        cancelled: Boolean success status
        reason: Error message if failed
    """
    try:
        body = await decode_request_body(req)
        queue_id = body.get("queue_id")

        if not queue_id:
            return msgpack_error("No queue_id provided", 400)

        cancelled = order_queue.cancel(queue_id)

        if cancelled:
            return msgpack_response({"cancelled": True})
        else:
            return msgpack_response({
                "cancelled": False,
                "reason": "Order not found or already processing"
            })

    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/orders/session/{session_id}")
async def get_session_orders_endpoint(session_id: str):
    """
    Get all orders for a session.

    Returns list of order status objects.
    """
    try:
        orders = order_queue.get_session_orders(session_id)
        return msgpack_response({"orders": orders})

    except Exception as e:
        logger.error(f"Error getting session orders: {e}")
        return msgpack_error(str(e), 500)


# === Session Cache Management ===

@app.post("/api/session/clear")
async def clear_session_endpoint(req: Request):
    """
    Clear session cache (for "New Chat" functionality).

    Request body: { sessionId: string }
    """
    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId")

        if not session_id:
            return msgpack_error("sessionId required", 400)

        # Clear session from session manager
        cleared = session_manager.clear_session(session_id)

        if cleared:
            logger.info(f"Cleared session cache: {session_id}")
            return msgpack_response({"status": "cleared", "sessionId": session_id})
        else:
            return msgpack_response({"status": "not_found", "sessionId": session_id})

    except Exception as e:
        logger.error(f"Error clearing session: {e}")
        return msgpack_error(str(e), 500)


@app.post("/api/session/clear-source")
async def clear_session_source_endpoint(req: Request):
    """
    Clear a specific source from session cache.
    Called when user clicks X on a source in the Loaded tab.

    Request body: { sessionId: string, sourceId: string }
    """
    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId")
        source_id = body.get("sourceId")

        if not session_id or not source_id:
            return msgpack_error("sessionId and sourceId required", 400)

        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_response({"status": "not_found", "sessionId": session_id})

        removed = cache.clear_source(source_id)
        logger.info(f"Cleared source '{source_id}' from session {session_id}: {removed} keys removed")
        return msgpack_response({
            "status": "cleared",
            "sourceId": source_id,
            "keys_removed": removed,
        })

    except Exception as e:
        logger.error(f"Error clearing session source: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/session/{session_id}/status")
async def get_session_status_endpoint(session_id: str):
    """
    Get session status for recovery prompt.

    Returns whether session has cached data and summary info.
    """
    try:
        cache = session_manager.get(session_id)

        if cache:
            status = cache.get_status()
            status["cached_results"] = len(cache._results)
            status["inventory"] = {
                "total_locations": status.get("total_locations", 0),
                "total_metrics": status.get("total_metrics", 0),
            }
            return msgpack_response({
                "exists": True,
                **status
            })
        else:
            return msgpack_response({
                "exists": False,
                "session_id": session_id
            })

    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/cache/inventory/{session_id}")
async def get_cache_inventory_endpoint(session_id: str):
    """
    Get detailed cache inventory for a session.

    Returns what data has been loaded (loc_ids, years, metrics).
    """
    try:
        cache = session_manager.get(session_id)

        if not cache:
            return msgpack_response({
                "exists": False,
                "session_id": session_id
            })

        # Get inventory stats
        inventory_stats = cache.inventory.stats()

        # Get combined signature for overall view
        combined = cache.inventory.combined_signature()

        return msgpack_response({
            "exists": True,
            "session_id": session_id,
            "inventory": {
                "entry_count": inventory_stats["entry_count"],
                "total_locations": inventory_stats["total_locations"],
                "total_years": inventory_stats["total_years"],
                "total_metrics": inventory_stats["total_metrics"],
                "year_range": inventory_stats["year_range"],
            },
            "combined_signature": {
                "loc_id_count": len(combined.loc_ids),
                "year_count": len(combined.years),
                "metric_count": len(combined.metrics),
                "years": sorted(combined.years) if combined.years else [],
                "metrics": sorted(combined.metrics) if combined.metrics else [],
            },
            "cached_results": len(cache._results),
        })

    except Exception as e:
        logger.error(f"Error getting cache inventory: {e}")
        return msgpack_error(str(e), 500)


@app.post("/api/cache/delta")
async def compute_cache_delta_endpoint(req: Request):
    """
    Compute what data needs to be fetched given what's already cached.

    Request body: {
        sessionId: string,
        want: {loc_ids: [...], years: [...], metrics: [...]}
    }

    Returns: {
        need_fetch: bool,
        delta: {loc_ids: [...], years: [...], metrics: [...]},
        have: {loc_ids: [...], years: [...], metrics: [...]}
    }
    """
    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId", "anonymous")
        want = body.get("want", {})

        if not want:
            return msgpack_error("'want' field required", 400)

        # Build requested signature
        from mapmover import CacheSignature

        requested = CacheSignature(
            loc_ids=frozenset(want.get("loc_ids", [])),
            years=frozenset(want.get("years", [])),
            metrics=frozenset(want.get("metrics", []))
        )

        # Get session cache
        cache = session_manager.get(session_id)

        if not cache:
            # No cache - need to fetch everything
            return msgpack_response({
                "need_fetch": True,
                "delta": {
                    "loc_ids": list(requested.loc_ids),
                    "years": sorted(requested.years),
                    "metrics": list(requested.metrics),
                },
                "have": {
                    "loc_ids": [],
                    "years": [],
                    "metrics": [],
                }
            })

        # Check if cache can serve
        can_serve = cache.can_serve(requested)

        if can_serve:
            return msgpack_response({
                "need_fetch": False,
                "delta": {
                    "loc_ids": [],
                    "years": [],
                    "metrics": [],
                },
                "have": {
                    "loc_ids": list(requested.loc_ids),
                    "years": sorted(requested.years),
                    "metrics": list(requested.metrics),
                }
            })

        # Compute what's missing
        delta = cache.compute_delta(requested)
        combined = cache.inventory.combined_signature()

        return msgpack_response({
            "need_fetch": True,
            "delta": {
                "loc_ids": list(delta.loc_ids),
                "years": sorted(delta.years),
                "metrics": list(delta.metrics),
            },
            "have": {
                "loc_ids": list(combined.loc_ids),
                "years": sorted(combined.years),
                "metrics": list(combined.metrics),
            }
        })

    except Exception as e:
        logger.error(f"Error computing cache delta: {e}")
        return msgpack_error(str(e), 500)


@app.post("/api/cache/export")
async def export_cache_endpoint(req: Request):
    """
    Export cached data as CSV.

    Request body: {
        sessionId: string,
        format: "csv" (default) | "json",
        filters: {loc_ids: [...], years: [...], metrics: [...]} (optional)
    }

    Returns: CSV file download or JSON data
    """
    import csv
    import io

    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId", "anonymous")
        export_format = body.get("format", "csv")
        filters = body.get("filters", {})

        # Get session cache
        cache = session_manager.get(session_id)

        if not cache:
            return msgpack_error("Session not found", 404)

        # Collect all cached results
        all_rows = []
        for request_key, result in cache._results.items():
            geojson = result.get("geojson", {})
            features = geojson.get("features", [])

            for feature in features:
                props = feature.get("properties", {})

                # Apply filters if provided
                if filters.get("loc_ids"):
                    if props.get("loc_id") not in filters["loc_ids"]:
                        continue

                if filters.get("years"):
                    year = props.get("year")
                    if year is not None and int(year) not in filters["years"]:
                        continue

                # Flatten properties for CSV
                row = {}
                for key, value in props.items():
                    # Skip geometry-related fields
                    if key in ["geometry", "type"]:
                        continue

                    # Filter by metrics if specified
                    if filters.get("metrics"):
                        non_metric_keys = {"loc_id", "year", "name", "country", "admin_level", "parent_id", "iso3"}
                        if key not in non_metric_keys and key not in filters["metrics"]:
                            continue

                    if isinstance(value, (dict, list)):
                        row[key] = json.dumps(value)
                    else:
                        row[key] = value

                all_rows.append(row)

        if not all_rows:
            return msgpack_error("No data in cache", 404)

        if export_format == "json":
            return msgpack_response({
                "format": "json",
                "row_count": len(all_rows),
                "data": all_rows
            })

        # CSV format
        # Get all column names from all rows
        columns = set()
        for row in all_rows:
            columns.update(row.keys())

        # Order columns: loc_id, year, name first, then alphabetical
        priority_cols = ["loc_id", "year", "name", "country", "admin_level"]
        ordered_cols = [c for c in priority_cols if c in columns]
        ordered_cols += sorted(c for c in columns if c not in priority_cols)

        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=ordered_cols, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_rows)
        csv_content = output.getvalue()

        # Return as downloadable file
        return Response(
            content=csv_content.encode('utf-8'),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=export_{session_id[:8]}.csv"
            }
        )

    except Exception as e:
        logger.error(f"Error exporting cache: {e}")
        return msgpack_error(str(e), 500)


@app.get("/api/orders/stats")
async def get_queue_stats_endpoint():
    """
    Get queue statistics (for monitoring/debugging).

    Returns count by status and pending count.
    """
    try:
        stats = order_queue.stats()
        return msgpack_response(stats)

    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        return msgpack_error(str(e), 500)


# === Main Entry Point ===

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7000)
