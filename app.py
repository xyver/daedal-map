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
import logging
from pathlib import Path

# Force UTF-8 encoding for all output streams
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import msgpack
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
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
    GLOBAL_DIR,
    # Session cache
    session_manager,
)

from mapmover.order_executor import execute_order

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
from mapmover.routes.disasters.earthquakes import router as earthquakes_router
from mapmover.routes.disasters.volcanoes import router as volcanoes_router
from mapmover.routes.disasters.landslides import router as landslides_router
from mapmover.routes.disasters.tsunamis import router as tsunamis_router
from mapmover.routes.disasters.hurricanes import router as hurricanes_router
from mapmover.routes.disasters.tornadoes import router as tornadoes_router
from mapmover.routes.disasters.floods import router as floods_router
from mapmover.routes.disasters.drought import router as drought_router
from mapmover.routes.disasters.wildfires import router as wildfires_router
from mapmover.routes.weather import router as weather_router
from mapmover.routes.chat import router as chat_router


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
app.include_router(earthquakes_router)
app.include_router(volcanoes_router)
app.include_router(landslides_router)
app.include_router(tsunamis_router)
app.include_router(hurricanes_router)
app.include_router(tornadoes_router)
app.include_router(floods_router)
app.include_router(drought_router)
app.include_router(wildfires_router)
app.include_router(weather_router)
app.include_router(chat_router)


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
