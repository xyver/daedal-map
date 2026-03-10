"""Geometry API router endpoints."""

import msgpack
from fastapi import APIRouter, Request

from mapmover import logger
from mapmover.geometry_handlers import (
    clear_cache as clear_geometry_cache,
    get_countries_geometry as get_countries_geometry_handler,
    get_location_children as get_location_children_handler,
    get_location_info,
    get_location_places as get_location_places_handler,
    get_selection_geometries as get_selection_geometries_handler,
    get_viewport_geometry as get_viewport_geometry_handler,
)
from mapmover.routes.disasters.helpers import msgpack_error, msgpack_response


router = APIRouter()


async def decode_request_body(request: Request) -> dict:
    """Decode MessagePack request body."""
    body_bytes = await request.body()
    return msgpack.unpackb(body_bytes, raw=False)


@router.get("/geometry/countries")
async def get_countries_geometry_endpoint(debug: bool = False):
    """Get all country geometries for initial map display."""
    try:
        result = get_countries_geometry_handler(debug=debug)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/countries: {e}")
        return msgpack_error(str(e), 500)


@router.get("/geometry/{loc_id}/children")
async def get_location_children_endpoint(loc_id: str):
    """Get child geometries for a location drill-down."""
    try:
        result = get_location_children_handler(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/children: {e}")
        return msgpack_error(str(e), 500)


@router.get("/geometry/{loc_id}/places")
async def get_location_places_endpoint(loc_id: str):
    """Get place points for a location as a separate overlay layer."""
    try:
        result = get_location_places_handler(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/places: {e}")
        return msgpack_error(str(e), 500)


@router.get("/geometry/{loc_id}/info")
async def get_location_info_endpoint(loc_id: str):
    """Get metadata about a specific location."""
    try:
        result = get_location_info(loc_id)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/{loc_id}/info: {e}")
        return msgpack_error(str(e), 500)


@router.get("/geometry/viewport")
async def get_viewport_geometry_endpoint(level: int = 0, bbox: str = None, debug: bool = False):
    """Get geometry features that intersect the viewport bounding box."""
    try:
        if bbox:
            parts = [float(x) for x in bbox.split(",")]
            if len(parts) != 4:
                return msgpack_error("bbox must be minLon,minLat,maxLon,maxLat", 400)
            bbox_tuple = tuple(parts)
        else:
            bbox_tuple = (-180, -90, 180, 90)

        result = get_viewport_geometry_handler(level, bbox_tuple, debug=debug)
        return msgpack_response(result)
    except Exception as e:
        logger.error(f"Error in /geometry/viewport: {e}")
        return msgpack_error(str(e), 500)


@router.post("/geometry/cache/clear")
async def clear_geometry_cache_endpoint():
    """Clear the geometry cache after data updates."""
    try:
        clear_geometry_cache()
        return msgpack_response({"message": "Geometry cache cleared"})
    except Exception as e:
        logger.error(f"Error clearing geometry cache: {e}")
        return msgpack_error(str(e), 500)


@router.post("/geometry/selection")
async def get_selection_geometry_endpoint(req: Request):
    """Get geometries for specific loc_ids for selection/disambiguation mode."""
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
