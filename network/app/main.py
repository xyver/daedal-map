from __future__ import annotations

import json
import math
import os
import heapq
from pathlib import Path
from typing import Any
from functools import lru_cache
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from shapely.geometry import LineString, Point, shape
from shapely.ops import nearest_points
from shapely.strtree import STRtree

from .scenarios import STATES, ScenarioStore


DATA_DIR = Path(os.environ.get("ROAD_EDITOR_DATA_DIR", Path(__file__).parents[1] / "data"))
BASELINE_DIR = DATA_DIR / "baseline"
STATIC_DIR = Path(__file__).parent / "static"
store = ScenarioStore(DATA_DIR)
app = FastAPI(title="Lake County Road Network Editor", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

FEMA_OPEN_SHELTERS_URL = "https://gis.fema.gov/arcgis/rest/services/NSS/OpenShelters/FeatureServer/0/query"
LIVE_SHELTER_CACHE_SECONDS = 120
START_POINT_SNAP_MAX_METERS = 150
_live_shelter_cache: dict[str, Any] = {"expires_at": 0.0, "payload": None}


class RoutePoint(BaseModel):
    id: str = ""
    name: str = "Unnamed shelter"
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)


class RouteRequest(BaseModel):
    origin: RoutePoint
    shelters: list[RoutePoint] = Field(default_factory=list, max_length=500)
    max_minutes: float = Field(default=60, gt=0, le=720)


class SaveScenarioRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    start_point: RoutePoint | None = None


def _baseline_path(filename: str) -> Path:
    path = BASELINE_DIR / filename
    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail="Lake baseline is not prepared. Run the documented prepare_lake_network.py Docker command first.",
        )
    return path


def _haversine_meters(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (*a, *b))
    dlon, dlat = lon2 - lon1, lat2 - lat1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371008.8 * 2 * math.asin(math.sqrt(value))


@lru_cache(maxsize=1)
def _graph() -> dict[str, Any]:
    return json.loads(_baseline_path("graph.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _segment_geometries() -> dict[str, dict[str, Any]]:
    """Index each editable segment geometry once for route display responses."""
    geometries: dict[str, dict[str, Any]] = {}
    for path in (BASELINE_DIR / "road_tiles").glob("*.geojson"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for feature in payload.get("features", []):
            segment_id = feature.get("properties", {}).get("segment_id")
            if segment_id:
                geometries[str(segment_id)] = {
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": {"segment_id": str(segment_id)},
                }
    return geometries


@lru_cache(maxsize=1)
def _road_spatial_index() -> tuple[list[LineString], STRtree]:
    lines = [LineString(feature["geometry"]["coordinates"]) for feature in _segment_geometries().values()]
    return lines, STRtree(lines)


def _snap_to_road(longitude: float, latitude: float) -> dict[str, Any]:
    point = Point(longitude, latitude)
    lines, index = _road_spatial_index()
    nearest_index = int(index.nearest(point))
    snapped_point = nearest_points(point, lines[nearest_index])[1]
    snapped_longitude, snapped_latitude = snapped_point.x, snapped_point.y
    distance_meters = _haversine_meters((longitude, latitude), (snapped_longitude, snapped_latitude))
    return {
        "longitude": snapped_longitude,
        "latitude": snapped_latitude,
        "distance_meters": round(distance_meters, 1),
        "max_distance_meters": START_POINT_SNAP_MAX_METERS,
        "can_set_start": distance_meters <= START_POINT_SNAP_MAX_METERS,
    }


def _nearest_node(nodes: dict[str, list[float]], longitude: float, latitude: float) -> tuple[str, float]:
    point = (longitude, latitude)
    node_id, coordinate = min(nodes.items(), key=lambda item: _haversine_meters(point, tuple(item[1])))
    return node_id, _haversine_meters(point, tuple(coordinate))


def _route_minutes(request: RouteRequest, scenario_id: str) -> dict[str, Any]:
    graph = _graph()
    nodes: dict[str, list[float]] = graph["nodes"]
    scenario_states = store.get(scenario_id)
    adjacency: dict[str, list[tuple[str, float, str]]] = {}
    for edge in graph["edges"]:
        state = scenario_states.get(edge["segment_id"], "open")
        if state == "closed":
            continue
        if state == "forward_only" and edge["direction"] != "forward":
            continue
        if state == "reverse_only" and edge["direction"] != "reverse":
            continue
        adjacency.setdefault(edge["from"], []).append((edge["to"], float(edge["seconds"]), edge["segment_id"]))

    origin_node, origin_snap_meters = _nearest_node(nodes, request.origin.longitude, request.origin.latitude)
    limit_seconds = request.max_minutes * 60
    distances = {origin_node: 0.0}
    predecessors: dict[str, tuple[str, str]] = {}
    queue: list[tuple[float, str]] = [(0.0, origin_node)]
    while queue:
        seconds, node_id = heapq.heappop(queue)
        if seconds != distances.get(node_id) or seconds > limit_seconds:
            continue
        for neighbor_id, edge_seconds, segment_id in adjacency.get(node_id, []):
            candidate = seconds + edge_seconds
            if candidate <= limit_seconds and candidate < distances.get(neighbor_id, math.inf):
                distances[neighbor_id] = candidate
                predecessors[neighbor_id] = (node_id, segment_id)
                heapq.heappush(queue, (candidate, neighbor_id))

    results = []
    for shelter in request.shelters:
        node_id, snap_meters = _nearest_node(nodes, shelter.longitude, shelter.latitude)
        seconds = distances.get(node_id)
        segment_ids: list[str] = []
        cursor = node_id
        while cursor != origin_node and cursor in predecessors:
            cursor, segment_id = predecessors[cursor]
            segment_ids.append(segment_id)
        segment_ids.reverse()
        results.append({
            "id": shelter.id,
            "name": shelter.name,
            "longitude": shelter.longitude,
            "latitude": shelter.latitude,
            "minutes": round(seconds / 60, 1) if seconds is not None else None,
            "reachable": seconds is not None,
            "snap_distance_meters": round(snap_meters, 1),
            "segment_ids": segment_ids if seconds is not None else [],
        })
    results.sort(key=lambda item: (not item["reachable"], item["minutes"] if item["minutes"] is not None else math.inf, item["name"]))
    route_segment_ids = {segment_id for result in results if result["reachable"] for segment_id in result["segment_ids"]}
    geometry_index = _segment_geometries()
    return {
        "origin_snap_distance_meters": round(origin_snap_meters, 1),
        "max_minutes": request.max_minutes,
        "total_shelters": len(results),
        "reachable_shelters": sum(result["reachable"] for result in results),
        "results": results,
        "route_features": [geometry_index[segment_id] for segment_id in route_segment_ids if segment_id in geometry_index],
    }


def _live_shelter_payload() -> dict[str, Any]:
    """Retrieve the current FEMA Open Shelters feed for the editable road scope."""
    now = datetime.now(timezone.utc)
    now_timestamp = now.timestamp()
    cached = _live_shelter_cache["payload"]
    if cached and now_timestamp < _live_shelter_cache["expires_at"]:
        return cached

    query = urlencode({
        "where": "state = 'CA'",
        "outFields": ",".join((
            "shelter_id", "shelter_name", "address", "city", "state", "zip",
            "shelter_status", "evacuation_capacity", "post_impact_capacity",
            "total_population", "hours_open", "hours_close", "org_name",
            "ada_compliant", "pet_accommodations_code", "wheelchair_accessible",
        )),
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "geojson",
    })
    try:
        with urlopen(f"{FEMA_OPEN_SHELTERS_URL}?{query}", timeout=20) as response:
            source = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail="Could not retrieve FEMA Open Shelters right now.") from exc

    scope = json.loads(_baseline_path("scope.json").read_text(encoding="utf-8"))
    scope_geometry = shape(scope["scope_geometry"])
    features: list[dict[str, Any]] = []
    for feature in source.get("features", []):
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates")
        if not coordinates or len(coordinates) < 2:
            continue
        longitude, latitude = coordinates[:2]
        if not scope_geometry.covers(Point(longitude, latitude)):
            continue
        properties = feature.get("properties") or {}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
            "properties": {
                "id": str(properties.get("shelter_id") or properties.get("objectid") or ""),
                "name": properties.get("shelter_name") or "Unnamed shelter",
                "status": properties.get("shelter_status") or "Open",
                "is_currently_open": True,
                "address": properties.get("address"),
                "city": properties.get("city"),
                "state": properties.get("state"),
                "capacity": properties.get("evacuation_capacity") or properties.get("post_impact_capacity"),
                "population": properties.get("total_population"),
                "hours_open": properties.get("hours_open"),
                "hours_close": properties.get("hours_close"),
            },
        })
    payload = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "FEMA Open Shelters",
            "fetched_at": now.isoformat(),
            "cache_seconds": LIVE_SHELTER_CACHE_SECONDS,
        },
    }
    _live_shelter_cache["payload"] = payload
    _live_shelter_cache["expires_at"] = now_timestamp + LIVE_SHELTER_CACHE_SECONDS
    return payload


@lru_cache(maxsize=256)
def _load_tile(tile_x: int, tile_y: int) -> dict[str, Any]:
    path = BASELINE_DIR / "road_tiles" / f"{tile_x}_{tile_y}.geojson"
    if not path.exists():
        return {"type": "FeatureCollection", "features": []}
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "baseline_ready": (BASELINE_DIR / "graph.json").exists(),
        "scope": "Lake County plus a small cross-border buffer",
        "cycle": list(STATES),
    }


@app.get("/api/saved-scenarios")
def saved_scenarios() -> dict[str, Any]:
    return {"scenarios": store.list_saved()}


@app.get("/api/scenarios/{scenario_id}")
def get_scenario(scenario_id: str) -> dict[str, Any]:
    return {"scenario_id": scenario_id, "segment_states": store.get(scenario_id), "shelter_changes": store.shelter_changes(scenario_id), "cycle": list(STATES)}


@app.post("/api/scenarios/{scenario_id}/save")
def save_scenario(scenario_id: str, request: SaveScenarioRequest) -> dict[str, Any]:
    try:
        return store.save_snapshot(scenario_id, request.name, request.start_point.model_dump() if request.start_point else None)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/scenarios/{scenario_id}/load/{saved_name}")
def load_scenario(scenario_id: str, saved_name: str) -> dict[str, Any]:
    try:
        return store.load_snapshot(scenario_id, saved_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/scenarios/{scenario_id}/shelters")
def get_scenario_shelters(scenario_id: str) -> dict[str, Any]:
    return {"scenario_id": scenario_id, **store.shelter_changes(scenario_id)}


@app.post("/api/scenarios/{scenario_id}/shelters")
def add_scenario_shelter(scenario_id: str, shelter: RoutePoint) -> dict[str, Any]:
    if not shelter.id:
        raise HTTPException(status_code=422, detail="A shelter id is required")
    changes = store.add_shelter(scenario_id, shelter.model_dump())
    return {"scenario_id": scenario_id, **changes}


@app.post("/api/scenarios/{scenario_id}/shelters/{shelter_id}/remove")
def remove_scenario_shelter(scenario_id: str, shelter_id: str) -> dict[str, Any]:
    try:
        changes = store.remove_shelter(scenario_id, shelter_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"scenario_id": scenario_id, **changes}


@app.post("/api/scenarios/{scenario_id}/shelters/{shelter_id}/reopen")
def reopen_scenario_shelter(scenario_id: str, shelter_id: str) -> dict[str, Any]:
    try:
        changes = store.reopen_shelter(scenario_id, shelter_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"scenario_id": scenario_id, **changes}


@app.post("/api/scenarios/{scenario_id}/shelters/{shelter_id}/delete")
def delete_scenario_shelter(scenario_id: str, shelter_id: str) -> dict[str, Any]:
    try:
        changes = store.delete_added_shelter(scenario_id, shelter_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"scenario_id": scenario_id, **changes}


@app.post("/api/scenarios/{scenario_id}/shelters/reset")
def reset_scenario_shelters(scenario_id: str) -> dict[str, str]:
    store.reset_shelters(scenario_id)
    return {"scenario_id": scenario_id, "status": "reset"}


@app.get("/api/scope")
def scope() -> dict[str, Any]:
    return json.loads(_baseline_path("scope.json").read_text(encoding="utf-8"))


@app.get("/api/shelters")
def shelters() -> dict[str, Any]:
    return json.loads(_baseline_path("shelters.geojson").read_text(encoding="utf-8"))


@app.get("/api/shelters/live")
def live_shelters() -> dict[str, Any]:
    return _live_shelter_payload()


@app.get("/api/study-area")
def study_area() -> dict[str, Any]:
    return json.loads(_baseline_path("study_area.geojson").read_text(encoding="utf-8"))


@app.get("/api/snap")
def snap(longitude: float, latitude: float) -> dict[str, Any]:
    if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
        raise HTTPException(status_code=422, detail="longitude and latitude must be valid coordinates")
    return _snap_to_road(longitude, latitude)


@app.get("/api/network")
def network(bbox: str, zoom: int, scenario_id: str = "default") -> dict[str, Any]:
    try:
        min_lon, min_lat, max_lon, max_lat = (float(value) for value in bbox.split(","))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="bbox must be min_lon,min_lat,max_lon,max_lat") from exc
    if min_lon >= max_lon or min_lat >= max_lat:
        raise HTTPException(status_code=422, detail="bbox must have increasing longitude and latitude")
    scope = json.loads(_baseline_path("scope.json").read_text(encoding="utf-8"))
    tile_size = float(scope["tile_size_degrees"])
    if zoom < 11:
        payload = json.loads(_baseline_path("overview_roads.geojson").read_text(encoding="utf-8"))
    else:
        x_values = range(math.floor(min_lon / tile_size), math.floor(max_lon / tile_size) + 1)
        y_values = range(math.floor(min_lat / tile_size), math.floor(max_lat / tile_size) + 1)
        if len(x_values) * len(y_values) > 100:
            raise HTTPException(status_code=413, detail="Zoom in one level to load local road segments.")
        payload = {"type": "FeatureCollection", "features": []}
        for tile_x in x_values:
            for tile_y in y_values:
                payload["features"].extend(_load_tile(tile_x, tile_y)["features"])
    states = store.get(scenario_id)
    for feature in payload["features"]:
        segment_id = feature["properties"]["segment_id"]
        feature["properties"]["scenario_state"] = states.get(segment_id, "open")
    return payload


@app.post("/api/scenarios/{scenario_id}/segments/{segment_id}/cycle")
def cycle_segment(scenario_id: str, segment_id: str) -> dict[str, str]:
    try:
        state = store.cycle(scenario_id, segment_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"scenario_id": scenario_id, "segment_id": segment_id, "state": state}


@app.post("/api/scenarios/{scenario_id}/clear")
def clear_scenario(scenario_id: str) -> dict[str, str]:
    try:
        store.clear(scenario_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"scenario_id": scenario_id, "status": "cleared"}


@app.post("/api/scenarios/{scenario_id}/routes")
def calculate_routes(scenario_id: str, request: RouteRequest) -> dict[str, Any]:
    return _route_minutes(request, scenario_id)
