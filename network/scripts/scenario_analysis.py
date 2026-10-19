"""Reusable graph analysis primitives for saved Road Network Editor scenarios."""
from __future__ import annotations

import hashlib
import heapq
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from shapely.geometry import Point
from shapely.strtree import STRtree


DEFAULT_DATA_DIR = Path(__file__).parents[1] / "data"


def haversine_meters(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (*a, *b))
    dlon, dlat = lon2 - lon1, lat2 - lat1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371008.8 * 2 * math.asin(math.sqrt(value))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def graph_fingerprint(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def load_active_scenario(data_dir: Path, scenario_id: str = "default") -> dict[str, Any]:
    path = data_dir / "scenarios" / f"{scenario_id}.json"
    if not path.exists():
        return {"segment_states": {}, "shelter_changes": {"added": [], "removed_ids": []}}
    return read_json(path)


def load_scenario(path: Path | None, data_dir: Path, scenario_id: str) -> dict[str, Any]:
    return read_json(path) if path else load_active_scenario(data_dir, scenario_id)


@dataclass
class NodeSnapper:
    node_ids: list[str]
    coordinates: list[tuple[float, float]]
    tree: STRtree

    @classmethod
    def from_graph(cls, graph: dict[str, Any]) -> "NodeSnapper":
        node_ids = list(graph["nodes"].keys())
        coordinates = [tuple(graph["nodes"][node_id]) for node_id in node_ids]
        return cls(node_ids, coordinates, STRtree([Point(value) for value in coordinates]))

    def snap(self, longitude: float, latitude: float) -> tuple[str, float]:
        point = Point(longitude, latitude)
        index = int(self.tree.nearest(point))
        return self.node_ids[index], haversine_meters((longitude, latitude), self.coordinates[index])


def active_shelters(data_dir: Path, scenario: dict[str, Any]) -> list[dict[str, Any]]:
    baseline = read_json(data_dir / "baseline" / "shelters.geojson")
    changes = scenario.get("shelter_changes", {})
    removed = {str(value) for value in changes.get("removed_ids", [])}
    shelters = []
    for feature in baseline.get("features", []):
        props = feature.get("properties", {})
        shelter_id = str(props.get("id", ""))
        if shelter_id and shelter_id not in removed:
            lon, lat = feature["geometry"]["coordinates"]
            shelters.append({"id": shelter_id, "name": props.get("name") or "Unnamed shelter", "longitude": lon, "latitude": lat})
    for shelter in changes.get("added", []):
        shelter_id = str(shelter.get("id", ""))
        if shelter_id and shelter_id not in removed:
            shelters.append({"id": shelter_id, "name": shelter.get("name") or "Unnamed shelter", "longitude": float(shelter["longitude"]), "latitude": float(shelter["latitude"])})
    return shelters


def reverse_adjacency(graph: dict[str, Any], scenario: dict[str, Any]) -> dict[str, list[tuple[str, float]]]:
    states = scenario.get("segment_states") or scenario.get("road_changes") or {}
    adjacency: dict[str, list[tuple[str, float]]] = {}
    for edge in graph["edges"]:
        state = states.get(edge["segment_id"], "open")
        if state == "closed":
            continue
        if state == "forward_only" and edge["direction"] != "forward":
            continue
        if state == "reverse_only" and edge["direction"] != "reverse":
            continue
        # Reverse graph: distance from a node to a shelter in the original graph.
        adjacency.setdefault(edge["to"], []).append((edge["from"], float(edge["seconds"])))
    return adjacency


def nearest_shelter_distances(
    graph: dict[str, Any], scenario: dict[str, Any], shelters: list[dict[str, Any]], max_minutes: float | None = None
) -> tuple[dict[str, tuple[float, str]], dict[str, float]]:
    snapper = NodeSnapper.from_graph(graph)
    adjacency = reverse_adjacency(graph, scenario)
    distances: dict[str, tuple[float, str]] = {}
    shelter_snap_meters: dict[str, float] = {}
    queue: list[tuple[float, str, str]] = []
    for shelter in shelters:
        node_id, snap_meters = snapper.snap(shelter["longitude"], shelter["latitude"])
        shelter_snap_meters[shelter["id"]] = snap_meters
        current = distances.get(node_id)
        if current is None or shelter["id"] < current[1]:
            distances[node_id] = (0.0, shelter["id"])
            heapq.heappush(queue, (0.0, shelter["id"], node_id))
    max_seconds = max_minutes * 60 if max_minutes is not None else math.inf
    while queue:
        seconds, shelter_id, node_id = heapq.heappop(queue)
        if distances.get(node_id) != (seconds, shelter_id) or seconds > max_seconds:
            continue
        for neighbor, edge_seconds in adjacency.get(node_id, []):
            candidate = seconds + edge_seconds
            previous = distances.get(neighbor)
            if candidate <= max_seconds and (previous is None or candidate < previous[0] or (candidate == previous[0] and shelter_id < previous[1])):
                distances[neighbor] = (candidate, shelter_id)
                heapq.heappush(queue, (candidate, shelter_id, neighbor))
    return distances, shelter_snap_meters


def normalize_origins(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = {"longitude": "longitude", "lon": "longitude", "representative_lon": "longitude", "latitude": "latitude", "lat": "latitude", "representative_lat": "latitude", "origin_id": "origin_id", "id": "origin_id", "origin_name": "origin_name", "name": "origin_name"}
    renamed = frame.rename(columns={column: aliases[column] for column in frame.columns if column in aliases}).copy()
    required = {"origin_id", "longitude", "latitude"}
    missing = required - set(renamed.columns)
    if missing:
        raise ValueError(f"Origin input is missing required columns: {sorted(missing)}")
    renamed["origin_id"] = renamed["origin_id"].astype(str)
    renamed["longitude"] = pd.to_numeric(renamed["longitude"], errors="coerce")
    renamed["latitude"] = pd.to_numeric(renamed["latitude"], errors="coerce")
    renamed = renamed.dropna(subset=["longitude", "latitude"]).copy()
    if "origin_name" not in renamed:
        renamed["origin_name"] = renamed["origin_id"]
    if "weight" not in renamed:
        renamed["weight"] = 1.0
    renamed["weight"] = pd.to_numeric(renamed["weight"], errors="coerce").fillna(1.0)
    return renamed


def normalize_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a simple potential-shelter CSV/Parquet table."""
    aliases = {
        "candidate_id": "candidate_id", "id": "candidate_id",
        "longitude": "longitude", "lon": "longitude",
        "latitude": "latitude", "lat": "latitude",
        "name": "name", "candidate_name": "name",
    }
    renamed = frame.rename(columns={column: aliases[column] for column in frame.columns if column in aliases}).copy()
    required = {"candidate_id", "longitude", "latitude"}
    missing = required - set(renamed.columns)
    if missing:
        raise ValueError(f"Candidate input is missing required columns: {sorted(missing)}")
    renamed["candidate_id"] = renamed["candidate_id"].astype(str)
    renamed["longitude"] = pd.to_numeric(renamed["longitude"], errors="coerce")
    renamed["latitude"] = pd.to_numeric(renamed["latitude"], errors="coerce")
    renamed = renamed.dropna(subset=["longitude", "latitude"]).copy()
    if "name" not in renamed:
        renamed["name"] = renamed["candidate_id"]
    return renamed


def snap_summary(graph: dict[str, Any], frame: pd.DataFrame, id_column: str, max_snap_meters: float) -> dict[str, Any]:
    """Return a concise automatic node-snap quality report for location inputs."""
    snapper = NodeSnapper.from_graph(graph)
    distances = [snapper.snap(float(row.longitude), float(row.latitude))[1] for row in frame.itertuples(index=False)]
    off_network = [str(row[0]) for row, distance in zip(frame[[id_column]].itertuples(index=False), distances) if distance > max_snap_meters]
    return {
        "location_count": len(frame),
        "max_snap_meters": max_snap_meters,
        "maximum_observed_snap_meters": round(max(distances), 1) if distances else 0.0,
        "off_network_count": len(off_network),
        "off_network_ids": off_network[:50],
    }


def access_results(data_dir: Path, scenario: dict[str, Any], origins: pd.DataFrame, thresholds: list[int]) -> pd.DataFrame:
    graph = read_json(data_dir / "baseline" / "graph.json")
    shelters = active_shelters(data_dir, scenario)
    distances, _ = nearest_shelter_distances(graph, scenario, shelters)
    snapper = NodeSnapper.from_graph(graph)
    names = {item["id"]: item["name"] for item in shelters}
    rows = []
    for origin in normalize_origins(origins).itertuples(index=False):
        node_id, snap_meters = snapper.snap(float(origin.longitude), float(origin.latitude))
        result = distances.get(node_id)
        minutes = result[0] / 60 if result else math.inf
        shelter_id = result[1] if result else None
        row = dict(origin._asdict())
        row.update({"nearest_shelter_id": shelter_id, "nearest_shelter_name": names.get(shelter_id), "nearest_minutes": round(minutes, 3) if math.isfinite(minutes) else None, "origin_snap_meters": round(snap_meters, 1)})
        for threshold in thresholds:
            row[f"within_{threshold}min"] = bool(minutes <= threshold)
        rows.append(row)
    return pd.DataFrame(rows)
