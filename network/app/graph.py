from __future__ import annotations

import heapq
import json
import math
from pathlib import Path
from typing import Iterable


def _distance_squared(point: tuple[float, float], candidate: tuple[float, float]) -> float:
    return (point[0] - candidate[0]) ** 2 + (point[1] - candidate[1]) ** 2


class EditableRoadGraph:
    """Small directed graph whose physical segments can be masked per scenario."""

    def __init__(self, path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.nodes: dict[str, tuple[float, float]] = {
            str(node_id): (float(value[0]), float(value[1]))
            for node_id, value in payload["nodes"].items()
        }
        self.edges: list[dict] = list(payload["edges"])

    def _nearest_node(self, lon: float, lat: float) -> str:
        point = (lon, lat)
        return min(self.nodes, key=lambda node_id: _distance_squared(point, self.nodes[node_id]))

    @staticmethod
    def _edge_enabled(edge: dict, segment_states: dict[str, str]) -> bool:
        state = segment_states.get(edge["segment_id"], "open")
        return (
            state == "open"
            or (state == "forward_only" and edge["direction"] == "forward")
            or (state == "reverse_only" and edge["direction"] == "reverse")
        )

    def _adjacency(self, segment_states: dict[str, str]) -> dict[str, list[tuple[str, float]]]:
        adjacency: dict[str, list[tuple[str, float]]] = {}
        for edge in self.edges:
            if self._edge_enabled(edge, segment_states):
                adjacency.setdefault(edge["from"], []).append((edge["to"], float(edge["seconds"])))
        return adjacency

    def route_many(
        self,
        *,
        origin_lon: float,
        origin_lat: float,
        destinations: Iterable[dict],
        segment_states: dict[str, str],
    ) -> list[dict]:
        origin = self._nearest_node(origin_lon, origin_lat)
        destinations = list(destinations)
        target_nodes = {
            str(destination["id"]): self._nearest_node(float(destination["lon"]), float(destination["lat"]))
            for destination in destinations
        }
        remaining = set(target_nodes.values())
        distances = {origin: 0.0}
        queue: list[tuple[float, str]] = [(0.0, origin)]
        adjacency = self._adjacency(segment_states)
        while queue and remaining:
            seconds, node = heapq.heappop(queue)
            if seconds != distances.get(node):
                continue
            remaining.discard(node)
            for next_node, edge_seconds in adjacency.get(node, []):
                candidate = seconds + edge_seconds
                if candidate < distances.get(next_node, math.inf):
                    distances[next_node] = candidate
                    heapq.heappush(queue, (candidate, next_node))
        return [
            {
                "id": str(destination["id"]),
                "minutes": round(distances[target_nodes[str(destination["id"])] ] / 60.0, 2)
                if target_nodes[str(destination["id"])] in distances
                else None,
            }
            for destination in destinations
        ]
