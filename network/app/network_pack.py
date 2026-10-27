"""Small, typed graph-pack contract shared by Network profiles.

The pack is immutable. Scenario changes are supplied separately when running
analysis, and source-specific builders live outside the viewer/runtime.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import math
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "network-pack-v1"
REQUIRED_FILES = ("nodes.json", "links.json", "features.geojson")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_pack(directory: Path, *, pack_id: str, version: str, network_type: str,
               bbox: list[float], capabilities: list[str], source: dict[str, Any],
               nodes: dict[str, list[float]], links: list[dict[str, Any]],
               features: list[dict[str, Any]]) -> dict[str, Any]:
    """Write a deterministic candidate pack after validating its graph."""
    if network_type not in {"road", "rail", "power", "river"}:
        raise ValueError(f"Unsupported network type: {network_type}")
    if directory.exists() and any(directory.iterdir()):
        raise ValueError(f"Candidate output is not empty: {directory}")
    directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "nodes.json": nodes,
        "links.json": links,
        "features.geojson": {"type": "FeatureCollection", "features": features},
    }
    for name, payload in payloads.items():
        (directory / name).write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "pack_id": pack_id,
        "version": version,
        "network_type": network_type,
        "bbox": bbox,
        "capabilities": sorted(set(capabilities)),
        "source": source,
        "files": {name: {"sha256": digest(directory / name), "bytes": (directory / name).stat().st_size}
                  for name in REQUIRED_FILES},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    Pack.open(directory)
    return manifest


@dataclass
class Pack:
    directory: Path
    manifest: dict[str, Any]
    nodes: dict[str, list[float]]
    links: list[dict[str, Any]]
    features: list[dict[str, Any]]

    @classmethod
    def open(cls, directory: Path) -> "Pack":
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("Unknown network pack schema")
        if manifest.get("network_type") not in {"road", "rail", "power", "river"}:
            raise ValueError("Unknown network type")
        for name in REQUIRED_FILES:
            expected = manifest.get("files", {}).get(name, {})
            path = directory / name
            if not path.is_file() or path.stat().st_size != expected.get("bytes") or digest(path) != expected.get("sha256"):
                raise ValueError(f"Network pack payload differs from manifest: {name}")
        nodes = json.loads((directory / "nodes.json").read_text(encoding="utf-8"))
        links = json.loads((directory / "links.json").read_text(encoding="utf-8"))
        features = json.loads((directory / "features.geojson").read_text(encoding="utf-8"))["features"]
        if not nodes or not links:
            raise ValueError("Network pack has no topology")
        for node_id, coordinate in nodes.items():
            if not node_id or len(coordinate) != 2 or not all(math.isfinite(float(x)) for x in coordinate):
                raise ValueError(f"Invalid node coordinate: {node_id}")
        feature_ids = [str(item.get("properties", {}).get("feature_id", "")) for item in features]
        feature_id_set = set(feature_ids)
        if len(feature_ids) != len(feature_id_set) or not all(feature_ids):
            raise ValueError("Display features need unique feature_id values")
        link_ids = set()
        referenced = set()
        for link in links:
            link_id = str(link.get("id", ""))
            feature_id = str(link.get("feature_id", ""))
            if not link_id or link_id in link_ids:
                raise ValueError(f"Duplicate or missing link ID: {link_id}")
            link_ids.add(link_id)
            if link.get("from") not in nodes or link.get("to") not in nodes:
                raise ValueError(f"Unresolved endpoint on link {link_id}")
            if feature_id not in feature_id_set:
                raise ValueError(f"Link has no display feature: {link_id}")
            if not isinstance(link.get("directed"), bool):
                raise ValueError(f"Link direction is undeclared: {link_id}")
            if "distance_km" in link and (not math.isfinite(float(link["distance_km"])) or float(link["distance_km"]) < 0):
                raise ValueError(f"Invalid distance on link {link_id}")
            referenced.add(feature_id)
        if feature_id_set != referenced:
            raise ValueError("Display feature has no topology link")
        return cls(directory, manifest, nodes, links, features)

    def _adjacency(self, disabled_features: set[str], disabled_nodes: set[str],
                   *, distance: bool = False) -> dict[str, list[tuple[str, float, str]]]:
        adjacency: dict[str, list[tuple[str, float, str]]] = {}
        for link in self.links:
            if link["feature_id"] in disabled_features or link["from"] in disabled_nodes or link["to"] in disabled_nodes:
                continue
            if distance and "distance_km" not in link:
                raise ValueError("Pack does not declare distance for every link")
            cost = float(link.get("distance_km", 1.0))
            adjacency.setdefault(link["from"], []).append((link["to"], cost, link["feature_id"]))
            if not link["directed"]:
                adjacency.setdefault(link["to"], []).append((link["from"], cost, link["feature_id"]))
        return adjacency

    def component(self, start_node: str, *, disabled_features: set[str] | None = None,
                  disabled_nodes: set[str] | None = None) -> set[str]:
        if start_node not in self.nodes:
            raise ValueError(f"Unknown start node: {start_node}")
        disabled_features = disabled_features or set()
        disabled_nodes = disabled_nodes or set()
        if start_node in disabled_nodes:
            return set()
        adjacency = self._adjacency(disabled_features, disabled_nodes)
        seen = {start_node}
        queue = deque([start_node])
        while queue:
            for neighbor, _, _ in adjacency.get(queue.popleft(), []):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
        return seen

    def trace(self, start_node: str, *, direction: str,
              disabled_features: set[str] | None = None) -> dict[str, Any]:
        """Trace source-declared directed links; a closure stops traversal."""
        capability = "trace_downstream" if direction == "downstream" else "trace_upstream"
        if direction not in {"downstream", "upstream"} or capability not in self.manifest["capabilities"]:
            raise ValueError(f"Pack does not support {direction} tracing")
        if start_node not in self.nodes:
            raise ValueError(f"Unknown start node: {start_node}")
        adjacency: dict[str, list[tuple[str, str]]] = {}
        for link in self.links:
            if link["feature_id"] in (disabled_features or set()):
                continue
            origin, target = (link["from"], link["to"]) if direction == "downstream" else (link["to"], link["from"])
            adjacency.setdefault(origin, []).append((target, link["feature_id"]))
        seen = {start_node}
        traversed: set[str] = set()
        queue = deque([start_node])
        while queue:
            for neighbor, feature_id in adjacency.get(queue.popleft(), []):
                traversed.add(feature_id)
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
        return {"node_ids": sorted(seen), "feature_ids": sorted(traversed)}

    def shortest_distance(self, start_node: str, end_node: str,
                          *, disabled_features: set[str] | None = None) -> dict[str, Any]:
        if "route_distance" not in self.manifest["capabilities"]:
            raise ValueError("Pack does not support distance routing")
        if start_node not in self.nodes or end_node not in self.nodes:
            raise ValueError("Unknown route endpoint")
        adjacency = self._adjacency(disabled_features or set(), set(), distance=True)
        distances = {start_node: 0.0}
        previous: dict[str, tuple[str, str]] = {}
        queue = [(0.0, start_node)]
        while queue:
            cost, node = heapq.heappop(queue)
            if cost != distances.get(node):
                continue
            if node == end_node:
                break
            for neighbor, edge_cost, feature_id in adjacency.get(node, []):
                candidate = cost + edge_cost
                if candidate < distances.get(neighbor, math.inf):
                    distances[neighbor] = candidate
                    previous[neighbor] = (node, feature_id)
                    heapq.heappush(queue, (candidate, neighbor))
        if end_node not in distances:
            return {"reachable": False, "distance_km": None, "feature_ids": []}
        path = []
        node = end_node
        while node != start_node:
            node, feature_id = previous[node]
            path.append(feature_id)
        path.reverse()
        return {"reachable": True, "distance_km": round(distances[end_node], 3), "feature_ids": path}
