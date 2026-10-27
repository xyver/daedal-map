"""Build the bounded Lake County road-editor graph from local canonical inputs.

This intentionally produces a transparent graph for the first scenario editor.
It does not try to mutate the existing Valhalla tiles; its stable segment IDs
are shared by the visual editor and the scenario shortest-path calculation.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
from collections import defaultdict
from pathlib import Path

import osmium
import pandas as pd
from shapely.geometry import LineString, Point, box, mapping, shape
from shapely.ops import unary_union


# Keep the prototype physically small.  The buffer retains just enough network
# across Lake County's border to avoid an artificial edge cut-off.
COUNTIES = ("Lake",)
SHELTER_COUNTIES = ("Lake", "Mendocino", "Glenn", "Colusa", "Yolo", "Napa", "Sonoma")
CROSS_BORDER_BUFFER_DEGREES = 0.08
DEFAULT_SPEED_KPH = {
    "motorway": 105, "trunk": 90, "primary": 80, "secondary": 65,
    "tertiary": 55, "unclassified": 45, "residential": 35, "service": 20,
}
EXCLUDED_HIGHWAYS = {"footway", "path", "cycleway", "bridleway", "steps", "pedestrian", "construction", "proposed"}
TILE_SIZE_DEGREES = 0.10
OVERVIEW_HIGHWAYS = {"motorway", "trunk", "primary", "secondary"}


def parse_speed_kph(value: str | None, highway: str) -> float:
    if value:
        number = "".join(char for char in value.split(";")[0] if char.isdigit() or char == ".")
        if number:
            speed = float(number)
            return speed * 1.60934 if "mph" in value.lower() else speed
    return float(DEFAULT_SPEED_KPH.get(highway, 35))


def haversine_meters(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (*a, *b))
    dlon, dlat = lon2 - lon1, lat2 - lat1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371008.8 * 2 * math.asin(math.sqrt(value))


class RoadBuilder(osmium.SimpleHandler):
    def __init__(self, intersection_nodes: set[str], scope_geometry) -> None:
        super().__init__()
        self.intersection_nodes = intersection_nodes
        self.scope_geometry = scope_geometry
        self.nodes: dict[str, tuple[float, float]] = {}
        self.edges: list[dict] = []
        self.features: list[dict] = []

    def way(self, way: osmium.osm.Way) -> None:
        highway = way.tags.get("highway")
        if not highway or highway in EXCLUDED_HIGHWAYS:
            return
        try:
            coordinates = [(float(node.location.lon), float(node.location.lat), str(node.ref)) for node in way.nodes]
        except osmium.InvalidLocationError:
            return
        if len(coordinates) < 2:
            return
        oneway_value = way.tags.get("oneway", "").lower()
        forward_only = oneway_value in {"yes", "true", "1"} or way.tags.get("junction") == "roundabout"
        reverse_only = oneway_value == "-1"
        speed_kph = parse_speed_kph(way.tags.get("maxspeed"), highway)
        split_indexes = {0, len(coordinates) - 1}
        split_indexes.update(index for index, node in enumerate(coordinates) if node[2] in self.intersection_nodes)
        ordered_splits = sorted(split_indexes)
        for start_index, end_index in zip(ordered_splits, ordered_splits[1:]):
            start, end = coordinates[start_index], coordinates[end_index]
            start_coord, end_coord = start[:2], end[:2]
            geometry = [node[:2] for node in coordinates[start_index : end_index + 1]]
            if not self.scope_geometry.intersects(LineString(geometry)):
                continue
            meters = sum(haversine_meters(a, b) for a, b in zip(geometry, geometry[1:]))
            if meters < 1:
                continue
            segment_id = f"osm-{way.id}-{start_index}-{end_index}"
            start_id, end_id = start[2], end[2]
            self.nodes[start_id] = start_coord
            self.nodes[end_id] = end_coord
            seconds = round(meters / (speed_kph * 1000 / 3600), 2)
            props = {
                "segment_id": segment_id,
                "osm_way_id": int(way.id),
                "name": way.tags.get("name") or way.tags.get("ref") or "Unnamed road",
                "highway": highway,
                "speed_kph": speed_kph,
                "meters": round(meters, 1),
                "oneway_baseline": forward_only or reverse_only,
            }
            self.features.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": geometry}, "properties": props})
            if not reverse_only:
                self.edges.append({"segment_id": segment_id, "direction": "forward", "from": start_id, "to": end_id, "seconds": seconds})
            if not forward_only:
                self.edges.append({"segment_id": segment_id, "direction": "reverse", "from": end_id, "to": start_id, "seconds": seconds})


class RoadNodeUsage(osmium.SimpleHandler):
    """Find OSM nodes shared by two or more drivable ways (road intersections)."""

    def __init__(self) -> None:
        super().__init__()
        self.counts: defaultdict[str, int] = defaultdict(int)

    def way(self, way: osmium.osm.Way) -> None:
        highway = way.tags.get("highway")
        if not highway or highway in EXCLUDED_HIGHWAYS:
            return
        for node in way.nodes:
            self.counts[str(node.ref)] += 1

    @property
    def intersections(self) -> set[str]:
        return {node_id for node_id, count in self.counts.items() if count > 1}


def selected_counties(data_root: Path) -> tuple[pd.DataFrame, object, tuple[float, float, float, float]]:
    counties = pd.read_parquet(data_root / "countries/USA/geometry/county.parquet")
    matches = counties[(counties["loc_id"].str.startswith("USA-CA-")) & counties["name"].isin(COUNTIES)].copy()
    missing = set(COUNTIES) - set(matches["name"])
    if missing:
        raise RuntimeError(f"Could not find California county geometry rows: {sorted(missing)}")
    lake_geometry = shape(json.loads(matches.iloc[0]["geometry"]))
    scope_geometry = lake_geometry.buffer(CROSS_BORDER_BUFFER_DEGREES)
    bounds = tuple(float(value) for value in scope_geometry.bounds)
    return matches, scope_geometry, bounds


def selected_shelter_counties(data_root: Path) -> pd.DataFrame:
    counties = pd.read_parquet(data_root / "countries/USA/geometry/county.parquet")
    matches = counties[(counties["loc_id"].str.startswith("USA-CA-")) & counties["name"].isin(SHELTER_COUNTIES)].copy()
    missing = set(SHELTER_COUNTIES) - set(matches["name"])
    if missing:
        raise RuntimeError(f"Could not find California shelter-context counties: {sorted(missing)}")
    return matches


def write_shelters(data_root: Path, county_ids: list[str], scope_geometry, output: Path) -> tuple[int, int]:
    """Emit only shelter points inside the editable road-network scope."""
    shelters = pd.read_parquet(data_root / "countries/USA/nss_shelters/shelters.parquet")
    shelters = shelters[
        shelters["county_loc_id"].isin(county_ids)
        & shelters["longitude"].notna()
        & shelters["latitude"].notna()
    ]
    features = []
    for _, row in shelters.iterrows():
        point = Point(float(row.longitude), float(row.latitude))
        if not scope_geometry.covers(point):
            continue
        is_open = bool(row.is_currently_open)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(row.longitude), float(row.latitude)]},
            "properties": {
                "id": str(row.location_id), "name": str(row.shelter_name),
                "county_loc_id": str(row.county_loc_id), "status": str(row.shelter_status_code),
                "is_currently_open": is_open,
            },
        })
    output.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")
    return len(features), sum(feature["properties"]["is_currently_open"] for feature in features)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare the editable Lake County regional road graph.")
    parser.add_argument("--pbf", default="/inputs/osm/california-latest.osm.pbf")
    parser.add_argument("--data-root", default="/inputs/source-data")
    parser.add_argument("--output-dir", default="/app/data/baseline")
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    counties, scope_geometry, bounds = selected_counties(Path(args.data_root))
    region_pbf = output / "lake_region.osm.pbf"
    bbox = ",".join(f"{value:.6f}" for value in bounds)
    subprocess.run(["osmium", "extract", "-b", bbox, "-o", str(region_pbf), "--overwrite", str(Path(args.pbf))], check=True)
    usage = RoadNodeUsage()
    usage.apply_file(str(region_pbf))
    builder = RoadBuilder(usage.intersections, scope_geometry)
    builder.apply_file(str(region_pbf), locations=True)
    (output / "graph.json").write_text(json.dumps({"nodes": builder.nodes, "edges": builder.edges}), encoding="utf-8")
    legacy_network = output / "network_segments.geojson"
    if legacy_network.exists():
        legacy_network.unlink()
    tiles_dir = output / "road_tiles"
    tiles_dir.mkdir(parents=True, exist_ok=True)
    for tile_path in tiles_dir.glob("*.geojson"):
        tile_path.unlink()
    tiles: defaultdict[tuple[int, int], list[dict]] = defaultdict(list)
    overview_features: list[dict] = []
    for feature in builder.features:
        coordinates = feature["geometry"]["coordinates"]
        midpoint = coordinates[len(coordinates) // 2]
        tile_key = (math.floor(midpoint[0] / TILE_SIZE_DEGREES), math.floor(midpoint[1] / TILE_SIZE_DEGREES))
        tiles[tile_key].append(feature)
        if feature["properties"]["highway"] in OVERVIEW_HIGHWAYS:
            overview_features.append(feature)
    for (tile_x, tile_y), features in tiles.items():
        (tiles_dir / f"{tile_x}_{tile_y}.geojson").write_text(
            json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8"
        )
    (output / "overview_roads.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": overview_features}), encoding="utf-8"
    )
    shelter_counties = selected_shelter_counties(Path(args.data_root))
    shelter_count, open_shelter_count = write_shelters(
        Path(args.data_root), shelter_counties["loc_id"].tolist(), scope_geometry, output / "shelters.geojson"
    )
    county_features = []
    county_geometries = []
    for _, county in shelter_counties.iterrows():
        county_geometry = shape(json.loads(county["geometry"]))
        county_geometries.append(county_geometry)
        county_features.append({
            "type": "Feature", "geometry": mapping(county_geometry),
            "properties": {"loc_id": str(county["loc_id"]), "name": str(county["name"])},
        })
    study_area = unary_union(county_geometries)
    # The world-sized exterior is clipped naturally by the Leaflet viewport.
    fog_geometry = box(-180, -85, 180, 85).difference(study_area)
    (output / "study_area.geojson").write_text(
        json.dumps({
            "counties": {"type": "FeatureCollection", "features": county_features},
            "fog": {"type": "Feature", "geometry": mapping(fog_geometry), "properties": {}},
        }), encoding="utf-8"
    )
    scope = {
        "counties": counties[["loc_id", "name"]].to_dict("records"),
        "bounds": bounds,
        "tile_size_degrees": TILE_SIZE_DEGREES,
        "cross_border_buffer_degrees": CROSS_BORDER_BUFFER_DEGREES,
        "scope_geometry": scope_geometry.__geo_interface__,
        "shelter_counties": shelter_counties[["loc_id", "name"]].to_dict("records"),
        "display_bounds": [
            float(shelter_counties["bbox_min_lon"].min()), float(shelter_counties["bbox_min_lat"].min()),
            float(shelter_counties["bbox_max_lon"].max()), float(shelter_counties["bbox_max_lat"].max()),
        ],
    }
    (output / "scope.json").write_text(json.dumps(scope, indent=2), encoding="utf-8")
    print(f"Prepared {len(builder.features):,} editable segments, {len(builder.edges):,} directed edges, {len(tiles):,} road tiles, {len(overview_features):,} overview roads, and {shelter_count:,} shelters ({open_shelter_count:,} open in snapshot).")


if __name__ == "__main__":
    main()
