"""Create Lake-road-scope origin CSVs from the DaedalMap USA geometry spine.

The road graph covers Lake County and a small cross-border buffer. By default
origins are Lake County only, while shelters and routes may cross that border.
An explicit option enables a future cross-border origin study.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from shapely.geometry import Point, shape

from scenario_analysis import NodeSnapper, read_json


LEVELS = {
    "county": "county.parquet",
    "tract": "tract/USA-CA.parquet",
    "blockgroup": "blockgroup/USA-CA.parquet",
    "block": "block/USA-CA.parquet",
}
LOCATION_COLUMNS = ["loc_id", "parent_id", "name", "centroid_lon", "centroid_lat"]


def geojson(value):
    return json.loads(value) if isinstance(value, str) else value


def touching_counties(geometry_root: Path, scope_geometry) -> pd.DataFrame:
    counties = pd.read_parquet(geometry_root / "county.parquet", columns=LOCATION_COLUMNS + ["geometry"])
    counties = counties[counties["loc_id"].str.startswith("USA-CA-")].copy()
    return counties[counties["geometry"].map(lambda value: shape(geojson(value)).intersects(scope_geometry))].copy()


def scoped_rows(path: Path, county_ids: set[str], scope_geometry) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=LOCATION_COLUMNS)
    mask = frame["loc_id"].map(lambda value: any(str(value).startswith(f"{county_id}-") for county_id in county_ids))
    frame = frame[mask].dropna(subset=["centroid_lon", "centroid_lat"]).copy()
    return frame[frame.apply(lambda row: scope_geometry.covers(Point(float(row.centroid_lon), float(row.centroid_lat))), axis=1)].copy()


parser = argparse.ArgumentParser(description="Prepare Lake County road-scope Census centroid CSVs.")
parser.add_argument("--geometry-root", type=Path, required=True, help="Path to county-map-data/countries/USA/geometry")
parser.add_argument("--data-dir", type=Path, default=Path(__file__).parents[1] / "data")
parser.add_argument("--output-dir", type=Path, help="Defaults to <data-dir>/analysis/census_origins")
parser.add_argument("--max-snap-meters", type=float, default=3000, help="Exclude centroids farther than this graph-node snap distance.")
parser.add_argument("--include-buffer-origins", action="store_true", help="Include origins from adjacent counties inside the road buffer, rather than Lake County only.")
args = parser.parse_args()

scope_path = args.data_dir / "baseline" / "scope.json"
scope_geometry = shape(json.loads(scope_path.read_text(encoding="utf-8"))["scope_geometry"])
snapper = NodeSnapper.from_graph(read_json(args.data_dir / "baseline" / "graph.json"))
output_dir = args.output_dir or args.data_dir / "analysis" / "census_origins"
output_dir.mkdir(parents=True, exist_ok=True)

counties = touching_counties(args.geometry_root, scope_geometry)
lake_county_id = "USA-CA-033"
origin_counties = counties if args.include_buffer_origins else counties[counties["loc_id"] == lake_county_id].copy()
county_ids = set(origin_counties["loc_id"])
summary = {
    "road_scope_counties": counties[["loc_id", "name"]].to_dict(orient="records"),
    "origin_counties": origin_counties[["loc_id", "name"]].to_dict(orient="records"),
    "include_buffer_origins": args.include_buffer_origins,
    "outputs": {},
}

for level, relative_path in LEVELS.items():
    frame = origin_counties[LOCATION_COLUMNS].copy() if level == "county" else scoped_rows(args.geometry_root / relative_path, county_ids, scope_geometry)
    if level == "county":
        frame = frame[frame.apply(lambda row: scope_geometry.covers(Point(float(row.centroid_lon), float(row.centroid_lat))), axis=1)].copy()
    frame["county_id"] = frame["loc_id"] if level == "county" else frame["loc_id"].map(lambda value: next(county_id for county_id in county_ids if str(value).startswith(f"{county_id}-")))
    result = pd.DataFrame({
        "origin_id": frame["loc_id"].astype(str),
        "origin_name": frame["name"].fillna(frame["loc_id"]).astype(str),
        "longitude": frame["centroid_lon"].astype(float),
        "latitude": frame["centroid_lat"].astype(float),
        "origin_level": level,
        "county_id": frame["county_id"].astype(str),
    })
    result["origin_snap_meters"] = [round(snapper.snap(lon, lat)[1], 1) for lon, lat in zip(result.longitude, result.latitude)]
    rejected = result[result["origin_snap_meters"] > args.max_snap_meters].copy()
    result = result[result["origin_snap_meters"] <= args.max_snap_meters].copy()
    output_path = output_dir / f"lake_scope_{level}_centroids.csv"
    result.to_csv(output_path, index=False)
    rejected_path = output_dir / f"lake_scope_{level}_centroids_off_network.csv"
    if len(rejected):
        rejected.to_csv(rejected_path, index=False)
    elif rejected_path.exists():
        rejected_path.unlink()
    summary["outputs"][level] = {
        "path": str(output_path),
        "origin_count": len(result),
        "off_network_count": len(rejected),
        "off_network_path": str(rejected_path) if len(rejected) else None,
    }

(output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
