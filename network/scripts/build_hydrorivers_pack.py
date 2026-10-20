"""Build a bounded HydroRIVERS candidate with source-declared downstream links.

Usage: python scripts/build_hydrorivers_pack.py --source-archive <HydroRIVERS zip>
This builder needs pyogrio and shapely; the pack reader does not.
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path

import pyogrio
from shapely.geometry import mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.network_pack import Pack, write_pack  # noqa: E402


DEFAULT_BBOX = [-123.4, 38.0, -120.7, 39.8]


def build(source: Path, output: Path, bbox: list[float]) -> dict:
    if len(bbox) != 4 or bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
        raise ValueError("bbox must be min_lon min_lat max_lon max_lat")
    if not source.is_file():
        raise FileNotFoundError(source)
    with zipfile.ZipFile(source) as archive:
        shapefiles = [name for name in archive.namelist() if name.endswith(".shp")]
    if len(shapefiles) != 1:
        raise ValueError("Expected one HydroRIVERS shapefile")
    path = "/vsizip/" + source.resolve().as_posix() + "/" + shapefiles[0]
    frame = pyogrio.read_dataframe(
        path, bbox=tuple(bbox), columns=["HYRIV_ID", "NEXT_DOWN", "LENGTH_KM", "ORD_STRA", "HYBAS_L12"]
    ).to_crs(4326)
    if frame.empty:
        raise ValueError("No HydroRIVERS reaches intersect the study area")
    ids = {int(value) for value in frame.HYRIV_ID}
    nodes = {}
    links = []
    features = []
    boundary_count = 0
    for row in frame.itertuples():
        reach_id = int(row.HYRIV_ID)
        downstream_id = int(row.NEXT_DOWN)
        feature_id = f"hyriv-{reach_id}"
        node_id = feature_id
        representative = row.geometry.representative_point()
        nodes[node_id] = [round(representative.x, 7), round(representative.y, 7)]
        if downstream_id in ids and downstream_id != reach_id:
            target = f"hyriv-{downstream_id}"
            boundary = False
        else:
            target = f"boundary-{reach_id}"
            endpoint = list(row.geometry.coords)[-1]
            nodes[target] = [round(endpoint[0], 7), round(endpoint[1], 7)]
            boundary = True
            boundary_count += 1
        links.append({"id": feature_id, "feature_id": feature_id, "from": node_id, "to": target,
                      "directed": True, "distance_km": round(float(row.LENGTH_KM), 6),
                      "boundary_exit": boundary})
        features.append({"type": "Feature", "geometry": mapping(row.geometry),
                         "properties": {"feature_id": feature_id, "source_reach_id": reach_id,
                                        "next_down": downstream_id, "length_km": float(row.LENGTH_KM),
                                        "stream_order": int(row.ORD_STRA), "basin_level_12": int(row.HYBAS_L12),
                                        "boundary_exit": boundary}})
    manifest = write_pack(
        output, pack_id="na-hydrorivers-norcal-v10", version="v10-candidate-1",
        network_type="river", bbox=bbox,
        capabilities=["inspect", "close_feature", "trace_downstream", "trace_upstream"],
        source={"name": "HydroRIVERS North America", "snapshot": "v10", "archive": source.name,
                "licence": "Check HydroSHEDS terms before redistribution", "distribution": "local_candidate"},
        nodes=nodes, links=links, features=features,
    )
    pack = Pack.open(output)
    return {"pack_id": manifest["pack_id"], "nodes": len(pack.nodes), "links": len(pack.links),
            "boundary_exits": boundary_count, "output": str(output)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parents[1] / "data/packs/na-hydrorivers-norcal-v10")
    parser.add_argument("--bbox", nargs=4, type=float, default=DEFAULT_BBOX,
                        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    args = parser.parse_args()
    print(json.dumps(build(args.source_archive, args.output_dir, args.bbox)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
