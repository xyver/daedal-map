"""Build an inspection-only transmission-line candidate from the federal archive.

Endpoint identity is coordinate-derived and unverified. This pack deliberately
does not offer connected-component, power-flow, or outage propagation analysis.
"""

from __future__ import annotations

import argparse
import json
import sys
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
    frame = pyogrio.read_dataframe(
        source, bbox=tuple(bbox), columns=["ID", "SUB_1", "SUB_2", "STATUS", "VOLTAGE"]
    ).to_crs(4326)
    if frame.empty:
        raise ValueError("No transmission lines intersect the study area")
    nodes = {}
    links = []
    features = []
    skipped_multipart = 0
    for row in frame.itertuples():
        if row.geometry.geom_type != "LineString":
            skipped_multipart += 1
            continue
        coordinates = list(row.geometry.coords)
        if len(coordinates) < 2:
            continue
        feature_id = f"transmission-{int(row.ID)}"
        ends = []
        for point in (coordinates[0], coordinates[-1]):
            lon, lat = round(point[0], 5), round(point[1], 5)
            node_id = f"endpoint-{lon:.5f}-{lat:.5f}"
            nodes[node_id] = [lon, lat]
            ends.append(node_id)
        links.append({"id": feature_id, "feature_id": feature_id, "from": ends[0], "to": ends[1],
                      "directed": False, "topology_evidence": "rounded_geometry_endpoint_only"})
        features.append({"type": "Feature", "geometry": mapping(row.geometry),
                         "properties": {"feature_id": feature_id, "source_line_id": int(row.ID),
                                        "substation_1": None if row.SUB_1 is None else str(row.SUB_1),
                                        "substation_2": None if row.SUB_2 is None else str(row.SUB_2),
                                        "status": None if row.STATUS is None else str(row.STATUS),
                                        "voltage_kv": None if row.VOLTAGE is None or float(row.VOLTAGE) < 0 else float(row.VOLTAGE)}})
    manifest = write_pack(
        output, pack_id="usa-transmission-norcal-candidate", version="2026-10-02-candidate-1",
        network_type="power", bbox=bbox, capabilities=["inspect"],
        source={"name": "Federal transmission lines archive", "snapshot": "2026-10-02 local pull",
                "archive": source.name, "licence": "Check source terms before redistribution",
                "distribution": "local_candidate", "topology_evidence": "coordinate_endpoint_candidate_only",
                "excluded_multipart_features": skipped_multipart},
        nodes=nodes, links=links, features=features,
    )
    pack = Pack.open(output)
    return {"pack_id": manifest["pack_id"], "nodes": len(pack.nodes), "links": len(pack.links),
            "excluded_multipart": skipped_multipart, "output": str(output)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parents[1] / "data/packs/usa-transmission-norcal-candidate")
    parser.add_argument("--bbox", nargs=4, type=float, default=DEFAULT_BBOX,
                        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    args = parser.parse_args()
    print(json.dumps(build(args.source_archive, args.output_dir, args.bbox)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
