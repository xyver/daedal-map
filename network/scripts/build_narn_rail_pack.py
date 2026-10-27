"""Build a bounded, private NARN 2024 rail candidate from the FRA archive.

Usage: python scripts/build_narn_rail_pack.py --source-archive <DS3.zip>
The importer needs pyogrio, geopandas, pyproj, and shapely. These are builder
dependencies, not viewer dependencies.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import tempfile
import zipfile
from pathlib import Path

import pyogrio
from pyproj import Transformer
from shapely.geometry import box, mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.network_pack import Pack, write_pack  # noqa: E402


INNER_NAME = "NTAD2024_GDB_Final_NARN_2024_10_16.zip"
DEFAULT_BBOX = [-123.4, 38.0, -120.7, 39.8]


def unpack_gdb(source: Path, temporary: Path) -> Path:
    with zipfile.ZipFile(source) as outer:
        with zipfile.ZipFile(io.BytesIO(outer.read(INNER_NAME))) as middle:
            nested = next(name for name in middle.namelist() if name.endswith(".gdb.zip"))
            with zipfile.ZipFile(io.BytesIO(middle.read(nested))) as inner:
                for item in inner.infolist():
                    target = (temporary / item.filename).resolve()
                    if not target.is_relative_to(temporary.resolve()):
                        raise ValueError("NARN archive contains a path outside the extraction area")
                inner.extractall(temporary)
    return next(temporary.glob("*.gdb"))


def field_id(value: object) -> str:
    if value is None:
        raise ValueError("Missing source graph ID")
    return str(int(value))


def build(source: Path, output: Path, bbox: list[float]) -> dict:
    if len(bbox) != 4 or bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
        raise ValueError("bbox must be min_lon min_lat max_lon max_lat")
    if not source.is_file():
        raise FileNotFoundError(source)
    with tempfile.TemporaryDirectory(prefix="narn_2024_") as working:
        gdb = unpack_gdb(source, Path(working))
        layer_names = [str(row[0]) for row in pyogrio.list_layers(gdb)]
        lines_layer = next(name for name in layer_names if name.startswith("narn_lines"))
        nodes_layer = next(name for name in layer_names if name.startswith("narn_nodes"))
        projected = Transformer.from_crs(4326, 3857, always_xy=True)
        a = projected.transform(bbox[0], bbox[1])
        b = projected.transform(bbox[2], bbox[3])
        bounds = box(a[0], a[1], b[0], b[1])
        lines = pyogrio.read_dataframe(
            gdb, layer=lines_layer,
            columns=["FRAARCID", "FRFRANODE", "TOFRANODE", "MILES", "RROWNER1", "STATEAB", "NET", "PASSNGR"],
        )
        lines = lines[lines.geometry.intersects(bounds)].copy()
        if lines.empty:
            raise ValueError("No NARN arcs intersect the selected study area")
        wanted_nodes = {field_id(value) for column in ("FRFRANODE", "TOFRANODE") for value in lines[column]}
        nodes_frame = pyogrio.read_dataframe(gdb, layer=nodes_layer, columns=["FRANODEID", "PASSNGRSTN"])
        nodes_frame["node_id"] = nodes_frame["FRANODEID"].map(field_id)
        nodes_frame = nodes_frame[nodes_frame["node_id"].isin(wanted_nodes)].to_crs(4326)
        nodes = {row.node_id: [round(row.geometry.x, 7), round(row.geometry.y, 7)]
                 for row in nodes_frame.itertuples()}
        if set(nodes) != wanted_nodes:
            raise ValueError(f"NARN arcs have {len(wanted_nodes - set(nodes))} unresolved node IDs")
        lines = lines.to_crs(4326)
        links = []
        features = []
        for row in lines.itertuples():
            feature_id = "narn-" + field_id(row.FRAARCID)
            miles = float(row.MILES)
            if miles < 0 or miles != miles:
                raise ValueError(f"Invalid NARN miles on {feature_id}")
            links.append({"id": feature_id, "feature_id": feature_id,
                          "from": field_id(row.FRFRANODE), "to": field_id(row.TOFRANODE),
                          "directed": False, "distance_km": round(miles * 1.609344, 6)})
            features.append({"type": "Feature", "geometry": mapping(row.geometry),
                             "properties": {"feature_id": feature_id, "source_arc_id": field_id(row.FRAARCID),
                                            "owner": None if row.RROWNER1 is None else str(row.RROWNER1),
                                            "state": None if row.STATEAB is None else str(row.STATEAB),
                                            "network_class": None if row.NET is None else str(row.NET),
                                            "passenger": None if row.PASSNGR is None else str(row.PASSNGR)}})
    manifest = write_pack(
        output, pack_id="usa-narn-rail-norcal-2024", version="2024-10-16-candidate-1",
        network_type="rail", bbox=bbox,
        capabilities=["inspect", "connected_component", "close_feature", "route_distance"],
        source={"name": "North American Rail Network", "snapshot": "2024-10-16",
                "archive": source.name, "licence": "US public domain", "distribution": "local_candidate"},
        nodes=nodes, links=links, features=features,
    )
    pack = Pack.open(output)
    return {"pack_id": manifest["pack_id"], "nodes": len(pack.nodes), "links": len(pack.links),
            "features": len(pack.features), "output": str(output)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parents[1] / "data/packs/usa-narn-rail-norcal-2024")
    parser.add_argument("--bbox", nargs=4, type=float, default=DEFAULT_BBOX,
                        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    args = parser.parse_args()
    print(json.dumps(build(args.source_archive, args.output_dir, args.bbox)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
