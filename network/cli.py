"""Inspect validated Network packs and export graph tables as Parquet."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point, shape

from .app.data_paths import network_data_dir
from .app.network_pack import Pack


def packs_root(override: str = "") -> Path:
    selected = override or os.getenv("NETWORK_PACKS_DIR", "")
    return Path(selected).expanduser() if selected else network_data_dir() / "packs"


def validated_packs(root: Path) -> list[Pack]:
    packs = []
    for directory in sorted(root.iterdir()) if root.is_dir() else []:
        if directory.is_dir() and (directory / "manifest.json").is_file():
            packs.append(Pack.open(directory))
    return packs


def _write_geoparquet(rows: list[dict], schema: pa.Schema, path: Path,
                      geometry_types: list[str], pack: Pack) -> None:
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": geometry_types}
    }}
    metadata = {b"geo": json.dumps(geo, separators=(",", ":")).encode("utf-8"),
                b"network_pack_id": pack.manifest["pack_id"].encode("utf-8"),
                b"network_pack_version": pack.manifest["version"].encode("utf-8")}
    table = pa.Table.from_pylist(rows, schema=schema).replace_schema_metadata(metadata)
    pq.write_table(table, path, compression="zstd")


def write_tables(pack: Pack, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    node_rows = [{"node_id": node_id, "geometry": Point(coordinate).wkb,
                  "containing_loc_id": None,
                  "geography_relation_status": "not_enriched"}
                 for node_id, coordinate in sorted(pack.nodes.items())]
    _write_geoparquet(node_rows, pa.schema([("node_id", pa.string()), ("geometry", pa.binary()),
                                           ("containing_loc_id", pa.string()),
                                           ("geography_relation_status", pa.string())]),
                      output / "nodes.parquet", ["Point"], pack)
    link_rows = []
    for link in pack.links:
        extra = {key: value for key, value in link.items()
                 if key not in {"id", "feature_id", "from", "to", "directed", "distance_km"}}
        link_rows.append({"link_id": link["id"], "feature_id": link["feature_id"],
                          "from_node": link["from"], "to_node": link["to"],
                          "directed": link["directed"], "distance_km": link.get("distance_km"),
                          "attributes_json": json.dumps(extra, sort_keys=True),
                          "containing_loc_id": None,
                          "geography_relation_status": "not_enriched"})
    link_schema = pa.schema([("link_id", pa.string()), ("feature_id", pa.string()),
                             ("from_node", pa.string()), ("to_node", pa.string()),
                             ("directed", pa.bool_()), ("distance_km", pa.float64()),
                             ("attributes_json", pa.string()),
                             ("containing_loc_id", pa.string()),
                             ("geography_relation_status", pa.string())])
    pq.write_table(pa.Table.from_pylist(link_rows, schema=link_schema),
                   output / "links.parquet", compression="zstd")
    feature_rows = []
    feature_types = set()
    for feature in pack.features:
        geometry = shape(feature["geometry"])
        if geometry.geom_type not in {"Point", "LineString", "Polygon", "MultiPoint",
                                      "MultiLineString", "MultiPolygon", "GeometryCollection"}:
            raise ValueError(f"Unsupported GeoParquet geometry: {geometry.geom_type}")
        feature_types.add(geometry.geom_type)
        properties = feature["properties"]
        feature_rows.append({"feature_id": properties["feature_id"], "geometry": geometry.wkb,
                             "properties_json": json.dumps(properties, sort_keys=True),
                             "containing_loc_id": None,
                             "geography_relation_status": "not_enriched"})
    feature_schema = pa.schema([("feature_id", pa.string()), ("geometry", pa.binary()),
                                ("properties_json", pa.string()),
                                ("containing_loc_id", pa.string()),
                                ("geography_relation_status", pa.string())])
    _write_geoparquet(feature_rows, feature_schema, output / "features.parquet",
                      sorted(feature_types), pack)
    relation_schema = pa.schema([
        ("pack_id", pa.string()), ("pack_version", pa.string()),
        ("feature_kind", pa.string()), ("feature_id", pa.string()),
        ("target_loc_id", pa.string()), ("target_family", pa.string()),
        ("relationship_type", pa.string()), ("length_in_target", pa.float64()),
        ("share_of_feature", pa.float64()), ("geometry_release", pa.string()),
        ("matching_method", pa.string()), ("status", pa.string()),
    ])
    pq.write_table(pa.Table.from_pylist([], schema=relation_schema),
                   output / "geography_relations.parquet", compression="zstd")
    (output / "export_manifest.json").write_text(json.dumps({
        "schema_version": "network-parquet-export-v1", "pack_id": pack.manifest["pack_id"],
        "pack_version": pack.manifest["version"], "network_type": pack.manifest["network_type"],
        "crs": "OGC:CRS84", "topology": "links.from_node/to_node reference nodes.node_id",
        "graph_identity": "pack_scoped_node_link_feature_ids",
        "reference_loc_id": {"status": "not_assigned"},
        "containing_loc_id": {"status": "not_enriched",
                              "rule": "smallest_admin_containing_full_geometry",
                              "geometry_release": None},
        "geography_relations": {"status": "not_enriched", "geometry_release": None,
                                "table": "geography_relations.parquet"},
        "files": {"nodes.parquet": len(node_rows), "links.parquet": len(link_rows),
                  "features.parquet": len(feature_rows), "geography_relations.parquet": 0}
    }, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packs-dir", default="", help="Folder containing validated network packs")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List validated packs and graph counts")
    export = commands.add_parser("export", help="Write one pack's GeoParquet and Parquet tables")
    export.add_argument("pack_id")
    export.add_argument("--output", required=True, type=Path)
    export_all = commands.add_parser("export-all", help="Write Parquet tables for every validated pack")
    export_all.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        packs = validated_packs(packs_root(args.packs_dir))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(f"Network pack validation failed: {exc}")
    if args.command == "list":
        for pack in packs:
            print(f"{pack.manifest['pack_id']}\t{pack.manifest['network_type']}\t{len(pack.nodes)} nodes\t{len(pack.links)} links")
        return 0
    if not packs:
        parser.error("No validated Network packs are installed")
    if args.command == "export":
        selected = next((pack for pack in packs if pack.manifest["pack_id"] == args.pack_id), None)
        if selected is None:
            parser.error(f"Network pack is not installed: {args.pack_id}")
        write_tables(selected, args.output)
        print(f"Exported {selected.manifest['pack_id']} tables to {args.output.resolve()}")
        return 0
    for pack in packs:
        destination = args.output / pack.manifest["pack_id"]
        write_tables(pack, destination)
        print(f"Exported {pack.manifest['pack_id']} tables to {destination.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
