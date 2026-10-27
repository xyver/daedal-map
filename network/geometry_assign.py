"""Trial Network placement against one pinned exact USA admin-spine table.

This is a bounded, read-only geometry consumer. It writes enrichment artifacts
beside a report; it never changes the source pack or Geometry release.
"""

from __future__ import annotations

import argparse
import json
import time
import zipfile
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb, prepare
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

from .app.network_pack import Pack


EXACT_ADMIN_MEMBER = "exact/admin/admin_0/0000_admin_0_3.parquet"
GEOMETRY_RELEASE = "usa-geometry-detail-1.3.5"


def _admin_rows(archive: Path, scope_loc_id: str) -> tuple[list[dict], dict]:
    with zipfile.ZipFile(archive) as source:
        package = json.loads(source.read("geometry.json"))
        if package.get("country_code") != "USA" or package.get("version") != "1.3.5":
            raise ValueError("v1 trial requires USA Geometry detail 1.3.5")
        member = source.getinfo(EXACT_ADMIN_MEMBER)
        columns = ["loc_id", "parent_id", "admin_level", "admin_1_loc_id", "geometry"]
        table = pq.ParquetFile(pa.BufferReader(source.read(member))).read(columns=columns)
    rows = [row for row in table.to_pylist()
            if (scope_loc_id == "USA" or row["admin_1_loc_id"] == scope_loc_id
                or row["loc_id"] == scope_loc_id)
            and row["geometry"] is not None and int(row["admin_level"]) <= 3]
    if not any(row["loc_id"] == scope_loc_id for row in rows):
        raise ValueError(f"Scope has no exact admin polygon: {scope_loc_id}")
    return rows, {"member": EXACT_ADMIN_MEMBER, "member_crc32": f"{member.CRC:08x}",
                  "member_bytes": member.file_size}


def _ancestry(loc_id: str, parents: dict[str, str | None]) -> list[str]:
    result = []
    while loc_id and loc_id in parents and loc_id not in result:
        result.append(loc_id)
        loc_id = parents[loc_id] or ""
    return result


def _common_ancestor(left: str, right: str, parents: dict[str, str | None]) -> str | None:
    right_ancestors = set(_ancestry(right, parents))
    return next((item for item in _ancestry(left, parents) if item in right_ancestors), None)


def assign(pack: Pack, archive: Path, scope_loc_id: str, output: Path,
           *, prepare_geometries: bool = True) -> dict:
    network_type = pack.manifest["network_type"]
    if network_type not in {"rail", "power", "river"}:
        raise ValueError("v1 trial supports rail, power, and river packs")
    endpoint_nodes = network_type in {"rail", "power"}
    start = time.perf_counter()
    rows, member_pin = _admin_rows(archive, scope_loc_id)
    loaded = time.perf_counter()
    by_id = {row["loc_id"]: from_wkb(row["geometry"]) for row in rows}
    levels = {row["loc_id"]: int(row["admin_level"]) for row in rows}
    parents = {row["loc_id"]: row["parent_id"] for row in rows}
    scope = by_id[scope_loc_id]
    ids = list(by_id)
    tree = STRtree([by_id[loc_id] for loc_id in ids])
    if prepare_geometries:
        prepare(list(by_id.values()))
    indexed = time.perf_counter()

    def covering(geometry) -> list[str]:
        return [ids[int(index)] for index in tree.query(geometry)
                if by_id[ids[int(index)]].covers(geometry)]

    def smallest(covered: list[str]) -> tuple[str | None, str]:
        if not covered:
            return None, "unresolved"
        depth = max(levels[loc_id] for loc_id in covered)
        peers = sorted(loc_id for loc_id in covered if levels[loc_id] == depth)
        if len(peers) == 1:
            return peers[0], "resolved"
        common = peers[0]
        for peer in peers[1:]:
            common = _common_ancestor(common, peer, parents)
            if common is None:
                return None, "ambiguous"
        return common, "boundary_ambiguous"

    geometries = {feature["properties"]["feature_id"]: shape(feature["geometry"])
                  for feature in pack.features}
    isolated_features = {feature_id: geometry for feature_id, geometry in geometries.items()
                         if scope.covers(geometry)}
    isolated_links = [link for link in pack.links if link["feature_id"] in isolated_features]
    node_ids = sorted({link[end] for link in isolated_links for end in ("from", "to")})
    isolated = time.perf_counter()
    node_rows = []
    node_places = {}
    for node_id in node_ids:
        point = Point(pack.nodes[node_id])
        place, status = smallest(covering(point))
        node_places[node_id] = place
        node_rows.append({"node_id": node_id, "containing_loc_id": place,
                          "admin_level": levels.get(place), "status": status})
    nodes_done = time.perf_counter()

    feature_rows = []
    promoted = 0
    deeper_than_node_candidate = 0
    endpoint_mismatch = 0
    for link in isolated_links:
        geometry = isolated_features[link["feature_id"]]
        left = node_places[link["from"]]
        right = node_places[link["to"]]
        candidate = _common_ancestor(left, right, parents) if endpoint_nodes and left and right else None
        initial_candidate = candidate
        while candidate and not by_id[candidate].covers(geometry):
            candidate = parents.get(candidate)
        if initial_candidate and candidate != initial_candidate:
            promoted += 1
        exact_place, status = smallest(covering(geometry))
        # Exact full-line coverage decides placement. Node stacks are an
        # inexpensive candidate and a check on the network's endpoint model.
        if candidate and exact_place and levels[exact_place] < levels[candidate]:
            raise ValueError(f"Node candidate is deeper than exact line container: {link['id']}")
        if initial_candidate and exact_place and levels[exact_place] > levels[initial_candidate]:
            deeper_than_node_candidate += 1
        if endpoint_nodes and (not geometry.buffer(0.0001).covers(Point(pack.nodes[link["from"]]))
                               or not geometry.buffer(0.0001).covers(Point(pack.nodes[link["to"]]))):
            endpoint_mismatch += 1
        feature_rows.append({"feature_id": link["feature_id"], "link_id": link["id"],
                             "from_node": link["from"], "to_node": link["to"],
                             "node_candidate_loc_id": initial_candidate,
                             "containing_loc_id": exact_place,
                             "admin_level": levels.get(exact_place), "status": status})
    features_done = time.perf_counter()

    output.mkdir(parents=True, exist_ok=True)
    node_schema = pa.schema([("node_id", pa.string()), ("containing_loc_id", pa.string()),
                             ("admin_level", pa.int16()), ("status", pa.string())])
    feature_schema = pa.schema([("feature_id", pa.string()), ("link_id", pa.string()),
                                ("from_node", pa.string()), ("to_node", pa.string()),
                                ("node_candidate_loc_id", pa.string()),
                                ("containing_loc_id", pa.string()),
                                ("admin_level", pa.int16()), ("status", pa.string())])
    pq.write_table(pa.Table.from_pylist(node_rows, schema=node_schema), output / "node_loc_ids.parquet")
    pq.write_table(pa.Table.from_pylist(feature_rows, schema=feature_schema), output / "feature_loc_ids.parquet")
    written = time.perf_counter()
    report = {
        "schema_version": "network-geometry-assignment-v1",
        "pack_id": pack.manifest["pack_id"], "pack_version": pack.manifest["version"],
        "pack_manifest_files": pack.manifest["files"],
        "scope_loc_id": scope_loc_id, "network_type": network_type,
        "prepared_exact_polygons": prepare_geometries,
        "node_candidate_method": "endpoint_common_ancestor" if endpoint_nodes else "not_applicable_reach_nodes",
        "geometry_release": GEOMETRY_RELEASE,
        "geometry_source": str(archive.resolve()), "geometry_member": member_pin,
        "admin_levels": [1, 2, 3], "containment_method": "query_exact_full_geometry_covers",
        "source_features": len(geometries), "isolated_features": len(isolated_features),
        "excluded_not_fully_in_state": len(geometries) - len(isolated_features),
        "isolated_nodes": len(node_rows),
        "node_status": dict(Counter(row["status"] for row in node_rows)),
        "feature_status": dict(Counter(row["status"] for row in feature_rows)),
        "feature_admin_level": dict(Counter(str(row["admin_level"]) for row in feature_rows)),
        "node_candidate_promoted_after_line_check": promoted,
        "line_more_specific_than_node_candidate": deeper_than_node_candidate,
        "links_with_endpoint_geometry_mismatch_over_0_0001_degrees": endpoint_mismatch,
        "timing_seconds": {"read_exact_bank": round(loaded - start, 3),
                           "build_spatial_index": round(indexed - loaded, 3),
                           "isolate_scope": round(isolated - indexed, 3),
                           "node_lookup": round(nodes_done - isolated, 3),
                           "line_lookup": round(features_done - nodes_done, 3),
                           "write_parquet": round(written - features_done, 3),
                           "total_before_report": round(written - start, 3)},
        "limits": ["Admin 4-5 are outside this first trial", "Overlap and clipped-length rows are not yet built"],
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-dir", required=True, type=Path)
    parser.add_argument("--geometry-zip", required=True, type=Path)
    parser.add_argument("--scope-loc-id", "--state-loc-id", dest="scope_loc_id", default="USA-CA")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = assign(Pack.open(args.pack_dir), args.geometry_zip, args.scope_loc_id, args.output)
    print(json.dumps({key: report[key] for key in ("scope_loc_id", "isolated_features",
                                               "isolated_nodes", "node_status", "feature_status",
                                               "feature_admin_level", "timing_seconds")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
