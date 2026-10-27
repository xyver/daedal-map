"""Benchmark real NARN rail subsets against one exact USA Geometry release.

Temporary assignment Parquet files are removed after each size. Only the
compact JSON summary named by --output persists.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path

from shapely import from_wkb, prepare
from shapely.geometry import Point, box, shape
from shapely.strtree import STRtree

from .app.network_pack import Pack
from .geometry_assign import _admin_rows, assign


def profile(pack_dir: Path, geometry_zip: Path, sizes: list[int], output: Path) -> dict:
    """Profile sample dispersion and line complexity without assigning IDs."""
    pack = Pack.open(pack_dir)
    admin_rows = _admin_rows(geometry_zip, "USA")[0]
    state_rows = [row for row in admin_rows
                  if int(row["admin_level"]) == 1]
    full_tree = STRtree([from_wkb(row["geometry"]) for row in admin_rows])
    state_ids = [row["loc_id"] for row in state_rows]
    states = [from_wkb(row["geometry"]) for row in state_rows]
    prepare(states)
    tree = STRtree(states)
    ordered = list(pack.links)
    random.Random(20261026).shuffle(ordered)
    by_feature = {feature["properties"]["feature_id"]: feature for feature in pack.features}
    results = []
    for size in sizes:
        links = ordered[:size]
        node_ids = {link[end] for link in links for end in ("from", "to")}
        node_states = set()
        outside_state = 0
        point_candidates = []
        for node_id in node_ids:
            point = Point(pack.nodes[node_id])
            point_candidates.append(len(full_tree.query(point)))
            matches = [state_ids[int(index)] for index in tree.query(point)
                       if states[int(index)].covers(point)]
            if matches:
                node_states.update(matches)
            else:
                outside_state += 1
        vertex_counts = []
        state_bbox_candidates = []
        full_line_candidates = []
        for link in links:
            geojson = by_feature[link["feature_id"]]["geometry"]
            coordinates = geojson["coordinates"]
            vertex_counts.append(sum(len(part) for part in coordinates)
                                 if geojson["type"] == "MultiLineString" else len(coordinates))
            line = shape(geojson)
            state_bbox_candidates.append(len(tree.query(box(*line.bounds))))
            full_line_candidates.append(len(full_tree.query(line)))
        results.append({
            "input_links": size, "input_nodes": len(node_ids),
            "states_touched_by_nodes": len(node_states),
            "nodes_outside_usa_state_polygons": outside_state,
            "vertices_mean": round(statistics.mean(vertex_counts), 2),
            "vertices_p95": sorted(vertex_counts)[int(.95 * (len(vertex_counts) - 1))],
            "vertices_max": max(vertex_counts),
            "state_bbox_candidates_mean": round(statistics.mean(state_bbox_candidates), 2),
            "state_bbox_candidates_p95": sorted(state_bbox_candidates)[
                int(.95 * (len(state_bbox_candidates) - 1))],
            "point_admin_bbox_candidates_mean": round(statistics.mean(point_candidates), 2),
            "point_admin_bbox_candidates_p95": sorted(point_candidates)[
                int(.95 * (len(point_candidates) - 1))],
            "line_admin_bbox_candidates_mean": round(statistics.mean(full_line_candidates), 2),
            "line_admin_bbox_candidates_p95": sorted(full_line_candidates)[
                int(.95 * (len(full_line_candidates) - 1))],
        })
    summary = {"schema_version": "network-geometry-spatial-profile-v1",
               "source_pack": pack.manifest["pack_id"],
               "network_type": pack.manifest["network_type"],
               "sample_method": "random seed 20261026; first N links",
               "results": results}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def point_only(pack_dir: Path, geometry_zip: Path, sizes: list[int], output: Path) -> dict:
    """Time equal-sized real node-coordinate batches on one warm exact index."""
    pack = Pack.open(pack_dir)
    rows = _admin_rows(geometry_zip, "USA")[0]
    polygons = [from_wkb(row["geometry"]) for row in rows]
    prepare(polygons)
    tree = STRtree(polygons)
    node_ids = list(pack.nodes)
    random.Random(20261026).shuffle(node_ids)
    if max(sizes) > len(node_ids):
        raise ValueError("Requested more point coordinates than the pack contains")
    results = []
    for size in sizes:
        points = [Point(pack.nodes[node_id]) for node_id in node_ids[:size]]
        timings = []
        matched = []
        for _ in range(3):
            start = time.perf_counter()
            count = 0
            for point in points:
                count += sum(polygons[int(index)].covers(point)
                             for index in tree.query(point))
            timings.append(round(time.perf_counter() - start, 3))
            matched.append(count)
        results.append({"points": size, "three_pass_seconds": timings,
                        "median_seconds": round(statistics.median(timings), 3),
                        "exact_polygon_matches": matched[0]})
    summary = {"schema_version": "network-geometry-point-benchmark-v1",
               "network_type": pack.manifest["network_type"],
               "sample_method": "random seed 20261026; first N distinct real graph nodes",
               "method": "one loaded, prepared exact USA Admin0-3 index; three in-process passes",
               "results": results}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def run(pack_dir: Path, geometry_zip: Path, sizes: list[int], output: Path) -> dict:
    started = time.perf_counter()
    pack = Pack.open(pack_dir)
    load_seconds = time.perf_counter() - started
    if pack.manifest["network_type"] not in {"rail", "power", "river"}:
        raise ValueError("Benchmark requires a rail, power, or river pack")
    ordered = list(pack.links)
    random.Random(20261026).shuffle(ordered)
    by_feature = {feature["properties"]["feature_id"]: feature for feature in pack.features}
    results = []
    for size in sizes:
        if size > len(ordered):
            raise ValueError(f"Requested {size} links, pack has {len(ordered)}")
        links = ordered[:size]
        node_ids = {link[end] for link in links for end in ("from", "to")}
        features = [by_feature[link["feature_id"]] for link in links]
        subset = Pack(pack.directory, pack.manifest,
                      {node_id: pack.nodes[node_id] for node_id in node_ids}, links, features)
        with tempfile.TemporaryDirectory(prefix=f"network_geometry_{size}_") as temporary:
            report = assign(subset, geometry_zip, "USA", Path(temporary))
        result = {
            "requested_links": size, "input_nodes": len(node_ids),
            "source_states": dict(sorted(Counter(
                str(feature["properties"].get("state") or "unknown") for feature in features
            ).items())),
            "isolated_links": report["isolated_features"],
            "excluded_not_fully_in_country": report["excluded_not_fully_in_state"],
            "isolated_nodes": report["isolated_nodes"],
            "node_status": report["node_status"],
            "feature_status": report["feature_status"],
            "feature_admin_level": report["feature_admin_level"],
            "node_candidate_promoted_after_line_check": report["node_candidate_promoted_after_line_check"],
            "line_more_specific_than_node_candidate": report["line_more_specific_than_node_candidate"],
            "links_with_endpoint_geometry_mismatch_over_0_0001_degrees": report[
                "links_with_endpoint_geometry_mismatch_over_0_0001_degrees"],
            "timing_seconds": report["timing_seconds"],
        }
        results.append(result)
        print(json.dumps({"requested_links": size, "input_nodes": len(node_ids),
                          "timing_seconds": report["timing_seconds"]}), flush=True)
    summary = {
        "schema_version": "network-geometry-benchmark-v1",
        "source_pack": pack.manifest["pack_id"], "source_version": pack.manifest["version"],
        "network_type": pack.manifest["network_type"],
        "source_bbox": pack.manifest["bbox"],
        "source_feature_sha256": pack.manifest["files"]["features.geojson"]["sha256"],
        "source_pack_links": len(pack.links), "source_pack_nodes": len(pack.nodes),
        "geometry_release": "usa-geometry-detail-1.3.5",
        "scope_loc_id": "USA", "admin_levels": [0, 1, 2, 3],
        "sample_method": "random seed 20261026; first N links from one shuffled national pack",
        "pack_load_seconds": round(load_seconds, 3),
        "run_kind": "local cold assignment per size; same process and OS file cache; output Parquet temporary",
        "results": results,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-dir", required=True, type=Path)
    parser.add_argument("--geometry-zip", required=True, type=Path)
    parser.add_argument("--sizes", nargs="+", type=int, default=[10000, 20000, 50000])
    parser.add_argument("--profile-only", action="store_true",
                        help="Count geographic spread and line complexity without assignment")
    parser.add_argument("--points-only", action="store_true",
                        help="Time repeated equal-sized node point lookups on one loaded index")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.profile_only and args.points_only:
        parser.error("Choose one benchmark mode")
    if args.points_only:
        print(json.dumps(point_only(args.pack_dir, args.geometry_zip, args.sizes, args.output)["results"],
                         indent=2))
    elif args.profile_only:
        print(json.dumps(profile(args.pack_dir, args.geometry_zip, args.sizes, args.output)["results"],
                         indent=2))
    else:
        run(args.pack_dir, args.geometry_zip, args.sizes, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
