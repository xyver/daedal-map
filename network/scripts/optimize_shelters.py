"""Greedy maximum-coverage recommendations for a small Lake County scenario."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scenario_analysis import (
    DEFAULT_DATA_DIR,
    NodeSnapper,
    access_results,
    load_scenario,
    nearest_shelter_distances,
    normalize_origins,
    read_json,
)


def read_table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


parser = argparse.ArgumentParser(description="Recommend shelter candidates using greedy maximum coverage.")
parser.add_argument("--origins", type=Path, required=True)
parser.add_argument("--candidates", type=Path, required=True, help="CSV/Parquet with candidate_id/id, longitude, latitude, optional name.")
parser.add_argument("--scenario", type=Path)
parser.add_argument("--scenario-id", default="default")
parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
parser.add_argument("--threshold-minutes", type=int, default=30)
parser.add_argument("--count", type=int, default=10)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()

scenario = load_scenario(args.scenario, args.data_dir, args.scenario_id)
origins = normalize_origins(read_table(args.origins)).reset_index(drop=True)
baseline = access_results(args.data_dir, scenario, origins, [args.threshold_minutes])
underserved = baseline[~baseline[f"within_{args.threshold_minutes}min"]].copy()
candidate_frame = read_table(args.candidates).rename(columns={"id": "candidate_id", "lon": "longitude", "lat": "latitude"})
required = {"candidate_id", "longitude", "latitude"}
if missing := required - set(candidate_frame.columns):
    raise ValueError(f"Candidate input is missing {sorted(missing)}")
if "name" not in candidate_frame:
    candidate_frame["name"] = candidate_frame["candidate_id"]
graph = read_json(args.data_dir / "baseline" / "graph.json")
snapper = NodeSnapper.from_graph(graph)
origin_nodes = {row.origin_id: snapper.snap(row.longitude, row.latitude)[0] for row in underserved.itertuples(index=False)}
coverage: dict[str, set[str]] = {}
candidate_rows = {}
for row in candidate_frame.itertuples(index=False):
    candidate = {"id": str(row.candidate_id), "name": str(row.name), "longitude": float(row.longitude), "latitude": float(row.latitude)}
    distances, _ = nearest_shelter_distances(graph, scenario, [candidate], max_minutes=args.threshold_minutes)
    coverage[candidate["id"]] = {origin_id for origin_id, node_id in origin_nodes.items() if node_id in distances}
    candidate_rows[candidate["id"]] = candidate
weights = underserved.set_index("origin_id")["weight"].to_dict()
uncovered = set(underserved["origin_id"].astype(str))
selected = []
for _ in range(args.count):
    choices = []
    for candidate_id, covered in coverage.items():
        if any(item["candidate_id"] == candidate_id for item in selected):
            continue
        newly_covered = covered & uncovered
        choices.append((sum(float(weights[origin_id]) for origin_id in newly_covered), candidate_id, newly_covered))
    if not choices:
        break
    score, candidate_id, newly_covered = max(choices, key=lambda item: (item[0], item[1]))
    if score <= 0:
        break
    selected.append({**candidate_rows[candidate_id], "candidate_id": candidate_id, "newly_covered_origins": len(newly_covered), "newly_covered_weight": score})
    uncovered -= newly_covered
args.output.parent.mkdir(parents=True, exist_ok=True)
payload = {"threshold_minutes": args.threshold_minutes, "requested_shelters": args.count, "underserved_origin_count": len(underserved), "remaining_underserved_origin_count": len(uncovered), "selected": selected}
args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(json.dumps(payload, indent=2))
