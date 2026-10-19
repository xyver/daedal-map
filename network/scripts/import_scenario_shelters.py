"""Import a CSV/Parquet list of custom shelters into the active editor scenario."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scenario_analysis import DEFAULT_DATA_DIR, NodeSnapper, normalize_candidates, read_json, snap_summary


def read_table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


parser = argparse.ArgumentParser(description="Import road-snapped custom shelters into an editor scenario.")
parser.add_argument("--input", type=Path, required=True, help="Candidate CSV/Parquet with candidate_id/id, longitude/lon, latitude/lat, optional name.")
parser.add_argument("--scenario-id", default="default")
parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
parser.add_argument("--max-snap-meters", type=float, default=3000)
parser.add_argument("--replace-added", action="store_true", help="Replace existing custom shelters instead of merging by ID.")
args = parser.parse_args()

frame = normalize_candidates(read_table(args.input))
graph = read_json(args.data_dir / "baseline" / "graph.json")
summary = snap_summary(graph, frame, "candidate_id", args.max_snap_meters)
if summary["off_network_count"]:
    raise ValueError(f"Refusing to import {summary['off_network_count']} off-network location(s): {summary['off_network_ids']}. Increase --max-snap-meters only intentionally.")

scenario_path = args.data_dir / "scenarios" / f"{args.scenario_id}.json"
scenario = read_json(scenario_path) if scenario_path.exists() else {"scenario_id": args.scenario_id, "segment_states": {}, "shelter_changes": {"added": [], "removed_ids": []}}
changes = scenario.setdefault("shelter_changes", {"added": [], "removed_ids": []})
existing = {} if args.replace_added else {str(item.get("id")): item for item in changes.get("added", []) if isinstance(item, dict)}
for row in frame.itertuples(index=False):
    shelter_id = f"csv-{row.candidate_id}"
    existing[shelter_id] = {"id": shelter_id, "name": str(row.name), "longitude": float(row.longitude), "latitude": float(row.latitude), "source": "csv"}
changes["added"] = list(existing.values())
changes["removed_ids"] = [value for value in changes.get("removed_ids", []) if value not in existing]
scenario["shelter_changes"] = changes
scenario_path.parent.mkdir(parents=True, exist_ok=True)
scenario_path.write_text(json.dumps(scenario, indent=2, sort_keys=True), encoding="utf-8")
summary["imported_shelter_count"] = len(frame)
summary["scenario_id"] = args.scenario_id
print(json.dumps(summary, indent=2))
