"""Export the active editor scenario as a portable analysis JSON package."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scenario_analysis import DEFAULT_DATA_DIR, graph_fingerprint, load_active_scenario, read_json


parser = argparse.ArgumentParser()
parser.add_argument("--scenario-id", default="default")
parser.add_argument("--saved-name", help="Export a named saved scenario, including its stored start point.")
parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()

graph_path = args.data_dir / "baseline" / "graph.json"
scope = read_json(args.data_dir / "baseline" / "scope.json")
if args.saved_name:
    safe_name = "".join(char for char in args.saved_name if char.isalnum() or char in "-_").strip("-_")
    saved_path = args.data_dir / "scenarios" / "saved" / f"{safe_name}.json"
    if not saved_path.exists():
        raise FileNotFoundError(f"Saved scenario not found: {args.saved_name}")
    scenario = read_json(saved_path)
    scenario_id = scenario.get("name", args.saved_name)
else:
    scenario = load_active_scenario(args.data_dir, args.scenario_id)
    scenario_id = args.scenario_id
payload = {
    "schema_version": "road-network-editor-scenario-v1",
    "exported_at": datetime.now(timezone.utc).isoformat(),
    "scenario_id": scenario_id,
    "baseline_graph_fingerprint": graph_fingerprint(graph_path),
    "scope": {"counties": scope.get("counties"), "cross_border_buffer_degrees": scope.get("cross_border_buffer_degrees")},
    "segment_states": scenario.get("segment_states", {}),
    "shelter_changes": scenario.get("shelter_changes", {"added": [], "removed_ids": []}),
    "start_point": scenario.get("start_point"),
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Exported scenario to {args.output}")
