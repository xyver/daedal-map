"""Stdio MCP tools for Lake County scenario analysis.

The server accepts only files beneath data/analysis so an LLM works with
portable, deliberately supplied inputs rather than arbitrary host paths.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pandas as pd
from mcp.server.fastmcp import FastMCP

ROOT = Path(__file__).parents[1]
DATA_DIR = Path(os.environ.get("ROAD_EDITOR_DATA_DIR", ROOT / "data"))
ANALYSIS_DIR = DATA_DIR / "analysis"
SCRIPTS_DIR = ROOT / "scripts"
mcp = FastMCP("Lake County Road Network Editor", json_response=True)


def safe_analysis_path(name: str, required_suffixes: tuple[str, ...] | None = None) -> Path:
    path = (ANALYSIS_DIR / name).resolve()
    if ANALYSIS_DIR.resolve() not in path.parents:
        raise ValueError("Only relative files inside data/analysis are allowed.")
    if required_suffixes and path.suffix.lower() not in required_suffixes:
        raise ValueError(f"Expected one of: {', '.join(required_suffixes)}")
    return path


def run_script(script: str, arguments: list[str]) -> dict:
    result = subprocess.run([sys.executable, str(SCRIPTS_DIR / script), *arguments], cwd=ROOT, capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError(result.stderr.strip() or result.stdout.strip() or f"{script} failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"message": result.stdout.strip()}


def table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


@mcp.tool()
def workspace_summary() -> dict:
    """Describe the fixed Lake County graph, available saves, and input directory."""
    scope = json.loads((DATA_DIR / "baseline" / "scope.json").read_text(encoding="utf-8"))
    saved = []
    for path in sorted((DATA_DIR / "scenarios" / "saved").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        saved.append({"name": payload.get("name", path.stem), "saved_at": payload.get("saved_at")})
    return {"scope": scope.get("counties"), "saved_scenarios": saved, "analysis_directory": "data/analysis", "csv_contract": "longitude/latitude plus origin_id or candidate_id; lon/lat and id aliases are accepted"}


@mcp.tool()
def inspect_locations(filename: str, kind: Literal["origins", "candidates"], max_snap_meters: float = 3000) -> dict:
    """Validate a CSV/Parquet location file and report automatic graph-snap quality without changing a scenario."""
    sys.path.insert(0, str(SCRIPTS_DIR))
    from scenario_analysis import normalize_candidates, normalize_origins, read_json, snap_summary
    path = safe_analysis_path(filename, (".csv", ".parquet"))
    frame = normalize_origins(table(path)) if kind == "origins" else normalize_candidates(table(path))
    graph = read_json(DATA_DIR / "baseline" / "graph.json")
    identifier = "origin_id" if kind == "origins" else "candidate_id"
    result = snap_summary(graph, frame, identifier, max_snap_meters)
    result.update({"kind": kind, "filename": filename, "bounds": {"west": float(frame.longitude.min()), "south": float(frame.latitude.min()), "east": float(frame.longitude.max()), "north": float(frame.latitude.max())}})
    return result


@mcp.tool()
def export_scenario(saved_name: str = "", output_name: str = "scenario.json") -> dict:
    """Export the active UI scenario or a named UI save as a portable analysis JSON file."""
    output = safe_analysis_path(output_name, (".json",))
    arguments = ["--data-dir", str(DATA_DIR), "--output", str(output)]
    if saved_name:
        arguments.extend(["--saved-name", saved_name])
    run_script("export_scenario.py", arguments)
    payload = json.loads(output.read_text(encoding="utf-8"))
    return {"scenario_file": f"data/analysis/{output.name}", "scenario_id": payload["scenario_id"], "road_edits": len(payload["segment_states"]), "added_shelters": len(payload["shelter_changes"].get("added", [])), "removed_shelters": len(payload["shelter_changes"].get("removed_ids", []))}


@mcp.tool()
def import_custom_shelters(filename: str, max_snap_meters: float = 3000, replace_added: bool = False) -> dict:
    """Add a validated CSV/Parquet candidate list as custom shelters to the active UI scenario."""
    input_path = safe_analysis_path(filename, (".csv", ".parquet"))
    arguments = ["--input", str(input_path), "--data-dir", str(DATA_DIR), "--max-snap-meters", str(max_snap_meters)]
    if replace_added:
        arguments.append("--replace-added")
    return run_script("import_scenario_shelters.py", arguments)


@mcp.tool()
def run_access(origins_file: str, scenario_file: str = "", thresholds_minutes: str = "15,30,60", output_name: str = "access.parquet") -> dict:
    """Calculate nearest active-shelter access for every origin under an exported scenario or the active UI scenario."""
    origins = safe_analysis_path(origins_file, (".csv", ".parquet"))
    output = safe_analysis_path(output_name, (".parquet",))
    arguments = ["--origins", str(origins), "--data-dir", str(DATA_DIR), "--thresholds", thresholds_minutes, "--output", str(output)]
    if scenario_file:
        arguments.extend(["--scenario", str(safe_analysis_path(scenario_file, (".json",)))])
    summary = run_script("run_scenario_access.py", arguments)
    summary["result_file"] = f"data/analysis/{output.name}"
    return summary


@mcp.tool()
def compare_access(baseline_file: str, scenario_file: str, output_name: str = "comparison.parquet") -> dict:
    """Compare two access result Parquet files by origin and return improved/worsened counts."""
    output = safe_analysis_path(output_name, (".parquet",))
    summary = run_script("compare_scenarios.py", ["--baseline", str(safe_analysis_path(baseline_file, (".parquet",))), "--scenario", str(safe_analysis_path(scenario_file, (".parquet",))), "--output", str(output)])
    summary["result_file"] = f"data/analysis/{output.name}"
    return summary


@mcp.tool()
def optimize_shelters(origins_file: str, candidates_file: str, scenario_file: str = "", count: int = 10, threshold_minutes: int = 30, output_name: str = "recommendations.json") -> dict:
    """Rank candidate sites by greedy weighted coverage of origins currently outside the travel-time threshold."""
    if not 1 <= count <= 100 or not 1 <= threshold_minutes <= 240:
        raise ValueError("count must be 1..100 and threshold_minutes must be 1..240")
    output = safe_analysis_path(output_name, (".json",))
    arguments = ["--origins", str(safe_analysis_path(origins_file, (".csv", ".parquet"))), "--candidates", str(safe_analysis_path(candidates_file, (".csv", ".parquet"))), "--data-dir", str(DATA_DIR), "--count", str(count), "--threshold-minutes", str(threshold_minutes), "--output", str(output)]
    if scenario_file:
        arguments.extend(["--scenario", str(safe_analysis_path(scenario_file, (".json",)))])
    result = run_script("optimize_shelters.py", arguments)
    result["result_file"] = f"data/analysis/{output.name}"
    return result


if __name__ == "__main__":
    mcp.run()
