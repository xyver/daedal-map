"""Compare two scenario-access result tables by origin."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


parser = argparse.ArgumentParser(description="Compare baseline and scenario Lake County access outputs.")
parser.add_argument("--baseline", type=Path, required=True)
parser.add_argument("--scenario", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()

baseline = pd.read_parquet(args.baseline).rename(columns={"nearest_minutes": "baseline_minutes"})
scenario = pd.read_parquet(args.scenario).rename(columns={"nearest_minutes": "scenario_minutes"})
columns = ["origin_id", "scenario_minutes"] + [column for column in scenario.columns if column.startswith("within_")]
comparison = baseline.merge(scenario[columns], on="origin_id", how="outer")
comparison["minutes_change"] = comparison["scenario_minutes"] - comparison["baseline_minutes"]
comparison["access_change"] = comparison["minutes_change"].map(lambda value: "unreachable" if pd.isna(value) else "improved" if value < -0.01 else "worsened" if value > 0.01 else "unchanged")
args.output.parent.mkdir(parents=True, exist_ok=True)
comparison.to_parquet(args.output, index=False)
summary = {
    "origin_count": len(comparison),
    "improved": int((comparison["access_change"] == "improved").sum()),
    "worsened": int((comparison["access_change"] == "worsened").sum()),
    "unchanged": int((comparison["access_change"] == "unchanged").sum()),
}
args.output.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
