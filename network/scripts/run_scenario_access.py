"""Calculate nearest active-shelter access for many Lake County origins in one graph pass."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scenario_analysis import DEFAULT_DATA_DIR, access_results, load_scenario, normalize_origins


def read_table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


parser = argparse.ArgumentParser(description="Run scenario-aware Lake County shelter access.")
parser.add_argument("--origins", type=Path, required=True, help="Prepared origin Parquet or coordinate CSV.")
parser.add_argument("--scenario", type=Path, help="Exported scenario JSON; omit for the active local scenario.")
parser.add_argument("--scenario-id", default="default")
parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
parser.add_argument("--thresholds", default="15,30,60")
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()

thresholds = sorted({int(value.strip()) for value in args.thresholds.split(",") if value.strip()})
scenario = load_scenario(args.scenario, args.data_dir, args.scenario_id)
results = access_results(args.data_dir, scenario, normalize_origins(read_table(args.origins)), thresholds)
args.output.parent.mkdir(parents=True, exist_ok=True)
results.to_parquet(args.output, index=False)
summary = {
    "origin_count": len(results),
    "thresholds_minutes": thresholds,
    "reachable_origin_counts": {str(value): int(results[f"within_{value}min"].sum()) for value in thresholds},
    "reachable_weight": {str(value): float(results.loc[results[f"within_{value}min"], "weight"].sum()) for value in thresholds},
    "unreachable_origin_count": int(results["nearest_minutes"].isna().sum()),
}
summary_path = args.output.with_suffix(".summary.json")
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
