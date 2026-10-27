"""Normalize a Lake County origin CSV/Parquet into the scenario analysis contract."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from scenario_analysis import normalize_origins


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


parser = argparse.ArgumentParser(description="Prepare coordinate origins for Lake County scenario analysis.")
parser.add_argument("--input", type=Path, required=True, help="CSV or Parquet with id/origin_id and longitude/latitude columns.")
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()

origins = normalize_origins(read_table(args.input))
args.output.parent.mkdir(parents=True, exist_ok=True)
origins.to_parquet(args.output, index=False)
print(f"Prepared {len(origins):,} origins -> {args.output}")
