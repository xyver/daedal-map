"""Build compact California planning reference data."""
from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from shapely.geometry import mapping, shape
from shapely.ops import unary_union


CA_PREFIX = "USA-CA-"


def ca_rows(path: Path, column: str = "loc_id") -> pd.DataFrame:
    frame = pd.read_parquet(path)
    return frame[frame[column].astype(str).str.startswith(CA_PREFIX)].copy()


def population_from_pl(directory: Path) -> pd.DataFrame:
    geo: dict[str, str] = {}
    with (directory / "cageo2020.pl").open(encoding="latin-1", newline="") as source:
        for row in csv.reader(source, delimiter="|"):
            if len(row) > 34 and row[2] == "750":
                geo[row[7]] = f"USA-CA-{row[14]}-{row[32]}-{row[33]}-{row[34]}"
    rows = []
    with (directory / "ca000012020.pl").open(encoding="latin-1", newline="") as source:
        for row in csv.reader(source, delimiter="|"):
            if len(row) > 5 and row[2:4] == ["000", "01"] and row[4] in geo:
                rows.append({"loc_id": geo[row[4]], "population": int(row[5] or 0), "year": 2020})
    return pd.DataFrame(rows)


def population_qa(population: pd.DataFrame, geometry: Path) -> dict:
    geometry_ids = set(pd.read_parquet(geometry, columns=["loc_id"])["loc_id"].astype(str))
    ids = set(population["loc_id"])
    lake = population[population["loc_id"].str.startswith("USA-CA-033-")]
    return {
        "row_count": len(population),
        "duplicate_loc_id_count": int(population.loc_id.duplicated().sum()),
        "missing_population_count": int(population.population.isna().sum()),
        "negative_population_count": int((population.population < 0).sum()),
        "zero_population_count": int((population.population == 0).sum()),
        "geometry_match_count": len(ids & geometry_ids),
        "geometry_only_count": len(geometry_ids - ids),
        "population_only_count": len(ids - geometry_ids),
        "lake_county_block_count": len(lake),
        "lake_county_population": int(lake.population.sum()),
    }


parser = argparse.ArgumentParser()
parser.add_argument("--source-data-root", type=Path, required=True)
parser.add_argument("--pl-directory", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, default=Path(__file__).parents[1] / "reference_data" / "california")
args = parser.parse_args()

usa = args.source_data_root / "countries" / "USA"
args.output_dir.mkdir(parents=True, exist_ok=True)
geometry_output = args.output_dir / "geometry"
geometry_output.mkdir(parents=True, exist_ok=True)
population = population_from_pl(args.pl_directory)
shelters = pd.read_parquet(usa / "nss_shelters" / "shelters.parquet")
outputs = {
    "population_blocks_2020.parquet": population,
    "cejst_classification.parquet": ca_rows(usa / "cejst_classification" / "classification.parquet"),
    "cejst_burdens.parquet": ca_rows(usa / "cejst_burdens" / "burdens.parquet"),
    "nri_counties.parquet": ca_rows(usa / "disasters" / "fema_nri" / "USA_full.parquet"),
    "future_nri_counties.parquet": ca_rows(usa / "future_nri" / "USA.parquet"),
    "nss_shelters.parquet": shelters[shelters.state.astype(str).str.upper().eq("CA")].copy(),
}
manifest = {
    "schema_version": "road-network-editor-california-reference-v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "population_source": "2020 Census P.L. 94-171 P1_001N total population",
    "population_qa": population_qa(population, usa / "geometry" / "block" / "USA-CA.parquet"),
    "files": {},
}
for filename, frame in outputs.items():
    output = args.output_dir / filename
    frame.to_parquet(output, index=False, compression="zstd")
    manifest["files"][filename] = {"row_count": len(frame), "bytes": output.stat().st_size}
counties = ca_rows(usa / "geometry" / "county.parquet")
county_path = geometry_output / "counties.parquet"
counties.to_parquet(county_path, index=False, compression="zstd")
for level in ("tract", "blockgroup"):
    source = usa / "geometry" / level / "USA-CA.parquet"
    shutil.copy2(source, geometry_output / f"{level}s.parquet")
state_geometry = unary_union([shape(json.loads(value) if isinstance(value, str) else value) for value in counties.geometry])
state_payload = {
    "type": "FeatureCollection",
    "features": [{"type": "Feature", "properties": {"loc_id": "USA-CA", "name": "California", "admin_level": 1}, "geometry": mapping(state_geometry)}],
}
(geometry_output / "state.geojson").write_text(json.dumps(state_payload, separators=(",", ":")), encoding="utf-8")
manifest["geometry"] = {
    "included_levels": ["state", "county", "tract", "blockgroup"],
    "excluded_levels": ["block"],
    "reason_blocks_excluded": "The California block geometry is about 249 MB and is kept as a future release/input bundle rather than normal Git data.",
    "files": {path.name: path.stat().st_size for path in sorted(geometry_output.iterdir())},
}
(args.output_dir / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
print(json.dumps(manifest, indent=2))
