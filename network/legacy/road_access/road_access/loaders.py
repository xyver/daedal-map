from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .schemas import PreparedInputsManifest


USA_ADMIN_LEVEL_TO_GEOMETRY = {
    "admin_2": ("county.parquet", False),
    "admin_3": ("tract", True),
    "admin_4": ("blockgroup", True),
    "admin_5": ("block", True),
}


def get_usa_geometry_path(data_root: Path, state_code: str, origin_level: str) -> Path:
    if origin_level not in USA_ADMIN_LEVEL_TO_GEOMETRY:
        raise ValueError(f"Unsupported USA origin level: {origin_level}")
    folder_or_file, partitioned = USA_ADMIN_LEVEL_TO_GEOMETRY[origin_level]
    geometry_root = data_root / "countries" / "USA" / "geometry"
    if partitioned:
        return geometry_root / folder_or_file / f"USA-{state_code}.parquet"
    return geometry_root / folder_or_file


def load_usa_admin_origins(
    data_root: Path,
    *,
    state_code: str,
    origin_level: str,
    representative_point_method: str = "geometry_centroid",
) -> pd.DataFrame:
    geometry_path = get_usa_geometry_path(data_root, state_code, origin_level)
    if not geometry_path.exists():
        raise FileNotFoundError(f"Origin geometry not found: {geometry_path}")

    columns = [
        "loc_id",
        "parent_id",
        "name",
        "centroid_lon",
        "centroid_lat",
    ]
    df = pd.read_parquet(geometry_path, columns=columns).copy()
    state_prefix = f"USA-{state_code}-"
    df = df[df["loc_id"].astype(str).str.startswith(state_prefix)].copy()
    df = df.dropna(subset=["centroid_lon", "centroid_lat"]).copy()
    df["origin_id"] = df["loc_id"]
    df["origin_name"] = df["name"]
    df["origin_level"] = origin_level
    df["country_iso3"] = "USA"
    df["state_code"] = state_code
    df["representative_lon"] = df["centroid_lon"].astype(float)
    df["representative_lat"] = df["centroid_lat"].astype(float)
    df["representative_point_method"] = representative_point_method
    return df[
        [
            "origin_id",
            "origin_name",
            "origin_level",
            "country_iso3",
            "state_code",
            "parent_id",
            "representative_lon",
            "representative_lat",
            "representative_point_method",
        ]
    ].rename(columns={"parent_id": "normalized_parent_loc_id"})


def load_nss_shelter_destinations(
    data_root: Path,
    *,
    state_code: str,
    destination_family: str = "shelter",
) -> pd.DataFrame:
    shelters_path = data_root / "countries" / "USA" / "nss_shelters" / "shelters.parquet"
    if not shelters_path.exists():
        raise FileNotFoundError(f"NSS shelters parquet not found: {shelters_path}")

    columns = [
        "location_id",
        "shelter_id",
        "shelter_name",
        "loc_id",
        "state",
        "latitude",
        "longitude",
        "is_currently_open",
        "is_currently_active",
        "is_candidate_shelter",
        "evacuation_capacity",
        "post_impact_capacity",
        "total_population",
        "assigned_geo_level",
    ]
    df = pd.read_parquet(shelters_path, columns=columns).copy()
    df = df[df["state"].astype(str).str.upper() == state_code.upper()].copy()
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df["destination_id"] = df["location_id"].fillna(df["shelter_id"])
    df["destination_name"] = df["shelter_name"]
    df["destination_family"] = destination_family
    df["country_iso3"] = "USA"
    df["state_code"] = state_code
    df["destination_lon"] = df["longitude"].astype(float)
    df["destination_lat"] = df["latitude"].astype(float)
    return df[
        [
            "destination_id",
            "destination_name",
            "destination_family",
            "country_iso3",
            "state_code",
            "loc_id",
            "assigned_geo_level",
            "destination_lon",
            "destination_lat",
            "is_currently_open",
            "is_currently_active",
            "is_candidate_shelter",
            "evacuation_capacity",
            "post_impact_capacity",
            "total_population",
        ]
    ].rename(
        columns={
            "loc_id": "source_loc_id",
            "assigned_geo_level": "source_geo_level",
        }
    )


def write_prepared_inputs(
    *,
    origins_df: pd.DataFrame,
    destinations_df: pd.DataFrame,
    output_dir: Path,
    manifest: PreparedInputsManifest,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    origins_path = output_dir / "origins.parquet"
    destinations_path = output_dir / "destinations.parquet"
    manifest_path = output_dir / "manifest.json"
    origins_df.to_parquet(origins_path, index=False)
    destinations_df.to_parquet(destinations_path, index=False)
    manifest_path.write_text(json.dumps(manifest.__dict__, indent=2), encoding="utf-8")
