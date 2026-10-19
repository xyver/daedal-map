from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _infer_admin_level_from_loc_id(loc_id: str) -> int:
    value = str(loc_id or "").strip()
    if not value:
        raise ValueError("loc_id is required")
    return max(0, len(value.split("-")) - 1)


def _load_results_df(results_path: Path) -> pd.DataFrame:
    if not results_path.exists():
        raise FileNotFoundError(f"Precomputed results not found: {results_path}")
    return pd.read_parquet(results_path)


def _table_admin_level(results_df: pd.DataFrame) -> int:
    if "origin_level" in results_df.columns and not results_df.empty:
        level_value = str(results_df["origin_level"].iloc[0]).strip().lower()
        if level_value.startswith("admin_"):
            try:
                return int(level_value.split("_", 1)[1])
            except ValueError:
                pass
    if results_df.empty:
        raise ValueError("Cannot detect table admin level from empty results table")
    return _infer_admin_level_from_loc_id(str(results_df["origin_id"].iloc[0]))


def _collapse_loc_id_to_admin_level(loc_id: str, target_admin_level: int) -> str:
    parts = str(loc_id or "").strip().split("-")
    if not parts or target_admin_level < 0:
        raise ValueError("Invalid loc_id collapse request")
    return "-".join(parts[: target_admin_level + 1])


def _exact_or_prefix_match(results_df: pd.DataFrame, normalized_loc_id: str) -> dict[str, Any]:
    exact_df = results_df[results_df["origin_id"].astype(str) == normalized_loc_id].copy()
    if not exact_df.empty:
        return {
            "lookup_mode": "exact_origin",
            "normalized_loc_id": normalized_loc_id,
            "result_count": len(exact_df),
            "rows": exact_df.to_dict("records"),
        }

    prefix = f"{normalized_loc_id}-"
    child_df = results_df[results_df["origin_id"].astype(str).str.startswith(prefix)].copy()
    if child_df.empty:
        return {
            "lookup_mode": "no_match",
            "normalized_loc_id": normalized_loc_id,
            "result_count": 0,
            "rows": [],
        }

    metric_columns = [
        col
        for col in child_df.columns
        if col.startswith("reachable_") or col in ("nearest_minutes", "nearest_distance_km")
    ]
    summary: dict[str, Any] = {
        "lookup_mode": "aggregated_children",
        "normalized_loc_id": normalized_loc_id,
        "child_origin_count": int(len(child_df)),
        "child_origin_level": child_df["origin_level"].iloc[0] if "origin_level" in child_df.columns else None,
        "normalized_admin_level": _infer_admin_level_from_loc_id(normalized_loc_id),
    }
    if "nearest_minutes" in child_df.columns:
        nearest = child_df["nearest_minutes"].astype(float)
        summary["nearest_minutes_min"] = round(float(nearest.min()), 4)
        summary["nearest_minutes_mean"] = round(float(nearest.mean()), 4)
        summary["nearest_minutes_median"] = round(float(nearest.median()), 4)
        summary["nearest_minutes_p90"] = round(float(nearest.quantile(0.9)), 4)
    if "nearest_distance_km" in child_df.columns:
        dist = child_df["nearest_distance_km"].astype(float)
        summary["nearest_distance_km_mean"] = round(float(dist.mean()), 4)
    for column in metric_columns:
        if column.startswith("reachable_"):
            series = child_df[column].fillna(0).astype(float)
            summary[f"{column}_sum"] = float(series.sum())
            summary[f"{column}_mean"] = round(float(series.mean()), 4)
    sample_rows = child_df.head(10).to_dict("records")
    return {
        "lookup_mode": "aggregated_children",
        "normalized_loc_id": normalized_loc_id,
        "result_count": len(child_df),
        "summary": summary,
        "sample_rows": sample_rows,
    }


def resolve_zip_to_canonical_loc_id(
    zip_code: str,
    *,
    data_root: Path,
    resolve_point_to_loc_id_stack,
) -> dict[str, Any]:
    zcta_path = data_root / "countries" / "USA" / "geometry" / "zcta" / "USA.parquet"
    if not zcta_path.exists():
        raise FileNotFoundError(f"USA ZCTA geometry not found: {zcta_path}")
    zip_str = str(zip_code).strip().zfill(5)
    zcta_loc_id = f"USA-Z-{zip_str}"
    zcta_df = pd.read_parquet(
        zcta_path,
        columns=["loc_id", "centroid_lon", "centroid_lat", "name"],
        filters=[("loc_id", "==", zcta_loc_id)],
    )
    if zcta_df.empty:
        raise ValueError(f"ZIP/ZCTA not found in geometry: {zip_str}")
    row = zcta_df.iloc[0]
    resolved = resolve_point_to_loc_id_stack(float(row["centroid_lon"]), float(row["centroid_lat"]))
    return {
        "input_kind": "zip",
        "input_value": zip_str,
        "zip_loc_id": zcta_loc_id,
        "zip_name": row.get("name"),
        "representative_lon": float(row["centroid_lon"]),
        "representative_lat": float(row["centroid_lat"]),
        "resolved": resolved,
    }


def lookup_precomputed_access(
    *,
    results_path: Path,
    normalized_loc_id: str,
) -> dict[str, Any]:
    results_df = _load_results_df(results_path)
    initial = _exact_or_prefix_match(results_df, normalized_loc_id)
    if initial.get("lookup_mode") != "no_match":
        return initial

    table_level = _table_admin_level(results_df)
    input_level = _infer_admin_level_from_loc_id(normalized_loc_id)
    if input_level > table_level:
        collapsed = _collapse_loc_id_to_admin_level(normalized_loc_id, table_level)
        collapsed_result = _exact_or_prefix_match(results_df, collapsed)
        collapsed_result["lookup_fallback"] = {
            "from_loc_id": normalized_loc_id,
            "to_loc_id": collapsed,
            "from_admin_level": input_level,
            "to_admin_level": table_level,
            "reason": "input resolved deeper than precomputed table grain",
        }
        return collapsed_result
    return initial


def format_lookup_payload(
    *,
    query: dict[str, Any],
    lookup_result: dict[str, Any],
) -> str:
    return json.dumps(
        {
            "query": query,
            "lookup_result": lookup_result,
        },
        indent=2,
    )
