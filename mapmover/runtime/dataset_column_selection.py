"""Fast structural selection of geography columns in a dataset profile.

This module does not open identity, reference, graph, or geometry artifacts.
The selected identifier column is verified separately by
``identify_reference_system`` in a declared country scope.
"""

from __future__ import annotations

import re
from typing import Any, Callable


_IDENTIFIER_HEADERS = {
    "loc_id", "locid", "daedalmap_loc_id", "geoid", "geoid10", "geoid20",
    "fips", "state_fips", "county_fips", "zip", "zip_code", "zipcode",
    "zcta", "zcta5", "postal_code", "postcode", "country_code", "iso3",
    "iso_3", "location_code", "municipality_code", "place_code",
    "admin_code", "admin_id", "region_code", "region_id", "district_code",
    "district_id", "watershed_code", "watershed_id", "huc", "huc8",
    "huc10", "huc12", "aiannhce", "reference_id", "geography_id",
}
_AXIS_TOKENS = {"latitude", "lat", "longitude", "lon", "lng", "long"}


def _header(name: Any) -> str:
    split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name or ""))
    return re.sub(r"[^a-z0-9]+", "_", split.lower()).strip("_")


def _axis(name: str) -> tuple[str, str] | None:
    parts = _header(name).split("_")
    if parts == ["y"]:
        return "lat", ""
    if parts == ["x"]:
        return "lon", ""
    lat = [part for part in parts if part in {"latitude", "lat"}]
    lon = [part for part in parts if part in {"longitude", "lon", "lng", "long"}]
    if len(lat) + len(lon) != 1:
        return None
    stem = "_".join(part for part in parts if part not in _AXIS_TOKENS)
    return ("lat" if lat else "lon"), stem


def identify_dataset_geography(
    columns: list[dict[str, Any]], *, dataset_context: dict[str, Any] | None = None,
    country_scope: str | None = None, cancelled: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Select recognized columns without interpreting their identifier values."""
    del dataset_context
    if not isinstance(columns, list) or not columns:
        return {"ok": False, "status": "invalid_request", "error": {
            "code": "dataset_columns_required", "message": "Supply sampled named columns."}}
    if len(columns) > 64:
        return {"ok": False, "status": "invalid_request", "error": {
            "code": "too_many_columns", "message": "Supply at most 64 columns."}}
    country = str(country_scope or "").strip().upper()
    if country and not re.fullmatch(r"[A-Z]{3}", country):
        return {"ok": False, "status": "invalid_request", "error": {
            "code": "invalid_country_scope", "message": "country_scope must be one ISO3 country."}}
    clean = []
    for raw in columns:
        if not isinstance(raw, dict) or not str(raw.get("name") or "").strip():
            continue
        values = [str(value).strip() for value in (raw.get("values") or [])[:32]
                  if isinstance(value, (str, int, float))]
        clean.append({"name": str(raw["name"]).strip(), "values": values,
                      "nonempty_count": raw.get("nonempty_count")})
    candidates: list[dict[str, Any]] = []
    axes: dict[tuple[str, str], dict[str, Any]] = {}
    for column in clean:
        axis = _axis(column["name"])
        if axis:
            axes[axis] = column
    for (kind, stem), latitude in axes.items():
        if kind != "lat" or ("lon", stem) not in axes:
            continue
        longitude = axes[("lon", stem)]
        pairs = []
        for raw_lat, raw_lon in zip(latitude["values"], longitude["values"]):
            try:
                lat, lon = float(raw_lat), float(raw_lon)
            except ValueError:
                continue
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                pairs.append(f"{lat},{lon}")
        if pairs:
            candidates.append({"id": f"dataset-coordinates-{stem or 'default'}",
                "kind": "coordinates", "header": f"{latitude['name']} + {longitude['name']}",
                "columns": [latitude["name"], longitude["name"]], "sampleValues": pairs,
                "nonempty": len(pairs), "formatMatchRate": round(len(pairs) / max(1, len(latitude["values"])), 6),
                "countryScope": country, "catalog": {"ok": True, "status": "matched",
                    "recommended_binding": {"mode": "coordinates", "columns": [latitude["name"], longitude["name"]]}}})
    if len(candidates) > 1:
        candidates = [max(candidates, key=lambda item: (len(item["sampleValues"]),
            int(item["nonempty"]), item["header"]))]
    for column in clean:
        if _header(column["name"]) not in _IDENTIFIER_HEADERS or not any(column["values"]):
            continue
        candidates.append({"id": f"dataset-column-{len(candidates) + 1}",
            "kind": "reference", "header": column["name"], "columns": [column["name"]],
            "sampleValues": list(dict.fromkeys(value for value in column["values"] if value)),
            "nonempty": int(column["nonempty_count"] or len(column["values"])),
            "formatMatchRate": 1.0, "localConfidence": "", "serverHinted": True,
            "confidenceScore": 0.0, "expectedSystem": "", "expectedLevel": "",
            "countryScope": country, "catalog": {"ok": True, "status": "not_checked",
                "recommended_binding": None}})
    candidates.sort(key=lambda item: (item["kind"] != "coordinates", item["header"].lower()))
    status = "matched" if len(candidates) == 1 else "ambiguous" if candidates else "unmatched"
    return {"ok": True, "status": status, "column_count": len(clean),
        "sample_value_count": sum(len(item["values"]) for item in clean),
        "candidates": candidates,
        "recommended_candidate_id": candidates[0]["id"] if len(candidates) == 1 else None,
        "warnings": ([] if candidates else [{"code": "geography_column_not_found",
            "message": "Choose the geography column; its header is not recognized."}]),
        "clarification": {"required": status != "matched",
            "reason": "select_geography_column" if status != "matched" else None}}
