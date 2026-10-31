"""MCP lookup against coverage catalogs supplied by the active data source.

This module does not choose a release. Its caller supplies the active catalog
directory; the hosted/local data source owns that selection.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path


ENV = "DAEDALMAP_GEOMETRY_ACTIVE_CATALOG_DIR"


def _catalogs() -> tuple[dict[str, dict], dict[str, str], dict[str, str]]:
    configured = os.getenv(ENV)
    if not configured:
        return {}, {}, {}
    root = Path(configured)
    result = {}
    excluded = {}
    releases = {}
    for path in sorted(root.glob("*_catalog.json")):
        catalog = json.loads(path.read_text(encoding="utf-8"))
        unit = catalog.get("unit")
        kind = catalog.get("unit_kind")
        if (catalog.get("schema") != "geometry_coverage_v1" or
                not re.fullmatch(r"\d+\.\d+(?:\.\d+)?", str(catalog.get("release") or "")) or
                not isinstance(unit, str) or
                not ((kind == "country" and re.fullmatch(r"[A-Z]{3}", unit)) or
                     (kind in {"global", "marine"} and unit == kind.upper())) or
                not path.name.startswith(f"{unit}_")):
            raise ValueError(f"invalid active coverage catalog identity: {path}")
        if unit in result:
            raise ValueError(f"multiple active catalogs supplied for {unit}")
        result[unit] = catalog
        releases[unit] = catalog["release"]
    return result, excluded, releases


def _years(values: list, requested_year: int | None):
    for value in values:
        if value == "static":
            yield requested_year if requested_year is not None else "static", "static"
        elif type(value) is int:
            yield value, "year"
        elif isinstance(value, list) and len(value) == 2:
            for year in range(value[0], value[1] + 1):
                yield year, "range"


def _cells(catalog: dict, requested_year: int | None) -> list[dict]:
    result = []
    for family, group in catalog.get("families", {}).items():
        for product_id, product in group.get("products", {}).items():
            for scope, years in product.get("coverage", {}).items():
                for year, time_basis in _years(years, requested_year):
                    result.append({"family_id": family, "scope_id": catalog["unit"],
                                   "jurisdiction": scope, "year": year,
                                   "time_basis": time_basis,
                                   "product_id": product_id, "agency": product["agency"],
                                   "subtype": product["subtype"],
                                   "form": product["form"], "packs": product["packs"]})
    for level, group in catalog.get("admin_spine", {}).items():
        for product_id, product in group.get("products", {}).items():
            for scope, years in product.get("coverage", {}).items():
                for year, time_basis in _years(years, requested_year):
                    result.append({"family_id": "administrative", "admin_level": int(level),
                                   "scope_id": catalog["unit"], "jurisdiction": scope,
                                   "year": year, "time_basis": time_basis,
                                   "product_id": product_id,
                                   "agency": product["agency"], "subtype": product["subtype"],
                                   "form": product["form"],
                                   "packs": product["packs"]})
    return result


def directory_status() -> dict:
    """Expose only the catalogs supplied by the active data source."""
    if not os.getenv(ENV):
        return {"ok": False, "error": {"code": "coverage_catalog_unconfigured",
                                       "message": "No active Geometry coverage catalog directory was supplied."}}
    try:
        catalogs, excluded, versions = _catalogs()
    except (ValueError, OSError, json.JSONDecodeError) as error:
        return {"ok": False, "error": {"code": "coverage_catalog_invalid",
                                       "message": str(error)}}
    if not catalogs:
        return {"ok": False, "error": {"code": "coverage_catalog_unavailable",
                                       "message": "The supplied directory has no coverage catalogs."}}
    families = sorted({row["family_id"] for catalog in catalogs.values()
                       for row in _cells(catalog, None)})
    return {"ok": True, "catalog": "geometry", "schema": "geometry_coverage_v1",
            "status": "active_catalog",
            "catalog_releases": versions,
            "families": [{"family_id": family} for family in families],
            "scopes": [{"scope_id": scope} for scope in sorted(catalogs)],
            "observed_families": families,
            "excluded_units": excluded,
            "absence_is_unverified": True}


def discover(*, family_id: str | None, scope_id: str | None,
             year: int | None, page: int = 0, page_size: int = 10,
             include_geometry: bool = True) -> dict:
    """Return availability or data for two or three declared dimensions."""
    if not os.getenv(ENV):
        return {"ok": False, "error": {"code": "coverage_catalog_unconfigured",
                                       "message": "No active Geometry coverage catalog directory was supplied."}}
    if sum(value is not None for value in (family_id, scope_id, year)) < 2:
        return {"ok": False, "error": {"code": "geography_dimensions_required",
                                       "message": "Supply two of scope_id, family_id, and year."}}
    try:
        catalogs, excluded, catalog_releases = _catalogs()
    except (ValueError, OSError, json.JSONDecodeError) as error:
        return {"ok": False, "error": {"code": "coverage_catalog_invalid",
                                       "message": str(error)}}
    if not catalogs:
        return {"ok": False, "error": {"code": "coverage_catalog_unavailable",
                                       "message": "The active data source supplied no coverage catalogs."},
                "excluded_units": excluded}
    family = family_id.strip().lower() if family_id else None
    scope = scope_id.strip().upper() if scope_id else None
    if scope and scope not in catalogs:
        reason = excluded.get(scope)
        return {"ok": False, "error": {"code": reason or "scope_not_in_catalog",
                                       "message": f"No active coverage catalog is supplied for {scope}."}}
    rows = [row for catalog in catalogs.values() for row in _cells(catalog, year)
            if (family is None or row["family_id"] == family)
            and (scope is None or row["scope_id"] == scope)
            and (year is None or row["year"] == year)]
    dimensions = sum(value is not None for value in (family, scope, year))
    if dimensions == 2:
        missing = "family" if family is None else "location" if scope is None else "time"
        field = {"family": "family_id", "location": "scope_id", "time": "year"}[missing]
        values = sorted({row[field] for row in rows}, key=lambda value: (type(value).__name__, str(value)))
        return {"ok": True, "catalog": "geometry_coverage_v1", "status": "partial_trial",
                "missing_dimension": missing, "available_values": values,
                "observed_cells": rows,
                "completeness": "incomplete", "absence_is_unverified": True,
                "catalog_releases": catalog_releases,
                "excluded_units": excluded}
    if not rows:
        return {"ok": False, "error": {"code": "coverage_unverified",
                                       "message": "This incomplete trial does not verify the requested cell."},
                "absence_is_unverified": True}
    if not any(row["jurisdiction"] == "WHOLE" for row in rows):
        return {"ok": False, "error": {"code": "partial_coverage",
                                       "message": "Only regional coverage is observed; the full-country result is unverified."},
                "observed_coverage": rows, "completeness": "incomplete"}
    groups: dict[tuple, set[str]] = {}
    for row in rows:
        key = (row["family_id"], row.get("admin_level"), row["jurisdiction"])
        groups.setdefault(key, set()).add(row["product_id"])
    if any(len(products) > 1 for products in groups.values()):
        return {"ok": False, "error": {"code": "ambiguous_composition",
                                       "message": "Overlapping products need a reviewed composition rule."},
                "observed_coverage": rows, "completeness": "incomplete"}
    from .geometry_coverage_data import read_dataset
    result = read_dataset(scope=scope, family=family, year=year, cells=rows,
                          page=page, page_size=page_size,
                          include_geometry=include_geometry)
    result["catalog_release"] = catalog_releases[scope]
    return result
