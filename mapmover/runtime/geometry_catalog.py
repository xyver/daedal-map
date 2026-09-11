"""Runtime discovery for named, shared geometry families.

Geometry is not a pack and should not be copied into every source catalog.
This module reads the generated geometry catalog once and gives every chat,
API, and MCP path the same name -> canonical ``loc_id`` lookup.
"""

from __future__ import annotations

import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..paths import GEOMETRY_DIR
from ..runtime_config import force_remote_data_reads, get_data_plane_mode
from .published_artifacts import read_artifact_json
from geometry_catalog_shared import (
    build_geometry_capability_summary,
    merge_crosswalk_catalog,
    published_geometry_catalog_records,
    public_geometry_catalog_records,
)
from material_policy_shared import combine_material_access


CATALOG_PATH = GEOMETRY_DIR / "geometry_catalog.json"
CROSSWALK_CATALOG_PATH = GEOMETRY_DIR / "crosswalk_catalog.json"


def _empty_country_catalog(country: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "country_code": country,
        "geometry_banks": [],
        "reference_systems": [],
        "crosswalks": [],
        "supporting_crosswalk_assets": [],
        "orphaned_geometry_correspondences": [],
        "summary": {},
    }


def _merge_crosswalks(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("crosswalks"):
        return payload
    crosswalks = None
    if _is_cloud_mode():
        try:
            crosswalks = read_artifact_json("geometry/crosswalk_catalog.json", lane="active")
        except Exception:
            crosswalks = None
    if not isinstance(crosswalks, dict) and not force_remote_data_reads():
        try:
            crosswalks = json.loads(CROSSWALK_CATALOG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            crosswalks = None
    return merge_crosswalk_catalog(payload, crosswalks)


def _normalize(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _is_cloud_mode() -> bool:
    return get_data_plane_mode() == "cloud"


def _fetch_geometry_catalog_from_s3() -> dict[str, Any] | None:
    # The runtime-selected immutable lane is normally published. Release smoke
    # deliberately selects staging and must evaluate that catalog as one
    # coherent activation snapshot.
    payload = read_artifact_json("geometry/geometry_catalog.json", lane="active")
    return payload if isinstance(payload, dict) else None


def _catalog_cache_epoch() -> int:
    try:
        seconds = max(1, int(os.environ.get("GEOMETRY_CATALOG_REVALIDATE_SECONDS", "300")))
    except ValueError:
        seconds = 300
    return int(time.monotonic() // seconds)


@lru_cache(maxsize=2)
def _load_geometry_catalog_cached(_epoch: int) -> dict[str, Any]:
    """Load the generated schema-1.1 geometry catalog."""
    if _is_cloud_mode():
        try:
            payload = _fetch_geometry_catalog_from_s3()
            if isinstance(payload, dict):
                return _merge_crosswalks(payload)
        except Exception:
            pass

    if not force_remote_data_reads():
        try:
            payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return _merge_crosswalks(payload)
        except (OSError, json.JSONDecodeError):
            pass

    return {
        "schema_version": "1.1.0",
        "geometry_collections": [],
        "geometry_families": [],
        "geometry_banks": [],
        "geometry_products": [],
        "release_packages": [],
        "crosswalk_artifacts": [],
        "resolver_groups": [],
        "named_reference_objects": [],
    }


def load_geometry_catalog() -> dict[str, Any]:
    """Load the sole runtime authority, revalidating its cloud object periodically."""
    return _load_geometry_catalog_cached(_catalog_cache_epoch())


@lru_cache(maxsize=64)
def load_country_geometry_catalog(country_scope: str) -> dict[str, Any]:
    """Load the additive detailed catalog for one maintained country."""
    country = str(country_scope or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", country):
        return _empty_country_catalog(country)
    relative = f"geometry/countries/{country}/{country}_catalog.json"
    if _is_cloud_mode():
        try:
            payload = read_artifact_json(relative, lane="active")
            if isinstance(payload, dict) and str(payload.get("country_code") or "").upper() == country:
                return payload
        except Exception:
            pass
    if not force_remote_data_reads():
        path = GEOMETRY_DIR / "countries" / country / f"{country}_catalog.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and str(payload.get("country_code") or "").upper() == country:
                return payload
        except (OSError, json.JSONDecodeError):
            pass
    return _empty_country_catalog(country)


def clear_geometry_catalog_cache() -> None:
    _load_geometry_catalog_cached.cache_clear()
    load_country_geometry_catalog.cache_clear()
    _named_index.cache_clear()
    _named_group_index.cache_clear()
    # Imported late: geometry_inventory reads this module's catalog loader.
    from .geometry_inventory import clear_geometry_inventory_cache

    clear_geometry_inventory_cache()
    from .admin_spine_query import clear_admin_spine_query_cache
    from .global_admin0_query import clear_global_admin0_query_cache
    from .marine_geometry import clear_marine_geometry_cache
    from .reference_graph import clear_reference_graph_cache
    from ..duckdb_helpers import clear_parquet_metadata_cache

    clear_admin_spine_query_cache()
    clear_global_admin0_query_cache()
    clear_marine_geometry_cache()
    clear_reference_graph_cache()
    clear_parquet_metadata_cache()


def geometry_capability_summary(catalog: dict[str, Any] | None = None) -> dict[str, Any]:
    """Read the generated capability contract, deriving it for older catalogs."""
    payload = catalog if isinstance(catalog, dict) else load_geometry_catalog()
    summary = payload.get("capability_summary") if isinstance(payload, dict) else None
    if isinstance(summary, dict) and summary:
        return dict(summary)
    return build_geometry_capability_summary(payload if isinstance(payload, dict) else {})


@lru_cache(maxsize=1)
def _named_index() -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    catalog = load_geometry_catalog()
    for entry in catalog.get("named_reference_objects") or []:
        if not isinstance(entry, dict):
            continue
        loc_id = str(entry.get("loc_id") or "").strip().upper()
        label = str(entry.get("label") or "").strip()
        if not loc_id or not label:
            continue
        normalized_entry = dict(entry)
        normalized_entry["loc_id"] = loc_id
        for alias in [loc_id, label, *(entry.get("aliases") or [])]:
            key = _normalize(str(alias))
            if key:
                index.setdefault(key, normalized_entry)
        # “Mediterranean” is a natural request for “Mediterranean Sea”.
        for suffix in (" sea", " ocean", " waters"):
            key = _normalize(label)
            if key.endswith(suffix):
                index.setdefault(key[: -len(suffix)].strip(), normalized_entry)
    return index


@lru_cache(maxsize=1)
def _named_group_index() -> dict[str, dict[str, Any]]:
    """Index explicit human-name groups before individual geometry aliases.

    A whole-ocean name can represent multiple IHO polygons (Pacific and
    Arctic). It is not safe to select whichever individual polygon happens to
    be first, nor to substitute a legacy X* SST product zone.
    """
    index: dict[str, dict[str, Any]] = {}
    catalog = load_geometry_catalog()
    for entry in catalog.get("resolver_groups") or []:
        if not isinstance(entry, dict):
            continue
        label = str(entry.get("label") or "").strip()
        loc_ids = [str(value).strip().upper() for value in entry.get("loc_ids") or [] if str(value).strip()]
        if not label:
            continue
        normalized = dict(entry)
        normalized["label"] = label
        normalized["loc_ids"] = loc_ids
        for alias in [label, *(entry.get("aliases") or [])]:
            key = _normalize(str(alias))
            if key:
                index.setdefault(key, normalized)
    return index


def resolve_geometry_name(value: str | None) -> dict[str, Any] | None:
    """Resolve a named shared geometry without falling back to land aliases."""
    key = _normalize(value)
    group = _named_group_index().get(key)
    if group:
        # Return an explicitly unresolved group as well. Callers can then give
        # a truthful "no approved geometry" result instead of treating a known
        # ocean name as an unknown place or falling back to an X* SST zone.
        return dict(group)
    entry = _named_index().get(key)
    if not entry or not bool(entry.get("resolvable", True)):
        return None
    return dict(entry)


def expand_geometry_loc_id(value: str | None) -> list[str]:
    """Return a resolvable geometry id plus all catalogued descendants.

    Named-water source rows use the smallest containing polygon. This shared
    expansion makes a parent selection include its detailed child waters.
    """
    root = str(value or "").strip().upper()
    if not root:
        return []
    catalog = load_geometry_catalog()
    entries = [
        entry
        for entry in catalog.get("named_reference_objects") or []
        if isinstance(entry, dict)
    ]
    known = {str(entry.get("loc_id") or "").strip().upper() for entry in entries if bool(entry.get("resolvable", True))}
    if root not in known:
        return []
    children: dict[str, list[str]] = {}
    for entry in entries:
        loc_id = str(entry.get("loc_id") or "").strip().upper()
        parent = str(entry.get("parent_loc_id") or "").strip().upper()
        if loc_id and parent:
            children.setdefault(parent, []).append(loc_id)
    expanded: list[str] = []
    pending = [root]
    while pending:
        loc_id = pending.pop(0)
        if loc_id in expanded:
            continue
        expanded.append(loc_id)
        pending.extend(sorted(children.get(loc_id) or []))
    return expanded


def is_known_geometry_loc_id(value: str | None) -> bool:
    entry = _named_index().get(_normalize(value))
    return bool(entry and entry.get("loc_id") == str(value or "").strip().upper() and entry.get("resolvable", True))


def geometry_bank_access_facts(
    *,
    scopes: set[str] | None = None,
    families: set[str] | None = None,
) -> tuple[set[str], bool]:
    """Return commercial-use permissions and hosted publication clearance.

    Every matched bank must carry the generated material-policy contract.
    Missing policy fails closed instead of reconstructing a decision from
    source envelopes at request time.
    """
    catalog = load_geometry_catalog() or {}
    banks = catalog.get("geometry_banks") or {}
    if isinstance(banks, dict):
        banks = list(banks.values())
    if not isinstance(banks, list):
        return set(), False

    normalized_scopes = {str(value).strip().upper() for value in (scopes or set()) if str(value).strip()}
    normalized_families = {str(value).strip().lower() for value in (families or set()) if str(value).strip()}
    matched_banks: list[dict[str, Any]] = []
    for bank in banks:
        if not isinstance(bank, dict):
            continue
        bank_scope = str(bank.get("scope") or "").strip().upper()
        bank_family = str(bank.get("family") or "").strip().lower()
        if normalized_scopes and bank_scope not in normalized_scopes:
            continue
        if normalized_families and bank_family not in normalized_families:
            continue
        matched_banks.append(bank)
    combined = combine_material_access(matched_banks)
    return set(combined.get("permissions") or set()), bool(combined.get("publication_cleared"))


def is_deprecated_geometry_loc_id(value: str | None) -> bool:
    entry = _named_index().get(_normalize(value))
    return bool(entry and entry.get("loc_id") == str(value or "").strip().upper() and not entry.get("resolvable", True))
