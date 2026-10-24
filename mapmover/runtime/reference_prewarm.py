"""Bounded startup warm set for high-frequency identity conversion."""
from __future__ import annotations

import logging
import hashlib
import json
import re
from pathlib import PurePosixPath
from typing import Any

from ..foundation_helpers import load_country_crosswalk, load_reference_dict
from .admin_spine_query import prewarm_shallow_identity_index
from .global_admin0_query import load_global_admin0_identities


logger = logging.getLogger(__name__)
_warmed_country_selection: tuple[tuple[str, str, str], ...] = ()


def country_routing_selection(catalog: dict[str, Any]) -> tuple[tuple[str, str, str], ...]:
    return tuple(sorted(
        (str(p.get("country_code") or ""), str(p.get("release_id") or ""), str(p.get("release_version") or ""))
        for p in catalog.get("country_profiles") or []
        if isinstance(p, dict) and p.get("release_status") in {"published", "approved_for_publication"}
    ))


def country_routing_selection_is_warm(catalog: dict[str, Any]) -> bool:
    return bool(_warmed_country_selection) and country_routing_selection(catalog) == _warmed_country_selection


def prewarm_country_routing_indexes(catalog: dict[str, Any] | None = None) -> dict[str, Any]:
    """Hydrate the selected country routing controls and Parquet indexes on disk."""
    from .geometry_catalog import load_geometry_catalog
    from .geometry_storage_layout import country_release_manifest_relative
    from .published_artifacts import (
        artifact_cache_enabled, artifact_prefix, artifact_ref,
        pin_artifact_paths, read_artifact_bytes, resolve_artifact_path,
    )

    # Candidate/staging lanes are mutable and intentionally bypass Tier 2.
    if artifact_ref("geometry/geometry_catalog.json", lane="active").prefix != artifact_prefix("published"):
        return {"skipped": "active lane is not published"}
    if not artifact_cache_enabled():
        raise RuntimeError("Published artifact cache is disabled; country routing indexes cannot be prewarmed")

    selected_catalog = catalog if catalog is not None else load_geometry_catalog()
    profiles = [p for p in selected_catalog.get("country_profiles") or []
                if isinstance(p, dict) and p.get("release_status") in {"published", "approved_for_publication"}]
    if not profiles:
        raise RuntimeError("No published country profiles to prewarm")
    total_bytes = 0
    countries = []
    warm_paths = ["geometry/geometry_catalog.json"]

    def control(relative: str) -> dict[str, Any]:
        payload = json.loads(read_artifact_bytes(relative, lane="active"))
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid country routing control: {relative}")
        warm_paths.append(relative)
        return payload

    for profile in profiles:
        country = str(profile.get("country_code") or "").upper()
        if not re.fullmatch(r"[A-Z]{3}", country):
            raise ValueError("Invalid published country code")
        base = f"geometry/countries/{country}"
        index = control(f"{base}/index.json")
        current = index.get("current") or {}
        release_id = str(profile.get("release_id") or "")
        version = str(profile.get("release_version") or "")
        manifest_path = country_release_manifest_relative(country, profile)
        pin = current.get("manifest") or {}
        if (str((index.get("release_unit") or {}).get("id") or "").upper() != country
                or current.get("release_id") != release_id
                or current.get("version") != version
                or pin.get("path") != manifest_path):
            raise ValueError(f"Country routing release selection changed: {country}")
        manifest_bytes = read_artifact_bytes(manifest_path, lane="active")
        warm_paths.append(manifest_path)
        if hashlib.sha256(manifest_bytes).hexdigest() != pin.get("sha256"):
            raise ValueError(f"Country routing manifest pin mismatch: {country}")
        manifest = json.loads(manifest_bytes)
        if str((manifest.get("release_unit") or {}).get("id") or "").upper() != country:
            raise ValueError(f"Country routing manifest owner mismatch: {country}")
        exact = str((manifest.get("runtime") or {}).get("exact") or "")
        canonical = profile.get("canonical_runtime_namespace_required") is True
        graph = f"{base}/reference/graph/" if canonical else f"{base}/reference/{release_id}/"
        expected_exact = (f"{base}/admin_spine/serving/exact/" if canonical
                          else f"{base}/admin_spine/exact/{release_id}/")
        if exact != expected_exact or (manifest.get("runtime") or {}).get("reference") != f"{base}/reference/":
            raise ValueError(f"Country routing runtime paths mismatch: {country}")
        graph_manifest = control(graph + "manifest.json")
        discovery_manifest = control(f"{base}/reference_discovery/manifest.json")
        graph_release = str(graph_manifest.get("release_id") or "")
        source_graph = (discovery_manifest.get("source_graphs") or {}).get(country) or {}
        if (not graph_release or discovery_manifest.get("status") != "PASS"
                or discovery_manifest.get("artifact") != "identifier_index.parquet"
                or set(discovery_manifest.get("source_graphs") or {}) != {country}
                or source_graph.get("release_id") != graph_release):
            raise ValueError(f"Country discovery index is not pinned to its graph: {country}")

        hashes = {}
        for item in manifest.get("objects") or []:
            for path in item.get("source_paths") or []:
                hashes[str(path)] = str(item.get("sha256") or "")
        selected = (
            (exact + "loc_id_routes.parquet", hashes.get(exact + "loc_id_routes.parquet")),
            (graph + "identity_routes.parquet", hashes.get(graph + "identity_routes.parquet")),
            (f"{base}/reference_discovery/identifier_index.parquet", discovery_manifest.get("sha256")),
        )
        for relative, expected_hash in selected:
            if not re.fullmatch(r"[0-9a-f]{64}", str(expected_hash or "")):
                raise ValueError(f"Country routing index lacks a hash pin: {relative}")
            local = resolve_artifact_path(relative, lane="active", max_bytes=64 * 1024 * 1024)
            if local is None:
                raise RuntimeError(f"Country routing index could not be cached: {relative}")
            digest = hashlib.sha256()
            with local.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != expected_hash:
                raise ValueError(f"Country routing index hash mismatch: {relative}")
            total_bytes += local.stat().st_size
            warm_paths.append(relative)
        countries.append(country)
    marine_reference_bytes = 0
    marine_profile = next((
        item for item in selected_catalog.get("domain_profiles") or []
        if isinstance(item, dict)
        and str(item.get("release_unit_id") or "").upper() == "MARINE"
    ), None)
    marine_release = marine_profile.get("active_release") if isinstance(marine_profile, dict) else None
    if isinstance(marine_release, dict) and marine_release.get("publication_status") in {
        "published", "approved_for_publication",
    }:
        manifest = str(marine_release.get("version_manifest_path") or "")
        if not manifest.startswith("geometry/domains/MARINE/") or not manifest.endswith("/manifest.json"):
            raise ValueError("Selected Marine release has no safe Reference manifest path")
        marine_root = PurePosixPath(manifest).parent
        for name in ("identities.parquet", "relationships.parquet"):
            relative = str(marine_root / "reference" / name)
            local = resolve_artifact_path(relative, lane="active", max_bytes=1024 * 1024)
            if local is None:
                raise RuntimeError(f"Selected Marine Reference artifact could not be cached: {relative}")
            marine_reference_bytes += local.stat().st_size
            warm_paths.append(relative)
    pinned_bytes = pin_artifact_paths(warm_paths)
    result = {"countries": countries, "index_bytes": total_bytes,
              "marine_reference_bytes": marine_reference_bytes, "pinned_bytes": pinned_bytes}
    global _warmed_country_selection
    _warmed_country_selection = country_routing_selection(selected_catalog)
    logger.info("Country routing index pre-warmer complete: %s", result)
    return result


def prewarm_reference_identities() -> dict[str, Any]:
    """Warm small country helpers plus the current USA conversion spine.

    This deliberately contains no polygons. Additional countries belong here
    only after observed conversion traffic justifies making their identity
    indexes part of every hosted process's resident set.
    """
    iso_codes = load_reference_dict("iso_codes.json") or {}
    country_aliases = load_reference_dict("country_aliases.json") or {}
    codes = list(dict.fromkeys(
        str(value).strip().upper()
        for section in ("common_countries", "commonly_missing")
        for value in ((iso_codes.get(section) or {}).get("codes") or [])
        if str(value).strip()
    ))
    admin0 = load_global_admin0_identities(codes) or {}
    usa_crosswalk = load_country_crosswalk("USA") or {}
    usa_rows = prewarm_shallow_identity_index("USA", maximum_level=2)
    result = {
        "iso_codes": len(codes),
        "country_aliases": len(country_aliases.get("aliases") or {}),
        "global_admin0_identities": len(admin0),
        "usa_crosswalk_sections": len(usa_crosswalk),
        "usa_admin0_2_identities": usa_rows,
    }
    logger.info("Reference identity pre-warmer complete: %s", result)
    return result
