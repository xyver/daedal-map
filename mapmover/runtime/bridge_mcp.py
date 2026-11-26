"""Admitted bridge discovery and bounded typed lookup for hosted MCP."""
from __future__ import annotations
from dataclasses import asdict
from functools import lru_cache
from typing import Any
from bridge_tools_shared import bridge_query_result
from .external_reference_adapters import (
    ExternalReferenceUnavailable, adapter_available, admitted_bridge, admitted_external_adapters,
    country_loc_id_aliases, get_external_adapter, identifier_matches, lookup_external_edges_batch, lookup_loc_id_edges_batch,
    _release_fingerprint,
)
from .geometry_catalog import load_geometry_catalog
from .published_artifacts import read_artifact_json
from ..duckdb_helpers import is_cloud_mode


def _error(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": message}}


def _record(system: str) -> dict[str, Any]:
    return next((row for row in load_geometry_catalog().get("external_reference_bridges") or []
                 if isinstance(row, dict) and row.get("external_system") == system), {})


@lru_cache(maxsize=16)
def _published_partition_metadata(path: str, fingerprint: str, system: str) -> tuple[dict[str, Any], ...]:
    manifest = read_artifact_json(path, lane="active")
    if (not isinstance(manifest, dict) or manifest.get("profile") != "external_reference_bridge"
            or manifest.get("external_system") != system
            or manifest.get("release_fingerprint") != fingerprint
            or _release_fingerprint(manifest) != fingerprint):
        raise ValueError("Published bridge release manifest does not match the selected catalog")
    return tuple({"country": str(row.get("country_iso3") or "").upper(),
                  "source_family": row.get("source_family"),
                  "internal_release": row.get("internal_spine_release"),
                  "row_count": row.get("row_count")}
                 for row in manifest.get("partitions") or [] if isinstance(row, dict))


def country_bridge_coverage(country_scope: str) -> list[dict[str, Any]]:
    """Admitted country partition claims, never a promise for each identifier."""
    scope = str(country_scope or "").strip().upper()
    result = []
    for adapter in admitted_external_adapters():
        bridge = admitted_bridge(adapter)
        if bridge is None:
            continue
        record = _record(adapter.system)
        if is_cloud_mode():
            try:
                partitions = _published_partition_metadata(
                    str(record.get("manifest_path") or ""), bridge.release_fingerprint, adapter.system)
            except Exception:
                partitions = ()
            selected = [part for part in partitions if part["country"] == scope]
        else:
            selected = [{"country": part.country, "source_family": None,
                         "internal_release": part.internal_release, "row_count": None}
                        for part in bridge.partitions if part.country == scope]
        if selected:
            result.append({"system": adapter.system, "source_release": bridge.source_release,
                           "partition_count": len(selected),
                           "source_families": sorted({str(part["source_family"]) for part in selected
                                                      if part["source_family"]}),
                           "internal_releases": sorted({str(part["internal_release"]) for part in selected
                                                        if part["internal_release"]}),
                           "row_count": sum(int(part["row_count"] or 0) for part in selected)
                                        if all(part["row_count"] is not None for part in selected) else None,
                           "coverage_status": "admitted_partition_present",
                           "coverage_note": "Country coverage does not guarantee a correspondence for every loc_id or external feature type. Use get_bridge and query_bridge for an exact ID.",
                           "mcp_endpoint": "https://app.daedalmap.com/mcp/bridges"})
    return result


def _summary(adapter: Any) -> dict[str, Any]:
    bridge = admitted_bridge(adapter)
    assert bridge is not None
    record = _record(adapter.system)
    runtime = record.get("runtime_artifact") or {}
    if is_cloud_mode():
        try:
            partition_rows = _published_partition_metadata(
                str(record.get("manifest_path") or ""), bridge.release_fingerprint, adapter.system)
            countries = sorted({row["country"] for row in partition_rows if row["country"]})
        except Exception:
            countries = []
    else:
        countries = sorted({part.country for part in bridge.partitions})
    return {
        "bridge_id": record.get("bridge_id") or adapter.system,
        "system": adapter.system, "label": adapter.label, "aliases": list(adapter.aliases),
        "source_release": bridge.source_release, "release_fingerprint": bridge.release_fingerprint,
        "row_count": record.get("row_count"),
        "partition_count": record.get("partition_count", len(bridge.partitions)),
        "countries": countries,
        "country_coverage_status": "catalogued" if countries else "unavailable",
        "bidirectional": True, "relationship_types": ["equivalent_identity", "overlaps", "contained_by"],
        "identity_rule": "Only equivalent_identity edges bind external IDs to loc_ids.",
        "source_license": bridge.source_license,
        "runtime_artifact_sha256": bridge.runtime_sha256 or runtime.get("sha256"),
        "execution_status": "not_yet_verified" if is_cloud_mode() else "artifact_verified" if adapter_available(adapter) else "artifact_unavailable",
        "execution_status_note": "Admission and a catalog pointer do not prove that the runtime artifact can be read. Use query_bridge to verify execution.",
        "has_shapes": False,
        "supported_external_id_kind": "division_id",
        "division_area_id_status": "not_published_in_selected_release",
        "geometry_confidence_meaning": "Minimum of source and target polygon area shares, rounded to four decimals. Zero may mean negligible rounded overlap or boundary contact; it does not prove positive-area overlap.",
        "area_share_meaning": {"loc_area_share": "Intersection area divided by the DaedalMap loc polygon area.", "external_area_share": "Intersection area divided by the Overture division polygon area."},
        "zero_overlap_note": "When both published shares are zero, boundary contact and sub-precision positive area remain indistinguishable. A positive published share establishes positive area at this precision.",
        "edge_provenance_status": "not_in_compact_artifact",
        "edge_provenance_note": "This release exposes source and internal releases plus the runtime artifact SHA-256, but the compact file omits per-edge IDs and generation hashes.",
        "country_loc_id_alias_source": "bridge_bundled_global_to_preferred_lookup",
        "country_loc_id_alias_note": "Explicit one-to-one country-to-global geometry mappings can resolve a country loc_id against the stored bridge ID; both IDs remain visible in query results.",
    }


def list_bridges() -> dict[str, Any]:
    return {"ok": True, "bridges": [_summary(adapter) for adapter in admitted_external_adapters()]}


def get_bridge(system: str) -> dict[str, Any]:
    adapter = get_external_adapter(system)
    if adapter is None or admitted_bridge(adapter) is None:
        return _error("bridge_not_admitted", "No admitted bridge matches this external system")
    return {"ok": True, "bridge": _summary(adapter)}


def _country_aliases_for_bridge_ids(country: str, bridge_ids: list[str]) -> dict[str, str]:
    """Return only unambiguous aliases from the published country crosswalk."""
    return country_loc_id_aliases(country, bridge_ids)


def query_bridge(*, system: str, country_scope: str, external_id: str | None = None,
                 loc_id: str | None = None, source_release: str | None = None,
                 internal_release: str | None = None, limit: int = 25) -> dict[str, Any]:
    adapter = get_external_adapter(system)
    if adapter is None or admitted_bridge(adapter) is None:
        return _error("bridge_not_admitted", "No admitted bridge matches this external system")
    scope = str(country_scope or "").strip().upper()
    if len(scope) != 3 or not scope.isalpha():
        return _error("invalid_country_scope", "country_scope must be one ISO3 country")
    if bool(external_id) == bool(loc_id):
        return _error("invalid_bridge_selector", "Supply exactly one of external_id or loc_id")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        return _error("invalid_limit", "limit must be an integer from 1 to 100")
    value = str(external_id or loc_id).strip()
    if not value:
        return _error("invalid_identifier", "The selected identifier must be nonempty")
    if external_id and not identifier_matches(adapter, value):
        return _error("invalid_external_identifier", "The external identifier does not match this system's format")
    if loc_id and not (value == scope or value.startswith(scope + "-")):
        return _error("scope_value_mismatch", "loc_id does not belong to country_scope")
    direction = "external_to_loc_id" if external_id else "loc_id_to_external"
    try:
        if external_id:
            grouped = lookup_external_edges_batch(adapter.system, [value], country_scope=scope,
                source_release=source_release, internal_release=internal_release)
        else:
            grouped = lookup_loc_id_edges_batch(adapter.system, [value], country_scope=scope,
                source_release=source_release, internal_release=internal_release, limit=None)
    except ExternalReferenceUnavailable:
        return _error("bridge_artifact_unavailable", "The admitted bridge runtime artifact could not be read")
    if grouped is None:
        return _error("bridge_artifact_unavailable", "The admitted bridge runtime artifact is unavailable")
    result = bridge_query_result(system=adapter.system, country_scope=scope, direction=direction,
        value=value, rows=[asdict(edge) for edge in grouped.get(value, [])], limit=limit)
    bridge = admitted_bridge(adapter)
    result["bridge_release_fingerprint"] = bridge.release_fingerprint if bridge else None
    result["runtime_artifact_sha256"] = bridge.runtime_sha256 if bridge else None
    result["source_release_filter"] = source_release
    result["internal_release_filter"] = internal_release
    if external_id:
        aliases = _country_aliases_for_bridge_ids(scope, result["identity_bindings"])
        if aliases:
            result["bridge_loc_id_bindings"] = list(result["identity_bindings"])
            result["identity_bindings"] = [aliases.get(value, value) for value in result["identity_bindings"]]
            result["country_loc_id_bindings"] = aliases
            result["country_loc_id_translation_source"] = "bridge_bundled_global_to_preferred_lookup"
    if loc_id:
        stored_ids = sorted({str(edge["loc_id"]) for edge in result["edges"]
                             if edge.get("loc_id") and edge["loc_id"] != value})
        if stored_ids:
            result["identity_translation"] = {
                "requested_loc_id": value,
                "bridge_loc_ids": stored_ids,
                "source": "bridge_bundled_global_to_preferred_lookup",
                "note": "The bridge edge retains its stored global geometry ID; the requested country loc_id is an explicit lookup alias.",
            }
    return result
