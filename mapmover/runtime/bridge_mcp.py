"""Admitted bridge discovery and bounded typed lookup for hosted MCP."""
from __future__ import annotations
from dataclasses import asdict
from typing import Any
from bridge_tools_shared import bridge_query_result
from .external_reference_adapters import (
    ExternalReferenceUnavailable, adapter_available, admitted_bridge, admitted_external_adapters,
    get_external_adapter, identifier_matches, lookup_external_edges_batch, lookup_loc_id_edges_batch,
)
from .geometry_catalog import load_geometry_catalog
from ..duckdb_helpers import is_cloud_mode


def _error(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": message}}


def _record(system: str) -> dict[str, Any]:
    return next((row for row in load_geometry_catalog().get("external_reference_bridges") or []
                 if isinstance(row, dict) and row.get("external_system") == system), {})


def _summary(adapter: Any) -> dict[str, Any]:
    bridge = admitted_bridge(adapter)
    assert bridge is not None
    record = _record(adapter.system)
    runtime = record.get("runtime_artifact") or {}
    return {
        "bridge_id": record.get("bridge_id") or adapter.system,
        "system": adapter.system, "label": adapter.label, "aliases": list(adapter.aliases),
        "source_release": bridge.source_release, "release_fingerprint": bridge.release_fingerprint,
        "row_count": record.get("row_count"),
        "partition_count": record.get("partition_count", len(bridge.partitions)),
        "countries": sorted({part.country for part in bridge.partitions}),
        "country_coverage_status": "catalogued" if bridge.partitions else "not_in_compact_catalog",
        "bidirectional": True, "relationship_types": ["equivalent_identity", "overlaps", "contained_by"],
        "identity_rule": "Only equivalent_identity edges bind external IDs to loc_ids.",
        "source_license": bridge.source_license,
        "runtime_artifact_sha256": bridge.runtime_sha256 or runtime.get("sha256"),
        "execution_status": "not_yet_verified" if is_cloud_mode() else "artifact_verified" if adapter_available(adapter) else "artifact_unavailable",
        "execution_status_note": "Admission and a catalog pointer do not prove that the runtime artifact can be read. Use query_bridge to verify execution.",
        "has_shapes": False,
    }


def list_bridges() -> dict[str, Any]:
    return {"ok": True, "bridges": [_summary(adapter) for adapter in admitted_external_adapters()]}


def get_bridge(system: str) -> dict[str, Any]:
    adapter = get_external_adapter(system)
    if adapter is None or admitted_bridge(adapter) is None:
        return _error("bridge_not_admitted", "No admitted bridge matches this external system")
    return {"ok": True, "bridge": _summary(adapter)}


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
    result["source_release_filter"] = source_release
    result["internal_release_filter"] = internal_release
    return result
