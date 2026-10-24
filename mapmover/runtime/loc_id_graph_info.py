"""Bounded, exact loc_id identity and direct-relationship reads for MCP."""

from __future__ import annotations

from typing import Any

from .reference_graph import (
    ReferenceRouteUnavailable,
    identities,
    indexed_public_alias_candidates,
    relationships_for_loc_id,
)


CONNECTION_LIMIT = 50


def _marine_info_items(loc_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Read the selected Marine release's small Reference tables by exact ID."""
    from .geometry_predicate_query import read_rows_by_ids
    from .marine_geometry import _active_domain_paths
    from ..duckdb_helpers import parquet_available

    domain = _active_domain_paths() or {}
    identity_path = domain.get("reference_identities")
    relationship_path = domain.get("reference_relationships")
    if (identity_path is None or relationship_path is None
            or not parquet_available(identity_path) or not parquet_available(relationship_path)):
        return {value: _route_error(value, "selected Marine Reference release is unavailable") for value in loc_ids}
    try:
        identities_df = read_rows_by_ids(
            identity_path, loc_ids, id_column="loc_id", columns=["name", "family", "level"]
        )
        outgoing_df = read_rows_by_ids(
            relationship_path, loc_ids, id_column="source_loc_id",
            columns=["target_loc_id", "relationship", "basis"],
        )
        incoming_df = read_rows_by_ids(
            relationship_path, loc_ids, id_column="target_loc_id",
            columns=["source_loc_id", "relationship", "basis"],
        )
    except Exception as exc:
        return {value: _route_error(value, f"selected Marine Reference could not be read: {type(exc).__name__}")
                for value in loc_ids}
    by_id = {str(row["loc_id"]).upper(): row for row in identities_df.to_dict("records")}
    connections: dict[str, list[dict[str, Any]]] = {value: [] for value in loc_ids}
    for frame, key, other, direction in (
        (outgoing_df, "source_loc_id", "target_loc_id", "outgoing"),
        (incoming_df, "target_loc_id", "source_loc_id", "incoming"),
    ):
        for row in frame.to_dict("records"):
            source = str(row.get(key) or "").upper()
            if source in connections:
                connections[source].append({
                    "loc_id": row.get(other), "family": None, "direction": direction,
                    "relationship_type": row.get("relationship"),
                    "relationship_subtype": None, "relationship_id": None,
                    "relationship_vintage": None,
                })
    results = {}
    for value in loc_ids:
        identity = by_id.get(value)
        if identity is None:
            results[value] = {"loc_id": value, "error": {
                "code": "not_found", "message": "loc_id has no identity in the selected Marine Reference release"}}
            continue
        edges = connections[value]
        results[value] = {
            "loc_id": value, "name": identity.get("name"), "family": identity.get("family"),
            "admin_level": None, "parent_id": None, "connections": edges[:CONNECTION_LIMIT],
            "connection_count": min(len(edges), CONNECTION_LIMIT),
            "connection_limit": CONNECTION_LIMIT, "has_more": len(edges) > CONNECTION_LIMIT,
        }
    return results


def graph_info_items(loc_ids: list[str]) -> list[dict[str, Any]]:
    """Return selected-graph records without catalog, shape, or adapter reads."""
    requested = [value.strip().upper() for value in loc_ids]
    unique = list(dict.fromkeys(requested))
    from .marine_geometry import is_marine_loc_id

    marine_ids = [value for value in unique if is_marine_loc_id(value)]
    if marine_ids:
        marine = _marine_info_items(marine_ids)
        country_ids = [value for value in requested if value not in marine]
        if not country_ids:
            return [marine[value] for value in requested]
        country = graph_info_items(country_ids)
        by_country = iter(country)
        return [marine[value] if value in marine else next(by_country) for value in requested]
    try:
        aliases = indexed_public_alias_candidates(unique)
    except Exception as exc:
        return [_route_error(value, f"preferred-public alias index could not be read: {type(exc).__name__}") for value in requested]
    if aliases is None:
        return [_route_error(value, "preferred-public alias index is absent or stale") for value in requested]

    canonical_by_requested: dict[str, str] = {}
    errors: dict[str, dict[str, Any]] = {}
    for value in unique:
        targets = sorted({str(row.get("loc_id") or "").strip().upper() for row in aliases.get(value) or [] if row.get("loc_id")})
        if len(targets) > 1:
            errors[value] = {
                "requested_loc_id": value,
                "canonical_loc_id": None,
                "candidate_loc_ids": targets,
                "error": {"code": "ambiguous_public_loc_id", "message": "public loc_id resolves to more than one graph identity"},
            }
        else:
            canonical_by_requested[value] = targets[0] if targets else value

    canonical_ids = list(dict.fromkeys(canonical_by_requested.values()))
    try:
        rows = identities(canonical_ids, require_route=True) if canonical_ids else []
    except ReferenceRouteUnavailable as exc:
        return [_route_error(value, str(exc)) for value in requested]
    except Exception as exc:
        return [_route_error(value, f"selected identities could not be read: {type(exc).__name__}") for value in requested]
    by_id = {str(row.get("loc_id") or "").upper(): row for row in rows}
    prepared: dict[str, dict[str, Any]] = {}
    for canonical in canonical_ids:
        identity = by_id.get(canonical)
        if identity is None:
            prepared[canonical] = {
                "loc_id": canonical,
                "error": {"code": "not_found", "message": "loc_id has no identity in the selected reference graph"},
            }
            continue
        try:
            edges = relationships_for_loc_id(canonical, limit=CONNECTION_LIMIT + 1)
        except Exception as exc:
            detail = str(exc)
            message = (
                "selected relationship partition lacks graph edge columns; graph release/index needs repair"
                if any(f'Referenced column "{column}" not found' in detail for column in ("source_loc_id", "target_loc_id"))
                else f"direct relationships could not be read: {type(exc).__name__}"
            )
            prepared[canonical] = _route_error(canonical, message)
            continue
        connections = []
        for edge in edges[:CONNECTION_LIMIT]:
            outgoing = str(edge.get("source_loc_id") or "").upper() == canonical
            connections.append({
                "loc_id": edge.get("target_loc_id") if outgoing else edge.get("source_loc_id"),
                "family": edge.get("target_family") if outgoing else edge.get("source_family"),
                "direction": "outgoing" if outgoing else "incoming",
                "relationship_type": edge.get("relationship_type"),
                "relationship_subtype": edge.get("relationship_subtype"),
                "relationship_id": edge.get("relationship_id"),
                "relationship_vintage": edge.get("relationship_vintage"),
                "valid_from": edge.get("valid_from"),
                "valid_to": edge.get("valid_to"),
                "authority": edge.get("authority"),
            })
        prepared[canonical] = {
            "loc_id": canonical,
            "name": identity.get("name"),
            "family": identity.get("geography_family") or identity.get("family"),
            "admin_level": identity.get("admin_level"),
            "parent_id": identity.get("parent_loc_id") or identity.get("parent_id"),
            "valid_from": identity.get("valid_from"),
            "valid_to": identity.get("valid_to"),
            "connections": connections,
            "connection_count": len(connections),
            "connection_limit": CONNECTION_LIMIT,
            "has_more": len(edges) > CONNECTION_LIMIT,
        }

    results = []
    for value in requested:
        if value in errors:
            results.append(errors[value])
            continue
        canonical = canonical_by_requested[value]
        result = dict(prepared[canonical])
        if canonical != value:
            result["requested_loc_id"] = value
            result["resolved_from_public_alias"] = True
            if result.get("error", {}).get("code") == "not_found":
                result["error"] = {"code": "invalid_public_loc_id_target", "message": "public alias target has no selected graph identity"}
        results.append(result)
    return results


def _route_error(loc_id: str, message: str) -> dict[str, Any]:
    return {"loc_id": loc_id, "error": {"code": "reference_route_unavailable", "message": message}}
