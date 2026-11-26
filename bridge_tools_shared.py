"""Shared Bridge MCP response contracts."""
from __future__ import annotations
from typing import Any

def bridge_edge(row: dict[str, Any], *, system: str) -> dict[str, Any]:
    relationship = str(row.get("relationship_type") or "overlaps")
    return {
        "external_system": system, "external_id": row.get("external_id") or row.get("gers_division_id"),
        "loc_id": row.get("loc_id"), "relationship_type": relationship,
        "identity_binding": relationship == "equivalent_identity", "is_primary": bool(row.get("is_primary")),
        "country_scope": row.get("country") or row.get("iso3"),
        "source_release": row.get("source_release") or row.get("overture_release"),
        "internal_release": row.get("internal_release") or row.get("daedalmap_release"),
        "source_level": row.get("source_level") if row.get("source_level") is not None else row.get("admin_level"),
        "external_subtype": row.get("external_subtype") or row.get("overture_subtype"),
        "identity_confidence": row.get("identity_confidence"), "geometry_confidence": row.get("geometry_confidence"),
        "external_name": row.get("external_name") or row.get("overture_name"), "loc_name": row.get("loc_name"),
        "edge_id": row.get("edge_id"), "partition_id": row.get("partition_id"),
        "bridge_generation_id": row.get("bridge_generation_id"), "edge_content_hash": row.get("edge_content_hash"),
    }

def bridge_query_result(*, system: str, country_scope: str, direction: str,
                        value: str, rows: list[dict[str, Any]], limit: int) -> dict[str, Any]:
    edges = [bridge_edge(row, system=system) for row in rows]
    edges.sort(key=lambda edge: (not edge["identity_binding"], not edge["is_primary"],
                                 -(edge["geometry_confidence"] or 0), str(edge["loc_id"]), str(edge["external_id"])))
    selected = edges[:limit]
    all_bindings = sorted({str(edge["loc_id"] if direction == "external_to_loc_id" else edge["external_id"])
                       for edge in edges if edge["identity_binding"]})
    return {"ok": True, "system": system, "country_scope": country_scope, "direction": direction,
            "value": value, "edges": selected, "edge_count": len(edges), "has_more": len(edges) > limit,
            "identity_bindings": all_bindings[:limit], "identity_binding_count": len(all_bindings),
            "identity_bindings_have_more": len(all_bindings) > limit,
            "identity_status": "unique" if len(all_bindings) == 1 else "ambiguous" if all_bindings else "none",
            "relationship_note": "Only equivalent_identity edges bind identities; overlaps and contained_by are relationship evidence."}
