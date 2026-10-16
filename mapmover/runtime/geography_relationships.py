"""Recorded and temporal relationships between canonical geographic identities.

The public ``compare_geographies`` tool is deliberately loc_id-keyed.  Name and
outside-code resolution remains the responsibility of ``resolve_reference``;
this module compares the resulting identities and their selected geometries.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any, Callable

from .geography_reference import canonicalize_loc_id, classify_loc_id_family


UN_YUGOSLAVIA_SOURCE = "https://www.un.org/en/about-us/member-states/yugoslavia"
ISO_YUGOSLAVIA_SOURCE = "https://www.iso.org/files/live/sites/isoorg/files/archive/pdf/en/iso_3166-3_newsletter_i-3.pdf"
ISO_SERBIA_MONTENEGRO_SOURCE = "https://www.iso.org/files/live/sites/isoorg/files/archive/pdf/en/iso_3166-3_newsletter_i-4.pdf"


# These are referents, not merely strings.  The ISO YUG assertion spans more
# than one referent, so time-aware resolution selects SFRY before 1992 and FRY
# afterwards instead of pretending that the code denotes one immutable state.
HISTORICAL_ENTITIES: dict[str, dict[str, Any]] = {
    "HIST-YUG-SFRY": {
        "loc_id": "HIST-YUG-SFRY",
        "name": "Socialist Federal Republic of Yugoslavia",
        "short_name": "Yugoslavia",
        "family": "historical_admin_boundary",
        "valid_from": "1945-11-29",
        "valid_to": "1992-04-27",
        "authority": "United Nations",
        "source_url": UN_YUGOSLAVIA_SOURCE,
        "successors": [
            {"loc_id": "BIH", "name": "Bosnia and Herzegovina", "relationship_type": "equal_legal_successor", "effective_from": "1992-05-22"},
            {"loc_id": "HRV", "name": "Croatia", "relationship_type": "equal_legal_successor", "effective_from": "1992-05-22"},
            {"loc_id": "SVN", "name": "Slovenia", "relationship_type": "equal_legal_successor", "effective_from": "1992-05-22"},
            {"loc_id": "MKD", "name": "North Macedonia", "relationship_type": "equal_legal_successor", "effective_from": "1993-04-08", "name_at_transition": "The former Yugoslav Republic of Macedonia"},
            {"loc_id": "HIST-YUG-FRY", "name": "Federal Republic of Yugoslavia", "relationship_type": "equal_legal_successor", "effective_from": "1992-04-27"},
        ],
    },
    "HIST-YUG-FRY": {
        "loc_id": "HIST-YUG-FRY",
        "name": "Federal Republic of Yugoslavia",
        "short_name": "Yugoslavia",
        "family": "historical_admin_boundary",
        "valid_from": "1992-04-27",
        "valid_to": "2003-02-04",
        "authority": "ISO 3166 Maintenance Agency / United Nations",
        "source_url": ISO_YUGOSLAVIA_SOURCE,
        "successors": [
            {
                "loc_id": "HIST-SCG",
                "name": "Serbia and Montenegro",
                "relationship_type": "renamed_state",
                "effective_from": "2003-02-04",
                "source_url": ISO_YUGOSLAVIA_SOURCE,
            }
        ],
    },
    "HIST-SCG": {
        "loc_id": "HIST-SCG",
        "name": "Serbia and Montenegro",
        "family": "historical_admin_boundary",
        "valid_from": "2003-02-04",
        "valid_to": "2006-06-03",
        "authority": "ISO 3166 Maintenance Agency / United Nations",
        "source_url": ISO_SERBIA_MONTENEGRO_SOURCE,
        "successors": [
            {"loc_id": "SRB", "name": "Serbia", "relationship_type": "continuing_state", "effective_from": "2006-06-03"},
            {"loc_id": "MNE", "name": "Montenegro", "relationship_type": "separated_state", "effective_from": "2006-06-03"},
        ],
    },
}


def _parse_date(value: str | date | None, *, field: str) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        if len(text) == 4:
            return date(int(text), 1, 1)
        if len(text) == 7:
            return date.fromisoformat(f"{text}-01")
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date or year") from exc


def _valid_on(entity: dict[str, Any], when: date | None) -> bool | None:
    if when is None:
        return None
    start = _parse_date(entity.get("valid_from"), field="valid_from")
    end = _parse_date(entity.get("valid_to"), field="valid_to")
    return (start is None or when >= start) and (end is None or when < end)


def _terminal_successors(
    loc_id: str,
    *,
    seen: set[str] | None = None,
    path: list[str] | None = None,
    relationship_chain: list[str] | None = None,
    terminal_hint: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    visited = set(seen or set())
    canonical = str(loc_id).strip().upper()
    if canonical in visited:
        return []
    visited.add(canonical)
    current_path = [*(path or []), canonical]
    entity = HISTORICAL_ENTITIES.get(canonical)
    if not entity:
        hint = terminal_hint or {}
        return [{
            "loc_id": canonical,
            "name": hint.get("name"),
            "relationship_type": "present_day_descendant",
            "path": current_path,
            "relationship_chain": list(relationship_chain or []),
            "geometry_request": {"tool": "get_geometry", "loc_id": canonical, "include_polygon": True},
        }]
    terminals: list[dict[str, Any]] = []
    for successor in entity.get("successors") or []:
        successor_id = str(successor.get("loc_id") or "").strip().upper()
        if not successor_id:
            continue
        nested = _terminal_successors(
            successor_id,
            seen=visited,
            path=current_path,
            relationship_chain=[*(relationship_chain or []), str(successor.get("relationship_type") or "successor")],
            terminal_hint=successor,
        )
        if nested:
            terminals.extend(nested)
        else:
            terminals.append(dict(successor))
    unique: dict[str, dict[str, Any]] = {}
    for item in terminals:
        item_id = str(item.get("loc_id") or "").strip().upper()
        if item_id:
            unique.setdefault(item_id, item)
    return list(unique.values())


def historical_entity_info(loc_id: str, *, as_of: str | date | None = None) -> dict[str, Any] | None:
    canonical = str(loc_id or "").strip().upper()
    entity = HISTORICAL_ENTITIES.get(canonical)
    if not entity:
        return None
    when = _parse_date(as_of, field="as_of")
    direct = []
    for item in entity.get("successors") or []:
        direct.append({
            **dict(item),
            "authority": item.get("authority") or entity.get("authority"),
            "source_url": item.get("source_url") or entity.get("source_url"),
        })
    return {
        **{key: value for key, value in entity.items() if key != "successors"},
        "as_of": when.isoformat() if when else None,
        "valid_at_requested_time": _valid_on(entity, when),
        "lifecycle_status": "historical",
        "direct_successors": direct,
        "present_day_descendants": _terminal_successors(canonical),
        "successor_cardinality": "many" if len(direct) > 1 else ("one" if direct else "none"),
        "successor_semantics": "Successor relationships are a graph; they do not assert identity equivalence or require one replacement.",
    }


def resolve_historical_country_reference(value: str, *, as_of: str | date | None = None) -> dict[str, Any] | None:
    """Resolve the maintained Yugoslavia/Serbia-Montenegro temporal assertions."""
    text = str(value or "").strip().upper().replace("_", "-")
    when = _parse_date(as_of, field="as_of")
    yugoslavia_aliases = {"YUG", "YU", "YUCS", "YUGOSLAVIA", "ISO3166-3-YUG", "HIST-YUG-SFRY", "HIST-YUG-FRY"}
    scg_aliases = {"SCG", "CS", "CSHH", "SERBIA AND MONTENEGRO", "HIST-SCG"}
    if text in yugoslavia_aliases:
        if text == "HIST-YUG-SFRY":
            selected = "HIST-YUG-SFRY"
        elif text == "HIST-YUG-FRY":
            selected = "HIST-YUG-FRY"
        elif when and when < date(1992, 4, 27):
            selected = "HIST-YUG-SFRY"
        else:
            selected = "HIST-YUG-FRY"
    elif text in scg_aliases:
        selected = "HIST-SCG"
    else:
        return None
    info = historical_entity_info(selected, as_of=when)
    result = {
        "ok": True,
        "from_system": "historical_country",
        "input": value,
        "as_of": when.isoformat() if when else None,
        "resolved_loc_id": selected,
        "resolved_family": "historical_admin_boundary",
        "match_type": "time_scoped_historical_identity",
        "valid_at_requested_time": info.get("valid_at_requested_time") if info else None,
        "lifecycle": info,
    }
    successor_ids = [
        str(item.get("loc_id") or "").strip().upper()
        for item in (info or {}).get("direct_successors") or []
        if str(item.get("loc_id") or "").strip()
    ]
    if successor_ids:
        successor_text = successor_ids[0] if len(successor_ids) == 1 else ", ".join(successor_ids)
        result["supersession"] = {
            "status": "superseded",
            "requested_loc_id": selected,
            "successor_loc_ids": successor_ids,
            "successor_included": False,
            "requires_explicit_selection": True,
            "prompt": f"This has been superseded by {successor_text}. Would you like that instead?",
        }
        if len(successor_ids) == 1:
            result["supersession"]["successor_loc_id"] = successor_ids[0]
    if text in yugoslavia_aliases and text not in {"HIST-YUG-SFRY", "HIST-YUG-FRY"}:
        assertions = []
        for referent_id in ("HIST-YUG-SFRY", "HIST-YUG-FRY"):
            referent = historical_entity_info(referent_id, as_of=when) or {}
            assertions.append({
                "loc_id": referent_id,
                "name": referent.get("name"),
                "valid_from": referent.get("valid_from"),
                "valid_to": referent.get("valid_to"),
                "valid_at_requested_time": referent.get("valid_at_requested_time"),
                "present_day_descendants": referent.get("present_day_descendants") or [],
            })
        result["name_history"] = assertions
        result["selection_note"] = (
            "Yugoslavia is a reused historical name/code assertion. The requested date selects a referent; "
            "when the date is later than every assertion, the latest expired referent is returned without creating a current country."
        )
    return result


def _identity_state(loc_id: str, when: date | None) -> dict[str, Any]:
    historical = historical_entity_info(loc_id, as_of=when)
    if historical:
        return historical
    canonical = canonicalize_loc_id(loc_id)
    from .reference_graph import identity_at

    graph_identity = identity_at(canonical, when)
    if graph_identity:
        valid_at = _valid_on(graph_identity, when)
        return {
            **graph_identity,
            "loc_id": canonical,
            "as_of": when.isoformat() if when else None,
            "valid_at_requested_time": valid_at,
            "lifecycle_status": "reference_graph_versioned",
            "temporal_confidence": "authority_release_window",
            "direct_successors": [],
            "present_day_descendants": [],
        }
    return {
        "loc_id": canonical,
        "family": classify_loc_id_family(canonical),
        "as_of": when.isoformat() if when else None,
        "valid_at_requested_time": None,
        "lifecycle_status": "unversioned",
        "temporal_confidence": "unknown",
        "direct_successors": [],
        "present_day_descendants": [],
    }


def _admin_spine_relationship(left_loc_id: str, right_loc_id: str,
                              left_identity: dict[str, Any], right_identity: dict[str, Any]) -> dict[str, Any] | None:
    """Return relationship evidence encoded by canonical Admin Spine loc_ids.

    Administrative loc_ids are hierarchical paths.  Prefix containment is
    therefore authoritative and does not require loading either polygon.
    Sibling paths still expose their common ancestor, but are not treated as a
    complete spatial answer because adjacency and boundary contact need shapes.
    """
    left = canonicalize_loc_id(left_loc_id)
    right = canonicalize_loc_id(right_loc_id)
    left_family = str(left_identity.get("family") or "")
    right_family = str(right_identity.get("family") or "")
    if not (left_family.startswith("admin_") and right_family.startswith("admin_")):
        return None

    left_parts = left.split("-")
    right_parts = right.split("-")
    common_parts: list[str] = []
    for left_part, right_part in zip(left_parts, right_parts):
        if left_part != right_part:
            break
        common_parts.append(left_part)
    common_ancestor = "-".join(common_parts) or None
    evidence = {
        "relationship_basis": "loc_id_admin_spine",
        "geometry_loaded": False,
        "common_ancestor_loc_id": common_ancestor,
        "left_admin_path": ["-".join(left_parts[:index]) for index in range(1, len(left_parts) + 1)],
        "right_admin_path": ["-".join(right_parts[:index]) for index in range(1, len(right_parts) + 1)],
    }
    if left == right:
        return {
            **evidence,
            "decisive": True,
            "hierarchy_relation": "same_identity",
            "spatial_relation": "equals",
            "intersects": True,
            "left_inside_right": True,
            "right_inside_left": True,
        }
    if len(left_parts) > len(right_parts) and left_parts[:len(right_parts)] == right_parts:
        return {
            **evidence,
            "decisive": True,
            "hierarchy_relation": "left_descends_from_right",
            "spatial_relation": "within",
            "intersects": True,
            "left_inside_right": True,
            "right_inside_left": False,
        }
    if len(right_parts) > len(left_parts) and right_parts[:len(left_parts)] == left_parts:
        return {
            **evidence,
            "decisive": True,
            "hierarchy_relation": "right_descends_from_left",
            "spatial_relation": "contains",
            "intersects": True,
            "left_inside_right": False,
            "right_inside_left": True,
        }
    return {
        **evidence,
        "decisive": False,
        "hierarchy_relation": "shared_ancestor" if common_ancestor else "separate_admin_trees",
    }


def _direct_graph_relationship(left_loc_id: str, right_loc_id: str,
                               edges: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Describe only the relation asserted by direct selected-graph edges."""
    if not edges:
        return None
    left = canonicalize_loc_id(left_loc_id)
    right = canonicalize_loc_id(right_loc_id)
    claims: set[str] = set()
    evidence = []
    for edge in edges:
        source = canonicalize_loc_id(str(edge.get("source_loc_id") or ""))
        target = canonicalize_loc_id(str(edge.get("target_loc_id") or ""))
        if {source, target} != {left, right}:
            continue
        kind = str(edge.get("relationship_type") or "").strip().lower()
        direction = "left_to_right" if source == left else "right_to_left"
        if kind in {"strict_parent", "contained_by", "within"}:
            claims.add("within" if direction == "left_to_right" else "contains")
        elif kind in {"contains", "parent_of"}:
            claims.add("contains" if direction == "left_to_right" else "within")
        elif kind in {"equals", "equivalent_identity"}:
            claims.add("equals")
        elif kind in {"disjoint", "no_overlap"}:
            claims.add("no_overlap")
        elif "overlap" in kind or kind == "intersects":
            claims.add("overlaps")
        evidence.append({
            "source_loc_id": source, "target_loc_id": target,
            "relationship_id": edge.get("relationship_id"),
            "relationship_type": edge.get("relationship_type"),
            "relationship_subtype": edge.get("relationship_subtype"),
            "relationship_vintage": edge.get("relationship_vintage"),
            "valid_from": edge.get("valid_from"), "valid_to": edge.get("valid_to"),
            "source_area_share": edge.get("source_area_share"),
            "target_area_share": edge.get("target_area_share"),
            "match_share": edge.get("match_share"), "method": edge.get("method"),
            "authority": edge.get("authority"), "direction": direction,
        })
    if not evidence:
        return None
    relation = next(iter(claims)) if len(claims) == 1 else "unknown"
    return {
        "relationship_basis": "selected_reference_graph",
        "geometry_loaded": False,
        "spatial_relation": relation,
        "intersects": (True if relation in {"equals", "within", "contains", "overlaps"}
                       else False if relation == "no_overlap" else None),
        "graph_evidence": evidence,
    }


def compare_geographies(
    left_loc_id: str,
    right_loc_id: str,
    *,
    as_of: str | date | None = None,
    left_as_of: str | date | None = None,
    right_as_of: str | date | None = None,
    include_successors: bool = True,
    resolution_fetcher: Callable[[str], dict[str, Any]] | None = None,
    identity_fetcher: Callable[[str, date | None], dict[str, Any]] | None = None,
    graph_fetcher: Callable[[str, str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Compare two identities using hierarchy and direct graph evidence only."""
    from .reference_exchange import resolve_loc_id_input

    resolution_fetcher = resolution_fetcher or resolve_loc_id_input
    identity_fetcher = identity_fetcher or _identity_state
    left_resolution = resolution_fetcher(left_loc_id)
    right_resolution = resolution_fetcher(right_loc_id)
    failed = [item for item in (left_resolution, right_resolution) if not item.get("ok")]
    if failed:
        return {
            "ok": False,
            "error": failed[0].get("error") or {
                "code": "public_loc_id_resolution_failed",
                "message": "preferred public loc_id could not be resolved safely",
            },
            "left_resolution": left_resolution,
            "right_resolution": right_resolution,
        }
    left_id = str(left_resolution.get("loc_id") or "").strip().upper()
    right_id = str(right_resolution.get("loc_id") or "").strip().upper()
    if not left_id or not right_id:
        return {"ok": False, "error": {"code": "invalid_comparison", "message": "left_loc_id and right_loc_id are required"}}
    common_when = _parse_date(as_of, field="as_of")
    left_when = _parse_date(left_as_of, field="left_as_of") or common_when
    right_when = _parse_date(right_as_of, field="right_as_of") or common_when
    left_identity = identity_fetcher(left_id, left_when)
    right_identity = identity_fetcher(right_id, right_when)
    if any(item.get("lifecycle_status") == "unversioned" for item in (left_identity, right_identity)):
        return {
            "ok": False,
            "error": {"code": "loc_id_not_found", "message": "both loc_ids must have selected graph or admitted historical identities"},
            "left": left_identity, "right": right_identity,
        }
    if left_resolution.get("resolved_from_public_alias"):
        left_identity["requested_loc_id"] = left_resolution.get("requested_loc_id")
        left_identity["resolved_from_public_alias"] = True
    if right_resolution.get("resolved_from_public_alias"):
        right_identity["requested_loc_id"] = right_resolution.get("requested_loc_id")
        right_identity["resolved_from_public_alias"] = True
    if not include_successors:
        for identity in (left_identity, right_identity):
            identity.pop("direct_successors", None)
            identity.pop("present_day_descendants", None)

    left_valid = left_identity.get("valid_at_requested_time")
    right_valid = right_identity.get("valid_at_requested_time")
    if left_valid is False or right_valid is False:
        temporal_relation = "one_or_more_not_valid"
    elif left_valid is True and right_valid is True:
        temporal_relation = "coexistent"
    else:
        temporal_relation = "coexistence_unknown"

    if left_valid is False or right_valid is False:
        return {
            "ok": True, "left": left_identity, "right": right_identity,
            "temporal_relation": temporal_relation,
            "relationship_basis": "selected_reference_graph",
            "geometry_loaded": False,
            "spatial_relation": "not_evaluated",
            "reason": "one or more identities are not valid at the requested time",
        }
    if left_when != right_when and (left_when is not None or right_when is not None):
        return {
            "ok": True, "left": left_identity, "right": right_identity,
            "temporal_relation": temporal_relation,
            "relationship_basis": "selected_reference_graph",
            "geometry_loaded": False, "spatial_relation": "not_evaluated",
            "reason": "pairwise spatial relation requires one shared comparison date",
        }
    if left_id == right_id:
        return {
            "ok": True, "left": left_identity, "right": right_identity,
            "temporal_relation": temporal_relation,
            "relationship_basis": "same_graph_identity",
            "geometry_loaded": False, "spatial_relation": "equals",
            "intersects": True,
        }

    admin_relationship = _admin_spine_relationship(left_id, right_id, left_identity, right_identity)
    if admin_relationship and admin_relationship.get("decisive"):
        return {
            "ok": True,
            "left": left_identity,
            "right": right_identity,
            "temporal_relation": temporal_relation,
            **{key: value for key, value in admin_relationship.items() if key != "decisive"},
        }

    if graph_fetcher is None:
        from .reference_graph import relationships_between_loc_ids

        graph_fetcher = relationships_between_loc_ids
    graph_edges = graph_fetcher(left_id, right_id)
    if left_when is not None:
        graph_edges = [edge for edge in graph_edges
                       if (edge.get("valid_from") or edge.get("valid_to")) and _valid_on(edge, left_when)]
    graph_relationship = _direct_graph_relationship(left_id, right_id, graph_edges)
    if graph_relationship:
        return {
            "ok": True,
            "left": left_identity,
            "right": right_identity,
            "temporal_relation": temporal_relation,
            **graph_relationship,
            **({
                "hierarchy_relation": admin_relationship.get("hierarchy_relation"),
                "common_ancestor_loc_id": admin_relationship.get("common_ancestor_loc_id"),
            } if admin_relationship else {}),
        }
    return {
        "ok": True,
        "left": left_identity,
        "right": right_identity,
        "temporal_relation": temporal_relation,
        "relationship_basis": "selected_reference_graph",
        "geometry_loaded": False,
        **({
            "hierarchy_relation": admin_relationship.get("hierarchy_relation"),
            "common_ancestor_loc_id": admin_relationship.get("common_ancestor_loc_id"),
        } if admin_relationship else {}),
        "spatial_relation": "unknown",
        "reason": "no direct pair relationship is published for the selected time in the selected graph; absence does not prove no overlap",
    }


def compare_geographies_batch(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compare many pairs with cached identity and exact-pair graph reads."""
    from .reference_exchange import resolve_loc_id_input
    from .reference_graph import ReferenceRouteUnavailable, relationships_between_loc_ids

    requested_ids: list[str] = []
    seen_requested: set[str] = set()
    for item in items:
        for field in ("left_loc_id", "right_loc_id"):
            requested = canonicalize_loc_id(str(item.get(field) or ""))
            if requested and requested not in seen_requested:
                seen_requested.add(requested)
                requested_ids.append(requested)

    resolutions = {requested: resolve_loc_id_input(requested) for requested in requested_ids}
    identity_cache: dict[tuple[str, date | None], dict[str, Any]] = {}
    graph_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    def cached_resolution(loc_id: str) -> dict[str, Any]:
        canonical = canonicalize_loc_id(loc_id)
        return resolutions.get(canonical) or resolve_loc_id_input(canonical)

    def cached_graph(left_loc_id: str, right_loc_id: str) -> list[dict[str, Any]]:
        left = canonicalize_loc_id(left_loc_id)
        right = canonicalize_loc_id(right_loc_id)
        key = tuple(sorted((left, right)))
        if key not in graph_cache:
            graph_cache[key] = relationships_between_loc_ids(left, right)
        return graph_cache[key]

    def cached_identity(loc_id: str, when: date | None) -> dict[str, Any]:
        key = (canonicalize_loc_id(loc_id), when)
        if key not in identity_cache:
            identity_cache[key] = _identity_state(key[0], when)
        return deepcopy(identity_cache[key])

    results: list[dict[str, Any]] = []
    for item in items:
        left_loc_id = str(item.get("left_loc_id") or "").strip()
        right_loc_id = str(item.get("right_loc_id") or "").strip()
        if not left_loc_id or not right_loc_id:
            results.append({
                "ok": False,
                "error": {
                    "code": "invalid_comparison",
                    "message": "left_loc_id and right_loc_id are required",
                },
            })
            continue
        try:
            results.append(compare_geographies(
                left_loc_id,
                right_loc_id,
                as_of=item.get("as_of"),
                left_as_of=item.get("left_as_of"),
                right_as_of=item.get("right_as_of"),
                include_successors=bool(item.get("include_successors", True)),
                resolution_fetcher=cached_resolution,
                identity_fetcher=cached_identity,
                graph_fetcher=cached_graph,
            ))
        except ReferenceRouteUnavailable as exc:
            results.append({"ok": False, "error": {"code": "reference_route_unavailable", "message": str(exc)}})
        except ValueError as exc:
            results.append({
                "ok": False,
                "error": {"code": "invalid_temporal_selector", "message": str(exc)},
            })
        except Exception as exc:
            results.append({
                "ok": False,
                "error": {"code": "compare_geographies_failed", "message": str(exc)},
            })
    return results
