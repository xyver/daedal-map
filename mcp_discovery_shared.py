"""Progressive public discovery projections for MCP catalog tools.

The generated catalogs remain the source of truth. Lite views return only the
fields needed to choose a pack and the next tool. Full MCP views add query
metadata; download views hand off the complete raw JSON without embedding it.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from catalog_coverage_shared import (
    normalize_scope,
    normalize_time_range,
    resolve_pack_coverage_match,
)


APP_ORIGIN = "https://app.daedalmap.com"
DATA_CATALOG_DOWNLOAD_URL = f"{APP_ORIGIN}/api/v1/catalog/download"
GEOMETRY_CATALOG_DOWNLOAD_URL = (
    f"{APP_ORIGIN}/api/v1/geometry/catalog/download"
)

CATALOG_DOWNLOADS = {
    "data": {
        "download_url": DATA_CATALOG_DOWNLOAD_URL,
        "summary_endpoint": f"{APP_ORIGIN}/api/v1/historical/catalog",
    },
    "geometry": {
        "download_url": GEOMETRY_CATALOG_DOWNLOAD_URL,
        "summary_endpoint": f"{APP_ORIGIN}/api/v1/geometry/catalog",
    },
}

# One canonical autonomous-agent path shared by instructions and help.
DATA_ACCESS_WORKFLOW = {
    "goal": "Find a published pack, narrow to its source and exact metrics, then retrieve only the requested rows.",
    "steps": [
        {
            "stage": "discover",
            "tool": "get_catalog",
            "arguments": {"catalog": "data"},
            "outcome": "Choose one pack_id from its topic, geography, and time coverage.",
        },
        {
            "stage": "inspect",
            "tool": "get_pack",
            "arguments": {"pack_id": "<selected pack_id>"},
            "outcome": "See which sources the pack contains, with each source's time and geographic coverage.",
        },
        {
            "stage": "narrow",
            "tool": "get_pack",
            "arguments": {"pack_id": "<selected pack_id>", "source_id": "<selected source_id>"},
            "outcome": (
                "List that source's metrics with names, units, and years, plus its filterable and sortable fields. "
                "Alternatives: search='<terms>' to match metrics across the pack, or metrics=[...] for full detail."
            ),
        },
        {
            "stage": "retrieve",
            "tool": "get_data",
            "arguments": {"pack_id": "<selected pack_id>", "metrics": ["<selected metric_id>"], "filters": {}, "limit": 100},
            "outcome": "Return the requested published data rows.",
        },
    ],
    "help": {
        "overview": {"tool": "get_tool_help", "arguments": {"topic": "overview"}},
        "exact_tool": {"tool": "get_tool_help", "arguments": {"tool_name": "<tool name>"}},
    },
}


def data_access_workflow() -> dict[str, Any]:
    """Return an isolated copy so responses cannot mutate shared guidance."""
    return deepcopy(DATA_ACCESS_WORKFLOW)


def _copy_present(source: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: source[key] for key in keys if source.get(key) is not None}


def _compact_pack_row(pack: dict[str, Any]) -> dict[str, Any]:
    pack_id = str(pack.get("pack_id") or pack.get("id") or "").strip()
    row = _copy_present(
        pack,
        (
            "pack_id",
            "pack_name",
            "title",
            "short_description",
            "category",
            "data_types",
            "scopes",
            "geographic_levels",
            "empty_geographic_levels_meaning",
            "temporal_start",
            "temporal_end",
            "canonical_available_through",
            "live_updates_enabled",
            "processing_state",
            "metric_count",
            "source_count",
            "free_detail",
            "paid_data_calls",
            "preferred_tool",
            "live_fallback_tool",
        ),
    )
    if pack_id:
        row.setdefault("pack_id", pack_id)
        row["next_call"] = {
            "tool": "get_pack",
            "arguments": {"pack_id": pack_id},
        }
        row["download_url"] = f"{APP_ORIGIN}/api/v1/packs/{pack_id}/download"
    return row


def compact_catalog_payload(payload: Any) -> Any:
    """Return the small, stable cross-pack discovery view used by get_catalog."""
    if not isinstance(payload, dict):
        return payload
    packs = [
        _compact_pack_row(item)
        for item in payload.get("packs") or []
        if isinstance(item, dict)
    ]
    result = _copy_present(
        payload,
        ("catalog_version", "schema_version", "generated_at", "source_mode"),
    )
    result.update(
        {
            "view": "lite",
            "pack_count": len(packs),
            "packs": packs,
            "usage": {
                "next_step": "Call get_pack for one selected pack before querying it.",
                "full_catalog": "Use the download URL for bulk catalog inspection instead of asking MCP to repeat the complete catalog.",
            },
            "next_step": data_access_workflow()["steps"][1],
            "full_catalog": {
                "download_url": DATA_CATALOG_DOWNLOAD_URL,
                "agent_catalog_url": f"{APP_ORIGIN}/api/v1/agent/catalog",
            },
        }
    )
    uncertain = [
        _compact_pack_row(item)
        for item in payload.get("uncertain_packs") or []
        if isinstance(item, dict)
    ]
    if uncertain or "uncertain_packs" in payload:
        result["uncertain_packs"] = uncertain
        result["uncertain_count"] = len(uncertain)
    result.update(_copy_present(
        payload,
        ("excluded_count", "result_status", "resolved_query"),
    ))
    return result


def filter_data_catalog_payload(
    payload: Any,
    *,
    loc_id: str | None = None,
    time_range: Any = None,
    detail: str | None = None,
) -> Any:
    """Resolve a conservative place/time catalog intersection.

    Confirmed matches stay in ``packs``. Packs whose authored coverage is not
    precise enough to decide are returned separately instead of being silently
    included as matches or discarded as misses.
    """
    if not isinstance(payload, dict):
        return payload
    requested_loc_id = str(loc_id or "").strip().upper() or None
    country = requested_loc_id.split("-", 1)[0] if requested_loc_id else None
    if country and normalize_scope(country) != country:
        raise ValueError("loc_id must begin with an ISO3 country code")
    requested_time = normalize_time_range(time_range)
    if not requested_loc_id and requested_time is None:
        result = dict(payload)
        result["resolved_query"] = {
            "catalog": "data", "loc_id": None, "country_scope": None,
            "time_range": None, "filter_applied": False,
        }
        return result

    matched: list[dict[str, Any]] = []
    uncertain: list[dict[str, Any]] = []
    excluded = 0
    for raw in payload.get("packs") or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        place_match, time_match = resolve_pack_coverage_match(row, country, requested_time)
        row["catalog_match"] = {
            "place": "match" if place_match is True else "unknown" if place_match is None else "no_match",
            "time": "match" if time_match is True else "unknown" if time_match is None else "no_match",
        }
        if place_match is False or time_match is False:
            excluded += 1
        elif place_match is None or time_match is None:
            uncertain.append(row)
        else:
            matched.append(row)

    result = dict(payload)
    selected_detail = str(detail or payload.get("view") or payload.get("detail") or "lite")
    result.update({
        "packs": matched,
        "pack_count": len(matched),
        "uncertain_packs": uncertain,
        "uncertain_count": len(uncertain),
        "excluded_count": excluded,
        "result_status": "empty" if not matched else "partial" if uncertain else "complete",
        "resolved_query": {
            "catalog": "data",
            "loc_id": requested_loc_id,
            "country_scope": country,
            "time_range": requested_time,
            "filter_applied": True,
            "unknown_coverage_policy": "returned_separately_not_counted_as_match",
            "rerun": {
                "tool": "get_catalog",
                "arguments": {
                    "catalog": "data", "detail": selected_detail,
                    **({"loc_id": requested_loc_id} if requested_loc_id else {}),
                    **({"time_range": requested_time} if requested_time else {}),
                },
            },
        },
    })
    return result


def full_catalog_payload(payload: Any) -> Any:
    """Project metric IDs already indexed in the published catalog."""
    if not isinstance(payload, dict):
        return payload
    def project(items: Any) -> list[dict[str, Any]]:
        projected = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            row = _compact_pack_row(item)
            row["metrics"] = list(item.get("metric_ids") or [])
            row["query_dimensions"] = {
                "data": "single_or_variable_within_pack",
                "location": "single_or_variable",
                "time": "single_or_variable",
            }
            row.update(_copy_present(item, ("supported_query_shapes",)))
            projected.append(row)
        return projected

    packs = project(payload.get("packs"))
    result = _copy_present(payload, ("catalog_version", "schema_version", "generated_at", "source_mode"))
    result.update({
        "catalog": "data",
        "detail": "full",
        "pack_count": len(packs),
        "packs": packs,
        "next_step": data_access_workflow()["steps"][1],
        "download": catalog_download_payload("data"),
    })
    uncertain = project(payload.get("uncertain_packs"))
    if uncertain or "uncertain_packs" in payload:
        result["uncertain_packs"] = uncertain
        result["uncertain_count"] = len(uncertain)
    result.update(_copy_present(
        payload,
        ("excluded_count", "result_status", "resolved_query"),
    ))
    return result


def catalog_download_payload(catalog: str) -> dict[str, Any]:
    """Return a link-only handoff without loading either catalog into memory."""
    selected = str(catalog or "data").strip().lower()
    if selected not in CATALOG_DOWNLOADS:
        raise ValueError("catalog must be 'data' or 'geometry'")
    next_step = (
        {
            "stage": "inspect",
            "tool": "get_geography",
            "arguments": {"scope_id": "<ISO3, GLOBAL, or MARINE>"},
        }
        if selected == "geometry"
        else data_access_workflow()["steps"][1]
    )
    return {
        "ok": True,
        "catalog": selected,
        "detail": "download",
        **CATALOG_DOWNLOADS[selected],
        "media_type": "application/json",
        "usage": "Download the complete public catalog directly; MCP does not inline the full catalog.",
        "next_step": next_step,
    }


def _data_query_call(payload: dict[str, Any]) -> dict[str, Any]:
    quick_start = payload.get("quick_start") if isinstance(payload.get("quick_start"), dict) else {}
    template = quick_start.get("first_query_template")
    if not isinstance(template, dict):
        template = {"pack_id": str(payload.get("pack_id") or "<selected pack_id>"), "limit": 100}
    if template.get("tool") and isinstance(template.get("arguments"), dict):
        execution_tool = str(template["tool"])
        arguments = deepcopy(template["arguments"])
    else:
        routing = payload.get("routing") if isinstance(payload.get("routing"), dict) else {}
        execution_tool = str(payload.get("preferred_tool") or routing.get("preferred_tool") or "get_data")
        arguments = deepcopy(template)
    legacy_data_tools = {
        "query_dataset",
        "get_earthquake_events",
        "get_volcanic_activity",
        "get_tsunami_events",
        "get_fx_rates",
    }
    if execution_tool in legacy_data_tools:
        execution_tool = "get_data"
    if execution_tool == "get_data":
        arguments.pop("source_id", None)
        arguments.setdefault("pack_id", str(payload.get("pack_id") or "<selected pack_id>"))
        arguments.setdefault("metrics", [])
        arguments.setdefault("filters", {})
    return {"tool": execution_tool, "arguments": arguments}


def _compact_quick_start(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    result = _copy_present(
        value,
        (
            "why_it_exists",
            "first_query_template",
            "starter_metrics",
            "starter_sources",
            "starter_tools",
        ),
    )
    start_here = value.get("start_here")
    if isinstance(start_here, list):
        result["start_here"] = start_here[:3]
    important_rules = value.get("important_rules")
    if isinstance(important_rules, list):
        result["important_rules"] = important_rules[:3]
    return result or None


def compact_pack_detail(payload: Any, *, catalog: str = "data") -> Any:
    """Return bounded one-pack routing metadata without source/provenance dumps."""
    if not isinstance(payload, dict):
        return payload
    selected_catalog = str(catalog or "data").strip().lower()
    if selected_catalog not in {"data", "geometry"}:
        raise ValueError("catalog must be 'data' or 'geometry'")
    pack_id = str(payload.get("pack_id") or payload.get("id") or "").strip()
    result = _copy_present(
        payload,
        (
            "pack_id",
            "pack_name",
            "title",
            "description",
            "catalog_short_description",
            "category",
            "data_types",
            "location",
            "topic_tags",
            "temporal_coverage",
            "coverage_description",
            "canonical_available_through",
            "live_updates_enabled",
            "processing_state",
            "source_count",
            "source_ids",
            "metric_count",
            "supported_query_shapes",
            "preferred_tool",
            "live_fallback_tool",
            "live_fallback_when",
            "pricing",
            "routing",
        ),
    )
    quick_start = _compact_quick_start(payload.get("quick_start"))
    if quick_start:
        result["quick_start"] = quick_start
    if selected_catalog == "geometry":
        tools = []
        for item in payload.get("tools") or []:
            if isinstance(item, dict) and item.get("name"):
                tools.append(_copy_present(item, ("name", "summary")))
        result.update({"tool_count": len(tools), "tools": tools})
        if payload.get("display_name") is not None:
            result["display_name"] = payload["display_name"]
    next_step = _data_query_call(payload)
    if quick_start and next_step.get("tool") == "get_data":
        quick_start["first_query_template"] = deepcopy(next_step["arguments"])
    result.update({
        "catalog": selected_catalog,
        "kind": "tool_family" if selected_catalog == "geometry" else "data_pack",
        "detail": "lite",
        "download_url": f"{APP_ORIGIN}/api/v1/packs/{pack_id}/download" if pack_id else None,
        "full_call": {
            "tool": "get_pack",
            "arguments": {
                "catalog": selected_catalog,
                "pack_id": pack_id,
                "detail": "full",
            },
        },
        "download_call": {
            "tool": "get_pack",
            "arguments": {
                "catalog": selected_catalog,
                "pack_id": pack_id,
                "detail": "download",
            },
        },
        "next_step": next_step,
    })
    return result


def annotate_full_pack_detail(payload: Any, *, catalog: str) -> Any:
    """Give full data and geometry records the same small response envelope."""
    if not isinstance(payload, dict):
        return payload
    selected_catalog = str(catalog or "data").strip().lower()
    if selected_catalog not in {"data", "geometry"}:
        raise ValueError("catalog must be 'data' or 'geometry'")
    result = dict(payload)
    pack_id = str(result.get("pack_id") or result.get("id") or "").strip()
    result.update({
        "catalog": selected_catalog,
        "kind": "tool_family" if selected_catalog == "geometry" else "data_pack",
        "detail": "full",
        "download_url": f"{APP_ORIGIN}/api/v1/packs/{pack_id}/download" if pack_id else None,
        "next_step": _data_query_call(payload),
    })
    return result


PACK_METRIC_DETAIL_LIMIT = 10
PACK_METRIC_SEARCH_LIMIT = 25
PACK_SOURCE_DESCRIPTION_CHARS = 280
PACK_SEARCH_MIN_CHARS = 2
PACK_SEARCH_MAX_CHARS = 100

# Source-native indicator code fields, mapped to one public field name.
_METRIC_SOURCE_CODE_FIELDS = ("wdi_code", "indicator_code", "source_code", "series_code")
# Fields copied into every compact metric line; full detail adds the rest.
_COMPACT_METRIC_FIELDS = ("name", "unit", "aggregation")
_QUERY_CONTRACT_FIELDS = (
    "time_field", "time_granularity", "location_field", "geographic_level",
    "filterable_fields", "sortable_fields", "default_limit",
)
_LEGACY_DATA_TOOLS = {
    "query_dataset", "get_earthquake_events", "get_volcanic_activity", "get_tsunami_events", "get_fx_rates",
}

SourceMetadataLoader = Any  # Callable[[str], dict | None]; Any keeps this module import-light.


def _metric_ids_of(source: dict[str, Any]) -> list[str]:
    metrics = source.get("metrics")
    if isinstance(metrics, dict):
        return sorted(str(key) for key in metrics if str(key))
    if isinstance(metrics, list):
        return sorted({
            str(value if isinstance(value, str) else value.get("metric_id") or value.get("id") or "")
            for value in metrics if isinstance(value, (str, dict))
        } - {""})
    if isinstance(source.get("metric_ids"), list):
        return sorted({str(value) for value in source["metric_ids"] if str(value)})
    return []


def _source_metadata(source_id: str, loader: SourceMetadataLoader) -> dict[str, Any]:
    if loader is None or not source_id:
        return {}
    try:
        metadata = loader(source_id)
    except Exception:
        return {}
    return metadata if isinstance(metadata, dict) else {}


def _source_metric_records(source: dict[str, Any], metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Merge catalog metric facts with the richer source metadata record."""
    records: dict[str, dict[str, Any]] = {}
    catalog_metrics = source.get("metrics") if isinstance(source.get("metrics"), dict) else {}
    metadata_metrics = metadata.get("metrics") if isinstance(metadata.get("metrics"), dict) else {}
    for metric_id in sorted(set(catalog_metrics) | set(metadata_metrics) | set(_metric_ids_of(source))):
        merged: dict[str, Any] = {}
        for layer in (catalog_metrics.get(metric_id), metadata_metrics.get(metric_id)):
            if isinstance(layer, dict):
                merged.update(layer)
            elif isinstance(layer, str) and layer.strip():
                merged.setdefault("description", layer)
        records[str(metric_id)] = merged
    return records


def _years_range(value: Any) -> list[Any] | None:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return [value[0], value[1]]
    return None


def _source_code(record: dict[str, Any]) -> dict[str, Any]:
    for field in _METRIC_SOURCE_CODE_FIELDS:
        value = record.get(field)
        if value not in (None, ""):
            return {"source_code": value, "source_code_system": field}
    return {}


def _compact_metric_line(metric_id: str, record: dict[str, Any], source_id: str,
                         also_in: list[str]) -> dict[str, Any]:
    line: dict[str, Any] = {"metric_id": metric_id, "source_id": source_id}
    line.update(_copy_present(record, _COMPACT_METRIC_FIELDS))
    years = _years_range(record.get("years"))
    if years:
        line["years"] = years
    if record.get("countries") is not None:
        line["country_count"] = record["countries"]
    line.update(_source_code(record))
    if also_in:
        line["also_in_sources"] = also_in
    return line


def _pack_metric_index(payload: dict[str, Any], loader: SourceMetadataLoader) -> list[dict[str, Any]]:
    """One entry per source with merged metric records, in catalog source order."""
    index = []
    for source in payload.get("sources") or []:
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("source_id") or "").strip()
        if not source_id:
            continue
        metadata = _source_metadata(source_id, loader)
        index.append({
            "source": source,
            "metadata": metadata,
            "source_id": source_id,
            "metrics": _source_metric_records(source, metadata),
        })
    return index


def _metric_locations(index: list[dict[str, Any]]) -> dict[str, list[str]]:
    locations: dict[str, list[str]] = {}
    for entry in index:
        for metric_id in entry["metrics"]:
            locations.setdefault(metric_id, []).append(entry["source_id"])
    return locations


def _temporal_bounds(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    bounds = _copy_present(value, ("start", "end", "granularity"))
    return bounds or None


def _source_summary(entry: dict[str, Any], pack_id: str) -> dict[str, Any]:
    source, metadata = entry["source"], entry["metadata"]
    summary = _copy_present(source, ("source_id", "source_name", "data_type", "time_granularity", "geographic_level"))
    description = str(metadata.get("description") or source.get("llm_summary") or "").strip()
    if description:
        if len(description) > PACK_SOURCE_DESCRIPTION_CHARS:
            description = description[:PACK_SOURCE_DESCRIPTION_CHARS].rsplit(" ", 1)[0] + "..."
        summary["description"] = description
    temporal = _temporal_bounds(metadata.get("temporal_coverage") or source.get("temporal_coverage"))
    if temporal:
        summary["temporal_coverage"] = temporal
    summary["metric_count"] = len(entry["metrics"])
    summary["metrics_call"] = {"tool": "get_pack", "arguments": {"pack_id": pack_id, "source_id": entry["source_id"]}}
    return summary


def _query_contract(entry: dict[str, Any]) -> dict[str, Any]:
    contract: dict[str, Any] = {}
    for field in _QUERY_CONTRACT_FIELDS:
        value = entry["metadata"].get(field)
        if value is None:
            value = entry["source"].get(field)
        if value is not None:
            contract[field] = deepcopy(value)
    temporal = _temporal_bounds(entry["metadata"].get("temporal_coverage") or entry["source"].get("temporal_coverage"))
    if temporal:
        contract["temporal_coverage"] = temporal
    return contract


def _pack_base(payload: dict[str, Any], catalog: str) -> dict[str, Any]:
    result = compact_pack_detail(payload, catalog=catalog)
    # The public get_pack schema takes pack_id plus one optional narrowing
    # selector. These older handoffs used removed catalog/detail arguments.
    result.pop("full_call", None)
    result.pop("download_call", None)
    if result.get("preferred_tool") in _LEGACY_DATA_TOOLS:
        result["preferred_tool"] = "get_data"
    return result


def _pack_error(pack_id: str, code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"pack_id": pack_id, "error": {"code": code, "message": message, "details": details}}


def mcp_full_pack_detail(payload: Any, *, catalog: str, source_metadata: SourceMetadataLoader = None) -> Any:
    """Shallow get_pack: what sources are in this pack and how to inspect each."""
    if not isinstance(payload, dict):
        return payload
    result = _pack_base(payload, catalog)
    pack_id = str(result.get("pack_id") or "").strip()
    result["detail"] = "sources"
    index = _pack_metric_index(payload, source_metadata)
    if index:
        result["sources"] = [_source_summary(entry, pack_id) for entry in index]
        result["source_count"] = len(index)
        result["metric_count"] = len(_metric_locations(index))
        result["narrowing"] = (
            "Call get_pack with one source_id to list that source's metrics and query fields, "
            "search to match metrics across this pack, or metrics for full detail on up to "
            f"{PACK_METRIC_DETAIL_LIMIT} exact metric ids."
        )
    elif isinstance(payload.get("metrics"), dict):
        result["metric_ids"] = sorted(str(key) for key in payload["metrics"] if str(key))
        result["metric_count"] = len(result["metric_ids"])
    return result


def mcp_pack_source_detail(payload: Any, *, source_id: str, source_metadata: SourceMetadataLoader = None) -> Any:
    """get_pack(source_id): every metric in one source plus its query contract."""
    if not isinstance(payload, dict):
        return payload
    result = _pack_base(payload, "data")
    pack_id = str(result.get("pack_id") or "").strip()
    index = _pack_metric_index(payload, source_metadata)
    entry = next((item for item in index if item["source_id"] == source_id), None)
    if entry is None:
        return _pack_error(
            pack_id, "source_not_in_pack", f"Source '{source_id}' is not in pack '{pack_id}'.",
            available_source_ids=[item["source_id"] for item in index],
        )
    locations = _metric_locations(index)
    result["detail"] = "source_metrics"
    result["source"] = _source_summary(entry, pack_id)
    result["source"].pop("metrics_call", None)
    result["query_contract"] = _query_contract(entry)
    result["metrics"] = [
        _compact_metric_line(metric_id, record, source_id, [s for s in locations[metric_id] if s != source_id])
        for metric_id, record in entry["metrics"].items()
    ]
    result["metric_count"] = len(result["metrics"])
    return result


def mcp_pack_metric_detail(payload: Any, *, metric_ids: list[str], source_metadata: SourceMetadataLoader = None) -> Any:
    """get_pack(metrics): full stored detail for a few exact metric ids."""
    if not isinstance(payload, dict):
        return payload
    result = _pack_base(payload, "data")
    pack_id = str(result.get("pack_id") or "").strip()
    index = _pack_metric_index(payload, source_metadata)
    details: list[dict[str, Any]] = []
    unknown: list[str] = []
    for metric_id in metric_ids:
        occurrences = []
        for entry in index:
            record = entry["metrics"].get(metric_id)
            if record is None:
                continue
            occurrence = {"source_id": entry["source_id"]}
            occurrence.update(_copy_present(entry["source"], ("source_name", "time_granularity", "geographic_level")))
            occurrence.update(deepcopy(record))
            for field in _METRIC_SOURCE_CODE_FIELDS:
                occurrence.pop(field, None)
            occurrence.update(_source_code(record))
            if "countries" in occurrence:
                occurrence["country_count"] = occurrence.pop("countries")
            occurrences.append(occurrence)
        if occurrences:
            details.append({"metric_id": metric_id, "occurrences": occurrences})
        else:
            unknown.append(metric_id)
    if not details:
        return _pack_error(
            pack_id, "metric_not_in_pack", f"None of the requested metrics are in pack '{pack_id}'.",
            unknown_metrics=unknown, retry_hint="Call get_pack with search or source_id to find exact metric ids.",
        )
    result["detail"] = "metric_detail"
    result["metrics"] = details
    if unknown:
        result["unknown_metrics"] = unknown
    return result


def _search_tokens(text: str) -> list[str]:
    return [token for token in "".join(ch.lower() if ch.isalnum() else " " for ch in text).split() if token]


def mcp_pack_metric_search(payload: Any, *, query: str, source_metadata: SourceMetadataLoader = None) -> Any:
    """get_pack(search): compact metric lines whose id, name, description, or keywords match every term."""
    if not isinstance(payload, dict):
        return payload
    result = _pack_base(payload, "data")
    tokens = _search_tokens(query)
    index = _pack_metric_index(payload, source_metadata)
    locations = _metric_locations(index)
    ranked: list[tuple[int, int, dict[str, Any]]] = []
    order = 0
    for entry in index:
        for metric_id, record in entry["metrics"].items():
            title = f"{metric_id} {record.get('name') or ''}".lower()
            keywords = record.get("keywords") if isinstance(record.get("keywords"), list) else []
            body = f"{title} {record.get('description') or ''} {' '.join(str(k) for k in keywords)}".lower()
            if not tokens or not all(token in body for token in tokens):
                continue
            rank = 0 if all(token in title for token in tokens) else 1
            others = [s for s in locations[metric_id] if s != entry["source_id"]]
            ranked.append((rank, order, _compact_metric_line(metric_id, record, entry["source_id"], others)))
            order += 1
    ranked.sort(key=lambda item: (item[0], item[1]))
    matches = [line for _, _, line in ranked]
    result["detail"] = "metric_search"
    result["search"] = query
    result["match_count"] = len(matches)
    result["truncated"] = len(matches) > PACK_METRIC_SEARCH_LIMIT
    result["metrics"] = matches[:PACK_METRIC_SEARCH_LIMIT]
    if not matches:
        result["retry_hint"] = "No metric matched every term. Try fewer or broader terms, or list a source with source_id."
    return result


def pack_download_payload(pack_id: str, *, catalog: str, payload: Any) -> dict[str, Any]:
    """Return a link-only raw-metadata handoff and the executable next call."""
    selected_catalog = str(catalog or "data").strip().lower()
    if selected_catalog not in {"data", "geometry"}:
        raise ValueError("catalog must be 'data' or 'geometry'")
    normalized = str(pack_id or "").strip()
    return {
        "ok": True,
        "catalog": selected_catalog,
        "kind": "tool_family" if selected_catalog == "geometry" else "data_pack",
        "pack_id": normalized,
        "detail": "download",
        "download_url": f"{APP_ORIGIN}/api/v1/packs/{normalized}/download",
        "media_type": "application/json",
        "next_step": _data_query_call(payload if isinstance(payload, dict) else {"pack_id": normalized}),
    }
