"""Compact public discovery projections for MCP catalog tools.

The generated catalogs remain the source of truth. MCP discovery returns only
the fields needed to choose a pack and the next tool; callers that need the
complete catalog or one rich pack record get a stable HTTP URL instead of a
large repeated tool payload.
"""
from __future__ import annotations

from typing import Any


APP_ORIGIN = "https://app.daedalmap.com"
DATA_CATALOG_DOWNLOAD_URL = "https://downloads.daedalmap.com/downloadable/catalog.json"
GEOMETRY_CATALOG_DOWNLOAD_URL = (
    "https://downloads.daedalmap.com/downloadable/geometry/geometry_catalog.json"
)


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
        row["detail_url"] = f"{APP_ORIGIN}/api/v1/packs/{pack_id}"
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
            "full_catalog": {
                "download_url": DATA_CATALOG_DOWNLOAD_URL,
                "agent_catalog_url": f"{APP_ORIGIN}/api/v1/agent/catalog",
            },
        }
    )
    return result


def compact_pack_detail(payload: Any) -> Any:
    """Return routing and first-call guidance without provenance/source dumps."""
    if not isinstance(payload, dict):
        return payload
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
            "quick_start",
            "routing",
        ),
    )
    result["view"] = "lite"
    result["detail"] = {
        "tool_call": {
            "tool": "get_pack",
            "arguments": {"pack_id": pack_id, "detail": "full"},
        },
        "url": f"{APP_ORIGIN}/api/v1/packs/{pack_id}" if pack_id else None,
        "purpose": "Full metrics, sources, provenance, license, and citation metadata.",
    }
    return result

