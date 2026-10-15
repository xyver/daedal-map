"""Warm the small shared runtime control catalogs through canonical loaders."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from mapmover import logger
from mapmover.catalog_cache_policy import CONTROL_CATALOG_CACHE_TTL_SECONDS
from mapmover.catalog_surface import catalog_surface_scope
from mapmover.data_loading import (
    load_api_catalog, load_api_guide, load_api_pack_detail, load_catalog, load_full_catalog,
)
from mapmover.ops_feed_registry import load_ops_feed_records
from mapmover.runtime.geometry_catalog import load_geometry_catalog, refresh_geometry_catalog


APP_CATALOG_REFRESH_AHEAD_SECONDS = max(5, min(240, CONTROL_CATALOG_CACHE_TTL_SECONDS - 30))
_refresh_state_lock = threading.Lock()
_refresh_state: dict[str, object] = {
    "state": "not_started", "last_success_at": None, "last_success_monotonic": None,
    "last_duration_ms": None, "last_error": None,
}


def _record_count(payload: dict, *keys: str) -> int:
    return max(
        (
            len(payload.get(key) or [])
            for key in keys
            if isinstance(payload.get(key), list)
        ),
        default=0,
    )


def prewarm_control_catalogs() -> dict[str, int]:
    """Populate caches for small catalogs shared across app and MCP surfaces."""
    published = load_catalog()
    wip = load_full_catalog()
    geometry = load_geometry_catalog()
    ops_feeds = load_ops_feed_records()

    counts = {
        "published": _record_count(published, "sources", "packs"),
        "wip": _record_count(wip, "sources", "packs"),
        "geometry": _record_count(
            geometry,
            "geometry_banks",
            "geometry_products",
            "country_profiles",
            "geometry_collections",
        ),
        "ops_feeds": len(ops_feeds),
    }
    missing = [name for name, count in counts.items() if count <= 0]
    if missing:
        raise RuntimeError(
            "control catalog prewarm returned no records for: " + ", ".join(missing)
        )
    logger.info(
        "Control catalogs warmed: published=%d wip=%d geometry=%d ops_feeds=%d",
        counts["published"],
        counts["wip"],
        counts["geometry"],
        counts["ops_feeds"],
    )
    return counts


def refresh_app_catalogs() -> dict[str, int]:
    """Revalidate small app catalogs before TTL expiry, replacing each only after its read succeeds."""
    with catalog_surface_scope("api"):
        published = load_catalog(force_refresh=True)
    wip = load_full_catalog(force_refresh=True)
    geometry = refresh_geometry_catalog()
    ops_feeds = load_ops_feed_records(force_refresh=True)
    agent = load_api_catalog(force_refresh=True)
    guide = load_api_guide(force_refresh=True)
    packs = agent.get("packs") or []
    if not guide or not packs:
        raise RuntimeError("app catalog refresh returned incomplete Agent Catalog metadata")
    for pack in packs:
        pack_id = str(pack.get("pack_id") or "").strip()
        if pack_id and not load_api_pack_detail(pack_id, force_refresh=True):
            raise RuntimeError(f"app catalog refresh could not load pack {pack_id}")
    counts = {
        "published": _record_count(published, "sources", "packs"),
        "wip": _record_count(wip, "sources", "packs"),
        "geometry": _record_count(geometry, "geometry_banks", "geometry_products", "country_profiles", "geometry_collections"),
        "ops_feeds": len(ops_feeds),
        "agent_packs": len(packs),
    }
    missing = [name for name, count in counts.items() if count <= 0]
    if missing:
        raise RuntimeError("app catalog refresh returned no records for: " + ", ".join(missing))
    return counts


def app_catalog_refresh_status() -> dict[str, object]:
    with _refresh_state_lock:
        status = dict(_refresh_state)
    last_success = status.pop("last_success_monotonic", None)
    status["age_seconds"] = round(time.monotonic() - last_success, 1) if isinstance(last_success, float) else None
    status["refresh_ahead_seconds"] = APP_CATALOG_REFRESH_AHEAD_SECONDS
    return status


def maintain_app_catalogs(stop_event: threading.Event, *, startup_monotonic: float) -> None:
    """Refresh the app process on the same cadence as the VPS residency heartbeat."""
    from mapmover.prewarm_status import prewarm_readiness

    while not stop_event.wait(1):
        state = str(prewarm_readiness().get("state") or "")
        if state == "ready":
            break
        if state == "failed":
            return
    else:
        return
    with _refresh_state_lock:
        _refresh_state.update({
            "state": "ready",
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_success_monotonic": startup_monotonic,
        })
    next_refresh = startup_monotonic + APP_CATALOG_REFRESH_AHEAD_SECONDS
    while not stop_event.wait(max(0, next_refresh - time.monotonic())):
        started = time.monotonic()
        with _refresh_state_lock:
            _refresh_state["state"] = "refreshing"
        try:
            counts = refresh_app_catalogs()
        except Exception as exc:
            logger.exception("App catalog refresh-ahead failed")
            with _refresh_state_lock:
                _refresh_state.update({"state": "error", "last_error": f"{type(exc).__name__}: {exc}"[:240]})
        else:
            with _refresh_state_lock:
                _refresh_state.update({
                    "state": "ready", "last_success_at": datetime.now(timezone.utc).isoformat(),
                    "last_success_monotonic": time.monotonic(),
                    "last_duration_ms": round((time.monotonic() - started) * 1000, 1),
                    "last_error": None, "counts": counts,
                })
        next_refresh = time.monotonic() + APP_CATALOG_REFRESH_AHEAD_SECONDS
