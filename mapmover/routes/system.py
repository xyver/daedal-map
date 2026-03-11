"""System, settings, queue, and cache API router endpoints."""

import csv
import io
import json
import os
from pathlib import Path

import msgpack
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from mapmover.auth_context import build_session_cache_key, get_authenticated_user
from mapmover import CacheSignature, logger, session_manager
from mapmover.order_queue import order_queue
from mapmover.routes.disasters.helpers import msgpack_error, msgpack_response
from mapmover.settings import get_settings_with_status, init_backup_folders, save_settings


router = APIRouter()
BASE_DIR = Path(__file__).resolve().parents[2]


async def decode_request_body(request: Request) -> dict:
    """Decode MessagePack request body."""
    body_bytes = await request.body()
    return msgpack.unpackb(body_bytes, raw=False)


@router.get("/health")
async def health_check():
    """Health check endpoint for Railway/Docker deployments."""
    return {"status": "healthy", "service": "county-map-api"}


@router.get("/debug/cache")
async def debug_cache():
    """List files in the S3 local cache directory (S3 mode only)."""
    from mapmover.duckdb_helpers import is_s3_mode
    from mapmover.paths import DATA_ROOT
    cache_dir = DATA_ROOT
    if not cache_dir.exists():
        return {"error": f"cache dir does not exist: {cache_dir}"}
    files = sorted(str(p.relative_to(cache_dir)) for p in cache_dir.rglob("*") if p.is_file())
    return {
        "s3_mode": is_s3_mode(),
        "cache_dir": str(cache_dir),
        "file_count": len(files),
        "files": files,
    }


@router.get("/api/catalog/overlays")
async def get_catalog_overlays():
    """Get overlay tree from the catalog for the frontend layer panel."""
    from mapmover.data_loading import load_catalog

    catalog = load_catalog()
    return msgpack_response(
        {
            "overlay_tree": catalog.get("overlay_tree", {}),
            "overlay_count": catalog.get("overlay_count", 0),
        }
    )


@router.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the frontend HTML shell."""
    template_path = BASE_DIR / "templates" / "index.html"
    return template_path.read_text(encoding="utf-8")


@router.get("/reference/admin-levels")
async def get_admin_levels():
    """Get admin level names for all countries."""
    try:
        ref_path = BASE_DIR / "mapmover" / "reference" / "admin_levels.json"
        if not ref_path.exists():
            return msgpack_error("admin_levels.json not found", 404)

        with open(ref_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return msgpack_response(data)
    except Exception as e:
        logger.error(f"Error loading admin_levels.json: {e}")
        return msgpack_error(str(e), 500)


@router.get("/settings")
async def get_settings():
    """Get current application settings."""
    try:
        return msgpack_response(get_settings_with_status())
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/auth/config")
async def get_auth_config():
    """Return safe public auth configuration for the frontend."""
    return {
        "enabled": bool(os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_ANON_KEY")),
        "supabase_url": os.getenv("SUPABASE_URL", ""),
        "supabase_anon_key": os.getenv("SUPABASE_ANON_KEY", ""),
    }


@router.get("/api/auth/me")
async def get_auth_me(req: Request):
    """
    Return the current user's identity and plan info.

    - Unauthenticated: returns guest defaults
    - Authenticated without service key: returns basic identity from token
    - Authenticated with service key: returns full profile and plan from Supabase
    """
    auth_user = get_authenticated_user(req)

    if not auth_user:
        return msgpack_response({
            "authenticated": False,
            "plan_id": "free",
            "enabled_shells": ["simple"],
            "max_packs": 2,
        })

    user_id = auth_user.get("id")
    email = auth_user.get("email")

    # Try to load full profile via service key
    service_key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if service_key:
        try:
            from supabase_client import SupabaseClient
            supa = SupabaseClient()
            context = supa.get_user_entitlement_context(user_id)
            if context and not context.get("error"):
                return msgpack_response({
                    "authenticated": True,
                    "user_id": user_id,
                    "email": email,
                    "plan_id": context.get("plan_id", "free"),
                    "is_admin": context.get("is_admin", False),
                    "enabled_shells": context.get("enabled_shells", ["simple"]),
                    "max_packs": context.get("max_packs", 2),
                    "org_id": context.get("org_id"),
                    "user_packs": context.get("user_packs", []),
                    "org_packs": context.get("org_packs", []),
                })
        except Exception as exc:
            logger.warning(f"Failed to load entitlement context: {exc}")

    # Fallback: identity from token only, default to free plan
    return msgpack_response({
        "authenticated": True,
        "user_id": user_id,
        "email": email,
        "plan_id": "free",
        "enabled_shells": ["simple"],
        "max_packs": 2,
    })


@router.post("/settings")
async def update_settings(req: Request):
    """Update application settings."""
    try:
        data = await decode_request_body(req)
        backup_path = data.get("backup_path", "")
        success = save_settings({"backup_path": backup_path})
        if not success:
            return msgpack_error("Failed to save settings", 500)

        return msgpack_response({"success": True, "settings": get_settings_with_status()})
    except Exception as e:
        logger.error(f"Error updating settings: {e}")
        return msgpack_error(str(e), 500)


@router.post("/settings/init-folders")
async def initialize_folders(req: Request):
    """Initialize the backup folder structure."""
    try:
        data = await decode_request_body(req)
        backup_path = data.get("backup_path", "")
        if not backup_path:
            return msgpack_error("Backup path is required", 400)

        save_settings({"backup_path": backup_path})
        folders = init_backup_folders(backup_path)
        return msgpack_response({"success": True, "folders": folders, "message": f"Initialized folders at {backup_path}"})
    except Exception as e:
        logger.error(f"Error initializing folders: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/orders/queue")
async def queue_order_endpoint(req: Request):
    """Add an order to the processing queue."""
    try:
        body = await decode_request_body(req)
        items = body.get("items", [])
        hints = body.get("hints", {})
        session_id = body.get("session_id", "default")
        if not items:
            return msgpack_error("No items provided", 400)

        queue_id = order_queue.add(items, hints, session_id)
        order = order_queue.get(queue_id)
        return msgpack_response(
            {
                "queue_id": queue_id,
                "status": "queued",
                "position": order.position if order else 0,
                "message": order.message if order else "Queued",
            }
        )
    except Exception as e:
        logger.error(f"Error queueing order: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/orders/status")
async def get_order_status_endpoint(req: Request):
    """Get status of one or more queued orders."""
    try:
        body = await decode_request_body(req)
        queue_ids = body.get("queue_ids", [])
        if not queue_ids:
            return msgpack_error("No queue_ids provided", 400)

        statuses = {}
        for qid in queue_ids:
            status = order_queue.get_status(qid)
            statuses[qid] = status if status else {"error": "Not found", "status": "not_found"}
        return msgpack_response(statuses)
    except Exception as e:
        logger.error(f"Error getting order status: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/orders/status/{queue_id}")
async def get_single_order_status_endpoint(queue_id: str):
    """Get status of a single queued order."""
    try:
        status = order_queue.get_status(queue_id)
        if not status:
            return msgpack_error("Order not found", 404)
        return msgpack_response(status)
    except Exception as e:
        logger.error(f"Error getting order status: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/orders/cancel")
async def cancel_order_endpoint(req: Request):
    """Cancel a pending order."""
    try:
        body = await decode_request_body(req)
        queue_id = body.get("queue_id")
        if not queue_id:
            return msgpack_error("No queue_id provided", 400)

        cancelled = order_queue.cancel(queue_id)
        if cancelled:
            return msgpack_response({"cancelled": True})
        return msgpack_response({"cancelled": False, "reason": "Order not found or already processing"})
    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/orders/session/{session_id}")
async def get_session_orders_endpoint(session_id: str):
    """Get all queued orders for a session."""
    try:
        return msgpack_response({"orders": order_queue.get_session_orders(session_id)})
    except Exception as e:
        logger.error(f"Error getting session orders: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/session/clear")
async def clear_session_endpoint(req: Request):
    """Clear session cache for a chat session."""
    try:
        body = await decode_request_body(req)
        frontend_session_id = body.get("sessionId")
        if not frontend_session_id:
            return msgpack_error("sessionId required", 400)
        auth_user = get_authenticated_user(req)
        session_id = build_session_cache_key(frontend_session_id, auth_user)

        cleared = session_manager.clear_session(session_id)
        if cleared:
            logger.info(f"Cleared session cache: {session_id}")
            return msgpack_response({"status": "cleared", "sessionId": frontend_session_id})
        return msgpack_response({"status": "not_found", "sessionId": frontend_session_id})
    except Exception as e:
        logger.error(f"Error clearing session: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/session/clear-source")
async def clear_session_source_endpoint(req: Request):
    """Clear a specific source from session cache."""
    try:
        body = await decode_request_body(req)
        frontend_session_id = body.get("sessionId")
        source_id = body.get("sourceId")
        if not frontend_session_id or not source_id:
            return msgpack_error("sessionId and sourceId required", 400)
        auth_user = get_authenticated_user(req)
        session_id = build_session_cache_key(frontend_session_id, auth_user)

        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_response({"status": "not_found", "sessionId": frontend_session_id})

        removed = cache.clear_source(source_id)
        logger.info(f"Cleared source '{source_id}' from session {session_id}: {removed} keys removed")
        return msgpack_response({"status": "cleared", "sourceId": source_id, "keys_removed": removed})
    except Exception as e:
        logger.error(f"Error clearing session source: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/session/{session_id}/status")
async def get_session_status_endpoint(session_id: str):
    """Get session status for recovery prompt."""
    try:
        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_response({"exists": False, "session_id": session_id})

        status = cache.get_status()
        status["cached_results"] = len(cache._results)
        status["inventory"] = {
            "total_locations": status.get("total_locations", 0),
            "total_metrics": status.get("total_metrics", 0),
        }
        return msgpack_response({"exists": True, **status})
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/cache/inventory/{session_id}")
async def get_cache_inventory_endpoint(session_id: str):
    """Get detailed cache inventory for a session."""
    try:
        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_response({"exists": False, "session_id": session_id})

        inventory_stats = cache.inventory.stats()
        combined = cache.inventory.combined_signature()
        return msgpack_response(
            {
                "exists": True,
                "session_id": session_id,
                "inventory": {
                    "entry_count": inventory_stats["entry_count"],
                    "total_locations": inventory_stats["total_locations"],
                    "total_years": inventory_stats["total_years"],
                    "total_metrics": inventory_stats["total_metrics"],
                    "year_range": inventory_stats["year_range"],
                },
                "combined_signature": {
                    "loc_id_count": len(combined.loc_ids),
                    "year_count": len(combined.years),
                    "metric_count": len(combined.metrics),
                    "years": sorted(combined.years) if combined.years else [],
                    "metrics": sorted(combined.metrics) if combined.metrics else [],
                },
                "cached_results": len(cache._results),
            }
        )
    except Exception as e:
        logger.error(f"Error getting cache inventory: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/cache/delta")
async def compute_cache_delta_endpoint(req: Request):
    """Compute what data needs to be fetched given what is already cached."""
    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId", "anonymous")
        want = body.get("want", {})
        if not want:
            return msgpack_error("'want' field required", 400)

        requested = CacheSignature(
            loc_ids=frozenset(want.get("loc_ids", [])),
            years=frozenset(want.get("years", [])),
            metrics=frozenset(want.get("metrics", [])),
        )
        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_response(
                {
                    "need_fetch": True,
                    "delta": {
                        "loc_ids": list(requested.loc_ids),
                        "years": sorted(requested.years),
                        "metrics": list(requested.metrics),
                    },
                    "have": {"loc_ids": [], "years": [], "metrics": []},
                }
            )

        if cache.can_serve(requested):
            return msgpack_response(
                {
                    "need_fetch": False,
                    "delta": {"loc_ids": [], "years": [], "metrics": []},
                    "have": {
                        "loc_ids": list(requested.loc_ids),
                        "years": sorted(requested.years),
                        "metrics": list(requested.metrics),
                    },
                }
            )

        delta = cache.compute_delta(requested)
        combined = cache.inventory.combined_signature()
        return msgpack_response(
            {
                "need_fetch": True,
                "delta": {
                    "loc_ids": list(delta.loc_ids),
                    "years": sorted(delta.years),
                    "metrics": list(delta.metrics),
                },
                "have": {
                    "loc_ids": list(combined.loc_ids),
                    "years": sorted(combined.years),
                    "metrics": list(combined.metrics),
                },
            }
        )
    except Exception as e:
        logger.error(f"Error computing cache delta: {e}")
        return msgpack_error(str(e), 500)


@router.post("/api/cache/export")
async def export_cache_endpoint(req: Request):
    """Export cached data as CSV or JSON."""
    try:
        body = await decode_request_body(req)
        session_id = body.get("sessionId", "anonymous")
        export_format = body.get("format", "csv")
        filters = body.get("filters", {})

        cache = session_manager.get(session_id)
        if not cache:
            return msgpack_error("Session not found", 404)

        all_rows = []
        for result in cache._results.values():
            features = result.get("geojson", {}).get("features", [])
            for feature in features:
                props = feature.get("properties", {})
                if filters.get("loc_ids") and props.get("loc_id") not in filters["loc_ids"]:
                    continue
                if filters.get("years"):
                    year = props.get("year")
                    if year is not None and int(year) not in filters["years"]:
                        continue

                row = {}
                for key, value in props.items():
                    if key in ["geometry", "type"]:
                        continue
                    if filters.get("metrics"):
                        non_metric_keys = {"loc_id", "year", "name", "country", "admin_level", "parent_id", "iso3"}
                        if key not in non_metric_keys and key not in filters["metrics"]:
                            continue
                    row[key] = json.dumps(value) if isinstance(value, (dict, list)) else value
                all_rows.append(row)

        if not all_rows:
            return msgpack_error("No data in cache", 404)

        if export_format == "json":
            return msgpack_response({"format": "json", "row_count": len(all_rows), "data": all_rows})

        columns = set()
        for row in all_rows:
            columns.update(row.keys())

        priority_cols = ["loc_id", "year", "name", "country", "admin_level"]
        ordered_cols = [c for c in priority_cols if c in columns]
        ordered_cols += sorted(c for c in columns if c not in priority_cols)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=ordered_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
        csv_content = output.getvalue()

        return Response(
            content=csv_content.encode("utf-8"),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=export_{session_id[:8]}.csv"},
        )
    except Exception as e:
        logger.error(f"Error exporting cache: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/orders/stats")
async def get_queue_stats_endpoint():
    """Get queue statistics for monitoring/debugging."""
    try:
        return msgpack_response(order_queue.stats())
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        return msgpack_error(str(e), 500)
