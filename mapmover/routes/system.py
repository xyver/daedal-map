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
from mapmover import ACCOUNT_URL, CacheSignature, logger, session_manager
from mapmover.order_queue import order_queue
from mapmover.routes.disasters.helpers import msgpack_error, msgpack_response
from mapmover.security import get_client_ip, rate_limiter
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


@router.post("/api/feedback")
async def submit_feedback(request: Request):
    """Accept anonymous feedback and write it to the Supabase feedback table.
    Accepts both msgpack (map app) and JSON (the .com site).
    """
    client_ip = get_client_ip(request)
    allowed, retry_after = rate_limiter.check(f"feedback:ip:{client_ip}", limit=8, window_seconds=600)
    if not allowed:
        response = msgpack_response({"error": "Too many feedback submissions", "retry_after": retry_after}, 429)
        response.headers["Retry-After"] = str(retry_after)
        return response

    try:
        content_type = request.headers.get("content-type", "")
        raw = await request.body()
        if "application/json" in content_type:
            body = json.loads(raw)
        else:
            body = msgpack.unpackb(raw, raw=False)
    except Exception:
        return msgpack_error("Invalid request body", 400)

    message = (body.get("message") or "").strip()
    if not message:
        return msgpack_error("Message is required", 400)
    if len(message) > 2000:
        return msgpack_error("Message too long (max 2000 chars)", 400)

    user_id = body.get("user_id") or None

    # Derive source from Origin header (daedalmap.io vs daedalmap.com vs local)
    origin = request.headers.get("origin", "") or request.headers.get("referer", "")
    if "daedalmap.io" in origin:
        source = "daedalmap.io"
    elif "daedalmap.com" in origin:
        source = "daedalmap.com"
    else:
        source = "local"

    try:
        from supabase_client import get_supabase_client
        sb = get_supabase_client()
        if sb:
            row = {"message": message, "source": source}
            if user_id:
                row["user_id"] = user_id
            sb.client.table("feedback").insert(row).execute()
        else:
            logger.warning("Feedback received but Supabase not configured: %s", message[:80])
    except Exception as exc:
        logger.error("Failed to save feedback: %s", exc)
        return msgpack_error("Could not save feedback right now", 500)

    return msgpack_response({"ok": True})


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


@router.get("/debug/s3")
async def debug_s3():
    """Test DuckDB S3/httpfs connectivity against a known small file in R2."""
    import traceback
    from mapmover.duckdb_helpers import is_s3_mode, _make_connection, path_to_uri
    from mapmover.paths import DATA_ROOT

    if not is_s3_mode():
        return {"s3_mode": False, "error": "Not in S3 mode"}

    # Use a small known file: global/un_sdg/06/all_countries.parquet
    test_path = DATA_ROOT / "global" / "un_sdg" / "06" / "all_countries.parquet"
    uri = path_to_uri(test_path)

    result = {"s3_mode": True, "uri": uri}
    try:
        con = _make_connection()
        rows = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [uri]).fetchone()
        con.close()
        result["row_count"] = rows[0] if rows else 0
        result["ok"] = True
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/geometry")
async def debug_geometry():
    """Test geometry loading and SDG order pipeline."""
    import traceback
    import pandas as pd
    from mapmover.paths import DATA_ROOT, GEOMETRY_DIR
    from mapmover.geometry_handlers import load_global_countries, get_geometry_path

    result = {
        "DATA_ROOT": str(DATA_ROOT),
        "GEOMETRY_DIR": str(GEOMETRY_DIR),
        "geometry_dir_exists": GEOMETRY_DIR.exists(),
    }

    global_csv = GEOMETRY_DIR / "global.csv"
    result["global_csv_path"] = str(global_csv)
    result["global_csv_exists"] = global_csv.exists()

    try:
        geom_path = get_geometry_path()
        result["get_geometry_path"] = str(geom_path) if geom_path else None
    except Exception as e:
        result["get_geometry_path_error"] = str(e)

    try:
        df = load_global_countries()
        if df is None:
            result["load_global_countries"] = None
        else:
            result["load_global_countries_rows"] = len(df)
            result["load_global_countries_cols"] = list(df.columns)
            has_geom = "geometry" in df.columns
            result["has_geometry_col"] = has_geom
            if has_geom:
                non_null = df["geometry"].notna().sum()
                result["non_null_geometry"] = int(non_null)
                sample = df[df["geometry"].notna()]["geometry"].iloc[0][:80] if non_null > 0 else None
                result["geometry_sample"] = sample
    except Exception as e:
        result["load_global_countries_error"] = str(e)
        result["traceback"] = traceback.format_exc()

    return result


def _get_entitled_packs(req: Request):
    """
    Return the set of pack_ids this request is entitled to, or None for full bypass.

    None  -> full bypass: all catalog sources returned, including those without pack_id.
             Applies to: master plan, is_admin=True, or no service key (dev/self-host).
    set() -> anonymous or entitlement lookup failed: geometry_global only.
    {..}  -> authenticated user: their entitled pack_ids from Supabase.

    Plan tiers:
      master      -> None (owner, sees everything including untagged/unreleased sources)
      is_admin    -> None (admin flag on any plan, same full bypass)
      enterprise  -> entitled packs from pack_entitlements
      pro         -> entitled packs from pack_entitlements
      member      -> entitled packs from pack_entitlements
      free        -> entitled packs from pack_entitlements (usually geometry_global only)
      anonymous   -> empty set
    """
    auth_user = get_authenticated_user(req)
    if not auth_user:
        return set()

    service_key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not service_key:
        # Dev / self-host mode: no entitlement enforcement
        return None

    user_id = auth_user.get("id")
    try:
        from supabase_client import SupabaseClient
        supa = SupabaseClient()
        context = supa.get_user_entitlement_context(user_id)
        if context and not context.get("error"):
            # Master plan or admin flag: full bypass, no pack_id filtering at all
            if context.get("plan_id") == "master" or context.get("is_admin"):
                return None
            user_packs = set(context.get("user_packs") or [])
            org_packs = set(context.get("org_packs") or [])
            return user_packs | org_packs
    except Exception as exc:
        logger.warning(f"Entitlement lookup failed for catalog filter: {exc}")

    # Fallback: authenticated but entitlement fetch failed
    return set()


@router.get("/api/catalog/sources")
async def get_catalog_sources(req: Request):
    """
    Return catalog sources filtered to what this request is entitled to see.

    Master / admin / no-service-key: all sources, including those without pack_id.
    Authenticated user: only sources whose pack_id is in their entitled set.
    Anonymous: empty list.

    Response fields per source: source_id, pack_id, source_name, category,
    data_type, scope, topic_tags.  Full catalog metadata is not included to
    keep the response small.
    """
    from mapmover.data_loading import load_catalog

    entitled = _get_entitled_packs(req)
    all_sources = load_catalog().get("sources", [])

    SUMMARY_KEYS = {"source_id", "pack_id", "source_name", "category", "data_type", "scope", "topic_tags"}

    if entitled is None:
        # Master / bypass: return everything
        sources = [{k: s.get(k) for k in SUMMARY_KEYS} for s in all_sources]
    elif not entitled:
        # Anonymous or entitlement lookup failed
        sources = []
    else:
        sources = [
            {k: s.get(k) for k in SUMMARY_KEYS}
            for s in all_sources
            if s.get("pack_id") in entitled
        ]

    return msgpack_response({"sources": sources, "total": len(sources)})


@router.get("/api/catalog/packs")
async def get_catalog_packs_list(req: Request):
    """
    Return the public pack library: all sources with a pack_id assigned.
    No auth required - pack_id assignment is the publish gate.
    Supports ?format=json for the .com packs browsing page.
    """
    from mapmover.data_loading import load_catalog
    from fastapi.responses import JSONResponse

    all_sources = load_catalog().get("sources", [])
    published = [s for s in all_sources if s.get("pack_id")]

    def resolve_pack_temporal(pack_id: str, pack_sources: list[dict], primary: dict) -> dict:
        starts = []
        ends = []
        granularities = []

        for src in pack_sources:
            tc = src.get("temporal_coverage", {}) or {}
            start = tc.get("start")
            end = tc.get("end")
            granularity = tc.get("granularity")
            if start not in (None, "", "unknown"):
                starts.append(start)
            if end not in (None, "", "unknown"):
                ends.append(end)
            if granularity not in (None, "", "unknown"):
                granularities.append(granularity)

        if starts or ends:
            return {
                "start": min(starts) if starts else None,
                "end": max(ends) if ends else None,
                "granularity": granularities[0] if granularities else (primary.get("temporal_coverage", {}) or {}).get("granularity"),
            }

        try:
            from mapmover.disaster_filters import get_disaster_metadata
            disaster_meta = get_disaster_metadata(pack_id)
            if disaster_meta:
                return {
                    "start": disaster_meta.get("data_min_year"),
                    "end": disaster_meta.get("data_max_year"),
                    "granularity": (primary.get("temporal_coverage", {}) or {}).get("granularity") or "yearly",
                }
        except Exception:
            pass

        return {
            "start": None,
            "end": None,
            "granularity": (primary.get("temporal_coverage", {}) or {}).get("granularity"),
        }

    pack_map = {}
    pack_counts = {}
    pack_sources_map = {}
    for s in published:
        pid = s["pack_id"]
        pack_counts[pid] = pack_counts.get(pid, 0) + 1
        pack_sources_map.setdefault(pid, []).append(s)
        if pid not in pack_map or s.get("source_id") == pid:
            pack_map[pid] = s

    packs = []
    for pid, s in pack_map.items():
        ref = s.get("reference", {})
        ref_src = ref.get("source", {})
        tc = resolve_pack_temporal(pid, pack_sources_map.get(pid, [s]), s)
        packs.append({
            "pack_id":        pid,
            "source_name":    ref_src.get("source_name") or s.get("source_name", ""),
            "description":    ref_src.get("description", ""),
            "category":       s.get("category", "other"),
            "data_type":      s.get("data_type", ""),
            "scope":          s.get("scope", ""),
            "topic_tags":     s.get("topic_tags") or [],
            "source_count":   pack_counts[pid],
            "temporal_start": tc.get("start"),
            "temporal_end":   tc.get("end"),
        })

    packs.sort(key=lambda p: (p["category"], p["source_name"].lower()))

    fmt = req.query_params.get("format", "")
    if fmt == "json":
        return JSONResponse({"packs": packs, "total": len(packs)})
    return msgpack_response({"packs": packs, "total": len(packs)})


@router.get("/api/catalog/packs/{pack_id}")
async def get_catalog_pack(pack_id: str, req: Request):
    """
    Return full metadata for a single pack by pack_id.
    Merges all sources sharing that pack_id into one pack profile.
    Published packs (those with a pack_id) are publicly readable without auth.
    Unpublished sources require master/bypass.
    Supports ?format=json for the .com public pack profile pages.
    """
    from mapmover.data_loading import load_catalog
    from fastapi.responses import JSONResponse

    entitled = _get_entitled_packs(req)
    all_sources = load_catalog().get("sources", [])

    pack_sources = [s for s in all_sources if s.get("pack_id") == pack_id]
    if not pack_sources:
        return msgpack_error("Pack not found", 404)

    # Published packs are publicly readable. Unpublished (no pack_id) require bypass.
    # Since we already filtered to pack_id sources, access is always allowed here.
    # (Entitlement gate is enforced at the catalog/sources level, not individual pack pages.)

    # Use the source whose source_id matches pack_id as primary, else first
    primary = next((s for s in pack_sources if s.get("source_id") == pack_id), pack_sources[0])
    ref = primary.get("reference", {})
    ref_source = ref.get("source", {})

    # Aggregate metrics across all sources in the pack
    all_metrics = {}
    for s in pack_sources:
        for k, v in (s.get("reference", {}).get("metrics", {}) or {}).items():
            all_metrics[k] = v

    subsources = []
    for s in pack_sources:
        sref = s.get("reference", {})
        sref_source = sref.get("source", {})
        smetrics = sref.get("metrics", {}) or {}
        stc = s.get("temporal_coverage", {}) or {}
        subsources.append({
            "source_id": s.get("source_id"),
            "source_name": sref_source.get("source_name") or s.get("source_name", ""),
            "description": sref_source.get("description", "") or s.get("description", ""),
            "path": s.get("path", ""),
            "metric_count": len(smetrics),
            "metrics": smetrics,
            "temporal_coverage": {
                "start": stc.get("start"),
                "end": stc.get("end"),
                "granularity": stc.get("granularity"),
            },
            "coverage_description": s.get("coverage_description", ""),
            "geographic_level": s.get("geographic_level"),
            "interaction_mode": s.get("interaction_mode"),
        })

    # Aggregate temporal coverage
    temporal_starts = [s["temporal_coverage"]["start"] for s in pack_sources if s.get("temporal_coverage", {}).get("start") not in (None, "", "unknown")]
    temporal_ends   = [s["temporal_coverage"]["end"]   for s in pack_sources if s.get("temporal_coverage", {}).get("end") not in (None, "", "unknown")]
    temporal = {
        "start": min(temporal_starts) if temporal_starts else None,
        "end":   max(temporal_ends)   if temporal_ends   else None,
        "granularity": primary.get("temporal_coverage", {}).get("granularity"),
    }
    if temporal["start"] is None and temporal["end"] is None:
        try:
            from mapmover.disaster_filters import get_disaster_metadata
            disaster_meta = get_disaster_metadata(pack_id)
            if disaster_meta:
                temporal["start"] = disaster_meta.get("data_min_year")
                temporal["end"] = disaster_meta.get("data_max_year")
                if not temporal.get("granularity") or temporal["granularity"] == "unknown":
                    temporal["granularity"] = "yearly"
        except Exception:
            pass

    pack = {
        "pack_id":            pack_id,
        "source_name":        ref_source.get("source_name") or primary.get("source_name", ""),
        "description":        ref_source.get("description", ""),
        "source_url":         ref_source.get("source_url", ""),
        "license":            ref_source.get("license", ""),
        "category":           primary.get("category", ""),
        "data_type":          primary.get("data_type", ""),
        "scope":              primary.get("scope", ""),
        "topic_tags":         primary.get("topic_tags") or [],
        "keywords":           primary.get("keywords") or [],
        "geographic_level":   primary.get("geographic_level"),
        "coverage_description": primary.get("coverage_description", ""),
        "temporal_coverage":  temporal,
        "metrics":            all_metrics,
        "llm_summary":        primary.get("llm_summary", ""),
        "source_count":       len(pack_sources),
        "source_ids":         [s["source_id"] for s in pack_sources],
        "subsources":         subsources,
    }

    fmt = req.query_params.get("format", "")
    if fmt == "json":
        return JSONResponse({"pack": pack})
    return msgpack_response({"pack": pack})


@router.get("/api/catalog/overlays")
async def get_catalog_overlays(req: Request):
    """Get overlay tree from the catalog, filtered to the user's entitled packs."""
    from mapmover.data_loading import load_catalog

    catalog = load_catalog()
    entitled = _get_entitled_packs(req)

    all_sources = catalog.get("sources", [])

    if entitled is None:
        # No service key - dev/self-host mode, return everything
        filtered_sources = all_sources
    else:
        # Filter to entitled packs; sources with no pack_id are excluded
        # geometry_global is always included for authenticated users
        entitled_with_base = entitled | {"geometry_global"}
        if entitled:
            # Authenticated with entitlements: include entitled packs + geometry_global
            filtered_sources = [
                s for s in all_sources
                if s.get("pack_id") in entitled_with_base
            ]
        else:
            # Anonymous: geometry_global only
            filtered_sources = [
                s for s in all_sources
                if s.get("pack_id") == "geometry_global"
            ]

    return msgpack_response(
        {
            "sources": filtered_sources,
            "overlay_tree": catalog.get("overlay_tree", {}),
            "overlay_count": len(filtered_sources),
        }
    )


@router.post("/api/admin/catalog/refresh")
async def admin_catalog_refresh(req: Request):
    """
    Force an immediate catalog.json refresh from R2.
    Restricted to master plan and is_admin users only.
    """
    from mapmover.data_loading import _refresh_catalog_from_s3, get_catalog_path
    import mapmover.data_loading as _dl

    auth_user = get_authenticated_user(req)
    if not auth_user:
        return msgpack_error("Unauthorized", 401)

    service_key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if service_key:
        try:
            from supabase_client import SupabaseClient
            supa = SupabaseClient()
            context = supa.get_user_entitlement_context(auth_user.get("id"))
            if not context or context.get("error"):
                return msgpack_error("Forbidden", 403)
            if context.get("plan_id") != "master" and not context.get("is_admin"):
                return msgpack_error("Forbidden", 403)
        except Exception as exc:
            logger.warning(f"Admin catalog refresh: entitlement check failed: {exc}")
            return msgpack_error("Entitlement check failed", 500)

    catalog_path = get_catalog_path()
    _refresh_catalog_from_s3(catalog_path)

    # Clear the in-memory cache so the next load_catalog() reads the fresh file
    _dl._catalog_cache = None
    _dl._catalog_cache_time = 0.0

    from mapmover.data_loading import load_catalog
    catalog = load_catalog()
    return msgpack_response({
        "ok": True,
        "source_count": len(catalog.get("sources", [])),
        "message": "Catalog refreshed from R2",
    })


@router.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the frontend HTML shell with cache-busting version stamps on static assets."""
    template_path = BASE_DIR / "templates" / "index.html"
    static_dir = BASE_DIR / "static"

    def _v(rel: str) -> str:
        p = static_dir / rel
        try:
            return str(int(p.stat().st_mtime))
        except OSError:
            return "0"

    html = template_path.read_text(encoding="utf-8")
    html = html.replace('href="/static/styles/map.css"', f'href="/static/styles/map.css?v={_v("styles/map.css")}"')
    html = html.replace('href="/static/styles/chat.css"', f'href="/static/styles/chat.css?v={_v("styles/chat.css")}"')
    return html


@router.get("/settings", response_class=HTMLResponse)
async def serve_settings_page():
    """Serve the standalone settings/account page."""
    from mapmover import SITE_URL
    template_path = BASE_DIR / "templates" / "settings.html"
    html = template_path.read_text(encoding="utf-8")
    html = html.replace("{{site_url}}", SITE_URL)
    return html


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


@router.get("/api/settings")
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
                    "credits_balance": context.get("credits_balance", 0),
                    "account_url": ACCOUNT_URL,
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


@router.post("/api/settings")
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


@router.post("/api/settings/init-folders")
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


@router.get("/debug/process")
async def debug_process():
    """Show process-level memory usage broken down by component."""
    import gc
    import sys
    import tracemalloc

    result = {}

    # RSS from /proc/self/status (Linux only - works on Railway)
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    result["rss_mb"] = round(int(line.split()[1]) / 1024, 1)
                elif line.startswith("VmPeak:"):
                    result["peak_mb"] = round(int(line.split()[1]) / 1024, 1)
                elif line.startswith("VmSize:"):
                    result["vms_mb"] = round(int(line.split()[1]) / 1024, 1)
    except Exception as e:
        result["proc_error"] = str(e)

    # Python object counts by type (top 20 by count)
    gc.collect()
    type_counts = {}
    for obj in gc.get_objects():
        t = type(obj).__name__
        type_counts[t] = type_counts.get(t, 0) + 1
    top_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:20]
    result["top_object_types"] = [{"type": t, "count": c} for t, c in top_types]

    # Top modules by their attribute sizes (approximation of import footprint)
    module_sizes = {}
    for name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        try:
            sz = sys.getsizeof(mod)
            module_sizes[name.split(".")[0]] = module_sizes.get(name.split(".")[0], 0) + sz
        except Exception:
            pass
    top_modules = sorted(module_sizes.items(), key=lambda x: x[1], reverse=True)[:15]
    result["top_modules_kb"] = [{"module": m, "kb": round(s / 1024, 1)} for m, s in top_modules]

    # tracemalloc snapshot - top 10 allocations by file
    if not tracemalloc.is_tracing():
        tracemalloc.start()
        result["tracemalloc"] = "just started - re-hit this endpoint in 30s for useful data"
    else:
        snapshot = tracemalloc.take_snapshot()
        stats = snapshot.statistics("filename")[:10]
        result["tracemalloc_top_mb"] = [
            {"file": str(s.traceback).split("/")[-1], "mb": round(s.size / (1024 * 1024), 2), "count": s.count}
            for s in stats
        ]

    return result


@router.get("/debug/memory")
async def debug_memory():
    """Show what is in the in-memory caches and estimated RAM usage."""
    import time
    from mapmover.duckdb_helpers import _CACHE, _CACHE_LOCK, DEFAULT_CACHE_TTL
    from mapmover.geometry_handlers import _country_parquet_cache, _country_parquet_cache_lock

    now = time.monotonic()

    # Disaster DataFrame cache
    with _CACHE_LOCK:
        cache_snapshot = list(_CACHE.items())

    disaster_entries = []
    for key, (df, expires_at) in cache_snapshot:
        permanent = expires_at == float("inf")
        ttl_remaining = None if permanent else max(0, expires_at - now)
        mem_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        disaster_entries.append({
            "key": key,
            "rows": len(df),
            "cols": len(df.columns),
            "mem_mb": round(mem_mb, 2),
            "permanent": permanent,
            "ttl_remaining_s": None if permanent else round(ttl_remaining),
            "expired": False if permanent else ttl_remaining == 0,
        })
    disaster_entries.sort(key=lambda x: x["mem_mb"], reverse=True)
    disaster_total_mb = sum(e["mem_mb"] for e in disaster_entries)

    # Geometry parquet cache (permanent, no TTL)
    with _country_parquet_cache_lock:
        geom_snapshot = list(_country_parquet_cache.items())

    geom_entries = []
    for key, df in geom_snapshot:
        mem_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        geom_entries.append({
            "key": str(key),
            "rows": len(df),
            "mem_mb": round(mem_mb, 2),
        })
    geom_entries.sort(key=lambda x: x["mem_mb"], reverse=True)
    geom_total_mb = sum(e["mem_mb"] for e in geom_entries)

    return {
        "disaster_cache": {
            "entry_count": len(disaster_entries),
            "total_mb": round(disaster_total_mb, 2),
            "default_ttl_s": DEFAULT_CACHE_TTL,
            "entries": disaster_entries,
        },
        "geometry_cache": {
            "entry_count": len(geom_entries),
            "total_mb": round(geom_total_mb, 2),
            "note": "permanent, no TTL",
            "entries": geom_entries,
        },
        "combined_cache_mb": round(disaster_total_mb + geom_total_mb, 2),
    }


@router.get("/api/orders/stats")
async def get_queue_stats_endpoint():
    """Get queue statistics for monitoring/debugging."""
    try:
        return msgpack_response(order_queue.stats())
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        return msgpack_error(str(e), 500)
