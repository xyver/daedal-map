"""Chat API router endpoints."""

import hashlib
import json
import os
import traceback

import msgpack
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

from mapmover.auth_context import build_session_cache_key, get_authenticated_user
from mapmover import logger, session_manager
from mapmover.order_executor import execute_order
from mapmover.order_taker import interpret_request
from mapmover.postprocessor import get_display_items, postprocess_order
from mapmover.preprocessor import preprocess_query
from mapmover.routes.disasters.helpers import msgpack_error, msgpack_response
from mapmover.security import get_client_ip, rate_limiter
from mapmover import ACCOUNT_URL


# ---------------------------------------------------------------------------
# Credit helpers - direct Supabase RPC, no billing module in the public repo.
# Fail-open: if Supabase is unavailable the call proceeds and is logged.
# Credit cost constants must stay in sync with billing.py in county-map-private.
# ---------------------------------------------------------------------------

_ACCOUNT_URL = ACCOUNT_URL

_CREDIT_COSTS = {
    "chat_turn": 1,   # plain chat response, no tool calls
    "tool_loop": 5,   # LLM used tools (order, navigate, multi-step)
}


def _credits_client():
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


def _get_credit_balance(user_id: str) -> int:
    """Return credit balance. Returns -1 if Supabase unavailable (fail-open)."""
    client = _credits_client()
    if not client:
        return -1
    try:
        result = (
            client.table("profiles")
            .select("credits_balance")
            .eq("id", user_id)
            .single()
            .execute()
        )
        return result.data.get("credits_balance", 0) if result.data else 0
    except Exception as e:
        logger.warning(f"[credits] balance check error for {user_id}: {e}")
        return -1


def _deduct_credits(user_id: str, operation: str) -> dict:
    """
    Atomically deduct credits via Postgres function.
    The Postgres function locks the row to prevent race conditions.
    Fails open if Supabase is unavailable - logs warning but allows the call.
    """
    client = _credits_client()
    if not client:
        return {"success": True, "warning": "billing_not_configured"}
    cost = _CREDIT_COSTS.get(operation, 1)
    try:
        result = client.rpc("deduct_credits", {
            "p_user_id":        user_id,
            "p_amount":         cost,
            "p_operation_type": operation,
            "p_notes":          None,
        }).execute()
        return result.data if result.data else {"success": False, "error": "rpc_no_response"}
    except Exception as e:
        logger.warning(f"[credits] deduction error for {user_id}: {e}")
        return {"success": True, "warning": str(e)}


router = APIRouter()


def _rate_limited_message(message: str, retry_after: int) -> Response:
    response = msgpack_response({"error": message, "retry_after": retry_after}, status_code=429)
    response.headers["Retry-After"] = str(retry_after)
    return response


async def decode_request_body(request: Request) -> dict:
    """Decode MessagePack request body."""
    body_bytes = await request.body()
    return msgpack.unpackb(body_bytes, raw=False)


@router.post("/chat")
async def chat_endpoint(req: Request):
    """Chat endpoint - Order Taker model."""
    try:
        body = await decode_request_body(req)

        frontend_session_id = body.get("sessionId", "anonymous")
        auth_user = get_authenticated_user(req)
        client_ip = get_client_ip(req)
        user_id = auth_user.get("id") if auth_user else None
        if user_id:
            allowed, retry_after = rate_limiter.check(f"chat:user:{user_id}", limit=60, window_seconds=60)
            if not allowed:
                return _rate_limited_message("Too many chat requests. Please slow down and try again shortly.", retry_after)
        else:
            allowed, retry_after = rate_limiter.check(f"chat:ip:{client_ip}", limit=20, window_seconds=60)
            if not allowed:
                return _rate_limited_message("Too many anonymous chat requests. Please wait a moment and try again.", retry_after)

        session_id = build_session_cache_key(frontend_session_id, auth_user)
        cache = session_manager.get_or_create(session_id)

        if body.get("confirmed_order"):
            try:
                confirmed_order = body["confirmed_order"]
                order_str = json.dumps(confirmed_order, sort_keys=True)
                request_key = hashlib.md5(order_str.encode()).hexdigest()[:16]
                result = execute_order(confirmed_order)
                if result.get("type") == "error":
                    return msgpack_response({"type": "error", "message": result.get("message", "Order execution failed.")}, status_code=400)

                if result.get("action") == "remove":
                    logger.info(f"Removal order executed: {result.get('count')} items from {result.get('source_id')}")
                    return msgpack_response({"type": "order_response", **result})
                if result.get("type") == "mixed_order":
                    logger.info(f"Mixed order executed: added {result.get('add_count', 0)}, removed {result.get('remove_count', 0)}")
                    return msgpack_response(result)

                force_refetch = body.get("force", False)
                if force_refetch:
                    logger.info("Force refetch requested - clearing session cache for this data")
                    cache.clear()

                is_events = result.get("type") == "events"
                is_geometry = result.get("data_type") == "geometry"
                event_type_to_overlay = {
                    "earthquake": "earthquakes",
                    "volcano": "volcanoes",
                    "tsunami": "tsunamis",
                    "hurricane": "hurricanes",
                    "wildfire": "wildfires",
                    "tornado": "tornadoes",
                    "flood": "floods",
                    "drought": "drought",
                    "landslide": "landslides",
                }
                event_type = result.get("event_type", "")
                source_id = event_type_to_overlay.get(event_type, event_type) if is_events else result.get("metric_key", "data")
                geojson = result["geojson"]
                features = geojson.get("features", [])
                original_count = len(features)

                if is_events:
                    new_features = cache.filter_events(features)
                    delta_count = len(new_features)
                    filtered_geojson = {"type": "FeatureCollection", "features": new_features}
                    filtered_year_data = None
                elif is_geometry:
                    new_features = cache.filter_geometry_features(features)
                    delta_count = len(new_features)
                    filtered_geojson = {"type": "FeatureCollection", "features": new_features}
                    filtered_year_data = None
                elif result.get("multi_year") and result.get("year_data"):
                    year_data = result["year_data"]
                    filtered_year_data = cache.filter_year_data(year_data)

                    new_loc_ids = set()
                    for loc_data in filtered_year_data.values():
                        new_loc_ids.update(loc_data.keys())

                    new_features = [f for f in features if (f.get("properties", {}).get("loc_id") or f.get("id")) in new_loc_ids]
                    delta_count = len(new_features)
                    filtered_geojson = {"type": "FeatureCollection", "features": new_features}
                else:
                    new_features = features
                    delta_count = original_count
                    filtered_geojson = geojson
                    filtered_year_data = None

                if delta_count == 0 and original_count > 0:
                    logger.debug(f"Dedup: all {original_count} features already sent, returning already_loaded")
                    return msgpack_response(
                        {
                            "type": "already_loaded",
                            "message": f"This data ({original_count} features) is already loaded on your map.",
                            "summary": result.get("summary", ""),
                        }
                    )

                response = {
                    "type": result.get("type", "data"),
                    "data_type": result.get("data_type"),
                    "source_id": result.get("source_id"),
                    "geojson": filtered_geojson,
                    "summary": result.get("summary", ""),
                    "count": delta_count,
                    "sources": result.get("sources", []),
                }

                if is_events:
                    response["event_type"] = result.get("event_type")
                    response["time_range"] = result.get("time_range")
                if is_geometry:
                    geo_level = result.get("geographic_level") or result.get("overlay_type", "zcta")
                    response["overlay_type"] = geo_level
                    response["geographic_level"] = geo_level
                if result.get("multi_year"):
                    response["multi_year"] = True
                    response["year_range"] = result["year_range"]
                    response["metric_key"] = result.get("metric_key")
                    response["available_metrics"] = result.get("available_metrics", [])
                    response["metric_year_ranges"] = result.get("metric_year_ranges", {})
                    response["year_data"] = filtered_year_data if filtered_year_data else {}

                if is_events and new_features:
                    cache.register_sent_events(new_features, source_id)
                elif is_geometry and new_features:
                    geo_source_id = result.get("source_id") or "geometry_zcta"
                    cache.register_sent_geometry(new_features, geo_source_id)
                elif filtered_year_data:
                    cache.register_sent_year_data(filtered_year_data)

                cache.touch()
                if delta_count < original_count:
                    logger.info(f"Delta sent: {delta_count}/{original_count} features ({original_count - delta_count} deduped)")

                return msgpack_response(response)
            except Exception as e:
                logger.error(f"Order execution error: {e}")
                return msgpack_response({"type": "error", "message": str(e)}, status_code=400)

        query = body.get("query", "")
        chat_history = body.get("chatHistory", [])
        viewport = body.get("viewport")
        resolved_location = body.get("resolved_location")
        active_overlays = body.get("activeOverlays")
        cache_stats = body.get("cacheStats")
        time_state = body.get("timeState")
        saved_order_names = body.get("savedOrderNames", [])
        loaded_data = body.get("loadedData", [])

        if not query:
            return msgpack_error("No query provided", 400)

        logger.debug(f"Chat query: {query[:100]}...")
        hints = preprocess_query(
            query,
            viewport=viewport,
            active_overlays=active_overlays,
            cache_stats=cache_stats,
            saved_order_names=saved_order_names,
            time_state=time_state,
            loaded_data=loaded_data,
        )

        if resolved_location:
            hints["location"] = {
                "matched_term": resolved_location.get("matched_term"),
                "iso3": resolved_location.get("iso3"),
                "country_name": resolved_location.get("country_name"),
                "loc_id": resolved_location.get("loc_id"),
                "is_subregion": resolved_location.get("loc_id") != resolved_location.get("iso3"),
                "source": "disambiguation_selection",
            }
            hints["disambiguation"] = None

        if hints.get("show_borders"):
            previous_options = body.get("previous_disambiguation_options", [])
            loc_ids_to_show = [opt.get("loc_id") for opt in previous_options if opt.get("loc_id")] if previous_options else []
            if loc_ids_to_show:
                from mapmover.data_loading import fetch_geometries_by_loc_ids

                geojson = fetch_geometries_by_loc_ids(loc_ids_to_show)
                return msgpack_response(
                    {
                        "type": "navigate",
                        "message": f"Showing {len(loc_ids_to_show)} locations on the map. Click any location to see data options.",
                        "locations": previous_options if previous_options else [{"loc_id": lid} for lid in loc_ids_to_show],
                        "loc_ids": loc_ids_to_show,
                        "original_query": query,
                        "geojson": geojson,
                    }
                )
            return msgpack_response(
                {
                    "type": "chat",
                    "reply": "I don't have a list of locations to display. Please first ask about specific locations (e.g., 'show me washington county') to get a list.",
                }
            )

        navigation = hints.get("navigation")
        if navigation and navigation.get("is_navigation"):
            locations = navigation.get("locations", [])
            if len(locations) == 1 and locations[0].get("drill_to_level"):
                loc = locations[0]
                loc_id = loc.get("loc_id")
                drill_level = loc.get("drill_to_level")
                name = loc.get("matched_term", loc_id)
                return msgpack_response(
                    {
                        "type": "drilldown",
                        "message": f"Showing {drill_level} of {name}...",
                        "loc_id": loc_id,
                        "name": name,
                        "drill_to_level": drill_level,
                        "original_query": query,
                    }
                )

        # Credit check - authenticated users only. Guests proceed freely.
        # Balance is checked before the LLM call; deduction happens after.
        # Both steps fail-open: Supabase errors do not block the user.
        if user_id:
            balance = _get_credit_balance(user_id)
            if balance != -1 and balance < _CREDIT_COSTS.get("chat_turn", 1):
                return msgpack_response({
                    "type": "chat",
                    "reply": (
                        "You have run out of credits. "
                        "Visit your account page to top up and continue."
                    ),
                    "out_of_credits": True,
                    "account_url": _ACCOUNT_URL,
                })

        result = interpret_request(query, chat_history, hints=hints)

        # Deduct credits based on what the LLM actually did.
        if user_id:
            op = "chat_turn" if result["type"] == "chat" else "tool_loop"
            deduct_result = _deduct_credits(user_id, op)
            if not deduct_result.get("success") and deduct_result.get("error") == "insufficient_credits":
                logger.warning(f"[credits] post-call deduction failed for {user_id}: {deduct_result}")

        if result["type"] == "order":
            result_summary = result.get("summary") or result.get("order", {}).get("summary") or "Data request"
            processed = postprocess_order(result["order"], hints)
            if processed.get("metric_warning") and not body.get("force_metrics"):
                display_items = get_display_items(processed.get("items", []), processed.get("derived_specs", []))
                full_order = {**result["order"], "items": display_items, "derived_specs": processed.get("derived_specs", [])}
                return msgpack_response(
                    {
                        "type": "metric_warning",
                        "message": processed["metric_warning"]["message"],
                        "metric_count": processed["metric_warning"]["count"],
                        "pending_order": full_order,
                        "full_order": processed,
                        "summary": result_summary,
                    }
                )

            display_items = get_display_items(processed.get("items", []), processed.get("derived_specs", []))
            return msgpack_response(
                {
                    "type": "order",
                    "order": {**result["order"], "items": display_items, "derived_specs": processed.get("derived_specs", [])},
                    "full_order": processed,
                    "summary": result_summary,
                    "validation_summary": processed.get("validation_summary"),
                    "all_valid": processed.get("all_valid", True),
                }
            )

        if result["type"] == "navigate":
            locations = result.get("locations", [])
            loc_ids = [loc.get("loc_id") for loc in locations if loc.get("loc_id")]
            geometry_overlay = result.get("geometry_overlay")
            geojson = {"type": "FeatureCollection", "features": []}
            if geometry_overlay:
                from mapmover.order_executor import execute_geometry_overlay

                geojson = execute_geometry_overlay(geometry_overlay, loc_ids)
            return msgpack_response(
                {
                    "type": "navigate",
                    "data_type": "geometry" if geometry_overlay else None,
                    "message": result.get("message", f"Showing {len(locations)} location(s)"),
                    "locations": locations,
                    "loc_ids": loc_ids,
                    "original_query": query,
                    "geojson": geojson,
                    "geometry_overlay": geometry_overlay,
                }
            )

        if result["type"] == "disambiguate":
            return msgpack_response(
                {
                    "type": "disambiguate",
                    "message": result.get("message", "Multiple locations found. Please select one."),
                    "query_term": result.get("query_term", "location"),
                    "original_query": query,
                    "options": result.get("options", []),
                    "geojson": {"type": "FeatureCollection", "features": []},
                }
            )

        if result["type"] == "filter_update":
            return msgpack_response(
                {
                    "type": "filter_update",
                    "overlay": result.get("overlay", ""),
                    "filters": result.get("filters", {}),
                    "message": result.get("message", "Updating filters"),
                }
            )

        if result["type"] == "overlay_toggle":
            return msgpack_response(
                {
                    "type": "overlay_toggle",
                    "overlay": result.get("overlay", ""),
                    "enabled": result.get("enabled", True),
                    "message": result.get("message", "Toggling overlay"),
                }
            )

        if result["type"] == "clarify":
            return msgpack_response(
                {"type": "clarify", "message": result["message"], "geojson": {"type": "FeatureCollection", "features": []}, "needsMoreInfo": True}
            )

        return msgpack_response(
            {
                "type": "chat",
                "message": result.get("message", "I'm not sure how to help with that."),
                "geojson": {"type": "FeatureCollection", "features": []},
                "auth_user": {"id": auth_user.get("id"), "email": auth_user.get("email")} if auth_user else None,
                "needsMoreInfo": False,
            }
        )
    except Exception as e:
        logger.error(f"Chat error: {e}")
        traceback.print_exc()
        return msgpack_response(
            {
                "type": "error",
                "message": "Sorry, I encountered an error. Please try again.",
                "geojson": {"type": "FeatureCollection", "features": []},
                "error": str(e),
            },
            status_code=500,
        )


@router.post("/chat/stream")
async def chat_stream_endpoint(req: Request):
    """Streaming chat endpoint - sends progress updates via SSE."""
    import asyncio
    import time

    t_start = time.time()
    body_bytes = await req.body()
    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        body = msgpack.unpackb(body_bytes, raw=False)
    t_parse = time.time()
    logger.debug(f"[TIMING] Body parse: {(t_parse - t_start) * 1000:.0f}ms")

    async def generate_events():
        try:
            frontend_session_id = body.get("sessionId", "anonymous")
            auth_user = get_authenticated_user(req)
            session_id = build_session_cache_key(frontend_session_id, auth_user)
            session_manager.get_or_create(session_id)

            if body.get("confirmed_order"):
                yield f"data: {json.dumps({'stage': 'fetching', 'message': 'Fetching data...'})}\n\n"
                try:
                    result = execute_order(body["confirmed_order"])
                    response = {
                        "type": "data",
                        "geojson": result["geojson"],
                        "summary": result["summary"],
                        "count": result["count"],
                        "sources": result.get("sources", []),
                    }
                    if result.get("multi_year"):
                        response["multi_year"] = True
                        response["year_data"] = result["year_data"]
                        response["year_range"] = result["year_range"]
                        response["metric_key"] = result.get("metric_key")
                        response["available_metrics"] = result.get("available_metrics", [])
                        response["metric_year_ranges"] = result.get("metric_year_ranges", {})
                    yield f"data: {json.dumps({'stage': 'complete', 'result': response})}\n\n"
                except Exception as e:
                    logger.error(f"Order execution error: {e}")
                    yield f"data: {json.dumps({'stage': 'complete', 'result': {'type': 'error', 'message': str(e)}})}\n\n"
                return

            query = body.get("query", "")
            chat_history = body.get("chatHistory", [])
            viewport = body.get("viewport")
            resolved_location = body.get("resolved_location")
            active_overlays = body.get("activeOverlays")
            cache_stats = body.get("cacheStats")
            time_state = body.get("timeState")
            saved_order_names = body.get("savedOrderNames", [])
            loaded_data = body.get("loadedData", [])

            if not query:
                yield f"data: {json.dumps({'stage': 'complete', 'result': {'type': 'error', 'message': 'No query provided'}})}\n\n"
                return

            t_preprocess_start = time.time()
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': 'Analyzing your request...'})}\n\n"
            await asyncio.sleep(0)

            hints = preprocess_query(
                query,
                viewport=viewport,
                active_overlays=active_overlays,
                cache_stats=cache_stats,
                saved_order_names=saved_order_names,
                time_state=time_state,
                loaded_data=loaded_data,
            )
            t_preprocess_end = time.time()
            logger.info(f"[TIMING] Preprocessing: {(t_preprocess_end - t_preprocess_start) * 1000:.0f}ms")

            if resolved_location:
                hints["location"] = {
                    "matched_term": resolved_location.get("matched_term"),
                    "iso3": resolved_location.get("iso3"),
                    "country_name": resolved_location.get("country_name"),
                    "loc_id": resolved_location.get("loc_id"),
                    "is_subregion": resolved_location.get("loc_id") != resolved_location.get("iso3"),
                    "source": "disambiguation_selection",
                }
                hints["disambiguation"] = None

            if hints.get("show_borders"):
                previous_options = body.get("previous_disambiguation_options", [])
                if previous_options:
                    loc_ids_to_show = [opt.get("loc_id") for opt in previous_options if opt.get("loc_id")]
                    if loc_ids_to_show:
                        from mapmover.data_loading import fetch_geometries_by_loc_ids

                        geojson = fetch_geometries_by_loc_ids(loc_ids_to_show)
                        result = {
                            "type": "navigate",
                            "message": f"Showing {len(loc_ids_to_show)} locations on the map.",
                            "locations": previous_options,
                            "loc_ids": loc_ids_to_show,
                            "original_query": query,
                            "geojson": geojson,
                        }
                        yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                        return
                result = {"type": "chat", "reply": "I don't have a list of locations to display."}
                yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                return

            navigation = hints.get("navigation")
            if navigation and navigation.get("is_navigation"):
                locations = navigation.get("locations", [])
                if len(locations) == 1 and locations[0].get("drill_to_level"):
                    loc = locations[0]
                    result = {
                        "type": "drilldown",
                        "message": f"Showing {loc.get('drill_to_level')} of {loc.get('matched_term', loc.get('loc_id'))}...",
                        "loc_id": loc.get("loc_id"),
                        "name": loc.get("matched_term", loc.get("loc_id")),
                        "drill_to_level": loc.get("drill_to_level"),
                        "original_query": query,
                    }
                    yield f"data: {json.dumps({'stage': 'complete', 'result': result})}\n\n"
                    return

            t_llm_start = time.time()
            yield f"data: {json.dumps({'stage': 'thinking', 'message': 'Understanding your intent...'})}\n\n"
            await asyncio.sleep(0)
            result = interpret_request(query, chat_history, hints=hints)
            t_llm_end = time.time()
            logger.info(f"[TIMING] LLM call: {(t_llm_end - t_llm_start) * 1000:.0f}ms")

            if result["type"] == "order":
                yield f"data: {json.dumps({'stage': 'preparing', 'message': 'Preparing your order...'})}\n\n"
                await asyncio.sleep(0)
                result_summary = result.get("summary") or result.get("order", {}).get("summary") or "Data request"
                processed = postprocess_order(result["order"], hints)
                display_items = get_display_items(processed.get("items", []), processed.get("derived_specs", []))
                final_result = {
                    "type": "order",
                    "order": {**result["order"], "items": display_items, "derived_specs": processed.get("derived_specs", [])},
                    "full_order": processed,
                    "summary": result_summary,
                    "validation_summary": processed.get("validation_summary"),
                    "all_valid": processed.get("all_valid", True),
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            elif result["type"] == "navigate":
                locations = result.get("locations", [])
                loc_ids = [loc.get("loc_id") for loc in locations if loc.get("loc_id")]
                geometry_overlay = result.get("geometry_overlay")
                geojson = {"type": "FeatureCollection", "features": []}
                if geometry_overlay:
                    from mapmover.order_executor import execute_geometry_overlay

                    geojson = execute_geometry_overlay(geometry_overlay, loc_ids)
                final_result = {
                    "type": "navigate",
                    "data_type": "geometry" if geometry_overlay else None,
                    "message": result.get("message", f"Showing {len(locations)} location(s)"),
                    "locations": locations,
                    "loc_ids": loc_ids,
                    "original_query": query,
                    "geojson": geojson,
                    "geometry_overlay": geometry_overlay,
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            elif result["type"] == "disambiguate":
                final_result = {
                    "type": "disambiguate",
                    "message": result.get("message", "Multiple locations found. Please select one."),
                    "query_term": result.get("query_term", "location"),
                    "original_query": query,
                    "options": result.get("options", []),
                    "geojson": {"type": "FeatureCollection", "features": []},
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            elif result["type"] == "filter_update":
                final_result = {
                    "type": "filter_update",
                    "overlay": result.get("overlay", ""),
                    "filters": result.get("filters", {}),
                    "message": result.get("message", "Updating filters"),
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            elif result["type"] == "overlay_toggle":
                final_result = {
                    "type": "overlay_toggle",
                    "overlay": result.get("overlay", ""),
                    "enabled": result.get("enabled", True),
                    "message": result.get("message", "Toggling overlay"),
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            elif result["type"] == "clarify":
                final_result = {"type": "clarify", "message": result["message"], "geojson": {"type": "FeatureCollection", "features": []}, "needsMoreInfo": True}
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
            else:
                final_result = {
                    "type": "chat",
                    "message": result.get("message", "I'm not sure how to help with that."),
                    "geojson": {"type": "FeatureCollection", "features": []},
                    "needsMoreInfo": False,
                }
                yield f"data: {json.dumps({'stage': 'complete', 'result': final_result})}\n\n"
        except Exception as e:
            logger.error(f"Chat stream error: {e}")
            traceback.print_exc()
            error_result = {
                "type": "error",
                "message": "Sorry, I encountered an error. Please try again.",
                "geojson": {"type": "FeatureCollection", "features": []},
                "error": str(e),
            }
            yield f"data: {json.dumps({'stage': 'complete', 'result': error_result})}\n\n"

    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
