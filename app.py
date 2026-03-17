"""
County Map API - FastAPI entry point.

This file is intentionally thin:
- app setup
- middleware/static mounting
- router registration
- startup initialization
"""

import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()  # Must run before any mapmover imports so env vars are set when paths.py executes

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from mapmover import initialize_catalog, load_conversions, logger
from mapmover.security import get_allowed_origins, is_https_request
from mapmover.order_executor import execute_order
from mapmover.order_queue import processor as order_processor
from mapmover.routes.chat import router as chat_router
from mapmover.routes.disasters.drought import router as drought_router
from mapmover.routes.disasters.earthquakes import router as earthquakes_router
from mapmover.routes.disasters.floods import router as floods_router
from mapmover.routes.disasters.hurricanes import router as hurricanes_router
from mapmover.routes.disasters.landslides import router as landslides_router
from mapmover.routes.disasters.related import router as related_events_router
from mapmover.routes.disasters.tornadoes import router as tornadoes_router
from mapmover.routes.disasters.tsunamis import router as tsunamis_router
from mapmover.routes.disasters.volcanoes import router as volcanoes_router
from mapmover.routes.disasters.wildfires import router as wildfires_router
from mapmover.routes.geometry import router as geometry_router
from mapmover.routes.system import router as system_router
from mapmover.routes.weather import router as weather_router


if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent
SECURITY_TXT_PATH = BASE_DIR / "static" / "security.txt"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize data catalog, conversions, and order processor on startup."""
    import asyncio
    import threading

    logger.info("Starting county-map API...")
    load_conversions()
    initialize_catalog()

    async def async_execute_order(items, hints):
        loop = asyncio.get_event_loop()
        order = {"items": items, "summary": hints.get("summary", "")}
        return await loop.run_in_executor(None, execute_order, order)

    order_processor.set_executor(async_execute_order)
    await order_processor.start()
    logger.info("Startup complete - data catalog and order processor initialized")

    # Fire pre-warmers in background threads so startup is not blocked.
    # In S3 mode this populates DuckDB httpfs metadata cache, our in-memory
    # DataFrame cache, and the geometry cache so cold R2 fetches do not hit
    # the first user requests.
    try:
        from mapmover.duckdb_helpers import is_s3_mode, prewarm_disaster_sources
        from mapmover.geometry_handlers import prewarm_geometry
        from mapmover.paths import GLOBAL_DIR
        if is_s3_mode():
            t_disaster = threading.Thread(
                target=prewarm_disaster_sources,
                args=(GLOBAL_DIR,),
                daemon=True,
                name="prewarm-disasters",
            )
            t_disaster.start()

            t_geom = threading.Thread(
                target=prewarm_geometry,
                daemon=True,
                name="prewarm-geometry",
            )
            t_geom.start()

            logger.info("Pre-warmers started: disasters + geometry")
    except Exception as exc:
        logger.warning("Pre-warmer failed to start: %s", exc)

    yield


app = FastAPI(
    title="County Map API",
    description="Geographic data exploration API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "Origin", "Referer", "X-Requested-With"],
)


@app.middleware("http")
async def static_no_cache(request: Request, call_next):
    """Force revalidation on static JS and CSS so deploys are immediately visible."""
    max_body_bytes = int(os.getenv("MAX_REQUEST_BODY_BYTES", "1048576"))
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_body_bytes:
                return JSONResponse({"error": "Request body too large"}, status_code=413)
        except ValueError:
            pass

    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/") and (path.endswith(".js") or path.endswith(".css")):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    if is_https_request(request):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /\n\n"
        "User-agent: GPTBot\n"
        "Allow: /\n\n"
        "User-agent: ClaudeBot\n"
        "Allow: /\n\n"
        "User-agent: GoogleBot\n"
        "Allow: /\n"
    )
    return PlainTextResponse(content)


@app.get("/llms.txt", include_in_schema=False)
async def llms_txt():
    content = (
        "# DaedalMap App\n\n"
        "DaedalMap is an open geographic query engine. This is the hosted app at daedalmap.io.\n\n"
        "## What it does\n"
        "- Accepts plain language questions about places, regions, and geographic events\n"
        "- Returns answers as interactive maps without requiring GIS expertise\n"
        "- Covers disasters, demographics, economics, climate, and risk data in one interface\n"
        "- Supports guest use without login and logged-in workspace persistence\n\n"
        "## Key links\n"
        "- App: https://daedalmap.io\n"
        "- Website and docs: https://daedalmap.com\n"
        "- Source coverage: https://daedalmap.com/docs/source-map\n"
        "- Data packs: https://daedalmap.com/packs\n"
        "- GitHub (open runtime): https://github.com/xyver/county-map\n\n"
        "## Crawlers and bots\n"
        "This site explicitly welcomes all crawlers, AI training scrapers, and indexing bots.\n"
    )
    return PlainTextResponse(content)


@app.get("/security.txt", include_in_schema=False)
async def security_txt():
    return FileResponse(SECURITY_TXT_PATH, media_type="text/plain; charset=utf-8")


@app.get("/.well-known/security.txt", include_in_schema=False)
async def well_known_security_txt():
    return FileResponse(SECURITY_TXT_PATH, media_type="text/plain; charset=utf-8")

app.include_router(system_router)
app.include_router(geometry_router)
app.include_router(earthquakes_router)
app.include_router(related_events_router)
app.include_router(volcanoes_router)
app.include_router(landslides_router)
app.include_router(tsunamis_router)
app.include_router(hurricanes_router)
app.include_router(tornadoes_router)
app.include_router(floods_router)
app.include_router(drought_router)
app.include_router(wildfires_router)
app.include_router(weather_router)
app.include_router(chat_router)



if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=7000)
