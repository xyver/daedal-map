"""Portable local server for DaedalMap's Geometry and Research MCPs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

BUNDLE_ROOT = Path(__file__).resolve().parent
SOURCE_REPO_ROOT = BUNDLE_ROOT.parents[1]
SOURCE_TOOLS_ROOT = BUNDLE_ROOT.parent
DEFAULT_RUNTIME_ROOT = SOURCE_REPO_ROOT if (SOURCE_REPO_ROOT / "mapmover").is_dir() else BUNDLE_ROOT / "runtime"
DEFAULT_RESEARCH_ROOT = (
    SOURCE_TOOLS_ROOT / "research-mcp"
    if (SOURCE_TOOLS_ROOT / "research-mcp" / "research_bundle.py").is_file()
    else BUNDLE_ROOT / "tools" / "research-mcp"
)
DEFAULT_DATA_ROOT = BUNDLE_ROOT / "data"
SUPPORTED_SERVICES = {"geometry", "research"}
DEFAULT_PORTS = {"geometry": 7010, "research": 7011}


def packaged_services() -> set[str]:
    """Return services selected by the exported bundle, or source defaults."""
    manifest_path = BUNDLE_ROOT / "BUNDLE-MANIFEST.json"
    if not manifest_path.is_file():
        return set(SUPPORTED_SERVICES)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"Invalid bundle manifest: {manifest_path}") from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("services"), list):
        raise ValueError(f"Bundle manifest has no valid services list: {manifest_path}")
    return _services(manifest["services"])


def default_port(services: str | Iterable[str]) -> int:
    selected = _services(services)
    return DEFAULT_PORTS["research"] if selected == {"research"} else DEFAULT_PORTS["geometry"]


def _select_services(value: str | Iterable[str] | None) -> set[str]:
    selected = _services(packaged_services() if value is None else value)
    unavailable = selected - packaged_services()
    if unavailable:
        raise ValueError(f"Requested service is not packaged: {', '.join(sorted(unavailable))}")
    return selected


def _services(value: str | Iterable[str]) -> set[str]:
    raw = value.split(",") if isinstance(value, str) else value
    selected = {str(item).strip().lower() for item in raw if str(item).strip()}
    unknown = selected - SUPPORTED_SERVICES
    if unknown:
        raise ValueError(f"Unknown services: {', '.join(sorted(unknown))}")
    if not selected:
        raise ValueError("At least one service is required")
    return selected


def _resolve_existing_dir(value: str | Path, label: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"{label} does not exist: {path}")
    return path


def _configure_environment(data_root: Path, runtime_root: Path) -> None:
    os.environ["INSTALL_MODE"] = "local"
    os.environ["RUNTIME_MODE"] = "local"
    os.environ["DEPLOYMENT"] = "local"
    os.environ["STORAGE_MODE"] = "local"
    os.environ["DAEDALMAP_GEOMETRY_READ_MODE"] = "local"
    os.environ["DAEDALMAP_DISABLE_RUNTIME_ANALYTICS"] = "1"
    os.environ["DATA_ROOT"] = str(data_root)
    os.environ["DAEDALMAP_PUBLIC_RUNTIME_ROOT"] = str(runtime_root)
    if str(runtime_root) not in sys.path:
        sys.path.insert(0, str(runtime_root))


def create_app(
    data_root: str | Path,
    *,
    services: str | Iterable[str] | None = None,
    runtime_root: str | Path = DEFAULT_RUNTIME_ROOT,
    research_root: str | Path = DEFAULT_RESEARCH_ROOT,
) -> FastAPI:
    """Create a loopback-oriented MCP app over one local DaedalMap data root."""

    selected = _select_services(services)
    data_path = _resolve_existing_dir(data_root, "Data root")
    runtime_path = _resolve_existing_dir(runtime_root, "Public runtime")
    downloaded_backend = None
    if "geometry" in selected and not (data_path / "geometry" / "geometry_catalog.json").is_file():
        geometry_root = SOURCE_TOOLS_ROOT / "geometry-mcp"
        if not (geometry_root / "downloaded_geometry_backend.py").is_file():
            geometry_root = BUNDLE_ROOT / "tools" / "geometry-mcp"
        if str(geometry_root) not in sys.path:
            sys.path.insert(0, str(geometry_root))
        from downloaded_geometry_backend import DownloadedGeometryBackend
        downloaded_backend = DownloadedGeometryBackend(data_path)
    if "geometry" in selected and downloaded_backend is None and not (runtime_path / "mapmover" / "routes" / "mcp.py").is_file():
        raise ValueError(f"Public runtime is incomplete: {runtime_path}")
    if "research" in selected and not (data_path / "catalog.json").is_file():
        raise ValueError(f"Research catalog not found: {data_path / 'catalog.json'}")
    if "geometry" in selected and downloaded_backend is None and not (data_path / "geometry" / "geometry_catalog.json").is_file():
        raise ValueError(
            f"Geometry catalog not found: {data_path / 'geometry' / 'geometry_catalog.json'}"
        )

    _configure_environment(data_path, runtime_path)
    app = FastAPI(
        title="DaedalMap Local MCP Runtime",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.data_root = data_path
    app.state.services = sorted(selected)

    if "geometry" in selected:
        if downloaded_backend is None:
            from mapmover.routes.mcp import mcp_endpoint, mcp_endpoint_info

            async def geometry_info():
                return await mcp_endpoint_info("geography")

            async def geometry_endpoint(request: Request):
                return await mcp_endpoint(request, "geography")
        else:
            from downloaded_geometry_backend import tool_definitions
            async def geometry_info():
                return JSONResponse({"serverInfo": {"name": "daedalmap/downloaded-geometry", "version": "0.1.0"}, "transport": "streamable-http", "scoped": True, "catalog": downloaded_backend.catalog()})

            async def geometry_endpoint(request: Request):
                body = await request.json()
                if not isinstance(body, dict) or body.get("jsonrpc") != "2.0":
                    return JSONResponse({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid JSON-RPC request"}})
                method = body.get("method")
                request_id = body.get("id")
                if method == "notifications/initialized": return JSONResponse({}, status_code=202)
                if method == "initialize": result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": False}}, "serverInfo": {"name": "daedalmap/downloaded-geometry", "version": "0.1.0"}}
                elif method == "tools/list": result = {"tools": tool_definitions()}
                elif method == "tools/call":
                    params = body.get("params") or {}
                    try:
                        if not isinstance(params, dict): raise ValueError("Invalid tool parameters")
                        output = downloaded_backend.call(str(params.get("name")), params.get("arguments", {}))
                        result = {"content": [{"type": "text", "text": json.dumps(output)}], "structuredContent": output, "isError": False}
                    except (ValueError, KeyError, TypeError) as exc:
                        result = {"content": [{"type": "text", "text": str(exc)}], "isError": True}
                elif method == "ping": result = {}
                else: return JSONResponse({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "Method not found"}})
                return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})

        @app.get("/mcp/geometry")
        @app.get("/mcp/geography")
        async def _geometry_info_route():
            return await geometry_info()

        @app.post("/mcp/geometry")
        @app.post("/mcp/geography")
        async def _geometry_endpoint_route(request: Request):
            return await geometry_endpoint(request)

    if "research" in selected:
        research_path = _resolve_existing_dir(research_root, "Research MCP runtime")
        if str(research_path) not in sys.path:
            sys.path.insert(0, str(research_path))
        import research_bundle

        # Bind the Research tools immediately so clients do not need an extra
        # where_is_research_data call before using the selected local root.
        research_bundle.research_mcp_module.set_local_data_source(data_path)

        @app.get("/mcp/research")
        async def research_info(request: Request):
            return await research_bundle.handle_get_info(request)

        @app.post("/mcp/research")
        async def research_endpoint(request: Request):
            return await research_bundle.handle_post_request(request)

    if len(selected) == 1:
        service = next(iter(selected))

        @app.get("/mcp")
        async def single_service_info(request: Request):
            if service == "geometry":
                return await geometry_info()
            return await research_info(request)

        @app.post("/mcp")
        async def single_service_endpoint(request: Request):
            if service == "geometry":
                return await geometry_endpoint(request)
            return await research_endpoint(request)

    @app.get("/")
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(
            {
                "ok": True,
                "server": "daedalmap-local-mcp-runtime",
                "version": "0.1.0",
                "runtime_mode": "local",
                "bind_recommendation": "127.0.0.1",
                "data_root": str(data_path),
                "services": sorted(selected),
                "endpoints": {name: f"/mcp/{name}" for name in sorted(selected)},
            }
        )

    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DaedalMap MCP tools over local data.")
    parser.add_argument("--data-root", default=os.getenv("DATA_ROOT", str(DEFAULT_DATA_ROOT)))
    parser.add_argument("--services", default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--log-level", default="info")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print("WARNING: non-loopback binding exposes an unauthenticated local MCP server.")
    selected = _select_services(args.services)
    app = create_app(args.data_root, services=selected)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port or default_port(selected), log_level=args.log_level)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
