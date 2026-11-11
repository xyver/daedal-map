"""Portable local server for DaedalMap's Geometry and Research MCPs."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

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


def _configure_environment(data_root: Path, runtime_root: Path, *,
                           cloud_source: dict[str, str] | None = None,
                           cache_dir: Path | None = None) -> None:
    os.environ["INSTALL_MODE"] = "local"
    os.environ["RUNTIME_MODE"] = "local"
    os.environ["DEPLOYMENT"] = "local"
    os.environ["STORAGE_MODE"] = "local"
    os.environ["DAEDALMAP_GEOMETRY_READ_MODE"] = "local"
    os.environ["DAEDALMAP_DISABLE_RUNTIME_ANALYTICS"] = "1"
    os.environ["DATA_ROOT"] = str(data_root)
    os.environ["DAEDALMAP_PUBLIC_RUNTIME_ROOT"] = str(runtime_root)
    if cloud_source is not None:
        os.environ["S3_BUCKET"] = cloud_source["bucket"]
        os.environ["S3_PREFIX"] = cloud_source["prefix"]
        os.environ["AWS_DEFAULT_REGION"] = cloud_source.get("region") or "auto"
        os.environ["PUBLISHED_ARTIFACT_CACHE_DIR"] = str(cache_dir / "geometry")
        if cloud_source.get("endpoint_url"):
            os.environ["S3_ENDPOINT_URL"] = cloud_source["endpoint_url"]
    if str(runtime_root) not in sys.path:
        sys.path.insert(0, str(runtime_root))
    from mapmover.runtime_config import set_local_data_plane_mode
    set_local_data_plane_mode("cloud" if cloud_source is not None else "local")
    from mapmover.runtime.geometry_catalog import clear_geometry_catalog_cache
    clear_geometry_catalog_cache()


def create_app(
    data_root: str | Path | None = None,
    *,
    services: str | Iterable[str] | None = None,
    runtime_root: str | Path = DEFAULT_RUNTIME_ROOT,
    research_root: str | Path = DEFAULT_RESEARCH_ROOT,
    cloud_source: dict[str, str] | None = None,
    cache_dir: str | Path | None = None,
) -> FastAPI:
    """Create a loopback MCP app over one selected local or cloud folder."""

    selected = _select_services(services)
    runtime_path = _resolve_existing_dir(runtime_root, "Public runtime")
    if str(runtime_path) not in sys.path:
        sys.path.insert(0, str(runtime_path))
    from mcp_source_root_shared import MCPSourceRoot
    if cloud_source is None:
        source = MCPSourceRoot.local(data_root or DEFAULT_DATA_ROOT)
        data_path = source.root
    else:
        cloud_source = dict(cloud_source)
        cloud_source["region"] = _cloud_region(cloud_source.get("region"), cloud_source.get("endpoint_url", ""))
        cache_path = Path(cache_dir or Path(tempfile.gettempdir()) / "daedalmap-mcp-cache").expanduser().resolve()
        source = MCPSourceRoot.cloud(bucket=cloud_source.get("bucket", ""),
            prefix=cloud_source.get("prefix", ""), endpoint_url=cloud_source.get("endpoint_url", ""),
            region=cloud_source["region"], cache_dir=cache_path / "research")
        cache_path.mkdir(parents=True, exist_ok=True)
        data_path = cache_path
    downloaded_backend = None
    if "geometry" in selected and cloud_source is None and not (data_path / "geometry" / "geometry_catalog.json").is_file():
        geometry_root = SOURCE_TOOLS_ROOT / "geometry-mcp"
        if not (geometry_root / "downloaded_geometry_backend.py").is_file():
            geometry_root = BUNDLE_ROOT / "tools" / "geometry-mcp"
        if str(geometry_root) not in sys.path:
            sys.path.insert(0, str(geometry_root))
        from downloaded_geometry_backend import DownloadedGeometryBackend
        downloaded_backend = DownloadedGeometryBackend(data_path)
    if "geometry" in selected and downloaded_backend is None and not (runtime_path / "mapmover" / "routes" / "mcp.py").is_file():
        raise ValueError(f"Public runtime is incomplete: {runtime_path}")
    if "research" in selected:
        try:
            source.read_json("catalog.json")
        except Exception as exc:
            raise ValueError(f"Research catalog unavailable in selected source: {source.location()}") from exc
    if "geometry" in selected and cloud_source is None and downloaded_backend is None and not (data_path / "geometry" / "geometry_catalog.json").is_file():
        raise ValueError(
            f"Geometry catalog not found: {data_path / 'geometry' / 'geometry_catalog.json'}"
        )

    _configure_environment(data_path, runtime_path, cloud_source=cloud_source,
                           cache_dir=data_path if cloud_source is not None else None)
    app = FastAPI(
        title="DaedalMap Local MCP Runtime",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.data_root = data_path
    app.state.source = source
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
                        result = {"content": [{"type": "text", "text": json.dumps(output)}], "structuredContent": output,
                                  "isError": bool(output.get("ok") is False or output.get("error"))}
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

        # Bind Research to the same source folder as Geometry at startup.
        research_bundle.research_mcp_module.set_downloaded_data_source(source)

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
                "data_source": source.kind,
                "source_folder": source.location(),
                **({"cloud_bucket": cloud_source["bucket"], "cloud_prefix": cloud_source["prefix"]}
                   if cloud_source is not None else {}),
                "bind_recommendation": "127.0.0.1",
                "cache_dir": str(data_path) if source.kind == "cloud" else None,
                "services": sorted(selected),
                "endpoints": {name: f"/mcp/{name}" for name in sorted(selected)},
            }
        )

    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DaedalMap MCP tools over local data.")
    parser.add_argument("--source", default=None,
                        help="One local folder or s3://bucket/prefix containing catalogs and data.")
    parser.add_argument("--data-root", default=None,
                        help="Legacy local folder option; prefer --source.")
    parser.add_argument("--services", default=None)
    parser.add_argument("--cache-dir", default=None,
                        help="Disposable local cache for cloud reads; never a data source.")
    parser.add_argument("--cloud-bucket", default=None,
                        help="Legacy S3-compatible bucket selector; requires an explicit prefix and local credentials.")
    parser.add_argument("--cloud-prefix", default=None,
                        help="Selected cloud folder, for example staging or published. Never a tool-call argument.")
    parser.add_argument("--cloud-endpoint", default=None,
                        help="S3-compatible endpoint URL for a user-owned cloud source.")
    parser.add_argument("--cloud-region", default=None,
                        help="Signing region for cloud storage; R2 uses auto, AWS S3 uses its bucket region.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--log-level", default="info")
    return parser.parse_args(argv)


def _cloud_region(explicit: str | None, endpoint: str) -> str:
    if explicit:
        return explicit
    if os.getenv("AWS_DEFAULT_REGION"):
        return os.environ["AWS_DEFAULT_REGION"]
    return "auto" if ".r2.cloudflarestorage.com" in endpoint else "us-east-1"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print("WARNING: non-loopback binding exposes an unauthenticated local MCP server.")
    selected = _select_services(args.services)
    source_arg = str(args.source or "").strip()
    cloud_requested = any((args.cloud_bucket, args.cloud_prefix, args.cloud_endpoint, args.cloud_region))
    if source_arg and (args.data_root or args.cloud_bucket or args.cloud_prefix):
        raise ValueError("Choose one source folder with --source; do not combine source selectors")
    if source_arg.lower().startswith("s3://"):
        parsed = urlsplit(source_arg)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or not parsed.hostname or not parsed.path.strip("/"):
            raise ValueError("Cloud source must be s3://bucket/folder without credentials or query")
        cloud_source = {"bucket": parsed.hostname, "prefix": parsed.path.strip("/"),
                        "endpoint_url": args.cloud_endpoint or os.getenv("S3_ENDPOINT_URL", ""),
                        "region": _cloud_region(args.cloud_region,
                            args.cloud_endpoint or os.getenv("S3_ENDPOINT_URL", ""))}
        local_source = None
    elif source_arg:
        if cloud_requested:
            raise ValueError("Cloud connection options require an s3:// source folder")
        cloud_source = None
        local_source = source_arg
    else:
        cloud_source = ({"bucket": args.cloud_bucket or os.getenv("S3_BUCKET", ""),
                         "prefix": args.cloud_prefix or "",
                         "endpoint_url": args.cloud_endpoint or os.getenv("S3_ENDPOINT_URL", ""),
                         "region": _cloud_region(args.cloud_region,
                             args.cloud_endpoint or os.getenv("S3_ENDPOINT_URL", ""))}
                        if cloud_requested else None)
        local_source = args.data_root or os.getenv("DATA_ROOT", str(DEFAULT_DATA_ROOT))
    app = create_app(local_source, services=selected, cloud_source=cloud_source,
                     cache_dir=args.cache_dir)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port or default_port(selected), log_level=args.log_level)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
