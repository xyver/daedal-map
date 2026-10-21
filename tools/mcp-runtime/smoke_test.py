"""Offline post-install smoke test for the extracted MCP runtime bundle."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

PROTOCOL_VERSION = "2025-06-18"
BUNDLE_ROOT = Path(__file__).resolve().parent
if str(BUNDLE_ROOT) not in sys.path:
    sys.path.insert(0, str(BUNDLE_ROOT))

from server import create_app, packaged_services  # noqa: E402


def _verify_bundle_manifest() -> int:
    manifest_path = BUNDLE_ROOT / "BUNDLE-MANIFEST.json"
    if not manifest_path.is_file():
        return 0
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files")
    if not isinstance(files, list) or manifest.get("file_count") != len(files):
        raise RuntimeError("Bundle manifest has an invalid file count")
    for record in files:
        path = BUNDLE_ROOT / record["path"]
        if not path.is_file():
            raise RuntimeError(f"Bundled file is missing: {record['path']}")
        content = path.read_bytes()
        if len(content) != record["size_bytes"]:
            raise RuntimeError(f"Bundled file has the wrong size: {record['path']}")
        if hashlib.sha256(content).hexdigest() != record["sha256"]:
            raise RuntimeError(f"Bundled file has the wrong digest: {record['path']}")
    return len(files)


def _post(client: TestClient, endpoint: str, method: str, params: dict | None = None) -> dict:
    response = client.post(
        endpoint,
        headers={
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        },
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
    )
    if response.status_code != 200:
        raise RuntimeError(f"{endpoint} {method} returned {response.status_code}: {response.text}")
    payload = response.json()
    rendered = json.dumps(payload)
    if "ModuleNotFoundError" in rendered or "ImportError" in rendered:
        raise RuntimeError(f"{endpoint} {method} hit an incomplete bundle: {rendered}")
    return payload


def _call(client: TestClient, service: str, name: str, arguments: dict) -> dict:
    return _post(
        client,
        f"/mcp/{service}",
        "tools/call",
        {"name": name, "arguments": arguments},
    )


def _fixture(root: Path) -> None:
    geometry = root / "geometry"
    geometry.mkdir(parents=True)
    (root / "catalog.json").write_text('{"sources": []}\n', encoding="utf-8")
    (geometry / "geometry_catalog.json").write_text(
        '{"countries": [], "country_profiles": [], "geometry_products": []}\n',
        encoding="utf-8",
    )


def main() -> int:
    verified_files = _verify_bundle_manifest()
    geometry_calls = {
        "get_tool_help": {"tool_name": "resolve_point"},
        "get_catalog": {},
        "get_pack": {"pack_id": "geography"},
        "resolve_point": {"lat": 0.0, "lon": 0.0},
        "resolve_deep_point": {"lat": 0.0, "lon": 0.0, "admin_1_loc_id": "USA-CA"},
        "get_loc_id_info": {"loc_id": "USA"},
        "identify_dataset_geography": {
            "columns": [{"name": "postal_code", "values": ["00601"]}],
        },
        "identify_reference_system": {"identifier": "00601"},
        "convert_reference": {"from_system": "zip", "value": "00601", "to_system": "loc_id"},
        "compare_geographies": {"left_loc_id": "USA", "right_loc_id": "CAN"},
        "get_geography": {"family_id": "admin"},
        "get_shape": {"loc_id": "USA", "include_polygon": False},
    }
    research_calls = {
        "get_tool_help": {"tool_name": "query_research_source_data"},
        "how_research_mcp_works": {},
        "where_is_research_data": {},
        "resolve_research_geography": {"locations": ["United States"]},
        "search_research_sources": {"query": "earthquakes"},
        "resolve_research_corpus_intent": {"query": "earthquakes"},
        "get_research_pack": {"pack_id": "earthquakes"},
        "ask_research_sources": {"source_ids": ["missing"], "question": "test"},
        "query_research_source_data": {
            "source_ids": ["missing"],
            "query": {"source_id": "missing", "metrics": []},
        },
    }

    with tempfile.TemporaryDirectory(prefix="daedalmap-mcp-smoke-") as temp_name:
        data_root = Path(temp_name) / "data"
        _fixture(data_root)
        available = packaged_services()
        modes = {}
        dispatched_calls = {service: 0 for service in available}
        modes_to_test = [service for service in ("geometry", "research") if service in available]
        if len(modes_to_test) == 2:
            modes_to_test.append("geometry,research")
        for services in modes_to_test:
            client = TestClient(create_app(data_root, services=services))
            if client.get("/health").status_code != 200:
                raise RuntimeError(f"Health check failed for services={services}")
            mode = {}
            for service in services.split(","):
                endpoint = f"/mcp/{service}"
                _post(client, endpoint, "initialize", {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "bundle-smoke", "version": "1.0"},
                })
                listed = _post(client, endpoint, "tools/list")["result"]["tools"]
                mode[service] = len(listed)
                if service == "research":
                    resources = _post(client, endpoint, "resources/list")["result"]["resources"]
                    if len(resources) != 3:
                        raise RuntimeError(f"Research resource mismatch: {len(resources)}")
                    for resource in resources:
                        contents = _post(client, endpoint, "resources/read", {"uri": resource["uri"]})["result"]["contents"]
                        if not contents or not contents[0].get("text"):
                            raise RuntimeError(f"Empty Research resource: {resource['uri']}")
                names = {tool["name"] for tool in listed}
                expected = set(geometry_calls if service == "geometry" else research_calls)
                if names != expected:
                    missing = sorted(expected - names)
                    unexpected = sorted(names - expected)
                    raise RuntimeError(
                        f"{service} tool mismatch: missing={missing}, unexpected={unexpected}"
                    )
                calls = geometry_calls if service == "geometry" else research_calls
                for name, arguments in calls.items():
                    _call(client, service, name, arguments)
                    dispatched_calls[service] += 1
            modes[services] = mode

    print(json.dumps({
        "status": "PASS",
        "verified_files": verified_files,
        "modes": modes,
        "dispatched_calls": dispatched_calls,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
