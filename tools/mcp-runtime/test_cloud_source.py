"""A local Geometry MCP can select one explicit cloud folder at startup."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient
import pytest


spec = importlib.util.spec_from_file_location("downloaded_mcp_server_cloud_test", Path(__file__).with_name("server.py"))
server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server)


def test_cloud_source_reads_selected_prefix_without_local_catalog(tmp_path, monkeypatch):
    from mapmover.runtime_config import get_data_plane_mode, set_local_data_plane_mode
    from mapmover.runtime.geometry_catalog import clear_geometry_catalog_cache
    from mapmover.runtime.published_artifacts import artifact_ref

    cloud_source = {"bucket": "test-bucket", "prefix": "staging", "endpoint_url": "https://example.invalid"}
    for name in ("S3_BUCKET", "S3_PREFIX", "S3_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    try:
        app = server.create_app(services="geometry", cloud_source=cloud_source, cache_dir=tmp_path)
        assert get_data_plane_mode() == "cloud"
        assert artifact_ref("geometry/geometry_catalog.json", lane="active").key == "staging/geometry/geometry_catalog.json"
        assert app.state.data_root == tmp_path.resolve()
        assert app.state.source.location() == "s3://test-bucket/staging/"
        catalog = {"schema_version": "1.3.0",
                   "geometry_family_definitions": [{"family_id": "administrative", "label": "Administrative"}],
                   "country_family_coverage": [{"country_code": "CAN", "label": "Canada",
                       "families": [{"family_id": "administrative", "available": True}]}],
                   "country_profiles": [{"country_code": "CAN", "release_status": "published"}],
                   "family_edition_index": []}
        with mock.patch("mapmover.runtime.geometry_catalog.read_artifact_json", return_value=catalog):
            clear_geometry_catalog_cache()
            client = TestClient(app, base_url="http://127.0.0.1")
            response = client.post("/mcp/geometry", json={"jsonrpc": "2.0", "id": 1,
                "method": "tools/call", "params": {"name": "get_catalog", "arguments": {"catalog": "geometry"}}})
            assert response.status_code == 200
            payload = response.json()["result"]["structuredContent"]
            assert [row["scope_id"] for row in payload["scopes"]] == ["CAN"]
    finally:
        clear_geometry_catalog_cache()
        set_local_data_plane_mode("local")


def test_cloud_source_requires_explicit_selection(tmp_path):
    with pytest.raises(ValueError, match="explicit bucket and prefix"):
        server.create_app(tmp_path, services="geometry", cloud_source={"bucket": "", "prefix": "staging"})
    with mock.patch("mcp_source_root_shared.MCPSourceRoot.read_json", return_value={"sources": []}):
        app = server.create_app(services="geometry,research", cache_dir=tmp_path,
                                cloud_source={"bucket": "test", "prefix": "staging"})
        assert app.state.services == ["geometry", "research"]
        client = TestClient(app)
        result = client.post("/mcp/research", json={"jsonrpc": "2.0", "id": 2,
            "method": "tools/call", "params": {"name": "where_is_research_data", "arguments": {}}})
        assert result.json()["result"]["structuredContent"]["active_data_source"] == "cloud_folder"


def test_cli_selects_one_source_folder():
    with mock.patch.object(server, "create_app", return_value=object()) as create, \
         mock.patch("uvicorn.run"):
        assert server.main(["--services", "research", "--source", "s3://bucket/staging",
                            "--cloud-endpoint", "https://example.invalid", "--cloud-region", "auto"]) == 0
    assert create.call_args.args[0] is None
    assert create.call_args.kwargs["cloud_source"] == {
        "bucket": "bucket", "prefix": "staging", "endpoint_url": "https://example.invalid", "region": "auto"}
    with pytest.raises(ValueError, match="one source folder"):
        server.main(["--source", "s3://bucket/staging", "--data-root", "C:\\data"])
