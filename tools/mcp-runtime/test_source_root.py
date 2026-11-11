"""A selected folder owns its catalogs and payloads in either storage mode."""

import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from mcp_source_root_shared import MCPSourceRoot


class _Objects:
    def __init__(self, values):
        self.values = values

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.values[Key])}

    def head_object(self, *, Bucket, Key):
        value = self.values[Key]
        return {"ETag": str(hash(value)), "ContentLength": len(value)}

    def download_fileobj(self, bucket, key, output):
        output.write(self.values[key])


def test_local_and_cloud_use_same_relative_catalog_and_data_paths(tmp_path, monkeypatch):
    values = {"staging/catalog.json": json.dumps({"sources": [{"source_id": "x"}]}).encode(),
              "staging/global/x/data.parquet": b"parquet fixture bytes"}
    objects = _Objects(values)
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *_, **__: objects))
    cloud = MCPSourceRoot.cloud(bucket="test", prefix="staging", cache_dir=tmp_path / "cache")
    local = tmp_path / "local"
    (local / "global" / "x").mkdir(parents=True)
    (local / "catalog.json").write_bytes(values["staging/catalog.json"])
    (local / "global" / "x" / "data.parquet").write_bytes(values["staging/global/x/data.parquet"])
    disk = MCPSourceRoot.local(local)

    assert cloud.read_json("catalog.json") == disk.read_json("catalog.json")
    assert cloud.local_file("global/x/data.parquet").read_bytes() == disk.local_file("global/x/data.parquet").read_bytes()
    assert cloud.local_file("global/x/data.parquet").is_relative_to(tmp_path / "cache")
    with pytest.raises((KeyError, FileNotFoundError)):
        cloud.read_bytes("global/x/missing.parquet")
    with pytest.raises(ValueError, match="within the selected folder"):
        cloud.read_bytes("../published/catalog.json")


def test_research_queries_selected_cloud_folder(tmp_path, monkeypatch):
    import duckdb
    import server

    parquet = tmp_path / "fixture.parquet"
    duckdb.sql("SELECT 'USA' AS loc_id, 7.0 AS value").write_parquet(str(parquet))
    values = {
        "staging/catalog.json": json.dumps({"sources": [{"source_id": "sample",
            "path": "global/sample", "pack_id": "sample", "data_type": "metrics"}]}).encode(),
        "staging/global/sample/metadata.json": json.dumps({"source_id": "sample",
            "runtime_primary_file": "data.parquet", "data_type": "metrics",
            "metrics": {"value": {"column": "value", "description": "Value"}},
            "temporal_coverage": {"kind": "static"}}).encode(),
        "staging/global/sample/data.parquet": parquet.read_bytes(),
    }
    objects = _Objects(values)
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *_, **__: objects))
    app = server.create_app(services="research", cache_dir=tmp_path / "cache",
                            cloud_source={"bucket": "test", "prefix": "staging"})
    client = TestClient(app)
    def call(name, arguments):
        response = client.post("/mcp/research", json={"jsonrpc": "2.0", "id": 1,
            "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        return response.json()["result"]["structuredContent"]
    assert call("get_catalog", {"catalog": "data"})["packs"][0]["pack_id"] == "sample"
    result = call("query_research_source_data", {"source_ids": ["sample"],
        "query": {"source_id": "sample", "metrics": ["value"], "filters": {}}})
    assert result["outcome"] == "ok", result.get("error")
    assert result["rows"][0]["value"] == 7.0
    assert call("where_is_research_data", {})["active_data_source"] == "cloud_folder"
