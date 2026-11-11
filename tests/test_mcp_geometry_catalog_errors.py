"""A missing selected geometry catalog must be visible to MCP callers."""
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mapmover.routes.mcp import router
from mapmover.runtime.reference_exchange import geography_detail, geography_directory


def test_missing_selected_catalog_is_typed_error():
    with mock.patch("mapmover.runtime.reference_exchange.load_geometry_catalog", return_value={}):
        assert geography_directory()["error"]["code"] == "geometry_catalog_unavailable"
        assert geography_detail(scope_id="CAN")["error"]["code"] == "geometry_catalog_unavailable"


def test_mcp_catalog_call_reports_missing_selected_catalog():
    app = FastAPI()
    app.include_router(router)
    with mock.patch("mapmover.runtime.reference_exchange.load_geometry_catalog", return_value={}), \
         mock.patch("mapmover.routes.mcp._live_tool_rate_limit_response", return_value=None):
        response = TestClient(app).post("/mcp/geography", json={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "get_catalog", "arguments": {"catalog": "geometry"}},
        })
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["isError"] is True
    assert result["structuredContent"]["error"]["code"] == "geometry_catalog_unavailable"
