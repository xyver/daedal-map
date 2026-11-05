"""The public MCP viewport contract routes one rectangle to the shared runtime."""
from __future__ import annotations

from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mapmover.routes.mcp import router


def _call(client, arguments):
    return client.post("/mcp/geography", json={
        "jsonrpc": "2.0", "id": "bbox-test", "method": "tools/call",
        "params": {"name": "resolve_bbox", "arguments": arguments},
    }).json()


def test_bbox_mcp_validates_then_dispatches_one_rectangle():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 1},
               {"lat": 1, "lon": 0}, {"lat": 1, "lon": 1}]
    with mock.patch("mapmover.routes.mcp._live_tool_rate_limit_response", return_value=None), \
            mock.patch("mapmover.routes.mcp.run_mcp_blocking", new_callable=mock.AsyncMock) as run:
        run.return_value = {"bbox": {"west": 0, "south": 0, "east": 1, "north": 1},
                            "best_match": None, "matches_by_admin_level": {"0": [], "1": [], "2": []}}
        success = _call(client, {"corners": corners})
        assert success["result"]["structuredContent"]["bbox"]["east"] == 1
        assert run.await_count == 1
        failure = _call(client, {"corners": corners[:2]})
        assert failure["result"]["structuredContent"]["error"]["code"] == "invalid_bbox"
        assert run.await_count == 1
        deep = _call(client, {"corners": corners, "shallow_loc_id": "CAN-NT"})
        assert deep["result"]["structuredContent"]["error"]["code"] == "bbox_contract_violation"
        deep = client.post("/mcp/geography", json={"jsonrpc": "2.0", "id": "bbox-deep",
                                                 "method": "tools/call", "params": {
                                                     "name": "resolve_deep_bbox",
                                                     "arguments": {"corners": corners, "shallow_loc_id": "CAN-NT"}}}).json()
        assert deep["result"]["structuredContent"]["bbox"]["east"] == 1
        assert run.await_count == 2
