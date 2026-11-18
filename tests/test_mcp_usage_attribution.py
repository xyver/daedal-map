"""Anonymous MCP usage attribution: session grouping, argument summary, and
recording of tools/call rejections that no handler logged."""
from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mapmover.routes.mcp import (
    MCP_SESSION_HEADER,
    _mcp_argument_analytics,
    router as mcp_router,
)


def _rpc(method: str, params: dict | None = None) -> dict:
    return {"jsonrpc": "2.0", "id": "t-1", "method": method, "params": params or {}}


class McpUsageAttributionTests(unittest.TestCase):
    def setUp(self) -> None:
        env = mock.patch.dict("os.environ", {"MCP_LIVE_TOOL_RATE_LIMIT": "10000"}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)

    def test_initialize_issues_anonymous_session_id_with_client_slug(self) -> None:
        response = self.client.post(
            "/mcp/free",
            json=_rpc("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "Claude AI", "version": "1.0"},
            }),
        )
        self.assertEqual(response.status_code, 200)
        session = response.headers.get(MCP_SESSION_HEADER) or ""
        self.assertRegex(session, r"^dm1\.claude-ai\.[0-9a-f]{24}$")
        other = self.client.post("/mcp/free", json=_rpc("initialize", {"clientInfo": {}}))
        self.assertRegex(other.headers.get(MCP_SESSION_HEADER) or "", r"^dm1\.unknown\.[0-9a-f]{24}$")
        self.assertNotEqual(session, other.headers.get(MCP_SESSION_HEADER))

    def test_unknown_or_foreign_session_header_is_accepted(self) -> None:
        for value in ("not-ours", "dm1.claude-ai.zz"):
            response = self.client.post(
                "/mcp/free", headers={MCP_SESSION_HEADER: value}, json=_rpc("tools/list")
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("result", response.json())

    def test_unknown_tool_rejection_writes_one_usage_row_with_session(self) -> None:
        session = "dm1.openai-mcp." + "a" * 24
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as log_mock:
            response = self.client.post(
                "/mcp/free",
                headers={MCP_SESSION_HEADER: session},
                json=_rpc("tools/call", {"name": "get_fire_data", "arguments": {"pack_id": "wildfires"}}),
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn("error", response.json())
        self.assertEqual(log_mock.call_count, 1)
        kwargs = log_mock.call_args.kwargs
        self.assertEqual(kwargs["decision"], "deny")
        self.assertEqual(kwargs["source_id"], "(unknown_tool)")
        self.assertEqual(kwargs["error_code"], "jsonrpc_-32601")
        metadata = kwargs["metadata"]
        self.assertEqual(metadata["requested_tool"], "get_fire_data")
        self.assertEqual(metadata["mcp_session_id"], "a" * 24)
        self.assertEqual(metadata["mcp_client_name"], "openai-mcp")
        self.assertEqual(metadata["event"], "mcp_protocol_rejection")

    def test_coordinate_rejection_is_recorded_with_typed_code(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as log_mock:
            self.client.post(
                "/mcp/free",
                json=_rpc("tools/call", {"name": "get_loc_id_info", "arguments": {"lat": 1.0, "lon": 2.0}}),
            )
        self.assertEqual(log_mock.call_count, 1)
        self.assertEqual(log_mock.call_args.kwargs["error_code"], "point_resolution_required")
        self.assertEqual(log_mock.call_args.kwargs["source_id"], "get_loc_id_info")

    def test_non_tool_methods_write_no_usage_row(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as log_mock:
            self.client.post("/mcp/free", json=_rpc("tools/list"))
            self.client.post("/mcp/free", json=_rpc("no/such/method"))
        log_mock.assert_not_called()

    def test_argument_summary_keeps_identifiers_and_drops_free_text(self) -> None:
        summary = _mcp_argument_analytics("convert_reference", {
            "from_system": "us_fips_county", "to_system": "loc_id",
            "value": "51059", "items": [{"value": "1"}, {"value": "2"}], "iso3": "USA",
        })["mcp_args"]
        self.assertEqual(summary, {
            "from_system": "us_fips_county", "to_system": "loc_id", "iso3": "USA", "items_count": 2,
        })
        help_summary = _mcp_argument_analytics(
            "get_tool_help", {"tool_name": "get_data", "question": "my private question"}
        )["mcp_args"]
        self.assertEqual(help_summary, {"tool_name": "get_data"})
        point = _mcp_argument_analytics(
            "resolve_point", {"scope_id": "USA", "family": "administrative", "lat": 38.9, "lon": -77.0}
        )["mcp_args"]
        self.assertEqual(point, {"scope_id": "USA", "family": "administrative"})
        data = _mcp_argument_analytics("get_data", {
            "pack_id": "world_bank_wdi", "metrics": ["NY.GDP.MKTP.CD"], "limit": 5,
            "filters": {"loc_ids": ["CAN"], "time": {"start": "2020"}},
        })["mcp_args"]
        self.assertEqual(data["pack_id"], "world_bank_wdi")
        self.assertEqual(data["metrics"], ["NY.GDP.MKTP.CD"])
        self.assertEqual(data["filter_keys"], ["loc_ids", "time"])
        shape = _mcp_argument_analytics("get_shape", {"loc_ids": ["USA-VA-059", "CAN-BC", "bad value"]})["mcp_args"]
        self.assertEqual(shape["loc_id_countries"], ["CAN", "USA", "other"])
        self.assertEqual(shape["loc_ids_count"], 3)
        self.assertEqual(_mcp_argument_analytics("get_pack", {"pack_id": "has spaces; drop"}), {"mcp_args": {"pack_id": "other"}})


if __name__ == "__main__":
    unittest.main()
