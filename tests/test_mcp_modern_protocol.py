"""Dual-era MCP endpoint: modern 2026-07-28 stateless requests and unchanged
legacy initialize-handshake behavior on the same /mcp routes."""
from __future__ import annotations

import base64
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mapmover.routes.mcp import (
    ADVERTISED_PROTOCOL_VERSIONS,
    MCP_MODERN_PROTOCOL_VERSION,
    META_SERVER_INFO,
    router as mcp_router,
)

MODERN = MCP_MODERN_PROTOCOL_VERSION


def _modern_meta(version: str = MODERN, *, capabilities: bool = True) -> dict:
    meta = {
        "io.modelcontextprotocol/protocolVersion": version,
        "io.modelcontextprotocol/clientInfo": {"name": "claude-ai", "version": "1.0"},
    }
    if capabilities:
        meta["io.modelcontextprotocol/clientCapabilities"] = {}
    return meta


def _modern_headers(method: str, name: str | None = None, version: str = MODERN) -> dict:
    headers = {"MCP-Protocol-Version": version, "Mcp-Method": method}
    if name is not None:
        headers["Mcp-Name"] = name
    return headers


class ModernProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        env = mock.patch.dict("os.environ", {"MCP_LIVE_TOOL_RATE_LIMIT": "10000"}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        # The tool rate limiter is process-wide; keep these calls out of the
        # shared test-client budget that other suites rely on.
        limiter = mock.patch("mapmover.routes.mcp._live_tool_rate_limit_response", return_value=None)
        limiter.start()
        self.addCleanup(limiter.stop)
        self.captured: dict = {}
        app = FastAPI()

        @app.middleware("http")
        async def capture(request, call_next):
            response = await call_next(request)
            self.captured = dict(getattr(request.state, "analytics_metadata", {}) or {})
            return response

        app.include_router(mcp_router)
        self.client = TestClient(app)

    def _modern(self, method: str, params: dict | None = None, *, path: str = "/mcp/free",
                headers: dict | None = None, name: str | None = None, rpc_id="m-1"):
        params = dict(params or {})
        params.setdefault("_meta", _modern_meta())
        body = {"jsonrpc": "2.0", "method": method, "params": params}
        if rpc_id is not None:
            body["id"] = rpc_id
        return self.client.post(path, headers=headers or _modern_headers(method, name), json=body)

    # --- discovery ------------------------------------------------------------

    def test_server_discover_advertises_both_eras_with_cache_hints(self) -> None:
        response = self._modern("server/discover")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["MCP-Protocol-Version"], MODERN)
        result = response.json()["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertEqual(result["supportedVersions"], ADVERTISED_PROTOCOL_VERSIONS)
        self.assertEqual(result["supportedVersions"][0], MODERN)
        self.assertIn("2025-06-18", result["supportedVersions"])
        self.assertEqual(set(result["capabilities"]), {"tools", "resources", "prompts"})
        self.assertIn("name", result["_meta"][META_SERVER_INFO])
        self.assertTrue(result["instructions"])
        self.assertGreater(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "public")
        self.assertNotIn("Mcp-Session-Id", response.headers)

    def test_modern_tools_list_is_complete_cacheable_and_identifies_server(self) -> None:
        response = self._modern("tools/list")
        self.assertEqual(response.status_code, 200)
        result = response.json()["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertGreater(len(result["tools"]), 0)
        self.assertGreater(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "public")
        self.assertIn(META_SERVER_INFO, result["_meta"])
        self.assertEqual(response.headers["MCP-Protocol-Version"], MODERN)

    def test_modern_tool_call_runs_and_records_client_identity(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as log_mock:
            response = self._modern(
                "tools/call", {"name": "get_tool_help", "arguments": {"topic": "overview"}},
                name="get_tool_help",
            )
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertNotIn("ttlMs", result)
        self.assertIn(META_SERVER_INFO, result["_meta"])
        self.assertEqual(self.captured["mcp_protocol_version"], MODERN)
        self.assertEqual(self.captured["mcp_client_name"], "claude-ai")
        self.assertEqual(log_mock.call_args.kwargs["metadata"]["mcp_client_name"], "claude-ai")

    def test_base64_encoded_mcp_name_is_decoded_before_comparison(self) -> None:
        encoded = "=?base64?" + base64.b64encode(b"get_tool_help").decode() + "?="
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            response = self._modern(
                "tools/call", {"name": "get_tool_help", "arguments": {"topic": "overview"}}, name=encoded,
            )
        self.assertEqual(response.status_code, 200, response.text)

    # --- modern validation errors -------------------------------------------

    def _assert_error(self, response, status: int, code: int) -> dict:
        self.assertEqual(response.status_code, status, response.text)
        error = response.json()["error"]
        self.assertEqual(error["code"], code)
        return error

    def test_unsupported_version_lists_supported_versions(self) -> None:
        response = self._modern(
            "tools/list", {"_meta": _modern_meta("2099-01-01")},
            headers=_modern_headers("tools/list", version="2099-01-01"),
        )
        error = self._assert_error(response, 400, -32022)
        self.assertEqual(error["data"]["requested"], "2099-01-01")
        self.assertEqual(error["data"]["supported"], ADVERTISED_PROTOCOL_VERSIONS)

    def test_header_and_meta_version_mismatch_is_rejected(self) -> None:
        response = self._modern(
            "tools/list", {"_meta": _modern_meta(MODERN)},
            headers=_modern_headers("tools/list", version="2025-06-18"),
        )
        self._assert_error(response, 400, -32020)

    def test_missing_or_wrong_mcp_method_header_is_rejected(self) -> None:
        self._assert_error(self._modern("tools/list", headers={"MCP-Protocol-Version": MODERN}), 400, -32020)
        self._assert_error(self._modern("tools/list", headers=_modern_headers("prompts/list")), 400, -32020)

    def test_missing_or_wrong_mcp_name_header_is_rejected(self) -> None:
        call = {"name": "get_tool_help", "arguments": {"topic": "overview"}}
        self._assert_error(self._modern("tools/call", call), 400, -32020)
        self._assert_error(self._modern("tools/call", call, name="get_catalog"), 400, -32020)

    def test_missing_client_capabilities_is_invalid_params(self) -> None:
        response = self._modern("tools/list", {"_meta": _modern_meta(capabilities=False)})
        self._assert_error(response, 400, -32602)

    def test_meta_version_without_header_is_rejected(self) -> None:
        response = self._modern("tools/list", headers={"Mcp-Method": "tools/list"})
        self._assert_error(response, 400, -32020)

    def test_removed_and_unknown_methods_are_http_404(self) -> None:
        self._assert_error(self._modern("ping"), 404, -32601)
        self._assert_error(self._modern("no/such_method"), 404, -32601)

    def test_unknown_tool_stays_a_200_tool_lookup_error(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            response = self._modern("tools/call", {"name": "get_fire_data", "arguments": {}}, name="get_fire_data")
        self._assert_error(response, 200, -32601)

    def test_modern_notification_is_accepted_without_body(self) -> None:
        response = self._modern("notifications/initialized", rpc_id=None)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.content, b"")

    # --- legacy era is unchanged ---------------------------------------------

    def test_legacy_initialize_negotiates_and_issues_session(self) -> None:
        for requested, expected in (("2025-11-25", "2025-11-25"), ("2025-06-18", "2025-06-18"), ("1999-01-01", "2025-06-18")):
            response = self.client.post("/mcp/free", json={
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": requested, "capabilities": {}, "clientInfo": {"name": "Claude AI"}},
            })
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["result"]["protocolVersion"], expected)
            self.assertNotIn("resultType", response.json()["result"])
            self.assertTrue(response.headers.get("Mcp-Session-Id", "").startswith("dm1.claude-ai."))
            self.assertEqual(self.captured["mcp_protocol_version"], expected)

    def test_legacy_requests_carry_no_modern_fields(self) -> None:
        response = self.client.post(
            "/mcp/free", headers={"MCP-Protocol-Version": "2025-06-18"},
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()["result"]
        self.assertNotIn("resultType", result)
        self.assertNotIn("ttlMs", result)
        self.assertEqual(response.headers["MCP-Protocol-Version"], "2025-06-18")
        ping = self.client.post("/mcp/free", json={"jsonrpc": "2.0", "id": 3, "method": "ping"})
        self.assertEqual(ping.json()["result"], {})

    def test_legacy_server_discover_without_modern_metadata_is_method_not_found(self) -> None:
        response = self.client.post(
            "/mcp/free", headers={"MCP-Protocol-Version": "2025-06-18"},
            json={"jsonrpc": "2.0", "id": 4, "method": "server/discover", "params": {}},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["error"]["code"], -32601)

    # --- retired facades --------------------------------------------------------

    def test_retired_facades_answer_410_and_point_to_the_umbrella(self) -> None:
        from mapmover.routes.system import router as system_router

        app = FastAPI()
        app.include_router(mcp_router)
        app.include_router(system_router)
        client = TestClient(app)
        for slug in ("currency", "worldpop", "distributed_manufacturing"):
            for response in (
                client.post(f"/mcp/{slug}", json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
                client.get(f"/mcp/{slug}"),
                client.get(f"/mcp/{slug}/server.json"),
                client.get(f"/.well-known/mcp/{slug}/server-card.json"),
            ):
                self.assertEqual(response.status_code, 410, (slug, response.request.url))
                self.assertEqual(response.json()["use_instead"], "https://app.daedalmap.com/mcp")
        self.assertEqual(client.post("/mcp/never_existed", json={"jsonrpc": "2.0", "id": 1, "method": "ping"}).status_code, 404)
        self.assertEqual(client.get("/mcp/never_existed/server.json").status_code, 404)

    # --- rollback switch -------------------------------------------------------

    def test_disabled_modern_protocol_restores_legacy_fallback_trigger(self) -> None:
        with mock.patch.dict("os.environ", {"MCP_MODERN_PROTOCOL_ENABLED": "0"}):
            response = self._modern("server/discover")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], -32000)


if __name__ == "__main__":
    unittest.main()
