from __future__ import annotations

import base64
import gzip
import io
import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mapmover.caller_identity import CONFIDENCE_VERIFIED, KIND_ACCOUNT, CallerIdentity
from mapmover.runtime.reference_exchange import get_geometry_availability, get_geometry_references
from mapmover.routes.mcp import (
    _jsonrpc_response,
    _tool_rate_limit_for_tier,
    _tool_result,
    router as mcp_router,
)


def _mcp_call(client: TestClient, method: str, params: dict | None = None, *, path: str = "/mcp/geography", headers: dict | None = None) -> dict:
    response = client.post(
        path,
        headers=headers or {},
        json={"jsonrpc": "2.0", "id": "test-1", "method": method, "params": params or {}},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _tool_call(client: TestClient, name: str, arguments: dict | None = None, *, path: str = "/mcp/geography", headers: dict | None = None) -> dict:
    envelope = _mcp_call(
        client,
        "tools/call",
        {"name": name, "arguments": arguments or {}},
        path=path,
        headers=headers,
    )
    return envelope["result"]["structuredContent"]


class McpReferenceExchangeToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._rate_limit_env = mock.patch.dict(
            "os.environ", {"MCP_LIVE_TOOL_RATE_LIMIT": "10000"}, clear=False
        )
        self._rate_limit_env.start()
        self.addCleanup(self._rate_limit_env.stop)
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)

    def test_geography_facade_lists_reference_exchange_tools_first_class(self) -> None:
        envelope = _mcp_call(self.client, "tools/list")
        tool_names = {tool["name"] for tool in envelope["result"]["tools"]}

        self.assertIn("how_geometry_works", tool_names)
        self.assertIn("list_reference_systems", tool_names)
        self.assertIn("identify_dataset_geography", tool_names)
        self.assertIn("identify_reference_system", tool_names)
        self.assertIn("read_geometry_catalog", tool_names)
        self.assertIn("resolve_reference", tool_names)
        self.assertIn("convert_reference", tool_names)
        self.assertIn("compare_geographies", tool_names)
        self.assertIn("check_geometry", tool_names)
        self.assertIn("get_geometry", tool_names)
        self.assertIn("compare_geographies", tool_names)
        self.assertIn("resolve_point", tool_names)
        self.assertIn("loc_id_info", tool_names)
        self.assertIn("resolve_loc_id_scope", tool_names)
        self.assertIn("estimate_geometry_package", tool_names)
        self.assertIn("create_geometry_export", tool_names)
        self.assertIn("estimate_conversion_job", tool_names)
        self.assertIn("create_conversion_job", tool_names)
        self.assertIn("get_job_status", tool_names)
        self.assertNotIn("check_geometries", tool_names)
        self.assertNotIn("resolve_points", tool_names)
        self.assertNotIn("loc_id_references", tool_names)
        self.assertNotIn("get_boundary", tool_names)
        self.assertNotIn("loc_id_hierarchy", tool_names)
        self.assertNotIn("family_to_admin", tool_names)
        self.assertNotIn("admin_to_family", tool_names)

        catalog_tool = next(
            tool for tool in envelope["result"]["tools"]
            if tool["name"] == "read_geometry_catalog"
        )
        catalog_views = catalog_tool["inputSchema"]["properties"]["view"]["enum"]
        self.assertIn("crosswalk_artifacts", catalog_views)
        self.assertNotIn("bridges", catalog_views)

    def test_dataset_geography_tool_selects_country_admin_binding(self) -> None:
        payload = _tool_call(
            self.client,
            "identify_dataset_geography",
            {
                "columns": [
                    {"name": "municipality_code", "values": ["1200013", "1200054"], "nonempty_count": 2},
                    {"name": "population", "values": ["4567", "8910"], "nonempty_count": 2},
                ],
                "dataset_context": {"file_name": "brazil.csv", "row_count": 2},
            },
        )

        selected = payload["candidates"][0]
        self.assertEqual(selected["header"], "municipality_code")
        self.assertEqual(selected["catalog"]["recommended_binding"]["country_scope"], "BRA")
        self.assertEqual(selected["catalog"]["recommended_binding"]["geo_level"], "admin_2")

    def test_geography_facade_has_coordinated_registry_identity(self) -> None:
        envelope = _mcp_call(
            self.client,
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "release-test", "version": "0.1.0"}},
        )

        self.assertEqual(envelope["result"]["serverInfo"]["name"], "com.daedalmap/geography")
        self.assertEqual(envelope["result"]["serverInfo"]["version"], "1.1.0")

    def test_large_structured_tool_result_summarizes_text_copy(self) -> None:
        payload = {
            "request_id": "large-shape-test",
            "ok": True,
            "results": [{"loc_id": "USA-AK-063", "geometry": "x" * 200}],
        }

        with mock.patch.dict("os.environ", {"MCP_TOOL_TEXT_INLINE_MAX_BYTES": "100"}):
            result = _tool_result(payload)

        self.assertEqual(result["structuredContent"]["request_id"], "large-shape-test")
        text = result["content"][0]["text"]
        self.assertIn("Large structured MCP result", text)
        self.assertIn("structuredContent", text)
        self.assertNotIn("x" * 200, text)

    def test_non_finite_numbers_do_not_break_the_tool_result(self) -> None:
        # Geometry banks carry rows with no measured centroid or bbox. Those
        # arrive as NaN, and Starlette renders with allow_nan=False, so an
        # unsanitized payload returned a 500 instead of the row.
        payload = {
            "request_id": "nan-test",
            "name": "Australian Capital Territory",
            "centroid": {"lon": float("nan"), "lat": float("nan")},
            "bbox": [float("nan"), 1.5, float("inf"), float("-inf")],
            "rows": [{"area": float("nan")}, {"area": 2.5}],
            "children_count": 9,
        }

        result = _tool_result(payload)
        structured = result["structuredContent"]

        self.assertIsNone(structured["centroid"]["lon"])
        self.assertIsNone(structured["centroid"]["lat"])
        self.assertEqual(structured["bbox"], [None, 1.5, None, None])
        self.assertEqual(structured["rows"], [{"area": None}, {"area": 2.5}])
        self.assertEqual(structured["name"], "Australian Capital Territory")
        self.assertEqual(structured["children_count"], 9)
        # Both copies must survive a strict parser.
        json.loads(json.dumps(structured, allow_nan=False))
        json.loads(result["content"][0]["text"])

    def test_jsonrpc_envelope_renders_non_finite_numbers(self) -> None:
        response = _jsonrpc_response(_tool_result({"bbox": [float("nan")]}), "test-1")

        decoded = json.loads(response.body)

        self.assertEqual(decoded["result"]["structuredContent"]["bbox"], [None])

    def test_geometry_family_help_explains_workflows_and_per_tool_help(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            payload = _tool_call(
                self.client,
                "how_geometry_works",
                {"question": "How do I match an uploaded Census dataset?"},
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["interaction_contract"]["per_tool_help"]["tool"], "get_tool_help")
        workflow_names = {workflow["name"] for workflow in payload["workflows"]}
        self.assertIn("known_or_suspected_dataset_identifiers", workflow_names)
        self.assertIn("partitioned_deep_points_across_multiple_regions", workflow_names)
        self.assertIn("known_loc_ids_to_shapes", workflow_names)
        self.assertNotIn("shapes_and_exports", workflow_names)
        self.assertEqual(payload["start_here"][0]["tool"], "read_geometry_catalog")
        self.assertEqual(payload["start_here"][0]["arguments"]["view"], "capabilities")
        self.assertEqual(payload["start_here"][0]["arguments"]["country_scope"], "<ISO3 when known>")
        self.assertIn("administrative_spine", payload["concepts"])
        self.assertIn("reference_families", payload["concepts"])
        deep_workflow = next(
            workflow for workflow in payload["workflows"]
            if workflow["name"] == "partitioned_deep_points_across_multiple_regions"
        )
        self.assertIn("fields accepted by resolve_point", deep_workflow["important"])
        self.assertIn("identify_reference_system", payload["available_tools"])
        self.assertNotIn("query_dataset", payload["available_tools"])
        self.assertEqual(analytics_mock.call_args.kwargs["capability_id"], "geometry_family_help")

    def test_reverse_geocoding_facade_lists_multipurpose_point_tool(self) -> None:
        envelope = _mcp_call(self.client, "tools/list", path="/mcp/reverse-geocoding")
        tool_names = {tool["name"] for tool in envelope["result"]["tools"]}

        self.assertIn("resolve_point", tool_names)
        self.assertNotIn("resolve_points", tool_names)
        self.assertNotIn("get_boundary", tool_names)

    def test_boundaries_facade_lists_geometry_preflight_tools(self) -> None:
        envelope = _mcp_call(self.client, "tools/list", path="/mcp/boundaries")
        tool_names = {tool["name"] for tool in envelope["result"]["tools"]}

        self.assertIn("check_geometry", tool_names)
        self.assertNotIn("check_geometries", tool_names)
        self.assertIn("get_geometry", tool_names)
        self.assertNotIn("get_boundary", tool_names)
        self.assertNotIn("resolve_points", tool_names)
        self.assertIn("resolve_loc_id_scope", tool_names)
        self.assertIn("estimate_geometry_package", tool_names)
        self.assertIn("create_geometry_export", tool_names)
        self.assertIn("get_job_status", tool_names)

    def test_resolve_point_tool_accepts_point_batch(self) -> None:
        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {
                    "point": {"lon": point["lon"], "lat": point["lat"]},
                    "matched": {"loc_id": f"TEST-{point['lat']}-{point['lon']}", "iso3": "USA"},
                    "deepest_resolved_loc_id": f"TEST-{point['lat']}-{point['lon']}",
                    "deepest_resolved_admin_level": "admin_2",
                    "stack": [{"loc_id": "USA"}, {"loc_id": f"TEST-{point['lat']}-{point['lon']}"}],
                    "target_admin_level": "admin_2",
                    "deeper_available": True,
                    "available_deeper_admin_levels": ["admin_3"],
                    "query_layout": "admin_0_3_plus_admin_1_deep",
                }
                for point in points
            ]

        with (
            mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve) as bulk_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {
                    "request_id": "mcp-bulk-test",
                    "batch_id": "batch-1",
                    "points": [
                        {"row_index": 10, "lon": -123.1, "lat": 49.2},
                        {"row_index": 11, "lon": -122.9, "lat": 49.1},
                    ],
                },
            )

        self.assertEqual(payload["batch_id"], "batch-1")
        bulk_mock.assert_called_once()
        self.assertEqual(payload["point_count"], 2)
        self.assertEqual(payload["resolved_count"], 2)
        self.assertEqual(payload["results"][0]["row_index"], 10)
        self.assertEqual(payload["results"][0]["deepest_resolved_loc_id"], "TEST-49.2--123.1")
        self.assertEqual(payload["results"][0]["resolution_mode"], "latest_available_per_depth")
        self.assertEqual(payload["results"][0]["resolution_schema_version"], "1.0.0")
        self.assertEqual(payload["results"][0]["query_layout"], "admin_0_3_plus_admin_1_deep")
        self.assertIn("join_keys", payload["results"][0])
        self.assertTrue(payload["results"][0]["deeper_available"])
        self.assertEqual(payload["results"][0]["available_deeper_admin_levels"], ["admin_3"])
        self.assertIsNone(bulk_mock.call_args.kwargs["target_admin_level"])
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["capability_id"], "point_lookup")
        self.assertEqual(analytics["pack_id"], "geography_tools")
        self.assertEqual(analytics["source_id"], "resolve_point")
        self.assertEqual(analytics["decision"], "allow")
        self.assertEqual(analytics["payment_rail"], "free")
        self.assertEqual(analytics["row_count"], 2)
        self.assertEqual(analytics["query_granularity"], "bulk_2")
        self.assertEqual(analytics["metadata"]["surface"], "agent_api_mcp")
        self.assertEqual(analytics["metadata"]["event"], "point_lookup")
        self.assertEqual(analytics["metadata"]["tool_mode"], "bulk")
        self.assertEqual(analytics["metadata"]["quantity"], 2)
        self.assertEqual(analytics["metadata"]["batch_id"], "batch-1")
        self.assertEqual(analytics["metadata"]["compute"]["input_count"], 2)
        self.assertEqual(analytics["metadata"]["compute"]["output_count"], 2)
        self.assertIn("point_resolver_ms", analytics["metadata"]["compute"]["stage_ms"])

    def test_resolve_point_tool_challenges_point_batch_over_free_limit(self) -> None:
        """Over the free allowance the verifier decides, and its price is passed through."""
        challenge = (
            "challenge",
            {
                "status": "challenge",
                "message": "Commercial access is required for this capability.",
                "context": {"pricing": {"price_display": "$0.011306", "amount_usdc_base_units": 11306}},
                "challenge": {"opaque": True, "headers": {}},
            },
        )
        with (
            mock.patch(
                "mapmover.routes.mcp._tool_effective_access",
                return_value={"allow": True, "settlement_required": True, "access_lane": "metered"},
            ),
            mock.patch("mapmover.routes.mcp._commercial_access_decision", return_value=challenge),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": 0, "lat": 0} for _ in range(101)], "country_scope": "USA", "target_admin_level": "admin_2"},
            )

        self.assertTrue(payload["payment_required"])
        self.assertEqual(payload["limits"]["free_batch_limit"], 100)
        self.assertEqual(payload["error"]["code"], "payment_required")
        # The caller must receive the verifier's real price, not a guess.
        self.assertEqual(payload["daedalmap_pricing"]["amount_usdc_base_units"], 11306)
        self.assertTrue(payload["challenge"]["opaque"])
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["decision"], "challenge")
        self.assertEqual(analytics["payment_rail"], "commercial_access")

    def test_resolve_point_bulk_requires_one_country_and_level(self) -> None:
        payload = _tool_call(
            self.client,
            "resolve_point",
            {"points": [{"lon": 0, "lat": 0} for _ in range(101)]},
        )
        self.assertEqual(payload["error"]["code"], "bulk_scope_required")
        self.assertEqual(payload["error"]["missing_fields"], ["country_scope", "target_admin_level"])

    def test_verified_account_uses_included_bulk_without_commercial_verifier(self) -> None:
        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {
                    "point": {"lon": point["lon"], "lat": point["lat"]},
                    "matched": {"loc_id": "USA-CA-037", "admin_level": 2, "iso3": "USA"},
                    "stack": [{"loc_id": "USA"}, {"loc_id": "USA-CA-037"}],
                    "target_admin_level": "admin_2",
                }
                for point in points
            ]

        # A real identity, not a Mock: the included allowance now depends on the
        # caller's access tier, so a Mock would silently invent a lane.
        identity = CallerIdentity(KIND_ACCOUNT, "user-1", CONFIDENCE_VERIFIED, auth_user_id="user-1")
        with (
            mock.patch("mapmover.routes.mcp.request_caller_identity", return_value=identity),
            mock.patch("mapmover.routes.mcp._tool_paid_bulk_enforced", return_value=False),
            mock.patch("mapmover.routes.mcp._commercial_access_decision") as verifier_mock,
            mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": -118.2, "lat": 34.0} for _ in range(101)], "country_scope": "USA", "target_admin_level": "admin_2"},
            )
        self.assertEqual(payload["point_count"], 101)
        self.assertEqual(payload["resolved_count"], 101)
        verifier_mock.assert_not_called()

    def test_global_admin_1_preset_replaces_country_scope_and_level(self) -> None:
        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {"matched": {"loc_id": "CAN-ON", "admin_level": 1}, "stack": [{"loc_id": "CAN"}, {"loc_id": "CAN-ON"}]}
                for _ in points
            ]

        # A real identity, not a Mock: the included allowance now depends on the
        # caller's access tier, so a Mock would silently invent a lane.
        identity = CallerIdentity(KIND_ACCOUNT, "user-1", CONFIDENCE_VERIFIED, auth_user_id="user-1")
        with (
            mock.patch("mapmover.routes.mcp.request_caller_identity", return_value=identity),
            mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve) as resolver_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": -79.4, "lat": 43.7} for _ in range(101)], "bulk_preset": "global_admin_1"},
            )

        self.assertEqual(payload["bulk_preset"], "global_admin_1")
        self.assertEqual(resolver_mock.call_args.kwargs["target_admin_level"], 1)
        self.assertIsNone(resolver_mock.call_args.kwargs["country_scope"])

    def test_global_preset_rejects_conflicting_country_scope(self) -> None:
        payload = _tool_call(
            self.client,
            "resolve_point",
            {"points": [{"lon": 0, "lat": 0} for _ in range(101)], "bulk_preset": "global_admin_0", "country_scope": "USA"},
        )
        self.assertEqual(payload["error"]["code"], "bulk_preset_conflict")
        self.assertEqual(payload["error"]["conflicting_fields"], ["country_scope"])

    def test_resolve_point_refuses_when_the_verifier_is_unreachable(self) -> None:
        """Fail closed: a paid request must never execute for free."""
        with (
            mock.patch(
                "mapmover.routes.mcp._tool_effective_access",
                return_value={"allow": True, "settlement_required": True, "access_lane": "metered"},
            ),
            mock.patch(
                "mapmover.routes.mcp._commercial_access_decision",
                return_value=("unavailable", {"error": {"code": "commercial_access_unavailable"}}),
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": 0, "lat": 0} for _ in range(101)], "country_scope": "USA", "target_admin_level": "admin_2"},
            )

        self.assertEqual(payload["error"]["code"], "commercial_access_unavailable")
        self.assertEqual(analytics_mock.call_args.kwargs["decision"], "deny")

    def test_resolve_point_executes_and_records_settlement_when_allowed(self) -> None:
        """A settled call runs, and lands in analytics as paid rather than free."""

        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {
                    "point": {"lon": point["lon"], "lat": point["lat"]},
                    "matched": {"loc_id": "TEST-1", "admin_level": 2, "iso3": "USA"},
                    "stack": [{"loc_id": "USA"}, {"loc_id": "TEST-1"}],
                    "target_admin_level": "admin_2",
                    "deeper_available": False,
                    "available_deeper_admin_levels": [],
                }
                for point in points
            ]

        allow = ("allow", {"status": "allow", "settlement": {"settlement_id": "settle-abc"}})
        with (
            mock.patch(
                "mapmover.routes.mcp._tool_effective_access",
                return_value={"allow": True, "settlement_required": True, "access_lane": "metered"},
            ),
            mock.patch("mapmover.routes.mcp._commercial_access_decision", return_value=allow),
            mock.patch(
                "mapmover.routes.mcp.settle_commercial_access",
                return_value=(True, {"status": "allow", "context": {"account_credit": {"charged_micro_usd": 0}}}),
            ) as settle_mock,
            mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": 0, "lat": 0} for _ in range(101)], "country_scope": "USA", "target_admin_level": "admin_2"},
            )

        self.assertEqual(payload["point_count"], 101)
        settle_kwargs = settle_mock.call_args.kwargs
        self.assertEqual(settle_kwargs["actual_pricing"]["amount_usdc_base_units"], 0)
        self.assertEqual(settle_kwargs["meter_receipt"]["successful_distinct_items"], 1)
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["decision"], "allow")
        self.assertEqual(analytics["payment_rail"], "paid")
        self.assertEqual(analytics["metadata"]["settlement_id"], "settle-abc")

    def test_resolve_point_tool_trusted_token_executes_over_free_limit(self) -> None:
        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {
                    "point": {"lon": point["lon"], "lat": point["lat"]},
                    "matched": {"loc_id": f"TEST-{point['row_index']}", "admin_level": 2, "iso3": "USA"},
                    "stack": [{"loc_id": "USA"}, {"loc_id": f"TEST-{point['row_index']}"}],
                    "target_admin_level": "admin_2",
                    "deeper_available": False,
                    "available_deeper_admin_levels": [],
                }
                for point in points
            ]

        with mock.patch.dict("os.environ", {"ARTIFACT_ACCESS_TOKENS": "tok_test_bypass"}):
            with (
                mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve) as bulk_mock,
                mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
            ):
                payload = _tool_call(
                    self.client,
                    "resolve_point",
                    {"points": [{"lon": 0, "lat": 0, "row_index": index} for index in range(101)], "country_scope": "USA", "target_admin_level": "admin_2"},
                    headers={"Authorization": "Bearer tok_test_bypass"},
                )

        self.assertEqual(payload["point_count"], 101)
        self.assertEqual(payload["resolved_count"], 101)
        bulk_mock.assert_called_once()
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["decision"], "allow")
        self.assertEqual(analytics["payment_rail"], "trusted_artifact")
        self.assertIsNotNone(analytics["artifact_token_id"])

    def test_resolve_point_tool_uses_per_tool_batch_limit_override(self) -> None:
        challenge = ("challenge", {"status": "challenge", "context": {}, "challenge": {}})
        with mock.patch.dict("os.environ", {"MCP_TOOL_BATCH_LIMIT_RESOLVE_POINT": "2"}):
            with (
                mock.patch(
                    "mapmover.routes.mcp._tool_effective_access",
                    return_value={"allow": True, "settlement_required": True, "access_lane": "metered"},
                ),
                mock.patch("mapmover.routes.mcp._commercial_access_decision", return_value=challenge),
                mock.patch("mapmover.routes.mcp.log_api_query_event"),
            ):
                payload = _tool_call(
                    self.client,
                    "resolve_point",
                    {"points": [{"lon": 0, "lat": 0} for _ in range(3)], "country_scope": "USA", "target_admin_level": "admin_2"},
                )

        self.assertEqual(payload["limits"]["free_batch_limit"], 2)
        self.assertEqual(payload["error"]["code"], "payment_required")

    def test_launch_free_waives_payment_but_keeps_item_limit(self) -> None:
        def fake_resolve(points, include_geometry=False, **_kwargs):
            return [
                {
                    "point": {"lon": point["lon"], "lat": point["lat"]},
                    "matched": {"loc_id": "USA-CA-037", "admin_level": 2, "iso3": "USA"},
                    "stack": [{"loc_id": "USA"}, {"loc_id": "USA-CA-037"}],
                    "target_admin_level": "admin_2",
                }
                for point in points
            ]

        policy = '{"schema_version":"1.0.0","policy_revision":"launch-test","mode":"launch_free"}'
        with mock.patch.dict(
            "os.environ",
            {
                "COMMERCIAL_ACCESS_ENABLED": "1",
                "MCP_TOOL_BATCH_LIMIT_RESOLVE_POINT": "2",
                "MCP_TOOL_PAID_BATCH_LIMIT_RESOLVE_POINT": "4",
                "DAEDALMAP_ACCESS_POLICY_JSON": policy,
            },
            clear=False,
        ):
            from access_policy_shared import clear_access_policy_cache

            clear_access_policy_cache()
            with (
                mock.patch(
                    "mapmover.runtime.geometry_catalog.geometry_bank_access_facts",
                    return_value=({"paid"}, True),
                ),
                mock.patch("mapmover.routes.mcp._commercial_access_decision") as verifier_mock,
                mock.patch("mapmover.geometry_handlers.resolve_points_to_locations", side_effect=fake_resolve),
                mock.patch("mapmover.routes.mcp.log_api_query_event"),
            ):
                payload = _tool_call(
                    self.client,
                    "resolve_point",
                    {
                        "points": [{"lon": 0, "lat": 0} for _ in range(3)],
                        "country_scope": "USA",
                        "target_admin_level": "admin_2",
                    },
                )
            clear_access_policy_cache()

        self.assertEqual(payload["point_count"], 3)
        self.assertIn("resolved_count", payload, payload)
        self.assertEqual(payload["resolved_count"], 3)
        verifier_mock.assert_not_called()

    def test_resolve_point_above_interactive_ceiling_returns_honest_v0_limit(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MCP_TOOL_BATCH_LIMIT_RESOLVE_POINT": "2", "MCP_TOOL_PAID_BATCH_LIMIT_RESOLVE_POINT": "3"}),
            mock.patch("mapmover.routes.mcp._tool_paid_bulk_enforced", return_value=True),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_point",
                {"points": [{"lon": 0, "lat": 0} for _ in range(4)], "country_scope": "USA", "target_admin_level": "admin_2"},
            )
        self.assertFalse(payload["payment_required"])
        self.assertEqual(payload["error"]["code"], "interactive_limit_exceeded")
        self.assertEqual(payload["delivery"]["required_mode"], "not_available_in_v0")

    def test_check_geometry_tool_accepts_loc_id_batch(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.get_geometry_availability",
                return_value={
                    "ok": True,
                    "requested": 3,
                    "available": 2,
                    "missing": 1,
                    "items": [
                        {"loc_id": "USA-CA-037", "has_shape": True},
                        {"loc_id": "USA-CA-075", "has_shape": True},
                        {"loc_id": "USA-NOPE", "has_shape": False, "error": "no geometry found"},
                    ],
                    "results": [
                        {"loc_id": "USA-CA-037", "has_shape": True},
                        {"loc_id": "USA-CA-075", "has_shape": True},
                        {"loc_id": "USA-NOPE", "has_shape": False, "error": "no geometry found"},
                    ],
                },
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "check_geometry",
                {"batch_id": "shapes-1", "loc_ids": ["USA-CA-037", "USA-CA-075", "USA-NOPE"]},
            )

        self.assertEqual(payload["batch_id"], "shapes-1")
        self.assertEqual(payload["requested"], 3)
        self.assertEqual(payload["available"], 2)
        self.assertEqual(payload["missing"], 1)
        self.assertEqual(payload["items"][2]["has_shape"], False)
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["capability_id"], "geometry_availability")
        self.assertEqual(analytics["pack_id"], "geography_tools")
        self.assertEqual(analytics["source_id"], "check_geometry")
        self.assertEqual(analytics["decision"], "allow")
        self.assertEqual(analytics["payment_rail"], "free")
        self.assertEqual(analytics["row_count"], 3)
        self.assertEqual(analytics["query_granularity"], "bulk_3")
        self.assertEqual(analytics["metadata"]["event"], "geometry_availability")
        self.assertEqual(analytics["metadata"]["tool_mode"], "bulk")
        self.assertEqual(analytics["metadata"]["quantity"], 3)
        self.assertEqual(analytics["metadata"]["available_count"], 2)
        self.assertEqual(analytics["metadata"]["missing_count"], 1)
        self.assertEqual(analytics["metadata"]["compute"]["input_count"], 3)
        self.assertEqual(analytics["metadata"]["compute"]["output_count"], 2)
        self.assertIn("geometry_availability_ms", analytics["metadata"]["compute"]["stage_ms"])

    def test_geometry_availability_uses_metadata_only_fetch(self) -> None:
        metadata_rows = [
                {
                    "loc_id": "USA-CA-037",
                    "name": "Los Angeles County",
                    "admin_level": 2,
                    "centroid_lon": -118.25,
                    "centroid_lat": 34.05,
                    "bbox_min_lon": -119.0,
                    "bbox_min_lat": 33.0,
                    "bbox_max_lon": -117.0,
                    "bbox_max_lat": 35.0,
                }
        ]
        with (
            mock.patch("mapmover.runtime.reference_exchange.get_selection_geometry_metadata", return_value=metadata_rows) as metadata_mock,
            mock.patch("mapmover.runtime.reference_exchange.get_selection_geometries") as geometry_mock,
        ):
            payload = get_geometry_availability(["USA-CA-037", "USA-NOPE"])

        metadata_mock.assert_called_once_with(["USA-CA-037", "USA-NOPE"])
        geometry_mock.assert_not_called()
        self.assertEqual(payload["requested"], 2)
        self.assertEqual(payload["available"], 1)
        self.assertEqual(payload["missing"], 1)
        self.assertEqual(payload["items"][0]["has_shape"], True)
        self.assertEqual(payload["items"][1]["has_shape"], False)

    def test_get_geometry_metadata_uses_metadata_only_fetch(self) -> None:
        metadata_rows = [
            {
                "loc_id": "USA-CA-037",
                "name": "Los Angeles County",
                "admin_level": 2,
                "centroid_lon": -118.25,
                "centroid_lat": 34.05,
                "bbox_min_lon": -119.0,
                "bbox_min_lat": 33.0,
                "bbox_max_lon": -117.0,
                "bbox_max_lat": 35.0,
            }
        ]
        with (
            mock.patch("mapmover.runtime.reference_exchange.get_selection_geometry_metadata", return_value=metadata_rows) as metadata_mock,
            mock.patch("mapmover.runtime.reference_exchange.get_selection_geometries") as geometry_mock,
            mock.patch("mapmover.runtime.reference_exchange.get_location_info", return_value={"loc_id": "USA-CA-037"}),
        ):
            payload = get_geometry_references(["USA-CA-037", "USA-NOPE"], include_polygon=False)

        metadata_mock.assert_called_once_with(["USA-CA-037", "USA-NOPE"])
        geometry_mock.assert_not_called()
        self.assertEqual(payload["available"], 1)
        self.assertEqual(payload["results"][0]["name"], "Los Angeles County")
        self.assertNotIn("geometry", payload["results"][0])

    def test_get_geometry_single_normalizes_legacy_string_error(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.get_geometry_reference",
                return_value={"ok": False, "loc_id": "USA-NOPE", "has_shape": False, "error": "no geometry found"},
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(self.client, "get_geometry", {"loc_id": "USA-NOPE"})

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "not_found")
        self.assertEqual(payload["error"]["message"], "no geometry found")
        self.assertEqual(analytics_mock.call_args.kwargs["error_code"], "not_found")

    def test_check_geometry_tool_uses_per_tool_batch_limit_override(self) -> None:
        with mock.patch.dict("os.environ", {"MCP_TOOL_BATCH_LIMIT_CHECK_GEOMETRY": "2"}):
            with mock.patch("mapmover.routes.mcp.log_api_query_event"):
                payload = _tool_call(
                    self.client,
                    "check_geometry",
                    {"loc_ids": ["USA", "CAN", "MEX"]},
                )

        self.assertEqual(payload["limit"], 2)
        self.assertEqual(payload["error"]["code"], "too_many_loc_ids")

    def test_get_geometry_tool_accepts_loc_id_batch(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.get_geometry_references",
                return_value={
                    "ok": True,
                    "requested": 2,
                    "items": [
                        {"ok": True, "loc_id": "USA-CA-037", "bbox": [-119, 33, -117, 35]},
                        {"ok": False, "loc_id": "USA-NOPE", "error": {"code": "not_found"}},
                    ],
                },
            ) as geometry_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "get_geometry",
                {"batch_id": "geo-1", "loc_ids": ["USA-CA-037", "USA-NOPE"]},
            )

        geometry_mock.assert_called_once_with(["USA-CA-037", "USA-NOPE"], include_polygon=False, include_info=False)
        self.assertEqual(payload["batch_id"], "geo-1")
        self.assertEqual(payload["requested"], 2)
        self.assertEqual(payload["items"][0]["loc_id"], "USA-CA-037")
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["source_id"], "get_geometry")
        self.assertEqual(analytics["capability_id"], "geometry_lookup")
        self.assertEqual(analytics["row_count"], 2)
        self.assertEqual(analytics["query_granularity"], "bulk_2")
        self.assertEqual(analytics["metadata"]["tool_mode"], "bulk")
        self.assertEqual(analytics["metadata"]["quantity"], 2)
        self.assertEqual(analytics["metadata"]["compute"]["input_count"], 2)
        self.assertEqual(analytics["metadata"]["compute"]["output_count"], 1)
        self.assertEqual(analytics["metadata"]["compute"]["include_polygon"], False)
        self.assertIn("geometry_fetch_ms", analytics["metadata"]["compute"]["stage_ms"])

    def test_point_and_geometry_schemas_keep_details_in_loc_id_info(self) -> None:
        envelope = _mcp_call(self.client, "tools/list")
        tools = {tool["name"]: tool for tool in envelope["result"]["tools"]}

        self.assertNotIn("include_geometry", tools["resolve_point"]["inputSchema"]["properties"])
        self.assertNotIn("include_info", tools["get_geometry"]["inputSchema"]["properties"])
        self.assertIn("Pass the returned stack loc_ids to loc_id_info", tools["resolve_point"]["description"])
        self.assertIn("drill-down tool", tools["loc_id_info"]["description"])
        self.assertIn("does not explain hierarchy", tools["get_geometry"]["description"])
        self.assertIn("never substituted", tools["get_geometry"]["description"])
        self.assertIn("explicit follow-up choice", tools["check_geometry"]["description"])

    def test_get_geometry_tool_trusted_token_bypasses_batch_limit(self) -> None:
        with mock.patch.dict("os.environ", {"ARTIFACT_ACCESS_TOKENS": "tok_test_bypass", "MCP_TOOL_BATCH_LIMIT_GET_GEOMETRY": "2"}):
            with (
                mock.patch(
                    "mapmover.runtime.reference_exchange.get_geometry_references",
                    return_value={
                        "ok": True,
                        "requested": 3,
                        "available": 3,
                        "missing": 0,
                        "results": [
                            {"ok": True, "loc_id": "USA-CA-037", "has_shape": True},
                            {"ok": True, "loc_id": "USA-NY-061", "has_shape": True},
                            {"ok": True, "loc_id": "USA-IL-031", "has_shape": True},
                        ],
                    },
                ) as geometry_mock,
                mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
            ):
                payload = _tool_call(
                    self.client,
                    "get_geometry",
                    {"loc_ids": ["USA-CA-037", "USA-NY-061", "USA-IL-031"]},
                    headers={"Authorization": "Bearer tok_test_bypass"},
                )

        geometry_mock.assert_called_once()
        self.assertEqual(payload["requested"], 3)
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["payment_rail"], "trusted_artifact")
        self.assertIsNotNone(analytics["artifact_token_id"])

    def test_loc_id_info_can_include_references(self) -> None:
        with (
            mock.patch(
                "mapmover.geometry_handlers.get_location_info",
                return_value={
                    "loc_id": "USA-AK-282",
                    "name": "Yakutat",
                    "admin_level": 2,
                    "parent_id": "USA-AK",
                    "family": "admin",
                    "subtype": "county_and_equivalent",
                    "iso3": "USA",
                    "centroid": {"lon": -140, "lat": 59},
                    "bbox": [-142, 58, -138, 60],
                    "has_polygon": True,
                    "source_vintage": "2021",
                    "source_system": "test_authority",
                    "release_id": "test-release-2021",
                    "children_count": 0,
                    "children_by_level": "{}",
                    "descendants_count": 0,
                },
            ),
            mock.patch(
                "mapmover.runtime.reference_exchange.loc_id_references",
                return_value={"ok": True, "references": [{"system": "overlay_nws_fire_weather_zone", "value": "USA-NWSFZ-AKZ317"}]},
            ) as references_mock,
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_id": "USA-AK-282", "include_references": True, "systems": ["nws_fire"]},
            )

        references_mock.assert_called_once()
        self.assertEqual(payload["loc_id"], "USA-AK-282")
        self.assertEqual(payload["subtype"], "county_and_equivalent")
        self.assertEqual(payload["source_vintage"], "2021")
        self.assertEqual(payload["release_id"], "test-release-2021")
        self.assertEqual(payload["reference_count"], 1)
        self.assertEqual(payload["references"]["references"][0]["system"], "overlay_nws_fire_weather_zone")

    def test_loc_id_info_returns_requested_history_before_successor_prompt(self) -> None:
        with mock.patch(
            "mapmover.geometry_handlers.get_location_info",
            return_value={
                "loc_id": "USA-CT-OLD",
                "name": "Historical Connecticut county",
                "admin_level": 2,
                "iso3": "USA",
                "valid_to": "2022-12-31",
                "superseded_by": "USA-CT-NEW",
                "has_polygon": True,
            },
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_id": "USA-CT-OLD"},
            )

        self.assertEqual(payload["loc_id"], "USA-CT-OLD")
        self.assertEqual(payload["name"], "Historical Connecticut county")
        self.assertEqual(payload["supersession"]["successor_loc_id"], "USA-CT-NEW")
        self.assertFalse(payload["supersession"]["successor_included"])

    def test_loc_id_info_accepts_preferred_public_alias_and_returns_canonical_id(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.resolve_loc_id_input",
                return_value={
                    "ok": True,
                    "status": "resolved",
                    "requested_loc_id": "USA-PLACE-SPRINGFIELD-IL",
                    "loc_id": "USA-IL-167-PLACE-12345",
                    "resolved_from_public_alias": True,
                    "public_alias": "USA-PLACE-SPRINGFIELD-IL",
                    "reference_system": "public.usa.place.v2",
                },
            ),
            mock.patch(
                "mapmover.geometry_handlers.get_location_info",
                return_value={
                    "loc_id": "USA-IL-167-PLACE-12345", "name": "Springfield",
                    "family": "place_or_municipality", "iso3": "USA",
                },
            ) as info_mock,
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_id": "USA-PLACE-SPRINGFIELD-IL"},
            )

        info_mock.assert_called_once_with("USA-IL-167-PLACE-12345", include_memberships=False)
        self.assertEqual(payload["loc_id"], "USA-IL-167-PLACE-12345")
        self.assertEqual(payload["requested_loc_id"], "USA-PLACE-SPRINGFIELD-IL")
        self.assertTrue(payload["resolved_from_public_alias"])

    def test_loc_id_info_batch_fetches_unique_metadata_once(self) -> None:
        def resolve(loc_id: str) -> dict:
            return {
                "ok": True,
                "requested_loc_id": loc_id,
                "loc_id": loc_id,
                "resolved_from_public_alias": False,
            }

        infos = [
            {"loc_id": "USA-CA-037", "name": "Los Angeles County", "admin_level": 2, "iso3": "USA"},
            {"loc_id": "USA-NY-061", "name": "New York County", "admin_level": 2, "iso3": "USA"},
        ]
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.resolve_loc_id_input",
                side_effect=resolve,
            ) as resolve_mock,
            mock.patch(
                "mapmover.geometry_handlers.get_location_infos",
                return_value=infos,
            ) as info_mock,
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_ids": ["USA-CA-037", "USA-NY-061", "USA-CA-037"]},
            )

        self.assertEqual(payload["found_count"], 3)
        resolve_mock.assert_not_called()
        info_mock.assert_called_once_with(
            ["USA-CA-037", "USA-NY-061"],
            include_memberships=False,
            fallback=False,
        )
        self.assertEqual(
            [result["loc_id"] for result in payload["results"]],
            ["USA-CA-037", "USA-NY-061", "USA-CA-037"],
        )

    def test_loc_id_info_rejects_ambiguous_preferred_public_alias(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.resolve_loc_id_input",
                return_value={
                    "ok": False,
                    "requested_loc_id": "USA-PLACE-SPRINGFIELD",
                    "loc_id": None,
                    "candidate_loc_ids": ["USA-IL-167-PLACE-12345", "USA-MO-077-PLACE-67890"],
                    "error": {"code": "ambiguous_public_loc_id", "message": "ambiguous"},
                },
            ),
            mock.patch("mapmover.geometry_handlers.get_location_info") as info_mock,
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_id": "USA-PLACE-SPRINGFIELD"},
            )

        info_mock.assert_not_called()
        self.assertEqual(payload["error"]["code"], "ambiguous_public_loc_id")
        self.assertIsNone(payload["canonical_loc_id"])

    def test_loc_id_info_hierarchy_follows_stored_country_parentage(self) -> None:
        rows = {
            "CAN-BC-5915004": {
                "loc_id": "CAN-BC-5915004",
                "name": "Surrey",
                "admin_level": 3,
                "parent_id": "CAN-BC-5915",
                "family": "admin_boundary",
                "iso3": "CAN",
            },
            "CAN-BC-5915": {
                "loc_id": "CAN-BC-5915",
                "name": "Greater Vancouver",
                "admin_level": 2,
                "parent_id": "CAN-BC",
                "family": "admin_boundary",
                "iso3": "CAN",
            },
            "CAN-BC": {
                "loc_id": "CAN-BC",
                "name": "British Columbia",
                "admin_level": 1,
                "parent_id": "CAN",
                "family": "admin_boundary",
                "iso3": "CAN",
            },
            "CAN": {
                "loc_id": "CAN",
                "name": "Canada",
                "admin_level": 0,
                "parent_id": None,
                "family": "admin_boundary",
                "iso3": "CAN",
            },
        }
        with mock.patch(
            "mapmover.geometry_handlers.get_location_info",
            side_effect=lambda loc_id, **_: rows[loc_id],
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {"loc_id": "CAN-BC-5915004", "include_hierarchy": True},
            )

        self.assertEqual(payload["hierarchy"]["relationship_mode"], "strict_stored_parent")
        self.assertEqual(payload["hierarchy"]["parent"], "CAN-BC-5915")
        self.assertEqual(payload["hierarchy"]["ancestors"], ["CAN-BC-5915", "CAN-BC", "CAN"])

    def test_loc_id_info_references_batch_uses_smaller_guard(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MCP_TOOL_REFERENCES_BATCH_LIMIT_LOC_ID_INFO": "2"}),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "loc_id_info",
                {
                    "loc_ids": ["USA-CA-037", "USA-NY-061", "USA-AK-282"],
                    "include_references": True,
                },
            )

        self.assertEqual(payload["limit"], 2)
        self.assertEqual(payload["error"]["code"], "too_many_loc_ids_for_references")
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["decision"], "deny")
        self.assertEqual(analytics["error_code"], "too_many_loc_ids_for_references")
        self.assertEqual(analytics["metadata"]["batch_limit"], 2)

    def test_tool_rate_limit_uses_per_tool_override(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "MCP_LIVE_TOOL_RATE_LIMIT": "10",
                "MCP_TOOL_RATE_LIMIT_RESOLVE_POINT": "4",
                "MCP_TOOL_RATE_WINDOW_SECONDS_RESOLVE_POINT": "30",
                "MCP_TOOL_RATE_LIMIT_RESOLVE_POINT_PLUS": "40",
            },
        ):
            self.assertEqual(_tool_rate_limit_for_tier("resolve_point", "free"), (4, 30))
            self.assertEqual(_tool_rate_limit_for_tier("resolve_point", "plus"), (40, 30))

    def test_get_pack_geography_prefers_reference_exchange(self) -> None:
        payload = _tool_call(self.client, "get_pack", {"pack_id": "geography"})

        self.assertEqual(payload["routing"]["preferred_tool"], "read_geometry_catalog")
        self.assertEqual(payload["quick_start"]["first_query_template"]["tool"], "read_geometry_catalog")
        starter_tools = set(payload["quick_start"]["starter_tools"])
        self.assertIn("read_geometry_catalog", starter_tools)
        self.assertIn("list_reference_systems", starter_tools)
        self.assertIn("resolve_reference", starter_tools)
        self.assertIn("convert_reference", starter_tools)

    def test_read_geometry_catalog_returns_agent_summary(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={
                "schema_version": "1.1.0",
                "generated_at": "2026-08-03T18:25:18Z",
                "geometry_families": [{"family": "admin_boundary", "label": "Admin", "feature_count": 10}],
                "geometry_products": [
                    {
                        "product_id": "global_admin_spine",
                        "label": "Global Admin Spine",
                        "scope": "Global",
                        "family": "admin_base",
                        "feature_count": 10,
                        "has_shapes": True,
                        "admin_coverage": {
                            "min_admin_level": 0,
                            "max_admin_level": 2,
                            "levels": [{"admin_level": "admin_2", "label": "county", "row_count": 10}],
                        },
                    }
                ],
                "crosswalk_artifacts": [{"source_family": "overlay_zcta", "status": "complete"}],
                "geometry_collections": [],
                "release_packages": [],
                "resolver_groups": [],
                "named_reference_objects": [],
            },
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "summary"})

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["view"], "summary")
        self.assertEqual(payload["schema_version"], "1.1.0")
        self.assertEqual(payload["counts"]["geometry_products"], 1)
        self.assertEqual(payload["admin_coverage"][0]["product_id"], "global_admin_spine")
        self.assertEqual(payload["app_summary_endpoint"], "https://app.daedalmap.com/api/v1/geometry/catalog")
        self.assertEqual(payload["catalog_path"], "geometry/geometry_catalog.json")
        self.assertNotIn("download_url", payload)

    def test_read_geometry_catalog_returns_concise_capabilities(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={
                "schema_version": "1.1.0",
                "global_admin_baseline": [
                    {"country_code": "BRA", "max_admin_level": 2},
                    {"country_code": "AUS", "max_admin_level": 2},
                ],
                "country_family_coverage": [{
                    "country_code": "AUS",
                    "label": "Australia",
                    "active_admin_depth": 6,
                    "available_family_ids": ["administrative"],
                }],
            },
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "capabilities"})

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["view"], "capabilities")
        self.assertEqual(payload["capabilities"]["enhanced_country_codes"], ["AUS"])
        self.assertEqual(payload["capabilities"]["global_baseline"]["geographic_entity_count"], 2)
        self.assertNotIn("candidate_countries", payload["capabilities"])
        self.assertNotIn("country_programs", payload["capabilities"])

    def test_read_geometry_catalog_country_view_is_catalog_driven(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={
                "schema_version": "1.1.1",
                "country_profiles": [{
                    "country_code": "NZL",
                    "label": "New Zealand",
                    "release_status": "published",
                    "release_version": "1.0.0",
                    "admin_levels": [{"level": 0}, {"level": 3}],
                    "query_layout_manifest": "geometry/countries/NZL/releases/geometry/r/runtime/admin_spine/manifest.json",
                    "reference_graph_manifest": "geometry/countries/NZL/releases/geometry/r/runtime/reference_graph/manifest.json",
                }],
                "country_family_coverage": [{
                    "country_code": "NZL",
                    "active_admin_depth": 3,
                    "available_family_ids": ["administrative", "place_or_municipality"],
                    "complete_family_ids": ["administrative"],
                    "admin_hierarchy_coverage_status": "complete",
                    "admin_hierarchy_coverage_complete": True,
                    "admin_hierarchy_node_count": 1234,
                    "families": [{
                        "family_id": "place_or_municipality",
                        "label": "Places",
                        "available": True,
                        "publication_status": "published",
                        "coverage_status": "partial",
                        "coverage_complete": False,
                        "coverage_basis": "partial_children",
                        "unresolved_jurisdictions": ["STL"],
                    }],
                }],
            },
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "countries"})

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["counts"]["country_profiles"], 1)
        self.assertEqual(payload["countries"][0]["country_code"], "NZL")
        self.assertEqual(payload["countries"][0]["active_admin_depth"], 3)
        self.assertEqual(
            payload["countries"][0]["available_family_ids"],
            ["administrative", "place_or_municipality"],
        )
        self.assertTrue(payload["countries"][0]["query_layout_available"])
        self.assertTrue(payload["countries"][0]["reference_graph_available"])
        self.assertEqual(payload["countries"][0]["complete_family_ids"], ["administrative"])
        self.assertTrue(payload["countries"][0]["admin_hierarchy_coverage_complete"])
        self.assertEqual(payload["countries"][0]["families"][0]["coverage_status"], "partial")
        self.assertEqual(payload["countries"][0]["families"][0]["unresolved_jurisdictions"], ["STL"])

    def test_read_geometry_catalog_full_view_does_not_expose_internal_country_candidates(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={
                "schema_version": "1.1.0",
                "global_admin_baseline": [{"country_code": "FRA", "max_admin_level": 2}],
                "country_family_coverage": [{
                    "country_code": "FRA",
                    "candidate_admin_depth": 4,
                    "candidate_admin_status": "ready_for_acquisition",
                }],
            },
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "full"})

        self.assertTrue(payload["ok"])
        self.assertNotIn("country_family_coverage", payload["catalog"])
        self.assertNotIn("candidate_countries", payload["catalog"]["capability_summary"])

    def test_read_geometry_catalog_filters_candidate_products(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.load_geometry_catalog",
            return_value={
                "geometry_products": [
                    {"product_id": "active", "release_state": "published", "admin_coverage": {}},
                    {"product_id": "internal", "release_state": "candidate_blocked", "admin_coverage": {}},
                    {"product_id": "local_candidate", "release_state": "adopted_local_candidate", "admin_coverage": {}},
                ],
            },
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "products"})

        self.assertEqual([item["product_id"] for item in payload["products"]], ["active"])
        self.assertEqual(payload["counts"]["geometry_products"], 1)
        self.assertEqual(payload["catalog_surface"], "published")

    def test_read_geometry_catalog_allows_wip_projection_only_for_local_loopback(self) -> None:
        catalog = {
            "geometry_products": [
                {"product_id": "active", "release_state": "published", "admin_coverage": {}},
                {"product_id": "internal", "release_state": "candidate_blocked", "admin_coverage": {}},
                {"product_id": "local_candidate", "release_state": "adopted_local_candidate", "admin_coverage": {}},
            ],
        }
        with (
            mock.patch("mapmover.routes.mcp.is_local_loopback_request", return_value=True),
            mock.patch("mapmover.runtime.reference_exchange.load_geometry_catalog", return_value=catalog),
        ):
            payload = _tool_call(
                self.client,
                "read_geometry_catalog",
                {"view": "products", "read_wip": True},
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["catalog_surface"], "wip")
        self.assertEqual(
            [item["product_id"] for item in payload["products"]],
            ["active", "internal", "local_candidate"],
        )
        self.assertEqual(payload["counts"]["geometry_products"], 3)

    def test_read_geometry_catalog_denies_wip_projection_for_hosted_callers(self) -> None:
        with (
            mock.patch("mapmover.routes.mcp.is_local_loopback_request", return_value=False),
            mock.patch("mapmover.runtime.reference_exchange.read_geometry_catalog") as reader,
        ):
            payload = _tool_call(
                self.client,
                "read_geometry_catalog",
                {"view": "full", "read_wip": True},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["catalog_surface"], "published")
        self.assertEqual(payload["error"]["code"], "wip_geometry_catalog_not_available")
        reader.assert_not_called()

    def test_read_geometry_catalog_logs_runtime_analytics(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.read_geometry_catalog",
                return_value={
                    "ok": True,
                    "view": "products",
                    "counts": {"geometry_products": 3, "geometry_banks": 2},
                    "products": [],
                },
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(self.client, "read_geometry_catalog", {"view": "products"})

        self.assertTrue(payload["ok"])
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["source_id"], "read_geometry_catalog")
        self.assertEqual(analytics["capability_id"], "geometry_catalog_discovery")
        self.assertEqual(analytics["row_count"], 3)
        self.assertEqual(analytics["metadata"]["event"], "geometry_catalog_discovery")
        self.assertEqual(analytics["metadata"]["view"], "products")
        self.assertEqual(analytics["metadata"]["compute"]["input_count"], 1)
        self.assertEqual(analytics["metadata"]["compute"]["output_count"], 3)

    def test_list_reference_systems_logs_runtime_analytics(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.list_reference_systems",
                return_value={
                    "ok": True,
                    "systems": [{"system": "daedalmap.loc_id"}, {"system": "overlay_zcta"}],
                    "crosswalk_artifacts": [],
                },
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(self.client, "list_reference_systems")

        self.assertTrue(payload["ok"])
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["source_id"], "list_reference_systems")
        self.assertEqual(analytics["capability_id"], "reference_system_discovery")
        self.assertEqual(analytics["row_count"], 2)
        self.assertEqual(analytics["metadata"]["event"], "reference_system_discovery")
        self.assertEqual(analytics["metadata"]["system_count"], 2)
        self.assertEqual(analytics["metadata"]["compute"]["input_count"], 1)
        self.assertEqual(analytics["metadata"]["compute"]["output_count"], 2)
        self.assertIn("catalog_lookup_ms", analytics["metadata"]["compute"]["stage_ms"])

    def test_list_reference_systems_denies_wip_on_hosted_request(self) -> None:
        payload = _tool_call(self.client, "list_reference_systems", {"read_wip": True})

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "wip_crosswalk_catalog_not_available")

    def test_list_reference_systems_allows_wip_on_local_loopback(self) -> None:
        app = FastAPI()
        app.include_router(mcp_router)
        local_client = TestClient(app, client=("127.0.0.1", 50000))
        expected = {"ok": True, "systems": [], "crosswalks": [{"crosswalk_id": "candidate"}]}
        with mock.patch(
            "mapmover.runtime.reference_exchange.list_reference_systems", return_value=expected,
        ) as listing, mock.patch(
            "mapmover.routes.mcp.is_local_loopback_request", return_value=True,
        ):
            payload = _tool_call(
                local_client,
                "list_reference_systems",
                {"country_scope": "CAN", "include_crosswalks": True, "read_wip": True},
            )

        self.assertTrue(payload["ok"], payload)
        self.assertEqual(payload["systems"], [])
        self.assertEqual(payload["crosswalks"], expected["crosswalks"])
        listing.assert_called_once_with(country_scope="CAN", include_crosswalks=True, read_wip=True)

    def test_resolve_reference_tool_resolves_zip_to_loc_id(self) -> None:
        payload = _tool_call(
            self.client,
            "resolve_reference",
            {
                "from_system": "zip",
                "value": "00601",
                "target_admin_level": "admin_2",
                "limit": 2,
            },
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["normalized_input"], "USA-Z-00601")
        self.assertEqual(payload["resolved_loc_id"], "USA-PR-001")
        self.assertEqual(payload["match_type"], "crosswalk_overlap")

    def test_resolve_reference_tool_selects_historical_identity_as_of_date(self) -> None:
        payload = _tool_call(
            self.client,
            "resolve_reference",
            {"from_system": "iso3166_3", "value": "YUG", "as_of": "2025"},
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["resolved_loc_id"], "HIST-YUG-FRY")
        self.assertFalse(payload["valid_at_requested_time"])
        self.assertEqual(
            {row["loc_id"] for row in payload["lifecycle"]["present_day_descendants"]},
            {"SRB", "MNE"},
        )

    def test_resolve_reference_tool_accepts_item_batch(self) -> None:
        payload = _tool_call(
            self.client,
            "resolve_reference",
            {
                "batch_id": "refs-1",
                "from_system": "zip",
                "target_admin_level": "admin_2",
                "items": [
                    {"row_index": 1, "value": "00601"},
                    {"row_index": 2, "value": "not-a-real-zcta"},
                ],
            },
        )

        self.assertEqual(payload["batch_id"], "refs-1")
        self.assertEqual(payload["item_count"], 2)
        self.assertEqual(payload["results"][0]["row_index"], 1)
        self.assertTrue(payload["results"][0]["ok"])
        self.assertEqual(payload["results"][0]["resolved_loc_id"], "USA-PR-001")
        self.assertEqual(payload["resolved_count"], 1)
        self.assertEqual(payload["unresolved_count"], 1)
        # The real analytics rows carry compute.input_count/output_count and
        # crosswalk_lookup_ms; other tests assert the shared shape with mocks.

    def test_resolve_reference_batch_uses_one_set_based_runtime_call(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.resolve_references_batch",
            return_value=[{"ok": True, "resolved_loc_id": "USA-PR-001"}],
        ) as batch_mock:
            payload = _tool_call(
                self.client,
                "resolve_reference",
                {"from_system": "zip", "items": [{"value": "00601"}]},
            )

        self.assertEqual(payload["resolved_count"], 1)
        batch_mock.assert_called_once()
        self.assertEqual(batch_mock.call_args.args[0][0]["value"], "00601")

    def test_resolve_reference_tool_uses_per_tool_batch_limit_override(self) -> None:
        with mock.patch.dict("os.environ", {"MCP_TOOL_BATCH_LIMIT_RESOLVE_REFERENCE": "1"}):
            payload = _tool_call(
                self.client,
                "resolve_reference",
                {"from_system": "zip", "items": [{"value": "00601"}, {"value": "00602"}]},
            )

        self.assertEqual(payload["limit"], 1)
        self.assertEqual(payload["item_count"], 2)
        self.assertEqual(payload["error"]["code"], "paid_bulk_unavailable")
        self.assertEqual(payload["limits"], {"free_batch_limit": 1, "paid_batch_limit": 2500})

    def test_resolve_reference_tool_normalizes_string_error(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.resolve_reference",
                return_value={"ok": False, "from_system": "zip", "input": "not-real", "error": "no crosswalk artifact found"},
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "resolve_reference",
                {"from_system": "zip", "value": "not-real", "target_admin_level": "admin_2"},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "not_found")
        self.assertEqual(payload["error"]["message"], "no crosswalk artifact found")
        self.assertEqual(analytics_mock.call_args.kwargs["error_code"], "not_found")

    def test_convert_reference_tool_composes_through_loc_id(self) -> None:
        payload = _tool_call(
            self.client,
            "convert_reference",
            {
                "from_system": "zip",
                "value": "00601",
                "to_system": "nws_fire",
                "target_admin_level": "admin_2",
                "limit": 2,
            },
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["loc_id"], "USA-PR-001")
        self.assertTrue(payload["results"])
        self.assertEqual(payload["results"][0]["system"], "overlay_nws_fire_weather_zone")

    def test_convert_reference_tool_accepts_item_batch(self) -> None:
        payload = _tool_call(
            self.client,
            "convert_reference",
            {
                "batch_id": "conversions-1",
                "from_system": "zip",
                "to_system": "nws_fire",
                "target_admin_level": "admin_2",
                "items": [
                    {"row_index": "a", "value": "00601"},
                    {"row_index": "b", "value": ""},
                ],
            },
        )

        self.assertEqual(payload["batch_id"], "conversions-1")
        self.assertEqual(payload["item_count"], 2)
        self.assertEqual(payload["results"][0]["row_index"], "a")
        self.assertTrue(payload["results"][0]["ok"])
        self.assertEqual(payload["results"][0]["loc_id"], "USA-PR-001")
        self.assertEqual(payload["converted_count"], 1)
        self.assertEqual(payload["unconverted_count"], 1)

    def test_convert_reference_batch_uses_one_set_based_runtime_call(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_exchange.convert_references_batch",
            return_value=[{"ok": True, "loc_id": "USA-PR-001", "results": []}],
        ) as batch_mock:
            payload = _tool_call(
                self.client,
                "convert_reference",
                {
                    "from_system": "zip",
                    "to_system": "nws_fire",
                    "items": [{"value": "00601"}],
                },
            )

        self.assertEqual(payload["converted_count"], 1)
        batch_mock.assert_called_once()
        self.assertEqual(batch_mock.call_args.args[0][0]["value"], "00601")

    def test_convert_reference_tool_normalizes_string_error(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.reference_exchange.convert_reference",
                return_value={"ok": False, "from_system": "zip", "input": "not-real", "to_system": "nws_fire", "error": "no crosswalk artifact found"},
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "convert_reference",
                {"from_system": "zip", "value": "not-real", "to_system": "nws_fire", "target_admin_level": "admin_2"},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "not_found")
        self.assertEqual(payload["error"]["message"], "no crosswalk artifact found")
        self.assertEqual(analytics_mock.call_args.kwargs["error_code"], "not_found")

    def test_convert_reference_tool_rejects_empty_target_results(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            payload = _tool_call(
                self.client,
                "convert_reference",
                {
                    "from_system": "zip",
                    "value": "10001",
                    "to_system": "huc",
                    "target_admin_level": "admin_2",
                    "limit": 2,
                },
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "unsupported_target_system")
        self.assertEqual(analytics_mock.call_args.kwargs["decision"], "deny")
        self.assertEqual(analytics_mock.call_args.kwargs["error_code"], "unsupported_target_system")

    def test_compare_geographies_tool_returns_spatial_and_temporal_relationship(self) -> None:
        expected = {
            "ok": True,
            "temporal_relation": "coexistent",
            "spatial_relation": "overlaps",
            "left_area_share": 0.18,
            "right_area_share": 0.03,
        }
        with (
            mock.patch("mapmover.runtime.geography_relationships.compare_geographies", return_value=expected) as compare_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "compare_geographies",
                {"left_loc_id": "USA-Z-90001", "right_loc_id": "USA-TRIBAL-1823", "as_of": "2025"},
            )

        self.assertEqual(payload["spatial_relation"], "overlaps")
        self.assertEqual(payload["left_area_share"], 0.18)
        compare_mock.assert_called_once_with(
            "USA-Z-90001",
            "USA-TRIBAL-1823",
            as_of="2025",
            left_as_of=None,
            right_as_of=None,
            include_successors=True,
        )
        self.assertEqual(analytics_mock.call_args.kwargs["capability_id"], "geography_comparison")

    def test_compare_geographies_tool_accepts_pair_batch(self) -> None:
        with mock.patch(
            "mapmover.runtime.geography_relationships.compare_geographies_batch",
            return_value=[
                {"ok": True, "spatial_relation": "disjoint"},
                {"ok": True, "spatial_relation": "disjoint"},
            ],
        ) as compare_mock:
            payload = _tool_call(
                self.client,
                "compare_geographies",
                {
                    "batch_id": "relations-1",
                    "items": [
                        {"id": "one", "left_loc_id": "USA-Z-90001", "right_loc_id": "USA-TRIBAL-1823"},
                        {"id": "two", "left_loc_id": "USA-Z-10001", "right_loc_id": "USA-TRIBAL-1823"},
                    ],
                },
            )

        self.assertEqual(payload["batch_id"], "relations-1")
        self.assertEqual(payload["item_count"], 2)
        self.assertEqual(payload["compared_count"], 2)
        self.assertEqual([row["row_index"] for row in payload["results"]], ["one", "two"])
        compare_mock.assert_called_once()
        self.assertEqual(len(compare_mock.call_args.args[0]), 2)

    def test_resolve_loc_id_scope_uses_geometry_index(self) -> None:
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.query_descendant_scope", return_value=None),
            mock.patch(
                "mapmover.runtime.geometry_tool_jobs.get_geometry_index",
                return_value={
                    "rows": [
                        {
                            "loc_id": "USA-CA-037",
                            "parent_id": "USA-CA",
                            "admin_level": 2,
                            "name": "Los Angeles County",
                            "bbox_min_lon": -119,
                            "bbox_min_lat": 33,
                            "bbox_max_lon": -117,
                            "bbox_max_lat": 35,
                            "centroid_lon": -118.25,
                            "centroid_lat": 34.05,
                        },
                        {"loc_id": "USA-CA-075", "parent_id": "USA-CA", "admin_level": 2, "name": "San Francisco County"},
                    ],
                    "count": 2,
                },
            ) as index_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_loc_id_scope",
                {"parent_loc_id": "USA-CA", "admin_level": "admin_2", "limit": 1},
            )

        index_mock.assert_called_once()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["returned_count"], 1)
        self.assertTrue(payload["truncated"])
        self.assertEqual(payload["loc_ids"], ["USA-CA-037"])

    def test_resolve_loc_id_scope_expands_country_to_counties(self) -> None:
        pd = __import__("pandas")
        base_rows = pd.DataFrame(
            [
                {"loc_id": "USA-MN-001", "parent_id": "USA-MN", "admin_level": 2, "name": "Aitkin County"},
                {"loc_id": "USA-MN-003", "parent_id": "USA-MN", "admin_level": 2, "name": "Anoka County"},
                {"loc_id": "USA-WY-001", "parent_id": "USA-WY", "admin_level": 2, "name": "Albany County"},
            ]
        )

        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.query_descendant_scope", return_value=None),
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_index", return_value={"rows": [], "count": 0}) as index_mock,
            mock.patch("mapmover.runtime.geometry_tool_jobs.load_country_parquet", return_value=base_rows) as base_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_loc_id_scope",
                {"parent_loc_id": "USA", "admin_level": "admin_2", "limit": 2},
            )

        index_mock.assert_called_once()
        base_mock.assert_called_once()
        self.assertEqual(base_mock.call_args.kwargs["admin_level"], 2)
        self.assertIn("loc_id", base_mock.call_args.kwargs["columns"])
        self.assertNotIn("geometry", base_mock.call_args.kwargs["columns"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["total_count"], 3)
        self.assertEqual(payload["returned_count"], 2)
        self.assertTrue(payload["truncated"])
        self.assertEqual(payload["loc_ids"], ["USA-MN-001", "USA-MN-003"])

    def test_resolve_loc_id_scope_pages_one_admin1_owned_deep_bank(self) -> None:
        direct = {
            "rows": [
                {"loc_id": "USA-TX-001-950100-1-000", "parent_id": "USA-TX-001-950100-1", "admin_level": 5, "name": "Block 1000"},
            ],
            "total_count": 668757,
            "single_bank": True,
        }
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_country_supported_deep_admin_levels", return_value=[4, 5]),
            mock.patch("mapmover.runtime.geometry_tool_jobs.query_descendant_scope", return_value=direct) as scope_mock,
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_index") as legacy_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_loc_id_scope",
                {"parent_loc_id": "USA-TX", "admin_level": "admin_5", "limit": 1},
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["total_count"], 668757)
        self.assertEqual(payload["returned_count"], 1)
        self.assertTrue(payload["truncated"])
        scope_mock.assert_called_once_with(
            "USA", "USA-TX", 5, bbox=None, limit=1, offset=0, count_only=False,
        )
        legacy_mock.assert_not_called()

    def test_resolve_loc_id_scope_rejects_unsupported_deep_country_level(self) -> None:
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_country_supported_deep_admin_levels", return_value=[]),
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_index") as index_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_loc_id_scope",
                {"parent_loc_id": "NGA", "admin_level": "admin_4", "limit": 5},
            )

        index_mock.assert_not_called()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "unsupported_admin_level")

    def test_resolve_loc_id_scope_rejects_too_broad_deep_country_level(self) -> None:
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_country_supported_deep_admin_levels", return_value=[3, 4]),
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_index") as index_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            payload = _tool_call(
                self.client,
                "resolve_loc_id_scope",
                {"parent_loc_id": "USA", "admin_level": "admin_4", "limit": 5},
            )

        index_mock.assert_not_called()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "scope_too_broad")

    def test_estimate_geometry_package_uses_availability_preflight(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.geometry_tool_jobs.get_geometry_availability",
                return_value={"ok": True, "requested": 2, "available": 1, "missing": 1, "items": []},
            ) as availability_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            payload = _tool_call(
                self.client,
                "estimate_geometry_package",
                {"loc_ids": ["USA-CA-037", "USA-NOPE"], "format": "geojson_gzip", "include_polygon": True},
            )

        availability_mock.assert_called_once_with(["USA-CA-037", "USA-NOPE"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["loc_id_count"], 2)
        self.assertEqual(payload["available_shape_count"], 1)
        self.assertEqual(payload["missing_shape_count"], 1)
        self.assertEqual(payload["create_call"]["tool"], "create_geometry_export")
        self.assertEqual(analytics_mock.call_args.kwargs["capability_id"], "geometry_package_estimate")
        self.assertEqual(analytics_mock.call_args.kwargs["metadata"]["compute"]["input_count"], 2)
        self.assertEqual(analytics_mock.call_args.kwargs["metadata"]["compute"]["estimated_transfer_bytes"], payload["estimated_transfer_bytes"])
        self.assertIn("runtime_ms", analytics_mock.call_args.kwargs["metadata"]["compute"]["stage_ms"])

    def test_create_geometry_export_inline_then_status(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.geometry_tool_jobs.get_geometry_references",
                return_value={"ok": True, "requested": 1, "available": 1, "missing": 0, "results": [{"loc_id": "USA-CA-037", "has_shape": True}]},
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_geometry_export",
                {"loc_ids": ["USA-CA-037"], "include_polygon": False},
            )
            status = _tool_call(self.client, "get_job_status", {"job_id": created["job_id"]})

        self.assertTrue(created["ok"])
        self.assertEqual(created["status"], "completed")
        self.assertEqual(created["result"]["delivery_mode"], "inline")
        self.assertEqual(created["next_call"]["tool"], "get_job_status")
        self.assertEqual(created["next_call"]["arguments"]["job_id"], created["job_id"])
        self.assertEqual(status["job_id"], created["job_id"])
        self.assertEqual(status["status"], "completed")

    def test_hosted_geometry_export_uses_estimate_quote_and_settles_actual_meter(self) -> None:
        reserved_quote = {
            "quote_id": "geoquote_test",
            "tool_name": "create_geometry_export",
            "capability_id": "geometry_export",
            "pricing_version": "test-v1",
            "quantity": 2,
            "charge_units": 2,
            "amount_usdc_base_units": 18000,
        }
        estimate = {"ok": True, "quote_id": "geoquote_test", "quote": reserved_quote}
        allow = (
            "allow",
            {
                "status": "allow",
                "context": {"request_fingerprint": "fp-1", "caller_binding": "caller-1"},
                "settlement": {"settlement_id": "settle-1"},
            },
        )
        geometry_result = {
            "ok": True,
            "requested": 2,
            "available": 1,
            "missing": 1,
            "results": [{"ok": True, "loc_id": "USA-CA-037", "has_shape": True}],
        }
        with mock.patch.dict("os.environ", {"COMMERCIAL_ACCESS_ENABLED": "1"}, clear=False):
            with (
                mock.patch("mapmover.routes.mcp._tool_paid_bulk_enforced", return_value=True),
                mock.patch("mapmover.runtime.geometry_tool_jobs.estimate_geometry_package", return_value=estimate),
                mock.patch("mapmover.routes.mcp._commercial_access_decision", return_value=allow) as authorize_mock,
                mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_references", return_value=geometry_result),
                mock.patch(
                    "mapmover.routes.mcp.settle_commercial_access",
                    return_value=(True, {"status": "allow", "context": {"account_credit": {"charged_micro_usd": 14000}}}),
                ) as settle_mock,
                mock.patch("mapmover.routes.mcp.log_api_query_event"),
            ):
                created = _tool_call(
                    self.client,
                    "create_geometry_export",
                    {"loc_ids": ["USA-CA-037", "USA-NOPE"], "include_polygon": True},
                )

        self.assertTrue(created["ok"])
        self.assertEqual(authorize_mock.call_args.kwargs["pricing_quote"], reserved_quote)
        actual = settle_mock.call_args.kwargs["actual_pricing"]
        self.assertEqual(actual["amount_usdc_base_units"], 14000)
        self.assertEqual(settle_mock.call_args.kwargs["meter_receipt"]["successful_items"], 1)

    def test_hosted_create_challenge_does_not_execute(self) -> None:
        estimate = {
            "ok": True,
            "quote_id": "convquote_test",
            "quote": {
                "quote_id": "convquote_test",
                "tool_name": "create_conversion_job",
                "capability_id": "conversion_job",
                "pricing_version": "test-v1",
                "quantity": 1,
                "charge_units": 1,
                "amount_usdc_base_units": 12000,
            },
        }
        challenge = ("challenge", {"status": "challenge", "context": {"pricing": estimate["quote"]}, "challenge": {"opaque": True}})
        with mock.patch.dict("os.environ", {"COMMERCIAL_ACCESS_ENABLED": "1"}, clear=False):
            with (
                mock.patch("mapmover.routes.mcp._tool_paid_bulk_enforced", return_value=True),
                mock.patch("mapmover.runtime.geometry_tool_jobs.estimate_conversion_job", return_value=estimate),
                mock.patch("mapmover.routes.mcp._commercial_access_decision", return_value=challenge),
                mock.patch("mapmover.runtime.geometry_tool_jobs.create_conversion_job") as execute_mock,
                mock.patch("mapmover.routes.mcp.log_api_query_event"),
            ):
                payload = _tool_call(
                    self.client,
                    "create_conversion_job",
                    {"items": [{"value": "00601"}]},
                )
        self.assertTrue(payload["payment_required"])
        self.assertEqual(payload["error"]["code"], "payment_required")
        execute_mock.assert_not_called()

    def test_large_geometry_export_returns_explicit_v0_limit(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "create_geometry_export",
                {"loc_ids": [f"USA-TEST-{index:03d}" for index in range(251)], "format": "geojson"},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "bounded_inline_limit_exceeded")
        self.assertEqual(payload["inline_limit"], 250)
        self.assertEqual(payload["guidance"]["action"], "use_download_or_custom_builder")
        self.assertFalse(payload["clarification"]["required"])

    def test_geometry_export_format_is_real_geojson_gzip(self) -> None:
        geometry_result = {
            "ok": True,
            "requested": 1,
            "available": 1,
            "missing": 0,
            "results": [{"ok": True, "loc_id": "USA-CA-037", "name": "Los Angeles", "has_shape": True, "geometry": {"type": "Point", "coordinates": [-118.2, 34.0]}}],
        }
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.get_geometry_references", return_value=geometry_result),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_geometry_export",
                {"loc_ids": ["USA-CA-037"], "format": "geojson_gzip", "output_name": "la-shape"},
            )

        artifact = created["artifact"]
        decoded = gzip.decompress(base64.b64decode(artifact["content_base64"]))
        feature_collection = json.loads(decoded)
        self.assertEqual(artifact["filename"], "la-shape.geojson.gz")
        self.assertEqual(feature_collection["type"], "FeatureCollection")
        self.assertEqual(feature_collection["features"][0]["properties"]["loc_id"], "USA-CA-037")

    def test_geometry_export_retry_is_idempotent_by_request_id(self) -> None:
        with (
            mock.patch(
                "mapmover.runtime.geometry_tool_jobs.get_geometry_references",
                return_value={"ok": True, "requested": 1, "available": 1, "missing": 0, "results": []},
            ),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            arguments = {
                "request_id": "geometry-export-idempotency-test",
                "loc_ids": ["USA-CA-037"],
                "include_polygon": False,
            }
            first = _tool_call(self.client, "create_geometry_export", arguments)
            retry = _tool_call(self.client, "create_geometry_export", arguments)
            conflict = _tool_call(
                self.client,
                "create_geometry_export",
                {**arguments, "loc_ids": ["USA-NY-061"]},
            )

        self.assertEqual(retry["job_id"], first["job_id"])
        self.assertEqual(retry["created_at"], first["created_at"])
        self.assertEqual(retry["next_call"], first["next_call"])
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["error"]["code"], "idempotency_conflict")

    def test_conversion_estimate_and_inline_create(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            estimate = _tool_call(
                self.client,
                "estimate_conversion_job",
                {"from_system": "zip", "target_admin_level": "admin_2", "items": [{"value": "00601"}, {"value": "not-real"}]},
            )
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {"from_system": "zip", "target_admin_level": "admin_2", "items": [{"row_index": 1, "value": "00601"}]},
            )

        self.assertTrue(estimate["ok"])
        self.assertEqual(estimate["row_count"], 2)
        self.assertEqual(estimate["create_call"]["tool"], "create_conversion_job")
        self.assertTrue(created["ok"])
        self.assertEqual(created["status"], "completed")
        self.assertEqual(created["next_call"]["arguments"]["job_id"], created["job_id"])
        self.assertEqual(created["result"]["row_count"], 1)
        self.assertEqual(created["result"]["converted_count"], 1)

    def test_large_conversion_returns_explicit_v0_limit(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "create_conversion_job",
                {"from_system": "zip", "items": [{"row_index": index, "value": "00601"} for index in range(7501)]},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "bounded_inline_limit_exceeded")
        self.assertEqual(payload["inline_limit"], 7500)
        self.assertEqual(payload["guidance"]["action"], "use_download_or_custom_builder")

    def test_conversion_csv_preserves_spreadsheet_columns_and_adds_loc_id(self) -> None:
        fake = {
            "ok": True,
            "resolved_loc_id": "USA-CA-073-000100",
            "resolved_family": "admin_boundary",
            "admin_level": "admin_3",
            "match_type": "exact_identifier_crosswalk",
            "source_vintage": "census_2020",
        }
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[fake]),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "from_system": "census_geoid",
                    "items": [{"row_index": 7, "value": "06073000100", "data": {"population": 1234, "label": "Tract A"}}],
                    "output_format": "csv",
                    "output_name": "cleaned-census",
                },
            )

        artifact = created["artifact"]
        self.assertEqual(artifact["filename"], "cleaned-census.csv")
        self.assertIn("population", artifact["content"])
        self.assertIn("loc_id", artifact["content"])
        self.assertIn("USA-CA-073-000100", artifact["content"])
        self.assertIsNone(created["result"]["output_rows"])

    def test_conversion_json_rows_preserves_spreadsheet_columns(self) -> None:
        fake = {
            "ok": True,
            "resolved_loc_id": "USA-CA-073-000100",
            "resolved_family": "admin_boundary",
            "admin_level": "admin_3",
            "match_type": "exact_identifier_crosswalk",
            "source_vintage": "census_2020",
        }
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[fake]),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "from_system": "census_geoid",
                    "items": [{"value": "06073000100", "data": {"population": 1234}}],
                    "output_format": "json_rows",
                },
            )

        row = created["result"]["output_rows"][0]
        self.assertEqual(row["population"], 1234)
        self.assertEqual(row["loc_id"], "USA-CA-073-000100")
        self.assertEqual(row["admin_level"], "admin_3")
        self.assertEqual(set(row), {"population", "row_index", "loc_id", "admin_level"})
        self.assertIsNone(created["artifact"])

    def test_large_conversion_coalesces_distinct_crosswalk_requests(self) -> None:
        items = [{"value": f"postal-{index:03d}"} for index in range(25)]
        batched = [
            {"ok": True, "resolved_loc_id": f"USA-CA-{index:03d}", "match_type": "crosswalk_overlap"}
            for index in range(25)
        ]
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=batched) as batch_mock,
            mock.patch("mapmover.runtime.geometry_tool_jobs._run_conversion_row") as single_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {"from_system": "postal_area", "items": items, "output_format": "json_rows"},
            )

        self.assertEqual(created["result"]["converted_count"], 25)
        batch_mock.assert_called_once()
        single_mock.assert_not_called()

    def test_conversion_parquet_is_readable(self) -> None:
        import pyarrow.parquet as pq

        fake = {"ok": True, "resolved_loc_id": "USA-CA-073-000100"}
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[fake]),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "from_system": "census_geoid",
                    "items": [{"value": "06073000100", "data": {"population": 1234}}],
                    "output_format": "parquet",
                },
            )

        artifact = created["artifact"]
        table = pq.read_table(io.BytesIO(base64.b64decode(artifact["content_base64"])))
        row = table.to_pylist()[0]
        self.assertEqual(row["population"], 1234)
        self.assertEqual(row["loc_id"], "USA-CA-073-000100")

    def test_identify_reference_system_and_bound_conversion_deduplicate_geoids(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            identified = _tool_call(
                self.client,
                "identify_reference_system",
                {
                    "identifiers": ["06073000100", "06073000201"],
                    "expected": {"system": "census_geoid", "geo_level": "tract", "vintage": "2020"},
                    "country_scope": "USA",
                },
            )
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "geography_binding": identified["recommended_binding"],
                    "items": [
                        {"row_index": 1, "value": "06073000100"},
                        {"row_index": 2, "value": "06073000100"},
                        {"row_index": 3, "value": "06073000201"},
                    ],
                },
            )

        self.assertEqual(identified["status"], "matched")
        self.assertNotIn("geometry_available_count", identified["candidates"][0])
        self.assertEqual(analytics_mock.call_args_list[0].kwargs["capability_id"], "reference_system_identification")
        self.assertEqual(created["result"]["distinct_geography_count"], 2)
        self.assertEqual(created["result"]["converted_count"], 3)
        self.assertTrue(created["result"]["resolution_plan"]["deduplicate_by_identifier"])
        self.assertEqual(created["result"]["results"][0]["resolved_loc_id"], "USA-CA-073-000100")
        self.assertEqual(created["result"]["results"][1]["row_index"], 2)

    def test_bound_conversion_executes_once_per_distinct_geography(self) -> None:
        fake = {"ok": True, "resolved_loc_id": "USA-CA-073-000100"}
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[fake, fake]) as resolver_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "geography_binding": {"mode": "reference", "system": "census_geoid", "geo_level": "tract", "vintage": "2020", "country_scope": "USA"},
                    "items": [{"value": "06073000100"}, {"value": "06073000100"}, {"value": "06073000201"}],
                },
            )

        resolver_mock.assert_called_once()
        self.assertEqual(len(resolver_mock.call_args.args[0]), 2)
        self.assertEqual(created["result"]["distinct_geography_count"], 2)

    def test_bound_conversion_delivers_partial_success_and_meters_only_matches(self) -> None:
        matched = {
            "ok": True,
            "resolved_loc_id": "USA-NC-185",
            "admin_level": "admin_2",
            "match_type": "admin_spine_exact",
        }
        unmatched = {
            "ok": False,
            "error": {
                "code": "admin_spine_match_not_found",
                "message": "identifier did not match the selected country admin spine",
            },
        }
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[matched, unmatched]) as resolver_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "geography_binding": {
                        "mode": "reference",
                        "system": "census_geoid",
                        "geo_level": "county",
                        "vintage": "2020",
                        "country_scope": "USA",
                    },
                    "items": [
                        {"row_index": 1, "value": "37185", "data": {"name": "Warren"}},
                        {"row_index": 2, "value": "09110", "data": {"name": "Connecticut exception"}},
                    ],
                    "output_format": "json_rows",
                },
            )

        resolver_mock.assert_called_once()
        self.assertTrue(created["ok"])
        self.assertEqual(created["result"]["converted_count"], 1)
        self.assertEqual(created["result"]["error_count"], 1)
        self.assertEqual(created["result"]["meter_receipt"]["successful_items"], 1)
        self.assertEqual(created["result"]["meter_receipt"]["unresolved_items"], 1)
        self.assertEqual(created["result"]["meter_receipt"]["successful_distinct_items"], 1)
        self.assertEqual(created["result"]["output_rows"][0]["loc_id"], "USA-NC-185")
        self.assertIsNone(created["result"]["output_rows"][1]["loc_id"])
        self.assertIn("did not match", created["result"]["output_rows"][1]["error"])

    def test_conversion_meter_charges_successful_rows_after_deduplication(self) -> None:
        matched = {"ok": True, "resolved_loc_id": "USA-NC-185", "admin_level": "admin_2"}
        with (
            mock.patch("mapmover.runtime.geometry_tool_jobs.resolve_references_batch", return_value=[matched]),
            mock.patch("mapmover.routes.mcp.log_api_query_event"),
        ):
            created = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "from_system": "census_geoid",
                    "items": [{"value": "37185"} for _ in range(250)],
                    "output_format": "json_rows",
                },
            )

        meter = created["result"]["meter_receipt"]
        self.assertEqual(meter["successful_items"], 250)
        self.assertEqual(meter["successful_distinct_items"], 1)
        self.assertEqual(meter["duplicate_items_collapsed"], 249)
        self.assertEqual(meter["charge_units"], 3)

    def test_identify_reference_system_enforces_public_identifier_cap(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "identify_reference_system",
                {"identifiers": [f"{index:011d}" for index in range(101)]},
            )

        self.assertEqual(payload["error"]["code"], "too_many_items")
        self.assertEqual(payload["limit"], 100)

    def test_bound_conversion_rejects_unavailable_vintage(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "create_conversion_job",
                {
                    "geography_binding": {"mode": "reference", "system": "census_geoid", "geo_level": "tract", "vintage": "2010", "country_scope": "USA"},
                    "items": [{"value": "06073000100"}],
                },
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "unsupported_geography_binding")

    def test_natural_language_tool_arguments_return_translation_guidance(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "identify_reference_system",
                {"question": "Are these 2020 census tract GEOIDs: 02013000100 and 02016000100?"},
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "identifiers_required")
        self.assertEqual(payload["warnings"][0]["code"], "strict_input_contract")
        self.assertEqual(payload["guidance"]["action"], "translate_then_retry")
        self.assertEqual(payload["clarification"]["questions"][0]["maps_to"], "identifiers")

    def test_conversion_contract_rejects_prose_and_unknown_row_fields(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event"):
            payload = _tool_call(
                self.client,
                "estimate_conversion_job",
                {
                    "from_system": "census_geoid",
                    "question": "Please match my Census data",
                    "items": [{"value": "01001", "population": 58764}],
                },
            )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "invalid_conversion_contract")
        self.assertEqual(payload["error"]["unknown_arguments"], ["question"])
        self.assertEqual(payload["error"]["item_errors"][0]["unknown_arguments"], ["population"])
        self.assertEqual(payload["guidance"]["action"], "translate_then_retry")
        self.assertFalse(payload["clarification"]["required"])


if __name__ == "__main__":
    unittest.main()
