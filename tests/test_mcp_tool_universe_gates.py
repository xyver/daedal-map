"""Cross-family guarantees for the MCP tool universe.

These lock in the unification described in
county-map-private/docs/future/API/tool_universe_contract.md:

- every tool dispatched inside routes/mcp.py writes an api_usage_events row
- all lanes use one access_lane enum shared with the dataset/query lane
- trusted artifact tokens lift item caps on every capped geometry tool
"""

from __future__ import annotations

import asyncio
import json
import unittest
from unittest import mock

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from mapmover.routes.mcp import (
    ACCESS_LANE_FREE,
    ACCESS_LANE_LOCAL_INSTALLED,
    ACCESS_LANE_PAID,
    ACCESS_LANE_TRUSTED_ARTIFACT,
    DATA_HELPER_CAPABILITIES,
    _shape_resolve_point_payload,
    _provenance_summary,
    _access_lane,
    _commercial_denial_details,
    _apply_mcp_payment_mode,
    _execute_paid_tool,
    _required_mcp_permission,
    router as mcp_router,
)
from mapmover.security import is_local_loopback_request


def _tool_call_envelope(
    client: TestClient,
    name: str,
    arguments: dict | None = None,
    *,
    path: str = "/mcp",
    headers: dict | None = None,
) -> dict:
    response = client.post(
        path,
        headers=headers or {},
        json={
            "jsonrpc": "2.0",
            "id": "gate-1",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


class ResolvePointPayloadShapeTests(unittest.TestCase):
    def test_marine_overlap_fields_survive_compact_mcp_shape(self):
        raw = {
            "point": {"lon": -130.0, "lat": 35.0},
            "matched": {"loc_id": "IHO1953-1", "family": "water_body"},
            "stack": [{"loc_id": "IHO1953-1", "family": "water_body"}],
            "deepest_resolved_family": "water_body",
            "overlap_families": [
                {"loc_id": "USA-EEZ-MRGID-1", "family": "marine_jurisdiction", "relationship": "marine_jurisdiction"}
            ],
            "resolution_family": "marine",
        }

        shaped = _shape_resolve_point_payload(raw, "request-1")

        self.assertEqual(shaped["deepest_resolved_family"], "water_body")
        self.assertEqual(shaped["resolution_family"], "marine")
        self.assertEqual(shaped["overlap_families"][0]["loc_id"], "USA-EEZ-MRGID-1")


class McpAccountScopeTests(unittest.TestCase):
    def test_tools_share_a_small_explicit_permission_vocabulary(self):
        self.assertEqual(_required_mcp_permission("get_catalog"), "packs:read")
        self.assertEqual(_required_mcp_permission("resolve_point"), "geometry:read")
        self.assertEqual(_required_mcp_permission("create_geometry_export"), "geometry:bulk")
        self.assertEqual(_required_mcp_permission("query_dataset"), "data:query")

    def test_smart_payment_choice_is_shared_by_paid_tools(self):
        detail = _commercial_denial_details("challenge", {
            "payment_choice_required": True,
            "message": "Choose a rail",
        })
        self.assertEqual(detail["code"], "payment_choice_required")
        self.assertEqual(detail["payment_options"]["account"]["endpoint"], "/mcp/account")
        self.assertEqual(detail["payment_options"]["x402"]["endpoint"], "/mcp/x402")

    def test_dataset_402_uses_the_same_route_choice(self):
        request = self._request_for_mode("smart")
        routed = _apply_mcp_payment_mode(request, {"challenge": {"amount": "quoted"}})
        detail = _commercial_denial_details("challenge", routed)
        self.assertEqual(detail["code"], "payment_choice_required")
        self.assertEqual(detail["challenge"], {"amount": "quoted"})

    def test_named_dataset_402_returns_payment_choices(self):
        request = self._request_for_mode("smart")
        challenge = {
            "daedalmap_pricing": {"price_display": "$0.01"},
            "challenge": {"amount": "10000"},
        }
        with mock.patch(
            "mapmover.routes.mcp.execute_query_dataset_payload",
            new=mock.AsyncMock(return_value=JSONResponse(challenge, status_code=402)),
        ):
            response = asyncio.run(
                _execute_paid_tool(request, "get_earthquake_events", {}, "paid-1")
            )
        result = json.loads(response.body)["result"]
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"]["error"]["code"], "payment_choice_required")
        self.assertEqual(result["structuredContent"]["payment_options"]["account"]["endpoint"], "/mcp/account")
        self.assertEqual(result["structuredContent"]["payment_options"]["x402"]["endpoint"], "/mcp/x402")

    @staticmethod
    def _request_for_mode(mode: str) -> Request:
        request = Request({"type": "http", "method": "POST", "path": "/mcp", "headers": []})
        request.state.mcp_access_mode = mode
        return request


class LocalRuntimeAccessTests(unittest.TestCase):
    @staticmethod
    def _request(client_host: str, *, forwarded_for: str | None = None) -> Request:
        headers = []
        if forwarded_for:
            headers.append((b"x-forwarded-for", forwarded_for.encode("ascii")))
        return Request({
            "type": "http",
            "method": "GET",
            "path": "/mcp/geography",
            "headers": headers,
            "client": (client_host, 12345),
            "server": ("127.0.0.1", 7000),
            "scheme": "http",
            "query_string": b"",
        })

    def test_only_direct_local_loopback_gets_unrestricted_lane(self) -> None:
        with mock.patch("mapmover.security.get_runtime_config", return_value={"runtime_mode": "local"}):
            self.assertTrue(is_local_loopback_request(self._request("127.0.0.1")))
            self.assertFalse(is_local_loopback_request(self._request("10.0.0.8", forwarded_for="127.0.0.1")))
        with mock.patch("mapmover.security.get_runtime_config", return_value={"runtime_mode": "cloud"}):
            self.assertFalse(is_local_loopback_request(self._request("127.0.0.1")))


class AccessLaneEnumTests(unittest.TestCase):
    def test_lane_values_match_the_dataset_query_lane(self) -> None:
        # api_query.execute_query_dataset_payload emits exactly these strings.
        self.assertEqual(ACCESS_LANE_FREE, "free")
        self.assertEqual(ACCESS_LANE_PAID, "paid")
        self.assertEqual(ACCESS_LANE_TRUSTED_ARTIFACT, "trusted_artifact")
        self.assertEqual(ACCESS_LANE_LOCAL_INSTALLED, "local_installed")

    def test_access_lane_resolution(self) -> None:
        self.assertEqual(_access_lane(None), ACCESS_LANE_FREE)
        self.assertEqual(_access_lane(None, paid=True), ACCESS_LANE_PAID)
        self.assertEqual(_access_lane("token"), ACCESS_LANE_TRUSTED_ARTIFACT)
        # A trusted token always wins, so QA traffic never looks like paid usage.
        self.assertEqual(_access_lane("token", paid=True), ACCESS_LANE_TRUSTED_ARTIFACT)

    def test_provenance_summary_normalizes_existing_fields_without_invention(self) -> None:
        summary = _provenance_summary({
            "matches": [{"source_system": "Statistics Canada", "source_vintage": "2021"}],
            "geometry_sources": {"left": {"geometry_bank": "geometry/countries/CAN/geometry.parquet"}},
            "license": None,
            "geometry": {"coordinates": [[1, 2]]},
        })
        self.assertEqual(summary["schema_version"], "daedalmap.tool_provenance.v1")
        self.assertEqual(summary["status"], "reported")
        self.assertEqual(summary["source_systems"], ["Statistics Canada"])
        self.assertEqual(summary["source_vintages"], ["2021"])
        self.assertEqual(summary["bank_ids"], ["geometry/countries/CAN/geometry.parquet"])
        self.assertNotIn("licenses", summary)

    def test_provenance_summary_reports_missing_instead_of_guessing(self) -> None:
        self.assertEqual(_provenance_summary({"loc_id": "CAN-BC"}), {
            "schema_version": "daedalmap.tool_provenance.v1",
            "status": "not_reported",
        })

    def test_provenance_summary_drops_missing_value_sentinels(self) -> None:
        summary = _provenance_summary({
            "source_system": "geoboundaries",
            "geometry_source": float("nan"),
            "rows": [{"source_system": "nan"}, {"source_vintage": "<NA>"}],
        })

        self.assertEqual(summary["source_systems"], ["geoboundaries"])
        self.assertNotIn("source_vintages", summary)

    def test_no_legacy_free_preview_lane_remains(self) -> None:
        """Scan the whole runtime package, not just one module.

        The first version of this test only read routes/mcp.py, which let
        routes/geometry.py keep emitting `free_preview` after the unification -
        so MCP and the HTTP batch endpoint disagreed about the same lane.
        """
        from pathlib import Path

        import mapmover

        package_root = Path(mapmover.__file__).parent
        offenders = [
            path.relative_to(package_root).as_posix()
            for path in package_root.rglob("*.py")
            if "free_preview" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(
            offenders,
            [],
            "these modules still emit the retired free_preview lane",
        )


class DataHelperTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)

    def test_every_free_data_helper_has_a_capability_id(self) -> None:
        expected = {
            "get_tool_help",
            "how_geometry_works",
            "get_catalog",
            "get_pack",
            "get_live_earthquake_events",
            "get_live_volcano_events",
            "get_disaster_links_for_event",
            "get_disaster_link_chain",
            "search_disaster_links",
        }
        self.assertEqual(set(DATA_HELPER_CAPABILITIES), expected)
        # capability ids must be distinct so analytics can group on them
        self.assertEqual(
            len(set(DATA_HELPER_CAPABILITIES.values())),
            len(DATA_HELPER_CAPABILITIES),
        )

    def test_get_catalog_writes_a_usage_row(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            envelope = _tool_call_envelope(self.client, "get_catalog")

        result = envelope["result"]["structuredContent"]
        self.assertEqual(
            result["public_catalogs"]["data"]["download_url"],
            "https://downloads.daedalmap.com/downloadable/catalog.json",
        )
        self.assertEqual(
            result["public_catalogs"]["geometry"]["catalog_path"],
            "geometry/geometry_catalog.json",
        )

        analytics_mock.assert_called_once()
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["capability_id"], "catalog_discovery")
        self.assertEqual(analytics["source_id"], "get_catalog")
        self.assertEqual(analytics["decision"], "allow")
        self.assertEqual(analytics["payment_rail"], ACCESS_LANE_FREE)
        self.assertEqual(analytics["metadata"]["surface"], "agent_api_mcp")
        self.assertEqual(analytics["metadata"]["access_lane"], ACCESS_LANE_FREE)

    def test_get_pack_missing_pack_logs_a_deny(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            _tool_call_envelope(
                self.client,
                "get_pack",
                {"pack_id": "definitely_not_a_real_pack"},
            )

        analytics_mock.assert_called_once()
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["capability_id"], "pack_detail_discovery")
        self.assertEqual(analytics["decision"], "deny")
        self.assertEqual(analytics["error_code"], "pack_not_found")


class CatalogDrivenPackFacadeTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)

    def test_new_catalog_pack_gets_generic_mcp_facade_without_registry_edit(self) -> None:
        catalog = {"packs": [{
            "pack_id": "future_pack",
            "title": "Future Pack",
            "short_description": "A newly catalog-admitted pack.",
            "category": "environment",
        }]}
        with mock.patch("mapmover.routes.mcp.load_api_catalog", return_value=catalog):
            response = self.client.get("/mcp/future_pack")
            listed = self.client.post(
                "/mcp/future_pack",
                json={"jsonrpc": "2.0", "id": "new-pack", "method": "tools/list", "params": {}},
            )
            resources = self.client.post(
                "/mcp/future_pack",
                json={"jsonrpc": "2.0", "id": "new-pack-resources", "method": "resources/list", "params": {}},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["serverInfo"]["name"], "com.daedalmap/future_pack")
        self.assertEqual(response.json()["serverInfo"]["title"], "Future Pack")
        names = {item["name"] for item in listed.json()["result"]["tools"]}
        self.assertEqual(names, {"get_tool_help", "get_catalog", "get_pack", "query_dataset"})
        uris = {item["uri"] for item in resources.json()["result"]["resources"]}
        self.assertIn("daedalmap://pack/future_pack", uris)

    def test_pack_absent_from_catalog_has_no_generic_facade(self) -> None:
        with mock.patch("mapmover.routes.mcp.load_api_catalog", return_value={"packs": []}):
            response = self.client.get("/mcp/not_published")
        self.assertEqual(response.status_code, 404)


class BlindCallerHelpTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)

    def test_every_published_tool_has_complete_guidance_and_valid_example_keys(self) -> None:
        from mcp_surface_shared import build_tool_definitions
        from mcp_tool_help_shared import validate_guidance_examples, validate_tool_guidance

        definitions = build_tool_definitions()
        names = {str(tool.get("name") or "") for tool in definitions}
        self.assertEqual(validate_tool_guidance(names), [])
        self.assertEqual(validate_guidance_examples(definitions), [])

    def test_geometry_get_info_advertises_cold_start_sequence(self) -> None:
        response = self.client.get("/mcp/geography")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("howToStart", payload)
        self.assertGreaterEqual(len(payload["howToStart"]), 3)
        self.assertIn("how_geometry_works", payload["tools"])
        self.assertIn("get_tool_help", payload["tools"])
        self.assertTrue(any("how_geometry_works" in step for step in payload["howToStart"]))
        self.assertTrue(any("get_tool_help" in step for step in payload["howToStart"]))

    def test_every_narrow_facade_exposes_the_free_help_tool(self) -> None:
        from pack_registry_shared import pack_tool_allowlists

        for facade, tools in pack_tool_allowlists().items():
            with self.subTest(facade=facade):
                self.assertIn("get_tool_help", tools)

    def test_help_reports_enforced_point_limits_and_paid_throughput(self) -> None:
        envelope = _tool_call_envelope(
            self.client,
            "get_tool_help",
            {"tool_name": "resolve_point"},
            path="/mcp/geography",
        )
        payload = envelope["result"]["structuredContent"]
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["access"]["pricing"], "paid_bulk_x402_base_usdc")
        self.assertEqual(payload["access"]["limits"]["free_item_limit"], 100)
        self.assertEqual(payload["access"]["limits"]["paid_item_limit"], 10000)
        self.assertTrue(payload["examples"])
        self.assertEqual(payload["interaction_contract"]["natural_language_owner"], "calling_client_llm")
        self.assertEqual(payload["interaction_contract"]["execution_input"], "strict_json_schema")
        self.assertIn("clarification_shape", payload["interaction_contract"])
        self.assertIn("loc_id_info", payload["recommended_next_calls"])
        self.assertIn("/mcp/reverse-geocoding", payload["available_on_facades"])
        self.assertEqual(payload["provenance"]["schema_version"], "daedalmap.tool_provenance.v1")

    def test_help_can_explain_geometry_family_helper(self) -> None:
        envelope = _tool_call_envelope(
            self.client,
            "get_tool_help",
            {"tool_name": "how_geometry_works"},
            path="/mcp/geography",
        )
        payload = envelope["result"]["structuredContent"]
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["tool_name"], "how_geometry_works")
        self.assertIn("get_tool_help", payload["recommended_next_calls"])

    def test_help_cannot_leak_tools_hidden_from_a_narrow_facade(self) -> None:
        envelope = _tool_call_envelope(
            self.client,
            "get_tool_help",
            {"tool_name": "resolve_point"},
            path="/mcp/currency",
        )
        payload = envelope["result"]["structuredContent"]
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "tool_not_found")

    def test_help_call_has_stable_free_analytics(self) -> None:
        with mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock:
            _tool_call_envelope(self.client, "get_tool_help", {"tool_name": "get_catalog"})
        analytics = analytics_mock.call_args.kwargs
        self.assertEqual(analytics["capability_id"], "tool_help_discovery")
        self.assertEqual(analytics["payment_rail"], ACCESS_LANE_FREE)


class TrustedArtifactBypassTests(unittest.TestCase):
    """Every capped geometry tool must be testable above its cap."""

    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(mcp_router)
        self.client = TestClient(app)
        self.token = "qa-universe-token"

    def _headers(self) -> dict:
        return {"authorization": f"Bearer {self.token}"}

    def test_capped_tools_reject_oversized_batches_without_a_token(self) -> None:
        cases = {
            "resolve_reference": {"from_system": "zip", "items": [{"value": str(i)} for i in range(200)]},
            "convert_reference": {
                "from_system": "zip",
                "to_system": "loc_id",
                "items": [{"value": str(i)} for i in range(200)],
            },
            "compare_geographies": {
                "items": [{"left_loc_id": "USA", "right_loc_id": "USA"} for _ in range(200)]
            },
            "loc_id_info": {"loc_ids": [f"USA-{i}" for i in range(200)]},
        }
        for tool, arguments in cases.items():
            with self.subTest(tool=tool):
                envelope = _tool_call_envelope(self.client, tool, arguments)
                result = envelope["result"]
                self.assertTrue(
                    result.get("isError"),
                    f"{tool} should reject an over-cap batch without a trusted token",
                )

    def test_trusted_token_lifts_the_cap_on_every_capped_tool(self) -> None:
        env = {"ARTIFACT_ACCESS_TOKENS": f"qa={self.token}"}
        cases = {
            "resolve_reference": {"from_system": "zip", "items": [{"value": str(i)} for i in range(200)]},
            "convert_reference": {
                "from_system": "zip",
                "to_system": "loc_id",
                "items": [{"value": str(i)} for i in range(200)],
            },
            "compare_geographies": {
                "items": [{"left_loc_id": "USA", "right_loc_id": "USA"} for _ in range(200)]
            },
            "loc_id_info": {"loc_ids": [f"USA-{i}" for i in range(200)]},
        }
        for tool, arguments in cases.items():
            with self.subTest(tool=tool):
                with mock.patch.dict("os.environ", env, clear=False):
                    envelope = _tool_call_envelope(
                        self.client, tool, arguments, headers=self._headers()
                    )
                result = envelope["result"]
                structured = result.get("structuredContent") or {}
                error_code = str((structured.get("error") or {}).get("code") or "")
                # The call may still fail on data grounds, but never on the cap.
                self.assertNotIn(
                    error_code,
                    {"too_many_items", "too_many_loc_ids", "too_many_loc_ids_for_references"},
                    f"{tool} still enforced its item cap against a trusted artifact token",
                )

    def test_trusted_token_bypasses_per_tool_call_rate_limit(self) -> None:
        env = {"ARTIFACT_ACCESS_TOKENS": f"qa={self.token}"}
        with (
            mock.patch.dict("os.environ", env, clear=False),
            mock.patch("mapmover.routes.mcp.rate_limiter.check", return_value=(False, 60)) as limiter_mock,
            mock.patch("mapmover.routes.mcp.log_api_query_event") as analytics_mock,
        ):
            envelope = _tool_call_envelope(
                self.client,
                "get_tool_help",
                {"tool_name": "get_catalog"},
                headers=self._headers(),
            )
        self.assertTrue(envelope["result"]["structuredContent"]["ok"])
        limiter_mock.assert_not_called()
        self.assertEqual(analytics_mock.call_args.kwargs["payment_rail"], ACCESS_LANE_TRUSTED_ARTIFACT)
        self.assertTrue(analytics_mock.call_args.kwargs["metadata"]["rate_limit_bypassed"])

    def test_local_runtime_bypasses_rate_but_keeps_item_caps(self) -> None:
        with (
            mock.patch("mapmover.routes.mcp.is_local_loopback_request", return_value=True),
            mock.patch("mapmover.routes.mcp.rate_limiter.check", return_value=(False, 60)) as limiter_mock,
            mock.patch(
                "mapmover.runtime.geometry_tool_jobs._run_conversion_row",
                return_value={"ok": True, "resolved_loc_id": "USA-CA-001"},
            ),
            mock.patch.dict(
                "os.environ",
                {"MCP_TOOL_BATCH_LIMIT_CREATE_CONVERSION_JOB": "2"},
                clear=False,
            ),
        ):
            help_envelope = _tool_call_envelope(
                self.client,
                "get_tool_help",
                {"tool_name": "create_conversion_job"},
            )
            create_envelope = _tool_call_envelope(
                self.client,
                "create_conversion_job",
                {
                    "from_system": "admin.native_id",
                    "items": [{"value": str(index)} for index in range(3)],
                },
            )

        access = help_envelope["result"]["structuredContent"]["access"]
        self.assertEqual(access["access_lane"], "local_installed")
        self.assertEqual(access["limits"]["free_item_limit"], 2)
        self.assertFalse(access["rate_limited_independently"])
        self.assertTrue(access["service_item_caps_enforced"])
        self.assertFalse(access["payment_required"])
        self.assertEqual(
            create_envelope["result"]["structuredContent"]["error"]["code"],
            "bounded_inline_limit_exceeded",
        )
        limiter_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class ToolAccessRegistryTests(unittest.TestCase):
    """The registry is the one place limits and free/paid are authored."""

    def test_every_dispatched_tool_is_registered(self) -> None:
        from mcp_surface_shared import build_tool_definitions
        from tool_access_shared import TOOL_ACCESS_REGISTRY

        published = {str(tool.get("name")) for tool in build_tool_definitions()}
        missing = sorted(published - set(TOOL_ACCESS_REGISTRY))
        self.assertEqual(
            missing,
            [],
            "every published MCP tool needs an access profile so nothing is silently ungoverned",
        )

    def test_limits_come_from_the_registry_not_inline_defaults(self) -> None:
        from pathlib import Path

        import mapmover.routes.mcp as mcp_module

        source = Path(mcp_module.__file__).read_text(encoding="utf-8")
        # An inline default would mean the registry is no longer the single
        # place to change a limit.
        self.assertNotIn("_tool_batch_item_limit(\"resolve_point\", default=", source)
        self.assertNotIn("fallback_env_names=(\"POINT_LOOKUP_BATCH_LIMIT\",)", source)

    def test_registry_values_reach_the_runtime(self) -> None:
        import mapmover.routes.mcp as mcp_module
        from tool_access_shared import tool_free_item_limit

        for tool in ("resolve_point", "get_geometry", "check_geometry", "loc_id_info"):
            with self.subTest(tool=tool):
                self.assertEqual(
                    mcp_module._tool_batch_item_limit(tool),
                    tool_free_item_limit(tool),
                )

    def test_env_override_still_wins_over_the_registry(self) -> None:
        import mapmover.routes.mcp as mcp_module

        with mock.patch.dict("os.environ", {"MCP_TOOL_BATCH_LIMIT_RESOLVE_POINT": "7"}, clear=False):
            self.assertEqual(mcp_module._tool_batch_item_limit("resolve_point"), 7)


class PaidBulkLicensingTests(unittest.TestCase):
    """Licence permission is the ceiling on what may be sold."""

    def test_free_permission_blocks_paid_bulk(self) -> None:
        from tool_access_shared import licensing_permits_paid_bulk

        self.assertFalse(licensing_permits_paid_bulk({"free"}))
        self.assertFalse(licensing_permits_paid_bulk({"paid", "free"}))
        self.assertFalse(licensing_permits_paid_bulk({"other"}))
        self.assertFalse(licensing_permits_paid_bulk({"paid", "other"}))
        self.assertTrue(licensing_permits_paid_bulk({"paid"}))

    def test_missing_licence_data_fails_closed_to_free(self) -> None:
        from tool_access_shared import licensing_permits_paid_bulk

        self.assertFalse(licensing_permits_paid_bulk(set()))
        self.assertFalse(licensing_permits_paid_bulk(None))

    def test_paid_bulk_is_blocked_when_a_bank_is_free_licensed(self) -> None:
        import mapmover.routes.mcp as mcp_module

        with mock.patch(
            "mapmover.runtime.geometry_catalog.geometry_bank_access_facts",
            return_value=({"paid", "free"}, True),
        ):
            self.assertFalse(mcp_module._tool_paid_bulk_enforced("resolve_point"))

    def test_paid_bulk_allowed_when_every_bank_permits_paid(self) -> None:
        import mapmover.routes.mcp as mcp_module

        with mock.patch(
            "mapmover.runtime.geometry_catalog.geometry_bank_access_facts",
            return_value=({"paid"}, True),
        ):
            self.assertTrue(mcp_module._tool_paid_bulk_enforced("resolve_point"))

    def test_conversion_billing_does_not_inherit_geometry_redistribution_terms(self) -> None:
        """Identity-only conversion may meter work without returning source geometry."""
        import mapmover.routes.mcp as mcp_module

        with (
            mock.patch(
                "mapmover.runtime.geometry_catalog.geometry_bank_access_facts",
                side_effect=AssertionError("conversion must not inspect geometry-bank licensing"),
            ),
            mock.patch.object(
                mcp_module,
                "resolve_effective_access",
                return_value={"settlement_required": True},
            ) as resolve_mock,
        ):
            self.assertTrue(mcp_module._tool_paid_bulk_enforced("create_conversion_job"))

        self.assertEqual(resolve_mock.call_args.kwargs["license_permissions"], {"paid"})
        self.assertTrue(resolve_mock.call_args.kwargs["publication_cleared"])

    def test_free_tools_never_enforce_paid_bulk(self) -> None:
        import mapmover.routes.mcp as mcp_module

        for tool in ("get_geometry", "check_geometry", "read_geometry_catalog", "get_catalog"):
            with self.subTest(tool=tool):
                self.assertFalse(mcp_module._tool_paid_bulk_enforced(tool))

    def test_quote_and_status_tools_stay_free(self) -> None:
        """Quotes and job polling must never be gated, or the paid flow breaks."""
        from tool_access_shared import tool_is_paid_bulk

        for tool in (
            "estimate_geometry_package",
            "estimate_conversion_job",
            "get_job_status",
            "read_geometry_catalog",
            "list_reference_systems",
            "get_catalog",
            "get_pack",
        ):
            with self.subTest(tool=tool):
                self.assertFalse(tool_is_paid_bulk(tool))
