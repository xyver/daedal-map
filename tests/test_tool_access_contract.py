from __future__ import annotations

import ast
import os
import unittest
from pathlib import Path
from unittest import mock

from tool_access_shared import (
    paid_bulk_tool_ids,
    resize_charge_quote,
    tool_account_item_limit,
    tool_charge_quote,
    tool_charge_units,
    tool_effective_item_limit,
    tool_meter,
    tool_payment_required_payload,
    tool_pricing_version,
    tool_quote,
)
from access_policy_shared import clear_access_policy_cache


ROOT = Path(__file__).resolve().parents[1]


class ToolAccessContractTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_access_policy_cache()

    def test_registry_source_has_no_duplicate_literal_keys(self) -> None:
        tree = ast.parse((ROOT / "tool_access_shared.py").read_text(encoding="utf-8"))
        duplicates: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            seen: set[object] = set()
            for key in node.keys:
                if not isinstance(key, ast.Constant):
                    continue
                if key.value in seen:
                    duplicates.append(f"line {key.lineno}: {key.value!r}")
                seen.add(key.value)
        self.assertEqual(duplicates, [])

    def test_every_paid_tool_has_a_versioned_meter_and_integer_quote(self) -> None:
        for tool_name in paid_bulk_tool_ids():
            with self.subTest(tool_name=tool_name):
                self.assertNotEqual(tool_pricing_version(tool_name), "unpriced-v0")
                self.assertTrue(tool_meter(tool_name).get("unit"))
                quote = tool_quote(tool_name, 1, free_limit=0)
                self.assertIsInstance(quote["amount_usdc_base_units"], int)
                self.assertGreaterEqual(quote["amount_usdc_base_units"], 0)

    def test_price_and_limit_levers_share_one_env_convention(self) -> None:
        env = {
            "MCP_TOOL_PRICE_BASE_MICRO_USD_CREATE_CONVERSION_JOB": "12345",
            "MCP_TOOL_PRICE_PER_UNIT_MICRO_USD_CREATE_CONVERSION_JOB": "321",
            "MCP_TOOL_BATCH_LIMIT_CREATE_CONVERSION_JOB": "4321",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            quote = tool_quote("create_conversion_job", 2, free_limit=0)
            self.assertEqual(quote["amount_usdc_base_units"], 13000)
            self.assertEqual(quote["price_credits"], 1.3)
            self.assertEqual(tool_effective_item_limit("create_conversion_job"), 4321)

    def test_shared_challenge_preserves_the_canonical_quote(self) -> None:
        payload = tool_payment_required_payload(
            "resolve_point", 101, free_limit=100, paid_limit=10_000, request_id="req-1"
        )
        self.assertEqual(payload["quote"]["capability_id"], "point_lookup")
        self.assertEqual(payload["quote"]["amount_usdc_base_units"], 10_000)
        self.assertEqual(payload["limits"], {"free_batch_limit": 100, "paid_batch_limit": 10_000})

    def test_conversion_job_uses_one_authored_meter_and_quote(self) -> None:
        units = tool_charge_units("create_conversion_job", 3144, minimum=True)
        quote = tool_charge_quote("create_conversion_job", units)
        self.assertEqual(units, 32)
        self.assertEqual(quote["amount_usdc_base_units"], 1_610_000)
        self.assertEqual(quote["pricing_source"], "registry")

    def test_dashboard_price_override_is_the_quote_authority(self) -> None:
        policy = (
            '{"schema_version":"1.0.0","policy_revision":"price-test-1",'
            '"mode":"enforce","pricing":{"tools":{"create_conversion_job":'
            '{"base_micro_usd":20000,"per_unit_micro_usd":60000}}}}'
        )
        with mock.patch.dict(os.environ, {"DAEDALMAP_ACCESS_POLICY_JSON": policy}, clear=False):
            clear_access_policy_cache()
            quote = tool_charge_quote("create_conversion_job", 32)
        self.assertEqual(quote["amount_usdc_base_units"], 1_940_000)
        self.assertEqual(quote["pricing_source"], "operator_policy")
        self.assertEqual(quote["pricing_version"], "operator-policy:price-test-1+credit-q1000")

    def test_actual_charge_resizes_the_estimate_without_repricing_it(self) -> None:
        estimate = tool_charge_quote("create_conversion_job", 32)
        actual = resize_charge_quote(estimate, 30)
        self.assertEqual(actual["pricing_version"], estimate["pricing_version"])
        self.assertEqual(actual["per_unit_micro_usd"], 50_000)
        self.assertEqual(actual["amount_usdc_base_units"], 1_510_000)


class AccountLaneTests(unittest.TestCase):
    """The middle rung of the entitlement ladder.

    Signing up must be worth a visible jump without handing over the paid
    ceiling, which is the thing a plan exists to sell.
    """

    def test_account_lane_sits_between_free_and_paid(self) -> None:
        for tool in ("resolve_point", "resolve_reference", "convert_reference", "loc_id_info"):
            with self.subTest(tool=tool):
                free = tool_effective_item_limit(tool, lane="free")
                account = tool_effective_item_limit(tool, lane="account")
                paid = tool_effective_item_limit(tool, lane="paid")
                self.assertLess(free, account)
                self.assertLessEqual(account, paid)

    def test_account_limit_never_exceeds_the_paid_limit(self) -> None:
        """The derived 10x must clamp, or a free account could outrank a paying one."""
        for tool in ("resolve_point", "check_geometry", "get_geometry", "resolve_loc_id_scope"):
            with self.subTest(tool=tool):
                self.assertLessEqual(tool_account_item_limit(tool), tool_effective_item_limit(tool, lane="paid"))

    def test_account_lane_has_its_own_env_override(self) -> None:
        with mock.patch.dict(os.environ, {"MCP_TOOL_ACCOUNT_BATCH_LIMIT_RESOLVE_POINT": "777"}, clear=False):
            self.assertEqual(tool_effective_item_limit("resolve_point", lane="account"), 777)
            self.assertEqual(tool_effective_item_limit("resolve_point", lane="free"), 100)


if __name__ == "__main__":
    unittest.main()
