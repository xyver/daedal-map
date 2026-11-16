"""Global explicit-family selection keeps the existing Admin default."""

import asyncio
from unittest.mock import AsyncMock, patch

from mapmover.routes.mcp import _execute_resolve_point_tool


def test_global_family_dispatches_to_shared_point_execution():
    with patch("mapmover.routes.mcp._execute_point_lookup_tool", new_callable=AsyncMock) as execute:
        asyncio.run(_execute_resolve_point_tool(None, {
            "lat": 38.795406, "lon": -115.555946,
            "scope_id": "GLOBAL", "family": "ecological_region",
        }, "test-global-family"))
    payload = execute.await_args.args[1]
    assert payload["point_scope_id"] == "GLOBAL"
    assert payload["family"] == "ecological_region"
    assert "country_scope" not in payload


def test_global_admin_remains_default():
    with patch("mapmover.routes.mcp._execute_point_lookup_tool", new_callable=AsyncMock) as execute:
        asyncio.run(_execute_resolve_point_tool(None, {
            "lat": 38.795406, "lon": -115.555946,
        }, "test-global-admin"))
    payload = execute.await_args.args[1]
    assert payload["point_scope_id"] == "GLOBAL"
    assert payload["family"] == "administrative"
