from __future__ import annotations

import contextlib
import io
import json
import unittest

import mcp_stdio


class StdioToolHelpTests(unittest.TestCase):
    def _call(self, target: str) -> dict:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            mcp_stdio._handle({
                "jsonrpc": "2.0",
                "id": "stdio-help",
                "method": "tools/call",
                "params": {"name": "get_tool_help", "arguments": {"tool_name": target}},
            })
        return json.loads(output.getvalue())

    def test_static_help_executes_without_runtime_data(self) -> None:
        envelope = self._call("resolve_points")
        payload = envelope["result"]["structuredContent"]
        self.assertFalse(envelope["result"]["isError"])
        self.assertEqual(payload["access"]["limits"]["free_item_limit"], 100)
        self.assertEqual(payload["access"]["limits"]["paid_item_limit"], 10000)
        self.assertIn("/mcp/geography", payload["available_on_facades"])

    def test_unknown_static_help_is_typed(self) -> None:
        envelope = self._call("not_a_tool")
        self.assertTrue(envelope["result"]["isError"])
        self.assertEqual(envelope["result"]["structuredContent"]["error"]["code"], "tool_not_found")


if __name__ == "__main__":
    unittest.main()
